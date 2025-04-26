// Package subscribers contains the subscriber registry for the EventStore.
package subscribers

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"goeventsource/src/internal/eventstore/models"
	"goeventsource/src/internal/port/outbound"
)

// Common errors
var (
	ErrSubscriberNotFound      = errors.New("subscriber not found")
	ErrSubscriberAlreadyExists = errors.New("subscriber already exists")
	ErrInvalidSubscriberID     = errors.New("invalid subscriber ID")
	ErrSubscriberClosed        = errors.New("subscriber is closed")
)

// WorkerPoolConfig defines configuration for the event delivery worker pool
type WorkerPoolConfig struct {
	// MaxWorkers is the maximum number of concurrent workers for event delivery
	MaxWorkers int
	// QueueSize is the size of the worker task queue
	QueueSize int
}

// DefaultWorkerPoolConfig returns sensible defaults for worker pool
func DefaultWorkerPoolConfig() WorkerPoolConfig {
	return WorkerPoolConfig{
		MaxWorkers: 50,
		QueueSize:  1000,
	}
}

// deliveryTask represents a task to deliver an event to a subscriber
type deliveryTask struct {
	subscriber outbound.Subscriber
	event      outbound.Event
	ctx        context.Context
	resultChan chan<- error
}

// workerPool manages a pool of workers for concurrent event delivery
type workerPool struct {
	taskQueue  chan deliveryTask
	workerWg   sync.WaitGroup
	ctx        context.Context
	cancelFunc context.CancelFunc
	logger     *SubscriberLogger
}

// newWorkerPool creates a new worker pool with the given configuration
func newWorkerPool(config WorkerPoolConfig, logger *SubscriberLogger) *workerPool {
	ctx, cancel := context.WithCancel(context.Background())
	pool := &workerPool{
		taskQueue:  make(chan deliveryTask, config.QueueSize),
		ctx:        ctx,
		cancelFunc: cancel,
		logger:     logger,
	}

	// Start the workers
	pool.workerWg.Add(config.MaxWorkers)
	for i := 0; i < config.MaxWorkers; i++ {
		go pool.worker()
	}

	return pool
}

// worker processes delivery tasks from the queue
func (wp *workerPool) worker() {
	defer wp.workerWg.Done()

	for {
		select {
		case <-wp.ctx.Done():
			return
		case task := <-wp.taskQueue:
			startTime := time.Now()
			var deliveryErr error

			// Use GetID() from the interface
			subscriberID := task.subscriber.GetID()

			// Log delivery attempt
			if wp.logger != nil {
				wp.logger.Debug(task.ctx, "Attempting event delivery", map[string]interface{}{
					"subscriber_id": subscriberID,
					"event_id":      task.event.ID,
					"event_type":    task.event.Type,
					"topic":         task.event.Topic,
				})
			}

			// Check if context is still valid before proceeding
			select {
			case <-task.ctx.Done():
				// Context cancelled before delivery attempt
				deliveryErr = task.ctx.Err()
			default:
				// We need the *concrete* timeout config for the specific subscriber.
				// This requires a type assertion or a method on the interface.
				// For now, let's assume we can get it via type assertion if it's a models.Subscriber.
				// A cleaner way might be to pass the calculated effective timeout into ReceiveEvent.
				var effectiveTimeout time.Duration = 1 * time.Second // Default if type assertion fails
				if subModel, ok := task.subscriber.(*models.Subscriber); ok {
					subscriberTimeout := subModel.Timeout.CurrentTimeout
					effectiveTimeout = subscriberTimeout

					if taskDeadline, ok := task.ctx.Deadline(); ok {
						callerTimeout := time.Until(taskDeadline)
						if callerTimeout < subscriberTimeout {
							effectiveTimeout = callerTimeout
						}
					}
				} else {
					// Handle case where subscriber is not *models.Subscriber (e.g., mock)
					// Use context deadline if available, otherwise default.
					if taskDeadline, ok := task.ctx.Deadline(); ok {
						effectiveTimeout = time.Until(taskDeadline)
					} // else keep the default
				}

				// Ensure timeout is not negative
				if effectiveTimeout < 0 {
					effectiveTimeout = 0
				}

				// Create delivery timeout context respecting the effective timeout
				deliveryCtx, cancel := context.WithTimeout(task.ctx, effectiveTimeout)
				deliveryErr = task.subscriber.ReceiveEvent(deliveryCtx, task.event)
				cancel() // Always cancel the context to avoid leaks
			}

			duration := time.Since(startTime)
			success := deliveryErr == nil

			// Log delivery outcome
			if wp.logger != nil {
				if deliveryErr != nil {
					// Get retry info using the interface method
					retryCount, _, nextTimeout := task.subscriber.GetRetryInfo()
					// Prepare subscriber details for logging
					// TODO: Add actual client connection details here if available (e.g., callback URL)
					subDetails := map[string]interface{}{
						"subscriber_topics": task.subscriber.GetTopics(),
						// "subscriber_callback_url": task.subscriber.CallbackURL, // Example - needs type assertion if specific to model
					}
					wp.logger.LogErrorDelivery(task.ctx, subscriberID, task.event.ID, deliveryErr, retryCount, nextTimeout, map[string]interface{}{"subscriber_details": subDetails})
				} else {
					// Log successful delivery using the standard method
					wp.logger.LogEventDelivery(task.ctx, subscriberID, task.event.ID, success, duration, nil)
				}
			}

			// Report result if channel provided
			if task.resultChan != nil {
				task.resultChan <- deliveryErr
			}
		}
	}
}

// submit adds a task to the worker pool
func (wp *workerPool) submit(ctx context.Context, subscriber outbound.Subscriber, event outbound.Event, resultChan chan<- error) bool {
	// Create a task
	task := deliveryTask{
		subscriber: subscriber,
		event:      event,
		ctx:        ctx,
		resultChan: resultChan,
	}

	// Try to submit the task
	select {
	case <-wp.ctx.Done():
		// Worker pool is shutting down
		return false
	case <-ctx.Done():
		// Context cancelled
		return false
	case wp.taskQueue <- task:
		// Task submitted successfully
		return true
	default:
		// Queue is full, task dropped
		return false
	}
}

// shutdown gracefully shuts down the worker pool
func (wp *workerPool) shutdown(timeout time.Duration) {
	// Signal workers to stop
	wp.cancelFunc()

	// Wait for workers to finish with timeout
	c := make(chan struct{})
	go func() {
		wp.workerWg.Wait()
		close(c)
	}()

	select {
	case <-c:
		// All workers finished
	case <-time.After(timeout):
		// Timeout reached
	}
}

// Registry is a thread-safe registry of subscribers
type Registry struct {
	// All subscribers, keyed by subscriber ID
	subscribers map[string]outbound.Subscriber

	// Subscribers by topic, for efficient event delivery
	topicSubscribers map[string]map[string]outbound.Subscriber

	// Worker pool for event delivery
	workerPool *workerPool

	// Logger for registry operations
	logger *SubscriberLogger

	// Channel to stop the timeout monitor
	timeoutMonitorStopCh chan struct{}

	// Mutex for thread safety
	mu sync.RWMutex
}

// NewRegistry creates a new registry with default configuration
func NewRegistry() *Registry {
	return NewRegistryWithConfig(DefaultWorkerPoolConfig())
}

// NewRegistryWithConfig creates a new registry with custom worker pool configuration
func NewRegistryWithConfig(poolConfig WorkerPoolConfig) *Registry {
	r := &Registry{
		subscribers:      make(map[string]outbound.Subscriber),
		topicSubscribers: make(map[string]map[string]outbound.Subscriber),
		logger:           NewSubscriberLogger(INFO),
	}
	r.workerPool = newWorkerPool(poolConfig, r.logger)

	// Start timeout monitor with default settings
	// Check every 30 seconds, deregister subscribers with more than 10 errors
	r.timeoutMonitorStopCh = r.StartTimeoutMonitor(30*time.Second, 10)

	return r
}

// SetLogger sets the logger for the registry
func (r *Registry) SetLogger(logger *SubscriberLogger) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.logger = logger
}

// RegisterOptions contains optional parameters for subscriber registration
type RegisterOptions struct {
	// ClientIP is the IP address of the client registering the subscriber
	ClientIP string

	// UserAgent is the user agent of the client registering the subscriber
	UserAgent string

	// ClientID is an identifier for the client registering the subscriber
	ClientID string

	// Expiration is when the subscription will expire (zero value means no expiration)
	Expiration time.Time

	// RequestID is a correlation ID for tracing the registration request
	RequestID string
}

// DefaultRegisterOptions returns default registration options
func DefaultRegisterOptions() RegisterOptions {
	return RegisterOptions{}
}

// Register adds a new subscriber to the registry
// Returns the created subscriber as outbound.Subscriber interface
func (r *Registry) Register(config models.SubscriberConfig) (outbound.Subscriber, error) {
	return r.RegisterWithClientInfo(context.Background(), config, DefaultRegisterOptions())
}

// RegisterWithClientInfo adds a new subscriber to the registry with client information for logging
// Returns the created subscriber as outbound.Subscriber interface
func (r *Registry) RegisterWithClientInfo(ctx context.Context, config models.SubscriberConfig, options RegisterOptions) (outbound.Subscriber, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Generate ID if not provided
	if config.ID == "" {
		config.ID = generateSubscriberID()
	} else {
		// Validate provided ID format
		if _, err := uuid.Parse(config.ID); err != nil {
			if r.logger != nil {
				r.logger.Error(ctx, "Invalid subscriber ID format", map[string]interface{}{
					"provided_id": config.ID,
					"error":       err.Error(),
				})
			}
			return nil, fmt.Errorf("%w: %s", ErrInvalidSubscriberID, config.ID)
		}
	}

	// Check if subscriber already exists
	if _, exists := r.subscribers[config.ID]; exists {
		if r.logger != nil {
			r.logger.Error(ctx, "Failed to register subscriber: ID already exists", map[string]interface{}{
				"subscriber_id": config.ID,
			})
		}
		return nil, fmt.Errorf("%w: %s", ErrSubscriberAlreadyExists, config.ID)
	}

	// Create the concrete subscriber model
	subscriber := models.NewSubscriber(config)
	if r.logger != nil {
		// Use a logger with the subscriber prefix if possible
		// No type assertion needed here, 'subscriber' is *models.Subscriber
		subscriber.SetLogger(r.logger.WithPrefix(fmt.Sprintf("[SUBSCRIBER %s] ", config.ID)).logger)
	}

	// Add to main registry
	r.subscribers[subscriber.ID] = subscriber // Store the concrete type which implements the interface

	// Add to topic maps
	r.addToTopicMapsLocked(subscriber) // Use internal locked version

	// Log registration
	if r.logger != nil {
		clientInfo := map[string]interface{}{
			"ip":         options.ClientIP,
			"user_agent": options.UserAgent,
			"client_id":  options.ClientID,
		}
		// Access fields directly on the concrete 'subscriber' type
		logFields := map[string]interface{}{
			"topics":        subscriber.Topics,
			"filter":        subscriber.Filter,
			"timeout":       subscriber.Timeout,
			"buffer_size":   subscriber.BufferSize,
			"creation_time": subscriber.CreatedAt,
		}

		if !options.Expiration.IsZero() {
			logFields["expiration"] = options.Expiration.Format(time.RFC3339)
			logFields["ttl"] = time.Until(options.Expiration).String()
		}

		r.logger.LogRegistration(ctx, subscriber.ID, clientInfo, logFields)
	}

	return subscriber, nil // Return the concrete type which satisfies the interface
}

// Get retrieves a subscriber by ID, returning the outbound.Subscriber interface
func (r *Registry) Get(id string) (outbound.Subscriber, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	subscriber, exists := r.subscribers[id]
	if !exists {
		return nil, ErrSubscriberNotFound
	}
	return subscriber, nil
}

// List returns subscribers matching the topic filter, as a slice of outbound.Subscriber interfaces
func (r *Registry) List(topic string) []outbound.Subscriber {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var result []outbound.Subscriber
	if topic == "" {
		// Return all subscribers
		result = make([]outbound.Subscriber, 0, len(r.subscribers))
		for _, sub := range r.subscribers {
			result = append(result, sub)
		}
	} else {
		// Return subscribers for the specific topic
		if topicMap, exists := r.topicSubscribers[topic]; exists {
			result = make([]outbound.Subscriber, 0, len(topicMap))
			for _, sub := range topicMap {
				result = append(result, sub)
			}
		}
	}
	return result
}

// ListByState returns subscribers matching the state filter
// Requires type assertion to access internal state
func (r *Registry) ListByState(state models.SubscriberState) []outbound.Subscriber {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]outbound.Subscriber, 0)
	for _, sub := range r.subscribers {
		// Type assertion needed here to access internal state
		if subModel, ok := sub.(*models.Subscriber); ok {
			if subModel.State == state {
				result = append(result, sub)
			}
		} else {
			// Handle non-models.Subscriber types if necessary, perhaps log a warning
			// Or assume only models.Subscriber can have meaningful state for this query
		}
	}
	return result
}

// Update allows modifying a subscriber's configuration.
// The updateFn receives the subscriber as an outbound.Subscriber interface.
// If modification of internal state (not on interface) is needed, type assertion is required within updateFn.
func (r *Registry) Update(id string, updateFn func(outbound.Subscriber) error) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	subscriber, exists := r.subscribers[id]
	if !exists {
		if r.logger != nil {
			r.logger.Warn(context.Background(), "Cannot update: subscriber not found",
				map[string]interface{}{"subscriber_id": id})
		}
		return ErrSubscriberNotFound
	}

	// Check if subscriber is closed
	if subscriber.GetState() == outbound.SubscriberStateClosed {
		if r.logger != nil {
			r.logger.Warn(context.Background(), "Cannot update: subscriber is closed",
				map[string]interface{}{"subscriber_id": id})
		}
		return ErrSubscriberClosed
	}

	err := updateFn(subscriber)
	if err != nil {
		if r.logger != nil {
			r.logger.Error(context.Background(), "Error updating subscriber",
				map[string]interface{}{"subscriber_id": id, "error": err.Error()})
		}
		return fmt.Errorf("update function failed: %w", err)
	}

	// Update topic maps if topics changed (requires type assertion or interface method)
	// Assuming updateFn handles calling subscriber.UpdateTopics if necessary.
	// We need to re-evaluate topic membership after update.
	// This requires knowing the *old* topics, which isn't straightforward with just the interface.
	// A possible solution is for updateFn to return old/new topics, or have UpdateTopics on interface.
	// For now, simplify: remove and re-add to be safe, assuming updateFn doesn't change topics directly.
	// --- Simplified Topic Update Handling (potential performance issue) ---
	r.removeFromTopicMapsLocked(subscriber) // Use internal locked version
	r.addToTopicMapsLocked(subscriber)      // Use internal locked version
	// --- End Simplified Handling ---

	if r.logger != nil {
		r.logger.Info(context.Background(), "Subscriber updated",
			map[string]interface{}{"subscriber_id": id})
	}
	return nil
}

// UpdateTopics updates the topics for a specific subscriber
// This still requires getting the concrete type to update the internal field.
// Consider adding UpdateTopics to the outbound.Subscriber interface.
func (r *Registry) UpdateTopics(id string, newTopics []string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	subscriber, exists := r.subscribers[id]
	if !exists {
		if r.logger != nil {
			r.logger.Warn(context.Background(), "Cannot update topics: subscriber not found",
				map[string]interface{}{"subscriber_id": id})
		}
		return ErrSubscriberNotFound
	}

	// Type assertion needed to update internal state
	subModel, ok := subscriber.(*models.Subscriber)
	if !ok {
		// Handle case where subscriber is not *models.Subscriber
		if r.logger != nil {
			r.logger.Error(context.Background(), "Cannot update topics: subscriber is not expected type",
				map[string]interface{}{"subscriber_id": id})
		}
		return fmt.Errorf("cannot update topics on non-standard subscriber type")
	}

	oldTopics := subModel.Topics

	// Remove from old topic maps
	r.removeFromTopicMapsLocked(subscriber)

	// Update the subscriber's topics
	subModel.Topics = newTopics
	// TODO: Should updateLastActivity be called here?

	// Add to new topic maps
	r.addToTopicMapsLocked(subscriber)

	if r.logger != nil {
		r.logger.Info(context.Background(), "Subscriber topics updated",
			map[string]interface{}{
				"subscriber_id": id,
				"old_topics":    oldTopics,
				"new_topics":    newTopics,
			})
	}
	return nil
}

// Deregister removes a subscriber from the registry
func (r *Registry) Deregister(id string) error {
	return r.deregisterWithReason(context.Background(), id, "manual", nil)
}

// _deregisterLocked removes a subscriber assuming the write lock is already held
func (r *Registry) _deregisterLocked(ctx context.Context, subscriber outbound.Subscriber, reason string, details map[string]interface{}) {
	subscriberID := subscriber.GetID()

	// Get stats before removal for logging - use interface method
	stats := subscriber.GetStats()

	// Get creation time via type assertion (if needed for lifetime calc)
	var creationTime time.Time
	if subModel, ok := subscriber.(*models.Subscriber); ok {
		creationTime = subModel.CreatedAt
	}
	lifetime := time.Since(creationTime) // May be zero if not models.Subscriber or recently created

	// Remove from topic maps
	r.removeFromTopicMapsLocked(subscriber)

	// Remove from subscribers map
	delete(r.subscribers, subscriberID)

	// Close the subscriber - use interface method
	err := subscriber.Close()
	if err != nil && r.logger != nil {
		r.logger.Error(ctx, "Error closing subscriber during deregistration", map[string]interface{}{
			"subscriber_id": subscriberID,
			"error":         err.Error(),
		})
	}

	if r.logger != nil {
		logFields := map[string]interface{}{
			"reason":           reason,
			"lifetime":         lifetime.String(),
			"events_delivered": stats.EventsDelivered,
			"events_dropped":   stats.EventsDropped,
			"error_count":      stats.ErrorCount,
			"timeout_count":    stats.TimeoutCount,
		}
		// Merge details if provided
		if details != nil {
			for k, v := range details {
				logFields[k] = v
			}
		}
		r.logger.LogLifecycleEvent(ctx, subscriberID, "deregistered", logFields)
	}
}

// deregisterWithReason removes a subscriber, logging the reason and details
func (r *Registry) deregisterWithReason(ctx context.Context, id string, reason string, details map[string]interface{}) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	subscriber, exists := r.subscribers[id]
	if !exists {
		if r.logger != nil {
			r.logger.Warn(ctx, "Cannot deregister: subscriber not found",
				map[string]interface{}{"subscriber_id": id})
		}
		return ErrSubscriberNotFound
	}

	// Call the internal helper that assumes lock is held
	r._deregisterLocked(ctx, subscriber, reason, details)

	return nil
}

// BroadcastEvent sends an event to all matching subscribers for the event's topic
// using the worker pool for concurrent, non-blocking delivery
func (r *Registry) BroadcastEvent(ctx context.Context, event outbound.Event) int {
	startTime := time.Now()

	r.mu.RLock()
	// Find subscribers for this topic
	topicMap, exists := r.topicSubscribers[event.Topic]
	if !exists || len(topicMap) == 0 {
		r.mu.RUnlock()
		if r.logger != nil {
			r.logger.Debug(ctx, "No subscribers found for topic",
				map[string]interface{}{
					"topic":    event.Topic,
					"event_id": event.ID,
				})
		}
		return 0
	}

	// Create a copy of the subscribers for this topic to avoid holding the lock during delivery
	subscribers := make([]outbound.Subscriber, 0, len(topicMap)) // Use interface slice
	for _, sub := range topicMap {
		subscribers = append(subscribers, sub)
	}
	r.mu.RUnlock()

	if r.logger != nil {
		r.logger.Info(ctx, "Broadcasting event",
			map[string]interface{}{
				"event_id":         event.ID,
				"topic":            event.Topic,
				"event_type":       event.Type,
				"subscriber_count": len(subscribers),
				"event_timestamp":  time.Unix(0, event.Timestamp),
				"event_version":    event.Version,
			})
	}

	// Count subscribers that should receive the event
	successCount := 0

	// Create a channel to receive task results
	resultChan := make(chan error, len(subscribers))
	deliveryCount := 0

	// Submit delivery tasks to the worker pool for subscribers that match the filter criteria
	for _, subscriber := range subscribers {
		// Apply filtering before submitting to worker pool
		if subscriber.GetState() == outbound.SubscriberStateActive { // Use interface method
			// Check if the event should be delivered based on the subscriber's filter criteria
			if ShouldDeliverEvent(subscriber.GetFilter(), event) { // Use interface method
				r.workerPool.submit(ctx, subscriber, event, resultChan)
				deliveryCount++
			} else if r.logger != nil {
				r.logger.Debug(ctx, "Event filtered out for subscriber",
					map[string]interface{}{
						"event_id":      event.ID,
						"subscriber_id": subscriber.GetID(), // Use interface method
						"event_type":    event.Type,
						"filter":        subscriber.GetFilter(), // Use interface method
					})
			}
		}
	}

	// Wait for all tasks to complete
	for i := 0; i < deliveryCount; i++ {
		err := <-resultChan
		if err == nil {
			successCount++
		}
	}

	// Log broadcast results
	if r.logger != nil {
		duration := time.Since(startTime)
		var deliveryRate float64
		if deliveryCount > 0 {
			deliveryRate = float64(successCount) / float64(deliveryCount) * 100
		}
		r.logger.Info(ctx, "Event broadcast completed",
			map[string]interface{}{
				"event_id":          event.ID,
				"topic":             event.Topic,
				"subscriber_count":  len(subscribers),
				"filtered_count":    deliveryCount,
				"success_count":     successCount,
				"duration":          duration.String(),
				"delivery_rate_pct": deliveryRate,
			})
	}

	return successCount
}

// CleanupInactive removes subscribers that have been inactive longer than the provided duration
// Requires type assertion to access internal LastActivityAt field.
func (r *Registry) CleanupInactive(inactiveDuration time.Duration) int {
	ctx := context.Background() // Use a background context for cleanup operations
	r.mu.Lock()
	defer r.mu.Unlock()

	cutoffTime := time.Now().Add(-inactiveDuration)
	removedCount := 0

	// Find inactive subscribers
	var toRemove []string
	for id, sub := range r.subscribers {
		// Type assertion needed here
		if subModel, ok := sub.(*models.Subscriber); ok {
			if subModel.LastActivityAt.Before(cutoffTime) {
				toRemove = append(toRemove, id)
			}
		} else {
			// Cannot determine inactivity for non-standard subscriber types
		}
	}

	// Hold the lock while removing to prevent concurrent modifications
	for _, id := range toRemove {
		subscriber, exists := r.subscribers[id]
		if !exists {
			// Should not happen if lock is held correctly, but safety first
			continue
		}

		// Prepare details for logging (requires type assertion for LastActivityAt)
		var lastActivity time.Time
		if subModel, ok := subscriber.(*models.Subscriber); ok {
			lastActivity = subModel.LastActivityAt
		}
		details := map[string]interface{}{
			"inactive_duration": inactiveDuration.String(),
			"last_activity_at":  lastActivity.Format(time.RFC3339),
		}

		// Call the internal helper which assumes the lock is held
		r._deregisterLocked(ctx, subscriber, "inactive_timeout", details)
		removedCount++
	}

	return removedCount
}

// Count returns the total number of subscribers in the registry
func (r *Registry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return len(r.subscribers)
}

// CountByTopic returns the number of subscribers for a given topic
func (r *Registry) CountByTopic(topic string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if topicMap, exists := r.topicSubscribers[topic]; exists {
		return len(topicMap)
	}
	return 0
}

// Shutdown gracefully shuts down the worker pool and closes subscribers
func (r *Registry) Shutdown(timeout time.Duration) {
	// Stop timeout monitor if it's running
	if r.timeoutMonitorStopCh != nil {
		close(r.timeoutMonitorStopCh)
		r.timeoutMonitorStopCh = nil
		r.logger.Info(context.Background(), "Timeout monitor stopped", nil)
	}

	// Shutdown the worker pool first
	if r.workerPool != nil {
		r.logger.Info(context.Background(), "Shutting down subscriber worker pool...", nil)
		r.workerPool.shutdown(timeout)
		r.logger.Info(context.Background(), "Subscriber worker pool shut down.", nil)
	}

	// Close all remaining subscribers
	r.mu.Lock()
	defer r.mu.Unlock()

	r.logger.Info(context.Background(), "Closing remaining subscribers...", map[string]interface{}{"count": len(r.subscribers)})
	for id, sub := range r.subscribers {
		r.logger.Debug(context.Background(), "Closing subscriber during shutdown", map[string]interface{}{"subscriber_id": id})
		sub.Close() // Use interface method
	}
	r.logger.Info(context.Background(), "All subscribers closed.", nil)

	// Clear maps
	r.subscribers = make(map[string]outbound.Subscriber)
	r.topicSubscribers = make(map[string]map[string]outbound.Subscriber)
}

// StartTimeoutMonitor starts a background goroutine that periodically checks for
// timed-out subscribers and automatically deregisters them
func (r *Registry) StartTimeoutMonitor(checkInterval time.Duration, maxErrorThreshold int) (stopCh chan struct{}) {
	stopCh = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())

	r.logger.Info(ctx, "Starting timeout monitor for subscriber registry",
		map[string]interface{}{
			"check_interval":  checkInterval.String(),
			"error_threshold": maxErrorThreshold,
		})

	go func() {
		ticker := time.NewTicker(checkInterval)
		defer ticker.Stop()
		defer cancel()

		for {
			select {
			case <-stopCh:
				r.logger.Info(ctx, "Stopping timeout monitor", nil)
				return
			case <-ticker.C:
				r.checkAndDeregisterTimedOutSubscribers(ctx, maxErrorThreshold)
			}
		}
	}()

	return stopCh
}

// checkAndDeregisterTimedOutSubscribers checks all subscribers and deregisters those
// that have exceeded their maximum retries or timeout threshold
func (r *Registry) checkAndDeregisterTimedOutSubscribers(ctx context.Context, maxErrorThreshold int) {
	r.mu.RLock()
	// Create a copy of subscriber IDs to avoid holding lock during potentially lengthy operations
	var subscribersToCheck []string
	for id := range r.subscribers {
		subscribersToCheck = append(subscribersToCheck, id)
	}
	r.mu.RUnlock()

	deregisteredCount := 0

	for _, id := range subscribersToCheck {
		// Get the subscriber with a read lock
		r.mu.RLock()
		subscriber, exists := r.subscribers[id]
		r.mu.RUnlock()

		if !exists {
			continue
		}

		// Get retry information from the subscriber
		retryCount, lastRetryTime, _ := subscriber.GetRetryInfo()
		stats := subscriber.GetStats()

		shouldDeregister := false
		deregReason := ""
		details := map[string]interface{}{}

		// Check if subscriber has exceeded maximum error threshold
		if stats.ErrorCount >= int64(maxErrorThreshold) {
			shouldDeregister = true
			deregReason = "max_errors_exceeded"
			details["error_count"] = stats.ErrorCount
			details["max_errors"] = maxErrorThreshold
		}

		// Check if subscriber has exceeded maximum retries
		// Only do this check for subscribers that have actually had retry attempts
		if !shouldDeregister && retryCount > 0 {
			// Check if the subscriber says it's in a cooldown state
			if subModel, ok := subscriber.(*models.Subscriber); ok {
				if !subModel.ShouldRetry() && !lastRetryTime.IsZero() {
					// Check if it's been too long since the last retry (double the cooldown period)
					timeout := subModel.Timeout
					if timeout.MaxRetries > 0 && retryCount >= timeout.MaxRetries {
						cooldownExpiry := lastRetryTime.Add(timeout.CooldownPeriod * 2)
						if time.Now().After(cooldownExpiry) {
							shouldDeregister = true
							deregReason = "max_retries_exceeded"
							details["retry_count"] = retryCount
							details["max_retries"] = timeout.MaxRetries
							details["cooldown_period"] = timeout.CooldownPeriod.String()
							details["last_retry_time"] = lastRetryTime.Format(time.RFC3339)
						}
					}
				}
			}
		}

		// If any deregistration criteria met, remove the subscriber
		if shouldDeregister {
			if err := r.deregisterWithReason(ctx, id, deregReason, details); err != nil {
				r.logger.Error(ctx, "Failed to automatically deregister timed-out subscriber",
					map[string]interface{}{
						"subscriber_id": id,
						"reason":        deregReason,
						"error":         err.Error(),
					})
			} else {
				deregisteredCount++
				r.logger.Info(ctx, "Automatically deregistered subscriber due to timeout",
					map[string]interface{}{
						"subscriber_id": id,
						"reason":        deregReason,
						"details":       details,
					})
			}
		}
	}

	if deregisteredCount > 0 {
		r.logger.Info(ctx, "Timeout monitor completed", map[string]interface{}{
			"deregistered_count": deregisteredCount,
			"total_checked":      len(subscribersToCheck),
		})
	}
}

// addToTopicMaps adds a subscriber to the relevant topic maps
func (r *Registry) addToTopicMaps(subscriber outbound.Subscriber) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.addToTopicMapsLocked(subscriber)
}

// addToTopicMapsLocked adds a subscriber to the relevant topic maps, assuming lock is held
func (r *Registry) addToTopicMapsLocked(subscriber outbound.Subscriber) {
	subscriberID := subscriber.GetID()
	for _, topic := range subscriber.GetTopics() {
		if _, exists := r.topicSubscribers[topic]; !exists {
			r.topicSubscribers[topic] = make(map[string]outbound.Subscriber)
		}
		if _, subExists := r.topicSubscribers[topic][subscriberID]; !subExists {
			r.topicSubscribers[topic][subscriberID] = subscriber
			if r.logger != nil {
				r.logger.Debug(context.Background(), "Added subscriber to topic map", map[string]interface{}{
					"subscriber_id": subscriberID,
					"topic":         topic,
				})
			}
		}
	}
}

// removeFromTopicMaps removes a subscriber from all topic maps
func (r *Registry) removeFromTopicMaps(subscriber outbound.Subscriber) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.removeFromTopicMapsLocked(subscriber)
}

// removeFromTopicMapsLocked removes a subscriber from all topic maps, assuming lock is held
func (r *Registry) removeFromTopicMapsLocked(subscriber outbound.Subscriber) {
	subscriberID := subscriber.GetID()
	for _, topic := range subscriber.GetTopics() {
		if topicMap, exists := r.topicSubscribers[topic]; exists {
			if _, subExists := topicMap[subscriberID]; subExists {
				delete(topicMap, subscriberID)
				if r.logger != nil {
					r.logger.Debug(context.Background(), "Removed subscriber from topic map", map[string]interface{}{
						"subscriber_id": subscriberID,
						"topic":         topic,
					})
				}
				// If the topic map is now empty, remove it
				if len(topicMap) == 0 {
					delete(r.topicSubscribers, topic)
				}
			}
		}
	}
}

// generateSubscriberID creates a new unique ID for a subscriber
func generateSubscriberID() string {
	return uuid.New().String()
}
