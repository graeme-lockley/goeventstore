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
	subscriber *models.Subscriber
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

			// Log delivery attempt
			if wp.logger != nil {
				wp.logger.Debug(task.ctx, "Attempting event delivery", map[string]interface{}{
					"subscriber_id": task.subscriber.ID,
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
				// Create delivery timeout context
				deliveryCtx, cancel := context.WithTimeout(task.ctx, task.subscriber.Timeout.CurrentTimeout)
				deliveryErr = task.subscriber.ReceiveEvent(deliveryCtx, task.event)
				cancel() // Always cancel the context to avoid leaks
			}

			duration := time.Since(startTime)
			success := deliveryErr == nil

			// Log delivery outcome
			if wp.logger != nil {
				logFields := map[string]interface{}{}
				if deliveryErr != nil {
					logFields["error"] = deliveryErr.Error()
				}
				wp.logger.LogEventDelivery(task.ctx, task.subscriber.ID, task.event.ID, success, duration, logFields)
			}

			// Report result if channel provided
			if task.resultChan != nil {
				task.resultChan <- deliveryErr
			}
		}
	}
}

// submit adds a task to the worker pool
func (wp *workerPool) submit(ctx context.Context, subscriber *models.Subscriber, event outbound.Event, resultChan chan<- error) bool {
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
	subscribers map[string]*models.Subscriber

	// Subscribers by topic, for efficient event delivery
	topicSubscribers map[string]map[string]*models.Subscriber

	// Worker pool for event delivery
	workerPool *workerPool

	// Logger for registry operations
	logger *SubscriberLogger

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
		subscribers:      make(map[string]*models.Subscriber),
		topicSubscribers: make(map[string]map[string]*models.Subscriber),
		logger:           NewSubscriberLogger(INFO),
	}
	r.workerPool = newWorkerPool(poolConfig, r.logger)
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
func (r *Registry) Register(config models.SubscriberConfig) (*models.Subscriber, error) {
	return r.RegisterWithClientInfo(context.Background(), config, DefaultRegisterOptions())
}

// RegisterWithClientInfo adds a new subscriber to the registry with client information for logging
func (r *Registry) RegisterWithClientInfo(ctx context.Context, config models.SubscriberConfig, options RegisterOptions) (*models.Subscriber, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Create context with request ID if provided
	if options.RequestID != "" {
		ctx = context.WithValue(ctx, "request_id", options.RequestID)
	}

	// Generate an ID if one isn't provided
	if config.ID == "" {
		config.ID = uuid.New().String()
	} else if _, exists := r.subscribers[config.ID]; exists {
		if r.logger != nil {
			r.logger.Error(ctx, "Failed to register subscriber: ID already exists",
				map[string]interface{}{"subscriber_id": config.ID})
		}
		return nil, ErrSubscriberAlreadyExists
	}

	// Create the subscriber
	subscriber := models.NewSubscriber(config)

	// Add to the main subscribers map
	r.subscribers[subscriber.ID] = subscriber

	// Add to topic-based maps
	r.addToTopicMaps(subscriber)

	if r.logger != nil {
		// Create detailed log fields with all relevant information
		logFields := map[string]interface{}{
			"topics":        subscriber.Topics,
			"filter":        subscriber.Filter,
			"buffer_size":   subscriber.BufferSize,
			"timeout":       subscriber.Timeout,
			"creation_time": subscriber.CreatedAt,
		}

		// Prepare client information for logging
		clientInfo := map[string]interface{}{}
		if options.ClientIP != "" {
			clientInfo["ip"] = options.ClientIP
		}
		if options.UserAgent != "" {
			clientInfo["user_agent"] = options.UserAgent
		}
		if options.ClientID != "" {
			clientInfo["id"] = options.ClientID
		}

		// Add expiration if set
		if !options.Expiration.IsZero() {
			logFields["expiration"] = options.Expiration.Format(time.RFC3339)
			logFields["ttl"] = options.Expiration.Sub(time.Now()).String()
		}

		// Use the specialized registration logger
		r.logger.LogRegistration(ctx, subscriber.ID, clientInfo, logFields)

		// Log detailed information at DEBUG level
		if len(subscriber.Topics) > 0 {
			r.logger.Debug(ctx, fmt.Sprintf("Subscriber registered for %d topics", len(subscriber.Topics)),
				map[string]interface{}{
					"subscriber_id": subscriber.ID,
					"topics":        subscriber.Topics,
				})
		}

		if len(subscriber.Filter.EventTypes) > 0 {
			r.logger.Debug(ctx, fmt.Sprintf("Subscriber registered with %d event type filters", len(subscriber.Filter.EventTypes)),
				map[string]interface{}{
					"subscriber_id": subscriber.ID,
					"event_types":   subscriber.Filter.EventTypes,
				})
		}

		if subscriber.Filter.FromVersion > 0 {
			r.logger.Debug(ctx, "Subscriber registered with version filter",
				map[string]interface{}{
					"subscriber_id": subscriber.ID,
					"from_version":  subscriber.Filter.FromVersion,
				})
		}
	}

	return subscriber, nil
}

// Get retrieves a subscriber by ID
func (r *Registry) Get(id string) (*models.Subscriber, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	subscriber, exists := r.subscribers[id]
	if !exists {
		if r.logger != nil {
			r.logger.Warn(context.Background(), "Subscriber not found",
				map[string]interface{}{"subscriber_id": id})
		}
		return nil, ErrSubscriberNotFound
	}

	return subscriber, nil
}

// List returns all subscribers for a specific topic, or all subscribers if topic is empty
func (r *Registry) List(topic string) []*models.Subscriber {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var subscribers []*models.Subscriber

	if topic == "" {
		// Return all subscribers
		subscribers = make([]*models.Subscriber, 0, len(r.subscribers))
		for _, subscriber := range r.subscribers {
			subscribers = append(subscribers, subscriber)
		}
	} else {
		// Return subscribers for the given topic
		topicMap, exists := r.topicSubscribers[topic]
		if !exists {
			return []*models.Subscriber{}
		}

		subscribers = make([]*models.Subscriber, 0, len(topicMap))
		for _, subscriber := range topicMap {
			subscribers = append(subscribers, subscriber)
		}
	}

	return subscribers
}

// ListByState returns all subscribers in a specific state
func (r *Registry) ListByState(state models.SubscriberState) []*models.Subscriber {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var subscribers []*models.Subscriber

	for _, subscriber := range r.subscribers {
		if subscriber.State == state {
			subscribers = append(subscribers, subscriber)
		}
	}

	return subscribers
}

// Update modifies a subscriber by applying the provided function
func (r *Registry) Update(id string, updateFn func(*models.Subscriber) error) error {
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

	// Apply the update function
	if err := updateFn(subscriber); err != nil {
		if r.logger != nil {
			r.logger.Error(context.Background(), "Error updating subscriber",
				map[string]interface{}{
					"subscriber_id": id,
					"error":         err.Error(),
				})
		}
		return err
	}

	// Update LastActivityAt timestamp
	subscriber.LastActivityAt = time.Now()

	if r.logger != nil {
		r.logger.Info(context.Background(), "Subscriber updated",
			map[string]interface{}{"subscriber_id": id})
	}

	return nil
}

// UpdateTopics updates the topics a subscriber is interested in
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

	oldTopics := subscriber.Topics

	// Remove from old topic maps
	r.removeFromTopicMaps(subscriber)

	// Update topics
	subscriber.Topics = newTopics

	// Add to new topic maps
	r.addToTopicMaps(subscriber)

	// Update LastActivityAt timestamp
	subscriber.LastActivityAt = time.Now()

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

// _deregisterLocked performs the core deregistration logic assuming the write lock is already held.
func (r *Registry) _deregisterLocked(ctx context.Context, subscriber *models.Subscriber, reason string, details map[string]interface{}) {
	// Get stats before removal for logging
	stats := subscriber.GetStats()
	duration := time.Since(subscriber.CreatedAt)

	// Remove from topic maps
	r.removeFromTopicMaps(subscriber)

	// Remove from subscribers map
	delete(r.subscribers, subscriber.ID)

	// Close the subscriber
	subscriber.Close()

	if r.logger != nil {
		logFields := map[string]interface{}{
			"reason":           reason,
			"lifetime":         duration.String(),
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
		r.logger.LogLifecycleEvent(ctx, subscriber.ID, "deregistered", logFields)
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
	subscribers := make([]*models.Subscriber, 0, len(topicMap))
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
		if subscriber.State == models.SubscriberStateActive {
			// Check if the event should be delivered based on the subscriber's filter criteria
			if ShouldDeliverEvent(subscriber.GetFilter(), event) {
				r.workerPool.submit(ctx, subscriber, event, resultChan)
				deliveryCount++
			} else if r.logger != nil {
				r.logger.Debug(ctx, "Event filtered out for subscriber",
					map[string]interface{}{
						"event_id":      event.ID,
						"subscriber_id": subscriber.ID,
						"event_type":    event.Type,
						"filter":        subscriber.Filter,
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
		r.logger.Info(ctx, "Event broadcast completed",
			map[string]interface{}{
				"event_id":          event.ID,
				"topic":             event.Topic,
				"subscriber_count":  len(subscribers),
				"filtered_count":    deliveryCount,
				"success_count":     successCount,
				"duration":          duration.String(),
				"delivery_rate_pct": float64(successCount) / float64(deliveryCount) * 100,
			})
	}

	return successCount
}

// CleanupInactive removes subscribers that have been inactive longer than the provided duration
func (r *Registry) CleanupInactive(inactiveDuration time.Duration) int {
	ctx := context.Background() // Use a background context for cleanup operations
	r.mu.Lock()
	defer r.mu.Unlock()

	cutoffTime := time.Now().Add(-inactiveDuration)
	removedCount := 0

	// Find inactive subscribers
	var toRemove []string
	for id, subscriber := range r.subscribers {
		if subscriber.LastActivityAt.Before(cutoffTime) {
			toRemove = append(toRemove, id)
		}
	}

	// Hold the lock while removing to prevent concurrent modifications
	for _, id := range toRemove {
		subscriber, exists := r.subscribers[id]
		if !exists {
			// Should not happen if lock is held correctly, but safety first
			continue
		}

		// Prepare details for logging
		details := map[string]interface{}{
			"inactive_duration": inactiveDuration.String(),
			"last_activity_at":  subscriber.LastActivityAt.Format(time.RFC3339),
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

	topicMap, exists := r.topicSubscribers[topic]
	if !exists {
		return 0
	}

	return len(topicMap)
}

// Shutdown gracefully shuts down the registry and its worker pool
func (r *Registry) Shutdown(timeout time.Duration) {
	// Shutdown the worker pool
	r.workerPool.shutdown(timeout)
}

// addToTopicMaps adds a subscriber to the topic maps
func (r *Registry) addToTopicMaps(subscriber *models.Subscriber) {
	for _, topic := range subscriber.Topics {
		if _, exists := r.topicSubscribers[topic]; !exists {
			r.topicSubscribers[topic] = make(map[string]*models.Subscriber)
		}
		r.topicSubscribers[topic][subscriber.ID] = subscriber

		if r.logger != nil {
			r.logger.Debug(context.Background(), "Added subscriber to topic map",
				map[string]interface{}{
					"subscriber_id": subscriber.ID,
					"topic":         topic,
				})
		}
	}
}

// removeFromTopicMaps removes a subscriber from the topic maps
func (r *Registry) removeFromTopicMaps(subscriber *models.Subscriber) {
	for _, topic := range subscriber.Topics {
		if topicMap, exists := r.topicSubscribers[topic]; exists {
			delete(topicMap, subscriber.ID)

			// If the topic map is now empty, remove it
			if len(topicMap) == 0 {
				delete(r.topicSubscribers, topic)
			}

			if r.logger != nil {
				r.logger.Debug(context.Background(), "Removed subscriber from topic map",
					map[string]interface{}{
						"subscriber_id": subscriber.ID,
						"topic":         topic,
					})
			}
		}
	}
}

// generateSubscriberID creates a unique subscriber ID
func generateSubscriberID() string {
	return fmt.Sprintf("sub_%s", uuid.New().String())
}
