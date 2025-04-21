// Package subscribers contains the subscriber registry for the EventStore.
package subscribers

import (
	"context"
	"errors"
	"fmt"
	"reflect"
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
}

// newWorkerPool creates a new worker pool with the given configuration
func newWorkerPool(config WorkerPoolConfig) *workerPool {
	ctx, cancel := context.WithCancel(context.Background())
	pool := &workerPool{
		taskQueue:  make(chan deliveryTask, config.QueueSize),
		ctx:        ctx,
		cancelFunc: cancel,
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
			// Check if context is still valid
			select {
			case <-task.ctx.Done():
				// Context cancelled, report error
				if task.resultChan != nil {
					task.resultChan <- task.ctx.Err()
				}
			default:
				// Create delivery timeout context
				deliveryCtx, cancel := context.WithTimeout(task.ctx, task.subscriber.Timeout.CurrentTimeout)
				err := task.subscriber.ReceiveEvent(deliveryCtx, task.event)
				cancel() // Always cancel the context to avoid leaks

				// Report result if channel provided
				if task.resultChan != nil {
					task.resultChan <- err
				}
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

// Registry manages a thread-safe collection of subscribers
type Registry struct {
	mu               sync.RWMutex
	subscribers      map[string]*models.Subscriber
	topicSubscribers map[string]map[string]*models.Subscriber
	workerPool       *workerPool
}

// NewRegistry creates a new empty registry with default worker pool configuration
func NewRegistry() *Registry {
	return NewRegistryWithConfig(DefaultWorkerPoolConfig())
}

// NewRegistryWithConfig creates a new registry with custom worker pool configuration
func NewRegistryWithConfig(poolConfig WorkerPoolConfig) *Registry {
	return &Registry{
		subscribers:      make(map[string]*models.Subscriber),
		topicSubscribers: make(map[string]map[string]*models.Subscriber),
		workerPool:       newWorkerPool(poolConfig),
	}
}

// Register adds a new subscriber to the registry
func (r *Registry) Register(config models.SubscriberConfig) (*models.Subscriber, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Generate an ID if one isn't provided
	if config.ID == "" {
		config.ID = uuid.New().String()
	} else if _, exists := r.subscribers[config.ID]; exists {
		return nil, ErrSubscriberAlreadyExists
	}

	// Create the subscriber
	subscriber := models.NewSubscriber(config)

	// Add to the main subscribers map
	r.subscribers[subscriber.ID] = subscriber

	// Add to topic-based maps
	r.addToTopicMaps(subscriber)

	return subscriber, nil
}

// Get retrieves a subscriber by ID
func (r *Registry) Get(id string) (*models.Subscriber, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	subscriber, exists := r.subscribers[id]
	if !exists {
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
		return ErrSubscriberNotFound
	}

	// Apply the update function
	if err := updateFn(subscriber); err != nil {
		return err
	}

	// Update LastActivityAt timestamp
	subscriber.LastActivityAt = time.Now()

	return nil
}

// UpdateTopics updates the topics a subscriber is interested in
func (r *Registry) UpdateTopics(id string, newTopics []string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	subscriber, exists := r.subscribers[id]
	if !exists {
		return ErrSubscriberNotFound
	}

	// Remove from old topic maps
	r.removeFromTopicMaps(subscriber)

	// Update topics
	subscriber.Topics = newTopics

	// Add to new topic maps
	r.addToTopicMaps(subscriber)

	// Update LastActivityAt timestamp
	subscriber.LastActivityAt = time.Now()

	return nil
}

// Deregister removes a subscriber from the registry
func (r *Registry) Deregister(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	subscriber, exists := r.subscribers[id]
	if !exists {
		return ErrSubscriberNotFound
	}

	// Remove from topic maps
	r.removeFromTopicMaps(subscriber)

	// Remove from subscribers map
	delete(r.subscribers, id)

	// Close the subscriber
	subscriber.Close()

	return nil
}

// BroadcastEvent sends an event to all matching subscribers for the event's topic
// using the worker pool for concurrent, non-blocking delivery
func (r *Registry) BroadcastEvent(ctx context.Context, event outbound.Event) int {
	r.mu.RLock()
	topicSubs, exists := r.topicSubscribers[event.Topic]
	if !exists {
		r.mu.RUnlock()
		return 0
	}

	// Make a copy of the subscribers to avoid holding the lock during delivery
	subscribers := make([]*models.Subscriber, 0, len(topicSubs))
	for _, subscriber := range topicSubs {
		if subscriber.State == models.SubscriberStateActive {
			subscribers = append(subscribers, subscriber)
		}
	}
	r.mu.RUnlock()

	if len(subscribers) == 0 {
		return 0
	}

	// Use a simple channel to collect results
	resultChan := make(chan error, len(subscribers))
	submittedCount := 0

	// Submit delivery tasks to the worker pool
	for _, subscriber := range subscribers {
		if shouldDeliverEvent(subscriber, event) {
			select {
			case <-ctx.Done():
				// Context cancelled
				goto processResults
			default:
				// Submit task to worker pool - fire and forget
				if r.workerPool.submit(ctx, subscriber, event, resultChan) {
					submittedCount++
				}
			}
		}
	}

processResults:
	// For tests and synchronous behavior, wait for immediate results
	// This is a compromise - in production we'd let the worker pool handle things asynchronously
	// but for tests we need deterministic behavior
	successCount := 0
	for i := 0; i < submittedCount; i++ {
		select {
		case err := <-resultChan:
			if err == nil {
				successCount++
			}
		case <-ctx.Done():
			// Context cancelled - don't wait for more results
			return successCount
		}
	}

	return successCount
}

// CleanupInactive removes subscribers that have been inactive longer than the provided duration
func (r *Registry) CleanupInactive(inactiveDuration time.Duration) int {
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

	// Remove them
	for _, id := range toRemove {
		subscriber := r.subscribers[id]
		r.removeFromTopicMaps(subscriber)
		delete(r.subscribers, id)
		subscriber.Close()
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
	}
}

// removeFromTopicMaps removes a subscriber from the topic maps
func (r *Registry) removeFromTopicMaps(subscriber *models.Subscriber) {
	for _, topic := range subscriber.Topics {
		if topicMap, exists := r.topicSubscribers[topic]; exists {
			delete(topicMap, subscriber.ID)
			// If the topic map is empty, remove it
			if len(topicMap) == 0 {
				delete(r.topicSubscribers, topic)
			}
		}
	}
}

// shouldDeliverEvent determines if an event should be delivered to a subscriber
// based on the subscriber's filter criteria
func shouldDeliverEvent(subscriber *models.Subscriber, event outbound.Event) bool {
	// Skip if version is below the threshold
	if event.Version < subscriber.Filter.FromVersion {
		return false
	}

	// Check event type filter if specified
	if len(subscriber.Filter.EventTypes) > 0 {
		matches := false
		for _, allowedType := range subscriber.Filter.EventTypes {
			if allowedType == event.Type {
				matches = true
				break
			}
		}
		if !matches {
			return false
		}
	}

	// Check metadata filters if specified
	if len(subscriber.Filter.Metadata) > 0 {
		for key, filterValue := range subscriber.Filter.Metadata {
			eventValue, exists := event.Metadata[key]
			if !exists || !reflect.DeepEqual(eventValue, filterValue) {
				return false
			}
		}
	}

	return true
}

// generateSubscriberID creates a unique subscriber ID
func generateSubscriberID() string {
	return fmt.Sprintf("sub_%s", uuid.New().String())
}
