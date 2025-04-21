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

// Registry manages a thread-safe collection of subscribers
type Registry struct {
	mu               sync.RWMutex
	subscribers      map[string]*models.Subscriber
	topicSubscribers map[string]map[string]*models.Subscriber
}

// NewRegistry creates a new empty registry
func NewRegistry() *Registry {
	return &Registry{
		subscribers:      make(map[string]*models.Subscriber),
		topicSubscribers: make(map[string]map[string]*models.Subscriber),
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

	successCount := 0
	for _, subscriber := range subscribers {
		if shouldDeliverEvent(subscriber, event) {
			select {
			case <-ctx.Done():
				// Context cancelled
				return successCount
			default:
				// Create a timeout context for this delivery based on subscriber's timeout settings
				deliveryCtx, cancel := context.WithTimeout(ctx, subscriber.Timeout.CurrentTimeout)
				err := subscriber.ReceiveEvent(deliveryCtx, event)
				cancel() // Always cancel the context to avoid leaks

				if err == nil {
					successCount++
				}
			}
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
