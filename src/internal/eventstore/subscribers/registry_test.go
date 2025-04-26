package subscribers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	"goeventsource/src/internal/eventstore/models"
	"goeventsource/src/internal/port/outbound"
)

func TestNewRegistry(t *testing.T) {
	registry := NewRegistry()

	if registry == nil {
		t.Fatal("Expected NewRegistry to return a non-nil registry")
	}

	if registry.subscribers == nil {
		t.Error("Expected subscribers map to be initialized")
	}

	if registry.topicSubscribers == nil {
		t.Error("Expected topicSubscribers map to be initialized")
	}

	if registry.Count() != 0 {
		t.Errorf("Expected new registry to have 0 subscribers, got %d", registry.Count())
	}
}

func TestRegister(t *testing.T) {
	registry := NewRegistry()

	// Test with explicit ID
	config := models.SubscriberConfig{
		ID:      "test-subscriber-1",
		Topics:  []string{"topic1", "topic2"},
		Filter:  models.SubscriberFilter{},
		Timeout: models.DefaultTimeoutConfig(),
	}

	subscriber, err := registry.Register(config)
	if err != nil {
		t.Fatalf("Unexpected error registering subscriber: %v", err)
	}

	if subscriber.ID != "test-subscriber-1" {
		t.Errorf("Expected subscriber ID to be 'test-subscriber-1', got %s", subscriber.ID)
	}

	// Test with generated ID
	config.ID = ""
	subscriber2, err := registry.Register(config)
	if err != nil {
		t.Fatalf("Unexpected error registering subscriber: %v", err)
	}

	if subscriber2.ID == "" {
		t.Error("Expected generated subscriber ID to be non-empty")
	}

	if subscriber2.ID == subscriber.ID {
		t.Errorf("Expected unique subscriber IDs, got duplicate: %s", subscriber.ID)
	}

	// Test duplicate ID
	duplicateConfig := models.SubscriberConfig{
		ID:      "test-subscriber-1", // Same as first subscriber
		Topics:  []string{"topic3"},
		Filter:  models.SubscriberFilter{},
		Timeout: models.DefaultTimeoutConfig(),
	}

	_, err = registry.Register(duplicateConfig)
	if err != ErrSubscriberAlreadyExists {
		t.Errorf("Expected ErrSubscriberAlreadyExists, got %v", err)
	}

	// Verify topic maps
	if registry.CountByTopic("topic1") != 2 { // Both subscribers are interested in topic1
		t.Errorf("Expected 2 subscribers for topic1, got %d", registry.CountByTopic("topic1"))
	}

	if registry.CountByTopic("topic2") != 2 { // Both subscribers are interested in topic2
		t.Errorf("Expected 2 subscribers for topic2, got %d", registry.CountByTopic("topic2"))
	}
}

func TestGet(t *testing.T) {
	registry := NewRegistry()

	// Register a subscriber
	config := models.SubscriberConfig{
		ID:      "test-subscriber-get",
		Topics:  []string{"topic1"},
		Filter:  models.SubscriberFilter{},
		Timeout: models.DefaultTimeoutConfig(),
	}

	original, _ := registry.Register(config)

	// Test successful get
	subscriber, err := registry.Get("test-subscriber-get")
	if err != nil {
		t.Fatalf("Unexpected error getting subscriber: %v", err)
	}

	if subscriber != original {
		t.Error("Expected Get to return the original subscriber")
	}

	// Test non-existent subscriber
	_, err = registry.Get("non-existent-subscriber")
	if err != ErrSubscriberNotFound {
		t.Errorf("Expected ErrSubscriberNotFound, got %v", err)
	}
}

func TestDeregister(t *testing.T) {
	// Create a buffer to capture log output
	var buf bytes.Buffer
	customLogger := log.New(&buf, "", 0)
	logger := NewSubscriberLogger(INFO)
	logger.logger = customLogger

	registry := NewRegistry()
	registry.SetLogger(logger)

	// Register a subscriber
	config := models.SubscriberConfig{
		ID:      "test-subscriber-deregister",
		Topics:  []string{"topic1", "topic2"},
		Filter:  models.SubscriberFilter{},
		Timeout: models.DefaultTimeoutConfig(),
	}

	_, _ = registry.Register(config)

	// Test successful deregistration
	err := registry.Deregister("test-subscriber-deregister")
	if err != nil {
		t.Fatalf("Unexpected error deregistering subscriber: %v", err)
	}

	// Verify subscriber is removed
	_, err = registry.Get("test-subscriber-deregister")
	if err != ErrSubscriberNotFound {
		t.Errorf("Expected subscriber to be removed, but Get returned: %v", err)
	}

	// Verify topic maps are updated
	if registry.CountByTopic("topic1") != 0 {
		t.Errorf("Expected 0 subscribers for topic1 after deregistration, got %d", registry.CountByTopic("topic1"))
	}

	if registry.CountByTopic("topic2") != 0 {
		t.Errorf("Expected 0 subscribers for topic2 after deregistration, got %d", registry.CountByTopic("topic2"))
	}

	// Verify log output contains the reason
	output := buf.String()
	logLines := strings.Split(strings.TrimSpace(output), "\n")
	foundLog := false
	for _, line := range logLines {
		var logEntry map[string]interface{}
		if err := json.Unmarshal([]byte(line), &logEntry); err == nil {
			// Check if it's the deregistered event for the correct subscriber
			if lifecycleEvent, ok := logEntry["lifecycle_event"].(string); ok && lifecycleEvent == "deregistered" {
				if sid, ok := logEntry["subscriber_id"].(string); ok && sid == "test-subscriber-deregister" {
					if logEntry["reason"] != "manual" {
						t.Errorf("Expected log reason to be 'manual', got: %v", logEntry["reason"])
					}
					foundLog = true
					break // Found the log we care about
				}
			}
		}
	}

	if !foundLog {
		t.Errorf("Could not find deregistration log entry for test-subscriber-deregister in output: %s", output)
	}

	// Test deregistering non-existent subscriber
	buf.Reset() // Clear buffer before testing non-existent case
	err = registry.Deregister("non-existent-subscriber")
	if err != ErrSubscriberNotFound {
		t.Errorf("Expected ErrSubscriberNotFound, got %v", err)
	}
	// Optionally, verify the warning log for non-existent subscriber was generated
}

func TestList(t *testing.T) {
	registry := NewRegistry()

	// Register subscribers with different topics
	configs := []models.SubscriberConfig{
		{
			ID:      "subscriber1",
			Topics:  []string{"topicA", "topicB"},
			Filter:  models.SubscriberFilter{},
			Timeout: models.DefaultTimeoutConfig(),
		},
		{
			ID:      "subscriber2",
			Topics:  []string{"topicA", "topicC"},
			Filter:  models.SubscriberFilter{},
			Timeout: models.DefaultTimeoutConfig(),
		},
		{
			ID:      "subscriber3",
			Topics:  []string{"topicB", "topicC"},
			Filter:  models.SubscriberFilter{},
			Timeout: models.DefaultTimeoutConfig(),
		},
	}

	for _, config := range configs {
		_, _ = registry.Register(config)
	}

	// Test listing all subscribers
	allSubscribers := registry.List("")
	if len(allSubscribers) != 3 {
		t.Errorf("Expected 3 subscribers, got %d", len(allSubscribers))
	}

	// Test listing by topic
	topicASubscribers := registry.List("topicA")
	if len(topicASubscribers) != 2 {
		t.Errorf("Expected 2 subscribers for topicA, got %d", len(topicASubscribers))
	}

	topicBSubscribers := registry.List("topicB")
	if len(topicBSubscribers) != 2 {
		t.Errorf("Expected 2 subscribers for topicB, got %d", len(topicBSubscribers))
	}

	topicCSubscribers := registry.List("topicC")
	if len(topicCSubscribers) != 2 {
		t.Errorf("Expected 2 subscribers for topicC, got %d", len(topicCSubscribers))
	}

	// Test listing for non-existent topic
	nonExistentSubscribers := registry.List("non-existent-topic")
	if len(nonExistentSubscribers) != 0 {
		t.Errorf("Expected 0 subscribers for non-existent topic, got %d", len(nonExistentSubscribers))
	}
}

func TestListByState(t *testing.T) {
	registry := NewRegistry()

	// Register subscribers
	configs := []models.SubscriberConfig{
		{
			ID:      "active1",
			Topics:  []string{"topic1"},
			Filter:  models.SubscriberFilter{},
			Timeout: models.DefaultTimeoutConfig(),
		},
		{
			ID:      "active2",
			Topics:  []string{"topic2"},
			Filter:  models.SubscriberFilter{},
			Timeout: models.DefaultTimeoutConfig(),
		},
		{
			ID:      "paused1",
			Topics:  []string{"topic3"},
			Filter:  models.SubscriberFilter{},
			Timeout: models.DefaultTimeoutConfig(),
		},
	}

	for _, config := range configs {
		_, _ = registry.Register(config)
	}

	// Pause one subscriber
	subscriber, _ := registry.Get("paused1")
	subscriber.Pause()

	// Test listing by state
	activeSubscribers := registry.ListByState(models.SubscriberStateActive)
	if len(activeSubscribers) != 2 {
		t.Errorf("Expected 2 active subscribers, got %d", len(activeSubscribers))
	}

	pausedSubscribers := registry.ListByState(models.SubscriberStatePaused)
	if len(pausedSubscribers) != 1 {
		t.Errorf("Expected 1 paused subscriber, got %d", len(pausedSubscribers))
	}

	closedSubscribers := registry.ListByState(models.SubscriberStateClosed)
	if len(closedSubscribers) != 0 {
		t.Errorf("Expected 0 closed subscribers, got %d", len(closedSubscribers))
	}
}

func TestUpdate(t *testing.T) {
	registry := NewRegistry()

	// Register a subscriber
	config := models.SubscriberConfig{
		ID:     "test-subscriber-update",
		Topics: []string{"topic1"},
		Filter: models.SubscriberFilter{
			EventTypes: []string{"typeA"},
		},
		Timeout: models.DefaultTimeoutConfig(),
	}

	_, _ = registry.Register(config)

	// Test updating subscriber
	err := registry.Update("test-subscriber-update", func(s *models.Subscriber) error {
		s.Filter.EventTypes = []string{"typeA", "typeB"}
		return nil
	})

	if err != nil {
		t.Fatalf("Unexpected error updating subscriber: %v", err)
	}

	// Verify update
	subscriber, _ := registry.Get("test-subscriber-update")
	if len(subscriber.Filter.EventTypes) != 2 {
		t.Errorf("Expected Filter.EventTypes to be updated to 2 types, got %d", len(subscriber.Filter.EventTypes))
	}

	// Test updating non-existent subscriber
	err = registry.Update("non-existent-subscriber", func(s *models.Subscriber) error {
		return nil
	})

	if err != ErrSubscriberNotFound {
		t.Errorf("Expected ErrSubscriberNotFound, got %v", err)
	}

	// Test update function returning error
	testErr := errors.New("test error")
	err = registry.Update("test-subscriber-update", func(s *models.Subscriber) error {
		return testErr
	})

	if err != testErr {
		t.Errorf("Expected update function error to be returned, got %v", err)
	}
}

func TestUpdateTopics(t *testing.T) {
	registry := NewRegistry()

	// Register a subscriber
	config := models.SubscriberConfig{
		ID:      "test-subscriber-update-topics",
		Topics:  []string{"topic1", "topic2"},
		Filter:  models.SubscriberFilter{},
		Timeout: models.DefaultTimeoutConfig(),
	}

	_, _ = registry.Register(config)

	// Test updating topics
	newTopics := []string{"topic3", "topic4"}
	err := registry.UpdateTopics("test-subscriber-update-topics", newTopics)

	if err != nil {
		t.Fatalf("Unexpected error updating topics: %v", err)
	}

	// Verify topic maps are updated
	if registry.CountByTopic("topic1") != 0 {
		t.Errorf("Expected 0 subscribers for topic1 after update, got %d", registry.CountByTopic("topic1"))
	}

	if registry.CountByTopic("topic2") != 0 {
		t.Errorf("Expected 0 subscribers for topic2 after update, got %d", registry.CountByTopic("topic2"))
	}

	if registry.CountByTopic("topic3") != 1 {
		t.Errorf("Expected 1 subscriber for topic3 after update, got %d", registry.CountByTopic("topic3"))
	}

	if registry.CountByTopic("topic4") != 1 {
		t.Errorf("Expected 1 subscriber for topic4 after update, got %d", registry.CountByTopic("topic4"))
	}

	// Verify subscriber's topics are updated
	subscriber, _ := registry.Get("test-subscriber-update-topics")
	if len(subscriber.Topics) != 2 {
		t.Errorf("Expected subscriber to have 2 topics, got %d", len(subscriber.Topics))
	}

	if subscriber.Topics[0] != "topic3" || subscriber.Topics[1] != "topic4" {
		t.Errorf("Expected subscriber topics to be [topic3, topic4], got %v", subscriber.Topics)
	}

	// Test updating topics for non-existent subscriber
	err = registry.UpdateTopics("non-existent-subscriber", newTopics)
	if err != ErrSubscriberNotFound {
		t.Errorf("Expected ErrSubscriberNotFound, got %v", err)
	}
}

func TestCleanupInactive(t *testing.T) {
	// Create a buffer to capture log output
	var buf bytes.Buffer
	customLogger := log.New(&buf, "", 0)
	logger := NewSubscriberLogger(INFO)
	logger.logger = customLogger

	registry := NewRegistry()
	registry.SetLogger(logger)

	// Register subscribers
	configs := []models.SubscriberConfig{
		{
			ID:      "active-subscriber",
			Topics:  []string{"topic1"},
			Filter:  models.SubscriberFilter{},
			Timeout: models.DefaultTimeoutConfig(),
		},
		{
			ID:      "inactive-subscriber",
			Topics:  []string{"topic2"},
			Filter:  models.SubscriberFilter{},
			Timeout: models.DefaultTimeoutConfig(),
		},
	}

	for _, config := range configs {
		_, _ = registry.Register(config)
	}

	// Make one subscriber inactive by manually setting LastActivityAt
	subscriber, _ := registry.Get("inactive-subscriber")
	subscriber.LastActivityAt = time.Now().Add(-10 * time.Minute)

	// Cleanup inactive subscribers (inactive for > 5 minutes)
	inactiveDuration := 5 * time.Minute
	removed := registry.CleanupInactive(inactiveDuration)

	if removed != 1 {
		t.Errorf("Expected 1 subscriber to be removed, got %d", removed)
	}

	// Verify inactive subscriber is removed
	if registry.Count() != 1 {
		t.Errorf("Expected 1 subscriber to remain, got %d", registry.Count())
	}

	_, err := registry.Get("inactive-subscriber")
	if err != ErrSubscriberNotFound {
		t.Errorf("Expected inactive subscriber to be removed, but Get returned: %v", err)
	}

	// Verify active subscriber remains
	_, err = registry.Get("active-subscriber")
	if err != nil {
		t.Errorf("Expected active subscriber to remain, got error: %v", err)
	}

	// Verify log output contains the reason
	output := buf.String()

	// We need to find the correct log line for the inactive subscriber
	logLines := strings.Split(strings.TrimSpace(output), "\n")
	foundLog := false
	for _, line := range logLines {
		var logEntry map[string]interface{}
		if err := json.Unmarshal([]byte(line), &logEntry); err == nil {
			// Check if it's the deregistered event for the correct subscriber
			if lifecycleEvent, ok := logEntry["lifecycle_event"].(string); ok && lifecycleEvent == "deregistered" {
				if sid, ok := logEntry["subscriber_id"].(string); ok && sid == "inactive-subscriber" {
					if logEntry["reason"] != "inactive_timeout" {
						t.Errorf("Expected log reason to be 'inactive_timeout', got: %v", logEntry["reason"])
					}
					// Check details embedded within the main log entry
					if detailsDuration, ok := logEntry["inactive_duration"].(string); !ok || detailsDuration != inactiveDuration.String() {
						t.Errorf("Expected inactive_duration '%s', got '%v'", inactiveDuration.String(), logEntry["inactive_duration"])
					}
					if _, ok := logEntry["last_activity_at"].(string); !ok {
						t.Errorf("Expected last_activity_at string in log entry")
					}

					// Check standard deregistration fields are also present
					if _, ok := logEntry["lifetime"].(string); !ok {
						t.Errorf("Expected lifetime string in log entry")
					}

					foundLog = true
					break
				}
			}
		}
	}

	if !foundLog {
		t.Errorf("Could not find deregistration log entry for inactive-subscriber in output: %s", output)
	}
}

func TestBroadcastEvent(t *testing.T) {
	registry := NewRegistry()

	// Helper function to create a subscriber with a channel we can read from
	createSubscriber := func(id string, topics []string, eventTypes []string, fromVersion int64) (*models.Subscriber, chan outbound.Event) {
		config := models.SubscriberConfig{
			ID:     id,
			Topics: topics,
			Filter: models.SubscriberFilter{
				EventTypes:  eventTypes,
				FromVersion: fromVersion,
			},
			Timeout:    models.DefaultTimeoutConfig(),
			BufferSize: 10,
		}

		subscriber, _ := registry.Register(config)
		eventChannel := subscriber.EventChannel

		// Create a goroutine to read from the subscriber's channel to prevent blocking
		readChannel := make(chan outbound.Event, 10)
		go func() {
			for event := range eventChannel {
				readChannel <- event
			}
			close(readChannel)
		}()

		return subscriber, readChannel
	}

	// Create subscribers with different configurations
	_, allEventsChannel := createSubscriber("all-events", []string{"test-topic"}, nil, 0)
	_, typeFilterChannel := createSubscriber("type-filter", []string{"test-topic"}, []string{"TestEvent"}, 0)
	_, versionFilterChannel := createSubscriber("version-filter", []string{"test-topic"}, nil, 5)
	pausedSub, _ := createSubscriber("paused", []string{"test-topic"}, nil, 0)
	pausedSub.Pause()

	// Create test event
	event := outbound.Event{
		ID:        "test-event",
		Topic:     "test-topic",
		Type:      "TestEvent",
		Data:      map[string]interface{}{"key": "value"},
		Metadata:  map[string]interface{}{},
		Timestamp: time.Now().UnixNano(),
		Version:   10,
	}

	// Broadcast event
	successCount := registry.BroadcastEvent(context.Background(), event)

	// We expect delivery to 3 active subscribers that match the event
	expectedSuccessCount := 3
	if successCount != expectedSuccessCount {
		t.Errorf("Expected %d successful deliveries, got %d", expectedSuccessCount, successCount)
	}

	// Verify event delivery to matching subscribers
	select {
	case receivedEvent := <-allEventsChannel:
		if receivedEvent.ID != event.ID {
			t.Errorf("Expected event ID %s, got %s", event.ID, receivedEvent.ID)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Timed out waiting for event in all-events subscriber")
	}

	select {
	case receivedEvent := <-typeFilterChannel:
		if receivedEvent.ID != event.ID {
			t.Errorf("Expected event ID %s, got %s", event.ID, receivedEvent.ID)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Timed out waiting for event in type-filter subscriber")
	}

	select {
	case receivedEvent := <-versionFilterChannel:
		if receivedEvent.ID != event.ID {
			t.Errorf("Expected event ID %s, got %s", event.ID, receivedEvent.ID)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Timed out waiting for event in version-filter subscriber")
	}

	// Create a non-matching event (wrong type)
	nonMatchingEvent := outbound.Event{
		ID:        "non-matching-event",
		Topic:     "test-topic",
		Type:      "OtherEvent", // Not matching typeFilterChannel
		Data:      map[string]interface{}{},
		Metadata:  map[string]interface{}{},
		Timestamp: time.Now().UnixNano(),
		Version:   10,
	}

	// Broadcast non-matching event
	successCount = registry.BroadcastEvent(context.Background(), nonMatchingEvent)

	// We expect delivery to 2 subscribers (all-events and version-filter)
	expectedSuccessCount = 2
	if successCount != expectedSuccessCount {
		t.Errorf("Expected %d successful deliveries for non-matching event, got %d", expectedSuccessCount, successCount)
	}
}

func TestConcurrentOperations(t *testing.T) {
	registry := NewRegistry()

	// Test concurrent registrations
	var wg sync.WaitGroup
	numGoroutines := 10
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()

			config := models.SubscriberConfig{
				Topics:  []string{"topic1", "topic2"},
				Filter:  models.SubscriberFilter{},
				Timeout: models.DefaultTimeoutConfig(),
			}

			_, err := registry.Register(config)
			if err != nil {
				t.Errorf("Unexpected error in concurrent registration: %v", err)
			}
		}(i)
	}

	wg.Wait()

	// Verify all subscribers were registered
	if registry.Count() != numGoroutines {
		t.Errorf("Expected %d subscribers after concurrent registration, got %d", numGoroutines, registry.Count())
	}

	// Test concurrent operations (get, list, update, deregister)
	subscribers := registry.List("")
	wg.Add(len(subscribers) * 3) // 3 operations per subscriber

	for _, subscriber := range subscribers {
		// Goroutine for Get
		go func(id string) {
			defer wg.Done()
			_, err := registry.Get(id)
			if err != nil {
				t.Errorf("Unexpected error in concurrent Get: %v", err)
			}
		}(subscriber.ID)

		// Goroutine for Update
		go func(id string) {
			defer wg.Done()
			err := registry.Update(id, func(s *models.Subscriber) error {
				s.Filter.FromVersion = 5
				return nil
			})
			if err != nil {
				t.Errorf("Unexpected error in concurrent Update: %v", err)
			}
		}(subscriber.ID)

		// Goroutine for List (shared operation)
		go func() {
			defer wg.Done()
			_ = registry.List("topic1")
		}()
	}

	wg.Wait()

	// Test concurrent deregistration
	subscribers = registry.List("")
	wg.Add(len(subscribers))

	for _, subscriber := range subscribers {
		go func(id string) {
			defer wg.Done()
			_ = registry.Deregister(id)
		}(subscriber.ID)
	}

	wg.Wait()

	// Verify all subscribers were deregistered
	if registry.Count() != 0 {
		t.Errorf("Expected 0 subscribers after concurrent deregistration, got %d", registry.Count())
	}
}

func TestEventFiltering(t *testing.T) {
	// Create subscriber with filters
	subFilter := outbound.SubscriberFilter{
		EventTypes:  []string{"TypeA", "TypeB"},
		FromVersion: 5,
		Metadata: map[string]interface{}{
			"source": "test-source",
		},
	}

	// Test matching event
	matchingEvent := outbound.Event{
		Topic:   "topic1",
		Type:    "TypeA",
		Version: 10,
		Metadata: map[string]interface{}{
			"source": "test-source",
			"other":  "value",
		},
	}

	if !ShouldDeliverEvent(subFilter, matchingEvent) {
		t.Error("Expected ShouldDeliverEvent to return true for matching event")
	}

	// Test non-matching event type
	wrongTypeEvent := outbound.Event{
		Topic:   "topic1",
		Type:    "TypeC", // Not in EventTypes filter
		Version: 10,
		Metadata: map[string]interface{}{
			"source": "test-source",
		},
	}

	if ShouldDeliverEvent(subFilter, wrongTypeEvent) {
		t.Error("Expected ShouldDeliverEvent to return false for wrong type event")
	}

	// Test version too low
	lowVersionEvent := outbound.Event{
		Topic:   "topic1",
		Type:    "TypeA",
		Version: 3, // Lower than FromVersion filter
		Metadata: map[string]interface{}{
			"source": "test-source",
		},
	}

	if ShouldDeliverEvent(subFilter, lowVersionEvent) {
		t.Error("Expected ShouldDeliverEvent to return false for low version event")
	}

	// Test missing metadata
	missingMetadataEvent := outbound.Event{
		Topic:   "topic1",
		Type:    "TypeA",
		Version: 10,
		Metadata: map[string]interface{}{
			"other": "value", // Missing "source" field required in filter
		},
	}

	if ShouldDeliverEvent(subFilter, missingMetadataEvent) {
		t.Error("Expected ShouldDeliverEvent to return false for missing metadata event")
	}

	// Test mismatched metadata
	wrongMetadataEvent := outbound.Event{
		Topic:   "topic1",
		Type:    "TypeA",
		Version: 10,
		Metadata: map[string]interface{}{
			"source": "other-source", // Different value than filter
		},
	}

	if ShouldDeliverEvent(subFilter, wrongMetadataEvent) {
		t.Error("Expected ShouldDeliverEvent to return false for wrong metadata event")
	}

	// Test with empty filters
	emptyFilter := outbound.SubscriberFilter{} // Empty filter accepts all events

	if !ShouldDeliverEvent(emptyFilter, matchingEvent) {
		t.Error("Expected ShouldDeliverEvent to return true for subscriber with empty filter")
	}
}

func TestRegisterWithClientInfo(t *testing.T) {
	// Create a custom logger with a buffer to capture output
	var buf bytes.Buffer
	customLogger := log.New(&buf, "", 0)

	logger := NewSubscriberLogger(DEBUG)
	logger.logger = customLogger

	registry := NewRegistry()
	registry.SetLogger(logger)

	// Create a configuration
	config := models.SubscriberConfig{
		ID:     "client-info-test",
		Topics: []string{"test-topic"},
		Filter: models.SubscriberFilter{
			EventTypes: []string{"TestEvent"},
		},
		Timeout:    models.DefaultTimeoutConfig(),
		BufferSize: 100,
	}

	// Create client info options
	options := RegisterOptions{
		ClientIP:   "10.0.0.1",
		UserAgent:  "TestBrowser/2.0",
		ClientID:   "test-client-001",
		RequestID:  "req-12345",
		Expiration: time.Now().Add(2 * time.Hour),
	}

	// Register with client info
	subscriber, err := registry.RegisterWithClientInfo(context.Background(), config, options)

	// Verify registration succeeded
	if err != nil {
		t.Fatalf("Expected successful registration, got error: %v", err)
	}

	if subscriber.ID != "client-info-test" {
		t.Errorf("Expected ID 'client-info-test', got: %s", subscriber.ID)
	}

	// Capture the log output
	output := buf.String()

	// Verify client info was logged
	if !strings.Contains(output, "10.0.0.1") {
		t.Errorf("Expected log to contain client IP, log output: %s", output)
	}

	if !strings.Contains(output, "TestBrowser/2.0") {
		t.Errorf("Expected log to contain user agent, log output: %s", output)
	}

	if !strings.Contains(output, "test-client-001") {
		t.Errorf("Expected log to contain client ID, log output: %s", output)
	}

	// Verify request ID was included
	if !strings.Contains(output, "req-12345") {
		t.Errorf("Expected log to contain request ID, log output: %s", output)
	}

	// Verify expiration was logged
	if !strings.Contains(output, "expiration") {
		t.Errorf("Expected log to contain expiration info, log output: %s", output)
	}

	// Test with default options (no client info)
	buf.Reset()

	config.ID = "default-options-test"
	subscriber, err = registry.Register(config)

	if err != nil {
		t.Fatalf("Expected successful registration with default options, got error: %v", err)
	}

	// Verify default registration still works
	if subscriber.ID != "default-options-test" {
		t.Errorf("Expected ID 'default-options-test', got: %s", subscriber.ID)
	}

	// Verify subscriber was added to registry
	retrievedSub, err := registry.Get("default-options-test")
	if err != nil {
		t.Errorf("Failed to retrieve subscriber: %v", err)
	}

	if retrievedSub.ID != "default-options-test" {
		t.Errorf("Expected retrieved subscriber ID 'default-options-test', got: %s", retrievedSub.ID)
	}
}
