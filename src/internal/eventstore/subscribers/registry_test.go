package subscribers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	config := models.SubscriberConfig{
		ID:     "test-subscriber-1",
		Topics: []string{"topic1", "topic2"},
	}

	subscriber, err := registry.Register(config)
	assert.NoError(t, err)
	assert.NotNil(t, subscriber)
	assert.Equal(t, "test-subscriber-1", subscriber.GetID())

	// Try registering again with same ID
	_, err = registry.Register(config)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrSubscriberAlreadyExists))

	// Register without ID
	configNoID := models.SubscriberConfig{
		Topics: []string{"topic1", "topic2"},
	}
	subscriber2, err := registry.Register(configNoID)
	assert.NoError(t, err)
	assert.NotNil(t, subscriber2)
	assert.NotEmpty(t, subscriber2.GetID())
	assert.NotEqual(t, subscriber.GetID(), subscriber2.GetID())

	// Verify registry count
	assert.Equal(t, 2, registry.Count())
}

func TestGet(t *testing.T) {
	registry := NewRegistry()
	config := models.SubscriberConfig{
		ID:     "test-subscriber-get",
		Topics: []string{"topic1"},
	}
	_, err := registry.Register(config)
	require.NoError(t, err)

	// Get existing subscriber
	subscriber, err := registry.Get("test-subscriber-get")
	assert.NoError(t, err)
	assert.NotNil(t, subscriber)
	assert.Equal(t, "test-subscriber-get", subscriber.GetID())

	// Get non-existent subscriber
	_, err = registry.Get("non-existent-subscriber")
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrSubscriberNotFound))
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
	config1 := models.SubscriberConfig{ID: "s1", Topics: []string{"A", "B"}}
	config2 := models.SubscriberConfig{ID: "s2", Topics: []string{"A", "C"}}
	config3 := models.SubscriberConfig{ID: "s3", Topics: []string{"B", "C"}}
	_, _ = registry.Register(config1)
	_, _ = registry.Register(config2)
	_, _ = registry.Register(config3)

	// List all
	allSubs := registry.List("")
	assert.Len(t, allSubs, 3)

	// List by topic A
	topicASubs := registry.List("A")
	assert.Len(t, topicASubs, 2)
	ids := make(map[string]bool)
	for _, sub := range topicASubs {
		ids[sub.GetID()] = true // Use GetID()
	}
	assert.True(t, ids["s1"])
	assert.True(t, ids["s2"])

	// List by topic B
	topicBSubs := registry.List("B")
	assert.Len(t, topicBSubs, 2)
	ids = make(map[string]bool)
	for _, sub := range topicBSubs {
		ids[sub.GetID()] = true // Use GetID()
	}
	assert.True(t, ids["s1"])
	assert.True(t, ids["s3"])

	// List non-existent topic
	topicDSubs := registry.List("D")
	assert.Len(t, topicDSubs, 0)
}

func TestListByState(t *testing.T) {
	registry := NewRegistry()
	cfgActive1 := models.SubscriberConfig{ID: "active1", Topics: []string{"t1"}}
	cfgActive2 := models.SubscriberConfig{ID: "active2", Topics: []string{"t2"}}
	cfgPaused1 := models.SubscriberConfig{ID: "paused1", Topics: []string{"t3"}}

	subActive1, _ := registry.Register(cfgActive1)
	_, _ = registry.Register(cfgActive2)
	subPaused1, _ := registry.Register(cfgPaused1)

	// Pause one subscriber
	err := subPaused1.Pause() // Use interface method
	require.NoError(t, err)

	// List active
	activeSubs := registry.ListByState(models.SubscriberStateActive)
	assert.Len(t, activeSubs, 2)
	activeIDs := make(map[string]bool)
	for _, sub := range activeSubs {
		activeIDs[sub.GetID()] = true // Use GetID()
	}
	assert.Contains(t, activeIDs, "active1")
	assert.Contains(t, activeIDs, "active2")

	// List paused
	pausedSubs := registry.ListByState(models.SubscriberStatePaused)
	assert.Len(t, pausedSubs, 1)
	assert.Equal(t, "paused1", pausedSubs[0].GetID()) // Use GetID()

	// List closed (none)
	closedSubs := registry.ListByState(models.SubscriberStateClosed)
	assert.Len(t, closedSubs, 0)

	// Close one active subscriber
	err = subActive1.Close() // Use interface method
	require.NoError(t, err)

	// List active again
	activeSubs = registry.ListByState(models.SubscriberStateActive)
	assert.Len(t, activeSubs, 1)
	assert.Equal(t, "active2", activeSubs[0].GetID()) // Use GetID()

	// List closed again
	closedSubs = registry.ListByState(models.SubscriberStateClosed)
	assert.Len(t, closedSubs, 1)
	assert.Equal(t, "active1", closedSubs[0].GetID()) // Use GetID()
}

func TestUpdate(t *testing.T) {
	registry := NewRegistry()
	config := models.SubscriberConfig{
		ID:     "test-subscriber-update",
		Topics: []string{"topic1"},
		Filter: models.SubscriberFilter{EventTypes: []string{"typeA"}},
	}
	subscriber, err := registry.Register(config)
	require.NoError(t, err)
	require.NotNil(t, subscriber)

	// Update the filter
	err = registry.Update("test-subscriber-update", func(sub outbound.Subscriber) error {
		// Type assertion needed to update filter details if not on interface
		newFilter := outbound.SubscriberFilter{
			EventTypes:  []string{"typeB", "typeC"},
			FromVersion: 10,
		}
		return sub.UpdateFilter(newFilter) // Use interface method if available
	})
	assert.NoError(t, err)

	// Verify update
	updatedSub, _ := registry.Get("test-subscriber-update")
	assert.NotNil(t, updatedSub)
	filter := updatedSub.GetFilter() // Use GetFilter()
	assert.Equal(t, []string{"typeB", "typeC"}, filter.EventTypes)
	assert.Equal(t, int64(10), filter.FromVersion)

	// Test update non-existent
	err = registry.Update("non-existent-subscriber", func(sub outbound.Subscriber) error { return nil })
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrSubscriberNotFound))

	// Test update function returning error
	err = registry.Update("test-subscriber-update", func(sub outbound.Subscriber) error {
		return errors.New("test error")
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "test error")

	// Test update closed subscriber
	_ = updatedSub.Close() // Close the subscriber
	err = registry.Update("test-subscriber-update", func(sub outbound.Subscriber) error { return nil })
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrSubscriberClosed))
}

func TestUpdateTopics(t *testing.T) {
	registry := NewRegistry()
	config := models.SubscriberConfig{
		ID:     "test-subscriber-update-topics",
		Topics: []string{"topic1", "topic2"},
	}
	subscriber, err := registry.Register(config)
	require.NoError(t, err)
	require.NotNil(t, subscriber)

	// Check initial topics
	assert.ElementsMatch(t, []string{"topic1", "topic2"}, subscriber.GetTopics()) // Use GetTopics()
	assert.Equal(t, 1, registry.CountByTopic("topic1"))
	assert.Equal(t, 1, registry.CountByTopic("topic2"))

	// Update topics
	err = registry.UpdateTopics("test-subscriber-update-topics", []string{"topic3", "topic4"})
	assert.NoError(t, err)

	// Verify update
	updatedSub, _ := registry.Get("test-subscriber-update-topics")
	assert.NotNil(t, updatedSub)
	assert.ElementsMatch(t, []string{"topic3", "topic4"}, updatedSub.GetTopics()) // Use GetTopics()
	assert.Equal(t, 0, registry.CountByTopic("topic1"))                           // Should be removed from old topics
	assert.Equal(t, 0, registry.CountByTopic("topic2"))
	assert.Equal(t, 1, registry.CountByTopic("topic3")) // Should be added to new topics
	assert.Equal(t, 1, registry.CountByTopic("topic4"))

	// Test update non-existent
	err = registry.UpdateTopics("non-existent-subscriber", []string{"topic5"})
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrSubscriberNotFound))
}

func TestCleanupInactive(t *testing.T) {
	registry := NewRegistry()
	logger := NewSubscriberLogger(DEBUG)
	var buf bytes.Buffer
	logger.logger = log.New(&buf, "", 0)
	registry.SetLogger(logger)

	config1 := models.SubscriberConfig{ID: "active-subscriber"}
	config2 := models.SubscriberConfig{ID: "inactive-subscriber"}
	// Capture both return values
	_, errAct := registry.Register(config1)
	require.NoError(t, errAct)
	inactiveSub, errInact := registry.Register(config2)
	require.NoError(t, errInact)

	inactiveSubModel, ok := inactiveSub.(*models.Subscriber)
	require.True(t, ok, "Inactive subscriber should be *models.Subscriber")
	inactiveSubModel.LastActivityAt = time.Now().Add(-2 * time.Hour)

	removedCount := registry.CleanupInactive(1 * time.Hour)
	assert.Equal(t, 1, removedCount)

	_, err := registry.Get("inactive-subscriber")
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrSubscriberNotFound))

	_, err = registry.Get("active-subscriber")
	assert.NoError(t, err)

	output := buf.String()
	assert.Contains(t, output, "deregistered")
	assert.Contains(t, output, "inactive_timeout")
	assert.Contains(t, output, "inactive-subscriber")

	removedCount = registry.CleanupInactive(1 * time.Hour)
	assert.Equal(t, 0, removedCount)
}

// Mock subscriber for broadcast testing
type mockDeliverySubscriber struct {
	models.Subscriber // Embed for default behavior, easier setup
	ReceivedEvents    chan outbound.Event
	DeliveryError     error         // Optional error to return on ReceiveEvent
	DeliveryDelay     time.Duration // Optional delay in ReceiveEvent
}

func newMockDeliverySubscriber(config models.SubscriberConfig, bufferSize int) *mockDeliverySubscriber {
	return &mockDeliverySubscriber{
		Subscriber:     *models.NewSubscriber(config),
		ReceivedEvents: make(chan outbound.Event, bufferSize),
	}
}

// Override ReceiveEvent
func (m *mockDeliverySubscriber) ReceiveEvent(ctx context.Context, event outbound.Event) error {
	if m.DeliveryDelay > 0 {
		select {
		case <-time.After(m.DeliveryDelay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if m.DeliveryError != nil {
		// Type assert to call model-specific method
		if subModel, ok := &m.Subscriber; ok { // Assuming m.Subscriber is the embedded *models.Subscriber
			subModel.HandleRetryIfError(m.DeliveryError)
		} else {
			// Log or handle error if type assertion fails unexpectedly
		}
		return m.DeliveryError
	}

	// Type assert to access EventChannel
	var eventChannel chan outbound.Event
	if subModel, ok := &m.Subscriber; ok {
		eventChannel = subModel.EventChannel
	} else {
		// Mock needs its own channel if not embedding, return error if channel is nil
		return errors.New("mock subscriber channel not initialized")
	}

	select {
	case eventChannel <- event: // Use the obtained channel
		// Type assert to call model-specific method
		if subModel, ok := &m.Subscriber; ok {
			subModel.ResetRetryIfSuccess()
		}
		return nil
	case <-ctx.Done():
		// Type assert to call model-specific method
		if subModel, ok := &m.Subscriber; ok {
			subModel.HandleRetryIfError(ctx.Err())
		}
		return ctx.Err()
	}
}

// Helper methods assumed to exist on models.Subscriber for mock interaction
// These would typically call handleRetry() or resetRetryCount()
func (s *Subscriber) HandleRetryIfError(err error) {
	if err != nil {
		s.handleRetry()
	}
}
func (s *Subscriber) ResetRetryIfSuccess() {
	s.resetRetryCount()
}

func TestBroadcastEvent(t *testing.T) {
	registry := NewRegistry()
	buf := bytes.Buffer{}
	logger := NewSubscriberLogger(DEBUG)
	logger.logger = log.New(&buf, "", 0)
	registry.SetLogger(logger)

	// Create mock subscribers
	bufferSize := 10
	sub1Config := models.SubscriberConfig{ID: "sub1", Topics: []string{"topicA", "topicB"}}
	sub2Config := models.SubscriberConfig{ID: "sub2", Topics: []string{"topicA"}}
	sub3Config := models.SubscriberConfig{ID: "sub3", Topics: []string{"topicC"}} // Different topic
	sub4Config := models.SubscriberConfig{ID: "sub4-paused", Topics: []string{"topicA"}}

	mockSub1 := newMockDeliverySubscriber(sub1Config, bufferSize)
	mockSub2 := newMockDeliverySubscriber(sub2Config, bufferSize)
	mockSub3 := newMockDeliverySubscriber(sub3Config, bufferSize)
	mockSub4Paused := newMockDeliverySubscriber(sub4Config, bufferSize)

	// Register subscribers (add mocks directly as they implement the interface)
	registry.subscribers[mockSub1.GetID()] = mockSub1
	registry.addToTopicMapsLocked(mockSub1)
	registry.subscribers[mockSub2.GetID()] = mockSub2
	registry.addToTopicMapsLocked(mockSub2)
	registry.subscribers[mockSub3.GetID()] = mockSub3
	registry.addToTopicMapsLocked(mockSub3)
	registry.subscribers[mockSub4Paused.GetID()] = mockSub4Paused
	registry.addToTopicMapsLocked(mockSub4Paused)

	// Pause one subscriber
	err := mockSub4Paused.Pause()
	require.NoError(t, err)

	// Create event for topic A
	eventA := outbound.Event{
		ID:    "event-a-1",
		Topic: "topicA",
		Type:  "TypeA",
	}

	// Broadcast event A
	successCount := registry.BroadcastEvent(context.Background(), eventA)
	assert.Equal(t, 2, successCount, "Expected 2 successful deliveries for topic A (sub1, sub2)")

	// Check received events
	select {
	case received := <-mockSub1.ReceivedEvents:
		assert.Equal(t, eventA.ID, received.ID)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Timed out waiting for event on mockSub1")
	}
	select {
	case received := <-mockSub2.ReceivedEvents:
		assert.Equal(t, eventA.ID, received.ID)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Timed out waiting for event on mockSub2")
	}
	// Sub3 should not receive (wrong topic)
	select {
	case <-mockSub3.ReceivedEvents:
		t.Fatal("mockSub3 should not have received event A")
	default:
	}
	// Sub4 should not receive (paused)
	select {
	case <-mockSub4Paused.ReceivedEvents:
		t.Fatal("mockSub4Paused should not have received event A")
	default:
	}

	// Check logs
	output := buf.String()
	assert.Contains(t, output, "Broadcasting event")
	assert.Contains(t, output, "\"subscriber_count\":4") // 4 total initially
	assert.Contains(t, output, "\"filtered_count\":2")   // 2 delivered (sub1, sub2)
	assert.Contains(t, output, "\"success_count\":2")
	assert.Contains(t, output, "Event broadcast completed")

	// Test broadcast to empty topic
	buf.Reset()
	eventD := outbound.Event{ID: "event-d-1", Topic: "topicD"}
	successCount = registry.BroadcastEvent(context.Background(), eventD)
	assert.Equal(t, 0, successCount)
	output = buf.String()
	assert.Contains(t, output, "No subscribers found for topic")
}

// ... TestConcurrentOperations, TestEventFiltering, TestRegisterWithClientInfo ...

// TestConcurrentOperations needs careful checking if it relies on specific model fields.

func TestEventFiltering(t *testing.T) {
	registry := NewRegistry()
	logger := NewSubscriberLogger(DEBUG)
	registry.SetLogger(logger)

	allEventsConfig := models.SubscriberConfig{ID: "all-events", Topics: []string{"test-topic"}}
	typeFilterConfig := models.SubscriberConfig{ID: "type-filter", Topics: []string{"test-topic"},
		Filter: models.SubscriberFilter{EventTypes: []string{"TestEvent", "KeepEvent"}},
	}
	versionFilterConfig := models.SubscriberConfig{ID: "version-filter", Topics: []string{"test-topic"},
		Filter: models.SubscriberFilter{FromVersion: 5},
	}
	pausedConfig := models.SubscriberConfig{ID: "paused-filter", Topics: []string{"test-topic"}}

	// Use mock subscribers for controlled event checking
	allEventsChannel := make(chan outbound.Event, 5)
	typeFilterChannel := make(chan outbound.Event, 5)
	versionFilterChannel := make(chan outbound.Event, 5)

	mockSubAll := &mockDeliverySubscriber{Subscriber: *models.NewSubscriber(allEventsConfig), ReceivedEvents: allEventsChannel}
	mockSubType := &mockDeliverySubscriber{Subscriber: *models.NewSubscriber(typeFilterConfig), ReceivedEvents: typeFilterChannel}
	mockSubVersion := &mockDeliverySubscriber{Subscriber: *models.NewSubscriber(versionFilterConfig), ReceivedEvents: versionFilterChannel}
	mockSubPausedReg, errPaused := registry.Register(pausedConfig)
	require.NoError(t, errPaused)
	err := mockSubPausedReg.Pause()
	require.NoError(t, err)

	// Add mocks to registry
	registry.subscribers[mockSubAll.GetID()] = mockSubAll
	registry.addToTopicMapsLocked(mockSubAll)
	registry.subscribers[mockSubType.GetID()] = mockSubType
	registry.addToTopicMapsLocked(mockSubType)
	registry.subscribers[mockSubVersion.GetID()] = mockSubVersion
	registry.addToTopicMapsLocked(mockSubVersion)
	// mockSubPaused already registered

	// Event matching all filters (except paused)
	event1 := outbound.Event{ID: "event-1", Topic: "test-topic", Type: "TestEvent", Version: 10}
	successCount := registry.BroadcastEvent(context.Background(), event1)
	assert.Equal(t, 3, successCount, "Event 1 should reach 3 subscribers")
	assert.Equal(t, event1.ID, (<-allEventsChannel).ID)
	assert.Equal(t, event1.ID, (<-typeFilterChannel).ID)
	assert.Equal(t, event1.ID, (<-versionFilterChannel).ID)

	// Event filtered by type
	event2 := outbound.Event{ID: "event-2", Topic: "test-topic", Type: "OtherEvent", Version: 10}
	successCount = registry.BroadcastEvent(context.Background(), event2)
	assert.Equal(t, 2, successCount, "Event 2 should reach 2 subscribers (all, version)")
	assert.Equal(t, event2.ID, (<-allEventsChannel).ID)
	assert.Equal(t, event2.ID, (<-versionFilterChannel).ID)
	// Ensure type filter channel is empty
	select {
	case <-typeFilterChannel:
		t.Fatal("Type filter channel should be empty after event 2")
	default:
	}

	// Event filtered by version
	event3 := outbound.Event{ID: "event-3", Topic: "test-topic", Type: "KeepEvent", Version: 4}
	successCount = registry.BroadcastEvent(context.Background(), event3)
	assert.Equal(t, 2, successCount, "Event 3 should reach 2 subscribers (all, type)")
	assert.Equal(t, event3.ID, (<-allEventsChannel).ID)
	assert.Equal(t, event3.ID, (<-typeFilterChannel).ID)
	// Ensure version filter channel is empty
	select {
	case <-versionFilterChannel:
		t.Fatal("Version filter channel should be empty after event 3")
	default:
	}

	// Event filtered by both type and version
	event4 := outbound.Event{ID: "event-4", Topic: "test-topic", Type: "FilteredEvent", Version: 3}
	successCount = registry.BroadcastEvent(context.Background(), event4)
	assert.Equal(t, 1, successCount, "Event 4 should reach 1 subscriber (all)")
	assert.Equal(t, event4.ID, (<-allEventsChannel).ID)
	// Ensure other channels are empty
	select {
	case <-typeFilterChannel:
		t.Fatal("Type filter channel should be empty after event 4")
	default:
	}
	select {
	case <-versionFilterChannel:
		t.Fatal("Version filter channel should be empty after event 4")
	default:
	}
}

// ... TestBroadcastErrorLogging - needs mock update ...

// --- Mock Subscriber for Error Testing (implements interface) ---
// (Keep the interface-based mock definition from previous step)

func TestBroadcastErrorLogging(t *testing.T) {
	// Create a buffer to capture log output
	var buf bytes.Buffer
	customLogger := log.New(&buf, "", 0)
	logger := NewSubscriberLogger(DEBUG) // Use DEBUG level
	logger.logger = customLogger

	// Initialize registry with the custom logger
	poolConfig := DefaultWorkerPoolConfig()
	// CORRECT Map Initialization using interface type
	registry := &Registry{
		subscribers:      make(map[string]outbound.Subscriber),
		topicSubscribers: make(map[string]map[string]outbound.Subscriber),
		logger:           logger,
	}
	registry.workerPool = newWorkerPool(poolConfig, registry.logger)

	// Create a mock subscriber that returns an error
	mockError := errors.New("simulated delivery error")
	mockConfig := models.SubscriberConfig{
		ID:     "mock-error-sub",
		Topics: []string{"error-topic"},
		Filter: models.SubscriberFilter{},
		Timeout: models.TimeoutConfig{ // Set some timeouts for retry info logging
			InitialTimeout:    50 * time.Millisecond,
			MaxTimeout:        200 * time.Millisecond,
			BackoffMultiplier: 2.0,
			MaxRetries:        3,
		},
		BufferSize: 10,
	}
	// Use the interface-based mock
	mockSub := NewMockErrorSubscriber(mockConfig, mockError, 0)

	// Add the mock subscriber (which implements the interface) directly
	registry.subscribers[mockSub.GetID()] = mockSub
	registry.addToTopicMapsLocked(mockSub) // Use locked version as we hold the lock conceptually

	// Create a test event
	event := outbound.Event{
		ID:        "error-test-event",
		Topic:     "error-topic",
		Type:      "ErrorEvent",
		Timestamp: time.Now().UnixNano(),
		Version:   1,
	}

	// Broadcast the event
	successCount := registry.BroadcastEvent(context.Background(), event)

	// Allow time for the worker to process
	time.Sleep(150 * time.Millisecond) // Increased sleep slightly

	// Expect 0 successful deliveries
	assert.Equal(t, 0, successCount, "Expected 0 successful deliveries for error test")

	// Verify the detailed error log
	output := buf.String()
	logLines := strings.Split(strings.TrimSpace(output), "\n")
	foundErrorLog := false
	for _, line := range logLines {
		var entry map[string]interface{}
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Logf("Skipping non-JSON log line: %s", line)
			continue
		}

		if level, ok := entry["level"].(string); ok && level == "WARN" {
			if msg, ok := entry["message"].(string); ok && strings.Contains(msg, "Event delivery failed") {
				if sid, ok := entry["subscriber_id"].(string); ok && sid == mockSub.GetID() {
					assert.Equal(t, mockError.Error(), entry["error"], "Error message mismatch in log")
					// Check for retry info (mock increments retry count to 1)
					assert.EqualValues(t, 1, entry["retry_count"], "Retry count mismatch in log")
					assert.Contains(t, entry, "next_timeout", "Next timeout missing in log")
					assert.Contains(t, entry, "subscriber_details", "Subscriber details missing in log")
					foundErrorLog = true
					break
				}
			}
		}
	}

	assert.True(t, foundErrorLog, "Expected WARN log for failed delivery not found. Logs:\n%s", output)
}

func TestConcurrentOperations(t *testing.T) {
	registry := NewRegistry()
	numSubscribers := 10
	var wg sync.WaitGroup
	startSignal := make(chan struct{})

	// Concurrent registration
	wg.Add(numSubscribers)
	for i := 0; i < numSubscribers; i++ {
		go func(id int) {
			defer wg.Done()
			<-startSignal // Wait for signal to start concurrently
			config := models.SubscriberConfig{
				ID:     fmt.Sprintf("concurrent-%d", id),
				Topics: []string{"topic1", "topic2"},
			}
			_, err := registry.Register(config)
			assert.NoError(t, err)
		}(i)
	}
	close(startSignal) // Signal workers to start
	wg.Wait()
	assert.Equal(t, numSubscribers, registry.Count())

	// Concurrent get and update
	startSignal = make(chan struct{})
	wg.Add(numSubscribers * 2)
	for i := 0; i < numSubscribers; i++ {
		go func(id int) { // Concurrent Get
			defer wg.Done()
			<-startSignal
			subID := fmt.Sprintf("concurrent-%d", id)
			sub, err := registry.Get(subID)
			assert.NoError(t, err)
			assert.NotNil(t, sub)
			assert.Equal(t, subID, sub.GetID()) // Use GetID()
		}(i)
		go func(id int) { // Concurrent Update
			defer wg.Done()
			<-startSignal
			subID := fmt.Sprintf("concurrent-%d", id)
			err := registry.Update(subID, func(s outbound.Subscriber) error {
				// Example update: change filter
				newFilter := outbound.SubscriberFilter{EventTypes: []string{fmt.Sprintf("type-%d", id)}}
				return s.UpdateFilter(newFilter)
			})
			// Tolerate potential race conditions where Get happens before Update finishes fully in map
			if err != nil {
				assert.ErrorIs(t, err, ErrSubscriberNotFound, "Update error should be Not Found if Get won race")
			}
		}(i)
	}
	close(startSignal)
	wg.Wait()

	// Concurrent deregistration
	startSignal = make(chan struct{})
	wg.Add(numSubscribers)
	for i := 0; i < numSubscribers; i++ {
		go func(id int) {
			defer wg.Done()
			<-startSignal
			subID := fmt.Sprintf("concurrent-%d", id)
			err := registry.Deregister(subID)
			assert.NoError(t, err)
		}(i)
	}
	close(startSignal)
	wg.Wait()
	assert.Equal(t, 0, registry.Count())
}

// --- Interface-based Mock Subscriber for Error Testing ---
type mockErrorSubscriber struct {
	id            string
	topics        []string
	filter        models.SubscriberFilter
	timeout       models.TimeoutConfig
	state         models.SubscriberState
	errorCount    int64
	timeoutCount  int64
	delay         time.Duration
	errorToReturn error
	mu            sync.RWMutex
}

func NewMockErrorSubscriber(config models.SubscriberConfig, err error, delay time.Duration) *mockErrorSubscriber {
	m := &mockErrorSubscriber{
		id:            config.ID,
		topics:        config.Topics,
		filter:        config.Filter,
		timeout:       config.Timeout,
		state:         models.SubscriberStateActive,
		errorToReturn: err,
		delay:         delay,
	}
	if m.timeout.InitialTimeout == 0 {
		m.timeout = models.DefaultTimeoutConfig()
	}
	return m
}

// Implement methods from outbound.Subscriber interface for mockErrorSubscriber
func (m *mockErrorSubscriber) ReceiveEvent(ctx context.Context, event outbound.Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.delay > 0 {
		select {
		case <-time.After(m.delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	m.errorCount++
	m.timeoutCount++
	m.timeout.CurrentRetryCount++
	m.timeout.LastRetryTime = time.Now()
	return m.errorToReturn
}
func (m *mockErrorSubscriber) GetID() string { return m.id }
func (m *mockErrorSubscriber) GetTopics() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	topicsCopy := make([]string, len(m.topics))
	copy(topicsCopy, m.topics)
	return topicsCopy
}
func (m *mockErrorSubscriber) GetFilter() outbound.SubscriberFilter {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.filter.ToOutboundFilter()
}
func (m *mockErrorSubscriber) GetState() outbound.SubscriberState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state.ToOutboundState()
}
func (m *mockErrorSubscriber) GetStats() outbound.SubscriberStats {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return outbound.SubscriberStats{
		ErrorCount:        m.errorCount,
		TimeoutCount:      m.timeoutCount,
		CurrentRetryCount: m.timeout.CurrentRetryCount,
	}
}
func (m *mockErrorSubscriber) Pause() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = models.SubscriberStatePaused
	return nil
}
func (m *mockErrorSubscriber) Resume() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = models.SubscriberStateActive
	return nil
}
func (m *mockErrorSubscriber) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = models.SubscriberStateClosed
	return nil
}
func (m *mockErrorSubscriber) UpdateFilter(filter outbound.SubscriberFilter) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.filter = models.SubscriberFilterFromOutbound(filter)
	return nil
}
func (m *mockErrorSubscriber) UpdateTimeout(config outbound.TimeoutConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.timeout = models.TimeoutConfigFromOutbound(config)
	return nil
}
func (m *mockErrorSubscriber) GetRetryInfo() (retryCount int, lastRetryTime time.Time, nextTimeout time.Duration) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	nextTimeoutCalc := m.timeout.InitialTimeout
	if m.timeout.BackoffMultiplier > 0 {
		nextTimeoutCalc = time.Duration(float64(m.timeout.InitialTimeout) * math.Pow(m.timeout.BackoffMultiplier, float64(m.timeout.CurrentRetryCount)))
	}
	if m.timeout.MaxTimeout > 0 && nextTimeoutCalc > m.timeout.MaxTimeout {
		nextTimeoutCalc = m.timeout.MaxTimeout
	}
	return m.timeout.CurrentRetryCount, m.timeout.LastRetryTime, nextTimeoutCalc
}

// --- End mockErrorSubscriber ---

// --- Mock Subscriber for Delivery Testing (Uses embedding) ---
type mockDeliverySubscriber struct {
	models.Subscriber // Embed for convenience
	ReceivedEvents    chan outbound.Event
	DeliveryError     error
	DeliveryDelay     time.Duration
}

func newMockDeliverySubscriber(config models.SubscriberConfig, bufferSize int) *mockDeliverySubscriber {
	m := &mockDeliverySubscriber{
		Subscriber:     *models.NewSubscriber(config),
		ReceivedEvents: make(chan outbound.Event, bufferSize),
	}
	m.Subscriber.SetLogger(log.New(io.Discard, "", 0)) // Use io.Discard
	return m
}

// Override ReceiveEvent for mockDeliverySubscriber
func (m *mockDeliverySubscriber) ReceiveEvent(ctx context.Context, event outbound.Event) error {
	if m.DeliveryDelay > 0 {
		select {
		case <-time.After(m.DeliveryDelay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if m.DeliveryError != nil {
		// Call the embedded HandleRetryIfError (assuming it exists on models.Subscriber)
		m.Subscriber.HandleRetryIfError(m.DeliveryError)
		return m.DeliveryError
	}

	// Access embedded channel directly
	eventChannel := m.Subscriber.EventChannel
	if eventChannel == nil {
		return errors.New("embedded subscriber channel not initialized")
	}

	select {
	case eventChannel <- event:
		// Call the embedded ResetRetryIfSuccess (assuming it exists on models.Subscriber)
		m.Subscriber.ResetRetryIfSuccess()
		return nil
	case <-ctx.Done():
		// Call the embedded HandleRetryIfError (assuming it exists on models.Subscriber)
		m.Subscriber.HandleRetryIfError(ctx.Err())
		return ctx.Err()
	}
}

// --- End mockDeliverySubscriber ---
