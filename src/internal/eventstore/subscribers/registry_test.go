package subscribers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
		ID:     "123e4567-e89b-12d3-a456-426614174000", // Valid UUID
		Topics: []string{"topic1", "topic2"},
	}

	subscriber, err := registry.Register(config)
	assert.NoError(t, err)
	assert.NotNil(t, subscriber)
	assert.Equal(t, "123e4567-e89b-12d3-a456-426614174000", subscriber.GetID())

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
		ID:     "00000000-0000-0000-0000-000000000001",
		Topics: []string{"topic1"},
	}
	_, err := registry.Register(config)
	require.NoError(t, err)

	// Get existing subscriber
	subscriber, err := registry.Get("00000000-0000-0000-0000-000000000001")
	assert.NoError(t, err)
	assert.NotNil(t, subscriber)
	assert.Equal(t, "00000000-0000-0000-0000-000000000001", subscriber.GetID())

	// Get non-existent subscriber
	_, err = registry.Get("00000000-0000-0000-0000-000000000002")
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
		ID:      "00000000-0000-0000-0000-000000000003",
		Topics:  []string{"topic1", "topic2"},
		Filter:  models.SubscriberFilter{},
		Timeout: models.DefaultTimeoutConfig(),
	}

	_, _ = registry.Register(config)

	// Test successful deregistration
	err := registry.Deregister("00000000-0000-0000-0000-000000000003")
	if err != nil {
		t.Fatalf("Unexpected error deregistering subscriber: %v", err)
	}

	// Verify subscriber is removed
	_, err = registry.Get("00000000-0000-0000-0000-000000000003")
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
				if sid, ok := logEntry["subscriber_id"].(string); ok && sid == "00000000-0000-0000-0000-000000000003" {
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
		t.Errorf("Could not find deregistration log entry for 00000000-0000-0000-0000-000000000003 in output: %s", output)
	}

	// Test deregistering non-existent subscriber
	buf.Reset() // Clear buffer before testing non-existent case
	err = registry.Deregister("00000000-0000-0000-0000-000000000999")
	if err != ErrSubscriberNotFound {
		t.Errorf("Expected ErrSubscriberNotFound, got %v", err)
	}
	// Optionally, verify the warning log for non-existent subscriber was generated
}

func TestList(t *testing.T) {
	registry := NewRegistry()
	config1 := models.SubscriberConfig{ID: "63e4cf84-7189-45a5-8c0c-c82165f5bb24", Topics: []string{"A", "B"}}
	config2 := models.SubscriberConfig{ID: "a2b4ca5f-317d-4a90-984c-9e4f0e274e04", Topics: []string{"A", "C"}}
	config3 := models.SubscriberConfig{ID: "1cc25ab0-eb4a-4f18-80f9-4a5d3d6aae58", Topics: []string{"B", "C"}}
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
	assert.True(t, ids["63e4cf84-7189-45a5-8c0c-c82165f5bb24"])
	assert.True(t, ids["a2b4ca5f-317d-4a90-984c-9e4f0e274e04"])

	// List by topic B
	topicBSubs := registry.List("B")
	assert.Len(t, topicBSubs, 2)
	ids = make(map[string]bool)
	for _, sub := range topicBSubs {
		ids[sub.GetID()] = true // Use GetID()
	}
	assert.True(t, ids["63e4cf84-7189-45a5-8c0c-c82165f5bb24"])
	assert.True(t, ids["1cc25ab0-eb4a-4f18-80f9-4a5d3d6aae58"])

	// List non-existent topic
	topicDSubs := registry.List("D")
	assert.Len(t, topicDSubs, 0)
}

func TestListByState(t *testing.T) {
	registry := NewRegistry()
	cfgActive1 := models.SubscriberConfig{ID: "a13dcf5b-4ea4-47b4-a835-88123e71dad0", Topics: []string{"t1"}}
	cfgActive2 := models.SubscriberConfig{ID: "b2c1e94f-48b2-4572-9d06-7bebd4a751a3", Topics: []string{"t2"}}
	cfgPaused1 := models.SubscriberConfig{ID: "c3b2d85e-9cb1-4671-8d17-6abc3b721fb2", Topics: []string{"t3"}}

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
	assert.Contains(t, activeIDs, "a13dcf5b-4ea4-47b4-a835-88123e71dad0")
	assert.Contains(t, activeIDs, "b2c1e94f-48b2-4572-9d06-7bebd4a751a3")

	// List paused
	pausedSubs := registry.ListByState(models.SubscriberStatePaused)
	assert.Len(t, pausedSubs, 1)
	assert.Equal(t, "c3b2d85e-9cb1-4671-8d17-6abc3b721fb2", pausedSubs[0].GetID()) // Use GetID()

	// List closed (none)
	closedSubs := registry.ListByState(models.SubscriberStateClosed)
	assert.Len(t, closedSubs, 0)

	// Close one active subscriber
	err = subActive1.Close() // Use interface method
	require.NoError(t, err)

	// List active again
	activeSubs = registry.ListByState(models.SubscriberStateActive)
	assert.Len(t, activeSubs, 1)
	assert.Equal(t, "b2c1e94f-48b2-4572-9d06-7bebd4a751a3", activeSubs[0].GetID()) // Use GetID()

	// List closed again
	closedSubs = registry.ListByState(models.SubscriberStateClosed)
	assert.Len(t, closedSubs, 1)
	assert.Equal(t, "a13dcf5b-4ea4-47b4-a835-88123e71dad0", closedSubs[0].GetID()) // Use GetID()
}

func TestUpdate(t *testing.T) {
	// Create registry with custom logger
	registry := NewRegistry()
	logger := NewSubscriberLogger(DEBUG)
	registry.SetLogger(logger)

	// Register a subscriber
	subConfig := models.SubscriberConfig{
		ID:     "00000000-0000-0000-0000-000000000004",
		Topics: []string{"topic1"},
		Filter: models.SubscriberFilter{
			EventTypes: []string{"type1"},
		},
	}
	sub, err := registry.Register(subConfig)
	require.NoError(t, err)
	require.NotNil(t, sub)

	// Add specific event types to filter
	err = registry.Update("00000000-0000-0000-0000-000000000004", func(sub outbound.Subscriber) error {
		filter := sub.GetFilter()
		filter.EventTypes = append(filter.EventTypes, "type2")
		return sub.UpdateFilter(filter)
	})
	assert.NoError(t, err)

	// Verify update
	updatedSub, err := registry.Get("00000000-0000-0000-0000-000000000004")
	require.NoError(t, err)
	filter := updatedSub.GetFilter()
	assert.Contains(t, filter.EventTypes, "type1")
	assert.Contains(t, filter.EventTypes, "type2")

	// Try to update non-existent subscriber
	err = registry.Update("00000000-0000-0000-0000-000000000099", func(sub outbound.Subscriber) error {
		return nil
	})
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrSubscriberNotFound))

	// Try update with error in function
	testErr := errors.New("test error")
	err = registry.Update("00000000-0000-0000-0000-000000000004", func(sub outbound.Subscriber) error {
		return testErr
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), testErr.Error())

	// Verify that the subscriber is unchanged after error
	sub2, err := registry.Get("00000000-0000-0000-0000-000000000004")
	require.NoError(t, err)
	assert.Equal(t, "00000000-0000-0000-0000-000000000004", sub2.GetID())
	assert.Equal(t, 2, len(sub2.GetFilter().EventTypes))
}

func TestUpdateTopics(t *testing.T) {
	registry := NewRegistry()
	config := models.SubscriberConfig{
		ID:     "00000000-0000-0000-0000-000000000005",
		Topics: []string{"topic1", "topic2"},
	}
	sub, err := registry.Register(config)
	require.NoError(t, err)
	require.NotNil(t, sub)

	// Verify the subscriber has the correct initial topics
	assert.Equal(t, []string{"topic1", "topic2"}, sub.GetTopics())

	// Update the topics
	err = registry.UpdateTopics("00000000-0000-0000-0000-000000000005", []string{"topic2", "topic3"})
	assert.NoError(t, err)

	// Verify the update
	updatedSub, err := registry.Get("00000000-0000-0000-0000-000000000005")
	require.NoError(t, err)
	assert.Equal(t, []string{"topic2", "topic3"}, updatedSub.GetTopics())

	// Check topic map updates
	assert.Equal(t, 0, registry.CountByTopic("topic1"), "Topic1 should have 0 subscribers")
	assert.Equal(t, 1, registry.CountByTopic("topic2"), "Topic2 should have 1 subscriber")
	assert.Equal(t, 1, registry.CountByTopic("topic3"), "Topic3 should have 1 subscriber")

	// Test updating non-existent subscriber
	err = registry.UpdateTopics("00000000-0000-0000-0000-000000000099", []string{"topic4"})
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrSubscriberNotFound))
}

func TestCleanupInactive(t *testing.T) {
	registry := NewRegistry()
	config1 := models.SubscriberConfig{ID: "10000000-0000-0000-0000-000000000001"}
	config2 := models.SubscriberConfig{ID: "20000000-0000-0000-0000-000000000002"}

	// Register the subscribers
	sub1, err := registry.Register(config1)
	require.NoError(t, err)
	_, err = registry.Register(config2)
	require.NoError(t, err)

	// Make sure both subscribers are initially present
	assert.Equal(t, 2, registry.Count())

	// Type assertion to access and modify LastActivityAt on sub1
	// This is implementation-specific; may need appropriate method on the interface instead
	if modelSub, ok := sub1.(*models.Subscriber); ok {
		// Set the first subscriber to be inactive for 2 hours
		modelSub.LastActivityAt = time.Now().Add(-2 * time.Hour)
	} else {
		t.Fatal("Type assertion failed, cannot manipulate LastActivityAt")
	}

	// Cleanup subscribers inactive for more than 1 hour
	removedCount := registry.CleanupInactive(1 * time.Hour)
	assert.Equal(t, 1, removedCount, "Should remove 1 inactive subscriber")
	assert.Equal(t, 1, registry.Count(), "Should have 1 subscriber left")

	// Verify the right subscriber was removed
	_, err = registry.Get("10000000-0000-0000-0000-000000000001")
	assert.True(t, errors.Is(err, ErrSubscriberNotFound), "First subscriber should be removed")

	// Verify the active subscriber is still there
	remainingSub, err := registry.Get("20000000-0000-0000-0000-000000000002")
	assert.NoError(t, err)
	assert.NotNil(t, remainingSub)
}

// Mock subscriber for broadcast testing
type mockDeliverySubscriber struct {
	subscriber     *models.Subscriber // Pointer to subscriber
	ReceivedEvents chan outbound.Event
	DeliveryError  error         // Optional error to return on ReceiveEvent
	DeliveryDelay  time.Duration // Optional delay in ReceiveEvent
}

func newMockDeliverySubscriber(config models.SubscriberConfig, bufferSize int) *mockDeliverySubscriber {
	return &mockDeliverySubscriber{
		subscriber:     models.NewSubscriber(config),
		ReceivedEvents: make(chan outbound.Event, bufferSize),
	}
}

// GetID implements the outbound.Subscriber interface
func (m *mockDeliverySubscriber) GetID() string {
	return m.subscriber.GetID()
}

// GetTopics implements the outbound.Subscriber interface
func (m *mockDeliverySubscriber) GetTopics() []string {
	return m.subscriber.GetTopics()
}

// GetFilter implements the outbound.Subscriber interface
func (m *mockDeliverySubscriber) GetFilter() outbound.SubscriberFilter {
	return m.subscriber.GetFilter()
}

// GetState implements the outbound.Subscriber interface
func (m *mockDeliverySubscriber) GetState() outbound.SubscriberState {
	return m.subscriber.GetState()
}

// ReceiveEvent implements the outbound.Subscriber interface
func (m *mockDeliverySubscriber) ReceiveEvent(ctx context.Context, event outbound.Event) error {
	if m.DeliveryDelay > 0 {
		select {
		case <-time.After(m.DeliveryDelay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if m.DeliveryError != nil {
		return m.DeliveryError
	}

	// Successfully received the event, pass it to the ReceivedEvents channel
	select {
	case m.ReceivedEvents <- event:
		// Also send to the real subscriber's EventChannel for proper activity tracking
		select {
		case m.subscriber.EventChannel <- event:
		default:
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// GetRetryInfo implements the outbound.Subscriber interface
func (m *mockDeliverySubscriber) GetRetryInfo() (retryCount int, lastRetryTime time.Time, nextTimeout time.Duration) {
	return m.subscriber.GetRetryInfo()
}

// GetStats implements the outbound.Subscriber interface
func (m *mockDeliverySubscriber) GetStats() outbound.SubscriberStats {
	return m.subscriber.GetStats()
}

// Close implements the outbound.Subscriber interface
func (m *mockDeliverySubscriber) Close() error {
	return m.subscriber.Close()
}

// Pause implements the outbound.Subscriber interface
func (m *mockDeliverySubscriber) Pause() error {
	return m.subscriber.Pause()
}

// Resume implements the outbound.Subscriber interface
func (m *mockDeliverySubscriber) Resume() error {
	return m.subscriber.Resume()
}

// UpdateFilter implements the outbound.Subscriber interface
func (m *mockDeliverySubscriber) UpdateFilter(filter outbound.SubscriberFilter) error {
	return m.subscriber.UpdateFilter(filter)
}

// UpdateTimeout implements the outbound.Subscriber interface
func (m *mockDeliverySubscriber) UpdateTimeout(config outbound.TimeoutConfig) error {
	return m.subscriber.UpdateTimeout(config)
}

func TestBroadcastEvent(t *testing.T) {
	registry := NewRegistry()
	buf := bytes.Buffer{}
	logger := NewSubscriberLogger(DEBUG)
	logger.logger = log.New(&buf, "", 0)
	registry.SetLogger(logger)

	// Create mock subscribers
	bufferSize := 10
	sub1Config := models.SubscriberConfig{ID: "550e8400-e29b-41d4-a716-446655440000", Topics: []string{"topicA", "topicB"}}
	sub2Config := models.SubscriberConfig{ID: "6ba7b810-9dad-11d1-80b4-00c04fd430c8", Topics: []string{"topicA"}}
	sub3Config := models.SubscriberConfig{ID: "6ba7b811-9dad-11d1-80b4-00c04fd430c9", Topics: []string{"topicC"}} // Different topic
	sub4Config := models.SubscriberConfig{ID: "6ba7b812-9dad-11d1-80b4-00c04fd430ca", Topics: []string{"topicA"}}

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
	assert.Contains(t, output, "\"subscriber_count\":3") // 3 total (sub1, sub2, sub3) - sub4 is paused
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

	allEventsConfig := models.SubscriberConfig{ID: "11111111-1111-1111-1111-111111111111", Topics: []string{"test-topic"}}
	typeFilterConfig := models.SubscriberConfig{ID: "22222222-2222-2222-2222-222222222222", Topics: []string{"test-topic"},
		Filter: models.SubscriberFilter{EventTypes: []string{"TestEvent", "KeepEvent"}},
	}
	versionFilterConfig := models.SubscriberConfig{ID: "33333333-3333-3333-3333-333333333333", Topics: []string{"test-topic"},
		Filter: models.SubscriberFilter{FromVersion: 5},
	}
	pausedConfig := models.SubscriberConfig{ID: "44444444-4444-4444-4444-444444444444", Topics: []string{"test-topic"}}

	// Use mock subscribers for controlled event checking
	allEventsChannel := make(chan outbound.Event, 5)
	typeFilterChannel := make(chan outbound.Event, 5)
	versionFilterChannel := make(chan outbound.Event, 5)

	mockSubAll := &mockDeliverySubscriber{subscriber: models.NewSubscriber(allEventsConfig), ReceivedEvents: allEventsChannel}
	mockSubType := &mockDeliverySubscriber{subscriber: models.NewSubscriber(typeFilterConfig), ReceivedEvents: typeFilterChannel}
	mockSubVersion := &mockDeliverySubscriber{subscriber: models.NewSubscriber(versionFilterConfig), ReceivedEvents: versionFilterChannel}
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

			// Use UUID v4 format IDs
			config := models.SubscriberConfig{
				ID:     fmt.Sprintf("a%d000000-0000-0000-0000-00000000000%d", id, id),
				Topics: []string{"topic1", "topic2"},
			}
			_, err := registry.Register(config)
			assert.NoError(t, err)
		}(i)
	}
	close(startSignal) // Signal workers to start
	wg.Wait()

	// Verify registration count
	assert.Equal(t, numSubscribers, registry.Count())

	// Concurrent update
	wg.Add(numSubscribers)
	for i := 0; i < numSubscribers; i++ {
		go func(id int) {
			defer wg.Done()
			subscriberID := fmt.Sprintf("a%d000000-0000-0000-0000-00000000000%d", id, id)
			err := registry.Update(subscriberID, func(s outbound.Subscriber) error {
				// Update filter to add event type
				filter := s.GetFilter()
				filter.EventTypes = append(filter.EventTypes, fmt.Sprintf("type-%d", id))
				return s.UpdateFilter(filter)
			})
			assert.NoError(t, err)
		}(i)
	}
	wg.Wait()

	// Verify each subscriber has been updated
	for i := 0; i < numSubscribers; i++ {
		subscriberID := fmt.Sprintf("a%d000000-0000-0000-0000-00000000000%d", i, i)
		sub, err := registry.Get(subscriberID)
		assert.NoError(t, err)
		assert.NotNil(t, sub)

		// Check the filter has been updated
		filter := sub.GetFilter()
		assert.Contains(t, filter.EventTypes, fmt.Sprintf("type-%d", i))
	}
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

// mockEvent implements the Event interface for testing
type mockEvent struct {
	id    string
	topic string
}

func (e *mockEvent) GetID() string {
	return e.id
}

func (e *mockEvent) GetType() string {
	return "mock"
}

func (e *mockEvent) GetTopic() string {
	return e.topic
}

func (e *mockEvent) GetData() []byte {
	return []byte(`{"test":"data"}`)
}

func (e *mockEvent) GetMetadata() map[string]string {
	return map[string]string{"test": "metadata"}
}

// DeliverToTopic delivers an event to all subscribers of a specific topic
func (r *Registry) DeliverToTopic(ctx context.Context, topic string, event interface{}) (int, error) {
	// Create or convert to an outbound.Event
	var outEvent outbound.Event
	if e, ok := event.(outbound.Event); ok {
		outEvent = e
	} else if me, ok := event.(*mockEvent); ok {
		// Convert string metadata to interface{} metadata
		metadata := make(map[string]interface{})
		for k, v := range me.GetMetadata() {
			metadata[k] = v
		}

		// Parse the bytes to a map for the Data field
		var data map[string]interface{}
		if err := json.Unmarshal(me.GetData(), &data); err != nil {
			// Use a default map if unmarshal fails
			data = map[string]interface{}{"raw_data": string(me.GetData())}
		}

		outEvent = outbound.Event{
			ID:        me.GetID(),
			Topic:     me.GetTopic(),
			Type:      me.GetType(),
			Data:      data,
			Metadata:  metadata,
			Timestamp: time.Now().UnixNano(),
			Version:   1, // Default version
		}
	} else {
		return 0, fmt.Errorf("unsupported event type: %T", event)
	}

	return r.BroadcastEvent(ctx, outEvent), nil
}

func TestDeliverToTopic(t *testing.T) {
	// Create a new registry
	registry := NewRegistry()

	// Create channels for mock subscribers
	sub1Channel := make(chan outbound.Event, 5)
	sub2Channel := make(chan outbound.Event, 5)
	sub3Channel := make(chan outbound.Event, 5)

	// Register a subscriber for topic1
	sub1Config := models.SubscriberConfig{
		ID:     "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
		Topics: []string{"topic1"},
	}
	mockSub1 := &mockDeliverySubscriber{
		subscriber:     models.NewSubscriber(sub1Config),
		ReceivedEvents: sub1Channel,
	}

	// Register a subscriber for topic2
	sub2Config := models.SubscriberConfig{
		ID:     "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
		Topics: []string{"topic2"},
	}
	mockSub2 := &mockDeliverySubscriber{
		subscriber:     models.NewSubscriber(sub2Config),
		ReceivedEvents: sub2Channel,
	}

	// Register a subscriber for both topic1 and topic2
	sub3Config := models.SubscriberConfig{
		ID:     "cccccccc-cccc-cccc-cccc-cccccccccccc",
		Topics: []string{"topic1", "topic2"},
	}
	mockSub3 := &mockDeliverySubscriber{
		subscriber:     models.NewSubscriber(sub3Config),
		ReceivedEvents: sub3Channel,
	}

	// Add mocks directly to registry
	registry.subscribers[mockSub1.GetID()] = mockSub1
	registry.addToTopicMapsLocked(mockSub1)
	registry.subscribers[mockSub2.GetID()] = mockSub2
	registry.addToTopicMapsLocked(mockSub2)
	registry.subscribers[mockSub3.GetID()] = mockSub3
	registry.addToTopicMapsLocked(mockSub3)

	// Create test events
	event1 := &mockEvent{id: "event1", topic: "topic1"}
	event2 := &mockEvent{id: "event2", topic: "topic2"}
	event3 := &mockEvent{id: "event3", topic: "topic3"} // No subscribers for this topic

	// Test delivering to topic1
	ctx := context.Background()
	count, err := registry.DeliverToTopic(ctx, "topic1", event1)
	require.NoError(t, err)
	assert.Equal(t, 2, count) // Both mockSub1 and mockSub3 should receive it

	// Verify sub1 received event1
	select {
	case received := <-mockSub1.ReceivedEvents:
		assert.Equal(t, "event1", received.ID)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Timeout waiting for sub1 to receive event")
	}

	// Verify sub3 received event1
	select {
	case received := <-mockSub3.ReceivedEvents:
		assert.Equal(t, "event1", received.ID)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Timeout waiting for sub3 to receive event")
	}

	// Verify sub2 did not receive event1
	select {
	case <-mockSub2.ReceivedEvents:
		t.Fatal("sub2 should not receive event1")
	case <-time.After(10 * time.Millisecond):
		// This is expected - no event for sub2
	}

	// Test delivering to topic2
	count, err = registry.DeliverToTopic(ctx, "topic2", event2)
	require.NoError(t, err)
	assert.Equal(t, 2, count) // Both mockSub2 and mockSub3 should receive it

	// Test delivering to topic with no subscribers
	count, err = registry.DeliverToTopic(ctx, "topic3", event3)
	require.NoError(t, err)
	assert.Equal(t, 0, count) // No subscribers for topic3
}
