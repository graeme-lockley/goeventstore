package models

import (
	"context"
	"sync"
	"testing"
	"time"

	"goeventsource/src/internal/port/outbound"
)

// TestSubscriberImplementsInterface verifies that the Subscriber struct implements the outbound.Subscriber interface
func TestSubscriberImplementsInterface(t *testing.T) {
	var _ outbound.Subscriber = (*Subscriber)(nil)
}

// TestNewSubscriber tests creating a new subscriber
func TestNewSubscriber(t *testing.T) {
	// Create a subscriber with explicit configuration
	config := SubscriberConfig{
		ID:     "test-subscriber-1",
		Topics: []string{"topic1", "topic2"},
		Filter: SubscriberFilter{
			EventTypes:  []string{"EventType1"},
			FromVersion: 10,
			Metadata: map[string]interface{}{
				"test": "value",
			},
		},
		Timeout: TimeoutConfig{
			InitialTimeout:    2 * time.Second,
			MaxTimeout:        20 * time.Second,
			BackoffMultiplier: 1.5,
			MaxRetries:        3,
			CooldownPeriod:    30 * time.Second,
			CurrentTimeout:    2 * time.Second,
			CurrentRetryCount: 0,
		},
		BufferSize: 50,
	}

	sub := NewSubscriber(config)

	// Verify subscriber properties
	if sub.ID != "test-subscriber-1" {
		t.Errorf("Expected ID to be 'test-subscriber-1', got %s", sub.ID)
	}

	if len(sub.Topics) != 2 {
		t.Errorf("Expected 2 topics, got %d", len(sub.Topics))
	}

	if len(sub.Filter.EventTypes) != 1 {
		t.Errorf("Expected 1 event type, got %d", len(sub.Filter.EventTypes))
	}

	if sub.Filter.FromVersion != 10 {
		t.Errorf("Expected FromVersion to be 10, got %d", sub.Filter.FromVersion)
	}

	if sub.Timeout.InitialTimeout != 2*time.Second {
		t.Errorf("Expected InitialTimeout to be 2s, got %v", sub.Timeout.InitialTimeout)
	}

	if sub.Timeout.MaxRetries != 3 {
		t.Errorf("Expected MaxRetries to be 3, got %d", sub.Timeout.MaxRetries)
	}

	if sub.BufferSize != 50 {
		t.Errorf("Expected BufferSize to be 50, got %d", sub.BufferSize)
	}

	if sub.State != SubscriberStateActive {
		t.Errorf("Expected State to be Active, got %s", sub.State)
	}

	if cap(sub.EventChannel) != 50 {
		t.Errorf("Expected channel capacity to be 50, got %d", cap(sub.EventChannel))
	}

	// Test with default buffer size
	config.BufferSize = 0
	sub = NewSubscriber(config)
	if sub.BufferSize != DefaultBufferSize {
		t.Errorf("Expected BufferSize to be %d, got %d", DefaultBufferSize, sub.BufferSize)
	}
}

// TestDefaultTimeoutConfig tests the default timeout configuration
func TestDefaultTimeoutConfig(t *testing.T) {
	config := DefaultTimeoutConfig()

	if config.InitialTimeout != time.Second {
		t.Errorf("Expected InitialTimeout to be 1s, got %v", config.InitialTimeout)
	}

	if config.MaxTimeout != 30*time.Second {
		t.Errorf("Expected MaxTimeout to be 30s, got %v", config.MaxTimeout)
	}

	if config.BackoffMultiplier != 2.0 {
		t.Errorf("Expected BackoffMultiplier to be 2.0, got %v", config.BackoffMultiplier)
	}

	if config.MaxRetries != 5 {
		t.Errorf("Expected MaxRetries to be 5, got %d", config.MaxRetries)
	}

	if config.CooldownPeriod != time.Minute {
		t.Errorf("Expected CooldownPeriod to be 1m, got %v", config.CooldownPeriod)
	}

	if config.CurrentTimeout != time.Second {
		t.Errorf("Expected CurrentTimeout to be 1s, got %v", config.CurrentTimeout)
	}

	if config.CurrentRetryCount != 0 {
		t.Errorf("Expected CurrentRetryCount to be 0, got %d", config.CurrentRetryCount)
	}
}

// TestSubscriberStateTransitions tests state transitions in the subscriber
func TestSubscriberStateTransitions(t *testing.T) {
	config := SubscriberConfig{
		ID:      "test-subscriber-2",
		Topics:  []string{"topic1"},
		Filter:  SubscriberFilter{},
		Timeout: DefaultTimeoutConfig(),
	}

	sub := NewSubscriber(config)

	// Initial state should be active
	if sub.State != SubscriberStateActive {
		t.Errorf("Expected initial state to be Active, got %s", sub.State)
	}

	// Test pause
	err := sub.Pause()
	if err != nil {
		t.Errorf("Unexpected error on pause: %v", err)
	}
	if sub.State != SubscriberStatePaused {
		t.Errorf("Expected state to be Paused, got %s", sub.State)
	}

	// Test resume
	err = sub.Resume()
	if err != nil {
		t.Errorf("Unexpected error on resume: %v", err)
	}
	if sub.State != SubscriberStateActive {
		t.Errorf("Expected state to be Active, got %s", sub.State)
	}

	// Test close
	err = sub.Close()
	if err != nil {
		t.Errorf("Unexpected error on close: %v", err)
	}
	if sub.State != SubscriberStateClosed {
		t.Errorf("Expected state to be Closed, got %s", sub.State)
	}

	// Verify that pause and resume do nothing on a closed subscriber
	prevTime := sub.LastActivityAt
	time.Sleep(1 * time.Millisecond) // Ensure time difference

	err = sub.Pause()
	if err != nil {
		t.Errorf("Unexpected error on pause of closed subscriber: %v", err)
	}
	if sub.State != SubscriberStateClosed {
		t.Errorf("Expected state to remain Closed, got %s", sub.State)
	}

	err = sub.Resume()
	if err != nil {
		t.Errorf("Unexpected error on resume of closed subscriber: %v", err)
	}
	if sub.State != SubscriberStateClosed {
		t.Errorf("Expected state to remain Closed, got %s", sub.State)
	}

	// LastActivityAt should have been updated for a closed subscriber
	if sub.LastActivityAt.Equal(prevTime) {
		t.Errorf("Expected LastActivityAt to be updated, but it remained unchanged")
	}
}

// TestSubscriberEventDelivery tests event delivery to the subscriber
func TestSubscriberEventDelivery(t *testing.T) {
	config := SubscriberConfig{
		ID:         "test-subscriber-3",
		Topics:     []string{"topic1"},
		Filter:     SubscriberFilter{},
		Timeout:    DefaultTimeoutConfig(),
		BufferSize: 5,
	}

	sub := NewSubscriber(config)

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// Create test event
	event := outbound.Event{
		ID:        "event-1",
		Topic:     "topic1",
		Type:      "TestEvent",
		Data:      map[string]interface{}{"key": "value"},
		Metadata:  map[string]interface{}{},
		Timestamp: time.Now().UnixNano(),
		Version:   1,
	}

	// Send event to the subscriber
	err := sub.ReceiveEvent(ctx, event)
	if err != nil {
		t.Errorf("Unexpected error on event delivery: %v", err)
	}

	// Verify that event was delivered by checking it's in the channel
	select {
	case receivedEvent := <-sub.EventChannel:
		if receivedEvent.ID != event.ID {
			t.Errorf("Expected event ID %s, got %s", event.ID, receivedEvent.ID)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Timed out waiting for event")
	}

	// Verify stats were updated
	stats := sub.GetStats()
	if stats.EventsDelivered != 1 {
		t.Errorf("Expected EventsDelivered to be 1, got %d", stats.EventsDelivered)
	}

	// Test delivery to paused subscriber
	sub.Pause()
	err = sub.ReceiveEvent(ctx, event)
	if err != nil {
		t.Errorf("Unexpected error on event delivery to paused subscriber: %v", err)
	}

	// Verify stats show dropped event
	stats = sub.GetStats()
	if stats.EventsDropped != 1 {
		t.Errorf("Expected EventsDropped to be 1, got %d", stats.EventsDropped)
	}

	// Test delivery timeout
	sub.Resume()

	// Fill the channel
	for i := 0; i < 5; i++ {
		sub.EventChannel <- event
	}

	// Create a short timeout
	shortCtx, shortCancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer shortCancel()

	// This should timeout as the channel is full
	err = sub.ReceiveEvent(shortCtx, event)
	if err == nil {
		t.Error("Expected timeout error, got nil")
	}

	// Verify timeout stats
	stats = sub.GetStats()
	if stats.TimeoutCount != 1 {
		t.Errorf("Expected TimeoutCount to be 1, got %d", stats.TimeoutCount)
	}
	if stats.ErrorCount != 1 {
		t.Errorf("Expected ErrorCount to be 1, got %d", stats.ErrorCount)
	}
}

// TestSubscriberConcurrency tests concurrent operations on a subscriber
func TestSubscriberConcurrency(t *testing.T) {
	config := SubscriberConfig{
		ID:         "test-subscriber-4",
		Topics:     []string{"topic1"},
		Filter:     SubscriberFilter{},
		Timeout:    DefaultTimeoutConfig(),
		BufferSize: 100,
	}

	sub := NewSubscriber(config)

	// Create a consumer goroutine that reads events
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		eventsRead := 0
		for range sub.EventChannel {
			eventsRead++
			// Read up to 50 events
			if eventsRead >= 50 {
				return
			}
		}
	}()

	// Send events concurrently from multiple goroutines
	const numGoroutines = 5
	const eventsPerGoroutine = 10

	var producerWg sync.WaitGroup
	producerWg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer producerWg.Done()

			for j := 0; j < eventsPerGoroutine; j++ {
				event := outbound.Event{
					ID:        "event-concurrent",
					Topic:     "topic1",
					Type:      "TestEvent",
					Data:      map[string]interface{}{"goroutine": goroutineID, "event": j},
					Metadata:  map[string]interface{}{},
					Timestamp: time.Now().UnixNano(),
					Version:   int64(goroutineID*100 + j),
				}

				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				sub.ReceiveEvent(ctx, event)
				cancel()
			}
		}(i)
	}

	// Wait for producers to finish
	producerWg.Wait()

	// Close subscriber to signal consumer to exit if not done already
	sub.Close()

	// Wait for consumer to finish
	wg.Wait()

	// Verify stats
	stats := sub.GetStats()
	if stats.EventsDelivered != numGoroutines*eventsPerGoroutine {
		t.Errorf("Expected EventsDelivered to be %d, got %d",
			numGoroutines*eventsPerGoroutine, stats.EventsDelivered)
	}
}

// TestTimeoutConfigConversion tests converting between internal and outbound timeout config
func TestTimeoutConfigConversion(t *testing.T) {
	internal := TimeoutConfig{
		InitialTimeout:    2 * time.Second,
		MaxTimeout:        20 * time.Second,
		BackoffMultiplier: 1.5,
		MaxRetries:        3,
		CooldownPeriod:    30 * time.Second,
		CurrentTimeout:    3 * time.Second,
		CurrentRetryCount: 1,
		LastRetryTime:     time.Now(),
	}

	// Convert to outbound
	outboundConfig := internal.ToOutboundTimeoutConfig()

	// Verify conversion preserved the expected fields
	if outboundConfig.InitialTimeout != internal.InitialTimeout {
		t.Errorf("Expected InitialTimeout to be %v, got %v", internal.InitialTimeout, outboundConfig.InitialTimeout)
	}

	if outboundConfig.MaxTimeout != internal.MaxTimeout {
		t.Errorf("Expected MaxTimeout to be %v, got %v", internal.MaxTimeout, outboundConfig.MaxTimeout)
	}

	if outboundConfig.BackoffMultiplier != internal.BackoffMultiplier {
		t.Errorf("Expected BackoffMultiplier to be %v, got %v", internal.BackoffMultiplier, outboundConfig.BackoffMultiplier)
	}

	if outboundConfig.MaxRetries != internal.MaxRetries {
		t.Errorf("Expected MaxRetries to be %d, got %d", internal.MaxRetries, outboundConfig.MaxRetries)
	}

	if outboundConfig.CooldownPeriod != internal.CooldownPeriod {
		t.Errorf("Expected CooldownPeriod to be %v, got %v", internal.CooldownPeriod, outboundConfig.CooldownPeriod)
	}

	// Convert back to internal and verify
	roundTripped := TimeoutConfigFromOutbound(outboundConfig)

	if roundTripped.InitialTimeout != internal.InitialTimeout {
		t.Errorf("Expected InitialTimeout to be %v, got %v", internal.InitialTimeout, roundTripped.InitialTimeout)
	}

	if roundTripped.MaxTimeout != internal.MaxTimeout {
		t.Errorf("Expected MaxTimeout to be %v, got %v", internal.MaxTimeout, roundTripped.MaxTimeout)
	}

	if roundTripped.BackoffMultiplier != internal.BackoffMultiplier {
		t.Errorf("Expected BackoffMultiplier to be %v, got %v", internal.BackoffMultiplier, roundTripped.BackoffMultiplier)
	}

	if roundTripped.MaxRetries != internal.MaxRetries {
		t.Errorf("Expected MaxRetries to be %d, got %d", internal.MaxRetries, roundTripped.MaxRetries)
	}

	if roundTripped.CooldownPeriod != internal.CooldownPeriod {
		t.Errorf("Expected CooldownPeriod to be %v, got %v", internal.CooldownPeriod, roundTripped.CooldownPeriod)
	}

	// Fields that should be reset
	if roundTripped.CurrentRetryCount != 0 {
		t.Errorf("Expected CurrentRetryCount to be reset to 0, got %d", roundTripped.CurrentRetryCount)
	}

	if !roundTripped.LastRetryTime.IsZero() {
		t.Errorf("Expected LastRetryTime to be reset to zero time, got %v", roundTripped.LastRetryTime)
	}
}

// TestSubscriberFilterConversion tests converting between internal and outbound subscriber filters
func TestSubscriberFilterConversion(t *testing.T) {
	internal := SubscriberFilter{
		EventTypes:  []string{"Type1", "Type2"},
		FromVersion: 42,
		Metadata: map[string]interface{}{
			"key1": "value1",
			"key2": 123,
		},
	}

	// Convert to outbound
	outboundFilter := internal.ToOutboundFilter()

	// Verify conversion
	if len(outboundFilter.EventTypes) != len(internal.EventTypes) {
		t.Errorf("Expected %d event types, got %d", len(internal.EventTypes), len(outboundFilter.EventTypes))
	}

	for i, eventType := range internal.EventTypes {
		if outboundFilter.EventTypes[i] != eventType {
			t.Errorf("Expected event type %s, got %s", eventType, outboundFilter.EventTypes[i])
		}
	}

	if outboundFilter.FromVersion != internal.FromVersion {
		t.Errorf("Expected FromVersion to be %d, got %d", internal.FromVersion, outboundFilter.FromVersion)
	}

	if len(outboundFilter.Metadata) != len(internal.Metadata) {
		t.Errorf("Expected %d metadata entries, got %d", len(internal.Metadata), len(outboundFilter.Metadata))
	}

	for k, v := range internal.Metadata {
		if outboundFilter.Metadata[k] != v {
			t.Errorf("Expected metadata %s to be %v, got %v", k, v, outboundFilter.Metadata[k])
		}
	}

	// Convert back to internal
	roundTripped := SubscriberFilterFromOutbound(outboundFilter)

	if len(roundTripped.EventTypes) != len(internal.EventTypes) {
		t.Errorf("Expected %d event types after round trip, got %d",
			len(internal.EventTypes), len(roundTripped.EventTypes))
	}

	for i, eventType := range internal.EventTypes {
		if roundTripped.EventTypes[i] != eventType {
			t.Errorf("Expected event type %s after round trip, got %s",
				eventType, roundTripped.EventTypes[i])
		}
	}

	if roundTripped.FromVersion != internal.FromVersion {
		t.Errorf("Expected FromVersion to be %d after round trip, got %d",
			internal.FromVersion, roundTripped.FromVersion)
	}

	if len(roundTripped.Metadata) != len(internal.Metadata) {
		t.Errorf("Expected %d metadata entries after round trip, got %d",
			len(internal.Metadata), len(roundTripped.Metadata))
	}

	for k, v := range internal.Metadata {
		if roundTripped.Metadata[k] != v {
			t.Errorf("Expected metadata %s to be %v after round trip, got %v",
				k, v, roundTripped.Metadata[k])
		}
	}
}
