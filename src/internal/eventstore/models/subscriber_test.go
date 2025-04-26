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

// TestTimeoutConfig tests the timeout configuration
func TestTimeoutConfig(t *testing.T) {
	// Test DefaultTimeoutConfig
	defaultConfig := DefaultTimeoutConfig()
	if defaultConfig.InitialTimeout != 1*time.Second {
		t.Errorf("Expected InitialTimeout to be 1s, got %v", defaultConfig.InitialTimeout)
	}
	if defaultConfig.MaxTimeout != 30*time.Second {
		t.Errorf("Expected MaxTimeout to be 30s, got %v", defaultConfig.MaxTimeout)
	}
	if defaultConfig.BackoffMultiplier != 2.0 {
		t.Errorf("Expected BackoffMultiplier to be 2.0, got %v", defaultConfig.BackoffMultiplier)
	}
	if defaultConfig.MaxRetries != 5 {
		t.Errorf("Expected MaxRetries to be 5, got %v", defaultConfig.MaxRetries)
	}
	if defaultConfig.CooldownPeriod != 1*time.Minute {
		t.Errorf("Expected CooldownPeriod to be 1m, got %v", defaultConfig.CooldownPeriod)
	}
	if defaultConfig.CurrentTimeout != 1*time.Second {
		t.Errorf("Expected CurrentTimeout to be 1s, got %v", defaultConfig.CurrentTimeout)
	}
	if defaultConfig.CurrentRetryCount != 0 {
		t.Errorf("Expected CurrentRetryCount to be 0, got %v", defaultConfig.CurrentRetryCount)
	}

	// Test TimeoutConfigFromOutbound
	outboundConfig := outbound.TimeoutConfig{
		InitialTimeout:    2 * time.Second,
		MaxTimeout:        20 * time.Second,
		BackoffMultiplier: 1.5,
		MaxRetries:        3,
		CooldownPeriod:    30 * time.Second,
	}
	config := TimeoutConfigFromOutbound(outboundConfig)
	if config.InitialTimeout != 2*time.Second {
		t.Errorf("Expected InitialTimeout to be 2s, got %v", config.InitialTimeout)
	}
	if config.MaxTimeout != 20*time.Second {
		t.Errorf("Expected MaxTimeout to be 20s, got %v", config.MaxTimeout)
	}
	if config.BackoffMultiplier != 1.5 {
		t.Errorf("Expected BackoffMultiplier to be 1.5, got %v", config.BackoffMultiplier)
	}
	if config.MaxRetries != 3 {
		t.Errorf("Expected MaxRetries to be 3, got %v", config.MaxRetries)
	}
	if config.CooldownPeriod != 30*time.Second {
		t.Errorf("Expected CooldownPeriod to be 30s, got %v", config.CooldownPeriod)
	}
	if config.CurrentTimeout != 2*time.Second {
		t.Errorf("Expected CurrentTimeout to be 2s, got %v", config.CurrentTimeout)
	}
	if config.CurrentRetryCount != 0 {
		t.Errorf("Expected CurrentRetryCount to be 0, got %v", config.CurrentRetryCount)
	}

	// Test ToOutboundTimeoutConfig
	outboundConverted := config.ToOutboundTimeoutConfig()
	if outboundConverted.InitialTimeout != 2*time.Second {
		t.Errorf("Expected InitialTimeout to be 2s, got %v", outboundConverted.InitialTimeout)
	}
	if outboundConverted.MaxTimeout != 20*time.Second {
		t.Errorf("Expected MaxTimeout to be 20s, got %v", outboundConverted.MaxTimeout)
	}
	if outboundConverted.BackoffMultiplier != 1.5 {
		t.Errorf("Expected BackoffMultiplier to be 1.5, got %v", outboundConverted.BackoffMultiplier)
	}
	if outboundConverted.MaxRetries != 3 {
		t.Errorf("Expected MaxRetries to be 3, got %v", outboundConverted.MaxRetries)
	}
	if outboundConverted.CooldownPeriod != 30*time.Second {
		t.Errorf("Expected CooldownPeriod to be 30s, got %v", outboundConverted.CooldownPeriod)
	}
}

// TestSubscriberRetryMechanism tests the retry mechanism
func TestSubscriberRetryMechanism(t *testing.T) {
	// Create a subscriber with custom timeout config
	config := SubscriberConfig{
		ID:     "test-retry-subscriber",
		Topics: []string{"test-topic"},
		Timeout: TimeoutConfig{
			InitialTimeout:    100 * time.Millisecond,
			MaxTimeout:        500 * time.Millisecond,
			BackoffMultiplier: 2.0,
			MaxRetries:        3,
			CooldownPeriod:    200 * time.Millisecond,
			CurrentTimeout:    100 * time.Millisecond,
			CurrentRetryCount: 0,
		},
	}
	subscriber := NewSubscriber(config)

	// Test initial state
	if subscriber.Timeout.CurrentRetryCount != 0 {
		t.Errorf("Expected initial CurrentRetryCount to be 0, got %d", subscriber.Timeout.CurrentRetryCount)
	}
	if subscriber.Timeout.CurrentTimeout != 100*time.Millisecond {
		t.Errorf("Expected initial CurrentTimeout to be 100ms, got %v", subscriber.Timeout.CurrentTimeout)
	}

	// Test handleRetry
	subscriber.handleRetry()
	if subscriber.Timeout.CurrentRetryCount != 1 {
		t.Errorf("Expected CurrentRetryCount to be 1 after first retry, got %d", subscriber.Timeout.CurrentRetryCount)
	}
	if subscriber.Timeout.CurrentTimeout != 200*time.Millisecond {
		t.Errorf("Expected CurrentTimeout to be 200ms after first retry, got %v", subscriber.Timeout.CurrentTimeout)
	}

	// Test second retry
	subscriber.handleRetry()
	if subscriber.Timeout.CurrentRetryCount != 2 {
		t.Errorf("Expected CurrentRetryCount to be 2 after second retry, got %d", subscriber.Timeout.CurrentRetryCount)
	}
	if subscriber.Timeout.CurrentTimeout != 400*time.Millisecond {
		t.Errorf("Expected CurrentTimeout to be 400ms after second retry, got %v", subscriber.Timeout.CurrentTimeout)
	}

	// Test third retry (should calculate next timeout based on backoff)
	subscriber.handleRetry()
	if subscriber.Timeout.CurrentRetryCount != 3 {
		t.Errorf("Expected CurrentRetryCount to be 3 after third retry, got %d", subscriber.Timeout.CurrentRetryCount)
	}
	expectedTimeout := 500 * time.Millisecond // 400ms * 2.0 = 800ms, but capped at MaxTimeout (500ms)
	if subscriber.Timeout.CurrentTimeout != expectedTimeout {
		t.Errorf("Expected CurrentTimeout to be %v after third retry, got %v", expectedTimeout, subscriber.Timeout.CurrentTimeout)
	}

	// Test fourth retry (should still be at max retries count)
	subscriber.handleRetry()
	if subscriber.Timeout.CurrentRetryCount != 4 {
		t.Errorf("Expected CurrentRetryCount to be 4 after fourth retry, got %d", subscriber.Timeout.CurrentRetryCount)
	}

	// Test resetRetryCount
	subscriber.resetRetryCount()
	if subscriber.Timeout.CurrentRetryCount != 0 {
		t.Errorf("Expected CurrentRetryCount to be 0 after reset, got %d", subscriber.Timeout.CurrentRetryCount)
	}
	if subscriber.Timeout.CurrentTimeout != 100*time.Millisecond {
		t.Errorf("Expected CurrentTimeout to be reset to 100ms, got %v", subscriber.Timeout.CurrentTimeout)
	}
}

// TestSubscriberCooldown tests the cooldown mechanism
func TestSubscriberCooldown(t *testing.T) {
	// Create a subscriber with custom timeout config with short cooldown for testing
	config := SubscriberConfig{
		ID:     "test-cooldown-subscriber",
		Topics: []string{"test-topic"},
		Timeout: TimeoutConfig{
			InitialTimeout:    50 * time.Millisecond,
			MaxTimeout:        200 * time.Millisecond,
			BackoffMultiplier: 2.0,
			MaxRetries:        2,                      // Just 2 retries for faster test
			CooldownPeriod:    100 * time.Millisecond, // Short cooldown period for testing
			CurrentTimeout:    50 * time.Millisecond,
			CurrentRetryCount: 0,
		},
	}
	subscriber := NewSubscriber(config)

	// Simulate reaching max retries
	subscriber.handleRetry() // retry 1
	subscriber.handleRetry() // retry 2

	// Should now be at max retries
	if subscriber.Timeout.CurrentRetryCount != 2 {
		t.Errorf("Expected CurrentRetryCount to be 2, got %d", subscriber.Timeout.CurrentRetryCount)
	}

	// Should be in cooldown
	if !subscriber.IsInCooldown() {
		t.Error("Expected subscriber to be in cooldown after max retries")
	}

	// Should not retry during cooldown
	if subscriber.ShouldRetry() {
		t.Error("Expected ShouldRetry to return false during cooldown")
	}

	// Wait for cooldown to elapse
	time.Sleep(150 * time.Millisecond)

	// Should no longer be in cooldown
	if subscriber.IsInCooldown() {
		t.Error("Expected subscriber to not be in cooldown after cooldown period")
	}

	// Should be able to retry again
	if !subscriber.ShouldRetry() {
		t.Error("Expected ShouldRetry to return true after cooldown period")
	}

	// Test resetAfterCooldown
	reset := subscriber.resetAfterCooldown()
	if !reset {
		t.Error("Expected resetAfterCooldown to return true after cooldown period")
	}

	// Retry count and timeout should be reset
	if subscriber.Timeout.CurrentRetryCount != 0 {
		t.Errorf("Expected CurrentRetryCount to be 0 after cooldown reset, got %d", subscriber.Timeout.CurrentRetryCount)
	}
	if subscriber.Timeout.CurrentTimeout != 50*time.Millisecond {
		t.Errorf("Expected CurrentTimeout to be reset to 50ms, got %v", subscriber.Timeout.CurrentTimeout)
	}
}

// TestReceiveEventWithTimeout tests event delivery with timeout
func TestReceiveEventWithTimeout(t *testing.T) {
	// Create a subscriber with custom timeout config
	config := SubscriberConfig{
		ID:     "test-receive-subscriber",
		Topics: []string{"test-topic"},
		Timeout: TimeoutConfig{
			InitialTimeout:    50 * time.Millisecond,
			MaxTimeout:        200 * time.Millisecond,
			BackoffMultiplier: 2.0,
			MaxRetries:        2,
			CooldownPeriod:    100 * time.Millisecond,
			CurrentTimeout:    50 * time.Millisecond,
			CurrentRetryCount: 0,
		},
		BufferSize: 1, // Small buffer to test backpressure
	}
	subscriber := NewSubscriber(config)

	// Create an event to test with
	event := outbound.Event{
		ID:        "test-event",
		Topic:     "test-topic",
		Type:      "TestEvent",
		Data:      map[string]interface{}{"key": "value"},
		Timestamp: time.Now().UnixNano(),
		Version:   1,
	}

	// Test successful delivery
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	err := subscriber.ReceiveEvent(ctx, event)
	cancel()
	if err != nil {
		t.Errorf("Expected successful delivery, got error: %v", err)
	}

	// Manually drain the event we just sent to ensure buffer is empty
	<-subscriber.EventChannel

	// Fill the buffer to test timeout
	err = subscriber.ReceiveEvent(context.Background(), event)
	if err != nil {
		t.Errorf("Expected successful delivery for buffer filling, got error: %v", err)
	}

	// This should timeout since buffer is full
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Millisecond)
	err = subscriber.ReceiveEvent(ctx, event)
	cancel()
	if err == nil {
		t.Error("Expected timeout error, got nil")
	}

	// Check that retry count was incremented
	if subscriber.Timeout.CurrentRetryCount != 1 {
		t.Errorf("Expected CurrentRetryCount to be 1 after timeout, got %d", subscriber.Timeout.CurrentRetryCount)
	}
	if subscriber.Timeout.CurrentTimeout != 100*time.Millisecond {
		t.Errorf("Expected CurrentTimeout to be 100ms after timeout, got %v", subscriber.Timeout.CurrentTimeout)
	}

	// Read from buffer to make space
	<-subscriber.EventChannel

	// Successfully deliver again (should reset retry count)
	ctx, cancel = context.WithTimeout(context.Background(), 50*time.Millisecond)
	err = subscriber.ReceiveEvent(ctx, event)
	cancel()
	if err != nil {
		t.Errorf("Expected successful delivery after timeout, got error: %v", err)
	}

	// Drain the channel again to avoid leaks
	<-subscriber.EventChannel

	// Check that retry count was reset
	if subscriber.Timeout.CurrentRetryCount != 0 {
		t.Errorf("Expected CurrentRetryCount to be 0 after successful delivery, got %d", subscriber.Timeout.CurrentRetryCount)
	}
	if subscriber.Timeout.CurrentTimeout != 50*time.Millisecond {
		t.Errorf("Expected CurrentTimeout to be reset to 50ms, got %v", subscriber.Timeout.CurrentTimeout)
	}
}

// TestUpdateTimeout tests updating the timeout configuration
func TestUpdateTimeout(t *testing.T) {
	// Create a subscriber with default timeout config
	subscriber := NewSubscriber(SubscriberConfig{
		ID:      "test-update-subscriber",
		Topics:  []string{"test-topic"},
		Timeout: DefaultTimeoutConfig(),
	})

	// Simulate some retries to change the current values
	subscriber.handleRetry()
	subscriber.handleRetry()

	if subscriber.Timeout.CurrentRetryCount != 2 {
		t.Errorf("Expected CurrentRetryCount to be 2 before update, got %d", subscriber.Timeout.CurrentRetryCount)
	}

	// Update timeout configuration
	newConfig := outbound.TimeoutConfig{
		InitialTimeout:    200 * time.Millisecond,
		MaxTimeout:        1 * time.Second,
		BackoffMultiplier: 1.5,
		MaxRetries:        4,
		CooldownPeriod:    500 * time.Millisecond,
	}

	err := subscriber.UpdateTimeout(newConfig)
	if err != nil {
		t.Errorf("Expected UpdateTimeout to succeed, got error: %v", err)
	}

	// Verify that current values were reset
	if subscriber.Timeout.CurrentRetryCount != 0 {
		t.Errorf("Expected CurrentRetryCount to be reset to 0, got %d", subscriber.Timeout.CurrentRetryCount)
	}
	if subscriber.Timeout.CurrentTimeout != 200*time.Millisecond {
		t.Errorf("Expected CurrentTimeout to be reset to 200ms, got %v", subscriber.Timeout.CurrentTimeout)
	}

	// Verify that new configuration was applied
	if subscriber.Timeout.InitialTimeout != 200*time.Millisecond {
		t.Errorf("Expected InitialTimeout to be 200ms, got %v", subscriber.Timeout.InitialTimeout)
	}
	if subscriber.Timeout.MaxTimeout != 1*time.Second {
		t.Errorf("Expected MaxTimeout to be 1s, got %v", subscriber.Timeout.MaxTimeout)
	}
	if subscriber.Timeout.BackoffMultiplier != 1.5 {
		t.Errorf("Expected BackoffMultiplier to be 1.5, got %v", subscriber.Timeout.BackoffMultiplier)
	}
	if subscriber.Timeout.MaxRetries != 4 {
		t.Errorf("Expected MaxRetries to be 4, got %d", subscriber.Timeout.MaxRetries)
	}
	if subscriber.Timeout.CooldownPeriod != 500*time.Millisecond {
		t.Errorf("Expected CooldownPeriod to be 500ms, got %v", subscriber.Timeout.CooldownPeriod)
	}
}

// TestGetRetryInfo tests getting retry information
func TestGetRetryInfo(t *testing.T) {
	// Create a subscriber with custom timeout config
	config := SubscriberConfig{
		ID:     "test-info-subscriber",
		Topics: []string{"test-topic"},
		Timeout: TimeoutConfig{
			InitialTimeout:    50 * time.Millisecond,
			MaxTimeout:        200 * time.Millisecond,
			BackoffMultiplier: 2.0,
			MaxRetries:        3,
			CooldownPeriod:    100 * time.Millisecond,
			CurrentTimeout:    50 * time.Millisecond,
			CurrentRetryCount: 0,
		},
	}
	subscriber := NewSubscriber(config)

	// Initial state
	current, max, timeout := subscriber.GetRetryInfo()
	if current != 0 || max != 3 || timeout != 50*time.Millisecond {
		t.Errorf("Expected (0, 3, 50ms), got (%d, %d, %v)", current, max, timeout)
	}

	// After one retry
	subscriber.handleRetry()
	current, max, timeout = subscriber.GetRetryInfo()
	if current != 1 || max != 3 || timeout != 100*time.Millisecond {
		t.Errorf("Expected (1, 3, 100ms), got (%d, %d, %v)", current, max, timeout)
	}
}
