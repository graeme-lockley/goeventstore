package subscribers

import (
	"testing"
	"time"

	"goeventsource/src/internal/port/outbound"

	"github.com/stretchr/testify/assert"
)

// getEventChannel is a helper function to cast the ChannelWrapper back to *EventChannel
func getEventChannel(wrapper interface{}) *EventChannel {
	if ec, ok := wrapper.(*EventChannel); ok {
		return ec
	}
	return nil
}

func TestChannelFactoryCreation(t *testing.T) {
	// Test with valid buffer size
	factory := NewChannelFactory(200, nil)
	assert.NotNil(t, factory)
	assert.Equal(t, 200, factory.defaultBufferSize)
	assert.Empty(t, factory.channels)

	// Test with invalid buffer size (should use default)
	factory = NewChannelFactory(0, nil)
	assert.NotNil(t, factory)
	assert.Equal(t, 100, factory.defaultBufferSize)
	assert.Empty(t, factory.channels)
}

func TestChannelCreation(t *testing.T) {
	factory := NewChannelFactory(100, nil)

	// Create a new channel
	channel := factory.CreateChannel("test-subscriber", 50)
	ec := getEventChannel(channel)
	assert.NotNil(t, ec)
	assert.Equal(t, 50, ec.Metadata.BufferSize)
	assert.Equal(t, "test-subscriber", ec.Metadata.SubscriberID)
	assert.Equal(t, 1, factory.GetChannelCount())

	// Try to get the same channel
	existingChannel := factory.GetChannel("test-subscriber", 200)
	ec2 := getEventChannel(existingChannel)
	assert.Same(t, ec, ec2)
	assert.Equal(t, 50, ec2.Metadata.BufferSize) // Should retain original buffer size
	assert.Equal(t, 1, factory.GetChannelCount())

	// Create a new channel with default buffer size
	channel2 := factory.CreateChannel("test-subscriber-2", 0)
	ec3 := getEventChannel(channel2)
	assert.NotNil(t, ec3)
	assert.Equal(t, 100, ec3.Metadata.BufferSize) // Should use factory default
	assert.Equal(t, 2, factory.GetChannelCount())
}

func TestChannelResize(t *testing.T) {
	factory := NewChannelFactory(100, nil)

	// Create a channel with small buffer size
	channel := getEventChannel(factory.CreateChannel("test-subscriber", 5))
	assert.Equal(t, 5, channel.Metadata.BufferSize)

	// Create test event
	event := outbound.Event{
		ID:   "test-event",
		Type: "test",
	}

	// Fill the channel with events
	for i := 0; i < 3; i++ {
		success := channel.TrySend(event)
		assert.True(t, success)
	}

	// Test direct resize on EventChannel
	transferred, err := channel.Resize(10)
	assert.NoError(t, err)
	assert.Equal(t, 3, transferred) // 3 events should be transferred
	assert.Equal(t, 10, channel.Metadata.BufferSize)
	assert.Equal(t, 10, channel.Metadata.Capacity)

	// Verify the events are still in the channel
	assert.Equal(t, 3, len(channel.Channel))

	// Test resize via factory
	newSize, transferred, err := factory.ResizeChannel("test-subscriber", 15)
	assert.NoError(t, err)
	assert.Equal(t, 15, newSize)
	assert.Equal(t, 3, transferred) // 3 events should be transferred

	// Verify the channel metadata is updated
	metadata := channel.GetMetadata()
	assert.Equal(t, 15, metadata.BufferSize)
	assert.Equal(t, 15, metadata.Capacity)
	assert.Equal(t, 3, metadata.Usage)

	// Add more events - should now fit in the larger buffer
	for i := 0; i < 10; i++ {
		success := channel.TrySend(event)
		assert.True(t, success)
	}

	// Try to resize with invalid size
	_, err = channel.Resize(0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid buffer size")

	// Try to resize through factory with invalid size
	_, _, err = factory.ResizeChannel("test-subscriber", -5)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid buffer size")

	// Try to resize a non-existent channel
	_, _, err = factory.ResizeChannel("non-existent", 10)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "subscriber channel not found")

	// Close the channel and try to resize it
	channel.Close()
	_, err = channel.Resize(20)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot resize closed channel")
}

func TestChannelSendAndMetadata(t *testing.T) {
	factory := NewChannelFactory(100, nil)
	channel := getEventChannel(factory.CreateChannel("test-subscriber", 5))

	// Create test event
	event := outbound.Event{
		ID:   "test-event",
		Type: "test",
	}

	// Send events successfully
	for i := 0; i < 3; i++ {
		success := channel.TrySend(event)
		assert.True(t, success)
	}

	// Check metadata updates
	metadata := channel.GetMetadata()
	assert.Equal(t, int64(3), metadata.TotalEvents)
	assert.Equal(t, 3, metadata.Usage)
	assert.Equal(t, 0, int(metadata.DroppedEvents))

	// Fill the channel and test overflow
	for i := 0; i < 2; i++ {
		success := channel.TrySend(event)
		assert.True(t, success)
	}

	// Now channel should be full, this should fail
	success := channel.TrySend(event)
	assert.False(t, success)

	// Check dropped events count
	metadata = channel.GetMetadata()
	assert.Equal(t, int64(5), metadata.TotalEvents)
	assert.Equal(t, int64(1), metadata.DroppedEvents)
}

func TestChannelRemoval(t *testing.T) {
	factory := NewChannelFactory(100, nil)
	channel1 := getEventChannel(factory.CreateChannel("subscriber1", 10))
	channel2 := getEventChannel(factory.CreateChannel("subscriber2", 20))

	assert.Equal(t, 2, factory.GetChannelCount())

	// Remove a channel
	factory.RemoveChannel("subscriber1")
	assert.Equal(t, 1, factory.GetChannelCount())

	// Verify channel is closed (use a non-blocking select instead of reading directly)
	select {
	case _, ok := <-channel1.Channel:
		assert.False(t, ok, "Channel should be closed")
	default:
		// If the channel is nil, we can't read from it at all
		assert.Nil(t, channel1.Channel, "Channel should be nil after closing")
	}

	// Other channel should still work
	event := outbound.Event{ID: "test"}
	assert.True(t, channel2.TrySend(event))

	// Remove non-existent channel (no error expected)
	factory.RemoveChannel("non-existent")
	assert.Equal(t, 1, factory.GetChannelCount())
}

func TestChannelHealthCheck(t *testing.T) {
	factory := NewChannelFactory(100, nil)
	factory.CreateChannel("healthy", 10)

	// Test non-existent channel
	metrics, healthy := factory.CheckChannelHealth("non-existent")
	assert.False(t, healthy)
	assert.False(t, metrics["exists"].(bool))

	// Test healthy channel
	metrics, healthy = factory.CheckChannelHealth("healthy")
	assert.True(t, healthy)
	assert.Equal(t, "healthy", metrics["subscriber_id"])
	assert.Equal(t, 0, metrics["usage"])
	assert.Equal(t, "0.00%", metrics["usage_percent"])

	// Create "unhealthy" channel by manipulating its metadata
	unhealthyChannel := getEventChannel(factory.CreateChannel("unhealthy", 10))

	// Force usage to high percentage
	for i := 0; i < 9; i++ {
		unhealthyChannel.TrySend(outbound.Event{ID: "test"})
	}

	// Check health
	metrics, healthy = factory.CheckChannelHealth("unhealthy")
	assert.False(t, healthy)
	assert.Equal(t, 9, metrics["usage"])
	assert.Equal(t, "90.00%", metrics["usage_percent"])
}

func TestCleanupInactiveChannels(t *testing.T) {
	factory := NewChannelFactory(100, nil)
	factory.CreateChannel("active", 10)
	factory.CreateChannel("inactive", 10)

	// Manually set the last activity time for the inactive channel to be in the past
	factory.channels["inactive"].mu.Lock()
	factory.channels["inactive"].Metadata.LastActivity = time.Now().Add(-10 * time.Minute)
	factory.channels["inactive"].mu.Unlock()

	// Run cleanup with 5-minute threshold
	removed := factory.CleanupInactiveChannels(5 * time.Minute)
	assert.Equal(t, 1, removed)
	assert.Equal(t, 1, factory.GetChannelCount())

	// Check that only the active channel remains
	_, exists := factory.channels["active"]
	assert.True(t, exists)
	_, exists = factory.channels["inactive"]
	assert.False(t, exists)
}

func TestChannelListAndHealthSummary(t *testing.T) {
	factory := NewChannelFactory(100, nil)
	factory.CreateChannel("channel1", 10)
	factory.CreateChannel("channel2", 20)

	// Test listing channels
	channels := factory.ListChannels()
	assert.Equal(t, 2, len(channels))

	// Verify we have both channels
	foundChannel1 := false
	foundChannel2 := false
	for _, ch := range channels {
		if ch.SubscriberID == "channel1" {
			foundChannel1 = true
			assert.Equal(t, 10, ch.BufferSize)
		}
		if ch.SubscriberID == "channel2" {
			foundChannel2 = true
			assert.Equal(t, 20, ch.BufferSize)
		}
	}
	assert.True(t, foundChannel1)
	assert.True(t, foundChannel2)

	// Test health summary
	summary := factory.CheckAllChannelsHealth()
	assert.Equal(t, 2, summary["total_channels"])
	assert.Equal(t, 2, summary["healthy_channels"])
	assert.Equal(t, 0, summary["unhealthy_channels"])
	assert.Equal(t, "100.00%", summary["health_percentage"])

	// Make a channel unhealthy
	factory.channels["channel1"].mu.Lock()
	factory.channels["channel1"].Metadata.LastActivity = time.Now().Add(-10 * time.Minute)
	factory.channels["channel1"].mu.Unlock()

	// Check summary again
	summary = factory.CheckAllChannelsHealth()
	assert.Equal(t, 2, summary["total_channels"])
	assert.Equal(t, 1, summary["healthy_channels"])
	assert.Equal(t, 1, summary["unhealthy_channels"])
	assert.Equal(t, "50.00%", summary["health_percentage"])

	// Verify unhealthy details
	unhealthyDetails, ok := summary["unhealthy_details"].([]map[string]interface{})
	assert.True(t, ok)
	assert.Equal(t, 1, len(unhealthyDetails))
	assert.Equal(t, "channel1", unhealthyDetails[0]["subscriber_id"])
}

func TestEventChannelMethods(t *testing.T) {
	// Create a channel directly for testing individual methods
	ec := &EventChannel{
		Channel: make(chan outbound.Event, 5),
		Metadata: ChannelMetadata{
			SubscriberID: "test",
			CreatedAt:    time.Now(),
			LastActivity: time.Now().Add(-1 * time.Hour), // Old timestamp
			BufferSize:   5,
			Capacity:     5,
		},
	}

	// Test RecordActivity
	oldActivity := ec.Metadata.LastActivity
	ec.RecordActivity()
	assert.True(t, ec.Metadata.LastActivity.After(oldActivity))

	// Test RecordEventSent
	assert.Equal(t, int64(0), ec.Metadata.TotalEvents)
	ec.RecordEventSent()
	assert.Equal(t, int64(1), ec.Metadata.TotalEvents)

	// Test RecordEventDropped
	assert.Equal(t, int64(0), ec.Metadata.DroppedEvents)
	ec.RecordEventDropped()
	assert.Equal(t, int64(1), ec.Metadata.DroppedEvents)

	// Test Close
	ec.Close()
	assert.Nil(t, ec.Channel)

	// Test TrySend on closed channel
	success := ec.TrySend(outbound.Event{ID: "test"})
	assert.False(t, success)
}
