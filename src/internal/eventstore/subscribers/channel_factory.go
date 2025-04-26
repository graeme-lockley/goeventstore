// Package subscribers contains the subscriber registry and related components for the EventStore.
package subscribers

import (
	"context"
	"fmt"
	"sync"
	"time"

	"goeventsource/src/internal/eventstore/models"
	"goeventsource/src/internal/port/outbound"
)

// ChannelMetadata contains metadata for a subscriber channel
type ChannelMetadata struct {
	// SubscriberID is the ID of the subscriber this channel belongs to
	SubscriberID string

	// CreatedAt is when the channel was created
	CreatedAt time.Time

	// LastActivity is when the channel last had activity
	LastActivity time.Time

	// BufferSize is the buffer size of the channel
	BufferSize int

	// Capacity is the current capacity of the channel
	Capacity int

	// Usage is the current usage of the channel (number of items in buffer)
	Usage int

	// DroppedEvents is the number of events dropped due to buffer overflow
	DroppedEvents int64

	// TotalEvents is the total number of events sent through the channel
	TotalEvents int64
}

// EventChannel is a wrapper around a standard Go channel with metadata
type EventChannel struct {
	// Channel is the actual Go channel for events
	Channel chan outbound.Event

	// Metadata contains information about the channel
	Metadata ChannelMetadata

	// mu protects metadata updates
	mu sync.RWMutex
}

// RecordActivity updates the last activity timestamp
func (ec *EventChannel) RecordActivity() {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	ec.Metadata.LastActivity = time.Now()
}

// RecordEventSent increments the total events counter and records activity
func (ec *EventChannel) RecordEventSent() {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	ec.Metadata.TotalEvents++
	ec.Metadata.LastActivity = time.Now()
}

// RecordEventDropped increments the dropped events counter
func (ec *EventChannel) RecordEventDropped() {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	ec.Metadata.DroppedEvents++
}

// GetMetadata returns a copy of the channel metadata
func (ec *EventChannel) GetMetadata() ChannelMetadata {
	ec.mu.RLock()
	defer ec.mu.RUnlock()

	// Calculate current usage
	ec.Metadata.Usage = len(ec.Channel)
	return ec.Metadata
}

// Close closes the underlying channel
func (ec *EventChannel) Close() {
	// Only close once
	ec.mu.Lock()
	defer ec.mu.Unlock()

	// Safe close using a nil check
	if ec.Channel != nil {
		close(ec.Channel)
		ec.Channel = nil
	}
}

// Resize changes the buffer size of the channel
// This creates a new channel with the new buffer size and transfers all existing events
// Returns the number of events transferred, or an error if the resize failed
func (ec *EventChannel) Resize(newBufferSize int) (int, error) {
	if newBufferSize <= 0 {
		return 0, fmt.Errorf("invalid buffer size: %d", newBufferSize)
	}

	ec.mu.Lock()
	defer ec.mu.Unlock()

	// Check if the channel is already closed
	if ec.Channel == nil {
		return 0, fmt.Errorf("cannot resize closed channel")
	}

	// Create a new channel with the new buffer size
	newChannel := make(chan outbound.Event, newBufferSize)

	// Save the old channel
	oldChannel := ec.Channel

	// Get the current length of the channel
	currentSize := len(oldChannel)

	// Transfer events from the old channel to the new one
	// Only transfer what we can without blocking
	transferCount := 0
	for i := 0; i < currentSize; i++ {
		select {
		case event, ok := <-oldChannel:
			if !ok {
				// Channel was closed during transfer
				break
			}
			select {
			case newChannel <- event:
				transferCount++
			default:
				// New channel is full, log and stop transferring
				// This shouldn't happen if newBufferSize >= len(oldChannel)
				ec.Metadata.DroppedEvents++
			}
		default:
			// No more events to read
			break
		}
	}

	// Update the channel and metadata
	ec.Channel = newChannel
	ec.Metadata.BufferSize = newBufferSize
	ec.Metadata.Capacity = newBufferSize
	ec.Metadata.LastActivity = time.Now()

	// Don't close the old channel here to avoid losing events
	// that might be added concurrently. The garbage collector will
	// take care of it once all references are gone.

	return transferCount, nil
}

// TrySend attempts to send an event to the channel without blocking
// Returns true if successful, false if the channel is full or closed
func (ec *EventChannel) TrySend(event outbound.Event) bool {
	select {
	case ec.Channel <- event:
		ec.RecordEventSent()
		return true
	default:
		ec.RecordEventDropped()
		return false
	}
}

// GetChannel returns the underlying event channel
func (ec *EventChannel) GetChannel() chan outbound.Event {
	return ec.Channel
}

// ChannelFactory creates and manages event channels for subscribers
type ChannelFactory struct {
	// channels maps subscriber IDs to their event channels
	channels map[string]*EventChannel

	// defaultBufferSize is the default buffer size for new channels
	defaultBufferSize int

	// mu protects the channels map
	mu sync.RWMutex

	// logger for factory operations
	logger *SubscriberLogger
}

// NewChannelFactory creates a new channel factory with the specified default buffer size
func NewChannelFactory(defaultBufferSize int, logger *SubscriberLogger) *ChannelFactory {
	if defaultBufferSize <= 0 {
		defaultBufferSize = 100 // Fallback default
	}

	return &ChannelFactory{
		channels:          make(map[string]*EventChannel),
		defaultBufferSize: defaultBufferSize,
		logger:            logger,
	}
}

// CreateChannel creates a new event channel for a subscriber
func (cf *ChannelFactory) CreateChannel(subscriberID string, bufferSize int) models.ChannelWrapper {
	cf.mu.Lock()
	defer cf.mu.Unlock()

	// Use default buffer size if not specified
	if bufferSize <= 0 {
		bufferSize = cf.defaultBufferSize
	}

	// Create channel with metadata
	now := time.Now()
	ec := &EventChannel{
		Channel: make(chan outbound.Event, bufferSize),
		Metadata: ChannelMetadata{
			SubscriberID:  subscriberID,
			CreatedAt:     now,
			LastActivity:  now,
			BufferSize:    bufferSize,
			Capacity:      bufferSize,
			Usage:         0,
			DroppedEvents: 0,
			TotalEvents:   0,
		},
	}

	// Store in registry
	cf.channels[subscriberID] = ec

	if cf.logger != nil {
		cf.logger.Debug(context.Background(), "Created channel for subscriber", map[string]interface{}{
			"subscriber_id": subscriberID,
			"buffer_size":   bufferSize,
		})
	}

	return ec
}

// GetChannel retrieves an existing channel or creates a new one
func (cf *ChannelFactory) GetChannel(subscriberID string, bufferSize int) models.ChannelWrapper {
	cf.mu.RLock()
	if ec, exists := cf.channels[subscriberID]; exists {
		cf.mu.RUnlock()
		return ec
	}
	cf.mu.RUnlock()

	// Channel doesn't exist, create a new one
	return cf.CreateChannel(subscriberID, bufferSize)
}

// ResizeChannel changes the buffer size of a channel for a subscriber
// Returns the new buffer size and number of events transferred, or an error if the resize failed
func (cf *ChannelFactory) ResizeChannel(subscriberID string, newBufferSize int) (int, int, error) {
	cf.mu.RLock()
	ec, exists := cf.channels[subscriberID]
	cf.mu.RUnlock()

	if !exists {
		return 0, 0, fmt.Errorf("subscriber channel not found: %s", subscriberID)
	}

	// Perform the resize
	transferCount, err := ec.Resize(newBufferSize)
	if err != nil {
		return 0, 0, err
	}

	if cf.logger != nil {
		cf.logger.Debug(context.Background(), "Resized channel for subscriber", map[string]interface{}{
			"subscriber_id":      subscriberID,
			"new_buffer_size":    newBufferSize,
			"events_transferred": transferCount,
		})
	}

	return newBufferSize, transferCount, nil
}

// RemoveChannel removes a channel from the registry and closes it
func (cf *ChannelFactory) RemoveChannel(subscriberID string) {
	cf.mu.Lock()
	defer cf.mu.Unlock()

	if ec, exists := cf.channels[subscriberID]; exists {
		// Close the channel
		ec.Close()

		// Remove from registry
		delete(cf.channels, subscriberID)

		if cf.logger != nil {
			cf.logger.Debug(context.Background(), "Removed channel for subscriber", map[string]interface{}{
				"subscriber_id":  subscriberID,
				"total_events":   ec.Metadata.TotalEvents,
				"dropped_events": ec.Metadata.DroppedEvents,
			})
		}
	}
}

// ListChannels returns a list of all channel metadata
func (cf *ChannelFactory) ListChannels() []ChannelMetadata {
	cf.mu.RLock()
	defer cf.mu.RUnlock()

	result := make([]ChannelMetadata, 0, len(cf.channels))
	for _, ec := range cf.channels {
		result = append(result, ec.GetMetadata())
	}

	return result
}

// GetChannelCount returns the total number of active channels
func (cf *ChannelFactory) GetChannelCount() int {
	cf.mu.RLock()
	defer cf.mu.RUnlock()

	return len(cf.channels)
}

// CheckChannelHealth checks the health of a specific channel
// Returns a map with health metrics and a boolean indicating if the channel is healthy
func (cf *ChannelFactory) CheckChannelHealth(subscriberID string) (map[string]interface{}, bool) {
	cf.mu.RLock()
	ec, exists := cf.channels[subscriberID]
	cf.mu.RUnlock()

	if !exists {
		return map[string]interface{}{
			"exists": false,
		}, false
	}

	metadata := ec.GetMetadata()

	// Calculate usage percentage
	usagePercent := 0.0
	if metadata.Capacity > 0 {
		usagePercent = float64(metadata.Usage) / float64(metadata.Capacity) * 100
	}

	// Calculate drop rate
	dropRate := 0.0
	if metadata.TotalEvents > 0 {
		dropRate = float64(metadata.DroppedEvents) / float64(metadata.TotalEvents) * 100
	}

	// Determine health based on metrics
	// Consider a channel unhealthy if:
	// 1. Usage is above 80%
	// 2. Drop rate is above 10%
	// 3. No activity in the last 5 minutes
	isHealthy := true

	if usagePercent > 80 {
		isHealthy = false
	}

	if dropRate > 10 {
		isHealthy = false
	}

	inactivityDuration := time.Since(metadata.LastActivity)
	if inactivityDuration > 5*time.Minute {
		isHealthy = false
	}

	return map[string]interface{}{
		"subscriber_id":       metadata.SubscriberID,
		"created_at":          metadata.CreatedAt,
		"last_activity":       metadata.LastActivity,
		"inactivity_duration": inactivityDuration.String(),
		"buffer_size":         metadata.BufferSize,
		"capacity":            metadata.Capacity,
		"usage":               metadata.Usage,
		"usage_percent":       fmt.Sprintf("%.2f%%", usagePercent),
		"total_events":        metadata.TotalEvents,
		"dropped_events":      metadata.DroppedEvents,
		"drop_rate":           fmt.Sprintf("%.2f%%", dropRate),
		"is_healthy":          isHealthy,
	}, isHealthy
}

// CheckAllChannelsHealth checks the health of all channels
// Returns a summary of health status and a list of unhealthy channels
func (cf *ChannelFactory) CheckAllChannelsHealth() map[string]interface{} {
	cf.mu.RLock()
	channelIDs := make([]string, 0, len(cf.channels))
	for id := range cf.channels {
		channelIDs = append(channelIDs, id)
	}
	cf.mu.RUnlock()

	totalChannels := len(channelIDs)
	healthyCount := 0
	unhealthyChannels := make([]map[string]interface{}, 0)

	for _, id := range channelIDs {
		metrics, isHealthy := cf.CheckChannelHealth(id)

		if isHealthy {
			healthyCount++
		} else {
			unhealthyChannels = append(unhealthyChannels, metrics)
		}
	}

	return map[string]interface{}{
		"total_channels":     totalChannels,
		"healthy_channels":   healthyCount,
		"unhealthy_channels": len(unhealthyChannels),
		"health_percentage":  fmt.Sprintf("%.2f%%", float64(healthyCount)/float64(totalChannels)*100),
		"unhealthy_details":  unhealthyChannels,
	}
}

// CleanupInactiveChannels removes channels that have been inactive for longer than the specified duration
func (cf *ChannelFactory) CleanupInactiveChannels(inactiveDuration time.Duration) int {
	cf.mu.RLock()
	channelIDs := make([]string, 0, len(cf.channels))
	for id, ec := range cf.channels {
		metadata := ec.GetMetadata()
		if time.Since(metadata.LastActivity) > inactiveDuration {
			channelIDs = append(channelIDs, id)
		}
	}
	cf.mu.RUnlock()

	// Remove inactive channels
	for _, id := range channelIDs {
		cf.RemoveChannel(id)
	}

	if len(channelIDs) > 0 && cf.logger != nil {
		cf.logger.Info(context.Background(), "Cleaned up inactive channels", map[string]interface{}{
			"removed_count":     len(channelIDs),
			"inactive_duration": inactiveDuration.String(),
		})
	}

	return len(channelIDs)
}
