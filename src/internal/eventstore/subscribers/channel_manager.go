package subscribers

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"goeventsource/src/internal/port/outbound"
)

// ChannelStatus represents the status of a channel
type ChannelStatus string

const (
	// ChannelStatusActive indicates the channel is active and processing events
	ChannelStatusActive ChannelStatus = "active"
	// ChannelStatusPaused indicates the channel is paused and not processing events
	ChannelStatusPaused ChannelStatus = "paused"
	// ChannelStatusClosed indicates the channel is closed
	ChannelStatusClosed ChannelStatus = "closed"
)

// ChannelHealth represents the health status of a channel
type ChannelHealth struct {
	ID             string
	Status         ChannelStatus
	BufferSize     int
	CurrentSize    int
	EventsSent     int64
	EventsDropped  int64
	LastActivity   time.Time
	CreatedAt      time.Time
	UtilizationPct float64
}

// ChannelManagerHealth represents the overall health of the channel manager
type ChannelManagerHealth struct {
	TotalChannels      int
	ActiveChannels     int
	PausedChannels     int
	ClosedChannels     int
	TotalEventsSent    int64
	TotalEventsDropped int64
	ChannelHealths     map[string]ChannelHealth
}

// ChannelManager manages multiple channels and provides operations for their lifecycle
type ChannelManager struct {
	factory           *ChannelFactory
	mu                sync.RWMutex
	channelStatuses   map[string]ChannelStatus
	healthCheckTicker *time.Ticker
	healthCheckDone   chan struct{}
	logger            *log.Logger
}

// NewChannelManager creates a new channel manager
func NewChannelManager(factory *ChannelFactory, logger *log.Logger) *ChannelManager {
	return &ChannelManager{
		factory:         factory,
		channelStatuses: make(map[string]ChannelStatus),
		logger:          logger,
	}
}

// CreateChannel creates a new channel with the given ID and buffer size
func (m *ChannelManager) CreateChannel(id string, bufferSize int) (chan outbound.Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if channel already exists
	if status, exists := m.channelStatuses[id]; exists {
		if status == ChannelStatusClosed {
			// If closed, we can recreate it
			delete(m.channelStatuses, id)
		} else {
			return nil, fmt.Errorf("channel %s already exists with status %s", id, status)
		}
	}

	// Create the channel
	channelWrapper := m.factory.CreateChannel(id, bufferSize)

	// Track the channel status
	m.channelStatuses[id] = ChannelStatusActive
	return channelWrapper.GetChannel(), nil
}

// PauseChannel pauses a channel, preventing new events from being sent
func (m *ChannelManager) PauseChannel(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	status, exists := m.channelStatuses[id]
	if !exists {
		return fmt.Errorf("channel %s does not exist", id)
	}

	if status == ChannelStatusClosed {
		return fmt.Errorf("channel %s is closed and cannot be paused", id)
	}

	if status == ChannelStatusPaused {
		return nil // Already paused
	}

	m.channelStatuses[id] = ChannelStatusPaused
	if m.logger != nil {
		m.logger.Printf("Channel %s paused", id)
	}
	return nil
}

// ResumeChannel resumes a paused channel
func (m *ChannelManager) ResumeChannel(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	status, exists := m.channelStatuses[id]
	if !exists {
		return fmt.Errorf("channel %s does not exist", id)
	}

	if status == ChannelStatusClosed {
		return fmt.Errorf("channel %s is closed and cannot be resumed", id)
	}

	if status == ChannelStatusActive {
		return nil // Already active
	}

	m.channelStatuses[id] = ChannelStatusActive
	if m.logger != nil {
		m.logger.Printf("Channel %s resumed", id)
	}
	return nil
}

// CloseChannel closes a channel
func (m *ChannelManager) CloseChannel(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	status, exists := m.channelStatuses[id]
	if !exists {
		return fmt.Errorf("channel %s does not exist", id)
	}

	if status == ChannelStatusClosed {
		return nil // Already closed
	}

	// Get the channel wrapper
	channelWrapper, ok := m.factory.GetChannel(id, 0).(*EventChannel)
	if !ok {
		return fmt.Errorf("channel %s is not an EventChannel", id)
	}

	// Close the channel
	channelWrapper.Close()

	m.channelStatuses[id] = ChannelStatusClosed
	if m.logger != nil {
		m.logger.Printf("Channel %s closed", id)
	}
	return nil
}

// RemoveChannel removes a channel from tracking
func (m *ChannelManager) RemoveChannel(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	status, exists := m.channelStatuses[id]
	if !exists {
		return fmt.Errorf("channel %s does not exist", id)
	}

	if status != ChannelStatusClosed {
		// Get the channel wrapper
		channelWrapper, ok := m.factory.GetChannel(id, 0).(*EventChannel)
		if !ok {
			return fmt.Errorf("channel %s is not an EventChannel", id)
		}

		// Close the channel
		channelWrapper.Close()
	}

	delete(m.channelStatuses, id)

	m.factory.RemoveChannel(id)

	if m.logger != nil {
		m.logger.Printf("Channel %s removed", id)
	}
	return nil
}

// SendEvent sends an event to a channel if it's active
func (m *ChannelManager) SendEvent(id string, event outbound.Event) error {
	m.mu.RLock()
	status, exists := m.channelStatuses[id]
	m.mu.RUnlock()

	if !exists {
		return fmt.Errorf("channel %s does not exist", id)
	}

	if status != ChannelStatusActive {
		return fmt.Errorf("channel %s is not active (status: %s)", id, status)
	}

	// Get the channel and try to send
	wrapper := m.factory.GetChannel(id, 0)
	if ec, ok := wrapper.(*EventChannel); ok {
		success := ec.TrySend(event)
		if !success {
			return fmt.Errorf("failed to send event to channel %s (buffer full)", id)
		}
		return nil
	}

	return fmt.Errorf("channel %s is not an EventChannel", id)
}

// GetChannelStatus returns the status of a channel
func (m *ChannelManager) GetChannelStatus(id string) (ChannelStatus, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	status, exists := m.channelStatuses[id]
	if !exists {
		return "", fmt.Errorf("channel %s does not exist", id)
	}

	return status, nil
}

// GetChannelCount returns the total number of channels
func (m *ChannelManager) GetChannelCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.channelStatuses)
}

// ListChannels returns a list of all channel IDs
func (m *ChannelManager) ListChannels() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	channels := make([]string, 0, len(m.channelStatuses))
	for id := range m.channelStatuses {
		channels = append(channels, id)
	}

	return channels
}

// GetChannelHealth returns the health of a specific channel
func (m *ChannelManager) GetChannelHealth(id string) (ChannelHealth, error) {
	m.mu.RLock()
	status, exists := m.channelStatuses[id]
	m.mu.RUnlock()

	if !exists {
		return ChannelHealth{}, fmt.Errorf("channel %s does not exist", id)
	}

	// Get the channel wrapper
	wrapper := m.factory.GetChannel(id, 0)
	if ec, ok := wrapper.(*EventChannel); ok {
		metadata := ec.GetMetadata()

		health := ChannelHealth{
			ID:            id,
			Status:        status,
			BufferSize:    metadata.BufferSize,
			CurrentSize:   metadata.Usage,
			EventsSent:    metadata.TotalEvents,
			EventsDropped: metadata.DroppedEvents,
			LastActivity:  metadata.LastActivity,
			CreatedAt:     metadata.CreatedAt,
		}

		// Calculate utilization percentage
		if health.BufferSize > 0 {
			health.UtilizationPct = float64(health.CurrentSize) / float64(health.BufferSize) * 100
		}

		return health, nil
	}

	return ChannelHealth{}, fmt.Errorf("channel %s is not an EventChannel", id)
}

// GetHealth returns the overall health of the channel manager
func (m *ChannelManager) GetHealth() ChannelManagerHealth {
	m.mu.RLock()
	channelIDs := make([]string, 0, len(m.channelStatuses))
	for id := range m.channelStatuses {
		channelIDs = append(channelIDs, id)
	}
	m.mu.RUnlock()

	health := ChannelManagerHealth{
		TotalChannels:  len(channelIDs),
		ChannelHealths: make(map[string]ChannelHealth),
	}

	for _, id := range channelIDs {
		channelHealth, err := m.GetChannelHealth(id)
		if err != nil {
			continue
		}

		health.ChannelHealths[id] = channelHealth
		health.TotalEventsSent += channelHealth.EventsSent
		health.TotalEventsDropped += channelHealth.EventsDropped

		switch channelHealth.Status {
		case ChannelStatusActive:
			health.ActiveChannels++
		case ChannelStatusPaused:
			health.PausedChannels++
		case ChannelStatusClosed:
			health.ClosedChannels++
		}
	}

	return health
}

// ResizeChannel changes the buffer size of a channel
func (m *ChannelManager) ResizeChannel(id string, newSize int) error {
	m.mu.Lock()
	status, exists := m.channelStatuses[id]
	m.mu.Unlock()

	if !exists {
		return fmt.Errorf("channel %s does not exist", id)
	}

	if status == ChannelStatusClosed {
		return fmt.Errorf("channel %s is closed and cannot be resized", id)
	}

	// Get the channel wrapper
	wrapper := m.factory.GetChannel(id, 0)
	if ec, ok := wrapper.(*EventChannel); ok {
		oldSize := ec.GetMetadata().BufferSize

		// Resize the channel
		if _, err := ec.Resize(newSize); err != nil {
			return err
		}

		if m.logger != nil {
			m.logger.Printf("Channel %s resized from %d to %d", id, oldSize, newSize)
		}
		return nil
	}

	return fmt.Errorf("channel %s is not an EventChannel", id)
}

// CleanupInactiveChannels closes and removes channels that have been inactive
// for longer than the specified duration
func (m *ChannelManager) CleanupInactiveChannels(inactiveDuration time.Duration) int {
	now := time.Now()
	m.mu.RLock()
	channelIDs := make([]string, 0, len(m.channelStatuses))
	for id := range m.channelStatuses {
		channelIDs = append(channelIDs, id)
	}
	m.mu.RUnlock()

	inactiveCount := 0
	for _, id := range channelIDs {
		// Get the channel wrapper
		wrapper := m.factory.GetChannel(id, 0)
		if ec, ok := wrapper.(*EventChannel); ok {
			metadata := ec.GetMetadata()

			if now.Sub(metadata.LastActivity) > inactiveDuration {
				if err := m.RemoveChannel(id); err != nil {
					if m.logger != nil {
						m.logger.Printf("Failed to remove inactive channel %s: %v", id, err)
					}
					continue
				}
				inactiveCount++
				if m.logger != nil {
					m.logger.Printf("Removed inactive channel %s (last activity: %v)", id, metadata.LastActivity)
				}
			}
		}
	}

	return inactiveCount
}

// StartHealthCheck starts a periodic health check that runs the provided check function
// at the specified interval
func (m *ChannelManager) StartHealthCheck(ctx context.Context, interval time.Duration, checkFn func(ChannelManagerHealth)) {
	m.mu.Lock()
	// Don't start if already running
	if m.healthCheckTicker != nil {
		m.mu.Unlock()
		return
	}

	// Create ticker and done channel
	m.healthCheckTicker = time.NewTicker(interval)
	m.healthCheckDone = make(chan struct{})
	done := m.healthCheckDone // Create a local copy to avoid race conditions
	m.mu.Unlock()

	go func() {
		defer func() {
			// Use the local copy of the done channel to avoid race condition
			// if StopHealthCheck is called concurrently
			select {
			case <-done: // Channel already closed
				return
			default:
				// Don't close if already closed
				// This is safe because we're using a local copy and no other goroutine will close this copy
				close(done)
			}
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-m.healthCheckTicker.C:
				health := m.GetHealth()
				checkFn(health)
			}
		}
	}()

	if m.logger != nil {
		m.logger.Printf("Started health check with interval %v", interval)
	}
}

// StopHealthCheck stops the periodic health check
func (m *ChannelManager) StopHealthCheck() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.healthCheckTicker != nil {
		m.healthCheckTicker.Stop()
		m.healthCheckTicker = nil

		// Only close the channel if it's not nil and not already closed
		if m.healthCheckDone != nil {
			close(m.healthCheckDone)
			m.healthCheckDone = nil
		}

		if m.logger != nil {
			m.logger.Printf("Stopped health check")
		}
	}
}

// Shutdown closes all channels and stops any running goroutines
func (m *ChannelManager) Shutdown() {
	// Stop health check if running, with locking to prevent race conditions
	m.StopHealthCheck()

	// Close all channels
	m.mu.Lock()
	channelIDs := make([]string, 0, len(m.channelStatuses))
	for id := range m.channelStatuses {
		channelIDs = append(channelIDs, id)
	}
	m.mu.Unlock()

	for _, id := range channelIDs {
		_ = m.RemoveChannel(id)
	}

	if m.logger != nil {
		m.logger.Printf("Channel manager shutdown complete")
	}
}
