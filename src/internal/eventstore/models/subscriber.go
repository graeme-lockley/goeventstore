// Package models contains domain models for the EventStore.
package models

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"goeventsource/src/internal/port/outbound"
)

// SubscriberState represents the current state of a subscriber
type SubscriberState string

const (
	// SubscriberStateActive indicates the subscriber is active and receiving events
	SubscriberStateActive SubscriberState = "active"
	// SubscriberStatePaused indicates the subscriber is temporarily paused
	SubscriberStatePaused SubscriberState = "paused"
	// SubscriberStateClosed indicates the subscriber has been permanently closed
	SubscriberStateClosed SubscriberState = "closed"
)

// ToOutboundState converts the internal SubscriberState to the outbound package's SubscriberState
func (s SubscriberState) ToOutboundState() outbound.SubscriberState {
	switch s {
	case SubscriberStateActive:
		return outbound.SubscriberStateActive
	case SubscriberStatePaused:
		return outbound.SubscriberStatePaused
	case SubscriberStateClosed:
		return outbound.SubscriberStateClosed
	default:
		return outbound.SubscriberStateActive
	}
}

// SubscriberFilter defines criteria for filtering events
type SubscriberFilter struct {
	// EventTypes is a list of event types the subscriber is interested in
	// If empty, all event types will be delivered
	EventTypes []string

	// FromVersion specifies the minimum version of events to receive
	// Events with a version less than this will be filtered out
	FromVersion int64

	// Metadata contains key-value pairs that must match event metadata
	// For an event to be delivered, all specified metadata must match
	Metadata map[string]interface{}
}

// ToOutboundFilter converts the internal SubscriberFilter to the outbound package's SubscriberFilter
func (f *SubscriberFilter) ToOutboundFilter() outbound.SubscriberFilter {
	return outbound.SubscriberFilter{
		EventTypes:  f.EventTypes,
		FromVersion: f.FromVersion,
		Metadata:    f.Metadata,
	}
}

// FromOutboundFilter creates a SubscriberFilter from an outbound SubscriberFilter
func SubscriberFilterFromOutbound(filter outbound.SubscriberFilter) SubscriberFilter {
	return SubscriberFilter{
		EventTypes:  filter.EventTypes,
		FromVersion: filter.FromVersion,
		Metadata:    filter.Metadata,
	}
}

// TimeoutConfig defines timeout behavior for a subscriber
type TimeoutConfig struct {
	// InitialTimeout is the initial timeout duration for event delivery
	InitialTimeout time.Duration

	// MaxTimeout is the maximum timeout duration after backoff
	MaxTimeout time.Duration

	// BackoffMultiplier is used to calculate the next timeout duration
	// NextTimeout = PreviousTimeout * BackoffMultiplier
	BackoffMultiplier float64

	// MaxRetries is the maximum number of delivery attempts before giving up
	MaxRetries int

	// CooldownPeriod is the duration to wait after max retries before trying again
	CooldownPeriod time.Duration

	// CurrentTimeout is the current timeout duration after backoff
	CurrentTimeout time.Duration

	// CurrentRetryCount is the current retry count
	CurrentRetryCount int

	// LastRetryTime is when the last retry attempt was made
	LastRetryTime time.Time
}

// ToOutboundTimeoutConfig converts the internal TimeoutConfig to the outbound package's TimeoutConfig
func (t *TimeoutConfig) ToOutboundTimeoutConfig() outbound.TimeoutConfig {
	return outbound.TimeoutConfig{
		InitialTimeout:    t.InitialTimeout,
		MaxTimeout:        t.MaxTimeout,
		BackoffMultiplier: t.BackoffMultiplier,
		MaxRetries:        t.MaxRetries,
		CooldownPeriod:    t.CooldownPeriod,
	}
}

// FromOutboundTimeoutConfig creates a TimeoutConfig from an outbound TimeoutConfig
func TimeoutConfigFromOutbound(config outbound.TimeoutConfig) TimeoutConfig {
	return TimeoutConfig{
		InitialTimeout:    config.InitialTimeout,
		MaxTimeout:        config.MaxTimeout,
		BackoffMultiplier: config.BackoffMultiplier,
		MaxRetries:        config.MaxRetries,
		CooldownPeriod:    config.CooldownPeriod,
		CurrentTimeout:    config.InitialTimeout,
		CurrentRetryCount: 0,
		LastRetryTime:     time.Time{},
	}
}

// DefaultTimeoutConfig returns a default TimeoutConfig with sensible defaults
func DefaultTimeoutConfig() TimeoutConfig {
	return TimeoutConfig{
		InitialTimeout:    1 * time.Second,
		MaxTimeout:        30 * time.Second,
		BackoffMultiplier: 2.0,
		MaxRetries:        5,
		CooldownPeriod:    1 * time.Minute,
		CurrentTimeout:    1 * time.Second,
		CurrentRetryCount: 0,
		LastRetryTime:     time.Time{},
	}
}

// Subscriber represents a subscriber to events
type Subscriber struct {
	// ID is a unique identifier for the subscriber
	ID string

	// Topics is a list of topics the subscriber is interested in
	Topics []string

	// Filter contains filtering criteria for events
	Filter SubscriberFilter

	// Timeout contains timeout configuration
	Timeout TimeoutConfig

	// BufferSize is the size of the channel buffer for this subscriber
	BufferSize int

	// EventChannel is the channel for delivering events to the subscriber
	EventChannel chan outbound.Event

	// State is the current state of the subscriber
	State SubscriberState

	// CreatedAt is when the subscriber was created
	CreatedAt time.Time

	// LastActivityAt is when the subscriber last had activity
	LastActivityAt time.Time

	// Statistics
	stats subscriberStats

	// Mutex for thread safety
	mu sync.RWMutex
}

// subscriberStats contains subscriber activity statistics
type subscriberStats struct {
	eventsDelivered    int64
	eventsDropped      int64
	lastEventTimestamp time.Time
	lastErrorTimestamp time.Time
	errorCount         int64
	timeoutCount       int64
	totalLatency       time.Duration
	deliveryCount      int64
}

// SubscriberConfig contains configuration for creating a new subscriber
type SubscriberConfig struct {
	// ID is a unique identifier for the subscriber
	ID string

	// Topics is a list of topics the subscriber is interested in
	Topics []string

	// Filter contains filtering criteria for events
	Filter SubscriberFilter

	// Timeout contains timeout configuration
	Timeout TimeoutConfig

	// BufferSize is the size of the channel buffer for this subscriber
	BufferSize int
}

// ToOutboundConfig converts the internal SubscriberConfig to the outbound package's SubscriberConfig
func (c *SubscriberConfig) ToOutboundConfig() outbound.SubscriberConfig {
	return outbound.SubscriberConfig{
		ID:         c.ID,
		Topics:     c.Topics,
		Filter:     c.Filter.ToOutboundFilter(),
		Timeout:    c.Timeout.ToOutboundTimeoutConfig(),
		BufferSize: c.BufferSize,
	}
}

// FromOutboundConfig creates a SubscriberConfig from an outbound SubscriberConfig
func SubscriberConfigFromOutbound(config outbound.SubscriberConfig) SubscriberConfig {
	return SubscriberConfig{
		ID:         config.ID,
		Topics:     config.Topics,
		Filter:     SubscriberFilterFromOutbound(config.Filter),
		Timeout:    TimeoutConfigFromOutbound(config.Timeout),
		BufferSize: config.BufferSize,
	}
}

// DefaultBufferSize is the default size for event channels
const DefaultBufferSize = 100

// NewSubscriber creates a new subscriber
func NewSubscriber(config SubscriberConfig) *Subscriber {
	// Use default buffer size if not specified
	bufferSize := config.BufferSize
	if bufferSize <= 0 {
		bufferSize = DefaultBufferSize
	}

	now := time.Now()
	return &Subscriber{
		ID:             config.ID,
		Topics:         config.Topics,
		Filter:         config.Filter,
		Timeout:        config.Timeout,
		BufferSize:     bufferSize,
		EventChannel:   make(chan outbound.Event, bufferSize),
		State:          SubscriberStateActive,
		CreatedAt:      now,
		LastActivityAt: now,
		stats:          subscriberStats{},
		mu:             sync.RWMutex{},
	}
}

// ReceiveEvent implements the outbound.Subscriber interface
func (s *Subscriber) ReceiveEvent(ctx context.Context, event outbound.Event) error {
	s.mu.RLock()
	state := s.State
	s.mu.RUnlock()

	if state != SubscriberStateActive {
		// Increment dropped events count if not active
		atomic.AddInt64(&s.stats.eventsDropped, 1)
		return nil
	}

	// Update last activity time
	s.updateLastActivity()

	// Use select to handle timeouts and cancellations
	select {
	case s.EventChannel <- event:
		// Event sent successfully
		s.recordEventDelivery()
		// Reset retry count on successful delivery
		s.resetRetryCount()
		return nil
	case <-ctx.Done():
		// Context cancelled or timed out
		atomic.AddInt64(&s.stats.timeoutCount, 1)
		s.recordDeliveryError()
		// Increment retry count and update timeout
		s.handleRetry()
		return ctx.Err()
	}
}

// GetID returns the subscriber's ID
func (s *Subscriber) GetID() string {
	return s.ID
}

// GetTopics returns the subscriber's topics
func (s *Subscriber) GetTopics() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// Return a copy to prevent modification
	topicsCopy := make([]string, len(s.Topics))
	copy(topicsCopy, s.Topics)
	return topicsCopy
}

// GetFilter returns the subscriber's filter
func (s *Subscriber) GetFilter() outbound.SubscriberFilter {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Filter.ToOutboundFilter()
}

// GetState returns the subscriber's state
func (s *Subscriber) GetState() outbound.SubscriberState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.State.ToOutboundState()
}

// GetStats returns the subscriber's statistics
func (s *Subscriber) GetStats() outbound.SubscriberStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var avgLatency time.Duration
	deliveryCount := atomic.LoadInt64(&s.stats.deliveryCount)
	if deliveryCount > 0 {
		totalLatency := atomic.LoadInt64((*int64)(&s.stats.totalLatency))
		avgLatency = time.Duration(totalLatency / deliveryCount)
	}

	return outbound.SubscriberStats{
		EventsDelivered:    atomic.LoadInt64(&s.stats.eventsDelivered),
		EventsDropped:      atomic.LoadInt64(&s.stats.eventsDropped),
		LastEventTimestamp: s.stats.lastEventTimestamp,
		LastErrorTimestamp: s.stats.lastErrorTimestamp,
		ErrorCount:         atomic.LoadInt64(&s.stats.errorCount),
		TimeoutCount:       atomic.LoadInt64(&s.stats.timeoutCount),
		CurrentRetryCount:  s.Timeout.CurrentRetryCount,
		AverageLatency:     avgLatency,
	}
}

// Pause temporarily stops event delivery
func (s *Subscriber) Pause() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Always update LastActivityAt, even if state doesn't change
	s.LastActivityAt = time.Now()

	if s.State != SubscriberStateClosed {
		s.State = SubscriberStatePaused
	}
	return nil
}

// Resume restarts event delivery
func (s *Subscriber) Resume() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Always update LastActivityAt, even if state doesn't change
	s.LastActivityAt = time.Now()

	if s.State != SubscriberStateClosed {
		s.State = SubscriberStateActive
	}
	return nil
}

// Close permanently shuts down the subscriber
func (s *Subscriber) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Always update LastActivityAt
	s.LastActivityAt = time.Now()

	if s.State != SubscriberStateClosed {
		s.State = SubscriberStateClosed
		close(s.EventChannel)
	}
	return nil
}

// UpdateFilter updates the subscriber's filter
func (s *Subscriber) UpdateFilter(filter outbound.SubscriberFilter) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Filter = SubscriberFilterFromOutbound(filter)
	s.updateLastActivity()
	return nil
}

// UpdateTimeout updates the subscriber's timeout configuration
func (s *Subscriber) UpdateTimeout(config outbound.TimeoutConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Timeout = TimeoutConfigFromOutbound(config)
	// Reset the current timeout to initial timeout when configuration is updated
	s.Timeout.CurrentTimeout = s.Timeout.InitialTimeout
	s.Timeout.CurrentRetryCount = 0
	s.updateLastActivity()
	return nil
}

// updateLastActivity updates the last activity timestamp
func (s *Subscriber) updateLastActivity() {
	s.LastActivityAt = time.Now()
}

// recordEventDelivery records a successful event delivery
func (s *Subscriber) recordEventDelivery() {
	now := time.Now()
	atomic.AddInt64(&s.stats.eventsDelivered, 1)
	s.mu.Lock()
	s.stats.lastEventTimestamp = now
	s.mu.Unlock()
}

// recordDeliveryError records an event delivery error
func (s *Subscriber) recordDeliveryError() {
	now := time.Now()
	atomic.AddInt64(&s.stats.errorCount, 1)
	s.mu.Lock()
	s.stats.lastErrorTimestamp = now
	s.mu.Unlock()
}

// handleRetry increments the retry count and updates the timeout duration using backoff
func (s *Subscriber) handleRetry() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Record retry attempt time
	s.Timeout.LastRetryTime = time.Now()

	// Increment retry count
	s.Timeout.CurrentRetryCount++

	// Check if we've reached max retries
	if s.Timeout.CurrentRetryCount > s.Timeout.MaxRetries {
		// Don't increase timeout further, we'll enter cooldown period
		return
	}

	// Calculate new timeout with backoff, but don't exceed max timeout
	newTimeout := time.Duration(float64(s.Timeout.CurrentTimeout) * s.Timeout.BackoffMultiplier)
	if newTimeout > s.Timeout.MaxTimeout {
		newTimeout = s.Timeout.MaxTimeout
	}
	s.Timeout.CurrentTimeout = newTimeout
}

// resetRetryCount resets retry count and timeout after successful delivery
func (s *Subscriber) resetRetryCount() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Only reset if we've had retries or in cooldown
	if s.Timeout.CurrentRetryCount > 0 {
		s.Timeout.CurrentRetryCount = 0
		s.Timeout.CurrentTimeout = s.Timeout.InitialTimeout
	}
}

// IsInCooldown returns true if the subscriber is in cooldown period after max retries
func (s *Subscriber) IsInCooldown() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// If we haven't reached max retries, we're not in cooldown
	if s.Timeout.CurrentRetryCount < s.Timeout.MaxRetries {
		return false
	}

	// Check if cooldown period has elapsed since last retry
	cooldownEnds := s.Timeout.LastRetryTime.Add(s.Timeout.CooldownPeriod)
	return time.Now().Before(cooldownEnds)
}

// ShouldRetry returns true if the subscriber should retry delivery
func (s *Subscriber) ShouldRetry() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Don't retry if max retries reached and still in cooldown
	if s.Timeout.CurrentRetryCount >= s.Timeout.MaxRetries {
		// Check if cooldown period has elapsed
		cooldownEnds := s.Timeout.LastRetryTime.Add(s.Timeout.CooldownPeriod)
		if time.Now().Before(cooldownEnds) {
			// Still in cooldown
			return false
		}
		// Cooldown period elapsed, can retry again
		return true
	}

	// Can retry if we haven't reached max retries
	return true
}

// GetRetryInfo returns current retry count, max retries, and current timeout
func (s *Subscriber) GetRetryInfo() (current int, max int, timeout time.Duration) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Timeout.CurrentRetryCount, s.Timeout.MaxRetries, s.Timeout.CurrentTimeout
}

// resetAfterCooldown resets retry count and timeout after cooldown period
// Returns true if reset was performed
func (s *Subscriber) resetAfterCooldown() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if we're in cooldown and cooldown period has elapsed
	if s.Timeout.CurrentRetryCount >= s.Timeout.MaxRetries {
		cooldownEnds := s.Timeout.LastRetryTime.Add(s.Timeout.CooldownPeriod)
		if time.Now().After(cooldownEnds) {
			// Cooldown period elapsed, reset
			s.Timeout.CurrentRetryCount = 0
			s.Timeout.CurrentTimeout = s.Timeout.InitialTimeout
			return true
		}
	}
	return false
}
