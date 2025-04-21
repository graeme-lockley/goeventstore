package outbound

import (
	"context"
	"time"
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

// SubscriberFilter defines criteria for filtering events
type SubscriberFilter struct {
	// EventTypes is a list of event types the subscriber is interested in
	// If empty, all event types will be delivered
	EventTypes []string `json:"event_types,omitempty"`

	// FromVersion specifies the minimum version of events to receive
	// Events with a version less than this will be filtered out
	FromVersion int64 `json:"from_version"`

	// Metadata contains key-value pairs that must match event metadata
	// For an event to be delivered, all specified metadata must match
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// TimeoutConfig defines timeout behavior for a subscriber
type TimeoutConfig struct {
	// InitialTimeout is the initial timeout duration for event delivery
	InitialTimeout time.Duration `json:"initial_timeout"`

	// MaxTimeout is the maximum timeout duration after backoff
	MaxTimeout time.Duration `json:"max_timeout"`

	// BackoffMultiplier is used to calculate the next timeout duration
	// NextTimeout = PreviousTimeout * BackoffMultiplier
	BackoffMultiplier float64 `json:"backoff_multiplier"`

	// MaxRetries is the maximum number of delivery attempts before giving up
	MaxRetries int `json:"max_retries"`

	// CooldownPeriod is the duration to wait after max retries before trying again
	CooldownPeriod time.Duration `json:"cooldown_period"`
}

// SubscriberConfig contains configuration for a subscriber
type SubscriberConfig struct {
	// ID is a unique identifier for the subscriber
	ID string `json:"id"`

	// Topics is a list of topics the subscriber is interested in
	Topics []string `json:"topics"`

	// Filter contains filtering criteria for events
	Filter SubscriberFilter `json:"filter"`

	// Timeout contains timeout configuration
	Timeout TimeoutConfig `json:"timeout"`

	// BufferSize is the size of the channel buffer for this subscriber
	BufferSize int `json:"buffer_size"`
}

// SubscriberStats contains statistics about a subscriber's activity
type SubscriberStats struct {
	// EventsDelivered is the total number of events successfully delivered
	EventsDelivered int64 `json:"events_delivered"`

	// EventsDropped is the number of events that could not be delivered
	EventsDropped int64 `json:"events_dropped"`

	// LastEventTimestamp is when the last event was delivered
	LastEventTimestamp time.Time `json:"last_event_timestamp"`

	// LastErrorTimestamp is when the last error occurred
	LastErrorTimestamp time.Time `json:"last_error_timestamp"`

	// ErrorCount is the total number of errors encountered
	ErrorCount int64 `json:"error_count"`

	// TimeoutCount is the number of timeouts encountered
	TimeoutCount int64 `json:"timeout_count"`

	// CurrentRetryCount is the current retry count for the last failed event
	CurrentRetryCount int `json:"current_retry_count"`

	// AverageLatency is the average time between event publication and delivery
	AverageLatency time.Duration `json:"average_latency"`
}

// Subscriber defines the interface for an event subscriber
type Subscriber interface {
	// ReceiveEvent delivers an event to the subscriber
	// Returns an error if the event could not be delivered
	// Context includes a timeout for delivery
	ReceiveEvent(ctx context.Context, event Event) error

	// GetID returns the unique identifier of the subscriber
	GetID() string

	// GetTopics returns the list of topics the subscriber is interested in
	GetTopics() []string

	// GetFilter returns the subscriber's filter configuration
	GetFilter() SubscriberFilter

	// GetState returns the current state of the subscriber
	GetState() SubscriberState

	// GetStats returns statistics about the subscriber's activity
	GetStats() SubscriberStats

	// Pause temporarily stops event delivery to the subscriber
	// Events published during the paused state will not be delivered
	Pause() error

	// Resume restarts event delivery after a pause
	Resume() error

	// Close permanently shuts down the subscriber
	// After closing, the subscriber will not receive any more events
	Close() error

	// UpdateFilter updates the subscriber's event filtering criteria
	UpdateFilter(filter SubscriberFilter) error

	// UpdateTimeout updates the subscriber's timeout configuration
	UpdateTimeout(config TimeoutConfig) error
}

// SubscriberRegistry defines the interface for managing multiple subscribers
type SubscriberRegistry interface {
	// RegisterSubscriber adds a new subscriber to the registry
	RegisterSubscriber(config SubscriberConfig) (Subscriber, error)

	// DeregisterSubscriber removes a subscriber from the registry
	DeregisterSubscriber(subscriberID string) error

	// GetSubscriber retrieves a subscriber by ID
	GetSubscriber(subscriberID string) (Subscriber, error)

	// ListSubscribers returns all subscribers in the registry
	// If topicFilter is provided, only subscribers interested in that topic are returned
	ListSubscribers(topicFilter string) ([]Subscriber, error)

	// BroadcastEvent sends an event to all interested subscribers
	// Returns the number of subscribers the event was delivered to
	BroadcastEvent(ctx context.Context, event Event) (int, error)

	// GetStats returns statistics about the registry
	GetStats() map[string]interface{}

	// Close gracefully shuts down all subscribers
	Close() error
}
