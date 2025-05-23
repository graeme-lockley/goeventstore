package outbound

import (
	"context"
)

// Event represents a domain event to be stored
type Event struct {
	ID        string                 `json:"id"`
	Topic     string                 `json:"topic"`
	Type      string                 `json:"type"`
	Data      map[string]interface{} `json:"data"`
	Metadata  map[string]interface{} `json:"metadata"`
	Timestamp int64                  `json:"timestamp"`
	Version   int64                  `json:"version"`
}

// TopicConfig contains configuration for a topic's storage
type TopicConfig struct {
	Name       string            `json:"name"`
	Adapter    string            `json:"adapter"`
	Connection string            `json:"connection"`
	Options    map[string]string `json:"options"`
}

// RepositoryState represents the current lifecycle state of a repository
type RepositoryState string

const (
	// StateUninitialized indicates the repository has been created but not initialized
	StateUninitialized RepositoryState = "uninitialized"
	// StateInitializing indicates the repository is in the process of initializing
	StateInitializing RepositoryState = "initializing"
	// StateReady indicates the repository is initialized and ready for use
	StateReady RepositoryState = "ready"
	// StateClosing indicates the repository is in the process of shutting down
	StateClosing RepositoryState = "closing"
	// StateClosed indicates the repository has been closed and should not be used
	StateClosed RepositoryState = "closed"
	// StateFailed indicates the repository has encountered a fatal error
	StateFailed RepositoryState = "failed"
)

// HealthStatus represents the operational health of a repository
type HealthStatus string

const (
	// StatusUp indicates the repository is operational
	StatusUp HealthStatus = "up"
	// StatusDegraded indicates the repository is operational but with issues
	StatusDegraded HealthStatus = "degraded"
	// StatusDown indicates the repository is not operational
	StatusDown HealthStatus = "down"
)

// HealthInfo contains standardized health information for a repository
type HealthInfo struct {
	Status         HealthStatus           `json:"status"`
	State          RepositoryState        `json:"state"`
	Message        string                 `json:"message,omitempty"`
	Error          string                 `json:"error,omitempty"`
	AdditionalInfo map[string]interface{} `json:"info,omitempty"`
}

// EventRepository defines the interface that all storage adapters must implement
type EventRepository interface {
	// Data plane operations
	AppendEvents(ctx context.Context, topic string, events []Event) error
	GetEvents(ctx context.Context, topic string, fromVersion int64) ([]Event, error)
	GetEventsByType(ctx context.Context, topic string, eventType string, fromVersion int64) ([]Event, error)
	GetLatestVersion(ctx context.Context, topic string) (int64, error)

	// Control plane operations
	CreateTopic(ctx context.Context, config TopicConfig) error
	DeleteTopic(ctx context.Context, topic string) error
	ListTopics(ctx context.Context) ([]TopicConfig, error)
	TopicExists(ctx context.Context, topic string) (bool, error)
	UpdateTopicConfig(ctx context.Context, config TopicConfig) error
	GetTopicConfig(ctx context.Context, topic string) (TopicConfig, error)

	// Enhanced lifecycle operations
	Initialize(ctx context.Context) error
	Close() error
	GetState() RepositoryState
	Reopen(ctx context.Context) error
	Reset(ctx context.Context) error

	// Enhanced health check
	Health(ctx context.Context) (map[string]interface{}, error)
	HealthInfo(ctx context.Context) (HealthInfo, error)
}
