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

	// Lifecycle operations
	Initialize(ctx context.Context) error
	Close() error

	// Health check
	Health(ctx context.Context) (map[string]interface{}, error)
}
