// Package models contains domain models for the EventStore.
package models

import (
	"errors"
	"fmt"
	"log"
	"os"

	"goeventsource/src/internal/port/outbound"
)

// Default values for event stream configuration
const (
	DefaultAdapter = "memory"
)

// EventStreamConfig represents the configuration for an event stream
type EventStreamConfig struct {
	Name       string            `json:"name"`
	Adapter    string            `json:"adapter"`
	Connection string            `json:"connection"`
	Options    map[string]string `json:"options"`
}

// NewEventStreamConfig creates a new EventStreamConfig with default values
func NewEventStreamConfig(name string) EventStreamConfig {
	return EventStreamConfig{
		Name:       name,
		Adapter:    DefaultAdapter,
		Connection: "",
		Options:    make(map[string]string),
	}
}

// Validate ensures that the EventStreamConfig has all required fields
func (c EventStreamConfig) Validate() error {
	if c.Name == "" {
		return errors.New("event stream name cannot be empty")
	}

	if c.Adapter == "" {
		return errors.New("adapter type cannot be empty")
	}

	// Connection can be empty for some adapters (like memory)
	// Specific adapter implementations should perform their own validation

	return nil
}

// ToTopicConfig converts EventStreamConfig to TopicConfig for compatibility with existing code
func (c EventStreamConfig) ToTopicConfig() outbound.TopicConfig {
	return outbound.TopicConfig{
		Name:       c.Name,
		Adapter:    c.Adapter,
		Connection: c.Connection,
		Options:    c.Options,
	}
}

// FromTopicConfig creates an EventStreamConfig from a TopicConfig
func FromTopicConfig(config outbound.TopicConfig) EventStreamConfig {
	return EventStreamConfig{
		Name:       config.Name,
		Adapter:    config.Adapter,
		Connection: config.Connection,
		Options:    config.Options,
	}
}

// EventStoreConfig represents the configuration for the EventStore
type EventStoreConfig struct {
	ConfigStream EventStreamConfig `json:"config_stream"`
	Logger       *log.Logger       `json:"-"` // Not serialized
}

// NewEventStoreConfig creates a new EventStoreConfig with default values
func NewEventStoreConfig() EventStoreConfig {
	configStream := NewEventStreamConfig("configuration")
	configStream.Adapter = "memory"
	configStream.Options = map[string]string{
		"inmemory": "true",
	}

	return EventStoreConfig{
		ConfigStream: configStream,
		Logger:       log.New(os.Stderr, "[EVENT-STORE] ", log.LstdFlags),
	}
}

// Validate ensures that the EventStoreConfig has all required fields
func (c EventStoreConfig) Validate() error {
	if err := c.ConfigStream.Validate(); err != nil {
		return fmt.Errorf("invalid config stream configuration: %w", err)
	}

	if c.Logger == nil {
		return errors.New("logger cannot be nil")
	}

	return nil
}

// WithLogger sets a custom logger for the EventStoreConfig
func (c EventStoreConfig) WithLogger(logger *log.Logger) EventStoreConfig {
	if logger != nil {
		c.Logger = logger
	}
	return c
}

// WithConfigStream sets a custom configuration stream for the EventStoreConfig
func (c EventStoreConfig) WithConfigStream(config EventStreamConfig) EventStoreConfig {
	if err := config.Validate(); err == nil {
		c.ConfigStream = config
	}
	return c
}
