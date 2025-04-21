package models

import (
	"log"
	"os"
	"testing"

	"goeventsource/src/internal/port/outbound"
)

func TestNewEventStreamConfig(t *testing.T) {
	config := NewEventStreamConfig("test-stream")

	if config.Name != "test-stream" {
		t.Errorf("Expected name to be 'test-stream', got '%s'", config.Name)
	}

	if config.Adapter != DefaultAdapter {
		t.Errorf("Expected adapter to be '%s', got '%s'", DefaultAdapter, config.Adapter)
	}

	if config.Connection != "" {
		t.Errorf("Expected empty connection, got '%s'", config.Connection)
	}

	if config.Options == nil {
		t.Error("Expected options to be initialized, got nil")
	}

	if len(config.Options) != 0 {
		t.Errorf("Expected empty options map, got %d items", len(config.Options))
	}
}

func TestEventStreamConfig_Validate(t *testing.T) {
	tests := []struct {
		name      string
		config    EventStreamConfig
		expectErr bool
	}{
		{
			name: "Valid config",
			config: EventStreamConfig{
				Name:    "valid-stream",
				Adapter: "memory",
			},
			expectErr: false,
		},
		{
			name: "Empty name",
			config: EventStreamConfig{
				Name:    "",
				Adapter: "memory",
			},
			expectErr: true,
		},
		{
			name: "Empty adapter",
			config: EventStreamConfig{
				Name:    "valid-stream",
				Adapter: "",
			},
			expectErr: true,
		},
		{
			name: "Empty connection is valid for memory adapter",
			config: EventStreamConfig{
				Name:       "valid-stream",
				Adapter:    "memory",
				Connection: "",
			},
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.expectErr && err == nil {
				t.Error("Expected error but got nil")
			}
			if !tt.expectErr && err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}
		})
	}
}

func TestEventStreamConfig_Conversion(t *testing.T) {
	// Test conversion to TopicConfig
	original := EventStreamConfig{
		Name:       "test-stream",
		Adapter:    "fs",
		Connection: "/tmp/events",
		Options: map[string]string{
			"createDirIfNotExist": "true",
		},
	}

	topicConfig := original.ToTopicConfig()

	if topicConfig.Name != original.Name {
		t.Errorf("Expected name to be '%s', got '%s'", original.Name, topicConfig.Name)
	}

	if topicConfig.Adapter != original.Adapter {
		t.Errorf("Expected adapter to be '%s', got '%s'", original.Adapter, topicConfig.Adapter)
	}

	if topicConfig.Connection != original.Connection {
		t.Errorf("Expected connection to be '%s', got '%s'", original.Connection, topicConfig.Connection)
	}

	if len(topicConfig.Options) != len(original.Options) {
		t.Errorf("Expected options map to have %d items, got %d", len(original.Options), len(topicConfig.Options))
	}

	// Test conversion from TopicConfig
	tc := outbound.TopicConfig{
		Name:       "another-stream",
		Adapter:    "postgres",
		Connection: "postgres://localhost/events",
		Options: map[string]string{
			"schema": "public",
			"table":  "events",
		},
	}

	converted := FromTopicConfig(tc)

	if converted.Name != tc.Name {
		t.Errorf("Expected name to be '%s', got '%s'", tc.Name, converted.Name)
	}

	if converted.Adapter != tc.Adapter {
		t.Errorf("Expected adapter to be '%s', got '%s'", tc.Adapter, converted.Adapter)
	}

	if converted.Connection != tc.Connection {
		t.Errorf("Expected connection to be '%s', got '%s'", tc.Connection, converted.Connection)
	}

	if len(converted.Options) != len(tc.Options) {
		t.Errorf("Expected options map to have %d items, got %d", len(tc.Options), len(converted.Options))
	}

	// Verify option values
	for k, v := range tc.Options {
		if converted.Options[k] != v {
			t.Errorf("Expected option '%s' to be '%s', got '%s'", k, v, converted.Options[k])
		}
	}
}

func TestNewEventStoreConfig(t *testing.T) {
	config := NewEventStoreConfig()

	if config.ConfigStream.Name != "configuration" {
		t.Errorf("Expected config stream name to be 'configuration', got '%s'", config.ConfigStream.Name)
	}

	if config.ConfigStream.Adapter != "memory" {
		t.Errorf("Expected config stream adapter to be 'memory', got '%s'", config.ConfigStream.Adapter)
	}

	if config.Logger == nil {
		t.Error("Expected logger to be initialized, got nil")
	}

	// Check default option for config stream
	if val, exists := config.ConfigStream.Options["inmemory"]; !exists || val != "true" {
		t.Errorf("Expected 'inmemory' option to be 'true', got '%s'", val)
	}
}

func TestEventStoreConfig_Validate(t *testing.T) {
	tests := []struct {
		name      string
		config    EventStoreConfig
		expectErr bool
	}{
		{
			name: "Valid config",
			config: EventStoreConfig{
				ConfigStream: EventStreamConfig{
					Name:    "configuration",
					Adapter: "memory",
				},
				Logger: log.New(os.Stderr, "", log.LstdFlags),
			},
			expectErr: false,
		},
		{
			name: "Invalid config stream",
			config: EventStoreConfig{
				ConfigStream: EventStreamConfig{
					Name:    "", // Invalid: empty name
					Adapter: "memory",
				},
				Logger: log.New(os.Stderr, "", log.LstdFlags),
			},
			expectErr: true,
		},
		{
			name: "Nil logger",
			config: EventStoreConfig{
				ConfigStream: EventStreamConfig{
					Name:    "configuration",
					Adapter: "memory",
				},
				Logger: nil,
			},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.expectErr && err == nil {
				t.Error("Expected error but got nil")
			}
			if !tt.expectErr && err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}
		})
	}
}

func TestEventStoreConfig_WithLogger(t *testing.T) {
	config := NewEventStoreConfig()

	// Test with valid logger
	customLogger := log.New(os.Stdout, "[TEST] ", log.LstdFlags)
	updatedConfig := config.WithLogger(customLogger)

	if updatedConfig.Logger != customLogger {
		t.Error("Expected logger to be updated with custom logger")
	}

	// Test with nil logger (should keep original)
	unchanged := updatedConfig.WithLogger(nil)

	if unchanged.Logger != customLogger {
		t.Error("Expected logger to remain unchanged when nil logger provided")
	}
}

func TestEventStoreConfig_WithConfigStream(t *testing.T) {
	config := NewEventStoreConfig()

	// Test with valid config stream
	customConfigStream := EventStreamConfig{
		Name:       "custom-config",
		Adapter:    "fs",
		Connection: "/tmp/config",
		Options:    map[string]string{"createDirIfNotExist": "true"},
	}

	updatedConfig := config.WithConfigStream(customConfigStream)

	if updatedConfig.ConfigStream.Name != customConfigStream.Name {
		t.Errorf("Expected config stream name to be '%s', got '%s'",
			customConfigStream.Name, updatedConfig.ConfigStream.Name)
	}

	// Test with invalid config stream (should keep original)
	invalidConfigStream := EventStreamConfig{
		Name:    "", // Invalid: empty name
		Adapter: "memory",
	}

	unchanged := updatedConfig.WithConfigStream(invalidConfigStream)

	if unchanged.ConfigStream.Name != customConfigStream.Name {
		t.Error("Expected config stream to remain unchanged when invalid config stream provided")
	}
}
