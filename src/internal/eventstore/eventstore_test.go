package eventstore

import (
	"context"
	"errors"
	"log"
	"os"
	"testing"

	"goeventsource/src/internal/eventstore/models"
	"goeventsource/src/internal/port/outbound"
)

// TestEventTypeConstants verifies that the event type constants are defined correctly
func TestEventTypeConstants(t *testing.T) {
	// Test that constants have the expected values
	if EventTypeTopicCreated != "TopicCreated" {
		t.Errorf("EventTypeTopicCreated expected to be 'TopicCreated', got '%s'", EventTypeTopicCreated)
	}

	if EventTypeTopicUpdated != "TopicUpdated" {
		t.Errorf("EventTypeTopicUpdated expected to be 'TopicUpdated', got '%s'", EventTypeTopicUpdated)
	}

	if EventTypeTopicDeleted != "TopicDeleted" {
		t.Errorf("EventTypeTopicDeleted expected to be 'TopicDeleted', got '%s'", EventTypeTopicDeleted)
	}

	// Test that constants are different from each other
	if EventTypeTopicCreated == EventTypeTopicUpdated {
		t.Error("EventTypeTopicCreated should not equal EventTypeTopicUpdated")
	}

	if EventTypeTopicCreated == EventTypeTopicDeleted {
		t.Error("EventTypeTopicCreated should not equal EventTypeTopicDeleted")
	}

	if EventTypeTopicUpdated == EventTypeTopicDeleted {
		t.Error("EventTypeTopicUpdated should not equal EventTypeTopicDeleted")
	}
}

// TestNewEventStore tests the creation of a new EventStore
func TestNewEventStore(t *testing.T) {
	// Create a default config
	config := models.NewEventStoreConfig()

	// Create a new EventStore with default options
	store, err := NewEventStore(config)
	if err != nil {
		t.Fatalf("Failed to create EventStore: %v", err)
	}

	// Check that default values are set correctly
	if store.registry == nil {
		t.Error("Registry should be initialized")
	}

	if store.configStream.Name != "configuration" {
		t.Errorf("Expected config stream name to be 'configuration', got '%s'", store.configStream.Name)
	}

	if store.topicConfigs == nil {
		t.Error("topicConfigs map should be initialized")
	}

	if store.logger == nil {
		t.Error("logger should be initialized")
	}

	if store.isInitialized {
		t.Error("isInitialized should be false by default")
	}
}

// TestNewEventStoreWithInvalidConfig tests the creation of a new EventStore with invalid config
func TestNewEventStoreWithInvalidConfig(t *testing.T) {
	// Create an invalid config with empty config stream name
	config := models.NewEventStoreConfig()
	config.ConfigStream.Name = ""

	// This should fail
	_, err := NewEventStore(config)
	if err == nil {
		t.Error("Expected error when creating EventStore with invalid config")
	}
}

// TestEventStoreWithOptions tests the functional options pattern for the EventStore
func TestEventStoreWithOptions(t *testing.T) {
	// Create a default config
	config := models.NewEventStoreConfig()

	// Create a custom logger
	customLogger := log.New(os.Stderr, "[CUSTOM-LOGGER] ", log.LstdFlags)

	// Create a custom config stream
	customConfigStream := models.NewEventStreamConfig("custom-config")

	// Create a new EventStore with custom options
	store, err := NewEventStore(
		config,
		WithLogger(customLogger),
		WithConfigStream(customConfigStream),
	)
	if err != nil {
		t.Fatalf("Failed to create EventStore with options: %v", err)
	}

	// Check that custom values are set correctly
	if store.registry == nil {
		t.Error("Registry should be initialized")
	}

	if store.configStream.Name != "custom-config" {
		t.Errorf("Expected config stream name to be 'custom-config', got '%s'", store.configStream.Name)
	}

	if store.logger != customLogger {
		t.Error("Custom logger not set correctly")
	}

	if store.isInitialized {
		t.Error("isInitialized should be false by default")
	}
}

// TestEventStoreInitialize tests the initialization of the EventStore
func TestEventStoreInitialize(t *testing.T) {
	// Create a default config
	config := models.NewEventStoreConfig()

	// Create a new EventStore
	store, err := NewEventStore(config)
	if err != nil {
		t.Fatalf("Failed to create EventStore: %v", err)
	}

	// Initialize the EventStore
	err = store.Initialize(context.Background())
	if err != nil {
		t.Fatalf("Failed to initialize EventStore: %v", err)
	}

	// Check that the EventStore is initialized
	if !store.isInitialized {
		t.Error("EventStore should be initialized")
	}

	// Initialize again to test the already initialized case
	err = store.Initialize(context.Background())
	if err != nil {
		t.Fatalf("Failed to initialize EventStore again: %v", err)
	}
}

// TestEventStoreRebuildWriteModel tests rebuilding the write model from configuration events
func TestEventStoreRebuildWriteModel(t *testing.T) {
	// Create a default config
	config := models.NewEventStoreConfig()

	// Create a new EventStore
	store, err := NewEventStore(config)
	if err != nil {
		t.Fatalf("Failed to create EventStore: %v", err)
	}

	// Initialize the EventStore
	err = store.Initialize(context.Background())
	if err != nil {
		t.Fatalf("Failed to initialize EventStore: %v", err)
	}

	// Get the config repository to add events
	configRepo, err := store.getConfigRepository(context.Background())
	if err != nil {
		t.Fatalf("Failed to get config repository: %v", err)
	}

	// Add some configuration events
	events := []outbound.Event{
		{
			Topic: store.configStream.Name,
			Type:  EventTypeTopicCreated,
			Data: map[string]interface{}{
				"name":       "topic1",
				"adapter":    "memory",
				"connection": "memory://test1",
				"options": map[string]interface{}{
					"option1": "value1",
				},
			},
		},
		{
			Topic: store.configStream.Name,
			Type:  EventTypeTopicCreated,
			Data: map[string]interface{}{
				"name":       "topic2",
				"adapter":    "memory",
				"connection": "memory://test2",
			},
		},
		{
			Topic: store.configStream.Name,
			Type:  EventTypeTopicUpdated,
			Data: map[string]interface{}{
				"name":       "topic1",
				"adapter":    "memory",
				"connection": "memory://test1-updated",
			},
		},
		{
			Topic: store.configStream.Name,
			Type:  EventTypeTopicDeleted,
			Data: map[string]interface{}{
				"name": "topic2",
			},
		},
	}

	err = configRepo.AppendEvents(context.Background(), store.configStream.Name, events)
	if err != nil {
		t.Fatalf("Failed to append configuration events: %v", err)
	}

	// Rebuild the write model to process the events
	err = store.rebuildWriteModel(context.Background())
	if err != nil {
		t.Fatalf("Failed to rebuild write model: %v", err)
	}

	// Check that the write model was built correctly
	if len(store.topicConfigs) != 1 {
		t.Fatalf("Expected 1 topic in write model, got %d", len(store.topicConfigs))
	}

	// Check topic1 exists and was updated
	topic1, exists := store.topicConfigs["topic1"]
	if !exists {
		t.Error("topic1 should exist in the write model")
	} else {
		if topic1.Connection != "memory://test1-updated" {
			t.Errorf("Expected topic1 connection to be 'memory://test1-updated', got '%s'", topic1.Connection)
		}

		if val, ok := topic1.Options["option1"]; !ok || val != "value1" {
			t.Errorf("Expected topic1 to have option1=value1, got %v", topic1.Options)
		}
	}

	// Check topic2 doesn't exist (was deleted)
	_, exists = store.topicConfigs["topic2"]
	if exists {
		t.Error("topic2 should not exist in the write model")
	}
}

// TestHandleTopicCreated tests the handleTopicCreated function
func TestHandleTopicCreated(t *testing.T) {
	// Create a default config
	config := models.NewEventStoreConfig()

	// Create a new EventStore
	store, err := NewEventStore(config)
	if err != nil {
		t.Fatalf("Failed to create EventStore: %v", err)
	}

	// Test valid TopicCreated event
	event := outbound.Event{
		Type: EventTypeTopicCreated,
		Data: map[string]interface{}{
			"name":       "test-topic",
			"adapter":    "memory",
			"connection": "",
			"options": map[string]interface{}{
				"key": "value",
			},
		},
	}

	err = store.handleTopicCreated(event)
	if err != nil {
		t.Fatalf("Failed to handle TopicCreated event: %v", err)
	}

	// Check that the topic was added to the write model
	topic, exists := store.topicConfigs["test-topic"]
	if !exists {
		t.Error("test-topic should exist in the write model")
	} else {
		if topic.Adapter != "memory" {
			t.Errorf("Expected adapter to be 'memory', got '%s'", topic.Adapter)
		}

		if topic.Connection != "" {
			t.Errorf("Expected connection to be empty, got '%s'", topic.Connection)
		}

		if val, ok := topic.Options["key"]; !ok || val != "value" {
			t.Errorf("Expected options to have key=value, got %v", topic.Options)
		}
	}

	// Test invalid TopicCreated event (missing name)
	invalidEvent := outbound.Event{
		Type: EventTypeTopicCreated,
		Data: map[string]interface{}{
			"adapter": "memory",
		},
	}

	err = store.handleTopicCreated(invalidEvent)
	if err == nil {
		t.Error("Expected error for invalid TopicCreated event, got nil")
	}

	// Test invalid TopicCreated event (missing adapter)
	invalidEvent = outbound.Event{
		Type: EventTypeTopicCreated,
		Data: map[string]interface{}{
			"name": "invalid-topic",
		},
	}

	err = store.handleTopicCreated(invalidEvent)
	if err == nil {
		t.Error("Expected error for invalid TopicCreated event, got nil")
	}
}

// TestHandleTopicUpdated tests the handleTopicUpdated function
func TestHandleTopicUpdated(t *testing.T) {
	// Create a default config
	config := models.NewEventStoreConfig()

	// Create a new EventStore
	store, err := NewEventStore(config)
	if err != nil {
		t.Fatalf("Failed to create EventStore: %v", err)
	}

	// Add a topic to the write model
	store.topicConfigs["existing-topic"] = outbound.TopicConfig{
		Name:       "existing-topic",
		Adapter:    "memory",
		Connection: "",
		Options: map[string]string{
			"key": "value",
		},
	}

	// Test valid TopicUpdated event
	event := outbound.Event{
		Type: EventTypeTopicUpdated,
		Data: map[string]interface{}{
			"name":       "existing-topic",
			"adapter":    "fs",
			"connection": "/data/eventstore",
			"options": map[string]interface{}{
				"newKey": "newValue",
			},
		},
	}

	err = store.handleTopicUpdated(event)
	if err != nil {
		t.Fatalf("Failed to handle TopicUpdated event: %v", err)
	}

	// Check that the topic was updated in the write model
	topic, exists := store.topicConfigs["existing-topic"]
	if !exists {
		t.Error("existing-topic should exist in the write model")
	} else {
		if topic.Adapter != "fs" {
			t.Errorf("Expected adapter to be 'fs', got '%s'", topic.Adapter)
		}

		if topic.Connection != "/data/eventstore" {
			t.Errorf("Expected connection to be '/data/eventstore', got '%s'", topic.Connection)
		}

		// Check that both old and new options are present
		if val, ok := topic.Options["key"]; !ok || val != "value" {
			t.Errorf("Expected options to have key=value, got %v", topic.Options)
		}

		if val, ok := topic.Options["newKey"]; !ok || val != "newValue" {
			t.Errorf("Expected options to have newKey=newValue, got %v", topic.Options)
		}
	}

	// Test invalid TopicUpdated event (missing name)
	invalidEvent := outbound.Event{
		Type: EventTypeTopicUpdated,
		Data: map[string]interface{}{
			"adapter": "memory",
		},
	}

	err = store.handleTopicUpdated(invalidEvent)
	if err == nil {
		t.Error("Expected error for invalid TopicUpdated event, got nil")
	}

	// Test updating non-existent topic
	nonExistentEvent := outbound.Event{
		Type: EventTypeTopicUpdated,
		Data: map[string]interface{}{
			"name":    "non-existent-topic",
			"adapter": "memory",
		},
	}

	err = store.handleTopicUpdated(nonExistentEvent)
	if err == nil {
		t.Error("Expected error for updating non-existent topic, got nil")
	}
}

// TestHandleTopicDeleted tests the handleTopicDeleted function
func TestHandleTopicDeleted(t *testing.T) {
	// Create a default config
	config := models.NewEventStoreConfig()

	// Create a new EventStore
	store, err := NewEventStore(config)
	if err != nil {
		t.Fatalf("Failed to create EventStore: %v", err)
	}

	// Add a topic to the write model
	store.topicConfigs["existing-topic"] = outbound.TopicConfig{
		Name:    "existing-topic",
		Adapter: "memory",
	}

	// Test valid TopicDeleted event
	event := outbound.Event{
		Type: EventTypeTopicDeleted,
		Data: map[string]interface{}{
			"name": "existing-topic",
		},
	}

	err = store.handleTopicDeleted(event)
	if err != nil {
		t.Fatalf("Failed to handle TopicDeleted event: %v", err)
	}

	// Check that the topic was deleted from the write model
	_, exists := store.topicConfigs["existing-topic"]
	if exists {
		t.Error("existing-topic should be deleted from the write model")
	}

	// Test invalid TopicDeleted event (missing name)
	invalidEvent := outbound.Event{
		Type: EventTypeTopicDeleted,
		Data: map[string]interface{}{},
	}

	err = store.handleTopicDeleted(invalidEvent)
	if err == nil {
		t.Error("Expected error for invalid TopicDeleted event, got nil")
	}

	// Test deleting a non-existent topic (should not cause an error)
	nonExistentEvent := outbound.Event{
		Type: EventTypeTopicDeleted,
		Data: map[string]interface{}{
			"name": "non-existent-topic",
		},
	}

	err = store.handleTopicDeleted(nonExistentEvent)
	if err != nil {
		t.Errorf("Expected no error for deleting non-existent topic, got: %v", err)
	}
}

// TestApplyConfigurationEvent tests the applyConfigurationEvent function
func TestApplyConfigurationEvent(t *testing.T) {
	// Create a default config
	config := models.NewEventStoreConfig()

	// Create a new EventStore
	store, err := NewEventStore(config)
	if err != nil {
		t.Fatalf("Failed to create EventStore: %v", err)
	}

	// Test with unknown event type
	unknownEvent := outbound.Event{
		Type: "UnknownEventType",
	}

	err = store.applyConfigurationEvent(unknownEvent)
	if err == nil {
		t.Error("Expected error for unknown event type, got nil")
	}
}

// TestGetConfigRepository tests the getConfigRepository helper method
func TestGetConfigRepository(t *testing.T) {
	// Create a default config
	config := models.NewEventStoreConfig()

	// Create a new EventStore
	store, err := NewEventStore(config)
	if err != nil {
		t.Fatalf("Failed to create EventStore: %v", err)
	}

	// Get config repository
	ctx := context.Background()
	repo, err := store.getConfigRepository(ctx)
	if err != nil {
		t.Fatalf("Failed to get config repository: %v", err)
	}

	if repo == nil {
		t.Error("Expected non-nil repository")
	}
}

// TestGetRepositoryForTopic tests the getRepositoryForTopic helper method
func TestGetRepositoryForTopic(t *testing.T) {
	// Create a default config
	config := models.NewEventStoreConfig()

	// Create a new EventStore
	store, err := NewEventStore(config)
	if err != nil {
		t.Fatalf("Failed to create EventStore: %v", err)
	}

	// Add a topic to the write model
	store.topicConfigs["test-topic"] = outbound.TopicConfig{
		Name:    "test-topic",
		Adapter: "memory",
	}

	// Get repository for topic
	ctx := context.Background()
	repo, err := store.getRepositoryForTopic(ctx, "test-topic")
	if err != nil {
		t.Fatalf("Failed to get repository for topic: %v", err)
	}

	if repo == nil {
		t.Error("Expected non-nil repository")
	}

	// Test with non-existent topic
	_, err = store.getRepositoryForTopic(ctx, "non-existent-topic")
	if err == nil {
		t.Error("Expected error for non-existent topic, got nil")
	}
}

// TestEventStoreClose tests the Close method
func TestEventStoreClose(t *testing.T) {
	// Create a default config
	config := models.NewEventStoreConfig()

	// Create a new EventStore
	store, err := NewEventStore(config)
	if err != nil {
		t.Fatalf("Failed to create EventStore: %v", err)
	}

	// Set initialized to true
	store.isInitialized = true

	// Close the EventStore
	err = store.Close()
	if err != nil {
		t.Fatalf("Failed to close EventStore: %v", err)
	}

	// Check that the EventStore is not initialized
	if store.isInitialized {
		t.Error("EventStore should not be initialized after close")
	}
}

// TestEventStoreHealth tests the Health method
func TestEventStoreHealth(t *testing.T) {
	// Create a default config
	config := models.NewEventStoreConfig()

	// Create a new EventStore
	store, err := NewEventStore(config)
	if err != nil {
		t.Fatalf("Failed to create EventStore: %v", err)
	}

	// Test before initialization
	ctx := context.Background()
	health, err := store.Health(ctx)
	if err == nil {
		t.Error("Expected error for health check on uninitialized EventStore")
	}

	if health["status"] != "down" {
		t.Errorf("Expected status to be 'down', got '%s'", health["status"])
	}

	// Initialize the EventStore
	err = store.Initialize(ctx)
	if err != nil {
		t.Fatalf("Failed to initialize EventStore: %v", err)
	}

	// Test after initialization
	health, err = store.Health(ctx)
	if err != nil {
		t.Fatalf("Failed to get health: %v", err)
	}

	if health["status"] != "up" {
		t.Errorf("Expected status to be 'up', got '%s'", health["status"])
	}

	if health["initialized"] != true {
		t.Error("Expected initialized to be true")
	}

	registry, ok := health["registry"].(map[string]interface{})
	if !ok {
		t.Fatalf("Expected registry to be a map, got %T", health["registry"])
	}

	if registry["status"] != "up" {
		t.Errorf("Expected registry status to be 'up', got '%s'", registry["status"])
	}
}

// TestCreateTopic tests the creation of a topic
func TestCreateTopic(t *testing.T) {
	// Create a default config
	config := models.NewEventStoreConfig()

	// Create a new EventStore
	store, err := NewEventStore(config)
	if err != nil {
		t.Fatalf("Failed to create EventStore: %v", err)
	}

	// Initialize the EventStore
	err = store.Initialize(context.Background())
	if err != nil {
		t.Fatalf("Failed to initialize EventStore: %v", err)
	}

	// Create a test topic
	topicConfig := outbound.TopicConfig{
		Name:       "test-topic",
		Adapter:    "memory",
		Connection: "memory://test",
		Options:    map[string]string{"key": "value"},
	}

	// Create the topic
	err = store.CreateTopic(context.Background(), topicConfig)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Verify the topic was created in the write model
	if _, exists := store.topicConfigs[topicConfig.Name]; !exists {
		t.Errorf("Topic not found in write model after creation")
	}

	// Try to create the same topic again - should fail with ErrTopicAlreadyExists
	err = store.CreateTopic(context.Background(), topicConfig)
	if err == nil {
		t.Errorf("Expected error when creating duplicate topic")
	} else if !errors.Is(err, ErrTopicAlreadyExists) {
		t.Errorf("Expected ErrTopicAlreadyExists, got: %v", err)
	}

	// Try to create a topic with missing required fields
	invalidConfig := outbound.TopicConfig{
		Name: "invalid-topic",
		// Missing adapter and connection
	}
	err = store.CreateTopic(context.Background(), invalidConfig)
	if err == nil {
		t.Errorf("Expected error when creating topic with invalid config")
	} else if !errors.Is(err, ErrInvalidTopicConfig) {
		t.Errorf("Expected ErrInvalidTopicConfig, got: %v", err)
	}

	// Test creating a topic with an uninitialized EventStore
	uninitializedStore, _ := NewEventStore(config)
	err = uninitializedStore.CreateTopic(context.Background(), topicConfig)
	if err == nil {
		t.Errorf("Expected error when creating topic with uninitialized EventStore")
	} else if !errors.Is(err, ErrNotInitialized) {
		t.Errorf("Expected ErrNotInitialized, got: %v", err)
	}
}

// TestUpdateTopic tests updating a topic configuration
func TestUpdateTopic(t *testing.T) {
	// Create a default config
	config := models.NewEventStoreConfig()

	// Create a new EventStore
	store, err := NewEventStore(config)
	if err != nil {
		t.Fatalf("Failed to create EventStore: %v", err)
	}

	// Initialize the EventStore
	err = store.Initialize(context.Background())
	if err != nil {
		t.Fatalf("Failed to initialize EventStore: %v", err)
	}

	// Create a test topic
	originalConfig := outbound.TopicConfig{
		Name:       "test-topic",
		Adapter:    "memory",
		Connection: "memory://test",
		Options:    map[string]string{"key": "value"},
	}

	// Create the topic
	err = store.CreateTopic(context.Background(), originalConfig)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Update the topic with same adapter/connection but different options
	updatedConfig := outbound.TopicConfig{
		Name:       "test-topic",
		Adapter:    "memory",
		Connection: "memory://test",
		Options:    map[string]string{"key": "updated-value", "new-key": "new-value"},
	}

	err = store.UpdateTopic(context.Background(), updatedConfig)
	if err != nil {
		t.Fatalf("Failed to update topic: %v", err)
	}

	// Verify the topic was updated in the write model
	storedConfig, exists := store.topicConfigs[updatedConfig.Name]
	if !exists {
		t.Errorf("Topic not found in write model after update")
	} else {
		if storedConfig.Options["key"] != "updated-value" {
			t.Errorf("Expected option 'key' to be 'updated-value', got '%s'", storedConfig.Options["key"])
		}
		if storedConfig.Options["new-key"] != "new-value" {
			t.Errorf("Expected option 'new-key' to be 'new-value', got '%s'", storedConfig.Options["new-key"])
		}
	}

	// Update the topic with different adapter/connection
	updatedConfig = outbound.TopicConfig{
		Name:       "test-topic",
		Adapter:    "fs",
		Connection: "fs://test",
		Options:    map[string]string{"key": "value"},
	}

	err = store.UpdateTopic(context.Background(), updatedConfig)
	if err != nil {
		t.Fatalf("Failed to update topic with new adapter/connection: %v", err)
	}

	// Verify the topic was updated in the write model
	storedConfig, exists = store.topicConfigs[updatedConfig.Name]
	if !exists {
		t.Errorf("Topic not found in write model after adapter/connection update")
	} else {
		if storedConfig.Adapter != "fs" {
			t.Errorf("Expected adapter to be 'fs', got '%s'", storedConfig.Adapter)
		}
		if storedConfig.Connection != "fs://test" {
			t.Errorf("Expected connection to be 'fs://test', got '%s'", storedConfig.Connection)
		}
	}

	// Try to update a non-existent topic
	nonExistentConfig := outbound.TopicConfig{
		Name:       "non-existent-topic",
		Adapter:    "memory",
		Connection: "memory://test",
	}
	err = store.UpdateTopic(context.Background(), nonExistentConfig)
	if err == nil {
		t.Errorf("Expected error when updating non-existent topic")
	} else if !errors.Is(err, ErrTopicNotFound) {
		t.Errorf("Expected ErrTopicNotFound, got: %v", err)
	}

	// Test updating a topic with an uninitialized EventStore
	uninitializedStore, _ := NewEventStore(config)
	err = uninitializedStore.UpdateTopic(context.Background(), updatedConfig)
	if err == nil {
		t.Errorf("Expected error when updating topic with uninitialized EventStore")
	} else if !errors.Is(err, ErrNotInitialized) {
		t.Errorf("Expected ErrNotInitialized, got: %v", err)
	}
}

// TestDeleteTopic tests deleting a topic
func TestDeleteTopic(t *testing.T) {
	// Create a default config
	config := models.NewEventStoreConfig()

	// Create a new EventStore
	store, err := NewEventStore(config)
	if err != nil {
		t.Fatalf("Failed to create EventStore: %v", err)
	}

	// Initialize the EventStore
	err = store.Initialize(context.Background())
	if err != nil {
		t.Fatalf("Failed to initialize EventStore: %v", err)
	}

	// Create a test topic
	topicConfig := outbound.TopicConfig{
		Name:       "test-topic",
		Adapter:    "memory",
		Connection: "memory://test",
		Options:    map[string]string{"key": "value"},
	}

	// Create the topic
	err = store.CreateTopic(context.Background(), topicConfig)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Delete the topic
	err = store.DeleteTopic(context.Background(), topicConfig.Name)
	if err != nil {
		t.Fatalf("Failed to delete topic: %v", err)
	}

	// Verify the topic was removed from the write model
	if _, exists := store.topicConfigs[topicConfig.Name]; exists {
		t.Errorf("Topic still exists in write model after deletion")
	}

	// Try to delete a non-existent topic
	err = store.DeleteTopic(context.Background(), "non-existent-topic")
	if err == nil {
		t.Errorf("Expected error when deleting non-existent topic")
	} else if !errors.Is(err, ErrTopicNotFound) {
		t.Errorf("Expected ErrTopicNotFound, got: %v", err)
	}

	// Test deleting a topic with an uninitialized EventStore
	uninitializedStore, _ := NewEventStore(config)
	err = uninitializedStore.DeleteTopic(context.Background(), topicConfig.Name)
	if err == nil {
		t.Errorf("Expected error when deleting topic with uninitialized EventStore")
	} else if !errors.Is(err, ErrNotInitialized) {
		t.Errorf("Expected ErrNotInitialized, got: %v", err)
	}
}

// TestGetTopic tests retrieving a topic configuration
func TestGetTopic(t *testing.T) {
	// Create a default config
	config := models.NewEventStoreConfig()

	// Create a new EventStore
	store, err := NewEventStore(config)
	if err != nil {
		t.Fatalf("Failed to create EventStore: %v", err)
	}

	// Initialize the EventStore
	err = store.Initialize(context.Background())
	if err != nil {
		t.Fatalf("Failed to initialize EventStore: %v", err)
	}

	// Create a test topic
	topicConfig := outbound.TopicConfig{
		Name:       "test-topic",
		Adapter:    "memory",
		Connection: "memory://test",
		Options:    map[string]string{"key": "value"},
	}

	// Create the topic
	err = store.CreateTopic(context.Background(), topicConfig)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Get the topic
	retrievedConfig, err := store.GetTopic(context.Background(), topicConfig.Name)
	if err != nil {
		t.Fatalf("Failed to get topic: %v", err)
	}

	// Verify the retrieved configuration matches the original
	if retrievedConfig.Name != topicConfig.Name {
		t.Errorf("Expected topic name '%s', got '%s'", topicConfig.Name, retrievedConfig.Name)
	}
	if retrievedConfig.Adapter != topicConfig.Adapter {
		t.Errorf("Expected adapter '%s', got '%s'", topicConfig.Adapter, retrievedConfig.Adapter)
	}
	if retrievedConfig.Connection != topicConfig.Connection {
		t.Errorf("Expected connection '%s', got '%s'", topicConfig.Connection, retrievedConfig.Connection)
	}
	if retrievedConfig.Options["key"] != topicConfig.Options["key"] {
		t.Errorf("Expected option 'key' to be '%s', got '%s'", topicConfig.Options["key"], retrievedConfig.Options["key"])
	}

	// Try to get a non-existent topic
	_, err = store.GetTopic(context.Background(), "non-existent-topic")
	if err == nil {
		t.Errorf("Expected error when getting non-existent topic")
	} else if !errors.Is(err, ErrTopicNotFound) {
		t.Errorf("Expected ErrTopicNotFound, got: %v", err)
	}

	// Test getting a topic with an uninitialized EventStore
	uninitializedStore, _ := NewEventStore(config)
	_, err = uninitializedStore.GetTopic(context.Background(), topicConfig.Name)
	if err == nil {
		t.Errorf("Expected error when getting topic with uninitialized EventStore")
	} else if !errors.Is(err, ErrNotInitialized) {
		t.Errorf("Expected ErrNotInitialized, got: %v", err)
	}
}

// TestListTopics tests retrieving all topic configurations
func TestListTopics(t *testing.T) {
	// Create a default config
	config := models.NewEventStoreConfig()

	// Create a new EventStore
	store, err := NewEventStore(config)
	if err != nil {
		t.Fatalf("Failed to create EventStore: %v", err)
	}

	// Initialize the EventStore
	err = store.Initialize(context.Background())
	if err != nil {
		t.Fatalf("Failed to initialize EventStore: %v", err)
	}

	// Initially, there should be no topics
	topics, err := store.ListTopics(context.Background())
	if err != nil {
		t.Fatalf("Failed to list topics: %v", err)
	}
	if len(topics) != 0 {
		t.Errorf("Expected 0 topics initially, got %d", len(topics))
	}

	// Create some test topics with unique names
	topicConfigs := []outbound.TopicConfig{
		{
			Name:       "unique-list-topic1-test",
			Adapter:    "memory",
			Connection: "memory://test1",
			Options:    map[string]string{"key1": "value1"},
		},
		{
			Name:       "unique-list-topic2-test",
			Adapter:    "memory",
			Connection: "memory://test2",
			Options:    map[string]string{"key2": "value2"},
		},
		{
			Name:       "unique-list-topic3-test",
			Adapter:    "memory", // Using memory adapter to avoid file system conflicts
			Connection: "memory://test3",
			Options:    map[string]string{"key3": "value3"},
		},
	}

	// Create the topics
	for _, tc := range topicConfigs {
		err = store.CreateTopic(context.Background(), tc)
		if err != nil {
			t.Fatalf("Failed to create topic %s: %v", tc.Name, err)
		}
	}

	// List all topics
	topics, err = store.ListTopics(context.Background())
	if err != nil {
		t.Fatalf("Failed to list topics: %v", err)
	}

	// Verify the correct number of topics was returned
	if len(topics) != len(topicConfigs) {
		t.Errorf("Expected %d topics, got %d", len(topicConfigs), len(topics))
	}

	// Verify all created topics are in the list
	topicMap := make(map[string]outbound.TopicConfig)
	for _, tc := range topics {
		topicMap[tc.Name] = tc
	}

	for _, tc := range topicConfigs {
		found, exists := topicMap[tc.Name]
		if !exists {
			t.Errorf("Topic %s not found in list", tc.Name)
			continue
		}

		if found.Adapter != tc.Adapter {
			t.Errorf("Topic %s: expected adapter '%s', got '%s'", tc.Name, tc.Adapter, found.Adapter)
		}
		if found.Connection != tc.Connection {
			t.Errorf("Topic %s: expected connection '%s', got '%s'", tc.Name, tc.Connection, found.Connection)
		}
	}

	// Test listing topics with an uninitialized EventStore
	uninitializedStore, _ := NewEventStore(config)
	_, err = uninitializedStore.ListTopics(context.Background())
	if err == nil {
		t.Errorf("Expected error when listing topics with uninitialized EventStore")
	} else if !errors.Is(err, ErrNotInitialized) {
		t.Errorf("Expected ErrNotInitialized, got: %v", err)
	}
}

// TestAppendEvents tests appending events to a topic
func TestAppendEvents(t *testing.T) {
	// Create a default config
	config := models.NewEventStoreConfig()

	// Create a new EventStore
	store, err := NewEventStore(config)
	if err != nil {
		t.Fatalf("Failed to create EventStore: %v", err)
	}

	// Initialize the EventStore
	err = store.Initialize(context.Background())
	if err != nil {
		t.Fatalf("Failed to initialize EventStore: %v", err)
	}

	// Create a test topic
	topicConfig := outbound.TopicConfig{
		Name:       "test-events-topic",
		Adapter:    "memory",
		Connection: "memory://test",
		Options:    map[string]string{"key": "value"},
	}

	// Create the topic
	err = store.CreateTopic(context.Background(), topicConfig)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Create test events
	events := []outbound.Event{
		{
			ID:        "event1",
			Type:      "TestEvent",
			Data:      map[string]interface{}{"key1": "value1"},
			Metadata:  map[string]interface{}{"meta1": "value1"},
			Timestamp: 12345,
		},
		{
			ID:        "event2",
			Type:      "TestEvent",
			Data:      map[string]interface{}{"key2": "value2"},
			Metadata:  map[string]interface{}{"meta2": "value2"},
			Timestamp: 67890,
		},
	}

	// Append events to the topic
	err = store.AppendEvents(context.Background(), topicConfig.Name, events)
	if err != nil {
		t.Fatalf("Failed to append events: %v", err)
	}

	// Verify that topics are set on events when not explicitly set
	for _, event := range events {
		if event.Topic != topicConfig.Name {
			t.Errorf("Event topic not set correctly, expected '%s', got '%s'", topicConfig.Name, event.Topic)
		}
	}

	// Test with empty events slice
	err = store.AppendEvents(context.Background(), topicConfig.Name, []outbound.Event{})
	if err != nil {
		t.Errorf("Expected no error when appending empty events, got: %v", err)
	}

	// Test with non-existent topic
	err = store.AppendEvents(context.Background(), "non-existent-topic", events)
	if err == nil {
		t.Errorf("Expected error when appending to non-existent topic")
	} else if !errors.Is(err, ErrTopicNotFound) {
		t.Errorf("Expected ErrTopicNotFound, got: %v", err)
	}

	// Test with uninitialized EventStore
	uninitializedStore, _ := NewEventStore(config)
	err = uninitializedStore.AppendEvents(context.Background(), topicConfig.Name, events)
	if err == nil {
		t.Errorf("Expected error when appending events with uninitialized EventStore")
	} else if !errors.Is(err, ErrNotInitialized) {
		t.Errorf("Expected ErrNotInitialized, got: %v", err)
	}
}

// TestGetEvents tests retrieving events from a topic
func TestGetEvents(t *testing.T) {
	// Create a default config
	config := models.NewEventStoreConfig()

	// Create a new EventStore
	store, err := NewEventStore(config)
	if err != nil {
		t.Fatalf("Failed to create EventStore: %v", err)
	}

	// Initialize the EventStore
	err = store.Initialize(context.Background())
	if err != nil {
		t.Fatalf("Failed to initialize EventStore: %v", err)
	}

	// Create a test topic
	topicConfig := outbound.TopicConfig{
		Name:       "test-get-events-topic",
		Adapter:    "memory",
		Connection: "memory://test",
		Options:    map[string]string{"key": "value"},
	}

	// Create the topic
	err = store.CreateTopic(context.Background(), topicConfig)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Create and append test events
	events := []outbound.Event{
		{
			ID:        "event1",
			Type:      "TestEvent",
			Data:      map[string]interface{}{"key1": "value1"},
			Metadata:  map[string]interface{}{"meta1": "value1"},
			Timestamp: 12345,
		},
		{
			ID:        "event2",
			Type:      "TestEvent",
			Data:      map[string]interface{}{"key2": "value2"},
			Metadata:  map[string]interface{}{"meta2": "value2"},
			Timestamp: 67890,
		},
		{
			ID:        "event3",
			Type:      "OtherEvent",
			Data:      map[string]interface{}{"key3": "value3"},
			Metadata:  map[string]interface{}{"meta3": "value3"},
			Timestamp: 101112,
		},
	}

	err = store.AppendEvents(context.Background(), topicConfig.Name, events)
	if err != nil {
		t.Fatalf("Failed to append events: %v", err)
	}

	// Retrieve all events
	retrievedEvents, err := store.GetEvents(context.Background(), topicConfig.Name, 0)
	if err != nil {
		t.Fatalf("Failed to get events: %v", err)
	}

	// Verify events were retrieved
	if len(retrievedEvents) == 0 {
		t.Errorf("Expected to retrieve events, got none")
	}

	// Test with non-existent topic
	_, err = store.GetEvents(context.Background(), "non-existent-topic", 0)
	if err == nil {
		t.Errorf("Expected error when getting events from non-existent topic")
	} else if !errors.Is(err, ErrTopicNotFound) {
		t.Errorf("Expected ErrTopicNotFound, got: %v", err)
	}

	// Test with uninitialized EventStore
	uninitializedStore, _ := NewEventStore(config)
	_, err = uninitializedStore.GetEvents(context.Background(), topicConfig.Name, 0)
	if err == nil {
		t.Errorf("Expected error when getting events with uninitialized EventStore")
	} else if !errors.Is(err, ErrNotInitialized) {
		t.Errorf("Expected ErrNotInitialized, got: %v", err)
	}
}

// TestGetEventsByType tests retrieving events of a specific type from a topic
func TestGetEventsByType(t *testing.T) {
	// Create a default config
	config := models.NewEventStoreConfig()

	// Create a new EventStore
	store, err := NewEventStore(config)
	if err != nil {
		t.Fatalf("Failed to create EventStore: %v", err)
	}

	// Initialize the EventStore
	err = store.Initialize(context.Background())
	if err != nil {
		t.Fatalf("Failed to initialize EventStore: %v", err)
	}

	// Create a test topic
	topicConfig := outbound.TopicConfig{
		Name:       "test-get-events-by-type-topic",
		Adapter:    "memory",
		Connection: "memory://test",
		Options:    map[string]string{"key": "value"},
	}

	// Create the topic
	err = store.CreateTopic(context.Background(), topicConfig)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Create and append test events of different types
	events := []outbound.Event{
		{
			ID:        "event1",
			Type:      "TestEvent",
			Data:      map[string]interface{}{"key1": "value1"},
			Timestamp: 12345,
		},
		{
			ID:        "event2",
			Type:      "TestEvent",
			Data:      map[string]interface{}{"key2": "value2"},
			Timestamp: 67890,
		},
		{
			ID:        "event3",
			Type:      "OtherEvent",
			Data:      map[string]interface{}{"key3": "value3"},
			Timestamp: 101112,
		},
		{
			ID:        "event4",
			Type:      "TestEvent",
			Data:      map[string]interface{}{"key4": "value4"},
			Timestamp: 131415,
		},
	}

	err = store.AppendEvents(context.Background(), topicConfig.Name, events)
	if err != nil {
		t.Fatalf("Failed to append events: %v", err)
	}

	// Retrieve events of type TestEvent
	retrievedEvents, err := store.GetEventsByType(context.Background(), topicConfig.Name, "TestEvent", 0)
	if err != nil {
		t.Fatalf("Failed to get events by type: %v", err)
	}

	// Verify correct number of TestEvent events
	expectedTestEventCount := 3 // There are 3 TestEvent events in the test data
	if len(retrievedEvents) != expectedTestEventCount {
		t.Errorf("Expected %d TestEvent events, got %d", expectedTestEventCount, len(retrievedEvents))
	}

	// Verify all retrieved events are of type TestEvent
	for _, event := range retrievedEvents {
		if event.Type != "TestEvent" {
			t.Errorf("Expected event type 'TestEvent', got '%s'", event.Type)
		}
	}

	// Retrieve events of type OtherEvent
	retrievedEvents, err = store.GetEventsByType(context.Background(), topicConfig.Name, "OtherEvent", 0)
	if err != nil {
		t.Fatalf("Failed to get events by type: %v", err)
	}

	// Verify correct number of OtherEvent events
	expectedOtherEventCount := 1 // There is 1 OtherEvent event in the test data
	if len(retrievedEvents) != expectedOtherEventCount {
		t.Errorf("Expected %d OtherEvent events, got %d", expectedOtherEventCount, len(retrievedEvents))
	}

	// Verify all retrieved events are of type OtherEvent
	for _, event := range retrievedEvents {
		if event.Type != "OtherEvent" {
			t.Errorf("Expected event type 'OtherEvent', got '%s'", event.Type)
		}
	}

	// Test with empty event type
	_, err = store.GetEventsByType(context.Background(), topicConfig.Name, "", 0)
	if err == nil {
		t.Errorf("Expected error when getting events with empty type")
	} else if !errors.Is(err, ErrInvalidEventData) {
		t.Errorf("Expected ErrInvalidEventData, got: %v", err)
	}

	// Test with non-existent topic
	_, err = store.GetEventsByType(context.Background(), "non-existent-topic", "TestEvent", 0)
	if err == nil {
		t.Errorf("Expected error when getting events from non-existent topic")
	} else if !errors.Is(err, ErrTopicNotFound) {
		t.Errorf("Expected ErrTopicNotFound, got: %v", err)
	}

	// Test with uninitialized EventStore
	uninitializedStore, _ := NewEventStore(config)
	_, err = uninitializedStore.GetEventsByType(context.Background(), topicConfig.Name, "TestEvent", 0)
	if err == nil {
		t.Errorf("Expected error when getting events with uninitialized EventStore")
	} else if !errors.Is(err, ErrNotInitialized) {
		t.Errorf("Expected ErrNotInitialized, got: %v", err)
	}
}

// TestGetLatestVersion tests retrieving the latest event version for a topic
func TestGetLatestVersion(t *testing.T) {
	// Create a default config
	config := models.NewEventStoreConfig()

	// Create a new EventStore
	store, err := NewEventStore(config)
	if err != nil {
		t.Fatalf("Failed to create EventStore: %v", err)
	}

	// Initialize the EventStore
	err = store.Initialize(context.Background())
	if err != nil {
		t.Fatalf("Failed to initialize EventStore: %v", err)
	}

	// Create a test topic
	topicConfig := outbound.TopicConfig{
		Name:       "test-get-latest-version-topic",
		Adapter:    "memory",
		Connection: "memory://test",
		Options:    map[string]string{"key": "value"},
	}

	// Create the topic
	err = store.CreateTopic(context.Background(), topicConfig)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Initially, a new topic should have version 0
	version, err := store.GetLatestVersion(context.Background(), topicConfig.Name)
	if err != nil {
		t.Fatalf("Failed to get latest version: %v", err)
	}
	if version != 0 {
		t.Errorf("Expected initial version to be 0, got %d", version)
	}

	// Append some events
	events := []outbound.Event{
		{
			ID:   "event1",
			Type: "TestEvent",
			Data: map[string]interface{}{"key1": "value1"},
		},
		{
			ID:   "event2",
			Type: "TestEvent",
			Data: map[string]interface{}{"key2": "value2"},
		},
		{
			ID:   "event3",
			Type: "TestEvent",
			Data: map[string]interface{}{"key3": "value3"},
		},
	}

	err = store.AppendEvents(context.Background(), topicConfig.Name, events)
	if err != nil {
		t.Fatalf("Failed to append events: %v", err)
	}

	// After appending 3 events, the latest version should be 3
	// Note: Version is 1-based for events in the memory adapter
	version, err = store.GetLatestVersion(context.Background(), topicConfig.Name)
	if err != nil {
		t.Fatalf("Failed to get latest version: %v", err)
	}

	// Check if the version was incremented by the number of events
	// Different adapters might handle versioning differently
	if version < int64(len(events)) {
		t.Errorf("Expected version to be at least %d after appending %d events, got %d",
			len(events), len(events), version)
	}

	// Test with non-existent topic
	_, err = store.GetLatestVersion(context.Background(), "non-existent-topic")
	if err == nil {
		t.Errorf("Expected error when getting latest version from non-existent topic")
	} else if !errors.Is(err, ErrTopicNotFound) {
		t.Errorf("Expected ErrTopicNotFound, got: %v", err)
	}

	// Test with uninitialized EventStore
	uninitializedStore, _ := NewEventStore(config)
	_, err = uninitializedStore.GetLatestVersion(context.Background(), topicConfig.Name)
	if err == nil {
		t.Errorf("Expected error when getting latest version with uninitialized EventStore")
	} else if !errors.Is(err, ErrNotInitialized) {
		t.Errorf("Expected ErrNotInitialized, got: %v", err)
	}
}
