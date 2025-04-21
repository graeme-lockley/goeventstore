package memory

import (
	"context"
	"testing"
	"time"

	"goeventsource/src/internal/port/outbound"
)

func TestMemoryEventRepository_BasicOperations(t *testing.T) {
	repo := NewMemoryEventRepository()
	ctx := context.Background()

	// Initialize the repository
	err := repo.Initialize(ctx)
	if err != nil {
		t.Fatalf("Failed to initialize repository: %v", err)
	}

	// Test topic creation
	err = repo.CreateTopic(ctx, outbound.TopicConfig{
		Name:    "test-topic",
		Options: map[string]string{"description": "Test Topic"},
	})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Test topic exists
	exists, err := repo.TopicExists(ctx, "test-topic")
	if err != nil {
		t.Fatalf("Failed to check if topic exists: %v", err)
	}
	if !exists {
		t.Fatal("Topic should exist but doesn't")
	}

	// Test topic list
	topics, err := repo.ListTopics(ctx)
	if err != nil {
		t.Fatalf("Failed to list topics: %v", err)
	}
	if len(topics) != 1 {
		t.Fatalf("Expected 1 topic, got %d", len(topics))
	}
	if topics[0].Name != "test-topic" {
		t.Fatalf("Expected topic name 'test-topic', got '%s'", topics[0].Name)
	}

	// Test topic config retrieval
	config, err := repo.GetTopicConfig(ctx, "test-topic")
	if err != nil {
		t.Fatalf("Failed to get topic config: %v", err)
	}
	if config.Name != "test-topic" {
		t.Fatalf("Expected topic name 'test-topic', got '%s'", config.Name)
	}

	// Test topic config update
	updatedConfig := outbound.TopicConfig{
		Name:    "test-topic",
		Options: map[string]string{"description": "Updated Description"},
	}
	err = repo.UpdateTopicConfig(ctx, updatedConfig)
	if err != nil {
		t.Fatalf("Failed to update topic config: %v", err)
	}

	config, err = repo.GetTopicConfig(ctx, "test-topic")
	if err != nil {
		t.Fatalf("Failed to get updated topic config: %v", err)
	}
	if config.Options["description"] != "Updated Description" {
		t.Fatalf("Expected updated description, got '%s'", config.Options["description"])
	}
}

func TestMemoryEventRepository_EventOperations(t *testing.T) {
	repo := NewMemoryEventRepository()
	ctx := context.Background()

	// Initialize the repository
	err := repo.Initialize(ctx)
	if err != nil {
		t.Fatalf("Failed to initialize repository: %v", err)
	}

	// Create topic
	err = repo.CreateTopic(ctx, outbound.TopicConfig{
		Name: "event-topic",
	})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Test initial version
	version, err := repo.GetLatestVersion(ctx, "event-topic")
	if err != nil {
		t.Fatalf("Failed to get latest version: %v", err)
	}
	if version != 0 {
		t.Fatalf("Expected initial version 0, got %d", version)
	}

	// Test append events
	events := []outbound.Event{
		{
			Type: "test-event-1",
			Data: map[string]interface{}{"test": "data1"},
		},
		{
			Type: "test-event-2",
			Data: map[string]interface{}{"test": "data2"},
		},
	}

	err = repo.AppendEvents(ctx, "event-topic", events)
	if err != nil {
		t.Fatalf("Failed to append events: %v", err)
	}

	// Test latest version updated
	version, err = repo.GetLatestVersion(ctx, "event-topic")
	if err != nil {
		t.Fatalf("Failed to get latest version: %v", err)
	}
	if version != 2 {
		t.Fatalf("Expected version 2, got %d", version)
	}

	// Test get events
	retrievedEvents, err := repo.GetEvents(ctx, "event-topic", 0)
	if err != nil {
		t.Fatalf("Failed to get events: %v", err)
	}
	if len(retrievedEvents) != 2 {
		t.Fatalf("Expected 2 events, got %d", len(retrievedEvents))
	}

	// Test get events from specific version
	retrievedEvents, err = repo.GetEvents(ctx, "event-topic", 2)
	if err != nil {
		t.Fatalf("Failed to get events from version: %v", err)
	}
	if len(retrievedEvents) != 1 {
		t.Fatalf("Expected 1 event, got %d", len(retrievedEvents))
	}

	// Test get events by type
	retrievedEvents, err = repo.GetEventsByType(ctx, "event-topic", "test-event-1", 0)
	if err != nil {
		t.Fatalf("Failed to get events by type: %v", err)
	}
	if len(retrievedEvents) != 1 {
		t.Fatalf("Expected 1 event, got %d", len(retrievedEvents))
	}
	if retrievedEvents[0].Type != "test-event-1" {
		t.Fatalf("Expected event type 'test-event-1', got '%s'", retrievedEvents[0].Type)
	}
}

func TestMemoryEventRepository_ErrorCases(t *testing.T) {
	repo := NewMemoryEventRepository()
	ctx := context.Background()

	// Initialize the repository
	err := repo.Initialize(ctx)
	if err != nil {
		t.Fatalf("Failed to initialize repository: %v", err)
	}

	// Test operations on non-existent topic
	_, err = repo.GetEvents(ctx, "non-existent", 0)
	if err == nil {
		t.Fatal("Expected error for GetEvents on non-existent topic, got nil")
	}

	_, err = repo.GetEventsByType(ctx, "non-existent", "event-type", 0)
	if err == nil {
		t.Fatal("Expected error for GetEventsByType on non-existent topic, got nil")
	}

	_, err = repo.GetLatestVersion(ctx, "non-existent")
	if err == nil {
		t.Fatal("Expected error for GetLatestVersion on non-existent topic, got nil")
	}

	err = repo.AppendEvents(ctx, "non-existent", []outbound.Event{})
	if err == nil {
		t.Fatal("Expected error for AppendEvents on non-existent topic, got nil")
	}

	// Test creating duplicate topic
	err = repo.CreateTopic(ctx, outbound.TopicConfig{Name: "dupe-topic"})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	err = repo.CreateTopic(ctx, outbound.TopicConfig{Name: "dupe-topic"})
	if err == nil {
		t.Fatal("Expected error for creating duplicate topic, got nil")
	}
}

func TestMemoryEventRepository_EventProperties(t *testing.T) {
	repo := NewMemoryEventRepository()
	ctx := context.Background()

	// Initialize the repository
	err := repo.Initialize(ctx)
	if err != nil {
		t.Fatalf("Failed to initialize repository: %v", err)
	}

	// Create topic
	err = repo.CreateTopic(ctx, outbound.TopicConfig{Name: "prop-topic"})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Test that events get proper IDs, timestamps, and versions
	events := []outbound.Event{
		{Type: "test-event", Data: map[string]interface{}{}},
	}

	err = repo.AppendEvents(ctx, "prop-topic", events)
	if err != nil {
		t.Fatalf("Failed to append events: %v", err)
	}

	retrievedEvents, err := repo.GetEvents(ctx, "prop-topic", 0)
	if err != nil {
		t.Fatalf("Failed to get events: %v", err)
	}
	if len(retrievedEvents) != 1 {
		t.Fatalf("Expected 1 event, got %d", len(retrievedEvents))
	}

	event := retrievedEvents[0]
	if event.ID == "" {
		t.Fatal("Event ID should not be empty")
	}
	if event.Timestamp == 0 {
		t.Fatal("Event timestamp should not be zero")
	}
	if event.Version != 1 {
		t.Fatalf("Expected version 1, got %d", event.Version)
	}
	if event.Topic != "prop-topic" {
		t.Fatalf("Expected topic 'prop-topic', got '%s'", event.Topic)
	}

	// Test custom properties preserved
	customEvent := outbound.Event{
		ID:        "custom-id",
		Type:      "custom-event",
		Timestamp: time.Now().UnixNano(),
		Data:      map[string]interface{}{"custom": "data"},
	}

	err = repo.AppendEvents(ctx, "prop-topic", []outbound.Event{customEvent})
	if err != nil {
		t.Fatalf("Failed to append custom event: %v", err)
	}

	retrievedEvents, err = repo.GetEvents(ctx, "prop-topic", 2)
	if err != nil {
		t.Fatalf("Failed to get custom event: %v", err)
	}
	if len(retrievedEvents) != 1 {
		t.Fatalf("Expected 1 event, got %d", len(retrievedEvents))
	}

	event = retrievedEvents[0]
	if event.ID != "custom-id" {
		t.Fatalf("Expected ID 'custom-id', got '%s'", event.ID)
	}
	if event.Type != "custom-event" {
		t.Fatalf("Expected type 'custom-event', got '%s'", event.Type)
	}
	if event.Data["custom"] != "data" {
		t.Fatalf("Expected data '{\"custom\":\"data\"}', got '%v'", event.Data)
	}
}

func TestMemoryEventRepository_DeleteTopic(t *testing.T) {
	repo := NewMemoryEventRepository()
	ctx := context.Background()

	// Initialize the repository
	err := repo.Initialize(ctx)
	if err != nil {
		t.Fatalf("Failed to initialize repository: %v", err)
	}

	// Create topics
	err = repo.CreateTopic(ctx, outbound.TopicConfig{Name: "topic-to-delete"})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Add events
	events := []outbound.Event{
		{Type: "test-event", Data: map[string]interface{}{}},
	}
	err = repo.AppendEvents(ctx, "topic-to-delete", events)
	if err != nil {
		t.Fatalf("Failed to append events: %v", err)
	}

	// Delete topic
	err = repo.DeleteTopic(ctx, "topic-to-delete")
	if err != nil {
		t.Fatalf("Failed to delete topic: %v", err)
	}

	// Verify topic no longer exists
	exists, err := repo.TopicExists(ctx, "topic-to-delete")
	if err != nil {
		t.Fatalf("Failed to check if topic exists: %v", err)
	}
	if exists {
		t.Fatal("Topic should not exist after deletion")
	}

	// Verify operations on deleted topic fail
	_, err = repo.GetEvents(ctx, "topic-to-delete", 0)
	if err == nil {
		t.Fatal("Expected error for GetEvents on deleted topic, got nil")
	}
}

func TestMemoryEventRepository_Health(t *testing.T) {
	repo := NewMemoryEventRepository()
	ctx := context.Background()

	// Initialize the repository
	err := repo.Initialize(ctx)
	if err != nil {
		t.Fatalf("Failed to initialize repository: %v", err)
	}

	// Create topic
	err = repo.CreateTopic(ctx, outbound.TopicConfig{Name: "health-topic"})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Add events
	events := []outbound.Event{
		{Type: "test-event", Data: map[string]interface{}{}},
		{Type: "test-event", Data: map[string]interface{}{}},
	}
	err = repo.AppendEvents(ctx, "health-topic", events)
	if err != nil {
		t.Fatalf("Failed to append events: %v", err)
	}

	// Test health
	health, err := repo.Health(ctx)
	if err != nil {
		t.Fatalf("Failed to get health: %v", err)
	}

	if health["status"] != "up" {
		t.Fatalf("Expected status 'up', got '%v'", health["status"])
	}

	if health["topicCount"].(int) != 1 {
		t.Fatalf("Expected topicCount 1, got %v", health["topicCount"])
	}

	eventCounts, ok := health["eventCounts"].(map[string]int)
	if !ok {
		t.Fatalf("Expected eventCounts to be map[string]int, got %T", health["eventCounts"])
	}

	if eventCounts["health-topic"] != 2 {
		t.Fatalf("Expected 2 events in health-topic, got %d", eventCounts["health-topic"])
	}
}
