package eventstore

import (
	"context"
	"log"
	"os"
	"testing"

	"goeventsource/src/internal/eventstore/models"
	"goeventsource/src/internal/port/outbound"
)

func TestEventStore_Lifecycle(t *testing.T) {
	// Create a logger for testing
	logger := log.New(os.Stdout, "[TEST] ", log.LstdFlags)

	// Create a temporary directory for the config stream
	configDir := t.TempDir()

	// Create a configuration with filesystem adapter for persistence
	configStream := models.EventStreamConfig{
		Name:       "config",
		Adapter:    "fs",
		Connection: configDir,
	}

	config := models.EventStoreConfig{
		ConfigStream: configStream,
		Logger:       logger,
	}

	// Create the EventStore
	es, err := NewEventStore(config)
	if err != nil {
		t.Fatalf("Failed to create EventStore: %v", err)
	}

	// Check initial state
	if es.isInitialized {
		t.Error("Expected EventStore to not be initialized initially")
	}

	// Initialize the EventStore
	ctx := context.Background()
	err = es.Initialize(ctx)
	if err != nil {
		t.Fatalf("Failed to initialize EventStore: %v", err)
	}

	// Check state after initialization
	if !es.isInitialized {
		t.Error("Expected EventStore to be initialized after Initialize")
	}

	// Create a test topic
	topicConfig := outbound.TopicConfig{
		Name:       "test-topic",
		Adapter:    "memory",
		Connection: "memory://default",
		Options:    make(map[string]string),
	}

	err = es.CreateTopic(ctx, topicConfig)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Get health information
	health, err := es.Health(ctx)
	if err != nil {
		t.Fatalf("Failed to get health information: %v", err)
	}

	// Check health information
	if health["status"] != string(outbound.StatusUp) {
		t.Errorf("Expected status to be up, got %s", health["status"])
	}

	if !health["initialized"].(bool) {
		t.Error("Expected initialized to be true in health information")
	}

	if health["topicCount"].(int) != 1 {
		t.Errorf("Expected topicCount to be 1, got %d", health["topicCount"])
	}

	// Check registry health
	registryHealth, ok := health["registry"].(map[string]interface{})
	if !ok {
		t.Fatal("Expected registry health to be a map")
	}

	if registryHealth["status"] != "up" {
		t.Errorf("Expected registry status to be up, got %s", registryHealth["status"])
	}

	// Close the EventStore
	err = es.Close()
	if err != nil {
		t.Fatalf("Failed to close EventStore: %v", err)
	}

	// Check state after closing
	if es.isInitialized {
		t.Error("Expected EventStore to not be initialized after Close")
	}

	// Get health information after closing
	health, err = es.Health(ctx)
	if err == nil {
		t.Error("Expected error when getting health after closing")
	}

	if health["status"] != string(outbound.StatusDown) {
		t.Errorf("Expected status to be down after closing, got %s", health["status"])
	}

	// Re-initialize the EventStore
	err = es.Initialize(ctx)
	if err != nil {
		t.Fatalf("Failed to re-initialize EventStore: %v", err)
	}

	// Check state after re-initialization
	if !es.isInitialized {
		t.Error("Expected EventStore to be initialized after re-Initialize")
	}

	// Verify that topics are loaded from configuration
	topics, err := es.ListTopics(ctx)
	if err != nil {
		t.Fatalf("Failed to list topics: %v", err)
	}

	if len(topics) != 1 {
		t.Errorf("Expected 1 topic after re-initialization, got %d", len(topics))
	}

	if len(topics) > 0 && topics[0].Name != "test-topic" {
		t.Errorf("Expected topic name to be 'test-topic', got '%s'", topics[0].Name)
	}
}

func TestEventStore_StateConsistency(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()
	configDir := t.TempDir()

	// Create a logger for testing
	logger := log.New(os.Stdout, "[TEST] ", log.LstdFlags)

	// Create a configuration with filesystem adapter for persistence
	configStream := models.EventStreamConfig{
		Name:       "config",
		Adapter:    "fs", // Using fs adapter for config stream for persistence
		Connection: configDir,
	}

	config := models.EventStoreConfig{
		ConfigStream: configStream,
		Logger:       logger,
	}

	// Create the EventStore
	es, err := NewEventStore(config)
	if err != nil {
		t.Fatalf("Failed to create EventStore: %v", err)
	}

	// Initialize the EventStore
	ctx := context.Background()
	err = es.Initialize(ctx)
	if err != nil {
		t.Fatalf("Failed to initialize EventStore: %v", err)
	}

	// Create a test topic with filesystem adapter
	topicConfig := outbound.TopicConfig{
		Name:       "fs-topic",
		Adapter:    "fs",
		Connection: tempDir,
		Options:    make(map[string]string),
	}

	err = es.CreateTopic(ctx, topicConfig)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Add some test events
	events := []outbound.Event{
		{
			Type: "test-event",
			Data: map[string]interface{}{
				"key": "value",
			},
		},
	}

	err = es.AppendEvents(ctx, "fs-topic", events)
	if err != nil {
		t.Fatalf("Failed to append events: %v", err)
	}

	// Close the EventStore
	err = es.Close()
	if err != nil {
		t.Fatalf("Failed to close EventStore: %v", err)
	}

	// Create a new EventStore with the same configuration
	es2, err := NewEventStore(config)
	if err != nil {
		t.Fatalf("Failed to create second EventStore: %v", err)
	}

	// Initialize the second EventStore
	err = es2.Initialize(ctx)
	if err != nil {
		t.Fatalf("Failed to initialize second EventStore: %v", err)
	}

	// Verify that the topic was recovered
	topics, err := es2.ListTopics(ctx)
	if err != nil {
		t.Fatalf("Failed to list topics: %v", err)
	}

	if len(topics) != 1 {
		t.Errorf("Expected 1 topic after re-initialization, got %d", len(topics))
	}

	if len(topics) > 0 && topics[0].Name != "fs-topic" {
		t.Errorf("Expected topic name to be 'fs-topic', got '%s'", topics[0].Name)
	}

	// Verify events were recovered
	recoveredEvents, err := es2.GetEvents(ctx, "fs-topic", 0)
	if err != nil {
		t.Fatalf("Failed to get events: %v", err)
	}

	if len(recoveredEvents) != 1 {
		t.Errorf("Expected 1 event after recovery, got %d", len(recoveredEvents))
	}

	// Get health information of the recovered store
	health, err := es2.Health(ctx)
	if err != nil {
		t.Fatalf("Failed to get health information: %v", err)
	}

	if health["status"] != string(outbound.StatusUp) {
		t.Errorf("Expected status to be up, got %s", health["status"])
	}

	// Close the second EventStore
	err = es2.Close()
	if err != nil {
		t.Fatalf("Failed to close second EventStore: %v", err)
	}
}

// TestEventStore_MultipleRepositories tests the lifecycle management with multiple repositories
func TestEventStore_MultipleRepositories(t *testing.T) {
	// Create a logger for testing
	logger := log.New(os.Stdout, "[TEST] ", log.LstdFlags)

	// Create a temporary directory for config stream
	configDir := t.TempDir()

	// Create a configuration with filesystem adapter for config stream
	configStream := models.EventStreamConfig{
		Name:       "config",
		Adapter:    "fs",
		Connection: configDir,
	}

	config := models.EventStoreConfig{
		ConfigStream: configStream,
		Logger:       logger,
	}

	// Create the EventStore
	es, err := NewEventStore(config)
	if err != nil {
		t.Fatalf("Failed to create EventStore: %v", err)
	}

	// Initialize the EventStore
	ctx := context.Background()
	if err := es.Initialize(ctx); err != nil {
		t.Fatalf("Failed to initialize EventStore: %v", err)
	}

	// Create temporary directories for each repository
	tempDir1 := t.TempDir()
	tempDir2 := t.TempDir()

	// Create topics with different repository types
	topics := []struct {
		name       string
		adapter    string
		connection string
	}{
		{"memory-topic-1", "memory", "memory://default1"},
		{"memory-topic-2", "memory", "memory://default2"},
		{"fs-topic-1", "fs", tempDir1},
		{"fs-topic-2", "fs", tempDir2},
	}

	// Create each topic
	for _, tc := range topics {
		topicConfig := outbound.TopicConfig{
			Name:       tc.name,
			Adapter:    tc.adapter,
			Connection: tc.connection,
			Options:    make(map[string]string),
		}

		if err := es.CreateTopic(ctx, topicConfig); err != nil {
			t.Fatalf("Failed to create topic %s: %v", tc.name, err)
		}
	}

	// Verify all topics were created
	storedTopics, err := es.ListTopics(ctx)
	if err != nil {
		t.Fatalf("Failed to list topics: %v", err)
	}

	if len(storedTopics) != len(topics) {
		t.Errorf("Expected %d topics, got %d", len(topics), len(storedTopics))
	}

	// Check health with all repositories
	health, err := es.Health(ctx)
	if err != nil {
		t.Fatalf("Failed to get health: %v", err)
	}

	// Verify overall status is up
	if health["status"] != string(outbound.StatusUp) {
		t.Errorf("Expected status to be up, got %s", health["status"])
	}

	// Verify repository stats
	if repoStats, ok := health["repositoryStats"].(map[string]int); ok {
		// We should have 5 repositories: 1 for config + 2 for memory topics (different connections) + 2 for fs directories
		expectedRepoCount := 5
		if repoStats["total"] != expectedRepoCount {
			t.Errorf("Expected %d repositories, got %d", expectedRepoCount, repoStats["total"])
		}

		// All repositories should be up
		if repoStats["up"] != expectedRepoCount {
			t.Errorf("Expected %d up repositories, got %d", expectedRepoCount, repoStats["up"])
		}
	} else {
		t.Error("Repository stats not found in health output")
	}

	// Verify adapter statistics
	if adapterStats, ok := health["adapterStats"].(map[string]int); ok {
		if adapterStats["memory"] != 2 {
			t.Errorf("Expected 2 memory topics, got %d", adapterStats["memory"])
		}
		if adapterStats["fs"] != 2 {
			t.Errorf("Expected 2 fs topics, got %d", adapterStats["fs"])
		}
	} else {
		t.Error("Adapter stats not found in health output")
	}

	// Test graceful closing
	if err := es.Close(); err != nil {
		t.Fatalf("Failed to close EventStore: %v", err)
	}

	// Verify the EventStore is no longer initialized
	if es.isInitialized {
		t.Error("EventStore should not be initialized after Close")
	}

	// Health check after close should show down status
	health, err = es.Health(ctx)
	if err == nil {
		t.Error("Expected error when getting health after closing")
	}
	if health["status"] != string(outbound.StatusDown) {
		t.Errorf("Expected status to be down after close, got %s", health["status"])
	}

	// Reinitialize the EventStore after closing
	if err := es.Initialize(ctx); err != nil {
		t.Fatalf("Failed to reinitialize EventStore: %v", err)
	}

	// Verify topics are still there after reinitialization
	storedTopics, err = es.ListTopics(ctx)
	if err != nil {
		t.Fatalf("Failed to list topics after reinitialization: %v", err)
	}

	if len(storedTopics) != len(topics) {
		t.Errorf("Expected %d topics after reinitialization, got %d", len(topics), len(storedTopics))
	}

	// Final cleanup
	if err := es.Close(); err != nil {
		t.Fatalf("Failed to close EventStore: %v", err)
	}
}

// TestEventStore_HealthWithDegradedRepositories tests the health reporting with degraded repositories
func TestEventStore_HealthWithDegradedRepositories(t *testing.T) {
	// This test would ideally mock repositories to simulate degraded or down status
	// Since we can't easily create a degraded repository with the current implementation,
	// we'll just verify the basic health check functionality

	// Create a logger for testing
	logger := log.New(os.Stdout, "[TEST] ", log.LstdFlags)

	// Create a temporary directory for config stream
	configDir := t.TempDir()

	// Create a configuration
	configStream := models.EventStreamConfig{
		Name:       "config",
		Adapter:    "fs",
		Connection: configDir,
	}

	config := models.EventStoreConfig{
		ConfigStream: configStream,
		Logger:       logger,
	}

	// Create the EventStore
	es, err := NewEventStore(config)
	if err != nil {
		t.Fatalf("Failed to create EventStore: %v", err)
	}

	// Check health before initialization (should be down)
	health, err := es.Health(context.Background())
	if err == nil {
		t.Error("Expected error when getting health before initialization")
	}
	if health["status"] != string(outbound.StatusDown) {
		t.Errorf("Expected status to be down before initialization, got %s", health["status"])
	}
	if health["reason"] != "not initialized" {
		t.Errorf("Expected reason to be 'not initialized', got %s", health["reason"])
	}

	// Initialize and verify health improves
	ctx := context.Background()
	if err := es.Initialize(ctx); err != nil {
		t.Fatalf("Failed to initialize EventStore: %v", err)
	}

	health, err = es.Health(ctx)
	if err != nil {
		t.Fatalf("Failed to get health after initialization: %v", err)
	}
	if health["status"] != string(outbound.StatusUp) {
		t.Errorf("Expected status to be up after initialization, got %s", health["status"])
	}

	// Cleanup
	if err := es.Close(); err != nil {
		t.Fatalf("Failed to close EventStore: %v", err)
	}
}

// TestEventStore_CloseAndReinitialize tests closing and reinitializing the EventStore
func TestEventStore_CloseAndReinitialize(t *testing.T) {
	// Create a logger for testing
	logger := log.New(os.Stdout, "[TEST] ", log.LstdFlags)

	// Create temporary directories for repositories
	tempDir := t.TempDir()
	configDir := t.TempDir()

	// Create a configuration with filesystem adapter for config stream
	configStream := models.EventStreamConfig{
		Name:       "config",
		Adapter:    "fs",
		Connection: configDir,
	}

	config := models.EventStoreConfig{
		ConfigStream: configStream,
		Logger:       logger,
	}

	// Create the EventStore
	es, err := NewEventStore(config)
	if err != nil {
		t.Fatalf("Failed to create EventStore: %v", err)
	}

	ctx := context.Background()
	if err := es.Initialize(ctx); err != nil {
		t.Fatalf("Failed to initialize EventStore: %v", err)
	}

	// Create topics with different repositories
	topics := []struct {
		name       string
		adapter    string
		connection string
	}{
		{"memory-topic", "memory", "memory://default"},
		{"fs-topic", "fs", tempDir},
	}

	for _, tc := range topics {
		topicConfig := outbound.TopicConfig{
			Name:       tc.name,
			Adapter:    tc.adapter,
			Connection: tc.connection,
			Options:    make(map[string]string),
		}

		if err := es.CreateTopic(ctx, topicConfig); err != nil {
			t.Fatalf("Failed to create topic %s: %v", tc.name, err)
		}

		// Add a test event to each topic
		event := outbound.Event{
			Type: "test-event",
			Data: map[string]interface{}{"topic": tc.name},
		}
		if err := es.AppendEvents(ctx, tc.name, []outbound.Event{event}); err != nil {
			t.Fatalf("Failed to append event to %s: %v", tc.name, err)
		}
	}

	// Close the EventStore
	if err := es.Close(); err != nil {
		t.Fatalf("Failed to close EventStore: %v", err)
	}

	// Reinitialize the EventStore
	if err := es.Initialize(ctx); err != nil {
		t.Fatalf("Failed to reinitialize EventStore: %v", err)
	}

	// Verify all topics are reloaded in the config
	storedTopics, err := es.ListTopics(ctx)
	if err != nil {
		t.Fatalf("Failed to list topics after reinitialization: %v", err)
	}

	if len(storedTopics) != len(topics) {
		t.Errorf("Expected %d topics after reinitialization, got %d", len(topics), len(storedTopics))
	}

	// Since memory repositories lose their topics when closed, we need to recreate
	// the topics in the newly created repositories.
	for _, tc := range topics {
		if tc.adapter == "memory" {
			// For memory adapters, we need to recreate the topic in the repository
			_, err := es.getRepositoryForTopic(ctx, tc.name)
			if err != nil {
				// Get the topic config from the EventStore
				topicConfig, err := es.GetTopic(ctx, tc.name)
				if err != nil {
					t.Fatalf("Failed to get topic config: %v", err)
				}

				// Create a new in-memory repository for this topic
				memRepo, err := es.registry.GetOrCreate(ctx, models.FromTopicConfig(topicConfig))
				if err != nil {
					t.Fatalf("Failed to get or create repository: %v", err)
				}

				// Create the topic in the memory repository
				if err := memRepo.CreateTopic(ctx, topicConfig); err != nil {
					t.Fatalf("Failed to recreate memory topic: %v", err)
				}
			}
		}
	}

	// Verify events are still accessible for filesystem topics
	// In a real application, memory topics would need to be repopulated from somewhere else
	fsTopics := []string{}
	for _, tc := range topics {
		if tc.adapter == "fs" {
			fsTopics = append(fsTopics, tc.name)
		}
	}

	for _, topicName := range fsTopics {
		events, err := es.GetEvents(ctx, topicName, 0)
		if err != nil {
			t.Fatalf("Failed to get events from %s after reinitialization: %v", topicName, err)
		}

		if len(events) != 1 {
			t.Errorf("Expected 1 event in %s after reinitialization, got %d", topicName, len(events))
			continue
		}

		if events[0].Type != "test-event" {
			t.Errorf("Expected event type 'test-event', got %s", events[0].Type)
		}
	}

	// Final cleanup
	if err := es.Close(); err != nil {
		t.Fatalf("Failed to close EventStore after reinitialization: %v", err)
	}
}
