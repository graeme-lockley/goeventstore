package fs

import (
	"context"
	"encoding/json"
	"fmt"
	"goeventsource/src/internal/port/outbound"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestFileSystemEventRepositoryStructFields(t *testing.T) {
	// This test simply verifies that the FileSystemEventRepository struct has the expected fields
	// by creating an instance of it. If the struct is missing fields, this will fail at compile time.

	repo := &FileSystemEventRepository{
		basePath:      "/tmp/eventstore",
		latestVersion: make(map[string]int64),
		topicConfigs:  make(map[string]outbound.TopicConfig),
	}

	// Basic verification that fields are accessible
	if repo.basePath != "/tmp/eventstore" {
		t.Errorf("Expected basePath to be /tmp/eventstore, got %s", repo.basePath)
	}

	// Verify maps are initialized
	if repo.latestVersion == nil {
		t.Error("latestVersion map should be initialized")
	}

	if repo.topicConfigs == nil {
		t.Error("topicConfigs map should be initialized")
	}
}

func TestNewFileSystemEventRepository(t *testing.T) {
	// Test that the constructor properly initializes the repository
	testPath := "/test/path"
	repo := NewFileSystemEventRepository(testPath)

	// Verify the base path is set correctly
	if repo.basePath != testPath {
		t.Errorf("Expected basePath to be %s, got %s", testPath, repo.basePath)
	}

	// Verify maps are initialized
	if repo.latestVersion == nil {
		t.Error("latestVersion map should be initialized")
	}

	if repo.topicConfigs == nil {
		t.Error("topicConfigs map should be initialized")
	}

	// Verify maps are empty
	if len(repo.latestVersion) != 0 {
		t.Errorf("Expected empty latestVersion map, got size %d", len(repo.latestVersion))
	}

	if len(repo.topicConfigs) != 0 {
		t.Errorf("Expected empty topicConfigs map, got size %d", len(repo.topicConfigs))
	}
}

func TestDirectoryHelperFunctions(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "fs_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir) // Clean up after test

	repo := NewFileSystemEventRepository(tempDir)

	// Test ensureBaseDir
	err = repo.ensureBaseDir()
	if err != nil {
		t.Errorf("ensureBaseDir failed: %v", err)
	}

	if _, err := os.Stat(tempDir); os.IsNotExist(err) {
		t.Error("Base directory was not created")
	}

	// Test ensureConfigsDir
	err = repo.ensureConfigsDir()
	if err != nil {
		t.Errorf("ensureConfigsDir failed: %v", err)
	}

	configDir := filepath.Join(tempDir, "configs")
	if _, err := os.Stat(configDir); os.IsNotExist(err) {
		t.Error("Configs directory was not created")
	}

	// Test ensureEventsDir
	err = repo.ensureEventsDir()
	if err != nil {
		t.Errorf("ensureEventsDir failed: %v", err)
	}

	eventsDir := filepath.Join(tempDir, "events")
	if _, err := os.Stat(eventsDir); os.IsNotExist(err) {
		t.Error("Events directory was not created")
	}

	// Test ensureTopicDir
	testTopic := "test-topic"
	err = repo.ensureTopicDir(testTopic)
	if err != nil {
		t.Errorf("ensureTopicDir failed: %v", err)
	}

	topicDir := filepath.Join(tempDir, "events", testTopic)
	if _, err := os.Stat(topicDir); os.IsNotExist(err) {
		t.Error("Topic directory was not created")
	}

	// Test getConfigDir
	if repo.getConfigDir() != configDir {
		t.Errorf("getConfigDir returned incorrect path, got %s, want %s", repo.getConfigDir(), configDir)
	}

	// Test getEventsDir
	if repo.getEventsDir() != eventsDir {
		t.Errorf("getEventsDir returned incorrect path, got %s, want %s", repo.getEventsDir(), eventsDir)
	}

	// Test getTopicDir
	if repo.getTopicDir(testTopic) != topicDir {
		t.Errorf("getTopicDir returned incorrect path, got %s, want %s", repo.getTopicDir(testTopic), topicDir)
	}
}

func TestInitialize(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "fs_init_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir) // Clean up after test

	// Create a repository
	repo := NewFileSystemEventRepository(tempDir)

	// Test initialization with empty directory
	ctx := context.Background()
	err = repo.Initialize(ctx)
	if err != nil {
		t.Fatalf("Initialize failed with empty directory: %v", err)
	}

	// Verify directory structure was created
	configDir := repo.getConfigDir()
	if _, err := os.Stat(configDir); os.IsNotExist(err) {
		t.Error("Configs directory was not created during initialization")
	}

	eventsDir := repo.getEventsDir()
	if _, err := os.Stat(eventsDir); os.IsNotExist(err) {
		t.Error("Events directory was not created during initialization")
	}

	// Test initialization with existing topics
	// Create a topic configuration file
	topicName := "test-topic"
	topicConfig := outbound.TopicConfig{
		Name:       topicName,
		Adapter:    "fs",
		Connection: tempDir,
		Options:    map[string]string{"option1": "value1"},
	}

	configData, err := json.Marshal(topicConfig)
	if err != nil {
		t.Fatalf("Failed to marshal topic config: %v", err)
	}

	configPath := filepath.Join(configDir, topicName+".json")
	err = os.WriteFile(configPath, configData, 0644)
	if err != nil {
		t.Fatalf("Failed to write topic config: %v", err)
	}

	// Create topic directory and event files
	topicDir := repo.getTopicDir(topicName)
	err = os.MkdirAll(topicDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create topic directory: %v", err)
	}

	// Create some event files
	for i := 1; i <= 3; i++ {
		eventFilename := filepath.Join(topicDir, fmt.Sprintf("%020d.json", i))
		err = os.WriteFile(eventFilename, []byte("{}"), 0644)
		if err != nil {
			t.Fatalf("Failed to create event file: %v", err)
		}
	}

	// Create a new repository to test loading existing config
	repo2 := NewFileSystemEventRepository(tempDir)
	err = repo2.Initialize(ctx)
	if err != nil {
		t.Fatalf("Initialize failed with existing config: %v", err)
	}

	// Verify topic config was loaded
	if len(repo2.topicConfigs) != 1 {
		t.Errorf("Expected 1 topic config, got %d", len(repo2.topicConfigs))
	}

	loadedConfig, exists := repo2.topicConfigs[topicName]
	if !exists {
		t.Errorf("Topic config for %s was not loaded", topicName)
	}

	if loadedConfig.Name != topicName {
		t.Errorf("Expected topic name %s, got %s", topicName, loadedConfig.Name)
	}

	if loadedConfig.Options["option1"] != "value1" {
		t.Errorf("Expected option1 value 'value1', got '%s'", loadedConfig.Options["option1"])
	}

	// Verify latest version was detected
	latestVersion, exists := repo2.latestVersion[topicName]
	if !exists {
		t.Errorf("Latest version for %s was not determined", topicName)
	}

	if latestVersion != 3 {
		t.Errorf("Expected latest version 3, got %d", latestVersion)
	}
}

func TestClose(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "fs_close_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir) // Clean up after test

	// Create a repository
	repo := NewFileSystemEventRepository(tempDir)
	ctx := context.Background()

	// Initialize the repository
	err = repo.Initialize(ctx)
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Create a test topic
	topicName := "test-topic"
	topicConfig := outbound.TopicConfig{
		Name:       topicName,
		Adapter:    "fs",
		Connection: tempDir,
		Options:    map[string]string{"option1": "value1"},
	}

	// Save the topic configuration
	configDir := repo.getConfigDir()
	configPath := filepath.Join(configDir, topicName+".json")
	configData, err := json.Marshal(topicConfig)
	if err != nil {
		t.Fatalf("Failed to marshal topic config: %v", err)
	}

	err = os.WriteFile(configPath, configData, 0644)
	if err != nil {
		t.Fatalf("Failed to write topic config: %v", err)
	}

	// Create topic directory and add an event file
	topicDir := repo.getTopicDir(topicName)
	if err := os.MkdirAll(topicDir, 0755); err != nil {
		t.Fatalf("Failed to create topic directory: %v", err)
	}

	// Create an event file with version 1
	testEvent := outbound.Event{
		ID:        "test-id",
		Topic:     topicName,
		Type:      "TestEvent",
		Version:   1,
		Timestamp: time.Now().UnixNano(),
		Data:      map[string]interface{}{"test": "data"},
	}

	eventData, err := json.Marshal(testEvent)
	if err != nil {
		t.Fatalf("Failed to marshal event: %v", err)
	}

	eventPath := filepath.Join(topicDir, "00000000000000000001.json")
	if err := os.WriteFile(eventPath, eventData, 0644); err != nil {
		t.Fatalf("Failed to write event file: %v", err)
	}

	// Load the topic
	repo.topicConfigs[topicName] = topicConfig
	repo.latestVersion[topicName] = 1

	// Verify maps have data
	if len(repo.topicConfigs) == 0 {
		t.Error("Expected topicConfigs to have data before close")
	}

	if len(repo.latestVersion) == 0 {
		t.Error("Expected latestVersion to have data before close")
	}

	// Close the repository
	err = repo.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify maps are cleared
	if len(repo.topicConfigs) != 0 {
		t.Errorf("Expected empty topicConfigs after close, got size %d", len(repo.topicConfigs))
	}

	if len(repo.latestVersion) != 0 {
		t.Errorf("Expected empty latestVersion after close, got size %d", len(repo.latestVersion))
	}

	// Verify that the config file still exists (Close shouldn't delete data)
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		t.Error("Config file should still exist after close")
	}

	// Verify we can reinitialize after close
	err = repo.Initialize(ctx)
	if err != nil {
		t.Fatalf("Failed to reinitialize after close: %v", err)
	}

	// Verify data is reloaded
	if len(repo.topicConfigs) == 0 {
		t.Error("Expected topicConfigs to be reloaded after reinitialize")
	}

	if len(repo.latestVersion) == 0 {
		t.Error("Expected latestVersion to be reloaded after reinitialize")
	}
}

// createTempDir creates a temporary directory for testing
func createTempDir(t *testing.T) string {
	tempDir, err := os.MkdirTemp("", "fs_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	return tempDir
}

func TestAppendEvents(t *testing.T) {
	// Create temporary directory for testing
	tempDir := createTempDir(t)
	defer os.RemoveAll(tempDir)

	// Create repository
	repo := NewFileSystemEventRepository(tempDir)
	ctx := context.Background()

	// Initialize the repository
	err := repo.Initialize(ctx)
	if err != nil {
		t.Fatalf("Failed to initialize repository: %v", err)
	}

	// Create a test topic
	topic := "test-topic"
	topicConfig := outbound.TopicConfig{
		Name:       topic,
		Adapter:    "fs",
		Connection: tempDir,
	}

	err = repo.CreateTopic(ctx, topicConfig)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Create test events
	events := []outbound.Event{
		{
			Type: "TestEvent1",
			Data: map[string]interface{}{
				"key1": "value1",
			},
			Metadata: map[string]interface{}{
				"meta1": "metadata1",
			},
		},
		{
			Type: "TestEvent2",
			Data: map[string]interface{}{
				"key2": "value2",
			},
		},
	}

	// Test appending events
	err = repo.AppendEvents(ctx, topic, events)
	if err != nil {
		t.Fatalf("Failed to append events: %v", err)
	}

	// Verify the latest version is updated correctly
	latestVersion, err := repo.GetLatestVersion(ctx, topic)
	if err != nil {
		t.Fatalf("Failed to get latest version: %v", err)
	}
	if latestVersion != 2 {
		t.Fatalf("Expected latest version to be 2, got %d", latestVersion)
	}

	// Check if event files were created with the right content
	topicDir := filepath.Join(tempDir, "events", topic)
	files, err := os.ReadDir(topicDir)
	if err != nil {
		t.Fatalf("Failed to read topic directory: %v", err)
	}

	if len(files) != 2 {
		t.Fatalf("Expected 2 event files, got %d", len(files))
	}

	// Read the first event file and verify contents
	firstEventPath := filepath.Join(topicDir, fmt.Sprintf("%020d.json", 1))
	firstEventData, err := os.ReadFile(firstEventPath)
	if err != nil {
		t.Fatalf("Failed to read first event file: %v", err)
	}

	var firstEvent outbound.Event
	err = json.Unmarshal(firstEventData, &firstEvent)
	if err != nil {
		t.Fatalf("Failed to unmarshal first event: %v", err)
	}

	if firstEvent.Type != "TestEvent1" {
		t.Fatalf("Expected event type TestEvent1, got %s", firstEvent.Type)
	}
	if firstEvent.Version != 1 {
		t.Fatalf("Expected event version 1, got %d", firstEvent.Version)
	}
	if firstEvent.Topic != topic {
		t.Fatalf("Expected event topic %s, got %s", topic, firstEvent.Topic)
	}
	if firstEvent.ID == "" {
		t.Fatal("Expected event ID to be generated, but it's empty")
	}
	if firstEvent.Timestamp == 0 {
		t.Fatal("Expected event timestamp to be set, but it's 0")
	}
	if val, ok := firstEvent.Data["key1"]; !ok || val != "value1" {
		t.Fatalf("Event data not preserved correctly, got: %v", firstEvent.Data)
	}
	if val, ok := firstEvent.Metadata["meta1"]; !ok || val != "metadata1" {
		t.Fatalf("Event metadata not preserved correctly, got: %v", firstEvent.Metadata)
	}

	// Test appending events with pre-set ID and timestamp
	customEvent := outbound.Event{
		ID:        "custom-id",
		Type:      "CustomEvent",
		Timestamp: 1234567890,
		Data:      map[string]interface{}{"custom": "data"},
	}

	err = repo.AppendEvents(ctx, topic, []outbound.Event{customEvent})
	if err != nil {
		t.Fatalf("Failed to append custom event: %v", err)
	}

	// Verify the latest version is updated correctly
	latestVersion, err = repo.GetLatestVersion(ctx, topic)
	if err != nil {
		t.Fatalf("Failed to get latest version: %v", err)
	}
	if latestVersion != 3 {
		t.Fatalf("Expected latest version to be 3, got %d", latestVersion)
	}

	// Read the custom event file and verify contents
	customEventPath := filepath.Join(topicDir, fmt.Sprintf("%020d.json", 3))
	customEventData, err := os.ReadFile(customEventPath)
	if err != nil {
		t.Fatalf("Failed to read custom event file: %v", err)
	}

	var retrievedCustomEvent outbound.Event
	err = json.Unmarshal(customEventData, &retrievedCustomEvent)
	if err != nil {
		t.Fatalf("Failed to unmarshal custom event: %v", err)
	}

	if retrievedCustomEvent.ID != "custom-id" {
		t.Fatalf("Expected custom ID 'custom-id', got: %s", retrievedCustomEvent.ID)
	}
	if retrievedCustomEvent.Timestamp != 1234567890 {
		t.Fatalf("Expected custom timestamp 1234567890, got: %d", retrievedCustomEvent.Timestamp)
	}

	// Test that appending to a non-existent topic returns an error
	err = repo.AppendEvents(ctx, "non-existent-topic", events)
	if err == nil {
		t.Fatal("Expected error when appending to non-existent topic, got nil")
	}
}

func TestGetEvents(t *testing.T) {
	// Create temporary directory for testing
	tempDir := createTempDir(t)
	defer os.RemoveAll(tempDir)

	// Create repository
	repo := NewFileSystemEventRepository(tempDir)
	ctx := context.Background()

	// Initialize the repository
	err := repo.Initialize(ctx)
	if err != nil {
		t.Fatalf("Failed to initialize repository: %v", err)
	}

	// Create a test topic
	topic := "test-topic"
	topicConfig := outbound.TopicConfig{
		Name:       topic,
		Adapter:    "fs",
		Connection: tempDir,
	}

	err = repo.CreateTopic(ctx, topicConfig)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Test getting events from empty topic
	events, err := repo.GetEvents(ctx, topic, 0)
	if err != nil {
		t.Fatalf("Failed to get events from empty topic: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("Expected empty events slice, got %d events", len(events))
	}

	// Create test events
	testEvents := []outbound.Event{
		{
			Type: "TestEvent1",
			Data: map[string]interface{}{
				"key1": "value1",
			},
		},
		{
			Type: "TestEvent2",
			Data: map[string]interface{}{
				"key2": "value2",
			},
		},
		{
			Type: "TestEvent3",
			Data: map[string]interface{}{
				"key3": "value3",
			},
		},
	}

	// Append events
	err = repo.AppendEvents(ctx, topic, testEvents)
	if err != nil {
		t.Fatalf("Failed to append events: %v", err)
	}

	// Test getting all events (fromVersion = 0)
	allEvents, err := repo.GetEvents(ctx, topic, 0)
	if err != nil {
		t.Fatalf("Failed to get events: %v", err)
	}
	if len(allEvents) != 3 {
		t.Fatalf("Expected 3 events with fromVersion=0, got %d", len(allEvents))
	}

	// Verify events are sorted by version
	for i := 0; i < len(allEvents)-1; i++ {
		if allEvents[i].Version >= allEvents[i+1].Version {
			t.Errorf("Events not sorted by version: %d >= %d",
				allEvents[i].Version, allEvents[i+1].Version)
		}
	}

	// Verify content of first event
	if allEvents[0].Type != "TestEvent1" {
		t.Errorf("Expected first event type to be TestEvent1, got %s", allEvents[0].Type)
	}
	if val, ok := allEvents[0].Data["key1"]; !ok || val != "value1" {
		t.Errorf("First event data not as expected, got: %v", allEvents[0].Data)
	}

	// Test getting events after version 1 (exclusive, should only get events 2-3)
	fromVersion1Events, err := repo.GetEvents(ctx, topic, 1)
	if err != nil {
		t.Fatalf("Failed to get events after version 1: %v", err)
	}
	if len(fromVersion1Events) != 2 {
		t.Fatalf("Expected 2 events after version 1, got %d", len(fromVersion1Events))
	}
	if fromVersion1Events[0].Version != 2 {
		t.Errorf("Expected first event version to be 2, got %d", fromVersion1Events[0].Version)
	}

	// Test getting events after version 2 (exclusive, should only get event 3)
	fromVersion2Events, err := repo.GetEvents(ctx, topic, 2)
	if err != nil {
		t.Fatalf("Failed to get events after version 2: %v", err)
	}
	if len(fromVersion2Events) != 1 {
		t.Fatalf("Expected 1 event after version 2, got %d", len(fromVersion2Events))
	}
	if fromVersion2Events[0].Version != 3 {
		t.Errorf("Expected event version to be 3, got %d", fromVersion2Events[0].Version)
	}

	// Test getting events from a version higher than available (should return empty)
	fromVersion3Events, err := repo.GetEvents(ctx, topic, 3)
	if err != nil {
		t.Fatalf("Failed to get events after version 3: %v", err)
	}
	if len(fromVersion3Events) != 0 {
		t.Fatalf("Expected 0 events after version 3, got %d", len(fromVersion3Events))
	}

	// Test getting events from a non-existent topic
	_, err = repo.GetEvents(ctx, "non-existent-topic", 0)
	if err == nil {
		t.Error("Expected error when getting events from non-existent topic, got nil")
	}

	// Add more events to ensure proper sorting
	moreEvents := []outbound.Event{
		{
			Type: "TestEvent4",
			Data: map[string]interface{}{"key4": "value4"},
		},
		{
			Type: "TestEvent5",
			Data: map[string]interface{}{"key5": "value5"},
		},
	}

	err = repo.AppendEvents(ctx, topic, moreEvents)
	if err != nil {
		t.Fatalf("Failed to append more events: %v", err)
	}

	// Verify all 5 events can be retrieved (from version 0)
	allEvents, err = repo.GetEvents(ctx, topic, 0)
	if err != nil {
		t.Fatalf("Failed to get all events: %v", err)
	}
	if len(allEvents) != 5 {
		t.Fatalf("Expected 5 events total with fromVersion=0, got %d", len(allEvents))
	}

	// Verify events after version 3 (should be events 4-5)
	afterVersion3Events, err := repo.GetEvents(ctx, topic, 3)
	if err != nil {
		t.Fatalf("Failed to get events after version 3: %v", err)
	}
	if len(afterVersion3Events) != 2 {
		t.Fatalf("Expected 2 events after version 3, got %d", len(afterVersion3Events))
	}
	if afterVersion3Events[0].Version != 4 {
		t.Errorf("Expected first event to be version 4, got %d", afterVersion3Events[0].Version)
	}
	if afterVersion3Events[1].Version != 5 {
		t.Errorf("Expected second event to be version 5, got %d", afterVersion3Events[1].Version)
	}
}

func TestGetEventsByType(t *testing.T) {
	// Create temporary directory for testing
	tempDir := createTempDir(t)
	defer os.RemoveAll(tempDir)

	// Create repository
	repo := NewFileSystemEventRepository(tempDir)
	ctx := context.Background()

	// Initialize the repository
	err := repo.Initialize(ctx)
	if err != nil {
		t.Fatalf("Failed to initialize repository: %v", err)
	}

	// Create a test topic
	topic := "test-topic"
	topicConfig := outbound.TopicConfig{
		Name:       topic,
		Adapter:    "fs",
		Connection: tempDir,
	}

	err = repo.CreateTopic(ctx, topicConfig)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Test getting events by type from empty topic
	events, err := repo.GetEventsByType(ctx, topic, "TestType", 0)
	if err != nil {
		t.Fatalf("Failed to get events by type from empty topic: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("Expected empty events slice, got %d events", len(events))
	}

	// Create test events with different types
	testEvents := []outbound.Event{
		{
			Type: "TypeA",
			Data: map[string]interface{}{
				"key1": "value1",
			},
		},
		{
			Type: "TypeB",
			Data: map[string]interface{}{
				"key2": "value2",
			},
		},
		{
			Type: "TypeA",
			Data: map[string]interface{}{
				"key3": "value3",
			},
		},
		{
			Type: "TypeC",
			Data: map[string]interface{}{
				"key4": "value4",
			},
		},
		{
			Type: "TypeB",
			Data: map[string]interface{}{
				"key5": "value5",
			},
		},
	}

	// Append events
	err = repo.AppendEvents(ctx, topic, testEvents)
	if err != nil {
		t.Fatalf("Failed to append events: %v", err)
	}

	// Test getting events of type "TypeA"
	typeAEvents, err := repo.GetEventsByType(ctx, topic, "TypeA", 0)
	if err != nil {
		t.Fatalf("Failed to get events of type TypeA: %v", err)
	}
	if len(typeAEvents) != 2 {
		t.Fatalf("Expected 2 events of type TypeA, got %d", len(typeAEvents))
	}
	for _, event := range typeAEvents {
		if event.Type != "TypeA" {
			t.Errorf("Expected event type to be TypeA, got %s", event.Type)
		}
	}

	// Verify events are sorted by version
	if len(typeAEvents) >= 2 && typeAEvents[0].Version >= typeAEvents[1].Version {
		t.Errorf("TypeA events not sorted by version: %d >= %d",
			typeAEvents[0].Version, typeAEvents[1].Version)
	}

	// Test getting events of type "TypeB"
	typeBEvents, err := repo.GetEventsByType(ctx, topic, "TypeB", 0)
	if err != nil {
		t.Fatalf("Failed to get events of type TypeB: %v", err)
	}
	if len(typeBEvents) != 2 {
		t.Fatalf("Expected 2 events of type TypeB, got %d", len(typeBEvents))
	}

	// Test getting events of type "TypeC"
	typeCEvents, err := repo.GetEventsByType(ctx, topic, "TypeC", 0)
	if err != nil {
		t.Fatalf("Failed to get events of type TypeC: %v", err)
	}
	if len(typeCEvents) != 1 {
		t.Fatalf("Expected 1 event of type TypeC, got %d", len(typeCEvents))
	}

	// Test getting events of a non-existent type
	nonExistentTypeEvents, err := repo.GetEventsByType(ctx, topic, "NonExistentType", 0)
	if err != nil {
		t.Fatalf("Failed to get events of non-existent type: %v", err)
	}
	if len(nonExistentTypeEvents) != 0 {
		t.Fatalf("Expected 0 events of non-existent type, got %d", len(nonExistentTypeEvents))
	}

	// Test getting events of a specific type with fromVersion
	// The third event (version 3) is TypeA, so getting TypeA events after version 3 should return none
	typeAEventsAfterVersion3, err := repo.GetEventsByType(ctx, topic, "TypeA", 3)
	if err != nil {
		t.Fatalf("Failed to get TypeA events after version 3: %v", err)
	}
	if len(typeAEventsAfterVersion3) != 0 {
		t.Fatalf("Expected 0 TypeA events after version 3, got %d", len(typeAEventsAfterVersion3))
	}

	// Test with non-existent topic
	_, err = repo.GetEventsByType(ctx, "non-existent-topic", "TypeA", 0)
	if err == nil {
		t.Error("Expected error when getting events by type from non-existent topic, got nil")
	}
}

func TestGetLatestVersion(t *testing.T) {
	// Create temporary directory for testing
	tempDir := createTempDir(t)
	defer os.RemoveAll(tempDir)

	// Create repository
	repo := NewFileSystemEventRepository(tempDir)
	ctx := context.Background()

	// Initialize the repository
	err := repo.Initialize(ctx)
	if err != nil {
		t.Fatalf("Failed to initialize repository: %v", err)
	}

	// Create a test topic
	topic := "test-topic"
	topicConfig := outbound.TopicConfig{
		Name:       topic,
		Adapter:    "fs",
		Connection: tempDir,
	}

	err = repo.CreateTopic(ctx, topicConfig)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Test getting latest version for a new topic
	latestVersion, err := repo.GetLatestVersion(ctx, topic)
	if err != nil {
		t.Fatalf("Failed to get latest version for new topic: %v", err)
	}
	if latestVersion != 0 {
		t.Errorf("Expected latest version to be 0 for new topic, got %d", latestVersion)
	}

	// Append a single event
	singleEvent := []outbound.Event{
		{
			Type: "TestEvent",
			Data: map[string]interface{}{
				"key": "value",
			},
		},
	}

	err = repo.AppendEvents(ctx, topic, singleEvent)
	if err != nil {
		t.Fatalf("Failed to append event: %v", err)
	}

	// Test getting latest version after appending one event
	latestVersion, err = repo.GetLatestVersion(ctx, topic)
	if err != nil {
		t.Fatalf("Failed to get latest version after appending one event: %v", err)
	}
	if latestVersion != 1 {
		t.Errorf("Expected latest version to be 1 after appending one event, got %d", latestVersion)
	}

	// Append multiple events
	multipleEvents := []outbound.Event{
		{
			Type: "TestEvent1",
			Data: map[string]interface{}{"key1": "value1"},
		},
		{
			Type: "TestEvent2",
			Data: map[string]interface{}{"key2": "value2"},
		},
		{
			Type: "TestEvent3",
			Data: map[string]interface{}{"key3": "value3"},
		},
	}

	err = repo.AppendEvents(ctx, topic, multipleEvents)
	if err != nil {
		t.Fatalf("Failed to append multiple events: %v", err)
	}

	// Test getting latest version after appending multiple events
	latestVersion, err = repo.GetLatestVersion(ctx, topic)
	if err != nil {
		t.Fatalf("Failed to get latest version after appending multiple events: %v", err)
	}
	if latestVersion != 4 {
		t.Errorf("Expected latest version to be 4 after appending multiple events, got %d", latestVersion)
	}

	// Test getting latest version for a non-existent topic
	_, err = repo.GetLatestVersion(ctx, "non-existent-topic")
	if err == nil {
		t.Error("Expected error when getting latest version for non-existent topic, got nil")
	}

	// Test with repository reload - create a new instance pointing to the same directory
	repo2 := NewFileSystemEventRepository(tempDir)
	err = repo2.Initialize(ctx)
	if err != nil {
		t.Fatalf("Failed to initialize second repository instance: %v", err)
	}

	// Verify the latest version is correctly loaded
	latestVersion, err = repo2.GetLatestVersion(ctx, topic)
	if err != nil {
		t.Fatalf("Failed to get latest version from reloaded repository: %v", err)
	}
	if latestVersion != 4 {
		t.Errorf("Expected latest version to be 4 from reloaded repository, got %d", latestVersion)
	}

	// Test with concurrent appending and checking
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			// Append an event
			event := []outbound.Event{
				{
					Type: fmt.Sprintf("ConcurrentEvent%d", index),
					Data: map[string]interface{}{"index": index},
				},
			}

			appendErr := repo.AppendEvents(ctx, topic, event)
			if appendErr != nil {
				t.Errorf("Failed to append event in goroutine %d: %v", index, appendErr)
				return
			}

			// Get and verify latest version increases
			_, getErr := repo.GetLatestVersion(ctx, topic)
			if getErr != nil {
				t.Errorf("Failed to get latest version in goroutine %d: %v", index, getErr)
			}
		}(i)
	}

	wg.Wait()

	// Final version should be 9 (4 + 5 concurrent events)
	finalVersion, err := repo.GetLatestVersion(ctx, topic)
	if err != nil {
		t.Fatalf("Failed to get final version: %v", err)
	}
	if finalVersion != 9 {
		t.Errorf("Expected final version to be 9, got %d", finalVersion)
	}
}
