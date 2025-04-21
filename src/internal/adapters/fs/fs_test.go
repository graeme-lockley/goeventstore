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
	configDir := filepath.Join(tempDir, "configs")
	err = os.MkdirAll(configDir, 0755)
	if err != nil {
		t.Errorf("Failed to create configs directory: %v", err)
	}

	if _, err := os.Stat(configDir); os.IsNotExist(err) {
		t.Error("Configs directory was not created")
	}

	// Test ensureEventsDir
	eventsDir := filepath.Join(tempDir, "events")
	err = os.MkdirAll(eventsDir, 0755)
	if err != nil {
		t.Errorf("Failed to create events directory: %v", err)
	}

	if _, err := os.Stat(eventsDir); os.IsNotExist(err) {
		t.Error("Events directory was not created")
	}

	// Test ensureTopicDir
	testTopic := "test-topic"
	topicDir := filepath.Join(tempDir, "events", testTopic)
	err = os.MkdirAll(topicDir, 0755)
	if err != nil {
		t.Errorf("Failed to create topic directory: %v", err)
	}

	if _, err := os.Stat(topicDir); os.IsNotExist(err) {
		t.Error("Topic directory was not created")
	}

	// Test getConfigDir
	if filepath.Join(tempDir, "configs") != configDir {
		t.Errorf("Incorrect config directory path, got %s, want %s", filepath.Join(tempDir, "configs"), configDir)
	}

	// Test getEventsDir
	if filepath.Join(tempDir, "events") != eventsDir {
		t.Errorf("Incorrect events directory path, got %s, want %s", filepath.Join(tempDir, "events"), eventsDir)
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
	configDir := filepath.Join(tempDir, "configs")
	if _, err := os.Stat(configDir); os.IsNotExist(err) {
		t.Error("Configs directory was not created during initialization")
	}

	eventsDir := filepath.Join(tempDir, "events")
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
		eventFilename := filepath.Join(topicDir, fmt.Sprintf("v%010d.json", i))
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

	// Create a test topic using the CreateTopic method
	topicName := "test-topic"
	topicConfig := outbound.TopicConfig{
		Name:       topicName,
		Adapter:    "fs",
		Connection: tempDir,
		Options:    map[string]string{"option1": "value1"},
	}

	// Create the topic using the proper method
	err = repo.CreateTopic(ctx, topicConfig)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Create and append a test event to the topic
	testEvent := outbound.Event{
		ID:        "test-id",
		Type:      "TestEvent",
		Data:      map[string]interface{}{"test": "data"},
		Timestamp: time.Now().UnixNano(),
	}

	err = repo.AppendEvents(ctx, topicName, []outbound.Event{testEvent})
	if err != nil {
		t.Fatalf("Failed to append event: %v", err)
	}

	// Verify maps have data before closing
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
	configPath := filepath.Join(tempDir, "configs", topicName+".json")
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
	firstEventPath := filepath.Join(topicDir, fmt.Sprintf("v%010d.json", 1))
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
	customEventPath := filepath.Join(topicDir, fmt.Sprintf("v%010d.json", 3))
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

func TestCreateTopic(t *testing.T) {
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
		Options:    map[string]string{"option1": "value1"},
	}

	// Create the topic
	err = repo.CreateTopic(ctx, topicConfig)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Verify topic is stored in memory
	if _, exists := repo.topicConfigs[topic]; !exists {
		t.Error("Topic configuration not stored in memory after creation")
	}

	// Verify latest version is initialized to 0
	if version, exists := repo.latestVersion[topic]; !exists || version != 0 {
		t.Errorf("Expected latest version to be initialized to 0, got %d", version)
	}

	// Verify topic configuration file was created
	configPath := filepath.Join(tempDir, "configs", topic+".json")
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		t.Error("Topic configuration file does not exist after creating topic")
	}

	// Read and verify the configuration file content
	configData, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("Failed to read topic configuration file: %v", err)
	}

	var savedConfig outbound.TopicConfig
	err = json.Unmarshal(configData, &savedConfig)
	if err != nil {
		t.Fatalf("Failed to unmarshal saved topic config: %v", err)
	}

	if savedConfig.Name != topic {
		t.Errorf("Saved topic name mismatch: expected %s, got %s", topic, savedConfig.Name)
	}
	if savedConfig.Adapter != "fs" {
		t.Errorf("Saved adapter mismatch: expected fs, got %s", savedConfig.Adapter)
	}
	if val, exists := savedConfig.Options["option1"]; !exists || val != "value1" {
		t.Errorf("Saved options mismatch: expected %v, got %v", topicConfig.Options, savedConfig.Options)
	}

	// Verify topic directory was created
	topicDir := repo.getTopicDir(topic)
	if _, err := os.Stat(topicDir); os.IsNotExist(err) {
		t.Error("Topic directory does not exist after creating topic")
	}

	// Test creating a topic that already exists (should fail)
	err = repo.CreateTopic(ctx, topicConfig)
	if err == nil {
		t.Error("Expected error when creating topic that already exists, got nil")
	}

	// Verify the error is a TopicError with ErrTopicAlreadyExists
	if !IsTopicAlreadyExists(err) {
		t.Errorf("Expected TopicError with ErrTopicAlreadyExists, got %T: %v", err, err)
	}
}

func TestDeleteTopic(t *testing.T) {
	// Create temporary directory for test
	tempDir := createTempDir(t)
	defer os.RemoveAll(tempDir)

	// Create repository
	repo := NewFileSystemEventRepository(tempDir)
	err := repo.Initialize(context.Background())
	if err != nil {
		t.Fatalf("Failed to initialize repository: %v", err)
	}

	// Create a test topic
	topic := "test-topic"
	config := outbound.TopicConfig{
		Name:    topic,
		Adapter: "fs",
	}
	err = repo.CreateTopic(context.Background(), config)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Add an event to the topic to ensure files are created
	events := []outbound.Event{
		{
			Type: "TestEvent",
			Data: map[string]interface{}{
				"key": "value",
			},
		},
	}
	err = repo.AppendEvents(context.Background(), topic, events)
	if err != nil {
		t.Fatalf("Failed to append events: %v", err)
	}

	// Verify topic directory and config file exist before deletion
	topicDir := filepath.Join(tempDir, "events", topic)
	configPath := filepath.Join(tempDir, "configs", topic+".json")

	if _, err := os.Stat(topicDir); os.IsNotExist(err) {
		t.Fatalf("Topic directory should exist before deletion: %v", err)
	}

	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		t.Fatalf("Config file should exist before deletion: %v", err)
	}

	// Delete the topic
	err = repo.DeleteTopic(context.Background(), topic)
	if err != nil {
		t.Fatalf("Failed to delete topic: %v", err)
	}

	// Verify topic is removed from in-memory maps
	repo.mu.RLock()
	_, exists := repo.latestVersion[topic]
	if exists {
		t.Errorf("Expected topic to be removed from latestVersion map")
	}

	_, exists = repo.topicConfigs[topic]
	if exists {
		t.Errorf("Expected topic to be removed from topicConfigs map")
	}
	repo.mu.RUnlock()

	// Verify topic directory and config file are deleted
	if _, err := os.Stat(topicDir); !os.IsNotExist(err) {
		t.Errorf("Topic directory should not exist after deletion")
	}

	if _, err := os.Stat(configPath); !os.IsNotExist(err) {
		t.Errorf("Config file should not exist after deletion")
	}

	// Test deleting a non-existent topic
	err = repo.DeleteTopic(context.Background(), "non-existent-topic")
	if err == nil {
		t.Errorf("Deleting a non-existent topic should return an error")
	} else if _, ok := err.(*TopicError); !ok {
		t.Errorf("Error should be a TopicError, got: %T", err)
	}
}

func TestListTopics(t *testing.T) {
	// Create temporary directory for test
	tempDir := createTempDir(t)
	defer os.RemoveAll(tempDir)

	// Create repository
	repo := NewFileSystemEventRepository(tempDir)
	err := repo.Initialize(context.Background())
	if err != nil {
		t.Fatalf("Failed to initialize repository: %v", err)
	}

	// Initially, no topics should exist
	topics, err := repo.ListTopics(context.Background())
	if err != nil {
		t.Fatalf("Failed to list topics: %v", err)
	}
	if len(topics) != 0 {
		t.Errorf("Initially no topics should exist, got %d", len(topics))
	}

	// Create multiple test topics
	testTopics := []struct {
		name    string
		adapter string
		options map[string]string
	}{
		{
			name:    "topic1",
			adapter: "fs",
			options: map[string]string{"option1": "value1"},
		},
		{
			name:    "topic2",
			adapter: "fs",
			options: map[string]string{"option2": "value2"},
		},
		{
			name:    "topic3",
			adapter: "fs",
			options: map[string]string{"option3": "value3"},
		},
	}

	// Create each topic
	for _, tc := range testTopics {
		config := outbound.TopicConfig{
			Name:    tc.name,
			Adapter: tc.adapter,
			Options: tc.options,
		}
		err := repo.CreateTopic(context.Background(), config)
		if err != nil {
			t.Fatalf("Failed to create topic %s: %v", tc.name, err)
		}
	}

	// List topics and verify
	topics, err = repo.ListTopics(context.Background())
	if err != nil {
		t.Fatalf("Failed to list topics: %v", err)
	}
	if len(topics) != len(testTopics) {
		t.Errorf("Should return all created topics, expected %d, got %d", len(testTopics), len(topics))
	}

	// Verify each topic is in the list with correct configuration
	topicMap := make(map[string]outbound.TopicConfig)
	for _, topic := range topics {
		topicMap[topic.Name] = topic
	}

	for _, expectedTopic := range testTopics {
		topic, exists := topicMap[expectedTopic.name]
		if !exists {
			t.Errorf("Topic %s should exist in the list", expectedTopic.name)
			continue
		}
		if topic.Adapter != expectedTopic.adapter {
			t.Errorf("Expected adapter %s for topic %s, got %s", expectedTopic.adapter, expectedTopic.name, topic.Adapter)
		}
		// Check if all options match
		if len(topic.Options) != len(expectedTopic.options) {
			t.Errorf("Options count mismatch for topic %s: expected %d, got %d",
				expectedTopic.name, len(expectedTopic.options), len(topic.Options))
		}
		for k, v := range expectedTopic.options {
			if topic.Options[k] != v {
				t.Errorf("Option mismatch for topic %s: expected %s=%s, got %s",
					expectedTopic.name, k, v, topic.Options[k])
			}
		}
	}

	// Delete one topic
	err = repo.DeleteTopic(context.Background(), testTopics[0].name)
	if err != nil {
		t.Fatalf("Failed to delete topic %s: %v", testTopics[0].name, err)
	}

	// Verify topic list is updated
	topics, err = repo.ListTopics(context.Background())
	if err != nil {
		t.Fatalf("Failed to list topics after deletion: %v", err)
	}
	if len(topics) != len(testTopics)-1 {
		t.Errorf("Should return one less topic after deletion, expected %d, got %d",
			len(testTopics)-1, len(topics))
	}

	// Verify the deleted topic is not in the list
	for _, topic := range topics {
		if topic.Name == testTopics[0].name {
			t.Errorf("Deleted topic %s should not be in the list", testTopics[0].name)
		}
	}
}

func TestTopicExists(t *testing.T) {
	// Create temporary directory for test
	tempDir := createTempDir(t)
	defer os.RemoveAll(tempDir)

	// Create repository
	repo := NewFileSystemEventRepository(tempDir)
	err := repo.Initialize(context.Background())
	if err != nil {
		t.Fatalf("Failed to initialize repository: %v", err)
	}

	// Initially, no topics should exist
	exists, err := repo.TopicExists(context.Background(), "test-topic")
	if err != nil {
		t.Fatalf("Failed to check if topic exists: %v", err)
	}
	if exists {
		t.Errorf("Topic should not exist initially")
	}

	// Create a test topic
	topic := "test-topic"
	config := outbound.TopicConfig{
		Name:    topic,
		Adapter: "fs",
		Options: map[string]string{"option1": "value1"},
	}
	err = repo.CreateTopic(context.Background(), config)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Check if topic exists after creation
	exists, err = repo.TopicExists(context.Background(), topic)
	if err != nil {
		t.Fatalf("Failed to check if topic exists: %v", err)
	}
	if !exists {
		t.Errorf("Topic should exist after creation")
	}

	// Check for a non-existent topic
	exists, err = repo.TopicExists(context.Background(), "non-existent-topic")
	if err != nil {
		t.Fatalf("Failed to check if topic exists: %v", err)
	}
	if exists {
		t.Errorf("Non-existent topic should not exist")
	}

	// Delete the topic
	err = repo.DeleteTopic(context.Background(), topic)
	if err != nil {
		t.Fatalf("Failed to delete topic: %v", err)
	}

	// Check if topic exists after deletion
	exists, err = repo.TopicExists(context.Background(), topic)
	if err != nil {
		t.Fatalf("Failed to check if topic exists: %v", err)
	}
	if exists {
		t.Errorf("Topic should not exist after deletion")
	}
}

func TestUpdateTopicConfig(t *testing.T) {
	// Create a temporary directory
	tempDir := createTempDir(t)

	// Initialize the repository
	repo := NewFileSystemEventRepository(tempDir)
	err := repo.Initialize(context.Background())
	if err != nil {
		t.Fatalf("Failed to initialize repository: %v", err)
	}

	// Create initial topic config
	initialConfig := outbound.TopicConfig{
		Name:       "test-topic",
		Adapter:    "fs",
		Connection: tempDir,
		Options: map[string]string{
			"option1": "value1",
			"option2": "value2",
		},
	}

	// Create a topic
	err = repo.CreateTopic(context.Background(), initialConfig)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Get the topic config
	retrievedConfig, err := repo.GetTopicConfig(context.Background(), "test-topic")
	if err != nil {
		t.Fatalf("Failed to get topic config: %v", err)
	}

	// Verify initial config
	if retrievedConfig.Name != initialConfig.Name {
		t.Errorf("Expected topic name to be %s, got %s", initialConfig.Name, retrievedConfig.Name)
	}
	if retrievedConfig.Adapter != initialConfig.Adapter {
		t.Errorf("Expected adapter to be %s, got %s", initialConfig.Adapter, retrievedConfig.Adapter)
	}
	if retrievedConfig.Connection != initialConfig.Connection {
		t.Errorf("Expected connection to be %s, got %s", initialConfig.Connection, retrievedConfig.Connection)
	}
	if retrievedConfig.Options["option1"] != initialConfig.Options["option1"] {
		t.Errorf("Expected option1 to be %s, got %s", initialConfig.Options["option1"], retrievedConfig.Options["option1"])
	}
	if retrievedConfig.Options["option2"] != initialConfig.Options["option2"] {
		t.Errorf("Expected option2 to be %s, got %s", initialConfig.Options["option2"], retrievedConfig.Options["option2"])
	}

	// Update the topic config
	updatedConfig := outbound.TopicConfig{
		Name:       "test-topic", // name must remain the same
		Adapter:    "fs",         // adapter type remains the same
		Connection: tempDir,      // connection remains the same
		Options: map[string]string{
			"option1": "new-value1", // changed value
			"option3": "value3",     // new option
			// option2 is removed
		},
	}

	err = repo.UpdateTopicConfig(context.Background(), updatedConfig)
	if err != nil {
		t.Fatalf("Failed to update topic config: %v", err)
	}

	// Get the updated config
	retrievedConfig, err = repo.GetTopicConfig(context.Background(), "test-topic")
	if err != nil {
		t.Fatalf("Failed to get updated topic config: %v", err)
	}

	// Verify updated config
	if retrievedConfig.Name != updatedConfig.Name {
		t.Errorf("Expected topic name to be %s, got %s", updatedConfig.Name, retrievedConfig.Name)
	}
	if retrievedConfig.Adapter != updatedConfig.Adapter {
		t.Errorf("Expected adapter to be %s, got %s", updatedConfig.Adapter, retrievedConfig.Adapter)
	}
	if retrievedConfig.Options["option1"] != updatedConfig.Options["option1"] {
		t.Errorf("Expected option1 to be %s, got %s", updatedConfig.Options["option1"], retrievedConfig.Options["option1"])
	}
	if retrievedConfig.Options["option3"] != updatedConfig.Options["option3"] {
		t.Errorf("Expected option3 to be %s, got %s", updatedConfig.Options["option3"], retrievedConfig.Options["option3"])
	}
	if _, exists := retrievedConfig.Options["option2"]; exists {
		t.Errorf("Expected option2 to be removed, but it still exists with value %s", retrievedConfig.Options["option2"])
	}

	// Try to update a non-existent topic
	err = repo.UpdateTopicConfig(context.Background(), outbound.TopicConfig{
		Name: "non-existent-topic",
	})
	if err == nil {
		t.Errorf("Expected error when updating non-existent topic, but got nil")
	}

	// Verify config file was actually updated on disk
	configPath := filepath.Join(tempDir, "configs", "test-topic.json")
	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("Failed to read config file: %v", err)
	}

	var fileConfig outbound.TopicConfig
	err = json.Unmarshal(data, &fileConfig)
	if err != nil {
		t.Fatalf("Failed to unmarshal config file: %v", err)
	}

	if fileConfig.Options["option1"] != updatedConfig.Options["option1"] {
		t.Errorf("Config file not updated correctly. Expected option1 to be %s, got %s",
			updatedConfig.Options["option1"], fileConfig.Options["option1"])
	}
	if fileConfig.Options["option3"] != updatedConfig.Options["option3"] {
		t.Errorf("Config file not updated correctly. Expected option3 to be %s, got %s",
			updatedConfig.Options["option3"], fileConfig.Options["option3"])
	}
	if _, exists := fileConfig.Options["option2"]; exists {
		t.Errorf("Config file not updated correctly. Expected option2 to be removed, but it still exists")
	}
}

func TestGetTopicConfig(t *testing.T) {
	// Create a temporary directory
	tempDir := createTempDir(t)

	// Initialize the repository
	repo := NewFileSystemEventRepository(tempDir)
	err := repo.Initialize(context.Background())
	if err != nil {
		t.Fatalf("Failed to initialize repository: %v", err)
	}

	// Try to get config for a non-existent topic
	_, err = repo.GetTopicConfig(context.Background(), "non-existent-topic")
	if err == nil {
		t.Errorf("Expected error when getting config for non-existent topic, but got nil")
	}

	// Create topic with configuration
	config := outbound.TopicConfig{
		Name:       "test-topic",
		Adapter:    "fs",
		Connection: tempDir,
		Options: map[string]string{
			"option1": "value1",
			"option2": "value2",
		},
	}

	err = repo.CreateTopic(context.Background(), config)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Get the topic config
	retrievedConfig, err := repo.GetTopicConfig(context.Background(), "test-topic")
	if err != nil {
		t.Fatalf("Failed to get topic config: %v", err)
	}

	// Verify retrieved config matches original
	if retrievedConfig.Name != config.Name {
		t.Errorf("Expected topic name to be %s, got %s", config.Name, retrievedConfig.Name)
	}
	if retrievedConfig.Adapter != config.Adapter {
		t.Errorf("Expected adapter to be %s, got %s", config.Adapter, retrievedConfig.Adapter)
	}
	if retrievedConfig.Connection != config.Connection {
		t.Errorf("Expected connection to be %s, got %s", config.Connection, retrievedConfig.Connection)
	}
	if retrievedConfig.Options["option1"] != config.Options["option1"] {
		t.Errorf("Expected option1 to be %s, got %s", config.Options["option1"], retrievedConfig.Options["option1"])
	}
	if retrievedConfig.Options["option2"] != config.Options["option2"] {
		t.Errorf("Expected option2 to be %s, got %s", config.Options["option2"], retrievedConfig.Options["option2"])
	}

	// Test retrieving config after repository is reinitialized
	// Close and reopen repository
	err = repo.Close()
	if err != nil {
		t.Fatalf("Failed to close repository: %v", err)
	}

	// Create a new instance of the repository with the same base path
	repo2 := NewFileSystemEventRepository(tempDir)
	err = repo2.Initialize(context.Background())
	if err != nil {
		t.Fatalf("Failed to initialize repository: %v", err)
	}

	// Get the topic config again
	retrievedConfig, err = repo2.GetTopicConfig(context.Background(), "test-topic")
	if err != nil {
		t.Fatalf("Failed to get topic config after reinitializing: %v", err)
	}

	// Verify config is still correct
	if retrievedConfig.Name != config.Name {
		t.Errorf("After reinitializing, expected topic name to be %s, got %s", config.Name, retrievedConfig.Name)
	}
	if retrievedConfig.Adapter != config.Adapter {
		t.Errorf("After reinitializing, expected adapter to be %s, got %s", config.Adapter, retrievedConfig.Adapter)
	}
	if retrievedConfig.Options["option1"] != config.Options["option1"] {
		t.Errorf("After reinitializing, expected option1 to be %s, got %s", config.Options["option1"], retrievedConfig.Options["option1"])
	}
	if retrievedConfig.Options["option2"] != config.Options["option2"] {
		t.Errorf("After reinitializing, expected option2 to be %s, got %s", config.Options["option2"], retrievedConfig.Options["option2"])
	}
}
