package fs

import (
	"context"
	"encoding/json"
	"fmt"
	"goeventsource/src/internal/port/outbound"
	"os"
	"path/filepath"
	"testing"
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
