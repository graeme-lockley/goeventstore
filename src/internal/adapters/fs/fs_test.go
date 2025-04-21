package fs

import (
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
