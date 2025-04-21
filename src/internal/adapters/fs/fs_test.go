package fs

import (
	"goeventsource/src/internal/port/outbound"
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
