package fs

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"goeventsource/src/internal/port/outbound"
)

// FileSystemEventRepository implements the EventRepository interface using file system storage
type FileSystemEventRepository struct {
	mu            sync.RWMutex
	basePath      string                          // Base path for storage
	latestVersion map[string]int64                // topic -> latest version (in-memory cache)
	topicConfigs  map[string]outbound.TopicConfig // In-memory cache of topic configurations
}

// NewFileSystemEventRepository creates a new file system event repository with the specified base path
func NewFileSystemEventRepository(basePath string) *FileSystemEventRepository {
	return &FileSystemEventRepository{
		basePath:      basePath,
		latestVersion: make(map[string]int64),
		topicConfigs:  make(map[string]outbound.TopicConfig),
	}
}

// ensureBaseDir creates the base directory if it doesn't exist
func (fs *FileSystemEventRepository) ensureBaseDir() error {
	if err := os.MkdirAll(fs.basePath, 0755); err != nil {
		return fmt.Errorf("failed to create base directory: %w", err)
	}
	return nil
}

// ensureConfigsDir creates the configs directory if it doesn't exist
func (fs *FileSystemEventRepository) ensureConfigsDir() error {
	configDir := fs.getConfigDir()
	if err := os.MkdirAll(configDir, 0755); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}
	return nil
}

// ensureEventsDir creates the events directory if it doesn't exist
func (fs *FileSystemEventRepository) ensureEventsDir() error {
	eventsDir := fs.getEventsDir()
	if err := os.MkdirAll(eventsDir, 0755); err != nil {
		return fmt.Errorf("failed to create events directory: %w", err)
	}
	return nil
}

// ensureTopicDir creates a topic-specific directory under events if it doesn't exist
func (fs *FileSystemEventRepository) ensureTopicDir(topic string) error {
	topicDir := fs.getTopicDir(topic)
	if err := os.MkdirAll(topicDir, 0755); err != nil {
		return fmt.Errorf("failed to create topic directory: %w", err)
	}
	return nil
}

// getConfigDir returns the path to the configs directory
func (fs *FileSystemEventRepository) getConfigDir() string {
	return filepath.Join(fs.basePath, "configs")
}

// getEventsDir returns the path to the events directory
func (fs *FileSystemEventRepository) getEventsDir() string {
	return filepath.Join(fs.basePath, "events")
}

// getTopicDir returns the path to a specific topic directory
func (fs *FileSystemEventRepository) getTopicDir(topic string) string {
	return filepath.Join(fs.getEventsDir(), topic)
}
