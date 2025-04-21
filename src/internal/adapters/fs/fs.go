package fs

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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

// Initialize prepares the repository for use
func (fs *FileSystemEventRepository) Initialize(ctx context.Context) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Create base directory structure
	if err := fs.ensureDirStructure(); err != nil {
		return err
	}

	// Load existing topic configurations
	if err := fs.loadTopicConfigurations(); err != nil {
		return err
	}

	return nil
}

// Close cleans up resources used by the repository
func (fs *FileSystemEventRepository) Close() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Clear in-memory caches
	fs.latestVersion = make(map[string]int64)
	fs.topicConfigs = make(map[string]outbound.TopicConfig)

	// No file handles to close as we open and close files as needed
	// No other resources to clean up

	return nil
}

// determineLatestVersion scans the topic directory to find the highest event version
func (fs *FileSystemEventRepository) determineLatestVersion(topic string) (int64, error) {
	topicDir := fs.getTopicDir(topic)

	// If directory doesn't exist yet, return 0 as the version
	if _, err := os.Stat(topicDir); os.IsNotExist(err) {
		return 0, nil
	}

	entries, err := os.ReadDir(topicDir)
	if err != nil {
		return 0, fmt.Errorf("failed to read topic directory: %w", err)
	}

	var latestVersion int64 = 0
	var parseErrors []string

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}

		// Extract version from filename (format: 00000000000000000001.json)
		baseName := strings.TrimSuffix(entry.Name(), ".json")
		version, err := strconv.ParseInt(baseName, 10, 64)
		if err != nil {
			// Log the error but continue processing other files
			parseErrors = append(parseErrors, fmt.Sprintf("%s: %v", entry.Name(), err))
			continue
		}

		if version > latestVersion {
			latestVersion = version
		}
	}

	// If we encountered parse errors but still found valid events, log the errors but don't fail
	if len(parseErrors) > 0 && latestVersion > 0 {
		fmt.Printf("WARNING: Failed to parse some event filenames for topic %s: %s\n",
			topic, strings.Join(parseErrors, "; "))
	}

	return latestVersion, nil
}

// ensureDirStructure creates all required directories for the repository
func (fs *FileSystemEventRepository) ensureDirStructure() error {
	// Create base directory
	if err := fs.ensureBaseDir(); err != nil {
		return err
	}

	// Create configs directory
	if err := fs.ensureConfigsDir(); err != nil {
		return err
	}

	// Create events directory
	if err := fs.ensureEventsDir(); err != nil {
		return err
	}

	return nil
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

// loadTopicConfigurations reads and loads all topic configurations from disk
func (fs *FileSystemEventRepository) loadTopicConfigurations() error {
	configDir := fs.getConfigDir()
	entries, err := os.ReadDir(configDir)
	if err != nil {
		return fmt.Errorf("failed to read config directory: %w", err)
	}

	// Process each topic configuration file
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}

		// Load topic configuration
		config, err := fs.loadTopicConfig(entry.Name())
		if err != nil {
			return err
		}

		// Store config in memory
		fs.topicConfigs[config.Name] = config

		// Determine latest version for this topic
		latestVersion, err := fs.determineLatestVersion(config.Name)
		if err != nil {
			return fmt.Errorf("failed to determine latest version for topic %s: %w", config.Name, err)
		}

		fs.latestVersion[config.Name] = latestVersion
	}

	return nil
}

// loadTopicConfig loads a single topic configuration from a file
func (fs *FileSystemEventRepository) loadTopicConfig(filename string) (outbound.TopicConfig, error) {
	configPath := filepath.Join(fs.getConfigDir(), filename)
	data, err := os.ReadFile(configPath)
	if err != nil {
		return outbound.TopicConfig{}, fmt.Errorf("failed to read config file %s: %w", configPath, err)
	}

	var config outbound.TopicConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return outbound.TopicConfig{}, fmt.Errorf("failed to parse config file %s: %w", configPath, err)
	}

	return config, nil
}
