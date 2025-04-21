package fs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"goeventsource/src/internal/port/outbound"

	"github.com/google/uuid"
)

// Common errors
var (
	ErrNotReady = errors.New("repository not in ready state")
	// Using ErrTopicAlreadyExists from errors.go instead of redefining it here
)

// FileSystemEventRepository implements the EventRepository interface using file system storage
type FileSystemEventRepository struct {
	mu            sync.RWMutex
	basePath      string                          // Base path for storage
	latestVersion map[string]int64                // topic -> latest version (in-memory cache)
	topicConfigs  map[string]outbound.TopicConfig // In-memory cache of topic configurations
	logger        *log.Logger                     // Standard Go logger
	state         outbound.RepositoryState        // Current state of the repository
}

// NewFileSystemEventRepository creates a new file system event repository with the specified base path
func NewFileSystemEventRepository(basePath string) *FileSystemEventRepository {
	return &FileSystemEventRepository{
		basePath:      basePath,
		latestVersion: make(map[string]int64),
		topicConfigs:  make(map[string]outbound.TopicConfig),
		logger:        log.New(os.Stderr, "[FS-REPO] ", log.LstdFlags),
		state:         outbound.StateUninitialized,
	}
}

// SetLogger allows setting a custom logger for the repository
func (fs *FileSystemEventRepository) SetLogger(logger *log.Logger) {
	if logger == nil {
		logger = log.New(os.Stderr, "[FS-REPO] ", log.LstdFlags)
	}
	fs.logger = logger
}

// Initialize initializes the file system event repository, creating directories if needed
func (fs *FileSystemEventRepository) Initialize(ctx context.Context) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Only initialize if not already in ready state
	if fs.state == outbound.StateReady {
		return nil
	}

	// Mark as initializing
	fs.state = outbound.StateInitializing

	fs.logger.Printf("Initializing file system event repository at %s", fs.basePath)

	// Ensure base directory exists
	if err := fs.ensureBaseDir(); err != nil {
		fs.logger.Printf("Failed to ensure base directory exists: %v", err)
		fs.state = outbound.StateFailed
		return err
	}

	// Ensure configs directory exists
	configDir := filepath.Join(fs.basePath, "configs")
	if err := os.MkdirAll(configDir, 0755); err != nil {
		fs.logger.Printf("Failed to create configs directory: %v", err)
		fs.state = outbound.StateFailed
		return fmt.Errorf("failed to create configs directory: %w", err)
	}

	// Ensure events directory exists
	eventsDir := filepath.Join(fs.basePath, "events")
	if err := os.MkdirAll(eventsDir, 0755); err != nil {
		fs.logger.Printf("Failed to create events directory: %v", err)
		fs.state = outbound.StateFailed
		return fmt.Errorf("failed to create events directory: %w", err)
	}

	// Load all topic configurations
	fs.latestVersion = make(map[string]int64)
	fs.topicConfigs = make(map[string]outbound.TopicConfig)

	entries, err := os.ReadDir(configDir)
	if err != nil {
		// If directory doesn't exist, just initialize empty maps
		if os.IsNotExist(err) {
			fs.logger.Printf("Config directory doesn't exist, initializing empty maps")
			fs.state = outbound.StateReady
			return nil
		}
		fs.logger.Printf("Failed to read configs directory: %v", err)
		fs.state = outbound.StateFailed
		return fmt.Errorf("failed to read configs directory: %w", err)
	}

	// Load each topic configuration
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}

		topic := strings.TrimSuffix(entry.Name(), ".json")

		config, err := fs.loadTopicConfig(topic)
		if err != nil {
			fs.logger.Printf("Failed to load topic config for %s: %v", topic, err)
			continue
		}

		fs.topicConfigs[topic] = config

		// Also determine the latest version
		latest, err := fs.determineLatestVersion(topic)
		if err != nil {
			fs.logger.Printf("Failed to determine latest version for %s: %v", topic, err)
			continue
		}

		fs.latestVersion[topic] = latest
	}

	fs.logger.Printf("Initialized file system event repository with %d topics", len(fs.topicConfigs))
	fs.state = outbound.StateReady
	return nil
}

// GetState returns the current state of the repository
func (fs *FileSystemEventRepository) GetState() outbound.RepositoryState {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.state
}

// Reopen reopens a closed repository
func (fs *FileSystemEventRepository) Reopen(ctx context.Context) error {
	fs.mu.Lock()

	// Only reopen if closed
	if fs.state != outbound.StateClosed {
		fs.mu.Unlock()
		return fmt.Errorf("repository is not in closed state, current state: %s", fs.state)
	}

	fs.logger.Printf("Reopening file system event repository at %s", fs.basePath)

	// Recreate data structures
	fs.latestVersion = make(map[string]int64)
	fs.topicConfigs = make(map[string]outbound.TopicConfig)

	// Mark as uninitialized so that Initialize will do a full reload
	fs.state = outbound.StateUninitialized

	// Unlock before calling Initialize
	fs.mu.Unlock()

	// Call initialize to reload data
	return fs.Initialize(ctx)
}

// Reset clears all data and reinitializes the repository
func (fs *FileSystemEventRepository) Reset(ctx context.Context) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Cannot reset if closed
	if fs.state == outbound.StateClosed {
		return fmt.Errorf("repository is closed, cannot reset")
	}

	fs.logger.Printf("Resetting file system event repository at %s", fs.basePath)

	// Mark as initializing
	fs.state = outbound.StateInitializing

	// Clear in-memory data
	fs.latestVersion = make(map[string]int64)
	fs.topicConfigs = make(map[string]outbound.TopicConfig)

	// Mark as uninitialized so that Initialize will do a full reload
	fs.state = outbound.StateUninitialized

	// Unlock before calling Initialize
	fs.mu.Unlock()

	// Call initialize to reload data
	return fs.Initialize(ctx)
}

// Close closes the file system event repository
func (fs *FileSystemEventRepository) Close() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Only close if in ready state
	if fs.state != outbound.StateReady {
		return nil
	}

	fs.logger.Printf("Closing file system event repository")

	// Mark as closing
	fs.state = outbound.StateClosing

	// Clear in-memory caches
	fs.latestVersion = nil
	fs.topicConfigs = nil

	// Mark as closed
	fs.state = outbound.StateClosed

	return nil
}

// AppendEvents appends events to a topic
func (fs *FileSystemEventRepository) AppendEvents(ctx context.Context, topic string, events []outbound.Event) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Check repository state
	if fs.state != outbound.StateReady {
		return ErrNotReady
	}

	// Verify topic exists
	if _, exists := fs.topicConfigs[topic]; !exists {
		return ErrTopicNotFound
	}

	// Get current latest version
	latestVersion := fs.latestVersion[topic]

	// Ensure topic directory exists
	topicDir := fs.getTopicDir(topic)
	if err := os.MkdirAll(topicDir, 0755); err != nil {
		return fmt.Errorf("failed to create topic directory: %w", err)
	}

	// Process and save each event
	for i := range events {
		// Set ID if not provided
		if events[i].ID == "" {
			events[i].ID = uuid.New().String()
		}

		// Set timestamp if not provided
		if events[i].Timestamp == 0 {
			events[i].Timestamp = time.Now().UnixNano()
		}

		// Increment version
		latestVersion++
		events[i].Version = latestVersion
		events[i].Topic = topic

		// Marshal event
		data, err := json.Marshal(events[i])
		if err != nil {
			return fmt.Errorf("failed to marshal event: %w", err)
		}

		// Write to file (use version as filename for ordering)
		filename := filepath.Join(topicDir, fmt.Sprintf("v%010d.json", latestVersion))

		// Write to temporary file first
		tempFilename := filename + ".tmp"
		if err := os.WriteFile(tempFilename, data, 0644); err != nil {
			return fmt.Errorf("failed to write event file: %w", err)
		}

		// Rename for atomic update
		if err := os.Rename(tempFilename, filename); err != nil {
			os.Remove(tempFilename) // Clean up temp file
			return fmt.Errorf("failed to finalize event file: %w", err)
		}
	}

	// Update latest version cache
	fs.latestVersion[topic] = latestVersion

	return nil
}

// GetEvents retrieves events for a topic, starting from a specific version
func (fs *FileSystemEventRepository) GetEvents(ctx context.Context, topic string, fromVersion int64) ([]outbound.Event, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	// Check repository state
	if fs.state != outbound.StateReady {
		return nil, ErrNotReady
	}

	// Verify topic exists
	if _, exists := fs.topicConfigs[topic]; !exists {
		return nil, ErrTopicNotFound
	}

	// Get topic directory
	topicDir := fs.getTopicDir(topic)

	// Check if topic directory exists
	_, err := os.Stat(topicDir)
	if os.IsNotExist(err) {
		return []outbound.Event{}, nil // Return empty slice if directory doesn't exist
	} else if err != nil {
		return nil, fmt.Errorf("failed to access topic directory: %w", err)
	}

	// List files in topic directory
	entries, err := os.ReadDir(topicDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read topic directory: %w", err)
	}

	// Process event files
	var events []outbound.Event

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}

		// Extract version from filename (assuming format vNNNNNNNNNN.json)
		versionStr := strings.TrimPrefix(strings.TrimSuffix(entry.Name(), ".json"), "v")
		version, err := strconv.ParseInt(versionStr, 10, 64)
		if err != nil {
			// Skip files that don't match expected format
			continue
		}

		// Skip events with lower version than requested
		if version < fromVersion {
			continue
		}

		// Skip events with exact fromVersion (we want events after the version)
		if version == fromVersion {
			continue
		}

		// Read and parse the event
		eventPath := filepath.Join(topicDir, entry.Name())
		data, err := os.ReadFile(eventPath)
		if err != nil {
			fs.logger.Printf("Failed to read event file %s: %v", eventPath, err)
			continue
		}

		var event outbound.Event
		if err := json.Unmarshal(data, &event); err != nil {
			fs.logger.Printf("Failed to unmarshal event from %s: %v", eventPath, err)
			continue
		}

		events = append(events, event)
	}

	// Sort events by version (may not be needed if filename guarantees order)
	sort.Slice(events, func(i, j int) bool {
		return events[i].Version < events[j].Version
	})

	return events, nil
}

// GetEventsByType retrieves events of a specific type for a topic
func (fs *FileSystemEventRepository) GetEventsByType(ctx context.Context, topic string, eventType string, fromVersion int64) ([]outbound.Event, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	// Check repository state
	if fs.state != outbound.StateReady {
		return nil, ErrNotReady
	}

	// Verify topic exists
	if _, exists := fs.topicConfigs[topic]; !exists {
		return nil, ErrTopicNotFound
	}

	// Get topic directory
	topicDir := fs.getTopicDir(topic)

	// Check if topic directory exists
	_, err := os.Stat(topicDir)
	if os.IsNotExist(err) {
		return []outbound.Event{}, nil // Return empty slice if directory doesn't exist
	} else if err != nil {
		return nil, fmt.Errorf("failed to access topic directory: %w", err)
	}

	// List files in topic directory
	entries, err := os.ReadDir(topicDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read topic directory: %w", err)
	}

	// Process event files
	var events []outbound.Event

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}

		// Extract version from filename (assuming format vNNNNNNNNNN.json)
		versionStr := strings.TrimPrefix(strings.TrimSuffix(entry.Name(), ".json"), "v")
		version, err := strconv.ParseInt(versionStr, 10, 64)
		if err != nil {
			// Skip files that don't match expected format
			continue
		}

		// Skip events with lower version than requested
		if version < fromVersion {
			continue
		}

		// Skip events with exact fromVersion (we want events after the version)
		if version == fromVersion {
			continue
		}

		// Read and parse the event
		eventPath := filepath.Join(topicDir, entry.Name())
		data, err := os.ReadFile(eventPath)
		if err != nil {
			fs.logger.Printf("Failed to read event file %s: %v", eventPath, err)
			continue
		}

		var event outbound.Event
		if err := json.Unmarshal(data, &event); err != nil {
			fs.logger.Printf("Failed to unmarshal event from %s: %v", eventPath, err)
			continue
		}

		// Only include events of the requested type
		if event.Type == eventType {
			events = append(events, event)
		}
	}

	// Sort events by version
	sort.Slice(events, func(i, j int) bool {
		return events[i].Version < events[j].Version
	})

	return events, nil
}

// GetLatestVersion returns the latest event version for a topic
func (fs *FileSystemEventRepository) GetLatestVersion(ctx context.Context, topic string) (int64, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	// Check repository state
	if fs.state != outbound.StateReady {
		return 0, ErrNotReady
	}

	// Verify topic exists
	if _, exists := fs.topicConfigs[topic]; !exists {
		return 0, ErrTopicNotFound
	}

	// Return cached value
	return fs.latestVersion[topic], nil
}

// CreateTopic creates a new topic
func (fs *FileSystemEventRepository) CreateTopic(ctx context.Context, config outbound.TopicConfig) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Check repository state
	if fs.state != outbound.StateReady {
		return ErrNotReady
	}

	// Check if topic already exists directly (without calling TopicExists which would try to acquire the same lock)
	if _, exists := fs.topicConfigs[config.Name]; exists {
		return ErrTopicAlreadyExists
	}

	// Ensure topic directory exists
	topicDir := fs.getTopicDir(config.Name)
	if err := os.MkdirAll(topicDir, 0755); err != nil {
		return fmt.Errorf("failed to create topic directory: %w", err)
	}

	// Marshal configuration
	data, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("failed to marshal topic config: %w", err)
	}

	// Write configuration to file
	configPath := fs.getTopicConfigPath(config.Name)

	// Write to temporary file first
	tempPath := configPath + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write topic config: %w", err)
	}

	// Rename for atomic update
	if err := os.Rename(tempPath, configPath); err != nil {
		os.Remove(tempPath) // Clean up temp file
		return fmt.Errorf("failed to finalize topic config: %w", err)
	}

	// Update in-memory state
	fs.topicConfigs[config.Name] = config
	fs.latestVersion[config.Name] = 0

	return nil
}

// DeleteTopic deletes a topic
func (fs *FileSystemEventRepository) DeleteTopic(ctx context.Context, topic string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Check repository state
	if fs.state != outbound.StateReady {
		return ErrNotReady
	}

	// Verify topic exists
	if _, exists := fs.topicConfigs[topic]; !exists {
		return NewTopicNotFoundError(topic)
	}

	// Delete topic directory
	topicDir := fs.getTopicDir(topic)
	if err := os.RemoveAll(topicDir); err != nil {
		return fmt.Errorf("failed to delete topic directory: %w", err)
	}

	// Delete configuration file
	configPath := fs.getTopicConfigPath(topic)
	if err := os.Remove(configPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete topic config: %w", err)
	}

	// Update in-memory state
	delete(fs.topicConfigs, topic)
	delete(fs.latestVersion, topic)

	return nil
}

// ListTopics returns a list of all topics
func (fs *FileSystemEventRepository) ListTopics(ctx context.Context) ([]outbound.TopicConfig, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	// Check repository state
	if fs.state != outbound.StateReady {
		return nil, ErrNotReady
	}

	// Convert map to slice
	topics := make([]outbound.TopicConfig, 0, len(fs.topicConfigs))
	for _, config := range fs.topicConfigs {
		topics = append(topics, config)
	}

	return topics, nil
}

// TopicExists checks if a topic exists
func (fs *FileSystemEventRepository) TopicExists(ctx context.Context, topic string) (bool, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	// Check repository state
	if fs.state != outbound.StateReady {
		return false, ErrNotReady
	}

	_, exists := fs.topicConfigs[topic]
	return exists, nil
}

// UpdateTopicConfig updates a topic's configuration
func (fs *FileSystemEventRepository) UpdateTopicConfig(ctx context.Context, config outbound.TopicConfig) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Check repository state
	if fs.state != outbound.StateReady {
		return ErrNotReady
	}

	// Verify topic exists
	if _, exists := fs.topicConfigs[config.Name]; !exists {
		return ErrTopicNotFound
	}

	// Marshal updated configuration
	data, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("failed to marshal topic config: %w", err)
	}

	// Write to configuration file
	configPath := fs.getTopicConfigPath(config.Name)

	// Write to temporary file first
	tempPath := configPath + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write topic config: %w", err)
	}

	// Rename for atomic update
	if err := os.Rename(tempPath, configPath); err != nil {
		os.Remove(tempPath) // Clean up temp file
		return fmt.Errorf("failed to finalize topic config: %w", err)
	}

	// Update in-memory state
	fs.topicConfigs[config.Name] = config

	return nil
}

// GetTopicConfig gets a topic's configuration
func (fs *FileSystemEventRepository) GetTopicConfig(ctx context.Context, topic string) (outbound.TopicConfig, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	// Check repository state
	if fs.state != outbound.StateReady {
		return outbound.TopicConfig{}, ErrNotReady
	}

	// Verify topic exists
	config, exists := fs.topicConfigs[topic]
	if !exists {
		return outbound.TopicConfig{}, ErrTopicNotFound
	}

	return config, nil
}

// Helper methods

// ensureBaseDir ensures the base directory exists
func (fs *FileSystemEventRepository) ensureBaseDir() error {
	if err := os.MkdirAll(fs.basePath, 0755); err != nil {
		return fmt.Errorf("failed to create base directory: %w", err)
	}
	return nil
}

// getTopicDir returns the path to a topic's events directory
func (fs *FileSystemEventRepository) getTopicDir(topic string) string {
	return filepath.Join(fs.basePath, "events", topic)
}

// loadTopicConfig loads a topic's configuration from disk
func (fs *FileSystemEventRepository) loadTopicConfig(topic string) (outbound.TopicConfig, error) {
	configPath := fs.getTopicConfigPath(topic)
	data, err := os.ReadFile(configPath)
	if err != nil {
		return outbound.TopicConfig{}, fmt.Errorf("failed to read topic config: %w", err)
	}

	var config outbound.TopicConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return outbound.TopicConfig{}, fmt.Errorf("failed to unmarshal topic config: %w", err)
	}

	return config, nil
}

// getTopicConfigPath returns the path to a topic's configuration file
func (fs *FileSystemEventRepository) getTopicConfigPath(topic string) string {
	return filepath.Join(fs.basePath, "configs", topic+".json")
}

// determineLatestVersion finds the latest event version for a topic by scanning the directory
func (fs *FileSystemEventRepository) determineLatestVersion(topic string) (int64, error) {
	topicDir := fs.getTopicDir(topic)

	// Ensure topic directory exists
	_, err := os.Stat(topicDir)
	if err != nil {
		if os.IsNotExist(err) {
			// If directory doesn't exist, latest version is 0
			return 0, nil
		}
		return 0, fmt.Errorf("failed to access topic directory: %w", err)
	}

	// List files in topic directory
	entries, err := os.ReadDir(topicDir)
	if err != nil {
		return 0, fmt.Errorf("failed to read topic directory: %w", err)
	}

	// Find highest version
	var latestVersion int64
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}

		// Extract version from filename (assuming format vNNNNNNNNNN.json)
		versionStr := strings.TrimPrefix(strings.TrimSuffix(entry.Name(), ".json"), "v")
		version, err := strconv.ParseInt(versionStr, 10, 64)
		if err != nil {
			// Skip files that don't match expected format
			continue
		}

		if version > latestVersion {
			latestVersion = version
		}
	}

	return latestVersion, nil
}

// Health returns health information about the file system repository
func (fs *FileSystemEventRepository) Health(ctx context.Context) (map[string]interface{}, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	// Create a base health report
	health := map[string]interface{}{
		"status":     string(outbound.StatusUp),
		"state":      string(fs.state),
		"type":       "filesystem",
		"basePath":   fs.basePath,
		"topicCount": len(fs.topicConfigs),
	}

	// If not in ready state, mark as down
	if fs.state != outbound.StateReady {
		health["status"] = string(outbound.StatusDown)
		health["message"] = "Repository is not in ready state"
		return health, fmt.Errorf("repository not in ready state: %s", fs.state)
	}

	// Check if the base directory exists and is accessible
	_, err := os.Stat(fs.basePath)
	if err != nil {
		health["status"] = string(outbound.StatusDown)
		health["error"] = fmt.Sprintf("base directory error: %v", err)
		return health, fmt.Errorf("base directory error: %w", err)
	}

	// Add information about each topic
	topicStats := make(map[string]interface{})
	for topic := range fs.topicConfigs {
		topicPath := fs.getTopicDir(topic)

		// Check if topic directory exists
		_, err := os.Stat(topicPath)
		if err != nil {
			topicStats[topic] = map[string]interface{}{
				"status": "error",
				"error":  fmt.Sprintf("directory error: %v", err),
			}
			continue
		}

		// Get the latest version
		latestVersion, exists := fs.latestVersion[topic]
		if !exists {
			latestVersion = 0
		}

		topicStats[topic] = map[string]interface{}{
			"status": "up",
			"path":   topicPath,
			"latest": latestVersion,
		}
	}

	health["topics"] = topicStats

	return health, nil
}

// HealthInfo returns structured health information
func (fs *FileSystemEventRepository) HealthInfo(ctx context.Context) (outbound.HealthInfo, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	// Default to up status
	status := outbound.StatusUp
	var message string
	var errorMsg string

	// Check state
	if fs.state != outbound.StateReady {
		status = outbound.StatusDown
		message = fmt.Sprintf("Repository is not in ready state: %s", fs.state)
		return outbound.HealthInfo{
			Status:  status,
			State:   fs.state,
			Message: message,
			AdditionalInfo: map[string]interface{}{
				"type":       "filesystem",
				"basePath":   fs.basePath,
				"topicCount": 0,
			},
		}, nil
	}

	// Check if the base directory exists and is accessible
	_, err := os.Stat(fs.basePath)
	if err != nil {
		status = outbound.StatusDown
		errorMsg = fmt.Sprintf("Base directory error: %v", err)
		return outbound.HealthInfo{
			Status: status,
			State:  fs.state,
			Error:  errorMsg,
			AdditionalInfo: map[string]interface{}{
				"type":       "filesystem",
				"basePath":   fs.basePath,
				"topicCount": 0,
			},
		}, err
	}

	// Prepare additional info
	additionalInfo := map[string]interface{}{
		"type":       "filesystem",
		"basePath":   fs.basePath,
		"topicCount": len(fs.topicConfigs),
	}

	// Add information about each topic
	topicStats := make(map[string]interface{})
	topicErrorCount := 0

	for topic := range fs.topicConfigs {
		topicPath := fs.getTopicDir(topic)

		// Check if topic directory exists
		_, err := os.Stat(topicPath)
		if err != nil {
			topicStats[topic] = map[string]interface{}{
				"status": "error",
				"error":  fmt.Sprintf("directory error: %v", err),
			}
			topicErrorCount++
			continue
		}

		// Get the latest version
		latestVersion, exists := fs.latestVersion[topic]
		if !exists {
			latestVersion = 0
		}

		topicStats[topic] = map[string]interface{}{
			"status": "up",
			"path":   topicPath,
			"latest": latestVersion,
		}
	}

	additionalInfo["topics"] = topicStats

	// If some topics have errors, mark as degraded
	if topicErrorCount > 0 && topicErrorCount < len(fs.topicConfigs) {
		status = outbound.StatusDegraded
		message = fmt.Sprintf("%d of %d topics have errors", topicErrorCount, len(fs.topicConfigs))
	} else if topicErrorCount == len(fs.topicConfigs) && len(fs.topicConfigs) > 0 {
		status = outbound.StatusDown
		message = "All topics have errors"
	}

	return outbound.HealthInfo{
		Status:         status,
		State:          fs.state,
		Message:        message,
		AdditionalInfo: additionalInfo,
	}, nil
}
