package fs

import (
	"context"
	"encoding/json"
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

// FileSystemEventRepository implements the EventRepository interface using file system storage
type FileSystemEventRepository struct {
	mu            sync.RWMutex
	basePath      string                          // Base path for storage
	latestVersion map[string]int64                // topic -> latest version (in-memory cache)
	topicConfigs  map[string]outbound.TopicConfig // In-memory cache of topic configurations
	logger        *log.Logger                     // Standard Go logger
}

// NewFileSystemEventRepository creates a new file system event repository with the specified base path
func NewFileSystemEventRepository(basePath string) *FileSystemEventRepository {
	return &FileSystemEventRepository{
		basePath:      basePath,
		latestVersion: make(map[string]int64),
		topicConfigs:  make(map[string]outbound.TopicConfig),
		logger:        log.New(os.Stderr, "[FS-REPO] ", log.LstdFlags),
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

	fs.logger.Printf("Initializing file system event repository at %s", fs.basePath)

	// Ensure base directory exists
	if err := fs.ensureBaseDir(); err != nil {
		fs.logger.Printf("Failed to ensure base directory exists: %v", err)
		return err
	}

	// Ensure configs directory exists
	configDir := filepath.Join(fs.basePath, "configs")
	if err := os.MkdirAll(configDir, 0755); err != nil {
		fs.logger.Printf("Failed to create configs directory: %v", err)
		return NewFileIOError(configDir, err)
	}

	// Ensure events directory exists
	eventsDir := filepath.Join(fs.basePath, "events")
	if err := os.MkdirAll(eventsDir, 0755); err != nil {
		fs.logger.Printf("Failed to create events directory: %v", err)
		return NewFileIOError(eventsDir, err)
	}

	// Load all topic configurations
	fs.latestVersion = make(map[string]int64)
	fs.topicConfigs = make(map[string]outbound.TopicConfig)

	entries, err := os.ReadDir(configDir)
	if err != nil {
		// If directory doesn't exist, just initialize empty maps
		if os.IsNotExist(err) {
			fs.logger.Printf("Config directory doesn't exist, initializing empty maps")
			return nil
		}
		fs.logger.Printf("Failed to read configs directory: %v", err)
		return NewFileIOError(configDir, err)
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
	return nil
}

// Close closes the file system event repository
func (fs *FileSystemEventRepository) Close() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	fs.logger.Printf("Closing file system event repository")
	// Nothing special to do for now, just clear maps
	fs.latestVersion = nil
	fs.topicConfigs = nil
	return nil
}

// AppendEvents appends events to a topic
func (fs *FileSystemEventRepository) AppendEvents(ctx context.Context, topic string, events []outbound.Event) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	fs.logger.Printf("Appending %d events to topic %s", len(events), topic)

	// Check if topic exists
	if _, exists := fs.topicConfigs[topic]; !exists {
		fs.logger.Printf("Topic %s not found", topic)
		return NewTopicNotFoundError(topic)
	}

	// Get current version
	currentVersion, exists := fs.latestVersion[topic]
	if !exists {
		// This should not happen as we check for topic existence above
		fs.logger.Printf("Topic %s exists but version not found, initializing to 0", topic)
		currentVersion = 0
	}

	// Ensure events directory exists
	topicDir := filepath.Join(fs.basePath, "events", topic)
	if err := os.MkdirAll(topicDir, 0755); err != nil {
		fs.logger.Printf("Failed to create topic events directory: %v", err)
		return NewFileIOError(topicDir, err)
	}

	// Process each event to set required fields and write to individual files
	for i := range events {
		// Set ID if not provided
		if events[i].ID == "" {
			events[i].ID = uuid.New().String()
		}

		// Set timestamp if not provided
		if events[i].Timestamp == 0 {
			events[i].Timestamp = time.Now().UnixNano()
		}

		// Set topic
		events[i].Topic = topic

		// Increment and assign version
		currentVersion++
		events[i].Version = currentVersion

		// Create filename with version (format: 00000000000000000001.json)
		// Using 20-digit zero-padded format to match tests
		eventPath := filepath.Join(topicDir, fmt.Sprintf("%020d.json", currentVersion))

		// Marshal individual event to JSON
		eventData, err := json.Marshal(events[i])
		if err != nil {
			fs.logger.Printf("Failed to marshal event: %v", err)
			return fmt.Errorf("failed to marshal event: %w", err)
		}

		// Write to file
		if err := os.WriteFile(eventPath, eventData, 0644); err != nil {
			fs.logger.Printf("Failed to write event to file: %v", err)
			return NewFileIOError(eventPath, err)
		}
	}

	// Update in-memory version
	fs.latestVersion[topic] = currentVersion
	fs.logger.Printf("Successfully appended events to topic %s, new version: %d", topic, currentVersion)

	return nil
}

// determineLatestVersion scans the topic directory to find the highest event version
func (fs *FileSystemEventRepository) determineLatestVersion(topic string) (int64, error) {
	topicDir := fs.getTopicDir(topic)
	fs.logger.Printf("[DEBUG] Topic %s: Determining latest version from directory: %s", topic, topicDir)

	// If directory doesn't exist yet, return 0 as the version
	if _, err := os.Stat(topicDir); os.IsNotExist(err) {
		fs.logger.Printf("[DEBUG] Topic %s: Topic directory doesn't exist, using version 0", topic)
		return 0, nil
	}

	entries, err := os.ReadDir(topicDir)
	if err != nil {
		fs.logger.Printf("[ERROR] Topic %s: Failed to read topic directory: %v", topic, err)
		return 0, NewFileIOError(topicDir, err)
	}

	var latestVersion int64 = 0
	var parseErrors []string
	var eventCount int

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}

		// Skip temporary files
		if strings.HasSuffix(entry.Name(), ".tmp") {
			fs.logger.Printf("[DEBUG] Topic %s: Skipping temporary file: %s", topic, entry.Name())
			continue
		}

		eventCount++

		// Extract version from filename (format: 00000000000000000001.json)
		baseName := strings.TrimSuffix(entry.Name(), ".json")
		version, err := strconv.ParseInt(baseName, 10, 64)
		if err != nil {
			// Try alternate format: events_1.json
			if strings.HasPrefix(baseName, "events_") {
				versionStr := strings.TrimPrefix(baseName, "events_")
				version, err = strconv.ParseInt(versionStr, 10, 64)
				if err != nil {
					// Log but skip files with invalid names
					parseErrors = append(parseErrors, fmt.Sprintf("%s: %v", entry.Name(), err))
					fs.logger.Printf("[WARN] Topic %s: Skipping file with invalid name format: %s", topic, entry.Name())
					continue
				}
			} else {
				// Log but skip files with invalid names
				parseErrors = append(parseErrors, fmt.Sprintf("%s: %v", entry.Name(), err))
				fs.logger.Printf("[WARN] Topic %s: Skipping file with invalid name format: %s", topic, entry.Name())
				continue
			}
		}

		if version > latestVersion {
			latestVersion = version
		}
	}

	// If we encountered parse errors but still found valid events, log the errors but don't fail
	if len(parseErrors) > 0 {
		fs.logger.Printf("[WARN] Topic %s: Failed to parse %d event filenames: %s",
			topic, len(parseErrors), strings.Join(parseErrors, "; "))
	}

	fs.logger.Printf("[INFO] Topic %s: Found %d event files, latest version is %d", topic, eventCount, latestVersion)
	return latestVersion, nil
}

// ensureDirStructure creates the base directory structure if it doesn't exist
func (fs *FileSystemEventRepository) ensureDirStructure() error {
	fs.logger.Printf("[DEBUG] Ensuring directory structure at %s", fs.basePath)

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

// ensureTopicDir ensures the topic directory exists
func (fs *FileSystemEventRepository) ensureTopicDir(topic string) error {
	topicDir := fs.getTopicDir(topic)
	fs.logger.Printf("[DEBUG] Ensuring topic directory exists: %s", topicDir)

	if err := os.MkdirAll(topicDir, 0755); err != nil {
		fs.logger.Printf("[ERROR] Failed to create topic directory: %v", err)
		return NewFileIOError(topicDir, err)
	}
	return nil
}

// getTopicsDir returns the path to the topics directory
func (fs *FileSystemEventRepository) getTopicsDir() string {
	// Use the events directory to store topics
	return fs.getEventsDir()
}

// getTopicDir returns the path to a specific topic directory
func (fs *FileSystemEventRepository) getTopicDir(topic string) string {
	return filepath.Join(fs.getTopicsDir(), topic)
}

// loadTopicConfig loads the configuration for a specific topic
func (fs *FileSystemEventRepository) loadTopicConfig(topic string) (outbound.TopicConfig, error) {
	configPath := fs.getTopicConfigPath(topic)
	fs.logger.Printf("[DEBUG] Loading topic config from %s", configPath)

	data, err := os.ReadFile(configPath)
	if err != nil {
		fs.logger.Printf("[ERROR] Failed to read topic configuration: %v", err)
		return outbound.TopicConfig{}, NewFileIOError(configPath, err)
	}

	var config outbound.TopicConfig
	if err := json.Unmarshal(data, &config); err != nil {
		fs.logger.Printf("[ERROR] Failed to unmarshal topic configuration: %v", err)
		return outbound.TopicConfig{}, fmt.Errorf("failed to unmarshal topic configuration: %w", err)
	}

	fs.logger.Printf("[DEBUG] Successfully loaded topic configuration for %s", topic)
	return config, nil
}

// getTopicConfigPath returns the path to the configuration file for a specific topic
func (fs *FileSystemEventRepository) getTopicConfigPath(topic string) string {
	// Check in the dedicated configs directory
	return filepath.Join(fs.getConfigDir(), topic+".json")
}

// CreateTopic creates a new topic
func (fs *FileSystemEventRepository) CreateTopic(ctx context.Context, config outbound.TopicConfig) error {
	fs.logger.Printf("[DEBUG] Creating topic: %s", config.Name)
	fs.mu.Lock()
	defer fs.mu.Unlock()

	name := config.Name

	// Verify topic doesn't already exist
	if _, exists := fs.topicConfigs[name]; exists {
		fs.logger.Printf("[ERROR] Topic %s already exists", name)
		return NewTopicAlreadyExistsError(name)
	}

	// Ensure base directories exist
	if err := fs.ensureDirStructure(); err != nil {
		fs.logger.Printf("[ERROR] Failed to ensure directory structure: %v", err)
		return err
	}

	// Ensure topic directory exists
	topicDir := fs.getTopicDir(name)
	if err := os.MkdirAll(topicDir, 0755); err != nil {
		fs.logger.Printf("[ERROR] Failed to create topic directory: %v", err)
		return NewFileIOError(topicDir, err)
	}

	// Write topic configuration to configs directory
	configPath := filepath.Join(fs.getConfigDir(), name+".json")
	configData, err := json.Marshal(config)
	if err != nil {
		fs.logger.Printf("[ERROR] Failed to marshal topic configuration: %v", err)
		return fmt.Errorf("failed to marshal topic configuration: %w", err)
	}

	if err := os.WriteFile(configPath, configData, 0644); err != nil {
		fs.logger.Printf("[ERROR] Failed to write topic configuration: %v", err)
		return NewFileIOError(configPath, err)
	}

	// Store in memory
	fs.topicConfigs[name] = config
	fs.latestVersion[name] = 0 // Initialize version to 0

	fs.logger.Printf("[INFO] Topic %s created successfully", name)
	return nil
}

// DeleteTopic deletes a topic and all its events
func (fs *FileSystemEventRepository) DeleteTopic(ctx context.Context, topic string) error {
	fs.logger.Printf("[DEBUG] Deleting topic: %s", topic)
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Verify topic exists
	if _, exists := fs.topicConfigs[topic]; !exists {
		fs.logger.Printf("[ERROR] Topic %s not found", topic)
		return NewTopicNotFoundError(topic)
	}

	// Delete topic directory with all event files
	topicDir := fs.getTopicDir(topic)
	if _, err := os.Stat(topicDir); !os.IsNotExist(err) {
		if err := os.RemoveAll(topicDir); err != nil {
			fs.logger.Printf("[ERROR] Failed to remove topic directory: %v", err)
			return NewFileIOError(topicDir, err)
		}
	}

	// Delete topic configuration file
	configPath := filepath.Join(fs.getConfigDir(), topic+".json")
	if _, err := os.Stat(configPath); !os.IsNotExist(err) {
		if err := os.Remove(configPath); err != nil {
			fs.logger.Printf("[ERROR] Failed to remove topic configuration file: %v", err)
			return NewFileIOError(configPath, err)
		}
	}

	// Remove from in-memory maps
	delete(fs.topicConfigs, topic)
	delete(fs.latestVersion, topic)

	fs.logger.Printf("[INFO] Topic %s deleted successfully", topic)
	return nil
}

// ListTopics returns a list of all topics
func (fs *FileSystemEventRepository) ListTopics(ctx context.Context) ([]outbound.TopicConfig, error) {
	fs.logger.Printf("[DEBUG] Listing all topics")
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	topics := make([]outbound.TopicConfig, 0, len(fs.topicConfigs))
	for _, config := range fs.topicConfigs {
		topics = append(topics, config)
	}

	fs.logger.Printf("[INFO] Found %d topics", len(topics))
	return topics, nil
}

// TopicExists checks if a topic exists
func (fs *FileSystemEventRepository) TopicExists(ctx context.Context, topic string) (bool, error) {
	fs.logger.Printf("[DEBUG] Checking if topic exists: %s", topic)
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	_, exists := fs.topicConfigs[topic]
	fs.logger.Printf("[DEBUG] Topic %s exists: %v", topic, exists)
	return exists, nil
}

// GetLatestVersion returns the latest version of a topic
func (fs *FileSystemEventRepository) GetLatestVersion(ctx context.Context, topic string) (int64, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	// Check if topic exists
	if _, exists := fs.topicConfigs[topic]; !exists {
		fs.logger.Printf("[ERROR] Topic %s not found", topic)
		return 0, NewTopicNotFoundError(topic)
	}

	// Get the latest version from memory
	latestVersion, exists := fs.latestVersion[topic]
	if !exists {
		// If not found in memory (should not happen), determine it from files
		fs.logger.Printf("[WARN] Version for topic %s not in memory, determining from files", topic)
		var err error
		latestVersion, err = fs.determineLatestVersion(topic)
		if err != nil {
			fs.logger.Printf("[ERROR] Failed to determine latest version: %v", err)
			return 0, err
		}
	}

	fs.logger.Printf("[DEBUG] Latest version for topic %s: %d", topic, latestVersion)
	return latestVersion, nil
}

// UpdateTopicConfig updates a topic's configuration
func (fs *FileSystemEventRepository) UpdateTopicConfig(ctx context.Context, config outbound.TopicConfig) error {
	fs.logger.Printf("[DEBUG] Updating topic configuration for: %s", config.Name)
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Verify topic exists
	if _, exists := fs.topicConfigs[config.Name]; !exists {
		fs.logger.Printf("[ERROR] Topic %s not found", config.Name)
		return NewTopicNotFoundError(config.Name)
	}

	// Marshal updated configuration
	data, err := json.Marshal(config)
	if err != nil {
		fs.logger.Printf("[ERROR] Failed to marshal topic config: %v", err)
		return fmt.Errorf("failed to marshal topic config: %w", err)
	}

	// Write to temporary file first
	configPath := filepath.Join(fs.getConfigDir(), config.Name+".json")
	tempPath := configPath + ".tmp"

	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		fs.logger.Printf("[ERROR] Failed to write temporary topic config file: %v", err)
		return NewFileIOError(tempPath, err)
	}

	// Rename for atomic update
	if err := os.Rename(tempPath, configPath); err != nil {
		// Try to clean up temp file
		os.Remove(tempPath)
		fs.logger.Printf("[ERROR] Failed to update topic config file: %v", err)
		return NewFileIOError(configPath, err)
	}

	// Update in-memory state
	fs.topicConfigs[config.Name] = config
	fs.logger.Printf("[INFO] Topic %s configuration updated successfully", config.Name)

	return nil
}

// GetTopicConfig gets a topic's configuration
func (fs *FileSystemEventRepository) GetTopicConfig(ctx context.Context, topic string) (outbound.TopicConfig, error) {
	fs.logger.Printf("[DEBUG] Getting configuration for topic: %s", topic)
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	// Verify topic exists
	config, exists := fs.topicConfigs[topic]
	if !exists {
		fs.logger.Printf("[ERROR] Topic %s not found", topic)
		return outbound.TopicConfig{}, NewTopicNotFoundError(topic)
	}

	fs.logger.Printf("[DEBUG] Successfully retrieved configuration for topic: %s", topic)
	return config, nil
}

// GetEvents retrieves events for a topic, starting from versions strictly greater than fromVersion
func (fs *FileSystemEventRepository) GetEvents(ctx context.Context, topic string, fromVersion int64) ([]outbound.Event, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	fs.logger.Printf("[DEBUG] Topic %s: Getting events with version > %d", topic, fromVersion)

	// Verify topic exists
	if _, exists := fs.topicConfigs[topic]; !exists {
		fs.logger.Printf("[ERROR] Topic %s not found", topic)
		return nil, NewTopicNotFoundError(topic)
	}

	topicDir := fs.getTopicDir(topic)

	// Check if topic directory exists
	if _, err := os.Stat(topicDir); os.IsNotExist(err) {
		// If the directory doesn't exist, return empty slice
		fs.logger.Printf("[WARN] Topic %s: Topic directory doesn't exist", topic)
		return []outbound.Event{}, nil
	}

	// Create a pattern for matching files with versions > fromVersion
	// Generate the filename pattern based on fromVersion
	var filePattern string

	// For pattern matching, we'll use the next version after fromVersion
	// since fromVersion itself should be excluded
	nextVersion := fromVersion + 1

	if nextVersion > 1 {
		// Create a pattern that matches all files with version >= nextVersion
		// The pattern needs to account for 20-digit padding
		paddedVersion := fmt.Sprintf("%020d", nextVersion)

		// All files with same first digits but last digit >= the nextVersion's digit
		filePattern = fmt.Sprintf("%s*.json", paddedVersion[:len(paddedVersion)-1])
	} else {
		// If nextVersion is 0 or 1, match all JSON files (since all versions start at 1)
		filePattern = "*.json"
	}

	fs.logger.Printf("[DEBUG] Topic %s: Using file pattern: %s", topic, filePattern)

	// Use filepath.Glob to find matching files
	matchingPaths, err := filepath.Glob(filepath.Join(topicDir, filePattern))
	if err != nil {
		fs.logger.Printf("[ERROR] Topic %s: Failed to find matching event files: %v", topic, err)
		return nil, NewFileIOError(topicDir, err)
	}

	fs.logger.Printf("[DEBUG] Topic %s: Found %d potentially matching files", topic, len(matchingPaths))

	var events []outbound.Event

	// Process each matching file
	for _, path := range matchingPaths {
		// Get just the filename
		filename := filepath.Base(path)

		// Skip temporary files
		if strings.HasSuffix(filename, ".tmp") {
			fs.logger.Printf("[DEBUG] Topic %s: Skipping temporary file: %s", topic, filename)
			continue
		}

		// Extract version from filename (format: 00000000000000000001.json)
		baseName := strings.TrimSuffix(filename, ".json")
		version, err := strconv.ParseInt(baseName, 10, 64)
		if err != nil {
			// Try alternate format: events_1.json
			if strings.HasPrefix(baseName, "events_") {
				versionStr := strings.TrimPrefix(baseName, "events_")
				version, err = strconv.ParseInt(versionStr, 10, 64)
				if err != nil {
					// Log but skip files with invalid names
					fs.logger.Printf("[WARN] Topic %s: Skipping file with invalid name format: %s", topic, filename)
					continue
				}
			} else {
				// Log but skip files with invalid names
				fs.logger.Printf("[WARN] Topic %s: Skipping file with invalid name format: %s", topic, filename)
				continue
			}
		}

		// Ensure version is > fromVersion (not just >=)
		if version <= fromVersion {
			fs.logger.Printf("[DEBUG] Topic %s: Skipping event with version %d (not > %d)", topic, version, fromVersion)
			continue
		}

		// Read and unmarshal event
		data, err := os.ReadFile(path)
		if err != nil {
			fs.logger.Printf("[WARN] Topic %s: Failed to read event file %s: %v", topic, path, err)
			return nil, NewFileIOError(path, err)
		}

		// Check if this is an array of events or a single event
		var fileContent = strings.TrimSpace(string(data))
		if strings.HasPrefix(fileContent, "[") {
			// This is an array of events
			var eventArray []outbound.Event
			if err := json.Unmarshal(data, &eventArray); err != nil {
				fs.logger.Printf("[WARN] Topic %s: Failed to parse event array in file %s: %v", topic, path, err)
				return nil, fmt.Errorf("%w: %v", ErrInvalidEventData, err)
			}
			events = append(events, eventArray...)
			fs.logger.Printf("[DEBUG] Topic %s: Added %d events from file %s", topic, len(eventArray), filename)
		} else {
			// This is a single event
			var event outbound.Event
			if err := json.Unmarshal(data, &event); err != nil {
				fs.logger.Printf("[WARN] Topic %s: Failed to parse event file %s: %v", topic, path, err)
				return nil, fmt.Errorf("%w: %v", ErrInvalidEventData, err)
			}
			events = append(events, event)
			fs.logger.Printf("[DEBUG] Topic %s: Added event with version %d", topic, event.Version)
		}
	}

	// Sort events by version to ensure consistent ordering
	sort.Slice(events, func(i, j int) bool {
		return events[i].Version < events[j].Version
	})

	fs.logger.Printf("[INFO] Topic %s: Retrieved %d events with version > %d", topic, len(events), fromVersion)
	return events, nil
}

// GetEventsByType retrieves events of a specific type for a topic, starting from versions strictly greater than fromVersion
func (fs *FileSystemEventRepository) GetEventsByType(ctx context.Context, topic string, eventType string, fromVersion int64) ([]outbound.Event, error) {
	fs.logger.Printf("[DEBUG] Topic %s: Getting events of type %s with version > %d", topic, eventType, fromVersion)

	// Get all events (with version > fromVersion)
	allEvents, err := fs.GetEvents(ctx, topic, fromVersion)
	if err != nil {
		return nil, err
	}

	if len(allEvents) == 0 {
		fs.logger.Printf("[DEBUG] Topic %s: No events found with version > %d", topic, fromVersion)
		return allEvents, nil
	}

	// Filter events by type
	var filteredEvents []outbound.Event
	for _, event := range allEvents {
		if event.Type == eventType {
			filteredEvents = append(filteredEvents, event)
		}
	}

	fs.logger.Printf("[INFO] Topic %s: Found %d events of type %s with version > %d",
		topic, len(filteredEvents), eventType, fromVersion)
	return filteredEvents, nil
}

// ensureBaseDir creates the base directory if it doesn't exist
func (fs *FileSystemEventRepository) ensureBaseDir() error {
	fs.logger.Printf("[DEBUG] Ensuring base directory exists: %s", fs.basePath)
	if err := os.MkdirAll(fs.basePath, 0755); err != nil {
		fs.logger.Printf("[ERROR] Failed to create base directory: %v", err)
		return NewFileIOError(fs.basePath, err)
	}
	return nil
}

// ensureConfigsDir creates the configs directory if it doesn't exist
func (fs *FileSystemEventRepository) ensureConfigsDir() error {
	configDir := fs.getConfigDir()
	fs.logger.Printf("[DEBUG] Ensuring configs directory exists: %s", configDir)
	if err := os.MkdirAll(configDir, 0755); err != nil {
		fs.logger.Printf("[ERROR] Failed to create configs directory: %v", err)
		return NewFileIOError(configDir, err)
	}
	return nil
}

// ensureEventsDir creates the events directory if it doesn't exist
func (fs *FileSystemEventRepository) ensureEventsDir() error {
	eventsDir := fs.getEventsDir()
	fs.logger.Printf("[DEBUG] Ensuring events directory exists: %s", eventsDir)
	if err := os.MkdirAll(eventsDir, 0755); err != nil {
		fs.logger.Printf("[ERROR] Failed to create events directory: %v", err)
		return NewFileIOError(eventsDir, err)
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
