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

// Custom error types
type FileIOError struct {
	Path string
	Err  error
}

func (e *FileIOError) Error() string {
	return fmt.Sprintf("file I/O error at %s: %v", e.Path, e.Err)
}

func (e *FileIOError) Unwrap() error {
	return e.Err
}

type TopicAlreadyExistsError struct {
	Topic string
}

func (e *TopicAlreadyExistsError) Error() string {
	return fmt.Sprintf("topic already exists: %s", e.Topic)
}

type TopicNotFoundError struct {
	Topic string
}

func (e *TopicNotFoundError) Error() string {
	return fmt.Sprintf("topic not found: %s", e.Topic)
}

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

// Initialize prepares the repository for use
func (fs *FileSystemEventRepository) Initialize(ctx context.Context) error {
	fs.logger.Printf("[INFO] Initializing FileSystemEventRepository at %s", fs.basePath)
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Ensure directory structure exists
	if err := fs.ensureDirStructure(); err != nil {
		fs.logger.Printf("[ERROR] Failed to ensure directory structure: %v", err)
		return err
	}

	// Load existing topic configurations from configs directory
	configDir := fs.getConfigDir()
	configEntries, err := os.ReadDir(configDir)
	if err != nil && !os.IsNotExist(err) {
		fs.logger.Printf("[ERROR] Failed to read configs directory: %v", err)
		return NewFileIOError(configDir, err)
	}

	if configEntries != nil {
		fs.logger.Printf("[DEBUG] Loading existing topic configurations from %s", configDir)
		// Process each topic configuration file
		for _, entry := range configEntries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
				continue
			}

			configPath := filepath.Join(configDir, entry.Name())
			data, err := os.ReadFile(configPath)
			if err != nil {
				fs.logger.Printf("[WARN] Failed to read config file %s: %v", configPath, err)
				continue
			}

			var config outbound.TopicConfig
			if err := json.Unmarshal(data, &config); err != nil {
				fs.logger.Printf("[WARN] Failed to parse config file %s: %v", configPath, err)
				continue
			}

			// Store in memory
			fs.topicConfigs[config.Name] = config
		}
	}

	// Load existing topics from events directory
	eventsDir := fs.getEventsDir()
	topicEntries, err := os.ReadDir(eventsDir)
	if err != nil && !os.IsNotExist(err) {
		fs.logger.Printf("[ERROR] Failed to read events directory: %v", err)
		return NewFileIOError(eventsDir, err)
	}

	if topicEntries != nil {
		fs.logger.Printf("[DEBUG] Loading existing topics from %s", eventsDir)
		// For each topic directory, load configuration and determine latest version
		for _, entry := range topicEntries {
			if !entry.IsDir() {
				continue
			}

			topicName := entry.Name()
			fs.logger.Printf("[DEBUG] Loading topic: %s", topicName)

			// Check if we already loaded this topic's config
			if _, exists := fs.topicConfigs[topicName]; !exists {
				// Try to load topic configuration from topic directory
				config, err := fs.loadTopicConfig(topicName)
				if err != nil {
					fs.logger.Printf("[WARN] Failed to load configuration for topic %s: %v", topicName, err)
					continue
				}
				fs.topicConfigs[topicName] = config
			}

			// Determine latest version
			latestVersion, err := fs.determineLatestVersion(topicName)
			if err != nil {
				fs.logger.Printf("[WARN] Failed to determine latest version for topic %s: %v", topicName, err)
				continue
			}
			fs.latestVersion[topicName] = latestVersion

			fs.logger.Printf("[INFO] Loaded topic %s with latest version %d", topicName, latestVersion)
		}
	}

	fs.logger.Printf("[INFO] FileSystemEventRepository initialized successfully. Loaded %d topics", len(fs.topicConfigs))
	return nil
}

// Close cleans up resources used by the repository
func (fs *FileSystemEventRepository) Close() error {
	fs.logger.Printf("[INFO] Closing FileSystemEventRepository")

	// Acquire the lock before modifying in-memory state
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Clear the in-memory maps
	fs.topicConfigs = make(map[string]outbound.TopicConfig)
	fs.latestVersion = make(map[string]int64)

	return nil
}

// AppendEvents adds new events to the store for a specific topic
func (fs *FileSystemEventRepository) AppendEvents(ctx context.Context, topic string, events []outbound.Event) error {
	if len(events) == 0 {
		fs.logger.Printf("[DEBUG] Topic %s: No events to append", topic)
		return nil // Nothing to do
	}

	fs.logger.Printf("[DEBUG] Topic %s: Appending %d events", topic, len(events))
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Verify topic exists
	if _, exists := fs.topicConfigs[topic]; !exists {
		fs.logger.Printf("[ERROR] Topic %s not found", topic)
		return NewTopicNotFoundError(topic)
	}

	// Get current latest version
	latestVersion := fs.latestVersion[topic]
	fs.logger.Printf("[DEBUG] Topic %s: Current latest version: %d", topic, latestVersion)

	// Ensure topic directory exists
	topicDir := fs.getTopicDir(topic)
	if err := fs.ensureTopicDir(topic); err != nil {
		fs.logger.Printf("[ERROR] Topic %s: Failed to ensure topic directory: %v", topic, err)
		return err
	}

	// Process each event
	for i := range events {
		// Set ID if not provided
		if events[i].ID == "" {
			events[i].ID = uuid.New().String()
		}

		// Set timestamp if not provided
		if events[i].Timestamp == 0 {
			events[i].Timestamp = time.Now().UnixNano()
		}

		// Increment and assign version
		latestVersion++
		events[i].Version = latestVersion
		events[i].Topic = topic

		// Marshal event to JSON
		data, err := json.Marshal(events[i])
		if err != nil {
			fs.logger.Printf("[ERROR] Topic %s: Failed to marshal event: %v", topic, err)
			return fmt.Errorf("%w: %v", ErrInvalidEventData, err)
		}

		// Create filename with padded version number (20 digits)
		filename := fmt.Sprintf("%020d.json", events[i].Version)
		eventPath := filepath.Join(topicDir, filename)
		fs.logger.Printf("[DEBUG] Topic %s: Writing event to %s", topic, eventPath)

		// Write event to temporary file first for atomic operation
		tempFile := eventPath + ".tmp"
		if err := os.WriteFile(tempFile, data, 0644); err != nil {
			fs.logger.Printf("[ERROR] Topic %s: Failed to write event file: %v", topic, err)
			return NewFileIOError(eventPath, err)
		}

		// Rename temporary file to final name (atomic)
		if err := os.Rename(tempFile, eventPath); err != nil {
			// Attempt to clean up temporary file
			os.Remove(tempFile)
			fs.logger.Printf("[ERROR] Topic %s: Failed to finalize event file: %v", topic, err)
			return NewFileIOError(eventPath, err)
		}

		fs.logger.Printf("[DEBUG] Topic %s: Successfully wrote event with version %d", topic, events[i].Version)
	}

	// Update latest version
	fs.latestVersion[topic] = latestVersion
	fs.logger.Printf("[INFO] Topic %s: Updated latest version to %d", topic, latestVersion)

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
			// Log the error but continue processing other files
			parseErrors = append(parseErrors, fmt.Sprintf("%s: %v", entry.Name(), err))
			fs.logger.Printf("[WARN] Topic %s: Failed to parse event filename %s: %v", topic, entry.Name(), err)
			continue
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
	// First check in the dedicated configs directory
	configInConfigsDir := filepath.Join(fs.getConfigDir(), topic+".json")
	if _, err := os.Stat(configInConfigsDir); err == nil {
		return configInConfigsDir
	}

	// Fall back to topic directory
	return filepath.Join(fs.getTopicDir(topic), topic+".json")
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
			// Log but skip files with invalid names
			fs.logger.Printf("[WARN] Topic %s: Skipping file with invalid name format: %s", topic, filename)
			continue
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

		var event outbound.Event
		if err := json.Unmarshal(data, &event); err != nil {
			fs.logger.Printf("[WARN] Topic %s: Failed to parse event file %s: %v", topic, path, err)
			return nil, fmt.Errorf("%w: %v", ErrInvalidEventData, err)
		}

		events = append(events, event)
		fs.logger.Printf("[DEBUG] Topic %s: Added event with version %d", topic, event.Version)
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
