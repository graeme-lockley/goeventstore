# File System Storage Adaptor Specification

## Overview

The File System Storage Adaptor implements the `EventRepository` interface using the local file system for persistence. It is designed for:

1. Development and testing in environments requiring persistence
2. Small to medium-scale applications with moderate durability requirements
3. Scenarios where database dependencies need to be avoided
4. Environments with limited infrastructure complexity

This adaptor stores events as JSON files organized in a directory structure, providing persistence across application restarts without requiring external database systems.

## Implementation Requirements

### Data Structures

The adaptor should utilize the following core data structures:

```go
type FileSystemEventRepository struct {
    mu            sync.RWMutex
    basePath      string
    latestVersion map[string]int64 // topic -> latest version (in-memory cache)
    topicConfigs  map[string]TopicConfig
}
```

### Directory Structure

The storage layout should be organized as follows:

```
/base-path/
├── configs/                   # Topic configuration storage
│   ├── topic1.json
│   └── topic2.json
└── events/                    # Event storage by topic
    ├── topic1/
    │   ├── 00000000000000000001.json
    │   ├── 00000000000000000002.json
    │   └── ...
    └── topic2/
        ├── 00000000000000000001.json
        └── ...
```

### Thread Safety

The implementation must handle concurrent access from multiple goroutines:

1. Use a read-write mutex (`sync.RWMutex`) for in-memory data structures
2. Implement file locking mechanisms for concurrent file access
3. Handle potential race conditions when creating/updating files

### File Operations

The adaptor should implement robust file handling:

1. Atomic file writes using temporary files and renaming
2. Error handling for all file system operations
3. Proper file permissions and directory creation
4. Handling of potential disk space issues

## Functional Specifications

### Initialization

```go
// NewFileSystemEventRepository creates a new instance of the file system repository
func NewFileSystemEventRepository(basePath string) *FileSystemEventRepository {
    return &FileSystemEventRepository{
        basePath:      basePath,
        latestVersion: make(map[string]int64),
        topicConfigs:  make(map[string]TopicConfig),
    }
}

// Initialize prepares the repository for use
func (fs *FileSystemEventRepository) Initialize(ctx context.Context) error {
    fs.mu.Lock()
    defer fs.mu.Unlock()
    
    // Create base directory if it doesn't exist
    if err := os.MkdirAll(fs.basePath, 0755); err != nil {
        return fmt.Errorf("failed to create base directory: %w", err)
    }
    
    // Create config directory
    configDir := filepath.Join(fs.basePath, "configs")
    if err := os.MkdirAll(configDir, 0755); err != nil {
        return fmt.Errorf("failed to create config directory: %w", err)
    }
    
    // Create events directory
    eventsDir := filepath.Join(fs.basePath, "events")
    if err := os.MkdirAll(eventsDir, 0755); err != nil {
        return fmt.Errorf("failed to create events directory: %w", err)
    }
    
    // Load existing topic configurations
    entries, err := os.ReadDir(configDir)
    if err != nil {
        return fmt.Errorf("failed to read config directory: %w", err)
    }
    
    // Process each topic configuration file
    for _, entry := range entries {
        if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
            continue
        }
        
        configPath := filepath.Join(configDir, entry.Name())
        data, err := os.ReadFile(configPath)
        if err != nil {
            return fmt.Errorf("failed to read config file %s: %w", configPath, err)
        }
        
        var config TopicConfig
        if err := json.Unmarshal(data, &config); err != nil {
            return fmt.Errorf("failed to parse config file %s: %w", configPath, err)
        }
        
        // Store in memory
        fs.topicConfigs[config.Name] = config
        
        // Determine latest version
        topicDir := filepath.Join(fs.basePath, "events", config.Name)
        if _, err := os.Stat(topicDir); os.IsNotExist(err) {
            fs.latestVersion[config.Name] = 0
            continue
        }
        
        latestVersion, err := fs.determineLatestVersion(config.Name)
        if err != nil {
            return fmt.Errorf("failed to determine latest version for topic %s: %w", config.Name, err)
        }
        
        fs.latestVersion[config.Name] = latestVersion
    }
    
    return nil
}

// determineLatestVersion scans the topic directory to find the highest event version
func (fs *FileSystemEventRepository) determineLatestVersion(topic string) (int64, error) {
    topicDir := filepath.Join(fs.basePath, "events", topic)
    
    entries, err := os.ReadDir(topicDir)
    if err != nil {
        return 0, err
    }
    
    var latestVersion int64 = 0
    
    for _, entry := range entries {
        if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
            continue
        }
        
        // Extract version from filename (format: 00000000000000000001.json)
        baseName := strings.TrimSuffix(entry.Name(), ".json")
        version, err := strconv.ParseInt(baseName, 10, 64)
        if err != nil {
            continue
        }
        
        if version > latestVersion {
            latestVersion = version
        }
    }
    
    return latestVersion, nil
}
```

### Event Operations

#### AppendEvents

```go
// AppendEvents adds new events to the store for a specific topic
func (fs *FileSystemEventRepository) AppendEvents(ctx context.Context, topic string, events []Event) error {
    fs.mu.Lock()
    defer fs.mu.Unlock()
    
    // Verify topic exists
    if _, exists := fs.topicConfigs[topic]; !exists {
        return errors.New("topic not found")
    }
    
    // Get current latest version
    latestVersion := fs.latestVersion[topic]
    
    // Ensure topic directory exists
    topicDir := filepath.Join(fs.basePath, "events", topic)
    if err := os.MkdirAll(topicDir, 0755); err != nil {
        return fmt.Errorf("failed to create topic directory: %w", err)
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
            return fmt.Errorf("failed to marshal event: %w", err)
        }
        
        // Create filename with padded version number
        filename := fmt.Sprintf("%020d.json", events[i].Version)
        eventPath := filepath.Join(topicDir, filename)
        
        // Write event to temporary file first for atomic operation
        tempFile := eventPath + ".tmp"
        if err := os.WriteFile(tempFile, data, 0644); err != nil {
            return fmt.Errorf("failed to write event file: %w", err)
        }
        
        // Rename temporary file to final name (atomic)
        if err := os.Rename(tempFile, eventPath); err != nil {
            // Attempt to clean up temporary file
            os.Remove(tempFile)
            return fmt.Errorf("failed to finalize event file: %w", err)
        }
    }
    
    // Update latest version
    fs.latestVersion[topic] = latestVersion
    
    return nil
}
```

#### GetEvents

```go
// GetEvents retrieves events for a topic, starting from a specific version
func (fs *FileSystemEventRepository) GetEvents(ctx context.Context, topic string, fromVersion int64) ([]Event, error) {
    fs.mu.RLock()
    
    // Verify topic exists
    if _, exists := fs.topicConfigs[topic]; !exists {
        fs.mu.RUnlock()
        return nil, errors.New("topic not found")
    }
    
    topicDir := filepath.Join(fs.basePath, "events", topic)
    
    // Check if directory exists
    if _, err := os.Stat(topicDir); os.IsNotExist(err) {
        fs.mu.RUnlock()
        return []Event{}, nil
    }
    
    fs.mu.RUnlock()
    
    // Read directory entries
    entries, err := os.ReadDir(topicDir)
    if err != nil {
        return nil, fmt.Errorf("failed to read topic directory: %w", err)
    }
    
    var events []Event
    
    // Process each event file
    for _, entry := range entries {
        if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
            continue
        }
        
        // Extract version from filename
        baseName := strings.TrimSuffix(entry.Name(), ".json")
        version, err := strconv.ParseInt(baseName, 10, 64)
        if err != nil {
            continue
        }
        
        // Skip events before fromVersion
        if version < fromVersion {
            continue
        }
        
        // Read and unmarshal event
        eventPath := filepath.Join(topicDir, entry.Name())
        data, err := os.ReadFile(eventPath)
        if err != nil {
            return nil, fmt.Errorf("failed to read event file %s: %w", eventPath, err)
        }
        
        var event Event
        if err := json.Unmarshal(data, &event); err != nil {
            return nil, fmt.Errorf("failed to parse event file %s: %w", eventPath, err)
        }
        
        events = append(events, event)
    }
    
    // Sort events by version
    sort.Slice(events, func(i, j int) bool {
        return events[i].Version < events[j].Version
    })
    
    return events, nil
}
```

#### GetEventsByType

```go
// GetEventsByType retrieves events of a specific type for a topic
func (fs *FileSystemEventRepository) GetEventsByType(ctx context.Context, topic string, eventType string, fromVersion int64) ([]Event, error) {
    // Get all events first
    allEvents, err := fs.GetEvents(ctx, topic, fromVersion)
    if err != nil {
        return nil, err
    }
    
    // Filter by type
    var filteredEvents []Event
    for _, event := range allEvents {
        if event.Type == eventType {
            filteredEvents = append(filteredEvents, event)
        }
    }
    
    return filteredEvents, nil
}
```

#### GetLatestVersion

```go
// GetLatestVersion returns the latest event version for a topic
func (fs *FileSystemEventRepository) GetLatestVersion(ctx context.Context, topic string) (int64, error) {
    fs.mu.RLock()
    defer fs.mu.RUnlock()
    
    // Verify topic exists
    if _, exists := fs.topicConfigs[topic]; !exists {
        return 0, errors.New("topic not found")
    }
    
    return fs.latestVersion[topic], nil
}
```

### Topic Management

#### CreateTopic

```go
// CreateTopic creates a new topic
func (fs *FileSystemEventRepository) CreateTopic(ctx context.Context, config TopicConfig) error {
    fs.mu.Lock()
    defer fs.mu.Unlock()
    
    // Verify topic doesn't already exist
    if _, exists := fs.topicConfigs[config.Name]; exists {
        return errors.New("topic already exists")
    }
    
    // Create topic directory
    topicDir := filepath.Join(fs.basePath, "events", config.Name)
    if err := os.MkdirAll(topicDir, 0755); err != nil {
        return fmt.Errorf("failed to create topic directory: %w", err)
    }
    
    // Marshal topic configuration
    data, err := json.Marshal(config)
    if err != nil {
        return fmt.Errorf("failed to marshal topic config: %w", err)
    }
    
    // Write topic configuration
    configPath := filepath.Join(fs.basePath, "configs", config.Name+".json")
    if err := os.WriteFile(configPath, data, 0644); err != nil {
        return fmt.Errorf("failed to write topic config: %w", err)
    }
    
    // Update in-memory state
    fs.topicConfigs[config.Name] = config
    fs.latestVersion[config.Name] = 0
    
    return nil
}
```

#### DeleteTopic

```go
// DeleteTopic deletes a topic
func (fs *FileSystemEventRepository) DeleteTopic(ctx context.Context, topic string) error {
    fs.mu.Lock()
    defer fs.mu.Unlock()
    
    // Verify topic exists
    if _, exists := fs.topicConfigs[topic]; !exists {
        return errors.New("topic not found")
    }
    
    // Remove topic directory
    topicDir := filepath.Join(fs.basePath, "events", topic)
    if err := os.RemoveAll(topicDir); err != nil {
        return fmt.Errorf("failed to remove topic directory: %w", err)
    }
    
    // Remove topic configuration
    configPath := filepath.Join(fs.basePath, "configs", topic+".json")
    if err := os.Remove(configPath); err != nil {
        return fmt.Errorf("failed to remove topic config: %w", err)
    }
    
    // Update in-memory state
    delete(fs.topicConfigs, topic)
    delete(fs.latestVersion, topic)
    
    return nil
}
```

#### ListTopics

```go
// ListTopics returns a list of all topics
func (fs *FileSystemEventRepository) ListTopics(ctx context.Context) ([]TopicConfig, error) {
    fs.mu.RLock()
    defer fs.mu.RUnlock()
    
    topics := make([]TopicConfig, 0, len(fs.topicConfigs))
    for _, config := range fs.topicConfigs {
        topics = append(topics, config)
    }
    
    return topics, nil
}
```

#### TopicExists

```go
// TopicExists checks if a topic exists
func (fs *FileSystemEventRepository) TopicExists(ctx context.Context, topic string) (bool, error) {
    fs.mu.RLock()
    defer fs.mu.RUnlock()
    
    _, exists := fs.topicConfigs[topic]
    return exists, nil
}
```

#### UpdateTopicConfig

```go
// UpdateTopicConfig updates a topic's configuration
func (fs *FileSystemEventRepository) UpdateTopicConfig(ctx context.Context, config TopicConfig) error {
    fs.mu.Lock()
    defer fs.mu.Unlock()
    
    // Verify topic exists
    if _, exists := fs.topicConfigs[config.Name]; !exists {
        return errors.New("topic not found")
    }
    
    // Marshal updated configuration
    data, err := json.Marshal(config)
    if err != nil {
        return fmt.Errorf("failed to marshal topic config: %w", err)
    }
    
    // Write to temporary file first
    configPath := filepath.Join(fs.basePath, "configs", config.Name+".json")
    tempPath := configPath + ".tmp"
    
    if err := os.WriteFile(tempPath, data, 0644); err != nil {
        return fmt.Errorf("failed to write topic config: %w", err)
    }
    
    // Rename for atomic update
    if err := os.Rename(tempPath, configPath); err != nil {
        os.Remove(tempPath) // Clean up temp file
        return fmt.Errorf("failed to update topic config: %w", err)
    }
    
    // Update in-memory state
    fs.topicConfigs[config.Name] = config
    
    return nil
}
```

#### GetTopicConfig

```go
// GetTopicConfig gets a topic's configuration
func (fs *FileSystemEventRepository) GetTopicConfig(ctx context.Context, topic string) (TopicConfig, error) {
    fs.mu.RLock()
    defer fs.mu.RUnlock()
    
    // Verify topic exists
    config, exists := fs.topicConfigs[topic]
    if !exists {
        return TopicConfig{}, errors.New("topic not found")
    }
    
    return config, nil
}
```

### Lifecycle Operations

```go
// Close cleans up resources
func (fs *FileSystemEventRepository) Close() error {
    // No active resources to clean up
    return nil
}
```

### Health Check

```go
// Health returns health information
func (fs *FileSystemEventRepository) Health(ctx context.Context) (map[string]interface{}, error) {
    fs.mu.RLock()
    defer fs.mu.RUnlock()
    
    health := map[string]interface{}{
        "status":     "up",
        "topicCount": len(fs.topicConfigs),
        "basePath":   fs.basePath,
    }
    
    // Check disk space
    var stat unix.Statfs_t
    if err := unix.Statfs(fs.basePath, &stat); err == nil {
        // Available blocks * block size
        available := stat.Bavail * uint64(stat.Bsize)
        total := stat.Blocks * uint64(stat.Bsize)
        health["diskSpace"] = map[string]uint64{
            "available": available,
            "total":     total,
            "used":      total - available,
        }
    }
    
    // Add event counts per topic
    topicStats := make(map[string]int)
    for topic := range fs.topicConfigs {
        topicDir := filepath.Join(fs.basePath, "events", topic)
        entries, err := os.ReadDir(topicDir)
        if err == nil {
            count := 0
            for _, entry := range entries {
                if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
                    count++
                }
            }
            topicStats[topic] = count
        } else {
            topicStats[topic] = -1 // Error indicator
        }
    }
    health["eventCounts"] = topicStats
    
    return health, nil
}
```

## Performance Characteristics

### Time Complexity

| Operation | Average Case | Worst Case | Notes |
|-----------|--------------|------------|-------|
| AppendEvents | O(n) | O(n) | n = number of events to append |
| GetEvents | O(m log m) | O(m log m) | m = number of event files, log m from sorting |
| GetEventsByType | O(m log m) | O(m log m) | m = number of event files |
| GetLatestVersion | O(1) | O(1) | Cached in memory |
| CreateTopic | O(1) | O(1) | Directory and file creation |
| DeleteTopic | O(m) | O(m) | m = number of event files |
| ListTopics | O(t) | O(t) | t = number of topics |
| TopicExists | O(1) | O(1) | In-memory lookup |

### Space Complexity

Overall space complexity is O(E + T), where:
- E = total number of event files across all topics
- T = number of topic configuration files

### I/O Considerations

1. **File Descriptors**: The implementation should minimize open file descriptors
2. **Disk Space**: Monitor available disk space during operations
3. **File System Limitations**: Be aware of limits on files per directory
4. **Caching**: Use in-memory caching to reduce disk reads

## Extension Points

### Partitioning

For topics with a large number of events, implement directory partitioning:

```
/topic/
├── part-0001/
│   ├── 00000000000000000001.json
│   └── ...
├── part-0002/
│   ├── 00000000000000001001.json
│   └── ...
└── ...
```

### Compression

Add support for compressed event storage:

```go
type FileSystemEventRepositoryConfig struct {
    CompressionEnabled bool
    CompressionLevel   int // -1 (default), 0 (no compression), 1-9 (compression levels)
}
```

### Index Files

Maintain index files to optimize event retrieval:

```
/topic/
├── events/
│   ├── 00000000000000000001.json
│   └── ...
└── indexes/
    ├── version.idx  // Version -> filename mapping
    └── type.idx     // Type -> version mapping
```

## Testing Strategy

### Unit Tests

1. **CRUD Operations**: Test all repository interface methods
2. **Error Handling**: Test for proper handling of I/O errors
3. **Edge Cases**: Test with non-existent directories, full disks, etc.
4. **Concurrency**: Test with multiple goroutines accessing the repository

### Integration Tests

1. **Persistence**: Verify events survive application restarts
2. **File Formats**: Ensure all files are properly formatted and readable
3. **Disk Usage**: Measure storage efficiency with different event sizes

## Example Usage

```go
// Create repository
repo := NewFileSystemEventRepository("/data/eventstore")
err := repo.Initialize(context.Background())
if err != nil {
    log.Fatalf("Failed to initialize repository: %v", err)
}

// Create topic
topicConfig := TopicConfig{
    Name:    "orders",
    Adapter: "fs",
    Connection: "/data/eventstore",
    Options: map[string]string{
        "createDirIfNotExist": "true",
    },
}
err = repo.CreateTopic(context.Background(), topicConfig)
if err != nil {
    log.Fatalf("Failed to create topic: %v", err)
}

// Append events
events := []Event{
    {
        Type: "OrderCreated",
        Data: map[string]interface{}{
            "orderId": "12345",
            "amount":  100.50,
        },
        Metadata: map[string]interface{}{
            "userId": "user-123",
        },
    },
}
err = repo.AppendEvents(context.Background(), "orders", events)
if err != nil {
    log.Fatalf("Failed to append events: %v", err)
}

// Retrieve events
storedEvents, err := repo.GetEvents(context.Background(), "orders", 0)
if err != nil {
    log.Fatalf("Failed to retrieve events: %v", err)
}
for _, event := range storedEvents {
    fmt.Printf("Event: %s, Version: %d\n", event.Type, event.Version)
}
```

## Conclusion

The File System Storage Adaptor provides a durable, self-contained implementation of the EventRepository interface without external dependencies. It balances simplicity with reliability, making it suitable for development, testing, and smaller production environments where simplicity is valued over high-scale performance. 