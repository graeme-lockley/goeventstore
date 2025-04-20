# In-Memory Storage Adaptor Specification

## Overview

The In-Memory Storage Adaptor implements the `EventRepository` interface using volatile memory storage. It is primarily designed for:

1. Development and testing environments
2. Small-scale applications with non-critical data
3. Temporary event caching
4. Unit tests that require a lightweight storage implementation

Being memory-based, all data is lost when the application restarts, making this adaptor unsuitable for production scenarios requiring data persistence.

## Implementation Requirements

### Data Structures

The adaptor should utilize the following core data structures:

```go
type MemoryEventRepository struct {
    mu            sync.RWMutex
    events        map[string][]Event    // topic -> ordered events
    topicConfigs  map[string]TopicConfig
    latestVersion map[string]int64      // topic -> latest version
}
```

### Thread Safety

As multiple goroutines may access the repository concurrently, the implementation must:

1. Use a read-write mutex (`sync.RWMutex`) to allow concurrent reads while ensuring exclusive access during writes
2. Lock appropriately for each operation (read lock for queries, write lock for mutations)
3. Consider the granularity of locking (per topic vs. global)

### Memory Management

The adaptor should implement strategies to manage memory consumption:

1. Consider optional configuration for maximum events per topic
2. Provide hooks for garbage collection of old events
3. Support compaction operations to reduce memory footprint

## Functional Specifications

### Initialization

```go
// NewMemoryEventRepository creates a new instance of the in-memory repository
func NewMemoryEventRepository() *MemoryEventRepository {
    return &MemoryEventRepository{
        events:        make(map[string][]Event),
        topicConfigs:  make(map[string]TopicConfig),
        latestVersion: make(map[string]int64),
    }
}

// Initialize prepares the repository for use
func (m *MemoryEventRepository) Initialize(ctx context.Context) error {
    // No persistent storage to initialize
    return nil
}
```

### Event Operations

#### AppendEvents

```go
// AppendEvents adds new events to the store for a specific topic
func (m *MemoryEventRepository) AppendEvents(ctx context.Context, topic string, events []Event) error {
    m.mu.Lock()
    defer m.mu.Unlock()
    
    // Verify topic exists
    if _, exists := m.events[topic]; !exists {
        return errors.New("topic not found")
    }
    
    // Get current latest version
    latestVersion := m.latestVersion[topic]
    
    // Process events
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
        
        // Append to storage
        m.events[topic] = append(m.events[topic], events[i])
    }
    
    // Update latest version
    m.latestVersion[topic] = latestVersion
    
    return nil
}
```

#### GetEvents

```go
// GetEvents retrieves events for a topic, starting from a specific version
func (m *MemoryEventRepository) GetEvents(ctx context.Context, topic string, fromVersion int64) ([]Event, error) {
    m.mu.RLock()
    defer m.mu.RUnlock()
    
    // Verify topic exists
    topicEvents, exists := m.events[topic]
    if !exists {
        return nil, errors.New("topic not found")
    }
    
    // Filter events by version
    var result []Event
    for _, event := range topicEvents {
        if event.Version >= fromVersion {
            result = append(result, event)
        }
    }
    
    // Ensure consistent ordering
    sort.Slice(result, func(i, j int) bool {
        return result[i].Version < result[j].Version
    })
    
    return result, nil
}
```

#### GetEventsByType

```go
// GetEventsByType retrieves events of a specific type for a topic
func (m *MemoryEventRepository) GetEventsByType(ctx context.Context, topic string, eventType string, fromVersion int64) ([]Event, error) {
    m.mu.RLock()
    defer m.mu.RUnlock()
    
    // Verify topic exists
    topicEvents, exists := m.events[topic]
    if !exists {
        return nil, errors.New("topic not found")
    }
    
    // Filter events by version and type
    var result []Event
    for _, event := range topicEvents {
        if event.Version >= fromVersion && event.Type == eventType {
            result = append(result, event)
        }
    }
    
    // Ensure consistent ordering
    sort.Slice(result, func(i, j int) bool {
        return result[i].Version < result[j].Version
    })
    
    return result, nil
}
```

#### GetLatestVersion

```go
// GetLatestVersion returns the latest event version for a topic
func (m *MemoryEventRepository) GetLatestVersion(ctx context.Context, topic string) (int64, error) {
    m.mu.RLock()
    defer m.mu.RUnlock()
    
    // Verify topic exists
    if _, exists := m.events[topic]; !exists {
        return 0, errors.New("topic not found")
    }
    
    return m.latestVersion[topic], nil
}
```

### Topic Management

#### CreateTopic

```go
// CreateTopic creates a new topic
func (m *MemoryEventRepository) CreateTopic(ctx context.Context, config TopicConfig) error {
    m.mu.Lock()
    defer m.mu.Unlock()
    
    // Verify topic doesn't already exist
    if _, exists := m.events[config.Name]; exists {
        return errors.New("topic already exists")
    }
    
    // Initialize topic
    m.events[config.Name] = []Event{}
    m.topicConfigs[config.Name] = config
    m.latestVersion[config.Name] = 0
    
    return nil
}
```

#### DeleteTopic

```go
// DeleteTopic deletes a topic
func (m *MemoryEventRepository) DeleteTopic(ctx context.Context, topic string) error {
    m.mu.Lock()
    defer m.mu.Unlock()
    
    // Verify topic exists
    if _, exists := m.events[topic]; !exists {
        return errors.New("topic not found")
    }
    
    // Remove topic data
    delete(m.events, topic)
    delete(m.topicConfigs, topic)
    delete(m.latestVersion, topic)
    
    return nil
}
```

#### ListTopics

```go
// ListTopics returns a list of all topics
func (m *MemoryEventRepository) ListTopics(ctx context.Context) ([]TopicConfig, error) {
    m.mu.RLock()
    defer m.mu.RUnlock()
    
    topics := make([]TopicConfig, 0, len(m.topicConfigs))
    for _, config := range m.topicConfigs {
        topics = append(topics, config)
    }
    
    return topics, nil
}
```

#### TopicExists

```go
// TopicExists checks if a topic exists
func (m *MemoryEventRepository) TopicExists(ctx context.Context, topic string) (bool, error) {
    m.mu.RLock()
    defer m.mu.RUnlock()
    
    _, exists := m.events[topic]
    return exists, nil
}
```

#### UpdateTopicConfig

```go
// UpdateTopicConfig updates a topic's configuration
func (m *MemoryEventRepository) UpdateTopicConfig(ctx context.Context, config TopicConfig) error {
    m.mu.Lock()
    defer m.mu.Unlock()
    
    // Verify topic exists
    if _, exists := m.events[config.Name]; !exists {
        return errors.New("topic not found")
    }
    
    // Update config
    m.topicConfigs[config.Name] = config
    
    return nil
}
```

#### GetTopicConfig

```go
// GetTopicConfig gets a topic's configuration
func (m *MemoryEventRepository) GetTopicConfig(ctx context.Context, topic string) (TopicConfig, error) {
    m.mu.RLock()
    defer m.mu.RUnlock()
    
    // Verify topic exists
    config, exists := m.topicConfigs[topic]
    if !exists {
        return TopicConfig{}, errors.New("topic not found")
    }
    
    return config, nil
}
```

### Lifecycle Operations

```go
// Close cleans up resources
func (m *MemoryEventRepository) Close() error {
    // No resources to clean up for in-memory implementation
    return nil
}
```

### Health Check

```go
// Health returns health information
func (m *MemoryEventRepository) Health(ctx context.Context) (map[string]interface{}, error) {
    m.mu.RLock()
    defer m.mu.RUnlock()
    
    health := map[string]interface{}{
        "status":     "up",
        "topicCount": len(m.topicConfigs),
    }
    
    // Add event counts
    topicStats := make(map[string]int)
    for topic, events := range m.events {
        topicStats[topic] = len(events)
    }
    health["eventCounts"] = topicStats
    
    // Memory stats (optional)
    var memStats runtime.MemStats
    runtime.ReadMemStats(&memStats)
    health["memoryUsage"] = map[string]uint64{
        "alloc":      memStats.Alloc,
        "totalAlloc": memStats.TotalAlloc,
        "sys":        memStats.Sys,
    }
    
    return health, nil
}
```

## Performance Characteristics

### Time Complexity

| Operation | Average Case | Worst Case | Notes |
|-----------|--------------|------------|-------|
| AppendEvents | O(n) | O(n) | n = number of events to append |
| GetEvents | O(m) | O(m) | m = total events in topic |
| GetEventsByType | O(m) | O(m) | m = total events in topic |
| GetLatestVersion | O(1) | O(1) | Constant time lookup |
| CreateTopic | O(1) | O(1) | Map insertion |
| DeleteTopic | O(1) | O(1) | Map deletion |
| ListTopics | O(t) | O(t) | t = number of topics |
| TopicExists | O(1) | O(1) | Map lookup |

### Space Complexity

Overall space complexity is O(E + T), where:
- E = total number of events across all topics
- T = number of topics

### Optimizations

1. **Pre-filtered Event Arrays**: For frequent `GetEventsByType` calls, consider maintaining type-specific event arrays
2. **Version-Based Indexing**: For large event collections, implement indexing by version for faster lookups
3. **Memory Recycling**: When events are not needed, clear rather than reallocate arrays

## Extension Points

### Event Limits

Add configuration to limit the maximum number of events stored per topic:

```go
type MemoryEventRepositoryConfig struct {
    MaxEventsPerTopic int // 0 means unlimited
}
```

## Testing Strategy

### Unit Tests

1. **Basic CRUD**: Test creating, retrieving, updating, and deleting topics and events
2. **Concurrency**: Verify thread safety with multiple goroutines accessing the repository
3. **Error Conditions**: Test error handling for all operations
4. **Edge Cases**: Test empty topics, maximum versions, etc.

### Benchmark Tests

1. **Throughput**: Measure events per second for reads and writes
2. **Memory Usage**: Track memory consumption with different event loads
3. **Concurrency Scaling**: Measure performance with increasing concurrent access

## Example Usage

```go
// Create repository
repo := NewMemoryEventRepository()
err := repo.Initialize(context.Background())
if err != nil {
    log.Fatalf("Failed to initialize repository: %v", err)
}

// Create topic
topicConfig := TopicConfig{
    Name:    "orders",
    Adapter: "memory",
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

The In-Memory Storage Adaptor provides a lightweight, fast implementation of the EventRepository interface, ideal for development, testing, and small-scale applications. While it doesn't offer persistence, it implements the full functionality required by the interface with strong concurrency support and efficient memory usage. 