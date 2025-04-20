# Storage Adaptor Design

## Overview

The Storage Adaptor in our Event Sourcing system is a crucial component that abstracts persistence mechanisms for event storage. Following the Repository Pattern and Clean Architecture principles, the storage adaptor provides a consistent interface for the core domain logic to interact with different storage backends without directly depending on their implementation details.

## Core Interface

The `EventRepository` interface serves as the cornerstone of our storage adaptor design:

```go
type EventRepository interface {
    // Data plane operations
    AppendEvents(ctx context.Context, topic string, events []Event) error
    GetEvents(ctx context.Context, topic string, fromVersion int64) ([]Event, error)
    GetEventsByType(ctx context.Context, topic string, eventType string, fromVersion int64) ([]Event, error)
    GetLatestVersion(ctx context.Context, topic string) (int64, error)
    
    // Control plane operations
    CreateTopic(ctx context.Context, config TopicConfig) error
    DeleteTopic(ctx context.Context, topic string) error
    ListTopics(ctx context.Context) ([]TopicConfig, error)
    TopicExists(ctx context.Context, topic string) (bool, error)
    UpdateTopicConfig(ctx context.Context, config TopicConfig) error
    GetTopicConfig(ctx context.Context, topic string) (TopicConfig, error)
    
    // Lifecycle operations
    Initialize(ctx context.Context) error
    Close() error
    
    // Health check
    Health(ctx context.Context) (map[string]interface{}, error)
}
```

## Design Considerations

### Separation of Concerns

The interface design follows a clear separation between:

1. **Data Plane Operations**: Core event storage and retrieval functionality
2. **Control Plane Operations**: Topic management and configuration
3. **Lifecycle Operations**: Initialization and cleanup
4. **Health Monitoring**: Status reporting for operational visibility

This separation ensures that each adaptor implementation can focus on specific responsibilities and makes the system easier to reason about, test, and maintain.

### Event Structure

The `Event` structure is designed to be storage-agnostic while containing all necessary information:

```go
type Event struct {
    ID        string                 `json:"id"`
    Topic     string                 `json:"topic"`
    Type      string                 `json:"type"`
    Data      map[string]interface{} `json:"data"`
    Metadata  map[string]interface{} `json:"metadata"`
    Timestamp int64                  `json:"timestamp"`
    Version   int64                  `json:"version"`
}
```

Key considerations:
- **ID**: Globally unique identifier for each event
- **Topic**: Logical grouping mechanism for events
- **Type**: Categorization for event handlers/projections
- **Data**: Payload carried by the event (domain-specific)
- **Metadata**: Operational information (user ID, correlation ID, etc.)
- **Timestamp**: When the event occurred (useful for auditing/debugging)
- **Version**: Sequential position in the event stream (critical for ordering)

### Topic Configuration

Each topic is configured with implementation-specific details:

```go
type TopicConfig struct {
    Name       string            `json:"name"`
    Adapter    string            `json:"adapter"`
    Connection string            `json:"connection"`
    Options    map[string]string `json:"options"`
}
```

The `Options` map provides extensibility for adaptor-specific settings without changing the core interface.

## Concurrency Considerations

### Thread Safety

All adaptor implementations must be thread-safe, as they will be accessed concurrently by multiple goroutines. Specific considerations include:

1. **Read/Write Locking**: Adaptors should use appropriate synchronization primitives (e.g., `sync.RWMutex`) to allow concurrent reads while protecting against concurrent writes
   
2. **Context Support**: All methods accept a `context.Context` parameter to support timeout and cancellation, which is essential for preventing blocked operations

3. **Optimistic Concurrency Control**: The event version serves as a sequence number to ensure correct ordering, and implementations should enforce version continuity

4. **Transactional Consistency**: When appending multiple events, implementations should ensure atomic operations where appropriate

### Implementation-Specific Concurrency

Different backends have unique concurrency characteristics:

1. **In-Memory**: Requires explicit synchronization within the adaptor
   
2. **File System**: Must handle file locking to prevent corruption during concurrent writes
   
3. **PostgreSQL**: Can leverage database transactions for ACID guarantees
   
4. **Azure Blob**: Must handle optimistic concurrency using ETags or similar mechanisms

## Error Handling

Adaptor implementations should return clear, meaningful errors that help diagnose issues:

1. **Standard Error Types**: Where possible, use standard Go errors to indicate common problems (e.g., `errors.New("topic not found")`)
   
2. **Wrapped Errors**: Use `fmt.Errorf("operation failed: %w", err)` to preserve underlying error context
   
3. **Error Categorization**: Consider providing helper functions to identify error types (e.g., `IsNotFound`, `IsConflict`)

## Performance Considerations

### Read Optimization

1. **Caching**: Implementations may maintain in-memory caches of events or metadata
   
2. **Partial Reads**: The interface supports retrieving events from a specific version to minimize data transfer
   
3. **Type Filtering**: `GetEventsByType` allows efficient filtering when only specific events are needed

### Write Optimization

1. **Batch Operations**: `AppendEvents` accepts multiple events to enable batched writes
   
2. **Version Handling**: Pre-calculating next versions in memory can reduce database lookups

## Pluggability Design

The adaptor system is designed for runtime configurability:

1. **Factory Pattern**: A factory function creates the appropriate adaptor based on configuration
   
2. **Registration Mechanism**: Adaptors register themselves with a central registry
   
3. **Dependency Injection**: The core logic receives the adaptor through its constructor

Example factory:

```go
func NewEventRepository(config TopicConfig) (EventRepository, error) {
    switch config.Adapter {
    case "memory":
        return memory.NewMemoryEventRepository(), nil
    case "fs":
        return fs.NewFileSystemEventRepository(config.Connection), nil
    case "postgres":
        return postgres.NewPostgresEventRepository(config.Connection), nil
    case "blob":
        return blob.NewBlobEventRepository(config.Connection), nil
    default:
        return nil, fmt.Errorf("unknown adapter type: %s", config.Adapter)
    }
}
```

## Testing Strategy

### Unit Testing

Each adaptor implementation should have comprehensive unit tests that verify:

1. **API Contract**: All interface methods work as expected
2. **Concurrency Behavior**: Multiple goroutines can safely interact with the adaptor
3. **Error Handling**: Expected errors are returned in appropriate circumstances
4. **Edge Cases**: Empty topics, version boundaries, etc.

### Integration Testing

Integration tests should verify that each adaptor works correctly with its backing store:

1. **Real Backend**: Tests should connect to actual instances (using testcontainers where appropriate)
2. **Data Persistence**: Events stored should be retrievable after restart
3. **Performance Characteristics**: Basic benchmarks for common operations

## Lessons Learned

Through our implementation of multiple storage adaptors, we've identified several key insights:

1. **Interface Stability**: The core interface has proven robust across diverse storage backends, validating our abstraction design

2. **Concurrency Challenges**: Each storage technology has unique concurrency behaviors that must be carefully managed:
   - In-memory storage requires explicit locking
   - File systems need careful coordination of reads/writes
   - Databases provide transaction support but require connection management

3. **Configuration Flexibility**: The generic `Options` map accommodates storage-specific parameters without interface changes

4. **Context Propagation**: Passing context through all operations enables proper timeout and cancellation behavior

5. **Version Management**: Treating event versions as a sequential stream simplifies ordering and consistency guarantees

6. **Adapter Complexity Spectrum**: Implementations range from simple (memory) to complex (PostgreSQL), with corresponding trade-offs in durability, performance, and operational overhead

## Future Considerations

1. **Partitioning**: For high-volume topics, implementing partitioning strategies could improve scalability

2. **Event Schema Evolution**: Adding support for schema versioning and migration within events

3. **Compression**: For storage efficiency, implementing transparent compression for event data

4. **Read Replicas**: Supporting read replicas for query-heavy workloads

5. **Encryption**: Adding at-rest encryption options for sensitive event data

6. **Performance Monitoring**: Enhancing the Health API with performance metrics for each adaptor

7. **Snapshot Support**: Implementing periodic state snapshots to optimize rebuilding projections

## Conclusion

The storage adaptor design provides a flexible, consistent interface for event persistence while accommodating the unique characteristics of different storage technologies. By carefully considering concurrency, error handling, and implementation-specific optimizations, we've created a system that can scale from simple in-memory testing to robust production deployments with minimal changes to the core business logic. 