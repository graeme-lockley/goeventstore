# PostgreSQL Storage Adaptor Specification

## Overview

The PostgreSQL Storage Adaptor implements the `EventRepository` interface using a PostgreSQL database for robust, scalable event persistence. It is designed for:

1. Production environments requiring high durability
2. Applications with strict data consistency requirements
3. Systems that need to handle high event throughput
4. Scenarios where advanced querying capabilities are needed

This adaptor leverages PostgreSQL's ACID properties to ensure reliable event storage and retrieval, making it suitable for critical business applications.

## Implementation Requirements

### Data Structures

The adaptor should utilize the following core data structures:

```go
type PostgresEventRepository struct {
    mu           sync.RWMutex
    db           *sql.DB
    topicConfigs map[string]TopicConfig
}
```

### Database Schema

The storage should use the following schema:

```sql
-- Topic configuration table
CREATE TABLE topic_configs (
    name VARCHAR(255) PRIMARY KEY,
    adapter VARCHAR(50) NOT NULL,
    connection TEXT NOT NULL,
    options JSONB NOT NULL
);

-- Events table
CREATE TABLE events (
    id VARCHAR(255) NOT NULL,
    topic VARCHAR(255) NOT NULL,
    type VARCHAR(255) NOT NULL,
    version BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    data JSONB NOT NULL,
    metadata JSONB NOT NULL,
    PRIMARY KEY (topic, version),
    UNIQUE (id)
);

-- Indexes for efficient queries
CREATE INDEX idx_events_topic_version ON events (topic, version);
CREATE INDEX idx_events_topic_type ON events (topic, type);
CREATE INDEX idx_events_id ON events (id);
```

### Thread Safety

The implementation must handle concurrent access from multiple goroutines:

1. Use a read-write mutex for in-memory state (topic configurations)
2. Rely on PostgreSQL's transaction isolation for data consistency
3. Implement proper connection pooling for concurrent database access

### Database Operations

The adaptor should implement robust database handling:

1. Use prepared statements for all database operations
2. Implement connection pooling with appropriate limits
3. Use transactions for atomic operations
4. Handle database errors with proper retries where appropriate

## Functional Specifications

### Initialization

```go
// NewPostgresEventRepository creates a new instance of the Postgres repository
func NewPostgresEventRepository(connectionString string) (*PostgresEventRepository, error) {
    db, err := sql.Open("postgres", connectionString)
    if err != nil {
        return nil, fmt.Errorf("failed to connect to database: %w", err)
    }
    
    // Test connection
    if err := db.Ping(); err != nil {
        db.Close()
        return nil, fmt.Errorf("failed to ping database: %w", err)
    }
    
    return &PostgresEventRepository{
        db:           db,
        topicConfigs: make(map[string]TopicConfig),
    }, nil
}

// Initialize prepares the repository for use
func (pg *PostgresEventRepository) Initialize(ctx context.Context) error {
    // Create tables if they don't exist
    if err := pg.createSchema(ctx); err != nil {
        return err
    }
    
    // Load existing topic configurations
    if err := pg.loadTopicConfigs(ctx); err != nil {
        return err
    }
    
    return nil
}

// createSchema ensures the required database tables exist
func (pg *PostgresEventRepository) createSchema(ctx context.Context) error {
    // Create topic_configs table
    _, err := pg.db.ExecContext(ctx, `
        CREATE TABLE IF NOT EXISTS topic_configs (
            name VARCHAR(255) PRIMARY KEY,
            adapter VARCHAR(50) NOT NULL,
            connection TEXT NOT NULL,
            options JSONB NOT NULL
        );
    `)
    if err != nil {
        return fmt.Errorf("failed to create topic_configs table: %w", err)
    }
    
    // Create events table
    _, err = pg.db.ExecContext(ctx, `
        CREATE TABLE IF NOT EXISTS events (
            id VARCHAR(255) NOT NULL,
            topic VARCHAR(255) NOT NULL,
            type VARCHAR(255) NOT NULL,
            version BIGINT NOT NULL,
            timestamp BIGINT NOT NULL,
            data JSONB NOT NULL,
            metadata JSONB NOT NULL,
            PRIMARY KEY (topic, version),
            UNIQUE (id)
        );
    `)
    if err != nil {
        return fmt.Errorf("failed to create events table: %w", err)
    }
    
    // Create indexes
    _, err = pg.db.ExecContext(ctx, `
        CREATE INDEX IF NOT EXISTS idx_events_topic_version ON events (topic, version);
        CREATE INDEX IF NOT EXISTS idx_events_topic_type ON events (topic, type);
        CREATE INDEX IF NOT EXISTS idx_events_id ON events (id);
    `)
    if err != nil {
        return fmt.Errorf("failed to create indexes: %w", err)
    }
    
    return nil
}

// loadTopicConfigs loads all topic configurations from the database
func (pg *PostgresEventRepository) loadTopicConfigs(ctx context.Context) error {
    pg.mu.Lock()
    defer pg.mu.Unlock()
    
    rows, err := pg.db.QueryContext(ctx, "SELECT name, adapter, connection, options FROM topic_configs")
    if err != nil {
        return fmt.Errorf("failed to query topic configurations: %w", err)
    }
    defer rows.Close()
    
    for rows.Next() {
        var config TopicConfig
        var optionsJSON []byte
        
        if err := rows.Scan(&config.Name, &config.Adapter, &config.Connection, &optionsJSON); err != nil {
            return fmt.Errorf("failed to scan topic configuration: %w", err)
        }
        
        // Parse options
        if err := json.Unmarshal(optionsJSON, &config.Options); err != nil {
            return fmt.Errorf("failed to parse options JSON: %w", err)
        }
        
        pg.topicConfigs[config.Name] = config
    }
    
    if err := rows.Err(); err != nil {
        return fmt.Errorf("error iterating topic configurations: %w", err)
    }
    
    return nil
}
```

### Event Operations

#### AppendEvents

```go
// AppendEvents adds new events to the store for a specific topic
func (pg *PostgresEventRepository) AppendEvents(ctx context.Context, topic string, events []Event) error {
    pg.mu.RLock()
    // Verify topic exists
    if _, exists := pg.topicConfigs[topic]; !exists {
        pg.mu.RUnlock()
        return errors.New("topic not found")
    }
    pg.mu.RUnlock()
    
    // Begin transaction
    tx, err := pg.db.BeginTx(ctx, nil)
    if err != nil {
        return fmt.Errorf("failed to begin transaction: %w", err)
    }
    defer tx.Rollback() // Rollback if not committed
    
    // Get current latest version
    var latestVersion int64
    err = tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(version), 0) FROM events WHERE topic = $1", topic).Scan(&latestVersion)
    if err != nil {
        return fmt.Errorf("failed to get latest version: %w", err)
    }
    
    // Prepare insert statement
    stmt, err := tx.PrepareContext(ctx, `
        INSERT INTO events (id, topic, type, version, timestamp, data, metadata)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
    `)
    if err != nil {
        return fmt.Errorf("failed to prepare statement: %w", err)
    }
    defer stmt.Close()
    
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
        
        // Marshal JSON data
        dataJSON, err := json.Marshal(events[i].Data)
        if err != nil {
            return fmt.Errorf("failed to marshal event data: %w", err)
        }
        
        // Marshal JSON metadata
        metadataJSON, err := json.Marshal(events[i].Metadata)
        if err != nil {
            return fmt.Errorf("failed to marshal event metadata: %w", err)
        }
        
        // Insert event
        _, err = stmt.ExecContext(
            ctx,
            events[i].ID,
            events[i].Topic,
            events[i].Type,
            events[i].Version,
            events[i].Timestamp,
            dataJSON,
            metadataJSON,
        )
        if err != nil {
            return fmt.Errorf("failed to insert event: %w", err)
        }
    }
    
    // Commit transaction
    if err := tx.Commit(); err != nil {
        return fmt.Errorf("failed to commit transaction: %w", err)
    }
    
    return nil
}
```

#### GetEvents

```go
// GetEvents retrieves events for a topic, starting from a specific version
func (pg *PostgresEventRepository) GetEvents(ctx context.Context, topic string, fromVersion int64) ([]Event, error) {
    pg.mu.RLock()
    // Verify topic exists
    if _, exists := pg.topicConfigs[topic]; !exists {
        pg.mu.RUnlock()
        return nil, errors.New("topic not found")
    }
    pg.mu.RUnlock()
    
    // Query events
    rows, err := pg.db.QueryContext(
        ctx,
        `SELECT id, topic, type, version, timestamp, data, metadata 
         FROM events 
         WHERE topic = $1 AND version >= $2 
         ORDER BY version ASC`,
        topic,
        fromVersion,
    )
    if err != nil {
        return nil, fmt.Errorf("failed to query events: %w", err)
    }
    defer rows.Close()
    
    return pg.scanEvents(rows)
}

// scanEvents reads events from database rows
func (pg *PostgresEventRepository) scanEvents(rows *sql.Rows) ([]Event, error) {
    var events []Event
    
    for rows.Next() {
        var event Event
        var dataJSON, metadataJSON []byte
        
        if err := rows.Scan(
            &event.ID,
            &event.Topic,
            &event.Type,
            &event.Version,
            &event.Timestamp,
            &dataJSON,
            &metadataJSON,
        ); err != nil {
            return nil, fmt.Errorf("failed to scan event row: %w", err)
        }
        
        // Parse JSON data
        if err := json.Unmarshal(dataJSON, &event.Data); err != nil {
            return nil, fmt.Errorf("failed to parse event data: %w", err)
        }
        
        // Parse JSON metadata
        if err := json.Unmarshal(metadataJSON, &event.Metadata); err != nil {
            return nil, fmt.Errorf("failed to parse event metadata: %w", err)
        }
        
        events = append(events, event)
    }
    
    if err := rows.Err(); err != nil {
        return nil, fmt.Errorf("error iterating event rows: %w", err)
    }
    
    return events, nil
}
```

#### GetEventsByType

```go
// GetEventsByType retrieves events of a specific type for a topic
func (pg *PostgresEventRepository) GetEventsByType(ctx context.Context, topic string, eventType string, fromVersion int64) ([]Event, error) {
    pg.mu.RLock()
    // Verify topic exists
    if _, exists := pg.topicConfigs[topic]; !exists {
        pg.mu.RUnlock()
        return nil, errors.New("topic not found")
    }
    pg.mu.RUnlock()
    
    // Query events by type
    rows, err := pg.db.QueryContext(
        ctx,
        `SELECT id, topic, type, version, timestamp, data, metadata 
         FROM events 
         WHERE topic = $1 AND type = $2 AND version >= $3 
         ORDER BY version ASC`,
        topic,
        eventType,
        fromVersion,
    )
    if err != nil {
        return nil, fmt.Errorf("failed to query events by type: %w", err)
    }
    defer rows.Close()
    
    return pg.scanEvents(rows)
}
```

#### GetLatestVersion

```go
// GetLatestVersion returns the latest event version for a topic
func (pg *PostgresEventRepository) GetLatestVersion(ctx context.Context, topic string) (int64, error) {
    pg.mu.RLock()
    // Verify topic exists
    if _, exists := pg.topicConfigs[topic]; !exists {
        pg.mu.RUnlock()
        return 0, errors.New("topic not found")
    }
    pg.mu.RUnlock()
    
    var latestVersion int64
    err := pg.db.QueryRowContext(
        ctx,
        "SELECT COALESCE(MAX(version), 0) FROM events WHERE topic = $1",
        topic,
    ).Scan(&latestVersion)
    
    if err != nil {
        return 0, fmt.Errorf("failed to get latest version: %w", err)
    }
    
    return latestVersion, nil
}
```

### Topic Management

#### CreateTopic

```go
// CreateTopic creates a new topic
func (pg *PostgresEventRepository) CreateTopic(ctx context.Context, config TopicConfig) error {
    pg.mu.Lock()
    defer pg.mu.Unlock()
    
    // Verify topic doesn't already exist
    if _, exists := pg.topicConfigs[config.Name]; exists {
        return errors.New("topic already exists")
    }
    
    // Marshal options to JSON
    optionsJSON, err := json.Marshal(config.Options)
    if err != nil {
        return fmt.Errorf("failed to marshal options: %w", err)
    }
    
    // Insert topic configuration
    _, err = pg.db.ExecContext(
        ctx,
        "INSERT INTO topic_configs (name, adapter, connection, options) VALUES ($1, $2, $3, $4)",
        config.Name,
        config.Adapter,
        config.Connection,
        optionsJSON,
    )
    if err != nil {
        return fmt.Errorf("failed to insert topic configuration: %w", err)
    }
    
    // Update in-memory state
    pg.topicConfigs[config.Name] = config
    
    return nil
}
```

#### DeleteTopic

```go
// DeleteTopic deletes a topic
func (pg *PostgresEventRepository) DeleteTopic(ctx context.Context, topic string) error {
    pg.mu.Lock()
    defer pg.mu.Unlock()
    
    // Verify topic exists
    if _, exists := pg.topicConfigs[topic]; !exists {
        return errors.New("topic not found")
    }
    
    // Begin transaction
    tx, err := pg.db.BeginTx(ctx, nil)
    if err != nil {
        return fmt.Errorf("failed to begin transaction: %w", err)
    }
    defer tx.Rollback() // Rollback if not committed
    
    // Delete events for the topic
    _, err = tx.ExecContext(ctx, "DELETE FROM events WHERE topic = $1", topic)
    if err != nil {
        return fmt.Errorf("failed to delete events: %w", err)
    }
    
    // Delete topic configuration
    _, err = tx.ExecContext(ctx, "DELETE FROM topic_configs WHERE name = $1", topic)
    if err != nil {
        return fmt.Errorf("failed to delete topic configuration: %w", err)
    }
    
    // Commit transaction
    if err := tx.Commit(); err != nil {
        return fmt.Errorf("failed to commit transaction: %w", err)
    }
    
    // Update in-memory state
    delete(pg.topicConfigs, topic)
    
    return nil
}
```

#### ListTopics

```go
// ListTopics returns a list of all topics
func (pg *PostgresEventRepository) ListTopics(ctx context.Context) ([]TopicConfig, error) {
    pg.mu.RLock()
    defer pg.mu.RUnlock()
    
    topics := make([]TopicConfig, 0, len(pg.topicConfigs))
    for _, config := range pg.topicConfigs {
        topics = append(topics, config)
    }
    
    return topics, nil
}
```

#### TopicExists

```go
// TopicExists checks if a topic exists
func (pg *PostgresEventRepository) TopicExists(ctx context.Context, topic string) (bool, error) {
    pg.mu.RLock()
    defer pg.mu.RUnlock()
    
    _, exists := pg.topicConfigs[topic]
    return exists, nil
}
```

#### UpdateTopicConfig

```go
// UpdateTopicConfig updates a topic's configuration
func (pg *PostgresEventRepository) UpdateTopicConfig(ctx context.Context, config TopicConfig) error {
    pg.mu.Lock()
    defer pg.mu.Unlock()
    
    // Verify topic exists
    if _, exists := pg.topicConfigs[topic]; !exists {
        return errors.New("topic not found")
    }
    
    // Marshal options to JSON
    optionsJSON, err := json.Marshal(config.Options)
    if err != nil {
        return fmt.Errorf("failed to marshal options: %w", err)
    }
    
    // Update topic configuration
    _, err = pg.db.ExecContext(
        ctx,
        "UPDATE topic_configs SET adapter = $1, connection = $2, options = $3 WHERE name = $4",
        config.Adapter,
        config.Connection,
        optionsJSON,
        config.Name,
    )
    if err != nil {
        return fmt.Errorf("failed to update topic configuration: %w", err)
    }
    
    // Update in-memory state
    pg.topicConfigs[config.Name] = config
    
    return nil
}
```

#### GetTopicConfig

```go
// GetTopicConfig gets a topic's configuration
func (pg *PostgresEventRepository) GetTopicConfig(ctx context.Context, topic string) (TopicConfig, error) {
    pg.mu.RLock()
    defer pg.mu.RUnlock()
    
    // Verify topic exists
    config, exists := pg.topicConfigs[topic]
    if !exists {
        return TopicConfig{}, errors.New("topic not found")
    }
    
    return config, nil
}
```

### Lifecycle Operations

```go
// Close cleans up resources
func (pg *PostgresEventRepository) Close() error {
    return pg.db.Close()
}
```

### Health Check

```go
// Health returns health information
func (pg *PostgresEventRepository) Health(ctx context.Context) (map[string]interface{}, error) {
    health := map[string]interface{}{
        "status": "up",
    }
    
    // Check database connection
    if err := pg.db.PingContext(ctx); err != nil {
        health["status"] = "down"
        health["error"] = err.Error()
        return health, nil
    }
    
    // Get database statistics
    stats := pg.db.Stats()
    health["connections"] = map[string]interface{}{
        "open":      stats.OpenConnections,
        "inUse":     stats.InUse,
        "idle":      stats.Idle,
        "waitCount": stats.WaitCount,
        "waitTime":  stats.WaitDuration.String(),
    }
    
    // Get topic count
    var topicCount int
    err := pg.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM topic_configs").Scan(&topicCount)
    if err == nil {
        health["topicCount"] = topicCount
    }
    
    // Get event counts per topic
    rows, err := pg.db.QueryContext(ctx, "SELECT topic, COUNT(*) FROM events GROUP BY topic")
    if err == nil {
        defer rows.Close()
        
        eventCounts := make(map[string]int)
        for rows.Next() {
            var topic string
            var count int
            if err := rows.Scan(&topic, &count); err == nil {
                eventCounts[topic] = count
            }
        }
        
        health["eventCounts"] = eventCounts
    }
    
    return health, nil
}
```

## Performance Characteristics

### Time Complexity

| Operation | Average Case | Worst Case | Notes |
|-----------|--------------|------------|-------|
| AppendEvents | O(n) | O(n) | n = number of events to append |
| GetEvents | O(m) | O(m) | m = number of events to retrieve |
| GetEventsByType | O(k) | O(k) | k = number of events of specified type |
| GetLatestVersion | O(1) | O(1) | Index lookup |
| CreateTopic | O(1) | O(1) | Single row insert |
| DeleteTopic | O(m) | O(m) | m = number of events in topic |
| ListTopics | O(t) | O(t) | t = number of topics |
| TopicExists | O(1) | O(1) | In-memory lookup |

### Space Complexity

Overall space complexity is O(E + T), where:
- E = total number of events across all topics
- T = number of topic configurations

### Scalability Considerations

1. **Connection Pooling**: Configure appropriate pool size based on expected concurrency
2. **Indexing**: The implementation includes indexes for common query patterns
3. **Transaction Isolation**: Uses PostgreSQL's transaction system for consistency
4. **Statement Preparation**: Uses prepared statements to reduce parsing overhead

## Extension Points

### Partitioning

For large event volumes, implement PostgreSQL table partitioning:

```sql
-- Create partitioned events table
CREATE TABLE events (
    id VARCHAR(255) NOT NULL,
    topic VARCHAR(255) NOT NULL,
    type VARCHAR(255) NOT NULL,
    version BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    data JSONB NOT NULL,
    metadata JSONB NOT NULL,
    PRIMARY KEY (topic, version)
) PARTITION BY LIST (topic);

-- Create partition for a specific topic
CREATE TABLE events_orders PARTITION OF events
    FOR VALUES IN ('orders');
```

### JSON Queries

Leverage PostgreSQL's JSON capabilities for advanced filtering:

```go
// GetEventsByProperty queries events by JSON property values
func (pg *PostgresEventRepository) GetEventsByProperty(ctx context.Context, topic string, path string, value interface{}, fromVersion int64) ([]Event, error) {
    // Query using JSON path expressions
    rows, err := pg.db.QueryContext(
        ctx,
        `SELECT id, topic, type, version, timestamp, data, metadata 
         FROM events 
         WHERE topic = $1 AND version >= $2 AND data->$3 = $4
         ORDER BY version ASC`,
        topic,
        fromVersion,
        path,
        value,
    )
    if err != nil {
        return nil, fmt.Errorf("failed to query events by property: %w", err)
    }
    defer rows.Close()
    
    return pg.scanEvents(rows)
}
```

### Paging Support

Add support for paginated queries:

```go
// GetEventsPaged retrieves a page of events
func (pg *PostgresEventRepository) GetEventsPaged(ctx context.Context, topic string, fromVersion int64, limit int, offset int) ([]Event, error) {
    rows, err := pg.db.QueryContext(
        ctx,
        `SELECT id, topic, type, version, timestamp, data, metadata 
         FROM events 
         WHERE topic = $1 AND version >= $2 
         ORDER BY version ASC
         LIMIT $3 OFFSET $4`,
        topic,
        fromVersion,
        limit,
        offset,
    )
    if err != nil {
        return nil, fmt.Errorf("failed to query events page: %w", err)
    }
    defer rows.Close()
    
    return pg.scanEvents(rows)
}
```

## Testing Strategy

### Unit Tests

1. **Database Mocking**: Use `sqlmock` to test repository logic without a real database
2. **Transaction Handling**: Verify proper transaction rollback on errors
3. **Concurrency**: Test with multiple goroutines accessing the repository
4. **Error Handling**: Test error propagation for database failures

### Integration Tests

1. **PostgreSQL Container**: Use testcontainers to run tests against real PostgreSQL
2. **Schema Initialization**: Verify schema creation works correctly
3. **ACID Properties**: Test transaction handling and constraints
4. **Performance**: Measure query execution times with different event volumes

## Example Usage

```go
// Create repository
repo, err := NewPostgresEventRepository("postgres://user:pass@localhost/eventstore")
if err != nil {
    log.Fatalf("Failed to create repository: %v", err)
}
defer repo.Close()

err = repo.Initialize(context.Background())
if err != nil {
    log.Fatalf("Failed to initialize repository: %v", err)
}

// Create topic
topicConfig := TopicConfig{
    Name:    "orders",
    Adapter: "postgres",
    Connection: "postgres://user:pass@localhost/eventstore",
    Options: map[string]string{
        "schema": "public",
        "table": "events",
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
            "amount": 100.50,
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

The PostgreSQL Storage Adaptor provides a robust, scalable implementation of the EventRepository interface for production environments. It leverages PostgreSQL's ACID transactions, indexing capabilities, and JSONB support to deliver a high-performance event store suitable for critical business applications. 