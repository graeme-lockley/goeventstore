# Azure Blob Storage Adaptor Specification

## Overview

The Azure Blob Storage Adaptor implements the `EventRepository` interface using Azure Blob Storage for cloud-based event persistence. It is designed for:

1. Cloud-native applications requiring scalable storage
2. Environments where high availability and geo-redundancy are required
3. Systems that need to manage large volumes of events
4. Applications with multi-region deployment requirements

This adaptor leverages Azure Blob Storage's high durability, scalability, and global replication capabilities.

## Implementation Requirements

### Data Structures

```go
type BlobEventRepository struct {
    mu            sync.RWMutex
    serviceClient *azblob.ServiceClient
    containerURLs map[string]*azblob.ContainerClient // topic -> container client
    topicConfigs  map[string]TopicConfig
    latestVersion map[string]int64 // topic -> latest version (in-memory cache)
}
```

### Storage Layout

```
container-{topic}/
├── config.json                # Topic configuration
├── metadata/
│   └── latest_version.json    # Latest version tracker
└── events/
    ├── 00000000000000000001.json  # Event (version as filename)
    ├── 00000000000000000002.json
    └── ...
```

### Thread Safety

1. Use a read-write mutex for in-memory state protection
2. Leverage Azure Blob Storage's ETag for optimistic concurrency control
3. Handle potential race conditions during event appending

### Blob Operations

1. Use Azure SDK's retry policies for resilient blob operations
2. Implement appropriate error handling for cloud storage issues
3. Manage connection lifecycle efficiently to avoid connection leaks
4. Use batch operations where appropriate for better performance

## Functional Specifications

### Initialization

```go
// NewBlobEventRepository creates a new instance of the Blob repository
func NewBlobEventRepository(connectionString string) (*BlobEventRepository, error) {
    // Create a service client from connection string
    serviceClient, err := azblob.NewServiceClientFromConnectionString(connectionString, nil)
    if err != nil {
        return nil, fmt.Errorf("failed to create service client: %w", err)
    }
    
    return &BlobEventRepository{
        serviceClient: serviceClient,
        containerURLs: make(map[string]*azblob.ContainerClient),
        topicConfigs:  make(map[string]TopicConfig),
        latestVersion: make(map[string]int64),
    }, nil
}

// Initialize prepares the repository for use
func (br *BlobEventRepository) Initialize(ctx context.Context) error {
    // List all containers (topics)
    pager := br.serviceClient.NewListContainersPager(nil)
    
    for pager.More() {
        resp, err := pager.NextPage(ctx)
        if err != nil {
            return fmt.Errorf("failed to list containers: %w", err)
        }
        
        for _, container := range resp.ContainerItems {
            if container.Name == nil || !strings.HasPrefix(*container.Name, "container-") {
                continue
            }
            
            topicName := strings.TrimPrefix(*container.Name, "container-")
            
            // Get container client
            containerClient := br.serviceClient.NewContainerClient(*container.Name)
            br.containerURLs[topicName] = containerClient
            
            // Load topic configuration
            if err := br.loadTopicConfig(ctx, topicName); err != nil {
                return err
            }
            
            // Load latest version
            if err := br.loadLatestVersion(ctx, topicName); err != nil {
                return err
            }
        }
    }
    
    return nil
}

// loadTopicConfig loads a topic's configuration
func (br *BlobEventRepository) loadTopicConfig(ctx context.Context, topic string) error {
    containerClient, exists := br.containerURLs[topic]
    if !exists {
        return fmt.Errorf("container for topic %s not found", topic)
    }
    
    // Get blob client for config
    blobClient := containerClient.NewBlockBlobClient("config.json")
    
    // Get config blob
    downloadResponse, err := blobClient.DownloadStream(ctx, nil)
    if err != nil {
        return fmt.Errorf("failed to download config: %w", err)
    }
    
    // Read config data
    configData, err := io.ReadAll(downloadResponse.Body)
    if err != nil {
        return fmt.Errorf("failed to read config data: %w", err)
    }
    defer downloadResponse.Body.Close()
    
    // Parse config
    var config TopicConfig
    if err := json.Unmarshal(configData, &config); err != nil {
        return fmt.Errorf("failed to parse config: %w", err)
    }
    
    br.topicConfigs[topic] = config
    
    return nil
}

// loadLatestVersion loads a topic's latest event version
func (br *BlobEventRepository) loadLatestVersion(ctx context.Context, topic string) error {
    containerClient, exists := br.containerURLs[topic]
    if !exists {
        return fmt.Errorf("container for topic %s not found", topic)
    }
    
    // Get blob client for latest version
    blobClient := containerClient.NewBlockBlobClient("metadata/latest_version.json")
    
    // Get latest version blob
    downloadResponse, err := blobClient.DownloadStream(ctx, nil)
    if err != nil {
        // If blob doesn't exist, version is 0
        if isStorageError(err, "BlobNotFound") {
            br.latestVersion[topic] = 0
            return nil
        }
        return fmt.Errorf("failed to download latest version: %w", err)
    }
    
    // Read version data
    versionData, err := io.ReadAll(downloadResponse.Body)
    if err != nil {
        return fmt.Errorf("failed to read version data: %w", err)
    }
    defer downloadResponse.Body.Close()
    
    // Parse version
    var versionInfo struct {
        Version int64 `json:"version"`
    }
    if err := json.Unmarshal(versionData, &versionInfo); err != nil {
        return fmt.Errorf("failed to parse version: %w", err)
    }
    
    br.latestVersion[topic] = versionInfo.Version
    
    return nil
}
```

### Event Operations

#### AppendEvents

```go
// AppendEvents adds new events to the store for a specific topic
func (br *BlobEventRepository) AppendEvents(ctx context.Context, topic string, events []Event) error {
    br.mu.Lock()
    containerClient, exists := br.containerURLs[topic]
    if !exists {
        br.mu.Unlock()
        return errors.New("topic not found")
    }
    
    // Get current latest version
    latestVersion := br.latestVersion[topic]
    br.mu.Unlock()
    
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
        filename := fmt.Sprintf("events/%020d.json", events[i].Version)
        
        // Upload event blob
        blobClient := containerClient.NewBlockBlobClient(filename)
        _, err = blobClient.UploadBuffer(ctx, data, &azblob.UploadBufferOptions{
            HTTPHeaders: &azblob.BlobHTTPHeaders{
                BlobContentType: to.Ptr("application/json"),
            },
            Metadata: map[string]*string{
                "eventId":   to.Ptr(events[i].ID),
                "eventType": to.Ptr(events[i].Type),
                "version":   to.Ptr(fmt.Sprintf("%d", events[i].Version)),
            },
        })
        if err != nil {
            return fmt.Errorf("failed to upload event: %w", err)
        }
    }
    
    // Update latest version in memory and in blob storage
    br.mu.Lock()
    br.latestVersion[topic] = latestVersion
    br.mu.Unlock()
    
    // Update latest version in blob storage
    versionInfo := struct {
        Version int64 `json:"version"`
    }{
        Version: latestVersion,
    }
    
    versionData, err := json.Marshal(versionInfo)
    if err != nil {
        return fmt.Errorf("failed to marshal version data: %w", err)
    }
    
    versionBlobClient := containerClient.NewBlockBlobClient("metadata/latest_version.json")
    _, err = versionBlobClient.UploadBuffer(ctx, versionData, &azblob.UploadBufferOptions{
        HTTPHeaders: &azblob.BlobHTTPHeaders{
            BlobContentType: to.Ptr("application/json"),
        },
    })
    if err != nil {
        return fmt.Errorf("failed to update latest version: %w", err)
    }
    
    return nil
}
```

#### GetEvents

```go
// GetEvents retrieves events for a topic, starting from a specific version
func (br *BlobEventRepository) GetEvents(ctx context.Context, topic string, fromVersion int64) ([]Event, error) {
    br.mu.RLock()
    containerClient, exists := br.containerURLs[topic]
    if !exists {
        br.mu.RUnlock()
        return nil, errors.New("topic not found")
    }
    br.mu.RUnlock()
    
    // List event blobs with prefix filter
    var events []Event
    eventsPrefix := "events/"
    
    // List all blobs in the events directory
    pager := containerClient.NewListBlobsFlatPager(&azblob.ListBlobsFlatOptions{
        Prefix: to.Ptr(eventsPrefix),
    })
    
    for pager.More() {
        resp, err := pager.NextPage(ctx)
        if err != nil {
            return nil, fmt.Errorf("failed to list event blobs: %w", err)
        }
        
        for _, blob := range resp.Segment.BlobItems {
            // Parse version from filename
            filename := *blob.Name
            versionStr := strings.TrimSuffix(strings.TrimPrefix(filename, eventsPrefix), ".json")
            version, err := strconv.ParseInt(versionStr, 10, 64)
            if err != nil {
                continue // Skip files with invalid names
            }
            
            // Skip events before fromVersion
            if version < fromVersion {
                continue
            }
            
            // Download event blob
            blobClient := containerClient.NewBlockBlobClient(filename)
            downloadResponse, err := blobClient.DownloadStream(ctx, nil)
            if err != nil {
                return nil, fmt.Errorf("failed to download event %s: %w", filename, err)
            }
            
            // Read event data
            eventData, err := io.ReadAll(downloadResponse.Body)
            if err != nil {
                downloadResponse.Body.Close()
                return nil, fmt.Errorf("failed to read event data: %w", err)
            }
            downloadResponse.Body.Close()
            
            // Parse event
            var event Event
            if err := json.Unmarshal(eventData, &event); err != nil {
                return nil, fmt.Errorf("failed to parse event %s: %w", filename, err)
            }
            
            events = append(events, event)
        }
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
func (br *BlobEventRepository) GetEventsByType(ctx context.Context, topic string, eventType string, fromVersion int64) ([]Event, error) {
    // Get all events first
    allEvents, err := br.GetEvents(ctx, topic, fromVersion)
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
func (br *BlobEventRepository) GetLatestVersion(ctx context.Context, topic string) (int64, error) {
    br.mu.RLock()
    defer br.mu.RUnlock()
    
    // Verify topic exists
    if _, exists := br.topicConfigs[topic]; !exists {
        return 0, errors.New("topic not found")
    }
    
    return br.latestVersion[topic], nil
}
```

### Topic Management

#### CreateTopic

```go
// CreateTopic creates a new topic
func (br *BlobEventRepository) CreateTopic(ctx context.Context, config TopicConfig) error {
    br.mu.Lock()
    defer br.mu.Unlock()
    
    // Verify topic doesn't already exist
    if _, exists := br.topicConfigs[config.Name]; exists {
        return errors.New("topic already exists")
    }
    
    // Create container for topic
    containerName := "container-" + config.Name
    containerClient := br.serviceClient.NewContainerClient(containerName)
    
    _, err := containerClient.Create(ctx, nil)
    if err != nil {
        return fmt.Errorf("failed to create container: %w", err)
    }
    
    // Create metadata directory
    metadataClient := containerClient.NewBlockBlobClient("metadata/.directory")
    _, err = metadataClient.UploadBuffer(ctx, []byte{}, nil)
    if err != nil {
        return fmt.Errorf("failed to create metadata directory: %w", err)
    }
    
    // Create events directory
    eventsClient := containerClient.NewBlockBlobClient("events/.directory")
    _, err = eventsClient.UploadBuffer(ctx, []byte{}, nil)
    if err != nil {
        return fmt.Errorf("failed to create events directory: %w", err)
    }
    
    // Save topic configuration
    configData, err := json.Marshal(config)
    if err != nil {
        return fmt.Errorf("failed to marshal config: %w", err)
    }
    
    configClient := containerClient.NewBlockBlobClient("config.json")
    _, err = configClient.UploadBuffer(ctx, configData, &azblob.UploadBufferOptions{
        HTTPHeaders: &azblob.BlobHTTPHeaders{
            BlobContentType: to.Ptr("application/json"),
        },
    })
    if err != nil {
        return fmt.Errorf("failed to upload config: %w", err)
    }
    
    // Initialize latest version
    versionInfo := struct {
        Version int64 `json:"version"`
    }{
        Version: 0,
    }
    
    versionData, err := json.Marshal(versionInfo)
    if err != nil {
        return fmt.Errorf("failed to marshal version data: %w", err)
    }
    
    versionClient := containerClient.NewBlockBlobClient("metadata/latest_version.json")
    _, err = versionClient.UploadBuffer(ctx, versionData, &azblob.UploadBufferOptions{
        HTTPHeaders: &azblob.BlobHTTPHeaders{
            BlobContentType: to.Ptr("application/json"),
        },
    })
    if err != nil {
        return fmt.Errorf("failed to initialize version: %w", err)
    }
    
    // Update in-memory state
    br.containerURLs[config.Name] = containerClient
    br.topicConfigs[config.Name] = config
    br.latestVersion[config.Name] = 0
    
    return nil
}
```

#### DeleteTopic

```go
// DeleteTopic deletes a topic
func (br *BlobEventRepository) DeleteTopic(ctx context.Context, topic string) error {
    br.mu.Lock()
    defer br.mu.Unlock()
    
    // Verify topic exists
    containerClient, exists := br.containerURLs[topic]
    if !exists {
        return errors.New("topic not found")
    }
    
    // Delete container
    _, err := containerClient.Delete(ctx, nil)
    if err != nil {
        return fmt.Errorf("failed to delete container: %w", err)
    }
    
    // Update in-memory state
    delete(br.containerURLs, topic)
    delete(br.topicConfigs, topic)
    delete(br.latestVersion, topic)
    
    return nil
}
```

#### Additional Topic Management Methods

```go
// ListTopics returns a list of all topics
func (br *BlobEventRepository) ListTopics(ctx context.Context) ([]TopicConfig, error) {
    br.mu.RLock()
    defer br.mu.RUnlock()
    
    topics := make([]TopicConfig, 0, len(br.topicConfigs))
    for _, config := range br.topicConfigs {
        topics = append(topics, config)
    }
    
    return topics, nil
}

// TopicExists checks if a topic exists
func (br *BlobEventRepository) TopicExists(ctx context.Context, topic string) (bool, error) {
    br.mu.RLock()
    defer br.mu.RUnlock()
    
    _, exists := br.topicConfigs[topic]
    return exists, nil
}

// UpdateTopicConfig updates a topic's configuration
func (br *BlobEventRepository) UpdateTopicConfig(ctx context.Context, config TopicConfig) error {
    br.mu.Lock()
    
    // Verify topic exists
    containerClient, exists := br.containerURLs[config.Name]
    if !exists {
        br.mu.Unlock()
        return errors.New("topic not found")
    }
    
    br.mu.Unlock()
    
    // Save updated configuration
    configData, err := json.Marshal(config)
    if err != nil {
        return fmt.Errorf("failed to marshal config: %w", err)
    }
    
    configClient := containerClient.NewBlockBlobClient("config.json")
    _, err = configClient.UploadBuffer(ctx, configData, &azblob.UploadBufferOptions{
        HTTPHeaders: &azblob.BlobHTTPHeaders{
            BlobContentType: to.Ptr("application/json"),
        },
    })
    if err != nil {
        return fmt.Errorf("failed to upload config: %w", err)
    }
    
    // Update in-memory state
    br.mu.Lock()
    br.topicConfigs[config.Name] = config
    br.mu.Unlock()
    
    return nil
}

// GetTopicConfig gets a topic's configuration
func (br *BlobEventRepository) GetTopicConfig(ctx context.Context, topic string) (TopicConfig, error) {
    br.mu.RLock()
    defer br.mu.RUnlock()
    
    // Verify topic exists
    config, exists := br.topicConfigs[topic]
    if !exists {
        return TopicConfig{}, errors.New("topic not found")
    }
    
    return config, nil
}
```

### Lifecycle Operations

```go
// Close cleans up resources
func (br *BlobEventRepository) Close() error {
    // No active resources to clean up specifically
    return nil
}
```

### Health Check

```go
// Health returns health information
func (br *BlobEventRepository) Health(ctx context.Context) (map[string]interface{}, error) {
    health := map[string]interface{}{
        "status": "up",
    }
    
    // Check Azure Blob Storage access
    pager := br.serviceClient.NewListContainersPager(&azblob.ListContainersOptions{
        MaxResults: to.Ptr(int32(1)),
    })
    
    _, err := pager.NextPage(ctx)
    if err != nil {
        health["status"] = "down"
        health["error"] = err.Error()
        return health, nil
    }
    
    br.mu.RLock()
    defer br.mu.RUnlock()
    
    health["topicCount"] = len(br.topicConfigs)
    
    // Add event counts per topic
    eventCounts := make(map[string]int)
    for topic, containerClient := range br.containerURLs {
        count := 0
        
        pager := containerClient.NewListBlobsFlatPager(&azblob.ListBlobsFlatOptions{
            Prefix: to.Ptr("events/"),
        })
        
        for pager.More() {
            resp, err := pager.NextPage(ctx)
            if err != nil {
                eventCounts[topic] = -1 // Error indicator
                break
            }
            
            count += len(resp.Segment.BlobItems)
        }
        
        eventCounts[topic] = count
    }
    
    health["eventCounts"] = eventCounts
    
    return health, nil
}
```

## Performance Characteristics

### Time Complexity

| Operation | Average Case | Worst Case | Notes |
|-----------|--------------|------------|-------|
| AppendEvents | O(n) | O(n) | n = number of events to append |
| GetEvents | O(m) | O(m) | m = number of events to retrieve |
| GetEventsByType | O(m) | O(m) | m = number of events to filter |
| GetLatestVersion | O(1) | O(1) | In-memory lookup |
| CreateTopic | O(1) | O(1) | Container creation |
| DeleteTopic | O(1) | O(1) | Container deletion |
| ListTopics | O(t) | O(t) | t = number of topics |
| TopicExists | O(1) | O(1) | In-memory lookup |

### Scalability Considerations

1. **Throughput**: Azure Blob Storage scales to handle thousands of operations per second
2. **Storage Capacity**: Virtually unlimited storage capacity
3. **Concurrency**: Supports many concurrent clients with automatic scaling
4. **Geo-Redundancy**: Optional geo-replication for disaster recovery

## Extension Points

### Hierarchical Partitioning

For topics with very large event counts, implement directory-based partitioning:

```
events/
├── year=2023/
│   ├── month=01/
│   │   ├── 00000000000000000001.json
│   │   └── ...
│   └── month=02/
│       ├── 00000000000000000101.json
│       └── ...
└── year=2024/
    └── ...
```

### Caching Layer

Add a caching layer for frequently accessed events:

```go
type BlobEventRepositoryWithCache struct {
    *BlobEventRepository
    cache    map[string]map[int64]Event // topic -> version -> event
    cacheMu  sync.RWMutex
    maxCache int // Maximum number of events to cache per topic
}
```

### Batch Operations

Implement batch retrieval for efficient reading:

```go
// GetEventsBatch retrieves events in batches for better performance
func (br *BlobEventRepository) GetEventsBatch(ctx context.Context, topic string, versions []int64) ([]Event, error) {
    // Implementation with goroutines and channels for parallel downloads
}
```

## Testing Strategy

### Unit Tests

1. **Mocked Storage**: Use the Azure Storage Emulator or mocked interfaces
2. **Concurrency Testing**: Verify thread safety with multiple goroutines
3. **Error Handling**: Test proper handling of storage errors
4. **Retry Logic**: Verify retry behavior for transient failures

### Integration Tests

1. **Real Azure Storage**: Test against actual Azure Blob Storage
2. **Network Resilience**: Test behavior under unstable network conditions
3. **Performance**: Benchmark operations under various load conditions

## Example Usage

```go
// Create repository
repo, err := NewBlobEventRepository("DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey;EndpointSuffix=core.windows.net")
if err != nil {
    log.Fatalf("Failed to create repository: %v", err)
}

err = repo.Initialize(context.Background())
if err != nil {
    log.Fatalf("Failed to initialize repository: %v", err)
}

// Create topic
topicConfig := TopicConfig{
    Name:    "orders",
    Adapter: "blob",
    Connection: "DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey;EndpointSuffix=core.windows.net",
    Options: map[string]string{
        "containerPrefix": "container-",
        "createIfNotExist": "true",
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
```

## Conclusion

The Azure Blob Storage Adaptor provides a cloud-native implementation of the EventRepository interface, offering high durability, virtually unlimited scaling, and global availability. It is particularly well-suited for applications that require robust event storage with geo-redundancy capabilities. 