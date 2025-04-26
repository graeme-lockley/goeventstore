// Package eventstore provides a high-level interface to the event repository
// with configuration management through events.
package eventstore

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"goeventsource/src/internal/eventstore/models"
	"goeventsource/src/internal/eventstore/subscribers"
	"goeventsource/src/internal/port/outbound"

	"github.com/google/uuid"
)

// Common errors for EventStore operations
var (
	ErrNotInitialized     = errors.New("eventstore not initialized")
	ErrTopicNotFound      = errors.New("topic not found")
	ErrInvalidTopicConfig = errors.New("invalid topic configuration")
	ErrTopicAlreadyExists = errors.New("topic already exists")
	ErrMissingConfigField = errors.New("missing required configuration field")
	ErrInvalidEventData   = errors.New("invalid event data")
)

// Configuration event types for the EventStore
const (
	// EventTypeTopicCreated is the event type for when a topic is created
	EventTypeTopicCreated = "TopicCreated"

	// EventTypeTopicUpdated is the event type for when a topic is updated
	EventTypeTopicUpdated = "TopicUpdated"

	// EventTypeTopicDeleted is the event type for when a topic is deleted
	EventTypeTopicDeleted = "TopicDeleted"
)

// EventStoreInterface defines the operations of the EventStore
type EventStoreInterface interface {
	// Initialization
	Initialize(ctx context.Context) error

	// Topic management
	CreateTopic(ctx context.Context, config outbound.TopicConfig) error
	UpdateTopic(ctx context.Context, config outbound.TopicConfig) error
	DeleteTopic(ctx context.Context, topicName string) error
	GetTopic(ctx context.Context, topicName string) (outbound.TopicConfig, error)
	ListTopics(ctx context.Context) ([]outbound.TopicConfig, error)

	// Event operations
	AppendEvents(ctx context.Context, topicName string, events []outbound.Event) error
	GetEvents(ctx context.Context, topicName string, fromVersion int64) ([]outbound.Event, error)
	GetEventsByType(ctx context.Context, topicName string, eventType string, fromVersion int64) ([]outbound.Event, error)
	GetLatestVersion(ctx context.Context, topicName string) (int64, error)

	// Subscriber management
	RegisterSubscriber(ctx context.Context, config outbound.SubscriberConfig) (outbound.Subscriber, error)
	DeregisterSubscriber(ctx context.Context, subscriberID string, reason string) error
	UpdateSubscriber(ctx context.Context, subscriberID string, updateFn func(outbound.Subscriber) error) error
	GetSubscriber(ctx context.Context, subscriberID string) (outbound.Subscriber, error)
	ListSubscribers(ctx context.Context, topicFilter string) ([]outbound.Subscriber, error)

	// Lifecycle and health
	Health(ctx context.Context) (map[string]interface{}, error)
	Close() error
}

// EventStore provides a higher-level interface to the event repository
// that includes configuration management through events
type EventStore struct {
	mu            sync.RWMutex
	registry      *RepositoryRegistry
	configStream  models.EventStreamConfig
	topicConfigs  map[string]outbound.TopicConfig // In-memory write model
	logger        *log.Logger
	isInitialized bool
}

// EventStoreOption is a functional option for configuring the EventStore
type EventStoreOption func(*EventStore)

// WithLogger sets a custom logger for the EventStore
func WithLogger(logger *log.Logger) EventStoreOption {
	return func(es *EventStore) {
		if logger != nil {
			es.logger = logger
		}
	}
}

// WithConfigStream sets a custom configuration stream
func WithConfigStream(config models.EventStreamConfig) EventStoreOption {
	return func(es *EventStore) {
		if err := config.Validate(); err == nil {
			es.configStream = config
		}
	}
}

// NewEventStore creates a new EventStore with the given configuration
func NewEventStore(config models.EventStoreConfig, options ...EventStoreOption) (*EventStore, error) {
	// Validate the configuration
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid event store configuration: %w", err)
	}

	es := &EventStore{
		registry:      NewRepositoryRegistry(),
		configStream:  config.ConfigStream,
		topicConfigs:  make(map[string]outbound.TopicConfig),
		logger:        config.Logger,
		isInitialized: false,
	}

	// Apply options
	for _, option := range options {
		option(es)
	}

	return es, nil
}

// getConfigRepository returns the repository for the configuration stream
func (es *EventStore) getConfigRepository(ctx context.Context) (outbound.EventRepository, error) {
	repo, err := es.registry.GetOrCreate(ctx, es.configStream)
	if err != nil {
		es.logger.Printf("Failed to get config repository: %v", err)
		return nil, fmt.Errorf("failed to get config repository: %w", err)
	}
	return repo, nil
}

// getRepositoryForTopic returns the repository for a topic
func (es *EventStore) getRepositoryForTopic(ctx context.Context, topicName string) (outbound.EventRepository, error) {
	// Get topic configuration from in-memory write model
	topicConfig, exists := es.topicConfigs[topicName]
	if !exists {
		es.logger.Printf("Topic not found: %s", topicName)
		return nil, fmt.Errorf("%w: %s", ErrTopicNotFound, topicName)
	}

	// Convert TopicConfig to EventStreamConfig
	streamConfig := models.FromTopicConfig(topicConfig)

	// Get or create repository for this stream
	repo, err := es.registry.GetOrCreate(ctx, streamConfig)
	if err != nil {
		es.logger.Printf("Failed to get repository for topic %s: %v", topicName, err)
		return nil, fmt.Errorf("failed to get repository for topic %s: %w", topicName, err)
	}

	return repo, nil
}

// Initialize initializes the EventStore with the provided configuration
// It creates the configuration topic if it doesn't exist and loads all configuration events
func (es *EventStore) Initialize(ctx context.Context) error {
	es.mu.Lock()
	defer es.mu.Unlock()

	if es.isInitialized {
		es.logger.Printf("EventStore already initialized")
		return nil
	}

	es.logger.Printf("Initializing EventStore with configuration topic: %s", es.configStream.Name)

	// Check if registry is nil or shutdown and recreate if needed
	if es.registry == nil || es.registry.isShutdown {
		es.logger.Printf("Creating new repository registry")
		es.registry = NewRepositoryRegistry()
	}

	// Get the configuration repository
	configRepo, err := es.getConfigRepository(ctx)
	if err != nil {
		es.logger.Printf("Failed to get configuration repository: %v", err)
		return fmt.Errorf("failed to get configuration repository: %w", err)
	}

	// Initialize the configuration repository
	if err := configRepo.Initialize(ctx); err != nil {
		es.logger.Printf("Failed to initialize configuration repository: %v", err)
		return fmt.Errorf("failed to initialize configuration repository: %w", err)
	}

	// Check if configuration topic exists
	exists, err := configRepo.TopicExists(ctx, es.configStream.Name)
	if err != nil {
		es.logger.Printf("Failed to check if configuration topic exists: %v", err)
		return fmt.Errorf("failed to check if configuration topic exists: %w", err)
	}

	// Create configuration topic if it doesn't exist
	if !exists {
		es.logger.Printf("Configuration topic %s does not exist, creating it", es.configStream.Name)
		configTopicConfig := es.configStream.ToTopicConfig()

		if err := configRepo.CreateTopic(ctx, configTopicConfig); err != nil {
			es.logger.Printf("Failed to create configuration topic: %v", err)
			return fmt.Errorf("failed to create configuration topic: %w", err)
		}
		es.logger.Printf("Created configuration topic: %s", es.configStream.Name)
	}

	// Load all configuration events and build the write model
	if err := es.rebuildWriteModel(ctx); err != nil {
		es.logger.Printf("Failed to rebuild write model: %v", err)
		return fmt.Errorf("failed to rebuild write model: %w", err)
	}

	es.isInitialized = true
	es.logger.Printf("EventStore initialized successfully with %d topics", len(es.topicConfigs))
	return nil
}

// rebuildWriteModel loads all configuration events and rebuilds the in-memory state
func (es *EventStore) rebuildWriteModel(ctx context.Context) error {
	// Clear existing state
	es.topicConfigs = make(map[string]outbound.TopicConfig)

	// Get the configuration repository
	configRepo, err := es.getConfigRepository(ctx)
	if err != nil {
		return fmt.Errorf("failed to get configuration repository: %w", err)
	}

	// Load all events from the configuration topic
	events, err := configRepo.GetEvents(ctx, es.configStream.Name, 0)
	if err != nil {
		return fmt.Errorf("failed to get configuration events: %w", err)
	}

	es.logger.Printf("Rebuilding write model from %d configuration events", len(events))

	// Process each event to build the write model
	for _, event := range events {
		if err := es.applyConfigurationEvent(event); err != nil {
			return fmt.Errorf("failed to apply configuration event: %w", err)
		}
	}

	return nil
}

// applyConfigurationEvent applies a single configuration event to the write model
func (es *EventStore) applyConfigurationEvent(event outbound.Event) error {
	switch event.Type {
	case EventTypeTopicCreated:
		return es.handleTopicCreated(event)
	case EventTypeTopicUpdated:
		return es.handleTopicUpdated(event)
	case EventTypeTopicDeleted:
		return es.handleTopicDeleted(event)
	default:
		return fmt.Errorf("unknown configuration event type: %s", event.Type)
	}
}

// handleTopicCreated handles a TopicCreated event
func (es *EventStore) handleTopicCreated(event outbound.Event) error {
	// Extract topic configuration from event data
	topicName, ok := event.Data["name"].(string)
	if !ok {
		return errors.New("invalid topic name in configuration event")
	}

	adapter, ok := event.Data["adapter"].(string)
	if !ok {
		return errors.New("invalid adapter in configuration event")
	}

	// Create topic configuration
	topicConfig := outbound.TopicConfig{
		Name:    topicName,
		Adapter: adapter,
	}

	// Extract optional connection
	if connection, ok := event.Data["connection"].(string); ok {
		topicConfig.Connection = connection
	}

	// Extract options map
	if optionsData, ok := event.Data["options"].(map[string]interface{}); ok {
		options := make(map[string]string)
		for key, value := range optionsData {
			if strValue, ok := value.(string); ok {
				options[key] = strValue
			}
		}
		topicConfig.Options = options
	} else {
		topicConfig.Options = make(map[string]string)
	}

	// Store in write model
	es.topicConfigs[topicName] = topicConfig
	es.logger.Printf("Applied TopicCreated event for topic: %s", topicName)

	return nil
}

// handleTopicUpdated handles a TopicUpdated event
func (es *EventStore) handleTopicUpdated(event outbound.Event) error {
	// Extract topic name
	topicName, ok := event.Data["name"].(string)
	if !ok {
		return errors.New("invalid topic name in configuration event")
	}

	// Check if topic exists in write model
	existingConfig, exists := es.topicConfigs[topicName]
	if !exists {
		return fmt.Errorf("topic %s not found in write model", topicName)
	}

	// Apply updates from event
	if adapter, ok := event.Data["adapter"].(string); ok {
		existingConfig.Adapter = adapter
	}

	if connection, ok := event.Data["connection"].(string); ok {
		existingConfig.Connection = connection
	}

	if optionsData, ok := event.Data["options"].(map[string]interface{}); ok {
		// If options provided in update, merge them with existing options
		for key, value := range optionsData {
			if strValue, ok := value.(string); ok {
				existingConfig.Options[key] = strValue
			}
		}
	}

	// Update in write model
	es.topicConfigs[topicName] = existingConfig
	es.logger.Printf("Applied TopicUpdated event for topic: %s", topicName)

	return nil
}

// handleTopicDeleted handles a TopicDeleted event
func (es *EventStore) handleTopicDeleted(event outbound.Event) error {
	// Extract topic name
	topicName, ok := event.Data["name"].(string)
	if !ok {
		return errors.New("invalid topic name in configuration event")
	}

	// Delete from write model (if exists, ignore if not)
	delete(es.topicConfigs, topicName)
	es.logger.Printf("Applied TopicDeleted event for topic: %s", topicName)

	return nil
}

// Close closes the EventStore and all its repositories
func (es *EventStore) Close() error {
	es.mu.Lock()
	defer es.mu.Unlock()

	if !es.isInitialized {
		es.logger.Printf("EventStore already closed or not initialized")
		return nil // Nothing to close
	}

	es.logger.Printf("Closing EventStore and all repositories")

	// Close all repositories through the registry
	err := es.registry.Close()
	if err != nil {
		es.logger.Printf("Error closing repositories: %v", err)
		// Continue with cleanup despite errors
	}

	// Clean up all in-memory resources
	es.topicConfigs = make(map[string]outbound.TopicConfig)
	es.isInitialized = false

	es.logger.Printf("EventStore closed successfully")
	return err
}

// Health returns comprehensive health information about the EventStore and all its repositories
func (es *EventStore) Health(ctx context.Context) (map[string]interface{}, error) {
	es.mu.RLock()
	defer es.mu.RUnlock()

	health := map[string]interface{}{
		"status":       string(outbound.StatusUp),
		"initialized":  es.isInitialized,
		"topicCount":   len(es.topicConfigs),
		"configStream": es.configStream.Name,
		"timestamp":    time.Now().UnixNano(),
	}

	// If not initialized, return basic health
	if !es.isInitialized {
		health["status"] = string(outbound.StatusDown)
		health["reason"] = "not initialized"
		return health, ErrNotInitialized
	}

	// Get health from registry
	registryHealth, err := es.registry.Health(ctx)
	if err != nil {
		health["status"] = string(outbound.StatusDegraded)
		health["reason"] = fmt.Sprintf("registry health check failed: %v", err)
	}

	// Include registry health in the response
	health["registry"] = registryHealth

	// Determine overall status based on registry status
	if registryStatus, ok := registryHealth["status"].(string); ok {
		if registryStatus == string(outbound.StatusDown) {
			health["status"] = string(outbound.StatusDown)
			health["reason"] = "registry is down"
		} else if registryStatus == string(outbound.StatusDegraded) {
			health["status"] = string(outbound.StatusDegraded)
			health["reason"] = "registry is degraded"
		}
	}

	// Get detailed repository health information
	repoStats := make(map[string]int)
	repoStats["total"] = 0
	repoStats["up"] = 0
	repoStats["down"] = 0
	repoStats["degraded"] = 0

	if repos, ok := registryHealth["repositories"].(map[string]interface{}); ok {
		repoStats["total"] = len(repos)

		// Count repositories by status
		for _, repoHealth := range repos {
			if repoMap, ok := repoHealth.(map[string]interface{}); ok {
				if status, ok := repoMap["status"].(string); ok {
					switch status {
					case string(outbound.StatusUp):
						repoStats["up"]++
					case string(outbound.StatusDown):
						repoStats["down"]++
					case string(outbound.StatusDegraded):
						repoStats["degraded"]++
					}
				}
			}
		}

		// Determine overall health based on repository status counts
		if repoStats["total"] > 0 {
			if repoStats["down"] == repoStats["total"] {
				health["status"] = string(outbound.StatusDown)
				health["reason"] = "all repositories are down"
			} else if repoStats["down"] > 0 || repoStats["degraded"] > 0 {
				health["status"] = string(outbound.StatusDegraded)
				health["reason"] = fmt.Sprintf("%d down, %d degraded repositories",
					repoStats["down"], repoStats["degraded"])
			}
		}
	}

	health["repositoryStats"] = repoStats

	// Include topic distribution by adapter
	adapterStats := make(map[string]int)
	for _, config := range es.topicConfigs {
		adapterStats[config.Adapter]++
	}
	health["adapterStats"] = adapterStats

	return health, err
}

// CreateTopic creates a new topic with the specified configuration
func (es *EventStore) CreateTopic(ctx context.Context, config outbound.TopicConfig) error {
	es.mu.Lock()
	defer es.mu.Unlock()

	if !es.isInitialized {
		return ErrNotInitialized
	}

	// Validate topic configuration
	if err := validateTopicConfig(config); err != nil {
		es.logger.Printf("Invalid topic configuration: %v", err)
		return fmt.Errorf("%w: %v", ErrInvalidTopicConfig, err)
	}

	// Check if topic already exists in write model
	if _, exists := es.topicConfigs[config.Name]; exists {
		es.logger.Printf("Topic already exists: %s", config.Name)
		return fmt.Errorf("%w: %s", ErrTopicAlreadyExists, config.Name)
	}

	// Convert TopicConfig to EventStreamConfig
	streamConfig := models.FromTopicConfig(config)

	// Get or create repository for this stream
	repo, err := es.registry.GetOrCreate(ctx, streamConfig)
	if err != nil {
		es.logger.Printf("Failed to get repository for topic %s: %v", config.Name, err)
		return fmt.Errorf("failed to get repository for topic %s: %w", config.Name, err)
	}

	// Create the topic in the repository
	if err := repo.CreateTopic(ctx, config); err != nil {
		es.logger.Printf("Failed to create topic %s in repository: %v", config.Name, err)
		return fmt.Errorf("failed to create topic %s in repository: %w", config.Name, err)
	}

	// Get configuration repository
	configRepo, err := es.getConfigRepository(ctx)
	if err != nil {
		// Try to rollback the topic creation since we couldn't record it
		_ = repo.DeleteTopic(ctx, config.Name)
		return fmt.Errorf("failed to get config repository: %w", err)
	}

	// Create a TopicCreated event
	event := outbound.Event{
		Topic: es.configStream.Name,
		Type:  EventTypeTopicCreated,
		Data: map[string]interface{}{
			"name":       config.Name,
			"adapter":    config.Adapter,
			"connection": config.Connection,
			"options":    config.Options,
		},
		Metadata: map[string]interface{}{
			"source": "eventstore",
		},
	}

	// Append the event to the configuration topic
	if err := configRepo.AppendEvents(ctx, es.configStream.Name, []outbound.Event{event}); err != nil {
		// Try to rollback the topic creation
		_ = repo.DeleteTopic(ctx, config.Name)
		es.logger.Printf("Failed to append topic created event: %v", err)
		return fmt.Errorf("failed to append topic created event: %w", err)
	}

	// Update the write model
	es.topicConfigs[config.Name] = config
	es.logger.Printf("Topic created: %s", config.Name)

	return nil
}

// UpdateTopic updates an existing topic with the specified configuration
func (es *EventStore) UpdateTopic(ctx context.Context, config outbound.TopicConfig) error {
	es.mu.Lock()
	defer es.mu.Unlock()

	if !es.isInitialized {
		return ErrNotInitialized
	}

	// Validate topic configuration
	if err := validateTopicConfig(config); err != nil {
		es.logger.Printf("Invalid topic configuration: %v", err)
		return fmt.Errorf("%w: %v", ErrInvalidTopicConfig, err)
	}

	// Check if topic exists in write model
	originalConfig, exists := es.topicConfigs[config.Name]
	if !exists {
		es.logger.Printf("Topic not found: %s", config.Name)
		return fmt.Errorf("%w: %s", ErrTopicNotFound, config.Name)
	}

	// Check if adapter or connection has changed
	adapterOrConnectionChanged := originalConfig.Adapter != config.Adapter || originalConfig.Connection != config.Connection

	var repo outbound.EventRepository
	var err error

	if adapterOrConnectionChanged {
		// Get or create new repository if adapter or connection changed
		streamConfig := models.FromTopicConfig(config)
		repo, err = es.registry.GetOrCreate(ctx, streamConfig)
		if err != nil {
			es.logger.Printf("Failed to get repository for updated topic %s: %v", config.Name, err)
			return fmt.Errorf("failed to get repository for updated topic %s: %w", config.Name, err)
		}

		// Check if topic exists in the new repository
		exists, err := repo.TopicExists(ctx, config.Name)
		if err != nil {
			es.logger.Printf("Failed to check if topic exists in new repository: %v", err)
			return fmt.Errorf("failed to check if topic exists in new repository: %w", err)
		}

		// Create the topic in the new repository if it doesn't exist
		if !exists {
			if err := repo.CreateTopic(ctx, config); err != nil {
				es.logger.Printf("Failed to create topic in new repository: %v", err)
				return fmt.Errorf("failed to create topic in new repository: %w", err)
			}
		} else {
			// Update the topic in the new repository
			if err := repo.UpdateTopicConfig(ctx, config); err != nil {
				es.logger.Printf("Failed to update topic in new repository: %v", err)
				return fmt.Errorf("failed to update topic in new repository: %w", err)
			}
		}
	} else {
		// Get the existing repository for this topic
		repo, err = es.getRepositoryForTopic(ctx, config.Name)
		if err != nil {
			es.logger.Printf("Failed to get repository for topic %s: %v", config.Name, err)
			return fmt.Errorf("failed to get repository for topic %s: %w", config.Name, err)
		}

		// Update the topic in the repository
		if err := repo.UpdateTopicConfig(ctx, config); err != nil {
			es.logger.Printf("Failed to update topic %s in repository: %v", config.Name, err)
			return fmt.Errorf("failed to update topic %s in repository: %w", config.Name, err)
		}
	}

	// Get configuration repository
	configRepo, err := es.getConfigRepository(ctx)
	if err != nil {
		es.logger.Printf("Failed to get config repository: %v", err)
		return fmt.Errorf("failed to get config repository: %w", err)
	}

	// Create a TopicUpdated event
	event := outbound.Event{
		Topic: es.configStream.Name,
		Type:  EventTypeTopicUpdated,
		Data: map[string]interface{}{
			"name":       config.Name,
			"adapter":    config.Adapter,
			"connection": config.Connection,
			"options":    config.Options,
		},
		Metadata: map[string]interface{}{
			"source":              "eventstore",
			"previous_adapter":    originalConfig.Adapter,
			"previous_connection": originalConfig.Connection,
		},
	}

	// Append the event to the configuration topic
	if err := configRepo.AppendEvents(ctx, es.configStream.Name, []outbound.Event{event}); err != nil {
		es.logger.Printf("Failed to append topic updated event: %v", err)
		return fmt.Errorf("failed to append topic updated event: %w", err)
	}

	// Update the write model
	es.topicConfigs[config.Name] = config
	es.logger.Printf("Topic updated: %s", config.Name)

	return nil
}

// DeleteTopic deletes a topic with the specified name
func (es *EventStore) DeleteTopic(ctx context.Context, topicName string) error {
	es.mu.Lock()
	defer es.mu.Unlock()

	if !es.isInitialized {
		return ErrNotInitialized
	}

	// Check if topic exists in write model
	_, exists := es.topicConfigs[topicName]
	if !exists {
		es.logger.Printf("Topic not found: %s", topicName)
		return fmt.Errorf("%w: %s", ErrTopicNotFound, topicName)
	}

	// Get repository for this topic
	repo, err := es.getRepositoryForTopic(ctx, topicName)
	if err != nil {
		es.logger.Printf("Failed to get repository for topic %s: %v", topicName, err)
		return fmt.Errorf("failed to get repository for topic %s: %w", topicName, err)
	}

	// Delete the topic in the repository
	if err := repo.DeleteTopic(ctx, topicName); err != nil {
		es.logger.Printf("Failed to delete topic %s in repository: %v", topicName, err)
		return fmt.Errorf("failed to delete topic %s in repository: %w", topicName, err)
	}

	// Get configuration repository
	configRepo, err := es.getConfigRepository(ctx)
	if err != nil {
		es.logger.Printf("Failed to get config repository: %v", err)
		return fmt.Errorf("failed to get config repository: %w", err)
	}

	// Create a TopicDeleted event
	event := outbound.Event{
		Topic: es.configStream.Name,
		Type:  EventTypeTopicDeleted,
		Data: map[string]interface{}{
			"name": topicName,
		},
		Metadata: map[string]interface{}{
			"source": "eventstore",
		},
	}

	// Append the event to the configuration topic
	if err := configRepo.AppendEvents(ctx, es.configStream.Name, []outbound.Event{event}); err != nil {
		es.logger.Printf("Failed to append topic deleted event: %v", err)
		return fmt.Errorf("failed to append topic deleted event: %w", err)
	}

	// Update the write model
	delete(es.topicConfigs, topicName)
	es.logger.Printf("Topic deleted: %s", topicName)

	return nil
}

// GetTopic retrieves a topic configuration by name
func (es *EventStore) GetTopic(ctx context.Context, topicName string) (outbound.TopicConfig, error) {
	es.mu.RLock()
	defer es.mu.RUnlock()

	if !es.isInitialized {
		return outbound.TopicConfig{}, ErrNotInitialized
	}

	// Get topic from the write model
	config, exists := es.topicConfigs[topicName]
	if !exists {
		es.logger.Printf("Topic not found: %s", topicName)
		return outbound.TopicConfig{}, fmt.Errorf("%w: %s", ErrTopicNotFound, topicName)
	}

	return config, nil
}

// ListTopics returns a list of all topic configurations
func (es *EventStore) ListTopics(ctx context.Context) ([]outbound.TopicConfig, error) {
	es.mu.RLock()
	defer es.mu.RUnlock()

	if !es.isInitialized {
		return nil, ErrNotInitialized
	}

	// Convert map to slice
	topics := make([]outbound.TopicConfig, 0, len(es.topicConfigs))
	for _, config := range es.topicConfigs {
		topics = append(topics, config)
	}

	return topics, nil
}

// validateTopicConfig validates a topic configuration
func validateTopicConfig(config outbound.TopicConfig) error {
	if config.Name == "" {
		return fmt.Errorf("%w: name", ErrMissingConfigField)
	}
	if config.Adapter == "" {
		return fmt.Errorf("%w: adapter", ErrMissingConfigField)
	}
	if config.Connection == "" {
		return fmt.Errorf("%w: connection", ErrMissingConfigField)
	}
	return nil
}

// AppendEvents appends events to a topic
func (es *EventStore) AppendEvents(ctx context.Context, topicName string, events []outbound.Event) error {
	es.mu.RLock()
	registry := es.registry // Capture registry reference before releasing lock
	initialized := es.isInitialized
	es.mu.RUnlock()

	if !initialized {
		return ErrNotInitialized
	}

	// Validate input
	if len(events) == 0 {
		return nil // Nothing to append
	}

	// Get repository for this topic
	repo, err := es.getRepositoryForTopic(ctx, topicName)
	if err != nil {
		es.logger.Printf("Failed to get repository for topic %s: %v", topicName, err)
		return fmt.Errorf("failed to get repository for topic %s: %w", topicName, err)
	}

	// Set topic name on all events if not already set
	for i := range events {
		events[i].Topic = topicName
	}

	// Append events to the repository
	if err := repo.AppendEvents(ctx, topicName, events); err != nil {
		es.logger.Printf("Failed to append events to topic %s: %v", topicName, err)
		return fmt.Errorf("failed to append events to topic %s: %w", topicName, err)
	}

	// Broadcast events to subscribers asynchronously
	go func() {
		// Create a new context for event broadcasting
		broadcastCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		subscriberRegistry := registry.GetSubscriberRegistry()
		totalDelivered := 0

		// Broadcast each event to subscribers
		for _, event := range events {
			// Broadcast event to subscribers
			delivered := subscriberRegistry.BroadcastEvent(broadcastCtx, event)
			totalDelivered += delivered

			// Log broadcast results
			es.logger.Printf("Event %s of type %s on topic %s delivered to %d subscribers",
				event.ID, event.Type, event.Topic, delivered)
		}

		// Log overall delivery statistics
		es.logger.Printf("Broadcast completed for %d events on topic %s, total deliveries: %d",
			len(events), topicName, totalDelivered)
	}()

	return nil
}

// GetEvents retrieves events from a topic starting from the specified version
func (es *EventStore) GetEvents(ctx context.Context, topicName string, fromVersion int64) ([]outbound.Event, error) {
	es.mu.RLock()
	defer es.mu.RUnlock()

	if !es.isInitialized {
		return nil, ErrNotInitialized
	}

	// Get repository for this topic
	repo, err := es.getRepositoryForTopic(ctx, topicName)
	if err != nil {
		es.logger.Printf("Failed to get repository for topic %s: %v", topicName, err)
		return nil, fmt.Errorf("failed to get repository for topic %s: %w", topicName, err)
	}

	// Get events from the repository
	events, err := repo.GetEvents(ctx, topicName, fromVersion)
	if err != nil {
		es.logger.Printf("Failed to get events from topic %s: %v", topicName, err)
		return nil, fmt.Errorf("failed to get events from topic %s: %w", topicName, err)
	}

	return events, nil
}

// GetEventsByType retrieves events of a specific type from a topic starting from the specified version
func (es *EventStore) GetEventsByType(ctx context.Context, topicName string, eventType string, fromVersion int64) ([]outbound.Event, error) {
	es.mu.RLock()
	defer es.mu.RUnlock()

	if !es.isInitialized {
		return nil, ErrNotInitialized
	}

	// Validate event type
	if eventType == "" {
		return nil, fmt.Errorf("%w: event type is required", ErrInvalidEventData)
	}

	// Get repository for this topic
	repo, err := es.getRepositoryForTopic(ctx, topicName)
	if err != nil {
		es.logger.Printf("Failed to get repository for topic %s: %v", topicName, err)
		return nil, fmt.Errorf("failed to get repository for topic %s: %w", topicName, err)
	}

	// Get events by type from the repository
	events, err := repo.GetEventsByType(ctx, topicName, eventType, fromVersion)
	if err != nil {
		es.logger.Printf("Failed to get events of type %s from topic %s: %v", eventType, topicName, err)
		return nil, fmt.Errorf("failed to get events of type %s from topic %s: %w", eventType, topicName, err)
	}

	return events, nil
}

// GetLatestVersion retrieves the latest event version for a topic
func (es *EventStore) GetLatestVersion(ctx context.Context, topicName string) (int64, error) {
	es.mu.RLock()
	defer es.mu.RUnlock()

	if !es.isInitialized {
		return 0, ErrNotInitialized
	}

	// Get repository for this topic
	repo, err := es.getRepositoryForTopic(ctx, topicName)
	if err != nil {
		es.logger.Printf("Failed to get repository for topic %s: %v", topicName, err)
		return 0, fmt.Errorf("failed to get repository for topic %s: %w", topicName, err)
	}

	// Get latest version from the repository
	version, err := repo.GetLatestVersion(ctx, topicName)
	if err != nil {
		es.logger.Printf("Failed to get latest version for topic %s: %v", topicName, err)
		return 0, fmt.Errorf("failed to get latest version for topic %s: %w", topicName, err)
	}

	return version, nil
}

// --- Subscriber Management Methods ---

// RegisterSubscriber adds a new subscriber to the registry
func (es *EventStore) RegisterSubscriber(ctx context.Context, config outbound.SubscriberConfig) (outbound.Subscriber, error) {
	es.mu.RLock() // Use RLock for read-only checks
	initialized := es.isInitialized
	registry := es.registry
	es.mu.RUnlock()

	if !initialized {
		return nil, ErrNotInitialized
	}

	// Validate subscriber configuration
	if err := validateSubscriberConfig(config); err != nil {
		es.logger.Printf("Invalid subscriber configuration: %v", err)
		return nil, fmt.Errorf("invalid subscriber configuration: %w", err)
	}

	// Convert from outbound.SubscriberConfig to models.SubscriberConfig
	internalConfig := models.SubscriberConfigFromOutbound(config)

	// Get the subscriber registry
	subscriberRegistry := registry.GetSubscriberRegistry()

	// Use RegisterWithClientInfo to provide request context
	options := subscribers.RegisterOptions{
		RequestID: fmt.Sprintf("es-%d", time.Now().UnixNano()),
	}

	// Extract client info from context if available
	if ctxClientIP, ok := ctx.Value("client_ip").(string); ok {
		options.ClientIP = ctxClientIP
	}
	if ctxUserAgent, ok := ctx.Value("user_agent").(string); ok {
		options.UserAgent = ctxUserAgent
	}
	if ctxClientID, ok := ctx.Value("client_id").(string); ok {
		options.ClientID = ctxClientID
	}

	// Log registration attempt with details
	es.logger.Printf("Attempting to register subscriber with config: topics=%v, filter_types=%v, buffer_size=%d",
		config.Topics, config.Filter.EventTypes, config.BufferSize)

	// Check for expiration in context
	if expiration, ok := ctx.Value("expiration").(time.Time); ok && !expiration.IsZero() {
		options.Expiration = expiration
		es.logger.Printf("Subscriber registration with expiration: %s", expiration.Format(time.RFC3339))
	}

	subscriber, err := subscriberRegistry.RegisterWithClientInfo(ctx, internalConfig, options)
	if err != nil {
		es.logger.Printf("Failed to register subscriber: %v", err)
		return nil, fmt.Errorf("failed to register subscriber: %w", err)
	}

	// Log successful registration with complete details
	es.logger.Printf("Subscriber registered successfully: ID=%s, topics=%v, filter_types=%v, from_version=%d, buffer_size=%d",
		subscriber.GetID(), subscriber.GetTopics(), subscriber.GetFilter().EventTypes,
		subscriber.GetFilter().FromVersion, config.BufferSize)

	return subscriber, nil
}

// validateSubscriberConfig performs comprehensive validation on subscriber configuration
func validateSubscriberConfig(config outbound.SubscriberConfig) error {
	// Check required fields
	if len(config.Topics) == 0 {
		return errors.New("at least one topic is required")
	}

	// Validate ID format if provided
	if config.ID != "" {
		if _, err := uuid.Parse(config.ID); err != nil {
			return fmt.Errorf("invalid subscriber ID format (must be UUID): %w", err)
		}
	}

	// Validate buffer size
	if config.BufferSize < 0 {
		return errors.New("buffer size cannot be negative")
	}

	// Validate timeout configuration if specified
	if config.Timeout.InitialTimeout < 0 {
		return errors.New("initial timeout cannot be negative")
	}
	if config.Timeout.MaxTimeout < 0 {
		return errors.New("max timeout cannot be negative")
	}
	if config.Timeout.BackoffMultiplier <= 0 {
		return errors.New("backoff multiplier must be positive")
	}
	if config.Timeout.MaxRetries < 0 {
		return errors.New("max retries cannot be negative")
	}

	return nil
}

// DeregisterSubscriber removes a subscriber
func (es *EventStore) DeregisterSubscriber(ctx context.Context, subscriberID string, reason string) error {
	es.mu.RLock()
	initialized := es.isInitialized
	registry := es.registry
	es.mu.RUnlock()

	if !initialized {
		return ErrNotInitialized
	}

	// Get the subscriber registry
	subscriberRegistry := registry.GetSubscriberRegistry()

	// First get the subscriber to access its information for logging
	subscriber, err := subscriberRegistry.Get(subscriberID)
	if err != nil {
		es.logger.Printf("Failed to find subscriber %s for deregistration: %v", subscriberID, err)
		return fmt.Errorf("failed to find subscriber for deregistration: %w", err)
	}

	// Gather subscriber information for logging before deregistration
	subscriberTopics := subscriber.GetTopics()
	subscriberStats := subscriber.GetStats()
	state := subscriber.GetState()

	// Log the deregistration intent with comprehensive details
	if reason == "" {
		reason = "manual" // Default reason if none provided
	}

	es.logger.Printf("Deregistering subscriber %s, reason: %s, topics: %v, state: %s, events_delivered: %d, events_dropped: %d, errors: %d",
		subscriberID, reason, subscriberTopics, state, subscriberStats.EventsDelivered,
		subscriberStats.EventsDropped, subscriberStats.ErrorCount)

	// Add extra diagnostic information to the log if available
	if requestID, ok := ctx.Value("request_id").(string); ok && requestID != "" {
		es.logger.Printf("Deregistration request ID: %s for subscriber: %s", requestID, subscriberID)
	}

	// First pause subscriber to prevent receiving new events during deregistration
	if state == outbound.SubscriberStateActive {
		if pauseErr := subscriber.Pause(); pauseErr != nil {
			es.logger.Printf("Warning: Could not pause subscriber %s before deregistration: %v", subscriberID, pauseErr)
		} else {
			es.logger.Printf("Subscriber %s paused before deregistration", subscriberID)
		}
	}

	// Use standard Deregister method which internally uses "manual" as the reason
	err = subscriberRegistry.Deregister(subscriberID)
	if err != nil {
		es.logger.Printf("Failed to deregister subscriber %s: %v", subscriberID, err)
		return fmt.Errorf("failed to deregister subscriber: %w", err)
	}

	es.logger.Printf("Subscriber deregistered successfully: %s, had %d events delivered, %d dropped, %d errors",
		subscriberID, subscriberStats.EventsDelivered, subscriberStats.EventsDropped, subscriberStats.ErrorCount)
	return nil
}

// UpdateSubscriber allows modifying a subscriber
func (es *EventStore) UpdateSubscriber(ctx context.Context, subscriberID string, updateFn func(outbound.Subscriber) error) error {
	es.mu.RLock()
	initialized := es.isInitialized
	registry := es.registry
	es.mu.RUnlock()

	if !initialized {
		return ErrNotInitialized
	}

	// Get the subscriber registry
	subscriberRegistry := registry.GetSubscriberRegistry()

	// Update the subscriber using the provided function
	err := subscriberRegistry.Update(subscriberID, updateFn)
	if err != nil {
		es.logger.Printf("Failed to update subscriber %s: %v", subscriberID, err)
		return fmt.Errorf("failed to update subscriber: %w", err)
	}

	es.logger.Printf("Subscriber updated successfully: %s", subscriberID)
	return nil
}

// GetSubscriber retrieves a subscriber by ID
func (es *EventStore) GetSubscriber(ctx context.Context, subscriberID string) (outbound.Subscriber, error) {
	es.mu.RLock()
	initialized := es.isInitialized
	registry := es.registry
	es.mu.RUnlock()

	if !initialized {
		return nil, ErrNotInitialized
	}

	// Get the subscriber registry
	subscriberRegistry := registry.GetSubscriberRegistry()

	// Get the subscriber
	subscriber, err := subscriberRegistry.Get(subscriberID)
	if err != nil {
		es.logger.Printf("Failed to get subscriber %s: %v", subscriberID, err)
		return nil, fmt.Errorf("failed to get subscriber: %w", err)
	}

	return subscriber, nil
}

// ListSubscribers returns subscribers matching the filter
func (es *EventStore) ListSubscribers(ctx context.Context, topicFilter string) ([]outbound.Subscriber, error) {
	es.mu.RLock()
	initialized := es.isInitialized
	registry := es.registry
	es.mu.RUnlock()

	if !initialized {
		return nil, ErrNotInitialized
	}

	// Get the subscriber registry
	subscriberRegistry := registry.GetSubscriberRegistry()

	// Get subscribers matching the topic filter
	subscribers := subscriberRegistry.List(topicFilter)

	es.logger.Printf("Found %d subscribers for topic filter: %s", len(subscribers), topicFilter)
	return subscribers, nil
}

// --- End Subscriber Management Methods ---
