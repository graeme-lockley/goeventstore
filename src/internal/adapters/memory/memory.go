package memory

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"goeventsource/src/internal/port/outbound"

	"github.com/google/uuid"
)

// MemoryEventRepository implements the EventRepository interface using in-memory storage
type MemoryEventRepository struct {
	mu            sync.RWMutex
	events        map[string][]outbound.Event // topic -> ordered events
	topicConfigs  map[string]outbound.TopicConfig
	latestVersion map[string]int64 // topic -> latest version
	state         outbound.RepositoryState
}

// NewMemoryEventRepository creates a new in-memory event repository
func NewMemoryEventRepository() *MemoryEventRepository {
	return &MemoryEventRepository{
		events:        make(map[string][]outbound.Event),
		topicConfigs:  make(map[string]outbound.TopicConfig),
		latestVersion: make(map[string]int64),
		state:         outbound.StateUninitialized,
	}
}

// Initialize prepares the repository for use
func (m *MemoryEventRepository) Initialize(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Only initialize if not already in ready state
	if m.state == outbound.StateReady {
		return nil
	}

	// Mark as initializing
	m.state = outbound.StateInitializing

	// Clear existing data if any
	m.events = make(map[string][]outbound.Event)
	m.topicConfigs = make(map[string]outbound.TopicConfig)
	m.latestVersion = make(map[string]int64)

	// Mark as ready
	m.state = outbound.StateReady
	return nil
}

// AppendEvents adds new events to the store for a specific topic
func (m *MemoryEventRepository) AppendEvents(ctx context.Context, topic string, events []outbound.Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check repository state
	if m.state != outbound.StateReady {
		return errors.New("repository not in ready state")
	}

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

// GetEvents retrieves events for a topic, starting from a specific version
func (m *MemoryEventRepository) GetEvents(ctx context.Context, topic string, fromVersion int64) ([]outbound.Event, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Check repository state
	if m.state != outbound.StateReady {
		return nil, errors.New("repository not in ready state")
	}

	// Verify topic exists
	topicEvents, exists := m.events[topic]
	if !exists {
		return nil, errors.New("topic not found")
	}

	// Filter events by version
	var result []outbound.Event
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

// GetEventsByType retrieves events of a specific type for a topic
func (m *MemoryEventRepository) GetEventsByType(ctx context.Context, topic string, eventType string, fromVersion int64) ([]outbound.Event, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Check repository state
	if m.state != outbound.StateReady {
		return nil, errors.New("repository not in ready state")
	}

	// Verify topic exists
	topicEvents, exists := m.events[topic]
	if !exists {
		return nil, errors.New("topic not found")
	}

	// Filter events by version and type
	var result []outbound.Event
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

// GetLatestVersion returns the latest event version for a topic
func (m *MemoryEventRepository) GetLatestVersion(ctx context.Context, topic string) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Check repository state
	if m.state != outbound.StateReady {
		return 0, errors.New("repository not in ready state")
	}

	// Verify topic exists
	if _, exists := m.events[topic]; !exists {
		return 0, errors.New("topic not found")
	}

	return m.latestVersion[topic], nil
}

// CreateTopic creates a new topic
func (m *MemoryEventRepository) CreateTopic(ctx context.Context, config outbound.TopicConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check repository state
	if m.state != outbound.StateReady {
		return errors.New("repository not in ready state")
	}

	// Verify topic doesn't already exist
	if _, exists := m.events[config.Name]; exists {
		return errors.New("topic already exists")
	}

	// Initialize topic
	m.events[config.Name] = []outbound.Event{}
	m.topicConfigs[config.Name] = config
	m.latestVersion[config.Name] = 0

	return nil
}

// DeleteTopic deletes a topic
func (m *MemoryEventRepository) DeleteTopic(ctx context.Context, topic string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check repository state
	if m.state != outbound.StateReady {
		return errors.New("repository not in ready state")
	}

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

// ListTopics returns a list of all topics
func (m *MemoryEventRepository) ListTopics(ctx context.Context) ([]outbound.TopicConfig, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Check repository state
	if m.state != outbound.StateReady {
		return nil, errors.New("repository not in ready state")
	}

	topics := make([]outbound.TopicConfig, 0, len(m.topicConfigs))
	for _, config := range m.topicConfigs {
		topics = append(topics, config)
	}

	return topics, nil
}

// TopicExists checks if a topic exists
func (m *MemoryEventRepository) TopicExists(ctx context.Context, topic string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Check repository state
	if m.state != outbound.StateReady {
		return false, errors.New("repository not in ready state")
	}

	_, exists := m.events[topic]
	return exists, nil
}

// UpdateTopicConfig updates a topic's configuration
func (m *MemoryEventRepository) UpdateTopicConfig(ctx context.Context, config outbound.TopicConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check repository state
	if m.state != outbound.StateReady {
		return errors.New("repository not in ready state")
	}

	// Verify topic exists
	if _, exists := m.events[config.Name]; !exists {
		return errors.New("topic not found")
	}

	// Update config
	m.topicConfigs[config.Name] = config

	return nil
}

// GetTopicConfig gets a topic's configuration
func (m *MemoryEventRepository) GetTopicConfig(ctx context.Context, topic string) (outbound.TopicConfig, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Check repository state
	if m.state != outbound.StateReady {
		return outbound.TopicConfig{}, errors.New("repository not in ready state")
	}

	// Verify topic exists
	config, exists := m.topicConfigs[topic]
	if !exists {
		return outbound.TopicConfig{}, errors.New("topic not found")
	}

	return config, nil
}

// Close cleans up resources
func (m *MemoryEventRepository) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Only close if in ready state
	if m.state != outbound.StateReady {
		return nil
	}

	// Mark as closing
	m.state = outbound.StateClosing

	// Clear data to free memory
	m.events = nil
	m.topicConfigs = nil
	m.latestVersion = nil

	// Mark as closed
	m.state = outbound.StateClosed
	return nil
}

// GetState returns the current state of the repository
func (m *MemoryEventRepository) GetState() outbound.RepositoryState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state
}

// Reopen reopens a closed repository
func (m *MemoryEventRepository) Reopen(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Only reopen if closed
	if m.state != outbound.StateClosed {
		return errors.New("repository is not in closed state")
	}

	// Recreate data structures
	m.events = make(map[string][]outbound.Event)
	m.topicConfigs = make(map[string]outbound.TopicConfig)
	m.latestVersion = make(map[string]int64)

	// Mark as ready
	m.state = outbound.StateReady
	return nil
}

// Reset clears all data and reinitializes the repository
func (m *MemoryEventRepository) Reset(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Mark as initializing
	m.state = outbound.StateInitializing

	// Clear all data
	m.events = make(map[string][]outbound.Event)
	m.topicConfigs = make(map[string]outbound.TopicConfig)
	m.latestVersion = make(map[string]int64)

	// Mark as ready
	m.state = outbound.StateReady
	return nil
}

// Health returns health information
func (m *MemoryEventRepository) Health(ctx context.Context) (map[string]interface{}, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	health := map[string]interface{}{
		"status":     string(outbound.StatusUp),
		"state":      string(m.state),
		"type":       "memory",
		"topicCount": len(m.topicConfigs),
	}

	// Add event counts
	topicStats := make(map[string]int)
	for topic, events := range m.events {
		topicStats[topic] = len(events)
	}
	health["eventCounts"] = topicStats

	// If not in ready state, mark as down
	if m.state != outbound.StateReady {
		health["status"] = string(outbound.StatusDown)
		health["message"] = "Repository is not in ready state"
	}

	return health, nil
}

// HealthInfo returns structured health information
func (m *MemoryEventRepository) HealthInfo(ctx context.Context) (outbound.HealthInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Default to up status
	status := outbound.StatusUp
	var message string

	// Check state
	if m.state != outbound.StateReady {
		status = outbound.StatusDown
		message = "Repository is not in ready state"
	}

	// Prepare additional info
	additionalInfo := map[string]interface{}{
		"type":       "memory",
		"topicCount": len(m.topicConfigs),
	}

	// Add event counts
	topicStats := make(map[string]int)
	for topic, events := range m.events {
		topicStats[topic] = len(events)
	}
	additionalInfo["eventCounts"] = topicStats

	return outbound.HealthInfo{
		Status:         status,
		State:          m.state,
		Message:        message,
		AdditionalInfo: additionalInfo,
	}, nil
}
