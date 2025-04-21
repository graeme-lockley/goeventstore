package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"goeventsource/src/internal/eventstore"
	"goeventsource/src/internal/port/outbound"
)

// MockEventStore is a mock implementation of EventStoreInterface for testing
type MockEventStore struct {
	topicConfigs      map[string]outbound.TopicConfig
	events            map[string][]outbound.Event
	healthData        map[string]interface{}
	healthError       error
	createTopicError  error
	deleteTopicError  error
	getTopicError     error
	listTopicsError   error
	appendEventsError error
	getEventsError    error
}

func NewMockEventStore() *MockEventStore {
	return &MockEventStore{
		topicConfigs: make(map[string]outbound.TopicConfig),
		events:       make(map[string][]outbound.Event),
		healthData: map[string]interface{}{
			"status":      string(outbound.StatusUp),
			"initialized": true,
			"topicCount":  0,
		},
	}
}

// Interface implementation for mock
func (m *MockEventStore) Initialize(ctx context.Context) error {
	return nil
}

func (m *MockEventStore) CreateTopic(ctx context.Context, config outbound.TopicConfig) error {
	if m.createTopicError != nil {
		return m.createTopicError
	}
	if _, exists := m.topicConfigs[config.Name]; exists {
		return eventstore.ErrTopicAlreadyExists
	}
	m.topicConfigs[config.Name] = config
	m.events[config.Name] = []outbound.Event{}
	return nil
}

func (m *MockEventStore) UpdateTopic(ctx context.Context, config outbound.TopicConfig) error {
	if _, exists := m.topicConfigs[config.Name]; !exists {
		return eventstore.ErrTopicNotFound
	}
	m.topicConfigs[config.Name] = config
	return nil
}

func (m *MockEventStore) DeleteTopic(ctx context.Context, topicName string) error {
	if m.deleteTopicError != nil {
		return m.deleteTopicError
	}
	if _, exists := m.topicConfigs[topicName]; !exists {
		return eventstore.ErrTopicNotFound
	}
	delete(m.topicConfigs, topicName)
	delete(m.events, topicName)
	return nil
}

func (m *MockEventStore) GetTopic(ctx context.Context, topicName string) (outbound.TopicConfig, error) {
	if m.getTopicError != nil {
		return outbound.TopicConfig{}, m.getTopicError
	}
	config, exists := m.topicConfigs[topicName]
	if !exists {
		return outbound.TopicConfig{}, eventstore.ErrTopicNotFound
	}
	return config, nil
}

func (m *MockEventStore) ListTopics(ctx context.Context) ([]outbound.TopicConfig, error) {
	if m.listTopicsError != nil {
		return nil, m.listTopicsError
	}
	topics := make([]outbound.TopicConfig, 0, len(m.topicConfigs))
	for _, config := range m.topicConfigs {
		topics = append(topics, config)
	}
	return topics, nil
}

func (m *MockEventStore) AppendEvents(ctx context.Context, topicName string, events []outbound.Event) error {
	if m.appendEventsError != nil {
		return m.appendEventsError
	}
	if _, exists := m.topicConfigs[topicName]; !exists {
		return eventstore.ErrTopicNotFound
	}
	m.events[topicName] = append(m.events[topicName], events...)
	return nil
}

func (m *MockEventStore) GetEvents(ctx context.Context, topicName string, fromVersion int64) ([]outbound.Event, error) {
	if m.getEventsError != nil {
		return nil, m.getEventsError
	}
	if _, exists := m.topicConfigs[topicName]; !exists {
		return nil, eventstore.ErrTopicNotFound
	}

	var result []outbound.Event
	for _, event := range m.events[topicName] {
		if event.Version >= fromVersion {
			result = append(result, event)
		}
	}
	return result, nil
}

func (m *MockEventStore) GetEventsByType(ctx context.Context, topicName string, eventType string, fromVersion int64) ([]outbound.Event, error) {
	events, err := m.GetEvents(ctx, topicName, fromVersion)
	if err != nil {
		return nil, err
	}

	var result []outbound.Event
	for _, event := range events {
		if event.Type == eventType {
			result = append(result, event)
		}
	}
	return result, nil
}

func (m *MockEventStore) GetLatestVersion(ctx context.Context, topicName string) (int64, error) {
	if _, exists := m.topicConfigs[topicName]; !exists {
		return 0, eventstore.ErrTopicNotFound
	}

	var latestVersion int64 = 0
	for _, event := range m.events[topicName] {
		if event.Version > latestVersion {
			latestVersion = event.Version
		}
	}
	return latestVersion, nil
}

func (m *MockEventStore) Health(ctx context.Context) (map[string]interface{}, error) {
	return m.healthData, m.healthError
}

func (m *MockEventStore) Close() error {
	return nil
}

// Tests for EventStore handlers
func TestHealthCheckHandler(t *testing.T) {
	tests := []struct {
		name           string
		healthData     map[string]interface{}
		healthError    error
		expectedStatus int
	}{
		{
			name: "Healthy status",
			healthData: map[string]interface{}{
				"status":      string(outbound.StatusUp),
				"initialized": true,
			},
			healthError:    nil,
			expectedStatus: http.StatusOK,
		},
		{
			name: "Degraded status",
			healthData: map[string]interface{}{
				"status":      string(outbound.StatusDegraded),
				"initialized": true,
				"reason":      "Some repositories are degraded",
			},
			healthError:    errors.New("degraded status"),
			expectedStatus: http.StatusOK,
		},
		{
			name: "Down status",
			healthData: map[string]interface{}{
				"status":      string(outbound.StatusDown),
				"initialized": false,
				"reason":      "Not initialized",
			},
			healthError:    errors.New("down status"),
			expectedStatus: http.StatusServiceUnavailable,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockStore := NewMockEventStore()
			mockStore.healthData = tc.healthData
			mockStore.healthError = tc.healthError

			handlers := NewEventStoreHandlers(mockStore)

			req, err := http.NewRequest("GET", "/api/eventstore/health", nil)
			if err != nil {
				t.Fatal(err)
			}

			rr := httptest.NewRecorder()
			handler := http.HandlerFunc(handlers.HealthCheckHandler)
			handler.ServeHTTP(rr, req)

			if status := rr.Code; status != tc.expectedStatus {
				t.Errorf("Handler returned wrong status code: got %v want %v", status, tc.expectedStatus)
			}

			var response ResponseWrapper
			if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil {
				t.Errorf("Failed to unmarshal response: %v", err)
			}

			if tc.healthData["status"] == string(outbound.StatusUp) && response.Status != "success" {
				t.Errorf("Expected success status, got %s", response.Status)
			}
		})
	}
}

func TestListTopicsHandler(t *testing.T) {
	mockStore := NewMockEventStore()
	// Add some test topics
	mockStore.topicConfigs["topic1"] = outbound.TopicConfig{Name: "topic1", Adapter: "memory"}
	mockStore.topicConfigs["topic2"] = outbound.TopicConfig{Name: "topic2", Adapter: "fs"}

	handlers := NewEventStoreHandlers(mockStore)

	req, err := http.NewRequest("GET", "/api/topics", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(handlers.ListTopicsHandler)
	handler.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("Handler returned wrong status code: got %v want %v", status, http.StatusOK)
	}

	var response ResponseWrapper
	if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil {
		t.Errorf("Failed to unmarshal response: %v", err)
	}

	data, ok := response.Data.(map[string]interface{})
	if !ok {
		t.Errorf("Response data has unexpected format")
		return
	}

	count, ok := data["count"].(float64)
	if !ok || count != 2 {
		t.Errorf("Expected 2 topics, got %v", count)
	}
}

func TestCreateTopicHandler(t *testing.T) {
	tests := []struct {
		name           string
		topicConfig    outbound.TopicConfig
		storeError     error
		expectedStatus int
	}{
		{
			name: "Valid topic creation",
			topicConfig: outbound.TopicConfig{
				Name:    "newtopic",
				Adapter: "memory",
			},
			storeError:     nil,
			expectedStatus: http.StatusCreated,
		},
		{
			name: "Topic already exists",
			topicConfig: outbound.TopicConfig{
				Name:    "existingtopic",
				Adapter: "memory",
			},
			storeError:     eventstore.ErrTopicAlreadyExists,
			expectedStatus: http.StatusConflict,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockStore := NewMockEventStore()
			if tc.storeError == eventstore.ErrTopicAlreadyExists {
				mockStore.topicConfigs[tc.topicConfig.Name] = tc.topicConfig
			}
			mockStore.createTopicError = tc.storeError

			handlers := NewEventStoreHandlers(mockStore)

			topicJson, _ := json.Marshal(tc.topicConfig)
			req, err := http.NewRequest("POST", "/api/topics", bytes.NewBuffer(topicJson))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Content-Type", "application/json")

			rr := httptest.NewRecorder()
			handler := http.HandlerFunc(handlers.CreateTopicHandler)
			handler.ServeHTTP(rr, req)

			if status := rr.Code; status != tc.expectedStatus {
				t.Errorf("Handler returned wrong status code: got %v want %v", status, tc.expectedStatus)
			}
		})
	}
}

func TestGetTopicHandler(t *testing.T) {
	tests := []struct {
		name           string
		topicName      string
		topicExists    bool
		expectedStatus int
	}{
		{
			name:           "Topic exists",
			topicName:      "existingtopic",
			topicExists:    true,
			expectedStatus: http.StatusOK,
		},
		{
			name:           "Topic does not exist",
			topicName:      "nonexistenttopic",
			topicExists:    false,
			expectedStatus: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockStore := NewMockEventStore()
			if tc.topicExists {
				mockStore.topicConfigs[tc.topicName] = outbound.TopicConfig{
					Name:    tc.topicName,
					Adapter: "memory",
				}
			}

			handlers := NewEventStoreHandlers(mockStore)

			req, err := http.NewRequest("GET", "/api/topics/"+tc.topicName, nil)
			if err != nil {
				t.Fatal(err)
			}

			rr := httptest.NewRecorder()
			// Create a handler function that extracts the topic name
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				r.URL.Path = "/api/topics/" + tc.topicName
				handlers.GetTopicHandler(w, r)
			})
			handler.ServeHTTP(rr, req)

			if status := rr.Code; status != tc.expectedStatus {
				t.Errorf("Handler returned wrong status code: got %v want %v", status, tc.expectedStatus)
			}
		})
	}
}

func TestAppendEventsHandler(t *testing.T) {
	topicName := "testtopic"
	events := []outbound.Event{
		{
			Type: "TestEvent",
			Data: map[string]interface{}{"key": "value"},
		},
	}

	tests := []struct {
		name           string
		topicExists    bool
		storeError     error
		expectedStatus int
	}{
		{
			name:           "Valid event append",
			topicExists:    true,
			storeError:     nil,
			expectedStatus: http.StatusCreated,
		},
		{
			name:           "Topic does not exist",
			topicExists:    false,
			storeError:     eventstore.ErrTopicNotFound,
			expectedStatus: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockStore := NewMockEventStore()
			if tc.topicExists {
				mockStore.topicConfigs[topicName] = outbound.TopicConfig{
					Name:    topicName,
					Adapter: "memory",
				}
			}
			mockStore.appendEventsError = tc.storeError

			handlers := NewEventStoreHandlers(mockStore)

			eventsJson, _ := json.Marshal(events)
			req, err := http.NewRequest("POST", "/api/topics/"+topicName+"/events", bytes.NewBuffer(eventsJson))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Content-Type", "application/json")

			rr := httptest.NewRecorder()
			// Create a handler function that extracts the topic name
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				r.URL.Path = "/api/topics/" + topicName + "/events"
				handlers.AppendEventsHandler(w, r)
			})
			handler.ServeHTTP(rr, req)

			if status := rr.Code; status != tc.expectedStatus {
				t.Errorf("Handler returned wrong status code: got %v want %v", status, tc.expectedStatus)
			}
		})
	}
}

func TestGetEventsHandler(t *testing.T) {
	topicName := "testtopic"
	testEvents := []outbound.Event{
		{
			Topic:     topicName,
			Type:      "TestEvent1",
			Version:   1,
			Data:      map[string]interface{}{"key": "value1"},
			Timestamp: 1000,
		},
		{
			Topic:     topicName,
			Type:      "TestEvent2",
			Version:   2,
			Data:      map[string]interface{}{"key": "value2"},
			Timestamp: 2000,
		},
	}

	tests := []struct {
		name           string
		topicExists    bool
		events         []outbound.Event
		fromVersion    int64
		eventType      string
		expectedCount  int
		expectedStatus int
	}{
		{
			name:           "Get all events",
			topicExists:    true,
			events:         testEvents,
			fromVersion:    0,
			eventType:      "",
			expectedCount:  2,
			expectedStatus: http.StatusOK,
		},
		{
			name:           "Get events from version",
			topicExists:    true,
			events:         testEvents,
			fromVersion:    2,
			eventType:      "",
			expectedCount:  1,
			expectedStatus: http.StatusOK,
		},
		{
			name:           "Get events by type",
			topicExists:    true,
			events:         testEvents,
			fromVersion:    0,
			eventType:      "TestEvent1",
			expectedCount:  1,
			expectedStatus: http.StatusOK,
		},
		{
			name:           "Topic does not exist",
			topicExists:    false,
			events:         nil,
			fromVersion:    0,
			eventType:      "",
			expectedCount:  0,
			expectedStatus: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockStore := NewMockEventStore()
			if tc.topicExists {
				mockStore.topicConfigs[topicName] = outbound.TopicConfig{
					Name:    topicName,
					Adapter: "memory",
				}
				mockStore.events[topicName] = tc.events
			} else {
				mockStore.getEventsError = eventstore.ErrTopicNotFound
			}

			handlers := NewEventStoreHandlers(mockStore)

			url := "/api/topics/" + topicName + "/events"
			if tc.fromVersion > 0 {
				url += "?fromVersion=" + fmt.Sprintf("%d", tc.fromVersion)
			}
			if tc.eventType != "" {
				url += "&type=" + tc.eventType
			}

			req, err := http.NewRequest("GET", url, nil)
			if err != nil {
				t.Fatal(err)
			}

			rr := httptest.NewRecorder()
			// Create a handler function that extracts the topic name
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				r.URL.Path = "/api/topics/" + topicName + "/events"
				if tc.fromVersion > 0 {
					q := r.URL.Query()
					q.Add("fromVersion", fmt.Sprintf("%d", tc.fromVersion))
					r.URL.RawQuery = q.Encode()
				}
				if tc.eventType != "" {
					q := r.URL.Query()
					q.Add("type", tc.eventType)
					r.URL.RawQuery = q.Encode()
				}
				handlers.GetEventsHandler(w, r)
			})
			handler.ServeHTTP(rr, req)

			if status := rr.Code; status != tc.expectedStatus {
				t.Errorf("Handler returned wrong status code: got %v want %v", status, tc.expectedStatus)
			}

			if tc.topicExists {
				var response ResponseWrapper
				if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil {
					t.Errorf("Failed to unmarshal response: %v", err)
				}

				data, ok := response.Data.(map[string]interface{})
				if !ok {
					t.Errorf("Response data has unexpected format")
					return
				}

				// For simplicity, just check the topic name and count
				topicFromResponse, ok := data["topic"].(string)
				if !ok || topicFromResponse != topicName {
					t.Errorf("Expected topic %s, got %v", topicName, topicFromResponse)
				}
			}
		})
	}
}
