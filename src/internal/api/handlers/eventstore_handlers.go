package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"goeventsource/src/internal/eventstore"
	"goeventsource/src/internal/port/outbound"
)

// EventStoreHandlers contains handlers for EventStore API endpoints
type EventStoreHandlers struct {
	store eventstore.EventStoreInterface
}

// NewEventStoreHandlers creates a new instance of EventStoreHandlers
func NewEventStoreHandlers(store eventstore.EventStoreInterface) *EventStoreHandlers {
	return &EventStoreHandlers{
		store: store,
	}
}

// HealthCheckHandler returns the health information from the EventStore
func (h *EventStoreHandlers) HealthCheckHandler(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	health, err := h.store.Health(ctx)
	status := http.StatusOK
	responseStatus := "success"

	// If we have an error or degraded status, set appropriate HTTP status
	if err != nil {
		if statusVal, ok := health["status"].(string); ok {
			if statusVal == string(outbound.StatusDown) {
				status = http.StatusServiceUnavailable
				responseStatus = "error"
			} else if statusVal == string(outbound.StatusDegraded) {
				status = http.StatusOK // Still OK but with warning
				responseStatus = "warning"
			}
		} else {
			status = http.StatusInternalServerError
			responseStatus = "error"
		}
	}

	response := ResponseWrapper{
		Status:  responseStatus,
		Data:    health,
		Message: getHealthMessage(health),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(response)
}

// ListTopicsHandler returns a list of all topics in the EventStore
func (h *EventStoreHandlers) ListTopicsHandler(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	topics, err := h.store.ListTopics(ctx)
	if err != nil {
		handleError(w, err, "Failed to list topics")
		return
	}

	response := ResponseWrapper{
		Status: "success",
		Data: map[string]interface{}{
			"topics": topics,
			"count":  len(topics),
		},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// Extract a helper function to get the topic name from URL path
func extractTopicFromPath(path string) string {
	// Handle only versioned URLs like "/api/v1/topics/my-topic"
	parts := strings.Split(path, "/")
	if len(parts) >= 5 && parts[2] == "v1" && parts[3] == "topics" {
		return parts[4]
	}

	return ""
}

// GetTopicHandler returns details of a specific topic
func (h *EventStoreHandlers) GetTopicHandler(w http.ResponseWriter, r *http.Request) {
	// Extract topic name from URL path
	topicName := extractTopicFromPath(r.URL.Path)
	if topicName == "" {
		handleError(w, fmt.Errorf("topic name is required"), "Missing topic name")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	topic, err := h.store.GetTopic(ctx, topicName)
	if err != nil {
		if err == eventstore.ErrTopicNotFound {
			handleError(w, err, "Topic not found", http.StatusNotFound)
			return
		}
		handleError(w, err, "Failed to get topic")
		return
	}

	response := ResponseWrapper{
		Status: "success",
		Data:   topic,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// CreateTopicHandler creates a new topic in the EventStore
func (h *EventStoreHandlers) CreateTopicHandler(w http.ResponseWriter, r *http.Request) {
	// Only accept POST requests
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var config outbound.TopicConfig
	if err := json.NewDecoder(r.Body).Decode(&config); err != nil {
		handleError(w, err, "Invalid request body")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	if err := h.store.CreateTopic(ctx, config); err != nil {
		if err == eventstore.ErrTopicAlreadyExists {
			handleError(w, err, fmt.Sprintf("Topic '%s' already exists", config.Name), http.StatusConflict)
			return
		}
		handleError(w, err, "Failed to create topic")
		return
	}

	response := ResponseWrapper{
		Status:  "success",
		Message: fmt.Sprintf("Topic '%s' created successfully", config.Name),
		Data:    config,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

// UpdateTopicHandler updates an existing topic in the EventStore
func (h *EventStoreHandlers) UpdateTopicHandler(w http.ResponseWriter, r *http.Request) {
	// Only accept PUT requests
	if r.Method != http.MethodPut {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// Extract topic name from URL path
	topicName := extractTopicFromPath(r.URL.Path)
	if topicName == "" {
		handleError(w, fmt.Errorf("topic name is required"), "Missing topic name")
		return
	}

	var config outbound.TopicConfig
	if err := json.NewDecoder(r.Body).Decode(&config); err != nil {
		handleError(w, err, "Invalid request body")
		return
	}

	// Ensure the topic name in the body matches the URL
	if config.Name != topicName {
		handleError(w, fmt.Errorf("topic name mismatch"), "Topic name in body does not match URL", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	if err := h.store.UpdateTopic(ctx, config); err != nil {
		if err == eventstore.ErrTopicNotFound {
			handleError(w, err, fmt.Sprintf("Topic '%s' not found", config.Name), http.StatusNotFound)
			return
		}
		handleError(w, err, "Failed to update topic")
		return
	}

	response := ResponseWrapper{
		Status:  "success",
		Message: fmt.Sprintf("Topic '%s' updated successfully", config.Name),
		Data:    config,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// DeleteTopicHandler deletes a topic from the EventStore
func (h *EventStoreHandlers) DeleteTopicHandler(w http.ResponseWriter, r *http.Request) {
	// Only accept DELETE requests
	if r.Method != http.MethodDelete {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// Extract topic name from URL path
	topicName := extractTopicFromPath(r.URL.Path)
	if topicName == "" {
		handleError(w, fmt.Errorf("topic name is required"), "Missing topic name")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	if err := h.store.DeleteTopic(ctx, topicName); err != nil {
		if err == eventstore.ErrTopicNotFound {
			handleError(w, err, fmt.Sprintf("Topic '%s' not found", topicName), http.StatusNotFound)
			return
		}
		handleError(w, err, "Failed to delete topic")
		return
	}

	response := ResponseWrapper{
		Status:  "success",
		Message: fmt.Sprintf("Topic '%s' deleted successfully", topicName),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// AppendEventsHandler appends events to a topic
func (h *EventStoreHandlers) AppendEventsHandler(w http.ResponseWriter, r *http.Request) {
	// Only accept POST requests
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// Extract topic name from URL path
	topicName := extractTopicFromPath(r.URL.Path)
	if topicName == "" {
		handleError(w, fmt.Errorf("topic name is required"), "Missing topic name")
		return
	}

	var events []outbound.Event
	if err := json.NewDecoder(r.Body).Decode(&events); err != nil {
		handleError(w, err, "Invalid request body")
		return
	}

	// Ensure each event has the correct topic name
	for i := range events {
		events[i].Topic = topicName
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	if err := h.store.AppendEvents(ctx, topicName, events); err != nil {
		if err == eventstore.ErrTopicNotFound {
			handleError(w, err, fmt.Sprintf("Topic '%s' not found", topicName), http.StatusNotFound)
			return
		}
		handleError(w, err, "Failed to append events")
		return
	}

	response := ResponseWrapper{
		Status:  "success",
		Message: fmt.Sprintf("%d events appended to topic '%s'", len(events), topicName),
		Data: map[string]interface{}{
			"eventCount": len(events),
			"topic":      topicName,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

// GetEventsHandler retrieves events from a topic
func (h *EventStoreHandlers) GetEventsHandler(w http.ResponseWriter, r *http.Request) {
	// Extract topic name from URL path
	topicName := extractTopicFromPath(r.URL.Path)
	if topicName == "" {
		handleError(w, fmt.Errorf("topic name is required"), "Missing topic name")
		return
	}

	// Parse fromVersion query parameter
	fromVersion := int64(0) // Default to 0
	if fromVersionStr := r.URL.Query().Get("fromVersion"); fromVersionStr != "" {
		if _, err := fmt.Sscanf(fromVersionStr, "%d", &fromVersion); err != nil {
			handleError(w, err, "Invalid fromVersion parameter")
			return
		}
	}

	// Parse eventType query parameter
	eventType := r.URL.Query().Get("type")

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	var events []outbound.Event
	var err error

	// If eventType is specified, use GetEventsByType
	if eventType != "" {
		events, err = h.store.GetEventsByType(ctx, topicName, eventType, fromVersion)
	} else {
		events, err = h.store.GetEvents(ctx, topicName, fromVersion)
	}

	if err != nil {
		if err == eventstore.ErrTopicNotFound {
			handleError(w, err, fmt.Sprintf("Topic '%s' not found", topicName), http.StatusNotFound)
			return
		}
		handleError(w, err, "Failed to get events")
		return
	}

	response := ResponseWrapper{
		Status: "success",
		Data: map[string]interface{}{
			"events":      events,
			"count":       len(events),
			"topic":       topicName,
			"fromVersion": fromVersion,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// GetLatestVersionHandler returns the latest event version for a topic
func (h *EventStoreHandlers) GetLatestVersionHandler(w http.ResponseWriter, r *http.Request) {
	// Extract topic name from URL path
	topicName := extractTopicFromPath(r.URL.Path)
	if topicName == "" {
		handleError(w, fmt.Errorf("topic name is required"), "Missing topic name")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	version, err := h.store.GetLatestVersion(ctx, topicName)
	if err != nil {
		if err == eventstore.ErrTopicNotFound {
			handleError(w, err, fmt.Sprintf("Topic '%s' not found", topicName), http.StatusNotFound)
			return
		}
		handleError(w, err, "Failed to get latest version")
		return
	}

	response := ResponseWrapper{
		Status: "success",
		Data: map[string]interface{}{
			"topic":         topicName,
			"latestVersion": version,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// handleError writes an error response to the response writer
func handleError(w http.ResponseWriter, err error, message string, statusCode ...int) {
	code := http.StatusInternalServerError
	if len(statusCode) > 0 {
		code = statusCode[0]
	}

	response := ResponseWrapper{
		Status:  "error",
		Message: message,
		Data: map[string]string{
			"error": err.Error(),
		},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(response)
}

// getHealthMessage returns a human-readable message based on health data
func getHealthMessage(health map[string]interface{}) string {
	if statusVal, ok := health["status"].(string); ok {
		switch statusVal {
		case string(outbound.StatusUp):
			return "Event store is healthy"
		case string(outbound.StatusDegraded):
			if reason, ok := health["reason"].(string); ok {
				return fmt.Sprintf("Event store is degraded: %s", reason)
			}
			return "Event store is degraded"
		case string(outbound.StatusDown):
			if reason, ok := health["reason"].(string); ok {
				return fmt.Sprintf("Event store is down: %s", reason)
			}
			return "Event store is down"
		}
	}
	return "Event store health status is unknown"
}
