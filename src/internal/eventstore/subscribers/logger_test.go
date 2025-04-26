package subscribers

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"strings"
	"testing"
	"time"
)

func TestSubscriberLogger_LogLevels(t *testing.T) {
	// Set up a buffer to capture log output
	var buf bytes.Buffer
	customLogger := log.New(&buf, "", 0)

	// Create a test context with request_id
	ctx := context.WithValue(context.Background(), "request_id", "test-request-id")

	// Create different level loggers
	debugLogger := &SubscriberLogger{
		logger:   customLogger,
		minLevel: DEBUG,
		prefix:   "[TEST] ",
	}

	infoLogger := &SubscriberLogger{
		logger:   customLogger,
		minLevel: INFO,
		prefix:   "[TEST] ",
	}

	// Test debug level
	debugLogger.Debug(ctx, "Debug message", map[string]interface{}{"key": "value"})
	output := buf.String()

	// Verify log entry
	if !strings.Contains(output, "DEBUG") {
		t.Errorf("Expected log to contain DEBUG level, got: %s", output)
	}

	if !strings.Contains(output, "Debug message") {
		t.Errorf("Expected log to contain debug message, got: %s", output)
	}

	// Clear buffer
	buf.Reset()

	// Debug level should be filtered by info logger
	infoLogger.Debug(ctx, "Debug message", map[string]interface{}{"key": "value"})
	output = buf.String()

	if output != "" {
		t.Errorf("Expected debug log to be filtered by info logger, got: %s", output)
	}

	// Test info level
	infoLogger.Info(ctx, "Info message", map[string]interface{}{"key": "value"})
	output = buf.String()

	if !strings.Contains(output, "INFO") {
		t.Errorf("Expected log to contain INFO level, got: %s", output)
	}

	// Clear buffer
	buf.Reset()
}

func TestSubscriberLogger_ContextValues(t *testing.T) {
	// Set up a buffer to capture log output
	var buf bytes.Buffer
	customLogger := log.New(&buf, "", 0)

	// Create a test context with request_id
	ctx := context.WithValue(context.Background(), "request_id", "test-request-id")

	logger := &SubscriberLogger{
		logger:   customLogger,
		minLevel: DEBUG,
		prefix:   "[TEST] ",
	}

	// Log a message with context
	logger.Info(ctx, "Test message", nil)
	output := buf.String()

	// Parse the JSON
	var entry map[string]interface{}
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &entry); err != nil {
		t.Fatalf("Failed to parse log JSON: %v", err)
	}

	// Check that request_id from context was included
	requestID, ok := entry["request_id"].(string)
	if !ok || requestID != "test-request-id" {
		t.Errorf("Expected request_id to be 'test-request-id', got: %v", entry["request_id"])
	}

	// Clear buffer
	buf.Reset()
}

func TestSubscriberLogger_LifecycleEventLogging(t *testing.T) {
	// Set up a buffer to capture log output
	var buf bytes.Buffer
	customLogger := log.New(&buf, "", 0)

	logger := &SubscriberLogger{
		logger:   customLogger,
		minLevel: INFO,
		prefix:   "[TEST] ",
	}

	// Log a lifecycle event
	subscriberID := "test-subscriber"
	logger.LogLifecycleEvent(context.Background(), subscriberID, "registered", map[string]interface{}{
		"topics": []string{"topic1", "topic2"},
	})

	output := buf.String()

	// Parse the JSON
	var entry map[string]interface{}
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &entry); err != nil {
		t.Fatalf("Failed to parse log JSON: %v", err)
	}

	// Check log fields
	if entry["level"] != "INFO" {
		t.Errorf("Expected level to be INFO, got: %v", entry["level"])
	}

	if !strings.Contains(entry["message"].(string), "registered") {
		t.Errorf("Expected message to contain 'registered', got: %v", entry["message"])
	}

	if entry["subscriber_id"] != "test-subscriber" {
		t.Errorf("Expected subscriber_id to be 'test-subscriber', got: %v", entry["subscriber_id"])
	}

	if entry["lifecycle_event"] != "registered" {
		t.Errorf("Expected lifecycle_event to be 'registered', got: %v", entry["lifecycle_event"])
	}

	// Clear buffer
	buf.Reset()
}

func TestSubscriberLogger_EventDeliveryLogging(t *testing.T) {
	// Set up a buffer to capture log output
	var buf bytes.Buffer
	customLogger := log.New(&buf, "", 0)

	logger := &SubscriberLogger{
		logger:   customLogger,
		minLevel: DEBUG,
		prefix:   "[TEST] ",
	}

	// Log a successful event delivery
	subscriberID := "test-subscriber"
	eventID := "test-event"
	duration := 10 * time.Millisecond

	logger.LogEventDelivery(context.Background(), subscriberID, eventID, true, duration, nil)

	output := buf.String()

	// Verify success delivery contains DEBUG level
	if !strings.Contains(output, "DEBUG") {
		t.Errorf("Expected successful delivery log to be DEBUG level, got: %s", output)
	}

	// Clear buffer
	buf.Reset()

	// Log a failed event delivery
	logger.LogEventDelivery(context.Background(), subscriberID, eventID, false, duration, nil)

	output = buf.String()

	// Verify failed delivery contains WARN level
	if !strings.Contains(output, "WARN") {
		t.Errorf("Expected failed delivery log to be WARN level, got: %s", output)
	}

	// Clear buffer
	buf.Reset()
}

func TestSubscriberLogger_LogRegistration(t *testing.T) {
	// Set up a buffer to capture log output
	var buf bytes.Buffer
	customLogger := log.New(&buf, "", 0)

	logger := &SubscriberLogger{
		logger:   customLogger,
		minLevel: INFO,
		prefix:   "[TEST] ",
	}

	// Create test context with request ID
	ctx := context.WithValue(context.Background(), "request_id", "test-registration-id")

	// Prepare client information
	clientInfo := map[string]interface{}{
		"ip":         "192.168.1.1",
		"user_agent": "TestAgent/1.0",
		"id":         "client-123",
	}

	// Prepare subscriber fields
	subscriberID := "test-subscriber-reg"
	fields := map[string]interface{}{
		"topics":      []string{"topic1", "topic2"},
		"buffer_size": 100,
		"ttl":         "1h0m0s",
		"expiration":  "2025-01-01T00:00:00Z",
	}

	// Log a registration
	logger.LogRegistration(ctx, subscriberID, clientInfo, fields)

	output := buf.String()

	// Parse the JSON
	var entry map[string]interface{}
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &entry); err != nil {
		t.Fatalf("Failed to parse log JSON: %v", err)
	}

	// Verify client information is properly included
	if entry["client_ip"] != "192.168.1.1" {
		t.Errorf("Expected client_ip to be '192.168.1.1', got: %v", entry["client_ip"])
	}

	if entry["client_user_agent"] != "TestAgent/1.0" {
		t.Errorf("Expected client_user_agent to be 'TestAgent/1.0', got: %v", entry["client_user_agent"])
	}

	// Verify TTL was properly renamed
	if entry["subscription_ttl"] != "1h0m0s" {
		t.Errorf("Expected subscription_ttl to be '1h0m0s', got: %v", entry["subscription_ttl"])
	}

	// Verify message format
	if !strings.Contains(entry["message"].(string), subscriberID) {
		t.Errorf("Expected message to include subscriber ID, got: %v", entry["message"])
	}

	// Verify request ID from context
	if entry["request_id"] != "test-registration-id" {
		t.Errorf("Expected request_id to be 'test-registration-id', got: %v", entry["request_id"])
	}

	// Clear buffer for next test
	buf.Reset()

	// Now check if the debug log for time-limited subscription was generated
	if logger.minLevel <= DEBUG {
		var debugEntry map[string]interface{}
		if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &debugEntry); err == nil {
			if debugEntry["level"] == "DEBUG" {
				if !strings.Contains(debugEntry["message"].(string), "Time-limited") {
					t.Errorf("Expected debug message to mention time-limited subscription, got: %v", debugEntry["message"])
				}
			}
		}
	}
}
