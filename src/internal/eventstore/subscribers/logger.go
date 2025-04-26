// Package subscribers contains the subscriber registry for the EventStore.
package subscribers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"runtime"
	"time"
)

// LogLevel represents the severity of a log message
type LogLevel int

const (
	// DEBUG level for detailed debugging information
	DEBUG LogLevel = iota
	// INFO level for general operational information
	INFO
	// WARN level for warning events
	WARN
	// ERROR level for error events
	ERROR
	// FATAL level for critical errors that require termination
	FATAL
)

// String returns the string representation of the log level
func (l LogLevel) String() string {
	return [...]string{"DEBUG", "INFO", "WARN", "ERROR", "FATAL"}[l]
}

// SubscriberLogger handles logging for subscriber operations
type SubscriberLogger struct {
	logger   *log.Logger
	minLevel LogLevel
	prefix   string
}

// NewSubscriberLogger creates a new subscriber logger
func NewSubscriberLogger(minLevel LogLevel) *SubscriberLogger {
	return &SubscriberLogger{
		logger:   log.New(os.Stderr, "[SUBSCRIBER] ", log.LstdFlags),
		minLevel: minLevel,
		prefix:   "[SUBSCRIBER] ",
	}
}

// WithPrefix returns a new logger with the specified prefix
func (l *SubscriberLogger) WithPrefix(prefix string) *SubscriberLogger {
	return &SubscriberLogger{
		logger:   log.New(os.Stderr, prefix, log.LstdFlags),
		minLevel: l.minLevel,
		prefix:   prefix,
	}
}

// logWithContext logs a message with context information
func (l *SubscriberLogger) logWithContext(ctx context.Context, level LogLevel, message string, fields map[string]interface{}) {
	if level < l.minLevel {
		return
	}

	// Don't continue for nil contexts
	if ctx == nil {
		ctx = context.Background()
	}

	// Create log entry with standard fields
	entry := map[string]interface{}{
		"level":     level.String(),
		"timestamp": time.Now().Format(time.RFC3339),
		"message":   message,
	}

	// Add request ID if present
	if requestID, ok := ctx.Value("request_id").(string); ok && requestID != "" {
		entry["request_id"] = requestID
	}

	// Add caller information
	_, file, line, ok := runtime.Caller(2)
	if ok {
		entry["caller"] = fmt.Sprintf("%s:%d", file, line)
	}

	// Add additional fields
	if fields != nil {
		for k, v := range fields {
			entry[k] = v
		}
	}

	// Convert to JSON
	jsonData, err := json.Marshal(entry)
	if err != nil {
		l.logger.Printf("Error marshaling log entry: %v", err)
		return
	}

	l.logger.Println(string(jsonData))

	// Exit application on fatal errors
	if level == FATAL {
		os.Exit(1)
	}
}

// Debug logs a debug message
func (l *SubscriberLogger) Debug(ctx context.Context, message string, fields map[string]interface{}) {
	l.logWithContext(ctx, DEBUG, message, fields)
}

// Info logs an info message
func (l *SubscriberLogger) Info(ctx context.Context, message string, fields map[string]interface{}) {
	l.logWithContext(ctx, INFO, message, fields)
}

// Warn logs a warning message
func (l *SubscriberLogger) Warn(ctx context.Context, message string, fields map[string]interface{}) {
	l.logWithContext(ctx, WARN, message, fields)
}

// Error logs an error message
func (l *SubscriberLogger) Error(ctx context.Context, message string, fields map[string]interface{}) {
	l.logWithContext(ctx, ERROR, message, fields)
}

// Fatal logs a fatal message and exits the application
func (l *SubscriberLogger) Fatal(ctx context.Context, message string, fields map[string]interface{}) {
	l.logWithContext(ctx, FATAL, message, fields)
}

// SubscriberLogFields creates a standardized log fields map for subscriber operations
func SubscriberLogFields(subscriberID string, additionalFields map[string]interface{}) map[string]interface{} {
	fields := map[string]interface{}{
		"subscriber_id": subscriberID,
	}

	// Add additional fields
	if additionalFields != nil {
		for k, v := range additionalFields {
			fields[k] = v
		}
	}

	return fields
}

// LogLifecycleEvent logs a subscriber lifecycle event
func (l *SubscriberLogger) LogLifecycleEvent(ctx context.Context, subscriberID, event string, fields map[string]interface{}) {
	logFields := SubscriberLogFields(subscriberID, fields)
	logFields["lifecycle_event"] = event
	l.Info(ctx, fmt.Sprintf("Subscriber lifecycle event: %s", event), logFields)
}

// LogStateChange logs a subscriber state change
func (l *SubscriberLogger) LogStateChange(ctx context.Context, subscriberID, oldState, newState string, fields map[string]interface{}) {
	logFields := SubscriberLogFields(subscriberID, fields)
	logFields["old_state"] = oldState
	logFields["new_state"] = newState
	l.Info(ctx, fmt.Sprintf("Subscriber state change: %s -> %s", oldState, newState), logFields)
}

// LogEventDelivery logs an event delivery attempt
func (l *SubscriberLogger) LogEventDelivery(ctx context.Context, subscriberID, eventID string, success bool, duration time.Duration, fields map[string]interface{}) {
	logFields := SubscriberLogFields(subscriberID, fields)
	logFields["event_id"] = eventID
	logFields["success"] = success
	logFields["duration"] = duration.String()

	if success {
		l.Debug(ctx, "Event delivered successfully", logFields)
	} else {
		l.Warn(ctx, "Event delivery failed", logFields)
	}
}

// LogRetryAttempt logs a retry attempt
func (l *SubscriberLogger) LogRetryAttempt(ctx context.Context, subscriberID string, retryCount int, nextTimeout time.Duration, fields map[string]interface{}) {
	logFields := SubscriberLogFields(subscriberID, fields)
	logFields["retry_count"] = retryCount
	logFields["next_timeout"] = nextTimeout.String()
	l.Info(ctx, "Retrying event delivery", logFields)
}
