package logging

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"
)

// Level represents logging level
type Level int

const (
	// DEBUG level
	DEBUG Level = iota
	// INFO level
	INFO
	// WARN level
	WARN
	// ERROR level
	ERROR
	// FATAL level
	FATAL
)

// String returns string representation of level
func (l Level) String() string {
	return [...]string{"DEBUG", "INFO", "WARN", "ERROR", "FATAL"}[l]
}

// LogEntry represents a structured log entry
type LogEntry struct {
	Level     string      `json:"level"`
	Timestamp string      `json:"timestamp"`
	Message   string      `json:"message"`
	Data      interface{} `json:"data,omitempty"`
}

// Logger is a structured logger
type Logger struct {
	stdLogger *log.Logger
	minLevel  Level
}

// NewLogger creates a new Logger
func NewLogger(minLevel Level) *Logger {
	return &Logger{
		stdLogger: log.New(os.Stdout, "", 0),
		minLevel:  minLevel,
	}
}

// log logs a message at the specified level
func (l *Logger) log(level Level, message string, data interface{}) {
	if level < l.minLevel {
		return
	}

	entry := LogEntry{
		Level:     level.String(),
		Timestamp: time.Now().Format(time.RFC3339),
		Message:   message,
		Data:      data,
	}

	jsonEntry, err := json.Marshal(entry)
	if err != nil {
		l.stdLogger.Printf(`{"level":"ERROR","message":"Error marshaling log entry","error":"%s"}`, err)
		return
	}

	l.stdLogger.Println(string(jsonEntry))

	// If fatal, exit
	if level == FATAL {
		os.Exit(1)
	}
}

// Debug logs a debug message
func (l *Logger) Debug(message string, data ...interface{}) {
	var d interface{}
	if len(data) > 0 {
		d = data[0]
	}
	l.log(DEBUG, message, d)
}

// Info logs an info message
func (l *Logger) Info(message string, data ...interface{}) {
	var d interface{}
	if len(data) > 0 {
		d = data[0]
	}
	l.log(INFO, message, d)
}

// Warn logs a warning message
func (l *Logger) Warn(message string, data ...interface{}) {
	var d interface{}
	if len(data) > 0 {
		d = data[0]
	}
	l.log(WARN, message, d)
}

// Error logs an error message
func (l *Logger) Error(message string, data ...interface{}) {
	var d interface{}
	if len(data) > 0 {
		d = data[0]
	}
	l.log(ERROR, message, d)
}

// Fatal logs a fatal message and exits
func (l *Logger) Fatal(message string, data ...interface{}) {
	var d interface{}
	if len(data) > 0 {
		d = data[0]
	}
	l.log(FATAL, message, d)
}

// RequestLogger logs HTTP request details
func (l *Logger) RequestLogger(method, path, remoteAddr, userAgent string, statusCode int, duration time.Duration) {
	l.Info(
		fmt.Sprintf("HTTP Request: %s %s", method, path),
		map[string]interface{}{
			"method":      method,
			"path":        path,
			"status_code": statusCode,
			"duration":    duration.String(),
			"remote_addr": remoteAddr,
			"user_agent":  userAgent,
		},
	)
}
