# GoEventSource

An event sourcing library implemented in Go that provides both a standalone EventStore and a REST API for event sourcing operations.

## Overview

GoEventSource is a flexible event sourcing implementation built with Go. It provides:

- A standalone EventStore library that can be embedded in your Go applications
- A RESTful API built with Go's standard library net/http package and Go 1.22's new ServeMux
- Multiple storage backends (in-memory, file system, with extensibility for others)
- Topic-based event organization with flexible configuration
- Comprehensive health monitoring and lifecycle management

## Using GoEventSource as a Library

GoEventSource can be easily embedded into your Go applications to add event sourcing capabilities.

### Installation

```bash
go get github.com/yourusername/goeventsource
```

### Basic Usage

```go
package main

import (
	"context"
	"log"
	"os"
	"time"
	
	"goeventsource/src/internal/eventstore"
	"goeventsource/src/internal/eventstore/models"
)

func main() {
	// Create a configuration for the EventStore
	config := models.EventStoreConfig{
		Logger: log.New(os.Stdout, "[EventStore] ", log.LstdFlags),
		ConfigStream: models.EventStreamConfig{
			Name:       "config",
			Adapter:    "memory", // Use in-memory storage for configuration
			Connection: "memory-config",
			Options:    make(map[string]string),
		},
	}
	
	// Create the EventStore instance
	store, err := eventstore.NewEventStore(config)
	if err != nil {
		log.Fatalf("Failed to create EventStore: %v", err)
	}
	
	// Initialize the EventStore
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	if err := store.Initialize(ctx); err != nil {
		log.Fatalf("Failed to initialize EventStore: %v", err)
	}
	
	// Create a topic
	topicConfig := models.TopicConfig{
		Name:       "user-events",
		Adapter:    "memory",
		Connection: "memory-user-events",
		Options:    map[string]string{"retention": "1000"},
	}
	
	if err := store.CreateTopic(ctx, topicConfig); err != nil {
		log.Fatalf("Failed to create topic: %v", err)
	}
	
	// Append events to the topic
	events := []models.Event{
		{
			Type: "UserCreated",
			Data: map[string]interface{}{
				"userId": "123",
				"email":  "user@example.com",
			},
			Metadata: map[string]interface{}{
				"source": "user-service",
			},
		},
	}
	
	if err := store.AppendEvents(ctx, "user-events", events); err != nil {
		log.Fatalf("Failed to append events: %v", err)
	}
	
	// Retrieve events from the topic
	retrievedEvents, err := store.GetEvents(ctx, "user-events", 0)
	if err != nil {
		log.Fatalf("Failed to retrieve events: %v", err)
	}
	
	log.Printf("Retrieved %d events", len(retrievedEvents))
	
	// Close the EventStore when done
	if err := store.Close(); err != nil {
		log.Printf("Error closing EventStore: %v", err)
	}
}
```

### Advanced Features

- **Multiple Storage Adapters**: Choose between memory, file system, or implement your own adapters
- **Concurrent Operations**: Thread-safe design allows concurrent event reads and writes
- **Flexible Configuration**: Configure each topic with different storage backends and options
- **Health Monitoring**: Built-in health checks for monitoring and diagnostics

## REST API

GoEventSource also provides a RESTful API for interacting with the EventStore through HTTP endpoints.

### API Documentation

For comprehensive API documentation including endpoint details, request/response formats, and examples, please refer to the [API Documentation](API_DOCS.md).

### Running the API Server

1. Set the PORT environment variable (optional, defaults to 8080)
2. Run the API: `go run src/cmd/api/main.go`

## Project Structure

- `src/cmd/api`: API server entry point
- `src/internal/eventstore`: Core EventStore implementation
  - `models`: Data models and configurations
  - `adapters`: Storage adapter implementations
- `src/internal/api`: API implementation (handlers, middleware)
- `src/internal/port`: Interface definitions and contracts

## Development

### Testing

Run the tests:

```bash
go test ./...
```

### Adding New Storage Adapters

GoEventSource is designed to be extensible. To add a new storage adapter:

1. Implement the `EventRepository` interface
2. Register your adapter in the repository factory
3. Use it in your configuration with the appropriate adapter name

## Features

- Event sourcing with strong consistency guarantees
- Multiple storage backend support
- Optimistic concurrency control
- Robust health monitoring
- Topic-based event organization
- Proper error handling with appropriate status codes
- Middleware for logging, CORS, and panic recovery
- Graceful shutdown handling
- EventStore lifecycle management 