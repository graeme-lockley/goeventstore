# GoEventSource API

A RESTful API for event sourcing built with Go, using the standard library's net/http package and the new ServeMux introduced in Go 1.22.

## Overview

The GoEventSource API provides a flexible and robust way to interact with an event-sourced system through HTTP endpoints. It exposes functionality for:

- Managing topics (streams of events)
- Appending events to topics
- Retrieving events from topics
- Checking system health

## API Endpoints

### Base Endpoints

- `GET /`: A simple hello world endpoint
- `GET /health`: Basic API health check

### EventStore Endpoints

#### Health

- `GET /api/eventstore/health`: Detailed health information about the EventStore

#### Topic Management

- `GET /api/topics`: List all topics
- `POST /api/topics`: Create a new topic
- `GET /api/topics/{topicName}`: Get details of a specific topic
- `DELETE /api/topics/{topicName}`: Delete a topic

#### Event Operations

- `GET /api/topics/{topicName}/events`: Retrieve events from a topic
  - Query parameters:
    - `fromVersion`: Only return events with version >= this value
    - `type`: Filter events by event type
- `POST /api/topics/{topicName}/events`: Append events to a topic

## Request/Response Examples

### Creating a Topic

**Request:**
```http
POST /api/topics
Content-Type: application/json

{
  "name": "orders",
  "adapter": "memory",
  "connection": "",
  "options": {
    "retention": "100"
  }
}
```

**Response:**
```json
{
  "status": "success",
  "message": "Topic 'orders' created successfully",
  "data": {
    "name": "orders",
    "adapter": "memory",
    "connection": "",
    "options": {
      "retention": "100"
    }
  }
}
```

### Appending Events

**Request:**
```http
POST /api/topics/orders/events
Content-Type: application/json

[
  {
    "type": "OrderCreated",
    "data": {
      "orderId": "12345",
      "customerId": "C789",
      "items": [{"productId": "P123", "quantity": 2}]
    },
    "metadata": {
      "source": "api",
      "userId": "U456"
    }
  }
]
```

**Response:**
```json
{
  "status": "success",
  "message": "1 events appended to topic 'orders'",
  "data": {
    "eventCount": 1,
    "topic": "orders"
  }
}
```

### Getting Events

**Request:**
```http
GET /api/topics/orders/events?fromVersion=0&type=OrderCreated
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "events": [
      {
        "id": "evt_123abc",
        "topic": "orders",
        "type": "OrderCreated",
        "data": {
          "orderId": "12345",
          "customerId": "C789",
          "items": [{"productId": "P123", "quantity": 2}]
        },
        "metadata": {
          "source": "api",
          "userId": "U456"
        },
        "timestamp": 1647359061000,
        "version": 1
      }
    ],
    "count": 1,
    "topic": "orders",
    "fromVersion": 0
  }
}
```

## Running the API

1. Set the PORT environment variable (optional, defaults to 8080)
2. Run the API: `go run src/cmd/api/main.go`

## Development

### Structure

- `src/cmd/api`: Main application entry point
- `src/internal/api`: API implementation (handlers, middleware)
- `src/internal/eventstore`: Core event sourcing functionality
- `src/internal/port`: Interface definitions

### Testing

Run the tests:

```bash
go test ./...
```

## Features

- Robust health monitoring
- Proper error handling with appropriate HTTP status codes
- Middleware for logging, CORS, and panic recovery
- Graceful shutdown handling
- EventStore lifecycle management 