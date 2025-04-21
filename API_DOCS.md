# GoEventSource API Documentation

This document serves as both a user guide and reference for the GoEventSource API, providing detailed information about endpoints, request/response formats, and examples.

## Introduction

GoEventSource is a RESTful API for event sourcing built with Go. It provides a robust and flexible way to store, retrieve, and manage events through a simple HTTP interface. The API is designed following event sourcing principles, where all changes to the system state are captured as a sequence of immutable events.

## API Versioning

The API uses versioning through URL paths. The current version is `v1`.

- All endpoints use versioned paths: `/api/v1/*`

## Core Concepts

### Topics

A topic is a stream of related events. For example, you might have topics for "orders", "users", or "inventory". Each topic can be configured with specific options for storage, retention, and more.

### Events

Events are immutable records of something that has happened in your system. Each event has:
- A type (e.g., "OrderCreated", "UserSignedUp")
- A payload containing the event data
- Metadata for additional context
- A version number indicating its position in the topic
- A timestamp showing when it was recorded

## Base Routes

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | Returns a simple hello world message to verify the API is running |
| GET | `/health` | Provides a basic health check of the API |

## EventStore Routes

### Health Check

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/v1/eventstore/health` | Returns detailed health information about the EventStore, including status of repositories |

#### Response Schema

```json
{
  "status": "success",
  "message": "Event store is healthy",
  "data": {
    "status": "up",
    "repositories": {
      "count": 3,
      "details": [
        {
          "name": "config",
          "adapter": "fs",
          "status": "up"
        },
        // ... other repositories
      ]
    },
    "timestamp": "2023-05-15T13:45:10Z"
  }
}
```

Possible status values:
- `up`: The system is fully operational
- `degraded`: The system is operational but with limitations
- `down`: The system is not operational

### Topic Management

#### List All Topics

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/v1/topics` | Returns a list of all topics in the EventStore |

##### Response Schema

```json
{
  "status": "success",
  "data": {
    "topics": [
      {
        "name": "orders",
        "adapter": "memory",
        "connection": "",
        "options": {
          "retention": "100"
        }
      },
      // ... other topics
    ],
    "count": 2
  }
}
```

#### Create a Topic

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/v1/topics` | Creates a new topic |

##### Request Schema

```json
{
  "name": "string",          // Required: Unique name for the topic
  "adapter": "string",       // Required: Storage adapter (e.g., "memory", "fs")
  "connection": "string",    // Optional: Connection information for the adapter
  "options": {               // Optional: Topic-specific configuration options
    "key": "value"
  }
}
```

Available adapters:
- `memory`: In-memory storage (not persistent)
- `fs`: File system storage

Common options:
- `retention`: Number of events to keep in the topic
- `compression`: Boolean indicating if event data should be compressed

##### Response Schema

```json
{
  "status": "success",
  "message": "Topic '{name}' created successfully",
  "data": {
    // Same as request schema
  }
}
```

#### Get Topic Details

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/v1/topics/{topicName}` | Returns details about a specific topic |

##### Response Schema

```json
{
  "status": "success",
  "data": {
    "name": "orders",
    "adapter": "memory",
    "connection": "",
    "options": {
      "retention": "100"
    },
    "statistics": {
      "eventCount": 42,
      "latestVersion": 42,
      "createdAt": "2023-05-15T10:30:00Z",
      "lastModifiedAt": "2023-05-15T13:45:10Z"
    }
  }
}
```

#### Update a Topic

| Method | Path | Description |
|--------|------|-------------|
| PUT | `/api/v1/topics/{topicName}` | Updates an existing topic |

##### Request Schema

```json
{
  "name": "string",          // Required: Must match topicName in URL
  "adapter": "string",       // Required: Storage adapter
  "connection": "string",    // Optional: Connection information
  "options": {               // Optional: Topic-specific configuration
    "key": "value"
  }
}
```

##### Response Schema

```json
{
  "status": "success",
  "message": "Topic '{name}' updated successfully",
  "data": {
    // Same as request schema
  }
}
```

#### Delete a Topic

| Method | Path | Description |
|--------|------|-------------|
| DELETE | `/api/v1/topics/{topicName}` | Deletes a topic |

##### Response Schema

```json
{
  "status": "success",
  "message": "Topic '{name}' deleted successfully"
}
```

### Event Operations

#### Get Events

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/v1/topics/{topicName}/events` | Retrieves events from a topic |

##### Query Parameters

- `fromVersion` (optional): Only return events with version >= this value (default: 0)
- `type` (optional): Filter events by event type

##### Response Schema

```json
{
  "status": "success",
  "data": {
    "events": [
      {
        "id": "string",
        "topic": "string",
        "type": "string",
        "data": {},
        "metadata": {},
        "timestamp": 1647359061000,
        "version": 1
      },
      // ... other events
    ],
    "count": 1,
    "topic": "string",
    "fromVersion": 0
  }
}
```

#### Append Events

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/v1/topics/{topicName}/events` | Appends events to a topic |

##### Request Schema

```json
[
  {
    "type": "string",    // Required: Event type identifier
    "data": {},          // Required: Event payload
    "metadata": {}       // Optional: Additional context for the event
  },
  // ... more events
]
```

##### Response Schema

```json
{
  "status": "success",
  "message": "{count} events appended to topic '{topicName}'",
  "data": {
    "eventCount": 1,
    "topic": "string"
  }
}
```

#### Get Latest Version

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/v1/topics/{topicName}/version` | Returns the latest event version for a topic |

##### Response Schema

```json
{
  "status": "success",
  "data": {
    "topic": "string",
    "latestVersion": 5
  }
}
```

## Error Handling

All error responses use a standardized format:

```json
{
  "status": "error",
  "message": "Human-readable error message",
  "data": {
    "error": "Error details"
  }
}
```

### Common HTTP Status Codes

| Status Code | Description |
|-------------|-------------|
| 200 OK | Request succeeded |
| 201 Created | Resource created successfully |
| 400 Bad Request | Invalid request format or parameters |
| 404 Not Found | Resource not found |
| 409 Conflict | Resource already exists or version conflict |
| 500 Internal Server Error | Server encountered an error |

## Usage Examples

### Creating a Topic

**Request:**
```http
POST /api/v1/topics
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
POST /api/v1/topics/orders/events
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
GET /api/v1/topics/orders/events?fromVersion=0&type=OrderCreated
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

### Updating a Topic

**Request:**
```http
PUT /api/v1/topics/orders
Content-Type: application/json

{
  "name": "orders",
  "adapter": "fs",
  "connection": "fs://data/orders",
  "options": {
    "retention": "200",
    "compression": "true"
  }
}
```

**Response:**
```json
{
  "status": "success",
  "message": "Topic 'orders' updated successfully",
  "data": {
    "name": "orders",
    "adapter": "fs",
    "connection": "fs://data/orders",
    "options": {
      "retention": "200",
      "compression": "true"
    }
  }
}
```

### Getting Latest Version

**Request:**
```http
GET /api/v1/topics/orders/version
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "topic": "orders",
    "latestVersion": 5
  }
}
```

## Best Practices

### Event Design

1. **Event naming**: Use past tense verbs for event types (e.g., "OrderCreated", "PaymentProcessed")
2. **Event data**: Include only the data that changed, not the entire entity
3. **Event size**: Keep events small for better performance
4. **Event schema**: Consider using a schema registry for versioning event schemas

### Topic Organization

1. **Topic naming**: Use kebab-case for topic names (e.g., "order-processing")
2. **Topic granularity**: Create topics around business domains or aggregates
3. **Topic options**: Configure retention based on your business needs

### API Usage

1. **Batch operations**: When appending multiple events, batch them in a single request
2. **Event ordering**: Ensure events are processed in the order they were appended
3. **Error handling**: Implement proper error handling and retries for failed requests
4. **Health checks**: Regularly monitor the health endpoint for system status 