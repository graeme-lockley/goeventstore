# API Documentation

This document provides detailed information about the Go Event Source API endpoints, request/response formats, and error handling.

## Base URL

In development: `http://localhost:8080`

## Authentication

The API currently does not implement authentication. This should be added in a production environment.

## Content Type

All endpoints accept and return JSON data.

- Request: `Content-Type: application/json`
- Response: `Content-Type: application/json`

## Common Response Format

All API responses follow a common format:

```json
{
  "status": "success|error",
  "data": { ... },
  "message": "Human readable message (optional)"
}
```

## Error Handling

Errors follow the same response format with appropriate HTTP status codes:

```json
{
  "status": "error",
  "message": "Error description",
  "error": {
    "code": "ERROR_CODE",
    "details": { ... }
  }
}
```

## Endpoints

### Control Plane

#### Health Check

Check the health of the service.

**Request:**
- Method: `GET`
- Path: `/health`

**Response:**
- Status: 200 OK

```json
{
  "status": "success",
  "data": {
    "status": "UP",
    "timestamp": "2023-08-15T12:34:56Z",
    "go_version": "go1.24.2",
    "go_os": "linux",
    "go_arch": "amd64"
  }
}
```

#### Create Topic

Create a new event topic.

**Request:**
- Method: `POST`
- Path: `/topics`

**Payload:**
```json
{
  "name": "orders",
  "adapter": "postgres",
  "connection": "postgres://user:pass@localhost/db",
  "options": {
    "schema": "public",
    "table": "order_events"
  }
}
```

**Response:**
- Status: 201 Created

```json
{
  "status": "success",
  "data": {
    "name": "orders",
    "adapter": "postgres"
  },
  "message": "Topic created successfully"
}
```

#### List Topics

List all available topics.

**Request:**
- Method: `GET`
- Path: `/topics`

**Response:**
- Status: 200 OK

```json
{
  "status": "success",
  "data": {
    "topics": [
      {
        "name": "orders",
        "adapter": "postgres"
      },
      {
        "name": "users",
        "adapter": "memory"
      }
    ]
  }
}
```

### Data Plane

#### Publish Events

Publish one or more events to a topic.

**Request:**
- Method: `POST`
- Path: `/topics/{topic}/events`

**Payload:**
```json
[
  {
    "type": "OrderCreated",
    "data": {
      "orderId": "12345",
      "customerId": "c789",
      "items": [{"productId": "p123", "quantity": 2}]
    },
    "metadata": {
      "source": "web-app",
      "correlation_id": "abc-123"
    }
  }
]
```

**Response:**
- Status: 202 Accepted

```json
{
  "status": "success",
  "data": {
    "accepted": 1,
    "first_event_id": "evt_123456",
    "first_version": 42
  },
  "message": "Events accepted"
}
```

#### Query Events

Retrieve events from a topic.

**Request:**
- Method: `GET`
- Path: `/topics/{topic}/events`
- Query Parameters:
  - `from`: Version to start from (integer, required)
  - `type`: Filter by event type (string, optional)
  - `limit`: Maximum number of events to return (integer, optional, default: 100)

**Response:**
- Status: 200 OK

```json
{
  "status": "success",
  "data": {
    "events": [
      {
        "id": "evt_123456",
        "topic": "orders",
        "type": "OrderCreated",
        "data": { ... },
        "metadata": { ... },
        "timestamp": 1691265378000,
        "version": 42
      }
    ],
    "next_version": 43,
    "has_more": false
  }
}
```

#### Subscribe to Events

Register a callback URL for event notifications.

**Request:**
- Method: `POST`
- Path: `/topics/{topic}/subscribe`

**Payload:**
```json
{
  "callback_url": "https://example.com/webhook",
  "from_version": 42,
  "filter": {
    "event_types": ["OrderCreated", "OrderCancelled"]
  },
  "headers": {
    "Authorization": "Bearer token123"
  }
}
```

**Response:**
- Status: 201 Created

```json
{
  "status": "success",
  "data": {
    "subscription_id": "sub_789012",
    "topic": "orders",
    "callback_url": "https://example.com/webhook"
  },
  "message": "Subscription created"
}
```

## HTTP Status Codes

- `200 OK`: The request was successful
- `201 Created`: The resource was created successfully
- `202 Accepted`: The request has been accepted for processing
- `400 Bad Request`: The request was invalid
- `404 Not Found`: The requested resource was not found
- `409 Conflict`: The request conflicts with the current state
- `422 Unprocessable Entity`: The request was well-formed but cannot be processed
- `500 Internal Server Error`: An error occurred on the server

## Rate Limiting

The API implements rate limiting to prevent abuse. Limits are specified in the response headers:

- `X-RateLimit-Limit`: Requests allowed per period
- `X-RateLimit-Remaining`: Requests remaining in current period
- `X-RateLimit-Reset`: Seconds until the rate limit resets 