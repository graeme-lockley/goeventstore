# Go Event Source

A modular, pluggable Event Store implementation in Go, following clean architecture, CQRS, and event sourcing principles.

## Overview

Go Event Source is a flexible event storage system that allows you to:

- Store and retrieve events using different storage backends
- Create and manage event topics at runtime
- Subscribe to events using transient callbacks
- Support multiple persistence strategies (memory, file system, PostgreSQL, Azure Blob)

The system is built with a hexagonal architecture, separating core domain logic from external adapters.

## Quick Start

### Running Locally

1. Clone the repository:
   ```sh
   git clone https://github.com/yourusername/goeventsource.git
   cd goeventsource
   ```

2. Build and run:
   ```sh
   go build -o bin/api ./cmd/api
   ./bin/api
   ```

3. Access the API:
   - Hello World endpoint: http://localhost:8080/
   - Health check: http://localhost:8080/health

### Running with Docker

1. Using Docker Compose:
   ```sh
   cd infra/docker
   docker-compose up
   ```

2. For development with hot-reload:
   ```sh
   cd infra/docker
   docker-compose -f docker-compose.dev.yml up
   ```

## API Documentation

### Endpoints

#### Control Plane

| Method | Endpoint        | Description                              |
|--------|-----------------|------------------------------------------|
| GET    | /health         | Health check, returns system information |
| POST   | /topics         | Create a new topic                       |
| GET    | /topics         | List all topics                          |

#### Data Plane

| Method | Endpoint                     | Description                                |
|--------|------------------------------|--------------------------------------------|
| POST   | /topics/{topic}/events       | Publish event(s) to a topic                |
| GET    | /topics/{topic}/events       | Query events from a topic                  |
| POST   | /topics/{topic}/subscribe    | Register callback for events (transient)   |

### Example Requests

#### Publish Event

```sh
curl -X POST http://localhost:8080/topics/orders/events \
  -H "Content-Type: application/json" \
  -d '[{
    "type": "OrderCreated",
    "data": {
      "orderId": "12345",
      "customerId": "c789",
      "items": [{"productId": "p123", "quantity": 2}]
    }
  }]'
```

#### Query Events

```sh
curl -X GET "http://localhost:8080/topics/orders/events?from=0"
```

## Project Structure

```
/
├── cmd/                   # Entrypoints
│   ├── api/               # REST API server
│   └── replay/            # Projection rebuilds
│
├── internal/
│   ├── api/               # HTTP handlers and middleware
│   ├── eventstore/        # CQRS core engine
│   ├── adapters/          # Storage adapters
│   └── port/              # Interfaces (ports)
│
├── infra/                 # Infrastructure
│   └── docker/            # Docker configurations
│
├── docs/                  # Documentation
└── test/                  # Integration tests
```

## Architecture

The project follows hexagonal architecture (ports and adapters):

- **Core Domain**: Contains business logic and domain models
- **Ports**: Interfaces that define how the core interacts with the outside world
- **Adapters**: Implementations of the interfaces (ports) for different technologies

## Storage Adapters

- **Memory**: In-memory storage for testing and development
- **File System**: JSON file-based storage
- **PostgreSQL**: Relational database storage
- **Azure Blob Storage**: Cloud-based object storage

## Testing

Run the test suite:

```sh
go test ./...
```

Generate coverage report:

```sh
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

## CI/CD

This project uses GitHub Actions for continuous integration and delivery:

- Automatically runs tests on pull requests
- Builds and pushes Docker images on merge to main
- Generates and uploads test coverage reports

## License

This project is licensed under the MIT License - see the LICENSE file for details. 