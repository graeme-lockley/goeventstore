# 📘 Event Store in Go – Requirements Document

## ✅ Overview

This project implements a modular, pluggable **Event Store** in **Go**, following:

- Clean Architecture
- CQRS (Command Query Responsibility Segregation)
- Event Sourcing
- Domain-Driven Design (DDD)
- Dependency Injection
- Repository Pattern

It exposes a RESTful API for managing/querying events, supports transient subscriptions, and allows runtime configuration of multiple topics, each with its own persistent storage.

---

## 🧱 Modules

### 1. `api` – REST API Gateway

**Responsibilities:**
- Expose REST endpoints for data and control planes
- Route commands/queries to the `eventstore`
- Manage transient subscriber callbacks

**Endpoints:**

**Control Plane:**
- `POST /topics` – Create topic
- `GET /health` – Health check
- `GET /topics` – List all topics

**Data Plane:**
- `POST /topics/{topic}/events` – Publish event
- `GET /topics/{topic}/events?from=123` – Query events
- `POST /topics/{topic}/subscribe?from=123` – Register callback (transient)

---

### 2. `eventstore` – Core CQRS Engine

**Responsibilities:**
- Handle all command and query operations
- Persist and retrieve events using runtime-configured adapters
- Broadcast events to projections and transient subscribers

**Submodules:**
- `commands/`
- `queries/`
- `topics/`
- `projections/`
- `subscribers/`

---

### 3. `adapters/`

**Submodules:**

- `memory/`: Volatile, in-memory event storage
- `fs/`: File system-based storage using JSON files
- `blob/`: Persists events as JSON to Azure Blob Storage
- `postgres/`: Stores events in PostgreSQL tables

---

## 🔁 Topic Configuration

Each topic is registered with a unique configuration:

```json
{
  "name": "orders",
  "adapter": "postgres",
  "connection": "postgres://user:pass@localhost/db",
  "schema": "public",
  "table": "order_events"
}
```

For the file system adapter, the configuration looks like:

```json
{
  "name": "users",
  "adapter": "fs",
  "connection": "/data/eventstore",
  "options": {
    "createDirIfNotExist": "true"
  }
}
```

---

## 🔄 CQRS Design

### Command Side
- Publishes events to configured adapter
- Broadcasts to projections and callbacks

### Query Side
- Powered by projections (event handlers)
- Read models optimized for querying

### Projections
- Background workers
- Keep query-side models up-to-date

---

## ⚙️ Technologies & Tools

| Component         | Technology         |
|------------------|--------------------|
| Language          | Go                 |
| Database          | PostgreSQL         |
| Cloud Storage     | Azure Blob Storage |
| Local Storage     | File System (JSON) |
| REST Framework    | chi / net/http     |
| Containerization  | Docker, Compose    |
| Testing           | `testing`, `httptest`, `dockertest` |

---

## 🧩 Design Patterns

- CQRS
- Event Sourcing
- DDD
- Repository Pattern
- Dependency Injection

---

## 📦 Project Directory Structure

```
/
├── src/                   # Source code
│   ├── cmd/               # Entrypoints
│   │   ├── api/           # Starts REST API server
│   │   └── replay/        # Runs projection rebuilds
│   │
│   └── internal/          # Internal packages
│       ├── api/           # HTTP handlers
│       ├── eventstore/    # CQRS core engine
│       │   ├── commands/
│       │   ├── queries/
│       │   ├── topics/
│       │   ├── projections/
│       │   └── subscribers/
│       ├── adapters/      # Storage adapters
│       │   ├── memory/
│       │   ├── fs/
│       │   ├── blob/
│       │   └── postgres/
│       └── port/          # Interfaces (ports)
│           ├── inbound/
│           └── outbound/
│
├── test/                  # Integration tests
│
├── infra/                 # Infrastructure
│   ├── workstation/       # Docker workstation setup
│   └── docker/            # Docker configuration
│
└── go.mod
```

Note that only integration tests are placed in the `test` directory. Unit tests are placed in the directory of the file that is under test.

---

## 🖼️ ASCII Architecture Diagram

```
                    +----------------+
                    |    REST API    |
                    |     (chi)      |
                    +----------------+
                             |
        +--------------------+-------------------+
        |                                        |
+---------------+                       +-----------------+
| Command Side  |                       |   Query Side    |
+---------------+                       +-----------------+
        |                                        |
+---------------+                       +-----------------+
|  Adapters      |     <-- CQRS -->     |  Projections     |
| (memory/fs/    |                       |  (event handlers)|
|   blob/postgres)|                       +-----------------+
+---------------+
        |
+-------------------------+
| Persistent Event Storage|
+-------------------------+
```

---

## ✅ Testing Strategy

### Unit Tests:
- Command and query handlers
- Topic and event validation
- Adapter mocks

### Integration Tests:
- Full API tests
- Blob and PostgreSQL storage tests
- Callback delivery

---

## 🚦 Startup Behavior

1. Load topic configurations
2. Initialize persistence adapters
3. Start projection processors
4. Launch HTTP API (chi)

---

Let me know if you'd like this scaffolded into code or deployed with a local Docker Compose setup!
