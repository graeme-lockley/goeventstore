# EventStore Implementation Backlog

## Overview
This backlog contains detailed tasks for implementing the EventStore functionality, which will provide a high-level interface to the event repository with configuration management through events.

## 1. Create EventStore Package Structure
- [x] Create `src/internal/eventstore` directory
- [x] Create `src/internal/eventstore/commands` directory for command handlers
- [x] Create `src/internal/eventstore/queries` directory for query handlers
- [x] Create `src/internal/eventstore/models` directory for domain models

## 2. Define Core Models and Interfaces
### 2.1 Configuration Event Types
- [x] Define `EventTypeTopicCreated` constant
- [x] Define `EventTypeTopicUpdated` constant
- [x] Define `EventTypeTopicDeleted` constant

### 2.2 EventStore Struct and Interface
- [x] Define `EventStore` struct with repository, config topic name, write model, mutex, and logger fields
- [x] Add `isInitialized` flag to track initialization state
- [x] Create functional options pattern for configuring the EventStore
  - [x] Implement `WithLogger` option
  - [x] Implement `WithConfigTopicName` option
- [x] Define `NewEventStore` constructor function

## 3. Implement Main EventStore Functionality
### 3.1 EventStore Initialization
- [x] Implement `Initialize` method to set up the EventStore
  - [x] Add logic to initialize underlying repository
  - [x] Check if configuration topic exists
  - [x] Create configuration topic if needed
  - [x] Load configuration events and build write model
  - [x] Set initialization flag

### 3.2 Write Model Management
- [x] Implement `rebuildWriteModel` function to load and process all configuration events
- [x] Create event handlers for different configuration event types:
  - [x] Implement `handleTopicCreated` handler
  - [x] Implement `handleTopicUpdated` handler 
  - [x] Implement `handleTopicDeleted` handler
- [x] Add validation and error handling for event processing

## 4. EventStore Refactoring for Multi-Repository Support
The EventStore needs to be refactored to manage multiple repositories based on event stream configurations.

### 4.1 Event Stream Configuration (Implement First)
- [x] Create `models/eventstream.go` file for configuration structures
- [x] Create an `EventStreamConfig` struct:
  - [x] Define fields for name, adapter, connection, and options
  - [x] Add validation method to ensure required fields are present
  - [x] Create constructor function with sensible defaults
- [x] Create an `EventStoreConfig` struct:
  - [x] Add field for configuration event stream
  - [x] Add field for logger
  - [x] Implement validation method
  - [x] Create constructor with default values
- [x] Write unit tests for both configuration structs in `models/eventstream_test.go`

### 4.2 Repository Management (Implement Second)
- [x] Create `repository_registry.go` file
- [x] Move existing repository factory code from `factory.go` (if exists)
- [x] Create a `RepositoryRegistry` struct:
  - [x] Add map to store repositories keyed by "adapter:connection"
  - [x] Implement mutex for thread safety
  - [x] Define initialization function
- [x] Implement core registry methods:
  - [x] `Get(adapter, connection string) (repository, bool)` to retrieve existing repositories
  - [x] `GetOrCreate(config EventStreamConfig) (repository, error)` to get or create repositories
  - [x] `createRepository(config EventStreamConfig) (repository, error)` to instantiate repositories
  - [x] `Close()` to properly close all managed repositories
  - [x] `Health()` to collect health information from all repositories
- [x] Write unit tests for the registry in `repository_registry_test.go`

### 4.3 Update EventStore Structure (Implement Third)
- [x] Update `EventStore` struct:
  - [x] Replace single repository field with registry
  - [x] Add configStream field to store configuration event stream settings
  - [x] Keep other fields (topicConfigs, logger, mutex, isInitialized)
- [x] Update `NewEventStore` constructor:
  - [x] Change parameter from repository to EventStoreConfig
  - [x] Initialize the repository registry
  - [x] Set the configuration event stream
  - [x] Keep the rest of the initialization logic
- [x] Create helper methods:
  - [x] `getConfigRepository() (repository, error)` to get the config stream repository
  - [x] `getRepositoryForTopic(topicName string) (repository, error)` with error handling for unknown topics
- [x] Update unit tests for the constructor

### 4.4 Update Initialize Method (Implement Fourth)
- [x] Update `Initialize` method:
  - [x] Use `getConfigRepository()` to get the config repository
  - [x] Create config topic if not exists
  - [x] Rebuild write model by loading all config events
- [x] Implement `rebuildWriteModel()` method:
  - [x] Clear existing state
  - [x] Load all events from configuration topic
  - [x] Apply each event to rebuild the write model
- [x] Implement `applyConfigurationEvent(event)` method:
  - [x] Handle topic created/updated/deleted events
- [x] Update existing unit tests

### 4.5 Update Topic Operations (Implement Fifth)
- [x] Update `CreateTopic` method:
  - [x] Validate topic configuration
  - [x] Get or create appropriate repository
  - [x] Create topic in that repository
  - [x] Get configuration event stream repository
  - [x] Record creation event in configuration topic
  - [x] Update write model
- [x] Update `UpdateTopic` method:
  - [x] Validate topic exists and configuration
  - [x] Get original topic configuration
  - [x] Check if adapter/connection changed
  - [x] If changed, get or create new repository
  - [x] Update topic in appropriate repository
  - [x] Record update event in configuration topic
  - [x] Update write model
- [x] Update `DeleteTopic` method:
  - [x] Validate topic exists
  - [x] Get repository for the topic
  - [x] Delete topic in that repository
  - [x] Record deletion event in configuration topic
  - [x] Update write model
- [x] Update `GetTopic` and `ListTopics` methods to use write model
- [x] Write unit tests for all topic operations

### 4.6 Update Event Operations (Implement Sixth)
- [x] Update `AppendEvents` method:
  - [x] Get repository for the topic
  - [x] Append events to that repository
- [x] Update `GetEvents` method:
  - [x] Get repository for the topic
  - [x] Retrieve events from that repository
- [x] Update `GetEventsByType` method:
  - [x] Get repository for the topic
  - [x] Retrieve events by type from that repository
- [x] Update `GetLatestVersion` method:
  - [x] Get repository for the topic
  - [x] Get latest version from that repository
- [x] Write unit tests for all event operations

### 4.7 Update Lifecycle Management (Implement Seventh)
- [x] Enhance `Close` method:
  - [x] Close all repositories through the registry
  - [x] Clean up any other resources
- [x] Update `Health` method:
  - [x] Collect health information from all repositories
  - [x] Aggregate and return combined health status
- [x] Write unit tests for lifecycle methods

### 4.8 Integration Testing (Implement Last)
- [ ] Create integration tests for multi-repository scenarios:
  - [ ] Test topics with different repository adapters
  - [ ] Test repository reuse for identical configurations
  - [ ] Test error handling for adapter failures
  - [ ] Test persistence across EventStore instances
  - [ ] Test concurrent operations on multiple repositories

## 5. Topic Management Operations
- [ ] Implement `CreateTopic` method
  - [ ] Validate topic configuration
  - [ ] Create topic in repository
  - [ ] Record creation event in configuration topic
  - [ ] Update write model
- [ ] Implement `UpdateTopic` method
  - [ ] Validate topic configuration
  - [ ] Update topic in repository
  - [ ] Record update event in configuration topic
  - [ ] Update write model
- [ ] Implement `DeleteTopic` method
  - [ ] Validate topic name
  - [ ] Delete topic in repository
  - [ ] Record deletion event in configuration topic
  - [ ] Update write model
- [ ] Implement `GetTopic` method to retrieve a specific topic configuration
- [ ] Implement `ListTopics` method to list all topic configurations

## 6. Event Operations
- [ ] Implement `AppendEvents` method to add events to a topic
- [ ] Implement `GetEvents` method to retrieve events from a topic
- [ ] Implement `GetEventsByType` method to retrieve events of a specific type
- [ ] Implement `GetLatestVersion` method to get the latest event version for a topic

## 7. Lifecycle and Health Operations
- [ ] Implement `Health` method to report health information
- [ ] Implement `Close` method to clean up resources

## 8. Implement Repository Factory
- [ ] Create `factory.go` file for repository creation
- [ ] Implement `NewEventRepository` function to create repositories based on adapter type
- [ ] Add support for memory adapter
- [ ] Add support for file system adapter
- [ ] Add placeholder for future adapters (postgres, blob)

## 9. Add Error Handling and Validation
- [ ] Create validation functions for topic configurations
- [ ] Add proper error types and error handling
- [ ] Implement thread-safety with mutex locks
- [ ] Add proper logging throughout the codebase

## 10. Create Tests for EventStore
### 10.1 Unit Tests
- [ ] Create `eventstore_test.go` file
- [ ] Test EventStore initialization
- [ ] Test topic creation, update, and deletion
- [ ] Test configuration event handling
- [ ] Test write model rebuilding
- [ ] Test event operations
- [ ] Test error conditions and edge cases

### 10.2 Integration Tests
- [ ] Test with memory adapter
- [ ] Test with file system adapter
- [ ] Test persistence across EventStore instances
- [ ] Test concurrent operations

## 11. Documentation
- [ ] Add godoc comments to all exported functions and types
- [ ] Create usage examples
- [ ] Document event sourcing approach for configuration management
