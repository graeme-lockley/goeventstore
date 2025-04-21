# File System Storage Adaptor Implementation Backlog

## 1. Core Infrastructure

- [x] 1.1. Define the `FileSystemEventRepository` struct with required fields:
   - Base path for storage
   - Read-write mutex for thread safety
   - Maps for caching latest versions and topic configs

- [x] 1.2. Implement constructor function `NewFileSystemEventRepository`
   - Accept base directory path parameter
   - Initialize maps and internal data structures

- [x] 1.3. Create directory structure helper functions
   - Create base directory function
   - Create configs subdirectory function
   - Create events subdirectory function
   - Create topic-specific subdirectories function

## 2. Initialization and Lifecycle

- [x] 2.1. Implement the `Initialize` method
   - Create the base directory structure if it doesn't exist
   - Load existing topic configurations from the configs directory
   - Determine latest versions for each topic
   - Handle initialization errors appropriately

- [x] 2.2. Implement the `Close` method
   - Add any necessary cleanup operations

- [x] 2.3. Implement `determineLatestVersion` helper function
   - Scan topic directory to find highest event version

## 3. Event Operations

- [ ] 3.1. Implement `AppendEvents` method
   - Add thread-safe locking
   - Verify topic exists
   - Get latest version from cache
   - Ensure topic directory exists
   - Generate missing event fields (ID, timestamp)
   - Increment and assign version numbers
   - Marshal events to JSON format
   - Create padded filename format for events
   - Implement atomic file writes using temp files
   - Update latest version cache

- [ ] 3.2. Implement `GetEvents` method
   - Add read locks for thread safety
   - Verify topic exists
   - Read directory entries for the topic
   - Filter events by version
   - Read and unmarshal event files
   - Sort events by version
   - Return ordered event list

- [ ] 3.3. Implement `GetEventsByType` method
   - Leverage GetEvents functionality
   - Filter results by event type

- [ ] 3.4. Implement `GetLatestVersion` method
   - Use cached version for efficiency
   - Handle errors for non-existent topics

## 4. Topic Management

- [ ] 4.1. Implement `CreateTopic` method
   - Verify topic doesn't already exist
   - Create topic directory
   - Marshal and save topic configuration
   - Update in-memory state

- [ ] 4.2. Implement `DeleteTopic` method
   - Verify topic exists
   - Remove topic directory and files
   - Remove topic configuration file
   - Update in-memory state

- [ ] 4.3. Implement `ListTopics` method
   - Return list of all topic configurations

- [ ] 4.4. Implement `TopicExists` method
   - Check in-memory map for efficiency

- [ ] 4.5. Implement `UpdateTopicConfig` method
   - Verify topic exists
   - Marshal updated configuration
   - Implement atomic file update
   - Update in-memory state

- [ ] 4.6. Implement `GetTopicConfig` method
   - Return cached config for efficiency

## 5. Health and Monitoring

- [ ] 5.1. Implement `Health` method
   - Report status of the repository
   - Include topic count and event counts
   - Add disk space information
   - Report any encountered errors

## 6. File System Utilities

- [ ] 6.1. Implement file locking mechanism for concurrent access

- [ ] 6.2. Create atomic file write utilities

- [ ] 6.3. Implement proper error handling for file operations

- [ ] 6.4. Add directory existence checking functions

- [ ] 6.5. Create event file naming convention utilities

## 7. Unit Tests

- [ ] 7.1. Test constructor and initialization
   - Test with existing and non-existing directories
   - Test loading existing configurations

- [ ] 7.2. Test event operations
   - Test appending events
   - Test retrieving events by version
   - Test retrieving events by type
   - Test version management
   - Test event ID and timestamp generation

- [ ] 7.3. Test topic management
   - Test topic creation
   - Test topic deletion
   - Test listing topics
   - Test topic existence checks
   - Test updating topic configuration
   - Test retrieving topic configuration

- [ ] 7.4. Test error conditions
   - Test operations on non-existent topics
   - Test concurrent access scenarios
   - Test file system error handling
   - Test invalid JSON handling

- [ ] 7.5. Test health reporting
   - Verify correct status reporting
   - Verify event counts
   - Test disk space reporting

## 8. Integration Tests

- [ ] 8.1. Test persistence across application restarts
   - Create topics and add events
   - Close and reopen repository
   - Verify all data is preserved

- [ ] 8.2. Test concurrent operations
   - Test multiple goroutines writing to the same topic
   - Test concurrent reads and writes
   - Test concurrent topic operations

- [ ] 8.3. Test performance
   - Benchmark appending large numbers of events
   - Benchmark retrieving events with various filters
   - Test with different directory sizes and depths

- [ ] 8.4. Test disk space management
   - Test behavior when disk space is low
   - Test with different file permissions

## 9. Edge Case Handling

- [ ] 9.1. Implement safety checks for maximum file counts

- [ ] 9.2. Add error handling for filesystem-specific limits

- [ ] 9.3. Add recovery mechanisms for partially written files

- [ ] 9.4. Handle long file paths and special characters in event data

## 10. Documentation

- [ ] 10.1. Add inline documentation for all public functions

- [ ] 10.2. Create usage examples

- [ ] 10.3. Document performance characteristics

- [ ] 10.4. Add troubleshooting guides for common issues

## 11. Optimization (Optional)

- [ ] 11.1. Implement event partitioning for large topics

- [ ] 11.2. Add optional compression support

- [ ] 11.3. Create index files for faster retrieval

- [ ] 11.4. Implement caching strategies for frequent queries 