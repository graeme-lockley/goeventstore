# EventStore Implementation Backlog

## Subscriber Implementation Task List

### Core Components

- [x] 1. Define the subscriber interface in `src/internal/port/outbound/subscriber.go`
   - Create interface with methods for receiving events
   - Include methods for lifecycle management (pause, resume, close)
   - Define error handling and timeout behavior contracts

- [x] 2. Create the subscriber model in `src/internal/eventstore/models/subscriber.go`
   - Define SubscriberConfig struct with ID, topic interests, and filtering criteria
   - Include timeout configuration with initial timeout duration
   - Add standoff configuration with initial, max, and multiplier values
   - Add retry configuration with max retry count
   - Include last activity timestamp field
   - Add subscription creation timestamp
   - Include subscriber state (active, paused, closed)

- [ ] 3. Implement the subscriber registry in `src/internal/eventstore/subscribers/registry.go`
   - Create thread-safe registry using sync.RWMutex
   - Implement Add, Get, List, Update, and Remove methods
   - Add methods for querying subscribers by topic, state
   - Implement subscription ID generation

- [ ] 4. Add subscription filtering logic in `src/internal/eventstore/subscribers/filter.go`
   - Implement topic-based filtering
   - Create event type filtering mechanism
   - Add version-based filtering logic (from_version)
   - Implement metadata filtering capability
   - Create composite filters with AND/OR logic
   - Include regex pattern matching for event types

- [ ] 5. Implement timeout configuration in subscriber model
   - Define TimeoutConfig struct with initial, maximum, and current timeout values
   - Include backoff multiplier for escalating timeouts
   - Add maximum retry count configuration
   - Create retry tracking with current retry count
   - Implement cooldown period after max retries

### Logging Infrastructure

- [ ] 6. Create structured logging for subscriber lifecycle events using native Go logging
   - Define standard log fields for subscriber operations
   - Implement log levels for different severity events
   - Create context-enriched logging with request IDs

- [ ] 7. Add registration logging with subscriber details
   - Log subscriber ID, topics, and configuration at registration
   - Include client information (IP, user agent if available)
   - Log registration timestamp and expiration if applicable

- [ ] 8. Implement deregistration logging with reason
   - Create log entries for manual deregistration with operator info
   - Add timeout-triggered deregistration logging with timeout details
   - Include error-triggered deregistration with error context
   - Log deregistration timestamp and subscription lifetime

- [ ] 9. Add event delivery attempt logging
   - Log event ID, subscriber ID, and timestamp for each delivery
   - Include delivery latency measurements
   - Track queue time before delivery attempt
   - Log delivery status (success, timeout, error)

- [ ] 10. Implement detailed error logging for failed deliveries
    - Log full error context including stack traces where available
    - Include retry count and next retry timing
    - Add correlation ID for tracking related error events
    - Log client connection details for network errors

### EventStore Integration

- [ ] 11. Extend EventStore interface with subscriber management methods
    - Add RegisterSubscriber method with subscriber configuration
    - Include DeregisterSubscriber method with reason parameter
    - Add ListSubscribers method with filtering options
    - Create GetSubscriber method for detailed subscriber info
    - Implement UpdateSubscriber method for configuration changes

- [ ] 12. Modify EventStore implementation to include subscriber registry
    - Add subscriber registry as a field in EventStore
    - Initialize registry in EventStore constructor
    - Include registry in health check reporting
    - Add registry cleanup on EventStore shutdown

- [ ] 13. Implement subscriber registration method with logging
    - Validate subscriber configuration
    - Generate unique subscriber ID
    - Store subscriber in registry
    - Log successful registration with details
    - Return subscription details including ID

- [ ] 14. Implement subscriber deregistration method with logging
    - Find subscriber in registry
    - Close subscriber channels
    - Remove from registry
    - Log deregistration with reason
    - Clean up any pending events

- [ ] 15. Add event broadcasting to subscribers in AppendEvents method
    - Find subscribers interested in topic
    - Apply filtering based on subscriber preferences
    - Launch goroutine for fan-out delivery
    - Track successful delivery count
    - Log broadcast statistics

- [ ] 16. Implement automatic deregistration for timed-out callbacks
    - Create monitor goroutine that checks for timed-out subscribers
    - Apply configured timeout policy
    - Execute deregistration for expired subscribers
    - Log timeout deregistration events
    - Update health metrics after deregistration

### Channel Management

- [ ] 17. Create a channel factory for subscriber communication
    - Implement channel creation with configurable buffer size
    - Add channel metadata (creation time, subscriber ID)
    - Create channel registry for tracking active channels
    - Implement channel health check functionality

- [ ] 18. Implement buffered channel creation with configurable buffer size
    - Allow buffer size configuration per subscriber
    - Set reasonable defaults based on expected event volume
    - Add overflow handling for full channels
    - Implement channel resizing capability if needed

- [ ] 19. Create a channel manager for handling subscriber channels
    - Implement thread-safe channel tracking
    - Add channel lifecycle management (create, pause, resume, close)
    - Create channel statistics collection
    - Add periodic channel health checking

- [ ] 20. Implement timeout detection based on subscriber configuration
    - Create timeout context for each event delivery
    - Track delivery time and compare against timeout threshold
    - Implement escalating timeout with configurable backoff
    - Add jitter to prevent thundering herd problems
    - Track timeout statistics per subscriber

- [ ] 21. Add channel cleanup logic for disconnected subscribers
    - Detect channel closure or disconnection
    - Clean up any pending events
    - Release channel resources
    - Update registry to reflect channel state
    - Log channel cleanup operations

- [ ] 22. Create timeout monitoring goroutine for each subscription
    - Launch dedicated goroutine per subscriber for timeout monitoring
    - Implement graceful shutdown of monitoring goroutines
    - Use context cancellation for cleanup
    - Track goroutine lifecycle in registry
    - Implement resource limits for total goroutines

### Event Broadcasting

- [ ] 23. Implement fan-out mechanism to deliver events to multiple subscribers
    - Create fan-out manager that tracks delivery targets
    - Use worker pool for scalable delivery
    - Implement delivery prioritization if needed
    - Track fan-out statistics (subscribers reached, time taken)
    - Add circuit breaking for overload protection

- [ ] 24. Create non-blocking event delivery using goroutines
    - Launch goroutine for each delivery attempt
    - Implement context with timeout for each delivery
    - Use select statements with timeout cases
    - Add delivery status tracking
    - Implement graceful cancellation

- [ ] 25. Add event filtering based on subscriber interest
    - Apply topic filters from subscription configuration
    - Filter events based on event type patterns
    - Add metadata-based filtering
    - Implement version range filtering
    - Create composite filter chains

- [ ] 26. Implement version-based filtering (from_version parameter)
    - Track last delivered version per subscriber
    - Filter events with versions below the from_version
    - Update version tracking after successful delivery
    - Handle version reset on resubscription
    - Add version conflict detection

- [ ] 27. Add event type filtering based on subscriber preferences
    - Support exact match event type filtering
    - Implement prefix/suffix matching
    - Add regex pattern matching for event types
    - Support inclusion and exclusion patterns
    - Create type hierarchy matching if applicable

- [ ] 28. Create timeout handling for event delivery with automatic deregistration
    - Set delivery timeout based on subscriber configuration
    - Implement escalating timeouts with configurable backoff
    - Add maximum retry limits before deregistration
    - Track timeout history per subscriber
    - Log timeout details including retry count

### Subscription Management

- [ ] 29. Implement subscription lifecycle management (create, pause, resume, cancel)
    - Create state machine for subscription lifecycle
    - Add transition validation between states
    - Implement pause/resume functionality
    - Create subscription cancellation with reason
    - Log all state transitions

- [ ] 30. Create subscription expiration mechanism with configurable TTL
    - Add TTL configuration in subscription model
    - Implement expiration checking in monitoring goroutine
    - Send expiration warning events before deregistration
    - Add TTL extension functionality
    - Log expiration events

- [ ] 31. Add subscription health checking mechanism
    - Define health criteria (event lag, error rate)
    - Implement periodic health check routine
    - Add health status reporting in subscriber API
    - Create automatic remediation for unhealthy subscriptions
    - Log health status changes

- [ ] 32. Implement timeout-based subscription cleanup
    - Configure maximum timeout duration
    - Track cumulative timeout duration
    - Implement escalating standoff periods between retries
    - Initial standoff: 1 second
    - Maximum standoff: 30 seconds
    - Standoff multiplier: 2.0 (doubles each time)
    - Maximum retry count: 5
    - Deregister after exceeding max retries or total timeout

- [ ] 33. Create subscription statistics tracking
    - Track events delivered count
    - Add error count and types
    - Measure average delivery latency
    - Track timeout occurrences
    - Implement delivery success rate calculation
    - Log statistics periodically

- [ ] 34. Add last activity timestamp tracking for timeouts
    - Update timestamp on every successful interaction
    - Create idle timeout based on last activity
    - Implement different timeouts for different operations
    - Add activity type tracking
    - Log inactivity warnings before timeout

### API Integration (Data Plane)

- [ ] 35. Implement the subscription handler in `src/internal/api/handlers/subscribers.go`
    - Create HTTP handler for subscription endpoints
    - Add request parsing and validation
    - Implement error handling and response formatting
    - Add security headers and CORS support if needed
    - Create rate limiting for subscription requests

- [ ] 36. Add validation for subscription request parameters
    - Validate topic existence
    - Check timeout configuration boundaries
    - Validate filter expressions
    - Implement retry configuration validation
    - Add input sanitation for security

- [ ] 37. Create endpoint for checking individual subscription status
    - Implement GET handler for subscription status
    - Return current state, statistics, and health
    - Add detailed error history if applicable
    - Include next scheduled retry if in backoff
    - Implement ETag/If-None-Match for efficient polling

- [ ] 38. Implement endpoint for updating subscription parameters
    - Create PATCH handler for subscription updates
    - Allow timeout configuration changes
    - Support filter updates
    - Implement validation for updates
    - Log all configuration changes

- [ ] 39. Add endpoint for canceling individual subscriptions
    - Create DELETE handler for subscription cancellation
    - Implement graceful cancellation with pending event handling
    - Add cancellation reason parameter
    - Return cancellation summary
    - Log cancellation with context

### API Integration (Control Plane)

- [ ] 40. Create admin endpoint for listing all active subscriptions
    - Implement GET handler with filtering parameters
    - Add pagination for large subscription lists
    - Include summary statistics per subscription
    - Support sorting by various criteria
    - Implement field selection for response customization

- [ ] 41. Implement admin endpoint for forcibly removing subscriptions
    - Create DELETE handler with subscriber ID parameter
    - Add force parameter for immediate removal
    - Implement batch removal capability
    - Return removal summary with counts
    - Log forced removals with operator context

- [ ] 42. Add subscription statistics endpoint for monitoring
    - Create GET handler for aggregated statistics
    - Implement time range parameters
    - Add grouping by topic, client, or error type
    - Support different output formats (JSON, Prometheus)
    - Include health score calculation

- [ ] 43. Implement subscription filtering in admin endpoints
    - Add filter parameters for topic, state, health
    - Implement time-based filtering (created, last active)
    - Support filtering by error count or type
    - Add custom query language if needed
    - Implement filter composition (AND/OR)

- [ ] 44. Create detailed subscription information endpoint
    - Implement GET handler for single subscription details
    - Include complete configuration
    - Add event delivery history
    - Show error history with context
    - Provide timeout and retry statistics

### Timeout Handling

- [ ] 45. Implement configurable timeout duration during subscription registration
    - Add timeout parameters in subscription request
    - Set reasonable defaults if not specified
    - Validate timeout values against system limits
    - Store timeout configuration in subscriber model
    - Log configured timeout values

- [ ] 46. Create context with timeout for each event delivery
    - Use context.WithTimeout for delivery operations
    - Pass timeout context to all delivery goroutines
    - Implement context cancellation on timeout
    - Add context metadata for tracking
    - Log context creation and cancellation

- [ ] 47. Add timeout detection logic for subscriber callbacks
    - Track start time for each delivery attempt
    - Compare elapsed time against timeout threshold
    - Implement timeout signal channel
    - Create timeout event structure
    - Log detailed timeout information

- [ ] 48. Implement automatic deregistration on timeout expiration
    - Configure maximum total timeout duration
    - Track cumulative timeout across retries
    - Implement escalating standoff with configurable multiplier
    - Initial standoff: 1 second
    - Maximum standoff: 30 seconds
    - Standoff multiplier: 2.0 (doubles each time)
    - Maximum retry count: 5
    - Add grace period before final deregistration
    - Log deregistration decision with full context

- [ ] 49. Create timeout logging with subscriber details
    - Log timeout occurrence with timestamp
    - Include full subscription configuration
    - Add current retry count and limits
    - Log next scheduled retry with backoff details
    - Include event information that triggered timeout

### Concurrency & Thread Safety

- [ ] 50. Implement thread-safe subscriber registry with RWMutex
    - Use sync.RWMutex for registry access
    - Prefer read locks for non-modifying operations
    - Implement fine-grained locking for better concurrency
    - Add deadlock detection in debug mode
    - Create lock contention metrics

- [ ] 51. Create context-aware event delivery with timeouts and cancellation
    - Use context package for all operations
    - Propagate cancellation through call chain
    - Add timeout to all contexts
    - Implement clean resource release on cancellation
    - Log context completion reason

- [ ] 52. Add graceful shutdown mechanism for active subscriptions
    - Implement shutdown signal handling
    - Send advance warning to subscribers about shutdown
    - Allow in-flight events to complete with deadline
    - Track shutdown progress
    - Log complete shutdown sequence

- [ ] 53. Implement backpressure handling for slow subscribers
    - Detect slow subscribers based on queue depth
    - Apply backpressure through flow control
    - Implement event batching for efficiency
    - Add circuit breaking for persistent slowness
    - Log backpressure application and release

- [ ] 54. Create reconnection mechanism for subscribers with temporary issues
    - Detect connection issues vs. application errors
    - Implement automatic reconnection attempts
    - Use exponential backoff between attempts
    - Add reconnection limit before deregistration
    - Log reconnection attempts and results

- [ ] 55. Implement concurrent delivery with goroutines for each subscriber
    - Create worker pool for delivery operations
    - Balance concurrency with resource constraints
    - Implement worker lifecycle management
    - Add worker metrics and health monitoring
    - Create adaptive worker scaling based on load

### Error Handling & Recovery

- [ ] 56. Implement comprehensive error handling for subscription operations
    - Create error types for different failure categories
    - Add error context enrichment
    - Implement error severity classification
    - Create recovery actions for different error types
    - Log errors with full context

- [ ] 57. Create a retry mechanism for failed event deliveries
    - Implement retry queue for failed deliveries
    - Add exponential backoff with jitter
    - Set maximum retry counts per event
    - Create retry history tracking
    - Log each retry attempt with context

- [ ] 58. Add logging for subscription errors and event delivery issues
    - Create structured error logs with context
    - Include subscriber details and event information
    - Add error categorization and tagging
    - Implement correlation IDs across log entries
    - Create log severity levels based on error impact

- [ ] 59. Implement circuit breaker for consistently failing subscribers
    - Define failure thresholds for circuit breaking
    - Create circuit states (closed, open, half-open)
    - Implement automatic recovery attempts
    - Add circuit state change notifications
    - Log all circuit state transitions

- [ ] 60. Create dead-letter mechanism for undeliverable events
    - Implement dead-letter storage for failed events
    - Add metadata about failure reason and attempts
    - Create API for querying dead-letter events
    - Implement retry capabilities from dead-letter
    - Log events sent to dead-letter

- [ ] 61. Add automatic deregistration after configurable number of failures
    - Configure maximum consecutive failure count
    - Track failure sequences per subscriber
    - Reset counter on successful delivery
    - Implement grace period before deregistration
    - Log approaching deregistration threshold as warning

### Testing

- [ ] 62. Write unit tests for subscriber registry
    - Test thread safety with concurrent operations
    - Verify correct registration and deregistration
    - Test filtering capabilities
    - Add error case testing
    - Implement performance benchmarks

- [ ] 63. Create test suite for event broadcasting mechanism
    - Test successful delivery to multiple subscribers
    - Verify error handling during broadcast
    - Test timeout handling with mock slow subscribers
    - Add concurrency testing with many subscribers
    - Create delivery order verification

- [ ] 64. Implement integration tests for subscription API
    - Test full request/response cycle
    - Verify correct status codes and response formats
    - Test input validation error handling
    - Add authentication/authorization testing if applicable
    - Create long-running subscription tests

- [ ] 65. Add benchmark tests for concurrent event broadcasting
    - Measure throughput under various loads
    - Test with different channel buffer sizes
    - Measure latency distributions
    - Test with varying subscriber counts
    - Identify concurrency bottlenecks

- [ ] 66. Create load tests for subscriber scalability
    - Test with hundreds/thousands of subscribers
    - Measure memory usage under load
    - Test CPU utilization with high event volume
    - Verify system stability under sustained load
    - Identify resource limits and breaking points

- [ ] 67. Implement timeout testing with simulated slow subscribers
    - Create deliberately slow subscriber mocks
    - Test timeout detection accuracy
    - Verify correct backoff implementation
    - Test deregistration after max retries
    - Verify resource cleanup after timeout

- [ ] 68. Add control plane API testing for subscription management
    - Test admin endpoints functionality
    - Verify filtering and pagination
    - Test forced deregistration behavior
    - Verify statistics accuracy
    - Create security and access control tests

### Documentation

- [ ] 69. Update API documentation with subscription endpoints
    - Document all endpoint URLs, methods, and parameters
    - Add request/response examples
    - Document error codes and handling
    - Include rate limiting information
    - Add security considerations

- [ ] 70. Create examples for subscribing to events
    - Provide curl examples for subscription creation
    - Add example client code in multiple languages
    - Include filtering examples
    - Demonstrate timeout configuration
    - Add reconnection handling examples

- [ ] 71. Document subscription filtering options
    - Explain topic filtering syntax
    - Document event type filtering patterns
    - Add metadata filtering examples
    - Include complex filter combinations
    - Provide performance considerations

- [ ] 72. Add operational documentation for subscription monitoring
    - Document available monitoring endpoints
    - Add Prometheus/Grafana integration examples
    - Include alerting recommendations
    - Document common operational issues
    - Add troubleshooting guide

- [ ] 73. Update README with subscription features
    - Add subscription feature overview
    - Include quick start guide
    - Document key configuration options
    - Add performance considerations
    - Include security best practices

- [ ] 74. Document timeout configuration options
    - Explain timeout parameters and defaults
    - Document backoff algorithm and configuration
    - Include retry limits and behavior
    - Add examples for different use cases
    - Provide performance impact considerations

- [ ] 75. Add control plane documentation for subscription management
    - Document admin endpoints and authentication
    - Explain subscription lifecycle management
    - Add subscription monitoring best practices
    - Include capacity planning guidelines
    - Document security considerations

