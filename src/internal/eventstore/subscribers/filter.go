// Package subscribers contains the subscriber registry and filtering mechanisms for the EventStore.
package subscribers

import (
	"regexp"
	"strings"

	"goeventsource/src/internal/port/outbound"
)

// FilterType represents the type of filter
type FilterType string

const (
	// FilterTypeExact represents an exact match filter
	FilterTypeExact FilterType = "exact"
	// FilterTypePrefix represents a prefix match filter
	FilterTypePrefix FilterType = "prefix"
	// FilterTypeSuffix represents a suffix match filter
	FilterTypeSuffix FilterType = "suffix"
	// FilterTypeRegex represents a regex pattern match filter
	FilterTypeRegex FilterType = "regex"
)

// FilterOperation represents the operation for composite filters
type FilterOperation string

const (
	// FilterOperationAnd represents an AND operation between filters
	FilterOperationAnd FilterOperation = "and"
	// FilterOperationOr represents an OR operation between filters
	FilterOperationOr FilterOperation = "or"
)

// EventFilter defines the interface for all event filters
type EventFilter interface {
	Matches(event outbound.Event) bool
}

// TopicFilter filters events based on topic
type TopicFilter struct {
	Topics []string
}

// Matches returns true if the event's topic matches any in the filter
func (f *TopicFilter) Matches(event outbound.Event) bool {
	if len(f.Topics) == 0 {
		return true // If no topics specified, match all
	}

	for _, topic := range f.Topics {
		if topic == event.Topic {
			return true
		}
	}

	return false
}

// VersionFilter filters events based on version
type VersionFilter struct {
	FromVersion int64
}

// Matches returns true if the event's version is greater than or equal to the filter's FromVersion
func (f *VersionFilter) Matches(event outbound.Event) bool {
	return event.Version >= f.FromVersion
}

// EventTypeFilter filters events based on event types
type EventTypeFilter struct {
	EventTypes []string
	FilterType FilterType
	patterns   []*regexp.Regexp // Pre-compiled regex patterns for efficient matching
}

// NewEventTypeFilter creates a new EventTypeFilter with the given types and filter type
func NewEventTypeFilter(eventTypes []string, filterType FilterType) *EventTypeFilter {
	filter := &EventTypeFilter{
		EventTypes: eventTypes,
		FilterType: filterType,
	}

	// Pre-compile regex patterns if needed
	if filterType == FilterTypeRegex {
		filter.patterns = make([]*regexp.Regexp, 0, len(eventTypes))
		for _, pattern := range eventTypes {
			if regex, err := regexp.Compile(pattern); err == nil {
				filter.patterns = append(filter.patterns, regex)
			}
		}
	}

	return filter
}

// Matches returns true if the event's type matches the filter criteria
func (f *EventTypeFilter) Matches(event outbound.Event) bool {
	if len(f.EventTypes) == 0 {
		return true // If no event types specified, match all
	}

	switch f.FilterType {
	case FilterTypeExact:
		for _, eventType := range f.EventTypes {
			if eventType == event.Type {
				return true
			}
		}
	case FilterTypePrefix:
		for _, prefix := range f.EventTypes {
			if strings.HasPrefix(event.Type, prefix) {
				return true
			}
		}
	case FilterTypeSuffix:
		for _, suffix := range f.EventTypes {
			if strings.HasSuffix(event.Type, suffix) {
				return true
			}
		}
	case FilterTypeRegex:
		for _, pattern := range f.patterns {
			if pattern.MatchString(event.Type) {
				return true
			}
		}
	}

	return false
}

// MetadataFilter filters events based on metadata key-value pairs
type MetadataFilter struct {
	Metadata map[string]interface{}
}

// Matches returns true if the event's metadata contains all key-value pairs in the filter
func (f *MetadataFilter) Matches(event outbound.Event) bool {
	if len(f.Metadata) == 0 {
		return true // If no metadata specified, match all
	}

	for key, filterValue := range f.Metadata {
		eventValue, exists := event.Metadata[key]
		if !exists {
			return false
		}

		// Compare values
		if filterValue != eventValue {
			return false
		}
	}

	return true
}

// CompositeFilter combines multiple filters with a logical operation
type CompositeFilter struct {
	Filters   []EventFilter
	Operation FilterOperation
}

// Matches returns true if the combination of filters matches according to the operation
func (f *CompositeFilter) Matches(event outbound.Event) bool {
	if len(f.Filters) == 0 {
		return true // No filters means match all
	}

	switch f.Operation {
	case FilterOperationAnd:
		for _, filter := range f.Filters {
			if !filter.Matches(event) {
				return false
			}
		}
		return true
	case FilterOperationOr:
		for _, filter := range f.Filters {
			if filter.Matches(event) {
				return true
			}
		}
		return false
	default:
		// Default to AND for unknown operations
		for _, filter := range f.Filters {
			if !filter.Matches(event) {
				return false
			}
		}
		return true
	}
}

// CreateFilterFromConfig builds a filter from subscriber configuration
func CreateFilterFromConfig(config outbound.SubscriberFilter) EventFilter {
	filters := make([]EventFilter, 0)

	// Add version filter
	if config.FromVersion > 0 {
		filters = append(filters, &VersionFilter{
			FromVersion: config.FromVersion,
		})
	}

	// Add event type filter
	if len(config.EventTypes) > 0 {
		filters = append(filters, NewEventTypeFilter(config.EventTypes, FilterTypeExact))
	}

	// Add metadata filter
	if len(config.Metadata) > 0 {
		filters = append(filters, &MetadataFilter{
			Metadata: config.Metadata,
		})
	}

	// If only one filter, return it directly
	if len(filters) == 1 {
		return filters[0]
	}

	// Otherwise, create a composite AND filter
	return &CompositeFilter{
		Filters:   filters,
		Operation: FilterOperationAnd,
	}
}

// ShouldDeliverEvent determines if an event should be delivered to a subscriber
// based on the subscriber's filter criteria
func ShouldDeliverEvent(filter outbound.SubscriberFilter, event outbound.Event) bool {
	eventFilter := CreateFilterFromConfig(filter)
	return eventFilter.Matches(event)
}
