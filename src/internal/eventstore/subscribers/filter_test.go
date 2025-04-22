package subscribers

import (
	"testing"

	"goeventsource/src/internal/port/outbound"
)

func TestTopicFilter(t *testing.T) {
	tests := []struct {
		name     string
		filter   TopicFilter
		event    outbound.Event
		expected bool
	}{
		{
			name:     "empty topics should match all",
			filter:   TopicFilter{Topics: []string{}},
			event:    outbound.Event{Topic: "users"},
			expected: true,
		},
		{
			name:     "matching topic should match",
			filter:   TopicFilter{Topics: []string{"users", "orders"}},
			event:    outbound.Event{Topic: "users"},
			expected: true,
		},
		{
			name:     "non-matching topic should not match",
			filter:   TopicFilter{Topics: []string{"users", "orders"}},
			event:    outbound.Event{Topic: "products"},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.filter.Matches(tt.event)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestVersionFilter(t *testing.T) {
	tests := []struct {
		name     string
		filter   VersionFilter
		event    outbound.Event
		expected bool
	}{
		{
			name:     "event version greater than filter should match",
			filter:   VersionFilter{FromVersion: 5},
			event:    outbound.Event{Version: 10},
			expected: true,
		},
		{
			name:     "event version equal to filter should match",
			filter:   VersionFilter{FromVersion: 5},
			event:    outbound.Event{Version: 5},
			expected: true,
		},
		{
			name:     "event version less than filter should not match",
			filter:   VersionFilter{FromVersion: 5},
			event:    outbound.Event{Version: 3},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.filter.Matches(tt.event)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestEventTypeFilter(t *testing.T) {
	tests := []struct {
		name       string
		eventTypes []string
		filterType FilterType
		event      outbound.Event
		expected   bool
	}{
		{
			name:       "empty event types should match all",
			eventTypes: []string{},
			filterType: FilterTypeExact,
			event:      outbound.Event{Type: "UserCreated"},
			expected:   true,
		},
		// Exact match tests
		{
			name:       "exact match - matching event type",
			eventTypes: []string{"UserCreated", "UserUpdated"},
			filterType: FilterTypeExact,
			event:      outbound.Event{Type: "UserCreated"},
			expected:   true,
		},
		{
			name:       "exact match - non-matching event type",
			eventTypes: []string{"UserCreated", "UserUpdated"},
			filterType: FilterTypeExact,
			event:      outbound.Event{Type: "UserDeleted"},
			expected:   false,
		},
		// Prefix match tests
		{
			name:       "prefix match - matching prefix",
			eventTypes: []string{"User"},
			filterType: FilterTypePrefix,
			event:      outbound.Event{Type: "UserCreated"},
			expected:   true,
		},
		{
			name:       "prefix match - non-matching prefix",
			eventTypes: []string{"User"},
			filterType: FilterTypePrefix,
			event:      outbound.Event{Type: "OrderCreated"},
			expected:   false,
		},
		// Suffix match tests
		{
			name:       "suffix match - matching suffix",
			eventTypes: []string{"Created"},
			filterType: FilterTypeSuffix,
			event:      outbound.Event{Type: "UserCreated"},
			expected:   true,
		},
		{
			name:       "suffix match - non-matching suffix",
			eventTypes: []string{"Created"},
			filterType: FilterTypeSuffix,
			event:      outbound.Event{Type: "UserUpdated"},
			expected:   false,
		},
		// Regex match tests
		{
			name:       "regex match - matching pattern",
			eventTypes: []string{"User.*"},
			filterType: FilterTypeRegex,
			event:      outbound.Event{Type: "UserCreated"},
			expected:   true,
		},
		{
			name:       "regex match - non-matching pattern",
			eventTypes: []string{"User.*"},
			filterType: FilterTypeRegex,
			event:      outbound.Event{Type: "OrderCreated"},
			expected:   false,
		},
		{
			name:       "regex match - complex pattern",
			eventTypes: []string{"(User|Order)Created"},
			filterType: FilterTypeRegex,
			event:      outbound.Event{Type: "OrderCreated"},
			expected:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter := NewEventTypeFilter(tt.eventTypes, tt.filterType)
			result := filter.Matches(tt.event)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestMetadataFilter(t *testing.T) {
	tests := []struct {
		name     string
		filter   MetadataFilter
		event    outbound.Event
		expected bool
	}{
		{
			name:     "empty metadata should match all",
			filter:   MetadataFilter{Metadata: map[string]interface{}{}},
			event:    outbound.Event{Metadata: map[string]interface{}{"userId": "123"}},
			expected: true,
		},
		{
			name: "matching metadata should match",
			filter: MetadataFilter{Metadata: map[string]interface{}{
				"userId": "123",
			}},
			event: outbound.Event{Metadata: map[string]interface{}{
				"userId":   "123",
				"tenantId": "456",
			}},
			expected: true,
		},
		{
			name: "non-matching metadata value should not match",
			filter: MetadataFilter{Metadata: map[string]interface{}{
				"userId": "123",
			}},
			event: outbound.Event{Metadata: map[string]interface{}{
				"userId":   "456",
				"tenantId": "456",
			}},
			expected: false,
		},
		{
			name: "missing metadata key should not match",
			filter: MetadataFilter{Metadata: map[string]interface{}{
				"userId": "123",
			}},
			event: outbound.Event{Metadata: map[string]interface{}{
				"tenantId": "456",
			}},
			expected: false,
		},
		{
			name: "multiple metadata keys should match exactly",
			filter: MetadataFilter{Metadata: map[string]interface{}{
				"userId":   "123",
				"tenantId": "456",
			}},
			event: outbound.Event{Metadata: map[string]interface{}{
				"userId":   "123",
				"tenantId": "456",
				"region":   "us-west",
			}},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.filter.Matches(tt.event)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestCompositeFilter(t *testing.T) {
	tests := []struct {
		name      string
		filters   []EventFilter
		operation FilterOperation
		event     outbound.Event
		expected  bool
	}{
		{
			name:      "empty filters should match all",
			filters:   []EventFilter{},
			operation: FilterOperationAnd,
			event:     outbound.Event{},
			expected:  true,
		},
		{
			name: "AND operation - all filters match",
			filters: []EventFilter{
				&VersionFilter{FromVersion: 5},
				&TopicFilter{Topics: []string{"users"}},
				NewEventTypeFilter([]string{"UserCreated"}, FilterTypeExact),
			},
			operation: FilterOperationAnd,
			event: outbound.Event{
				Version: 10,
				Topic:   "users",
				Type:    "UserCreated",
			},
			expected: true,
		},
		{
			name: "AND operation - one filter doesn't match",
			filters: []EventFilter{
				&VersionFilter{FromVersion: 5},
				&TopicFilter{Topics: []string{"users"}},
				NewEventTypeFilter([]string{"UserCreated"}, FilterTypeExact),
			},
			operation: FilterOperationAnd,
			event: outbound.Event{
				Version: 10,
				Topic:   "users",
				Type:    "UserUpdated",
			},
			expected: false,
		},
		{
			name: "OR operation - one filter matches",
			filters: []EventFilter{
				&VersionFilter{FromVersion: 20},
				&TopicFilter{Topics: []string{"users"}},
				NewEventTypeFilter([]string{"UserCreated"}, FilterTypeExact),
			},
			operation: FilterOperationOr,
			event: outbound.Event{
				Version: 10,
				Topic:   "users",
				Type:    "UserUpdated",
			},
			expected: true,
		},
		{
			name: "OR operation - no filters match",
			filters: []EventFilter{
				&VersionFilter{FromVersion: 20},
				&TopicFilter{Topics: []string{"users"}},
				NewEventTypeFilter([]string{"UserCreated"}, FilterTypeExact),
			},
			operation: FilterOperationOr,
			event: outbound.Event{
				Version: 10,
				Topic:   "orders",
				Type:    "OrderCreated",
			},
			expected: false,
		},
		{
			name: "Unknown operation defaults to AND",
			filters: []EventFilter{
				&VersionFilter{FromVersion: 5},
				&TopicFilter{Topics: []string{"users"}},
			},
			operation: "unknown",
			event: outbound.Event{
				Version: 10,
				Topic:   "users",
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter := &CompositeFilter{
				Filters:   tt.filters,
				Operation: tt.operation,
			}
			result := filter.Matches(tt.event)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestCreateFilterFromConfig(t *testing.T) {
	tests := []struct {
		name     string
		config   outbound.SubscriberFilter
		event    outbound.Event
		expected bool
	}{
		{
			name: "empty config should match all",
			config: outbound.SubscriberFilter{
				EventTypes:  []string{},
				FromVersion: 0,
				Metadata:    map[string]interface{}{},
			},
			event: outbound.Event{
				Type:    "UserCreated",
				Version: 1,
				Topic:   "users",
			},
			expected: true,
		},
		{
			name: "event type filter only",
			config: outbound.SubscriberFilter{
				EventTypes:  []string{"UserCreated", "UserUpdated"},
				FromVersion: 0,
				Metadata:    map[string]interface{}{},
			},
			event: outbound.Event{
				Type:    "UserCreated",
				Version: 1,
				Topic:   "users",
			},
			expected: true,
		},
		{
			name: "version filter only",
			config: outbound.SubscriberFilter{
				EventTypes:  []string{},
				FromVersion: 5,
				Metadata:    map[string]interface{}{},
			},
			event: outbound.Event{
				Type:    "UserCreated",
				Version: 10,
				Topic:   "users",
			},
			expected: true,
		},
		{
			name: "metadata filter only",
			config: outbound.SubscriberFilter{
				EventTypes:  []string{},
				FromVersion: 0,
				Metadata: map[string]interface{}{
					"userId": "123",
				},
			},
			event: outbound.Event{
				Type:    "UserCreated",
				Version: 1,
				Topic:   "users",
				Metadata: map[string]interface{}{
					"userId": "123",
				},
			},
			expected: true,
		},
		{
			name: "combined filters - all match",
			config: outbound.SubscriberFilter{
				EventTypes:  []string{"UserCreated"},
				FromVersion: 5,
				Metadata: map[string]interface{}{
					"userId": "123",
				},
			},
			event: outbound.Event{
				Type:    "UserCreated",
				Version: 10,
				Topic:   "users",
				Metadata: map[string]interface{}{
					"userId": "123",
				},
			},
			expected: true,
		},
		{
			name: "combined filters - one doesn't match",
			config: outbound.SubscriberFilter{
				EventTypes:  []string{"UserCreated"},
				FromVersion: 5,
				Metadata: map[string]interface{}{
					"userId": "123",
				},
			},
			event: outbound.Event{
				Type:    "UserUpdated",
				Version: 10,
				Topic:   "users",
				Metadata: map[string]interface{}{
					"userId": "123",
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter := CreateFilterFromConfig(tt.config)
			result := filter.Matches(tt.event)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestShouldDeliverEvent(t *testing.T) {
	tests := []struct {
		name     string
		filter   outbound.SubscriberFilter
		event    outbound.Event
		expected bool
	}{
		{
			name: "combined filters - should deliver",
			filter: outbound.SubscriberFilter{
				EventTypes:  []string{"UserCreated"},
				FromVersion: 5,
				Metadata: map[string]interface{}{
					"userId": "123",
				},
			},
			event: outbound.Event{
				Type:    "UserCreated",
				Version: 10,
				Topic:   "users",
				Metadata: map[string]interface{}{
					"userId": "123",
				},
			},
			expected: true,
		},
		{
			name: "combined filters - should not deliver",
			filter: outbound.SubscriberFilter{
				EventTypes:  []string{"UserCreated"},
				FromVersion: 5,
				Metadata: map[string]interface{}{
					"userId": "123",
				},
			},
			event: outbound.Event{
				Type:    "UserUpdated",
				Version: 10,
				Topic:   "users",
				Metadata: map[string]interface{}{
					"userId": "123",
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ShouldDeliverEvent(tt.filter, tt.event)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}
