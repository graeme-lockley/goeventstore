package fs

import (
	"errors"
	"testing"
)

func TestCustomErrors(t *testing.T) {
	// Test topic not found error
	err := NewTopicNotFoundError("test-topic")
	if !IsTopicNotFound(err) {
		t.Errorf("Expected IsTopicNotFound to return true")
	}
	if errors.Is(err, ErrTopicAlreadyExists) {
		t.Errorf("Error should not be of type ErrTopicAlreadyExists")
	}

	// Test topic already exists error
	err = NewTopicAlreadyExistsError("test-topic")
	if !IsTopicAlreadyExists(err) {
		t.Errorf("Expected IsTopicAlreadyExists to return true")
	}
	if errors.Is(err, ErrTopicNotFound) {
		t.Errorf("Error should not be of type ErrTopicNotFound")
	}

	// Test file IO error
	fileErr := NewFileIOError("/path/to/file", errors.New("test error"))
	if !IsFileIO(fileErr) {
		t.Errorf("Expected IsFileIO to return true")
	}

	// Test error unwrapping
	var topicErr *TopicError
	if !errors.As(err, &topicErr) {
		t.Errorf("Expected to unwrap to TopicError")
	}

	if topicErr.TopicName != "test-topic" {
		t.Errorf("Expected topic name to be 'test-topic', got '%s'", topicErr.TopicName)
	}

	// Test error messages
	tnfErr := NewTopicNotFoundError("orders")
	if tnfErr.Error() != `topic "orders": topic not found` {
		t.Errorf("Unexpected error message: %s", tnfErr.Error())
	}

	fileIOErr := NewFileIOError("/data/file.json", errors.New("permission denied"))
	if !errors.Is(fileIOErr, ErrFileIO) {
		t.Errorf("Expected IsFileIO to return true")
	}
}
