package fs

import (
	"errors"
	"fmt"
)

// Error types for the file system event repository
var (
	// ErrTopicNotFound is returned when a specified topic doesn't exist
	ErrTopicNotFound = errors.New("topic not found")

	// ErrTopicAlreadyExists is returned when trying to create a topic that already exists
	ErrTopicAlreadyExists = errors.New("topic already exists")

	// ErrInvalidEventData is returned when event data can't be marshaled or unmarshaled
	ErrInvalidEventData = errors.New("invalid event data")

	// ErrFileIO is returned when a file operation fails
	ErrFileIO = errors.New("file I/O error")

	// ErrInvalidFilename is returned when a filename doesn't match the expected format
	ErrInvalidFilename = errors.New("invalid filename format")
)

// TopicError wraps an error with topic information
type TopicError struct {
	TopicName string
	Err       error
}

// Error returns the error message with topic information
func (e *TopicError) Error() string {
	return fmt.Sprintf("topic %q: %v", e.TopicName, e.Err)
}

// Unwrap returns the wrapped error
func (e *TopicError) Unwrap() error {
	return e.Err
}

// NewTopicNotFoundError creates a new topic not found error
func NewTopicNotFoundError(topic string) error {
	return &TopicError{
		TopicName: topic,
		Err:       ErrTopicNotFound,
	}
}

// NewTopicAlreadyExistsError creates a new topic already exists error
func NewTopicAlreadyExistsError(topic string) error {
	return &TopicError{
		TopicName: topic,
		Err:       ErrTopicAlreadyExists,
	}
}

// FileError wraps an error with file path information
type FileError struct {
	FilePath string
	Err      error
}

// Error returns the error message with file path information
func (e *FileError) Error() string {
	return fmt.Sprintf("file %q: %v", e.FilePath, e.Err)
}

// Unwrap returns the wrapped error
func (e *FileError) Unwrap() error {
	return e.Err
}

// NewFileIOError creates a new file I/O error
func NewFileIOError(path string, err error) error {
	return &FileError{
		FilePath: path,
		Err:      fmt.Errorf("%w: %v", ErrFileIO, err),
	}
}

// IsTopicNotFound checks if error is topic not found
func IsTopicNotFound(err error) bool {
	return errors.Is(err, ErrTopicNotFound)
}

// IsTopicAlreadyExists checks if error is topic already exists
func IsTopicAlreadyExists(err error) bool {
	return errors.Is(err, ErrTopicAlreadyExists)
}

// IsFileIO checks if error is a file I/O error
func IsFileIO(err error) bool {
	return errors.Is(err, ErrFileIO)
}

// IsInvalidEventData checks if error is invalid event data
func IsInvalidEventData(err error) bool {
	return errors.Is(err, ErrInvalidEventData)
}

// IsInvalidFilename checks if error is invalid filename format
func IsInvalidFilename(err error) bool {
	return errors.Is(err, ErrInvalidFilename)
}
