// Package eventstore provides a high-level interface to the event repository
// with configuration management through events.
package eventstore

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"goeventsource/src/internal/adapters/fs"
	"goeventsource/src/internal/adapters/memory"
	"goeventsource/src/internal/eventstore/models"
	"goeventsource/src/internal/port/outbound"
)

// Common errors for repository operations
var (
	ErrUnknownAdapter       = errors.New("unknown adapter type")
	ErrRepositoryInitFailed = errors.New("failed to initialize repository")
	ErrRepositoryNotFound   = errors.New("repository not found for the given configuration")
	ErrInvalidConfiguration = errors.New("invalid repository configuration")
	ErrRepositoryClosed     = errors.New("repository is closed and cannot be used")
)

// RepositoryRegistry manages multiple repositories based on their configurations
type RepositoryRegistry struct {
	mu           sync.RWMutex
	repositories map[string]outbound.EventRepository // map[adapter:connection]repository
	isShutdown   bool                                // Flag to track registry shutdown state
}

// NewRepositoryRegistry creates a new repository registry
func NewRepositoryRegistry() *RepositoryRegistry {
	return &RepositoryRegistry{
		repositories: make(map[string]outbound.EventRepository),
		isShutdown:   false,
	}
}

// Get retrieves a repository by adapter and connection string
func (r *RepositoryRegistry) Get(adapter, connection string) (outbound.EventRepository, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Check if registry is shutdown
	if r.isShutdown {
		return nil, false
	}

	key := buildRepositoryKey(adapter, connection)
	repo, exists := r.repositories[key]

	// Verify the repository is not in a closed or failed state
	if exists && (repo.GetState() == outbound.StateClosed || repo.GetState() == outbound.StateFailed) {
		return nil, false
	}

	return repo, exists
}

// GetOrCreate retrieves an existing repository or creates a new one if it doesn't exist
func (r *RepositoryRegistry) GetOrCreate(ctx context.Context, config models.EventStreamConfig) (outbound.EventRepository, error) {
	// Validate configuration
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidConfiguration, err)
	}

	key := buildRepositoryKey(config.Adapter, config.Connection)

	// First try to get existing repository
	r.mu.RLock()
	repo, exists := r.repositories[key]
	r.mu.RUnlock()

	// Check if registry is shutdown
	if r.isShutdown {
		return nil, errors.New("repository registry is shutdown")
	}

	// If repository exists, check its state
	if exists {
		state := repo.GetState()
		if state == outbound.StateReady {
			// If repository is ready, return it
			return repo, nil
		} else if state == outbound.StateClosed {
			// If repository is closed, try to reopen it
			if err := repo.Reopen(ctx); err != nil {
				// If reopening fails, we'll create a new one below
				exists = false
			} else {
				// Successfully reopened, return it
				return repo, nil
			}
		} else if state == outbound.StateFailed {
			// If repository failed, we'll create a new one
			exists = false
		}
	}

	// If not found or needs recreation, create a new one
	if !exists {
		r.mu.Lock()
		defer r.mu.Unlock()

		// Check if registry is shutdown
		if r.isShutdown {
			return nil, errors.New("repository registry is shutdown")
		}

		// Check again to avoid race condition
		repo, exists = r.repositories[key]
		if exists && repo.GetState() == outbound.StateReady {
			return repo, nil
		}

		// Create and initialize a new repository
		newRepo, err := r.createRepository(config)
		if err != nil {
			return nil, err
		}

		// Initialize the repository
		if err := newRepo.Initialize(ctx); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrRepositoryInitFailed, err)
		}

		// Store the repository
		r.repositories[key] = newRepo
		return newRepo, nil
	}

	return repo, nil
}

// createRepository creates a new repository based on adapter type
func (r *RepositoryRegistry) createRepository(config models.EventStreamConfig) (outbound.EventRepository, error) {
	switch config.Adapter {
	case "memory":
		return memory.NewMemoryEventRepository(), nil
	case "fs":
		return fs.NewFileSystemEventRepository(config.Connection), nil
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnknownAdapter, config.Adapter)
	}

	// Additional adapters can be added here (postgres, blob, etc.)
}

// Close closes all repositories managed by the registry
func (r *RepositoryRegistry) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var lastErr error

	// Mark registry as shutdown
	r.isShutdown = true

	// Close all repositories
	for key, repo := range r.repositories {
		if err := repo.Close(); err != nil {
			lastErr = fmt.Errorf("failed to close repository %s: %w", key, err)
		}
	}

	// Clear the repositories map
	r.repositories = make(map[string]outbound.EventRepository)

	return lastErr
}

// ReopenRepository attempts to reopen a closed repository
func (r *RepositoryRegistry) ReopenRepository(ctx context.Context, adapter, connection string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Check if registry is shutdown
	if r.isShutdown {
		return errors.New("repository registry is shutdown")
	}

	key := buildRepositoryKey(adapter, connection)
	repo, exists := r.repositories[key]
	if !exists {
		return fmt.Errorf("%w: %s", ErrRepositoryNotFound, key)
	}

	// Check if repository is closed
	if repo.GetState() != outbound.StateClosed {
		return fmt.Errorf("repository %s is not closed, current state: %s", key, repo.GetState())
	}

	// Attempt to reopen
	return repo.Reopen(ctx)
}

// ResetRepository resets a repository to its initial state
func (r *RepositoryRegistry) ResetRepository(ctx context.Context, adapter, connection string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Check if registry is shutdown
	if r.isShutdown {
		return errors.New("repository registry is shutdown")
	}

	key := buildRepositoryKey(adapter, connection)
	repo, exists := r.repositories[key]
	if !exists {
		return fmt.Errorf("%w: %s", ErrRepositoryNotFound, key)
	}

	// Reset the repository
	return repo.Reset(ctx)
}

// Health collects health information from all repositories
func (r *RepositoryRegistry) Health(ctx context.Context) (map[string]interface{}, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := map[string]interface{}{
		"status":          "up",
		"repositoryCount": len(r.repositories),
		"repositories":    make(map[string]interface{}),
		"isShutdown":      r.isShutdown,
	}

	if r.isShutdown {
		result["status"] = "down"
		result["message"] = "Registry is shutdown"
		return result, errors.New("registry is shutdown")
	}

	repoHealth := make(map[string]interface{})
	var lastErr error
	degradedCount := 0
	downCount := 0

	// Collect health from all repositories
	for key, repo := range r.repositories {
		// Get structured health info if available
		healthInfo, err := repo.HealthInfo(ctx)

		// Handle errors or populate health
		if err != nil {
			repoHealth[key] = map[string]interface{}{
				"status": "error",
				"error":  err.Error(),
				"state":  string(repo.GetState()),
			}
			lastErr = fmt.Errorf("failed to get health for repository %s: %w", key, err)
			downCount++
		} else {
			// Convert HealthInfo to map
			healthMap := map[string]interface{}{
				"status": string(healthInfo.Status),
				"state":  string(healthInfo.State),
				"type":   healthInfo.AdditionalInfo["type"],
			}

			// Add additional fields if present
			if healthInfo.Message != "" {
				healthMap["message"] = healthInfo.Message
			}
			if healthInfo.Error != "" {
				healthMap["error"] = healthInfo.Error
			}

			// Add all additional info
			for k, v := range healthInfo.AdditionalInfo {
				if k != "type" { // Avoid duplicate
					healthMap[k] = v
				}
			}

			repoHealth[key] = healthMap

			// Count status types
			if healthInfo.Status == outbound.StatusDegraded {
				degradedCount++
			} else if healthInfo.Status == outbound.StatusDown {
				downCount++
			}
		}
	}

	result["repositories"] = repoHealth

	// Set overall status based on repository health
	if downCount > 0 {
		if downCount == len(r.repositories) {
			result["status"] = "down"
			result["message"] = "All repositories are down"
		} else {
			result["status"] = "degraded"
			result["message"] = fmt.Sprintf("%d repositories are down", downCount)
		}
	} else if degradedCount > 0 {
		result["status"] = "degraded"
		result["message"] = fmt.Sprintf("%d repositories are degraded", degradedCount)
	}

	return result, lastErr
}

// buildRepositoryKey creates a key for the repositories map
func buildRepositoryKey(adapter, connection string) string {
	return fmt.Sprintf("%s:%s", adapter, connection)
}
