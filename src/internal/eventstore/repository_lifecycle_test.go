package eventstore

import (
	"context"
	"testing"

	"goeventsource/src/internal/eventstore/models"
	"goeventsource/src/internal/port/outbound"
)

func TestRepositoryLifecycleManagement(t *testing.T) {
	// Create a new registry
	registry := NewRepositoryRegistry()
	ctx := context.Background()

	// Test with memory adapter
	config := models.EventStreamConfig{
		Name:    "test-lifecycle",
		Adapter: "memory",
	}

	// Get a repository
	repo, err := registry.GetOrCreate(ctx, config)
	if err != nil {
		t.Fatalf("Failed to create repository: %v", err)
	}

	// Check initial state
	if repo.GetState() != outbound.StateReady {
		t.Errorf("Expected repository to be in ready state after creation, got %s", repo.GetState())
	}

	// Get health info
	healthInfo, err := repo.HealthInfo(ctx)
	if err != nil {
		t.Errorf("Failed to get health info: %v", err)
	}
	if healthInfo.Status != outbound.StatusUp {
		t.Errorf("Expected health status to be up, got %s", healthInfo.Status)
	}
	if healthInfo.State != outbound.StateReady {
		t.Errorf("Expected state to be ready, got %s", healthInfo.State)
	}

	// Close the repository
	err = repo.Close()
	if err != nil {
		t.Errorf("Failed to close repository: %v", err)
	}

	// Check state after closing
	if repo.GetState() != outbound.StateClosed {
		t.Errorf("Expected repository to be in closed state after closing, got %s", repo.GetState())
	}

	// Get health info after closing
	healthInfo, err = repo.HealthInfo(ctx)
	if err != nil {
		t.Errorf("Failed to get health info after closing: %v", err)
	}
	if healthInfo.Status != outbound.StatusDown {
		t.Errorf("Expected health status to be down after closing, got %s", healthInfo.Status)
	}
	if healthInfo.State != outbound.StateClosed {
		t.Errorf("Expected state to be closed, got %s", healthInfo.State)
	}

	// Try to use the repository when closed (should fail)
	_, err = repo.GetLatestVersion(ctx, "test-topic")
	if err == nil {
		t.Error("Expected error when using closed repository, got nil")
	}

	// Reopen the repository
	err = repo.Reopen(ctx)
	if err != nil {
		t.Errorf("Failed to reopen repository: %v", err)
	}

	// Check state after reopening
	if repo.GetState() != outbound.StateReady {
		t.Errorf("Expected repository to be in ready state after reopening, got %s", repo.GetState())
	}

	// Get health info after reopening
	healthInfo, err = repo.HealthInfo(ctx)
	if err != nil {
		t.Errorf("Failed to get health info after reopening: %v", err)
	}
	if healthInfo.Status != outbound.StatusUp {
		t.Errorf("Expected health status to be up after reopening, got %s", healthInfo.Status)
	}

	// Test the registry's health report
	registryHealth, err := registry.Health(ctx)
	if err != nil {
		t.Errorf("Failed to get registry health: %v", err)
	}
	if registryHealth["status"] != "up" {
		t.Errorf("Expected registry status to be up, got %s", registryHealth["status"])
	}

	// Close repository again
	err = repo.Close()
	if err != nil {
		t.Errorf("Failed to close repository again: %v", err)
	}

	// Test reopening through registry
	err = registry.ReopenRepository(ctx, "memory", "")
	if err != nil {
		t.Errorf("Failed to reopen repository through registry: %v", err)
	}

	// Get repository again and check its state
	repo, exists := registry.Get("memory", "")
	if !exists {
		t.Error("Repository not found after reopening")
	} else if repo.GetState() != outbound.StateReady {
		t.Errorf("Expected repository to be in ready state after reopening through registry, got %s", repo.GetState())
	}

	// Test reset functionality
	err = registry.ResetRepository(ctx, "memory", "")
	if err != nil {
		t.Errorf("Failed to reset repository: %v", err)
	}

	// Check state after reset
	repo, exists = registry.Get("memory", "")
	if !exists {
		t.Error("Repository not found after reset")
	} else if repo.GetState() != outbound.StateReady {
		t.Errorf("Expected repository to be in ready state after reset, got %s", repo.GetState())
	}

	// Shutdown registry
	err = registry.Close()
	if err != nil {
		t.Errorf("Failed to close registry: %v", err)
	}

	// Verify shutdown state
	registryHealth, err = registry.Health(ctx)
	if registryHealth["status"] != "down" {
		t.Errorf("Expected registry status to be down after closing, got %s", registryHealth["status"])
	}
	if !registryHealth["isShutdown"].(bool) {
		t.Error("Expected registry to be marked as shutdown")
	}

	// Try to get a repository after registry shutdown (should fail)
	_, exists = registry.Get("memory", "")
	if exists {
		t.Error("Expected repository to not be available after registry shutdown")
	}
}
