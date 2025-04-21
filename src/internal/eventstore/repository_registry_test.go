package eventstore

import (
	"context"
	"fmt"
	"testing"

	"goeventsource/src/internal/eventstore/models"
)

func TestNewRepositoryRegistry(t *testing.T) {
	registry := NewRepositoryRegistry()

	if registry == nil {
		t.Fatal("Expected non-nil registry")
	}

	if registry.repositories == nil {
		t.Fatal("Expected initialized repositories map")
	}

	if len(registry.repositories) != 0 {
		t.Errorf("Expected empty repositories map, got %d items", len(registry.repositories))
	}
}

func TestRepositoryRegistry_Get(t *testing.T) {
	registry := NewRepositoryRegistry()

	// Get with empty registry should return not found
	repo, exists := registry.Get("memory", "")
	if exists {
		t.Error("Expected repository to not exist in empty registry")
	}
	if repo != nil {
		t.Error("Expected nil repository when not found")
	}

	// Create a memory repository for testing
	config := models.EventStreamConfig{
		Name:    "test-stream",
		Adapter: "memory",
	}

	// Add a repository
	ctx := context.Background()
	addedRepo, err := registry.GetOrCreate(ctx, config)
	if err != nil {
		t.Fatalf("Failed to create repository: %v", err)
	}

	// Verify repository retrieval
	repo, exists = registry.Get("memory", "")
	if !exists {
		t.Error("Expected repository to exist in registry")
	}
	if repo != addedRepo {
		t.Error("Expected same repository instance to be returned")
	}

	// Get with wrong adapter should return not found
	repo, exists = registry.Get("unknown", "")
	if exists {
		t.Error("Expected repository with wrong adapter to not exist")
	}
	if repo != nil {
		t.Error("Expected nil repository when not found")
	}
}

func TestRepositoryRegistry_GetOrCreate(t *testing.T) {
	registry := NewRepositoryRegistry()
	ctx := context.Background()

	// Test creating a memory repository
	memoryConfig := models.EventStreamConfig{
		Name:    "memory-stream",
		Adapter: "memory",
	}

	repo1, err := registry.GetOrCreate(ctx, memoryConfig)
	if err != nil {
		t.Fatalf("Failed to create memory repository: %v", err)
	}
	if repo1 == nil {
		t.Fatal("Expected non-nil memory repository")
	}

	// Test getting the same repository
	repo2, err := registry.GetOrCreate(ctx, memoryConfig)
	if err != nil {
		t.Fatalf("Failed to get existing memory repository: %v", err)
	}
	if repo2 != repo1 {
		t.Error("Expected same repository instance to be returned")
	}

	// Test with invalid configuration
	invalidConfig := models.EventStreamConfig{
		Name:    "", // Empty name is invalid
		Adapter: "memory",
	}

	_, err = registry.GetOrCreate(ctx, invalidConfig)
	if err == nil {
		t.Error("Expected error for invalid configuration")
	}

	// Test with unknown adapter
	unknownConfig := models.EventStreamConfig{
		Name:    "unknown-stream",
		Adapter: "unknown",
	}

	_, err = registry.GetOrCreate(ctx, unknownConfig)
	if err == nil {
		t.Error("Expected error for unknown adapter")
	}
}

func TestRepositoryRegistry_CreateRepository(t *testing.T) {
	registry := NewRepositoryRegistry()

	// Test creating a memory repository
	memoryConfig := models.EventStreamConfig{
		Name:    "memory-stream",
		Adapter: "memory",
	}

	repo, err := registry.createRepository(memoryConfig)
	if err != nil {
		t.Fatalf("Failed to create memory repository: %v", err)
	}
	if repo == nil {
		t.Fatal("Expected non-nil memory repository")
	}

	// Test creating a filesystem repository
	fsConfig := models.EventStreamConfig{
		Name:       "fs-stream",
		Adapter:    "fs",
		Connection: "/tmp/eventstore",
	}

	repo, err = registry.createRepository(fsConfig)
	if err != nil {
		t.Fatalf("Failed to create filesystem repository: %v", err)
	}
	if repo == nil {
		t.Fatal("Expected non-nil filesystem repository")
	}

	// Test with unknown adapter
	unknownConfig := models.EventStreamConfig{
		Name:    "unknown-stream",
		Adapter: "unknown",
	}

	repo, err = registry.createRepository(unknownConfig)
	if err == nil {
		t.Error("Expected error for unknown adapter")
	}
	if repo != nil {
		t.Error("Expected nil repository for unknown adapter")
	}
}

func TestRepositoryRegistry_Close(t *testing.T) {
	registry := NewRepositoryRegistry()
	ctx := context.Background()

	// Create a few repositories
	for i := 0; i < 3; i++ {
		config := models.EventStreamConfig{
			Name:    fmt.Sprintf("test-stream-%d", i),
			Adapter: "memory",
		}

		_, err := registry.GetOrCreate(ctx, config)
		if err != nil {
			t.Fatalf("Failed to create repository: %v", err)
		}
	}

	// Verify some repositories were created
	initialCount := len(registry.repositories)
	if initialCount == 0 {
		t.Error("Expected at least one repository to be created")
	}

	// Close all repositories
	err := registry.Close()
	if err != nil {
		t.Fatalf("Failed to close repositories: %v", err)
	}

	// Verify repositories were cleared
	if len(registry.repositories) != 0 {
		t.Errorf("Expected empty repositories map after close, got %d items", len(registry.repositories))
	}
}

func TestRepositoryRegistry_Health(t *testing.T) {
	registry := NewRepositoryRegistry()
	ctx := context.Background()

	// Empty registry should return healthy
	health, err := registry.Health(ctx)
	if err != nil {
		t.Fatalf("Failed to get health from empty registry: %v", err)
	}
	if health["status"] != "up" {
		t.Errorf("Expected status 'up', got '%v'", health["status"])
	}
	if health["repositoryCount"].(int) != 0 {
		t.Errorf("Expected 0 repositories, got %v", health["repositoryCount"])
	}

	// Create a repository
	config := models.EventStreamConfig{
		Name:    "health-test-stream",
		Adapter: "memory",
	}

	_, err = registry.GetOrCreate(ctx, config)
	if err != nil {
		t.Fatalf("Failed to create repository: %v", err)
	}

	// Get health again
	health, err = registry.Health(ctx)
	if err != nil {
		t.Fatalf("Failed to get health: %v", err)
	}
	if health["status"] != "up" {
		t.Errorf("Expected status 'up', got '%v'", health["status"])
	}
	if health["repositoryCount"].(int) != 1 {
		t.Errorf("Expected 1 repository, got %v", health["repositoryCount"])
	}

	repos, ok := health["repositories"].(map[string]interface{})
	if !ok {
		t.Fatalf("Expected repositories to be a map, got %T", health["repositories"])
	}
	if len(repos) != 1 {
		t.Errorf("Expected 1 repository in health report, got %d", len(repos))
	}
}

func TestBuildRepositoryKey(t *testing.T) {
	testCases := []struct {
		adapter    string
		connection string
		expected   string
	}{
		{"memory", "", "memory:"},
		{"fs", "/tmp/eventstore", "fs:/tmp/eventstore"},
		{"postgres", "postgres://localhost/events", "postgres:postgres://localhost/events"},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s_%s", tc.adapter, tc.connection), func(t *testing.T) {
			key := buildRepositoryKey(tc.adapter, tc.connection)
			if key != tc.expected {
				t.Errorf("Expected key '%s', got '%s'", tc.expected, key)
			}
		})
	}
}
