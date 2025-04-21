package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"goeventsource/src/internal/api/handlers"
	"goeventsource/src/internal/api/logging"
	"goeventsource/src/internal/api/middleware"
	"goeventsource/src/internal/eventstore"
	"goeventsource/src/internal/eventstore/models"
)

var logger = logging.NewLogger(logging.INFO)

func main() {
	// Log API startup
	logger.Info("Starting Event Source API")

	// Get port from environment or use default
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// Initialize EventStore
	store, err := setupEventStore()
	if err != nil {
		logger.Fatal("Failed to initialize EventStore", map[string]interface{}{"error": err.Error()})
	}

	// Create EventStore handlers
	esHandlers := handlers.NewEventStoreHandlers(store)

	// Create a new router
	mux := http.NewServeMux()

	// Register base routes
	mux.HandleFunc("GET /", handlers.HelloWorldHandler)
	mux.HandleFunc("GET /health", handlers.HealthCheckHandler)

	// ---------- Versioned API Routes (v1) ----------

	// Register v1 EventStore routes
	mux.HandleFunc("GET /api/v1/eventstore/health", esHandlers.HealthCheckHandler)

	// Topic management endpoints - v1 paths
	mux.HandleFunc("GET /api/v1/topics", esHandlers.ListTopicsHandler)
	mux.HandleFunc("POST /api/v1/topics", esHandlers.CreateTopicHandler)
	mux.HandleFunc("GET /api/v1/topics/{topicName}", esHandlers.GetTopicHandler)
	mux.HandleFunc("PUT /api/v1/topics/{topicName}", esHandlers.UpdateTopicHandler)
	mux.HandleFunc("DELETE /api/v1/topics/{topicName}", esHandlers.DeleteTopicHandler)

	// Event operations endpoints - v1 paths
	mux.HandleFunc("GET /api/v1/topics/{topicName}/events", esHandlers.GetEventsHandler)
	mux.HandleFunc("POST /api/v1/topics/{topicName}/events", esHandlers.AppendEventsHandler)
	mux.HandleFunc("GET /api/v1/topics/{topicName}/version", esHandlers.GetLatestVersionHandler)

	// Apply middleware to the router
	var handler http.Handler = mux
	handler = middleware.Logger(handler)
	handler = middleware.Recoverer(handler)
	handler = middleware.CORS(handler)

	// Create server
	server := &http.Server{
		Addr:         ":" + port,
		Handler:      handler,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Start server in a goroutine
	go func() {
		logger.Info("Starting server", map[string]interface{}{"port": port})
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("Server startup failed", map[string]interface{}{"error": err.Error()})
		}
	}()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	// Shutdown server with timeout
	logger.Info("Shutting down server")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Close the EventStore
	if err := store.Close(); err != nil {
		logger.Error("Failed to close EventStore", map[string]interface{}{"error": err.Error()})
	}

	if err := server.Shutdown(ctx); err != nil {
		logger.Error("Server shutdown failed", map[string]interface{}{"error": err.Error()})
		os.Exit(1)
	}

	logger.Info("Server exited properly")
}

// setupEventStore initializes and returns an EventStore instance
func setupEventStore() (*eventstore.EventStore, error) {
	// Create a default configuration for the EventStore
	config := models.EventStoreConfig{
		Logger: log.New(os.Stdout, "[EventStore] ", log.LstdFlags),
		ConfigStream: models.EventStreamConfig{
			Name:       "config",
			Adapter:    "fs", // Use in-memory adapter for configuration by default
			Connection: "config-repository",
			Options:    make(map[string]string),
		},
	}

	// Create the EventStore instance
	store, err := eventstore.NewEventStore(config)
	if err != nil {
		return nil, err
	}

	// Initialize the EventStore
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := store.Initialize(ctx); err != nil {
		return nil, err
	}

	return store, nil
}
