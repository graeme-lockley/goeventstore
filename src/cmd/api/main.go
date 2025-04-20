package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"goeventsource/src/internal/api/handlers"
	"goeventsource/src/internal/api/logging"
	"goeventsource/src/internal/api/middleware"
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

	// Create a new router
	mux := http.NewServeMux()

	// Register routes
	mux.HandleFunc("GET /", handlers.HelloWorldHandler)
	mux.HandleFunc("GET /health", handlers.HealthCheckHandler)

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

	if err := server.Shutdown(ctx); err != nil {
		logger.Error("Server shutdown failed", map[string]interface{}{"error": err.Error()})
		os.Exit(1)
	}

	logger.Info("Server exited properly")
}
