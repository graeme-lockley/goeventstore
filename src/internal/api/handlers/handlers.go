package handlers

import (
	"encoding/json"
	"net/http"
	"runtime"
	"time"
)

// ResponseWrapper is a standard response format for all API endpoints
type ResponseWrapper struct {
	Status  string      `json:"status"`
	Data    interface{} `json:"data,omitempty"`
	Message string      `json:"message,omitempty"`
}

// HelloWorldHandler returns a simple hello world message
func HelloWorldHandler(w http.ResponseWriter, r *http.Request) {
	response := ResponseWrapper{
		Status:  "success",
		Data:    map[string]string{"message": "Hello World from Event Source API!"},
		Message: "Welcome to the Event Source API",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// HealthCheckHandler returns the health status of the service
func HealthCheckHandler(w http.ResponseWriter, r *http.Request) {
	healthData := map[string]interface{}{
		"status":     "UP",
		"timestamp":  time.Now().Format(time.RFC3339),
		"go_version": runtime.Version(),
		"go_os":      runtime.GOOS,
		"go_arch":    runtime.GOARCH,
	}

	response := ResponseWrapper{
		Status: "success",
		Data:   healthData,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}
