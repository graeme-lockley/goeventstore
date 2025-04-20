.PHONY: all build clean test test-cover lint format run run-dev docker-build docker-run docker-dev help

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GORUN=$(GOCMD) run
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod
GOVET=$(GOCMD) vet
BINARY_NAME=api
VERSION?=$(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
BUILD_DIR=./bin
LDFLAGS=-ldflags "-X main.Version=$(VERSION)"
DOCKER_COMPOSE=docker-compose
DOCKER_COMPOSE_FILE=./infra/docker/docker-compose.yml
DOCKER_COMPOSE_DEV_FILE=./infra/docker/docker-compose.dev.yml

all: test build

build:
	mkdir -p $(BUILD_DIR)
	$(GOBUILD) -o $(BUILD_DIR)/$(BINARY_NAME) $(LDFLAGS) ./src/cmd/api/...

clean:
	$(GOCLEAN)
	rm -rf $(BUILD_DIR)

test:
	$(GOTEST) -v ./src/...

test-cover:
	$(GOTEST) -coverprofile=coverage.out ./src/...
	$(GOCMD) tool cover -html=coverage.out

lint:
	golangci-lint run ./src/...

format:
	$(GOCMD) fmt ./src/...
	goimports -w ./src

run:
	$(GORUN) ./src/cmd/api/main.go

run-dev:
	air -c .air.toml

docker-build:
	docker build -t goeventsource:$(VERSION) -f infra/docker/Dockerfile .

docker-run:
	docker run -p 8080:8080 goeventsource:$(VERSION)

docker-compose:
	cd infra/docker && $(DOCKER_COMPOSE) -f docker-compose.yml up

docker-compose-build:
	cd infra/docker && $(DOCKER_COMPOSE) -f docker-compose.yml build

docker-dev:
	cd infra/docker && $(DOCKER_COMPOSE) -f docker-compose.dev.yml up

tidy:
	$(GOMOD) tidy

deps-update:
	$(GOMOD) tidy
	$(GOGET) -u ./src/...

help:
	@echo "Available targets:"
	@echo "  all               : Run tests and build"
	@echo "  build             : Build the binary"
	@echo "  clean             : Clean build artifacts"
	@echo "  test              : Run tests"
	@echo "  test-cover        : Run tests with coverage"
	@echo "  lint              : Run linters"
	@echo "  format            : Format code"
	@echo "  run               : Run the API locally"
	@echo "  run-dev           : Run the API with hot-reload (requires air)"
	@echo "  docker-build      : Build Docker image"
	@echo "  docker-run        : Run Docker container"
	@echo "  docker-compose    : Run with Docker Compose"
	@echo "  docker-compose-build : Build with Docker Compose"
	@echo "  docker-dev        : Run development environment with Docker Compose"
	@echo "  tidy              : Tidy go.mod file"
	@echo "  deps-update       : Update dependencies" 