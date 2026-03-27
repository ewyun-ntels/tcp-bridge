# Makefile for tcp-bridge
.PHONY: build clean container deps help push

# Variables
APP_NAME := tcp-bridge
VERSION := $(shell git describe --tags --abbrev=0 2>/dev/null || echo "v0.1.0")
BUILD_TIME := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
GIT_COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
GO_VERSION := $(shell go version | awk '{print $$3}')

# Build flags
LDFLAGS := -ldflags "-X main.version=$(VERSION) -X main.buildTime=$(BUILD_TIME) -X main.gitCommit=$(GIT_COMMIT) -w -s"
BUILD_DIR := ./bin
DOCKER_IMAGE := 192.168.61.145/ewy/tcp-bridge
DOCKER_TAG := $(VERSION)

# Default target
help: ## Show this help message
	@echo "Available targets:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# Build targets
build: ## Build the application binary
	@echo "Building $(APP_NAME) $(VERSION)..."
	@mkdir -p $(BUILD_DIR)
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build $(LDFLAGS) -o $(BUILD_DIR)/$(APP_NAME) ./cmd/$(APP_NAME)
	@echo "Build completed: $(BUILD_DIR)/$(APP_NAME)"

build-local: ## Build for local development (current OS/arch)
	@echo "Building $(APP_NAME) for local development..."
	@mkdir -p $(BUILD_DIR)
	go build $(LDFLAGS) -o $(BUILD_DIR)/$(APP_NAME) ./cmd/$(APP_NAME)
	@echo "Local build completed: $(BUILD_DIR)/$(APP_NAME)"

# Dependency management
deps: ## Download and tidy dependencies
	@echo "Managing dependencies..."
	go mod download
	go mod tidy
	go mod verify

deps-update: ## Update all dependencies
	@echo "Updating dependencies..."
	go get -u ./...
	go mod tidy

# Container targets
container: ## Build Docker image
	@echo "Building Docker image $(DOCKER_IMAGE):$(DOCKER_TAG)..."
	docker build -t $(DOCKER_IMAGE):$(DOCKER_TAG) -t $(DOCKER_IMAGE):latest .
	@echo "Docker image built: $(DOCKER_IMAGE):$(DOCKER_TAG)"

push: container ## Push Docker image
	@echo "Pushing Docker image $(DOCKER_IMAGE):$(DOCKER_TAG)..."
	docker push $(DOCKER_IMAGE):$(DOCKER_TAG)
	docker push $(DOCKER_IMAGE):latest
	@echo "Docker image pushed: $(DOCKER_IMAGE):$(DOCKER_TAG), latest"

# Cleanup targets
clean: ## Clean build artifacts
	@echo "Cleaning build artifacts..."
	rm -rf $(BUILD_DIR)
	rm -f coverage.out coverage.html
	@echo "Cleanup completed"

clean-all: clean ## Clean everything including Docker images
	@echo "Cleaning all Docker images for $(DOCKER_IMAGE)..."
	-docker images $(DOCKER_IMAGE) --format '{{.Repository}}:{{.Tag}}' | xargs -r docker rmi
	-docker images --filter "dangling=true" --filter "label=app=$(APP_NAME)" -q | xargs -r docker rmi

# Release targets
release: clean build container ## Build release (clean, build, container)
	@echo "Release build completed for version $(VERSION)"

# Info targets
info: ## Show build information
	@echo "Project: $(APP_NAME)"
	@echo "Version: $(VERSION)"
	@echo "Build Time: $(BUILD_TIME)"
	@echo "Git Commit: $(GIT_COMMIT)"
	@echo "Go Version: $(GO_VERSION)"

# Vendor targets (if using vendor)
vendor: ## Create vendor directory
	@echo "Creating vendor directory..."
	go mod vendor

vendor-clean: ## Remove vendor directory
	@echo "Removing vendor directory..."
	rm -rf vendor/
