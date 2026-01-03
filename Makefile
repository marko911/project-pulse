# Mirador / Project Pulse Makefile
# High-throughput blockchain data platform

# Variables
GO := go
GOFLAGS := -v
BINDIR := bin
PROTODIR := api/proto
SERVICES := adapter-solana adapter-evm processor fixture-recorder outbox-publisher gap-detector

.PHONY: all build test lint clean proto proto-lint proto-breaking dev up down logs help $(SERVICES)

# Default target
all: build

# Build all services
build: $(SERVICES)

$(SERVICES):
	@echo "Building $@..."
	@mkdir -p $(BINDIR)
	$(GO) build $(GOFLAGS) -o $(BINDIR)/$@ ./cmd/$@

# Run all tests
test:
	$(GO) test -race -cover ./...

# Run tests with verbose output
test-v:
	$(GO) test -race -cover -v ./...

# Lint code
lint:
	@command -v golangci-lint >/dev/null 2>&1 || { echo "golangci-lint not installed"; exit 1; }
	golangci-lint run ./...

# Format code
fmt:
	$(GO) fmt ./...
	@command -v goimports >/dev/null 2>&1 && goimports -w . || true

# Generate protobuf code (using buf - preferred)
proto:
	@command -v buf >/dev/null 2>&1 || { echo "buf not installed. Install: https://buf.build/docs/installation"; exit 1; }
	cd $(PROTODIR) && buf generate

# Lint protobuf schemas
proto-lint:
	@command -v buf >/dev/null 2>&1 || { echo "buf not installed"; exit 1; }
	cd $(PROTODIR) && buf lint

# Check protobuf breaking changes
proto-breaking:
	@command -v buf >/dev/null 2>&1 || { echo "buf not installed"; exit 1; }
	cd $(PROTODIR) && buf breaking --against '.git#branch=main'

# Download dependencies
deps:
	$(GO) mod download
	$(GO) mod tidy

# Start development environment (Docker Compose)
up:
	docker compose -f deployments/docker/docker-compose.yml up -d

# Stop development environment
down:
	docker compose -f deployments/docker/docker-compose.yml down

# View logs
logs:
	docker compose -f deployments/docker/docker-compose.yml logs -f

# Start development environment with rebuild
dev: up
	@echo "Development environment started"

# Clean build artifacts
clean:
	rm -rf $(BINDIR)
	$(GO) clean -cache

# Verify go.mod and go.sum are tidy
verify:
	$(GO) mod verify

# Run specific service (usage: make run SERVICE=processor)
run:
	$(GO) run ./cmd/$(SERVICE)

# Help
help:
	@echo "Mirador Makefile targets:"
	@echo "  all       - Build all services (default)"
	@echo "  build     - Build all services"
	@echo "  test      - Run tests with race detection and coverage"
	@echo "  test-v    - Run tests with verbose output"
	@echo "  lint      - Run golangci-lint"
	@echo "  fmt       - Format code"
	@echo "  proto     - Generate protobuf code (buf)"
	@echo "  proto-lint- Lint protobuf schemas"
	@echo "  proto-breaking - Check for breaking proto changes"
	@echo "  deps      - Download and tidy dependencies"
	@echo "  up        - Start Docker Compose dev environment"
	@echo "  down      - Stop Docker Compose dev environment"
	@echo "  logs      - Tail Docker Compose logs"
	@echo "  dev       - Start development environment"
	@echo "  clean     - Remove build artifacts"
	@echo "  verify    - Verify go.mod/go.sum"
	@echo "  run       - Run a service (SERVICE=name)"
	@echo "  help      - Show this help"
