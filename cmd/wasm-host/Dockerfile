# Build stage
FROM golang:1.24-bookworm AS builder

WORKDIR /build

# Install wasmtime-go dependencies (requires CGO)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libc6-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy go mod files first for better caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the wasm-host binary
# CGO is required for wasmtime-go
RUN CGO_ENABLED=1 GOOS=linux go build -o wasm-host ./cmd/wasm-host

# Runtime stage
FROM debian:bookworm-slim

WORKDIR /app

# Install runtime dependencies for wasmtime
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create cache directory
RUN mkdir -p /tmp/wasm-cache

# Copy binary from builder
COPY --from=builder /build/wasm-host .

# Expose metrics port
EXPOSE 9091

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -qO- http://localhost:9091/health || exit 1

ENTRYPOINT ["./wasm-host"]
