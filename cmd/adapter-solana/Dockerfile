# Build stage
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/adapter-solana ./cmd/adapter-solana

# Final stage
FROM alpine:3.19

WORKDIR /app

# Install ca-certificates for HTTPS
RUN apk --no-cache add ca-certificates

COPY --from=builder /bin/adapter-solana /app/adapter-solana

# Run as non-root user
RUN adduser -D pulse
USER pulse

ENTRYPOINT ["/app/adapter-solana"]
