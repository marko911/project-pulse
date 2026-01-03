// Package adapter defines the common interface for blockchain adapters.
package adapter

import (
	"context"
)

// Event represents a normalized blockchain event before full canonical conversion.
type Event struct {
	Chain           string
	CommitmentLevel string
	BlockNumber     uint64
	BlockHash       string
	ParentHash      string // Parent block hash for chain continuity verification
	TxHash          string
	TxIndex         uint32
	EventIndex      uint32
	EventType       string
	Accounts        []string
	Timestamp       int64
	Payload         []byte
	ProgramID       string
	NativeValue     uint64
}

// Adapter defines the interface all blockchain adapters must implement.
type Adapter interface {
	// Name returns the adapter identifier (e.g., "solana", "ethereum").
	Name() string

	// Start begins streaming events. Events are sent to the provided channel.
	// The adapter should respect context cancellation.
	Start(ctx context.Context, events chan<- Event) error

	// Stop gracefully shuts down the adapter.
	Stop(ctx context.Context) error

	// Health returns the current health status of the adapter connection.
	Health(ctx context.Context) error
}

// Source abstracts the origin of blockchain events (RPC, file replay, mock).
// This interface enables deterministic testing by allowing adapters to consume
// events from recorded fixtures instead of live RPC connections.
type Source interface {
	// Name returns the source identifier (e.g., "file", "rpc", "mock").
	Name() string

	// Stream sends events to the provided channel.
	// Implementations should:
	// - Respect context cancellation
	// - Return nil when all events are consumed (for finite sources like files)
	// - Block until context cancelled (for infinite sources like RPC)
	Stream(ctx context.Context, events chan<- Event) error
}

// Config holds common adapter configuration.
type Config struct {
	// RPC endpoint URL
	Endpoint string

	// Commitment level to stream (processed, confirmed, finalized)
	CommitmentLevel string

	// Optional: accounts to filter
	Accounts []string

	// Optional: program IDs to filter (Solana) or contract addresses (EVM)
	Programs []string

	// Batch size for bulk operations
	BatchSize int

	// Reconnect settings
	MaxRetries   int
	RetryDelayMs int
}
