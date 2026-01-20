// Package processor implements the core event processing and normalization logic.
package processor

import (
	"context"

	"github.com/marko911/project-pulse/internal/adapter"
	protov1 "github.com/marko911/project-pulse/pkg/proto/v1"
)

// Processor handles event normalization and processing.
type Processor interface {
	// Process normalizes incoming adapter events to canonical format.
	Process(ctx context.Context, event adapter.Event) (*protov1.CanonicalEvent, error)

	// Start begins processing events from the input channel.
	Start(ctx context.Context, input <-chan adapter.Event, output chan<- *protov1.CanonicalEvent) error

	// Stop gracefully shuts down the processor.
	Stop(ctx context.Context) error

	// Health returns the current health status.
	Health(ctx context.Context) error
}

// Normalizer converts chain-specific events to canonical format.
type Normalizer interface {
	// Normalize converts an adapter event to canonical protobuf format.
	Normalize(ctx context.Context, event adapter.Event) (*protov1.CanonicalEvent, error)

	// Chain returns the chain identifier this normalizer handles.
	Chain() string
}

// Config holds processor configuration.
type Config struct {
	// WorkerCount is the number of parallel processing workers.
	WorkerCount int

	// BufferSize is the size of internal event buffers.
	BufferSize int

	// BrokerEndpoint is the Redpanda/Kafka broker address.
	BrokerEndpoint string

	// InputTopic is the topic to consume raw events from.
	InputTopic string

	// OutputTopic is the topic to publish canonical events to.
	OutputTopic string

	// ConsumerGroup is the Kafka consumer group name for coordinated consumption.
	ConsumerGroup string

	// PartitionCount is the number of partitions for output topics (0 = use broker default).
	PartitionCount int

	// PartitionKeyStrategy controls how events are distributed across partitions.
	// Options: "chain_block" (default - chain:blockNumber), "account" (by first account),
	// "event_type" (by event type), "round_robin" (no key, round-robin distribution)
	PartitionKeyStrategy string
}

// DefaultConfig returns sensible default configuration.
func DefaultConfig() Config {
	return Config{
		WorkerCount:          4,
		BufferSize:           10000,
		BrokerEndpoint:       "localhost:9092",
		InputTopic:           "raw-events",
		OutputTopic:          "canonical-events",
		ConsumerGroup:        "processor",
		PartitionCount:       0, // Use broker default
		PartitionKeyStrategy: "chain_block",
	}
}
