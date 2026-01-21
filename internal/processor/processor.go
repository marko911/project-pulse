package processor

import (
	"context"

	"github.com/marko911/project-pulse/internal/adapter"
	protov1 "github.com/marko911/project-pulse/pkg/proto/v1"
)

type Processor interface {
	Process(ctx context.Context, event adapter.Event) (*protov1.CanonicalEvent, error)

	Start(ctx context.Context, input <-chan adapter.Event, output chan<- *protov1.CanonicalEvent) error

	Stop(ctx context.Context) error

	Health(ctx context.Context) error
}

type Normalizer interface {
	Normalize(ctx context.Context, event adapter.Event) (*protov1.CanonicalEvent, error)

	Chain() string
}

type Config struct {
	WorkerCount int

	BufferSize int

	BrokerEndpoint string

	InputTopic string

	OutputTopic string

	ConsumerGroup string

	PartitionCount int

	PartitionKeyStrategy string
}

func DefaultConfig() Config {
	return Config{
		WorkerCount:          4,
		BufferSize:           10000,
		BrokerEndpoint:       "localhost:9092",
		InputTopic:           "raw-events",
		OutputTopic:          "canonical-events",
		ConsumerGroup:        "processor",
		PartitionCount:       0,
		PartitionKeyStrategy: "chain_block",
	}
}
