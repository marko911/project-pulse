package adapter

import (
	"context"
)

type Event struct {
	Chain           string
	CommitmentLevel string
	BlockNumber     uint64
	BlockHash       string
	ParentHash      string
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

type Adapter interface {
	Name() string

	Start(ctx context.Context, events chan<- Event) error

	Stop(ctx context.Context) error

	Health(ctx context.Context) error
}

type Source interface {
	Name() string

	Stream(ctx context.Context, events chan<- Event) error
}

type Config struct {
	Endpoint string

	CommitmentLevel string

	Accounts []string

	Programs []string

	BatchSize int

	MaxRetries   int
	RetryDelayMs int
}
