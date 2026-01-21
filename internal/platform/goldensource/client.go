package goldensource

import (
	"context"
	"errors"
	"time"

	protov1 "github.com/marko911/project-pulse/pkg/proto/v1"
)

var (
	ErrNotConnected   = errors.New("golden source client not connected")
	ErrBlockNotFound  = errors.New("block not found")
	ErrChainMismatch  = errors.New("chain ID mismatch")
	ErrHashMismatch   = errors.New("block hash mismatch")
	ErrParentMismatch = errors.New("parent hash mismatch")
	ErrTxCountDrift   = errors.New("transaction count drift detected")
)

type BlockData struct {
	Chain protov1.Chain

	BlockNumber uint64

	BlockHash string

	ParentHash string

	TransactionCount uint32

	Timestamp time.Time

	FetchedAt time.Time
}

type VerificationResult struct {
	Verified bool

	BlockNumber uint64

	Errors []error

	Primary *BlockData

	Golden *BlockData

	VerifiedAt time.Time
}

type Client interface {
	Name() string

	Chain() protov1.Chain

	Connect(ctx context.Context) error

	Close() error

	IsConnected() bool

	GetBlockData(ctx context.Context, blockNumber uint64) (*BlockData, error)

	GetLatestBlockNumber(ctx context.Context) (uint64, error)

	VerifyBlock(ctx context.Context, primary *BlockData) (*VerificationResult, error)
}

type Config struct {
	URL string

	Chain protov1.Chain

	Name string

	Timeout time.Duration

	MaxRetries int

	RetryInterval time.Duration
}

func DefaultConfig() Config {
	return Config{
		Timeout:       10 * time.Second,
		MaxRetries:    3,
		RetryInterval: 1 * time.Second,
	}
}

func Verify(primary, golden *BlockData) *VerificationResult {
	result := &VerificationResult{
		Verified:    true,
		BlockNumber: primary.BlockNumber,
		Primary:     primary,
		Golden:      golden,
		Errors:      make([]error, 0),
		VerifiedAt:  time.Now(),
	}

	if primary.BlockHash != golden.BlockHash {
		result.Verified = false
		result.Errors = append(result.Errors, ErrHashMismatch)
	}

	if primary.ParentHash != golden.ParentHash {
		result.Verified = false
		result.Errors = append(result.Errors, ErrParentMismatch)
	}

	if primary.TransactionCount != golden.TransactionCount {
		result.Verified = false
		result.Errors = append(result.Errors, ErrTxCountDrift)
	}

	if primary.Chain != golden.Chain {
		result.Verified = false
		result.Errors = append(result.Errors, ErrChainMismatch)
	}

	return result
}
