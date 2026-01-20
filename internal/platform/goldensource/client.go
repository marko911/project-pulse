// Package goldensource provides a secondary RPC client for data integrity verification.
// Golden Source clients connect to independent RPC providers to cross-validate
// block data received from the primary adapter, ensuring correctness.
package goldensource

import (
	"context"
	"errors"
	"time"

	protov1 "github.com/marko911/project-pulse/pkg/proto/v1"
)

// Common errors returned by golden source clients.
var (
	ErrNotConnected   = errors.New("golden source client not connected")
	ErrBlockNotFound  = errors.New("block not found")
	ErrChainMismatch  = errors.New("chain ID mismatch")
	ErrHashMismatch   = errors.New("block hash mismatch")
	ErrParentMismatch = errors.New("parent hash mismatch")
	ErrTxCountDrift   = errors.New("transaction count drift detected")
)

// BlockData represents the essential block information retrieved from a golden source.
// This is the minimal data needed for integrity verification.
type BlockData struct {
	// Chain identifies the blockchain network
	Chain protov1.Chain

	// BlockNumber is the height of this block
	BlockNumber uint64

	// BlockHash is the canonical hash of this block
	BlockHash string

	// ParentHash is the hash of the parent block (for chain continuity)
	ParentHash string

	// TransactionCount is the number of transactions in this block
	TransactionCount uint32

	// Timestamp when the block was produced
	Timestamp time.Time

	// FetchedAt when this data was retrieved from the golden source
	FetchedAt time.Time
}

// VerificationResult represents the outcome of comparing primary vs golden source data.
type VerificationResult struct {
	// Verified indicates whether all checks passed
	Verified bool

	// BlockNumber that was verified
	BlockNumber uint64

	// Errors contains any mismatches detected
	Errors []error

	// Primary is the data from the primary source (adapter)
	Primary *BlockData

	// Golden is the data from the golden source (secondary RPC)
	Golden *BlockData

	// VerifiedAt when the verification was performed
	VerifiedAt time.Time
}

// Client defines the interface for golden source verification clients.
// Implementations connect to secondary RPC providers to fetch block data
// for cross-validation against the primary adapter data.
type Client interface {
	// Name returns the client identifier (e.g., "evm-alchemy", "solana-triton")
	Name() string

	// Chain returns the chain this client serves
	Chain() protov1.Chain

	// Connect establishes connection to the golden source RPC
	Connect(ctx context.Context) error

	// Close terminates the connection
	Close() error

	// IsConnected returns the current connection status
	IsConnected() bool

	// GetBlockData retrieves block data for verification
	GetBlockData(ctx context.Context, blockNumber uint64) (*BlockData, error)

	// GetLatestBlockNumber returns the latest block number from the golden source
	GetLatestBlockNumber(ctx context.Context) (uint64, error)

	// VerifyBlock compares primary data against golden source data
	// Returns a VerificationResult with any mismatches found
	VerifyBlock(ctx context.Context, primary *BlockData) (*VerificationResult, error)
}

// Config holds common configuration for golden source clients.
type Config struct {
	// URL is the RPC endpoint for the golden source
	URL string

	// Chain identifies which blockchain this client serves
	Chain protov1.Chain

	// Name is an optional friendly name for this golden source
	Name string

	// Timeout for RPC calls
	Timeout time.Duration

	// MaxRetries for transient failures
	MaxRetries int

	// RetryInterval between retry attempts
	RetryInterval time.Duration
}

// DefaultConfig returns sensible defaults for production use.
func DefaultConfig() Config {
	return Config{
		Timeout:       10 * time.Second,
		MaxRetries:    3,
		RetryInterval: 1 * time.Second,
	}
}

// Verify compares primary and golden block data and returns a detailed result.
// This is a helper function that can be used by any Client implementation.
func Verify(primary, golden *BlockData) *VerificationResult {
	result := &VerificationResult{
		Verified:    true,
		BlockNumber: primary.BlockNumber,
		Primary:     primary,
		Golden:      golden,
		Errors:      make([]error, 0),
		VerifiedAt:  time.Now(),
	}

	// Check block hash
	if primary.BlockHash != golden.BlockHash {
		result.Verified = false
		result.Errors = append(result.Errors, ErrHashMismatch)
	}

	// Check parent hash
	if primary.ParentHash != golden.ParentHash {
		result.Verified = false
		result.Errors = append(result.Errors, ErrParentMismatch)
	}

	// Check transaction count (allow small drift for timing issues)
	if primary.TransactionCount != golden.TransactionCount {
		result.Verified = false
		result.Errors = append(result.Errors, ErrTxCountDrift)
	}

	// Check chain consistency
	if primary.Chain != golden.Chain {
		result.Verified = false
		result.Errors = append(result.Errors, ErrChainMismatch)
	}

	return result
}
