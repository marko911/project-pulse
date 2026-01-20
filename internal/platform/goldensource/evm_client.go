package goldensource

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"

	protov1 "github.com/marko911/project-pulse/pkg/proto/v1"
)

// EVMClient implements Client for EVM-compatible chains (Ethereum, Polygon, etc.).
type EVMClient struct {
	cfg    *Config
	logger *slog.Logger

	mu        sync.RWMutex
	client    *ethclient.Client
	rpcClient *rpc.Client
	connected bool
	chainID   *big.Int
}

// NewEVMClient creates a new EVM golden source client.
func NewEVMClient(cfg *Config, logger *slog.Logger) *EVMClient {
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.Name == "" {
		cfg.Name = fmt.Sprintf("evm-golden-%s", chainName(cfg.Chain))
	}
	return &EVMClient{
		cfg:    cfg,
		logger: logger.With("component", cfg.Name),
	}
}

// Name returns the client identifier.
func (c *EVMClient) Name() string {
	return c.cfg.Name
}

// Chain returns the chain this client serves.
func (c *EVMClient) Chain() protov1.Chain {
	return c.cfg.Chain
}

// Connect establishes connection to the golden source RPC.
func (c *EVMClient) Connect(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.logger.Info("connecting to golden source RPC", "url", maskURL(c.cfg.URL))

	var lastErr error
	for attempt := 0; attempt <= c.cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			c.logger.Debug("retrying connection", "attempt", attempt)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(c.cfg.RetryInterval):
			}
		}

		var err error
		c.rpcClient, err = rpc.DialContext(ctx, c.cfg.URL)
		if err != nil {
			c.logger.Warn("connection failed", "error", err, "attempt", attempt)
			lastErr = err
			continue
		}

		c.client = ethclient.NewClient(c.rpcClient)

		// Verify connection by fetching chain ID
		c.chainID, err = c.client.ChainID(ctx)
		if err != nil {
			c.logger.Warn("chain ID check failed", "error", err)
			c.client.Close()
			lastErr = err
			continue
		}

		c.connected = true
		c.logger.Info("connected to golden source",
			"chain_id", c.chainID.String(),
		)
		return nil
	}

	return fmt.Errorf("failed to connect after %d attempts: %w", c.cfg.MaxRetries, lastErr)
}

// Close terminates the connection.
func (c *EVMClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.client != nil {
		c.client.Close()
		c.client = nil
		c.rpcClient = nil
		c.connected = false
		c.logger.Info("golden source connection closed")
	}
	return nil
}

// IsConnected returns the current connection status.
func (c *EVMClient) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.connected
}

// GetBlockData retrieves block data for verification.
func (c *EVMClient) GetBlockData(ctx context.Context, blockNumber uint64) (*BlockData, error) {
	c.mu.RLock()
	client := c.client
	c.mu.RUnlock()

	if client == nil {
		return nil, ErrNotConnected
	}

	// Create context with timeout
	if c.cfg.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.cfg.Timeout)
		defer cancel()
	}

	block, err := client.BlockByNumber(ctx, big.NewInt(int64(blockNumber)))
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return nil, ErrBlockNotFound
		}
		return nil, fmt.Errorf("failed to fetch block %d: %w", blockNumber, err)
	}

	return &BlockData{
		Chain:            c.cfg.Chain,
		BlockNumber:      block.NumberU64(),
		BlockHash:        block.Hash().Hex(),
		ParentHash:       block.ParentHash().Hex(),
		TransactionCount: uint32(len(block.Transactions())),
		Timestamp:        time.Unix(int64(block.Time()), 0),
		FetchedAt:        time.Now(),
	}, nil
}

// GetLatestBlockNumber returns the latest block number from the golden source.
func (c *EVMClient) GetLatestBlockNumber(ctx context.Context) (uint64, error) {
	c.mu.RLock()
	client := c.client
	c.mu.RUnlock()

	if client == nil {
		return 0, ErrNotConnected
	}

	// Create context with timeout
	if c.cfg.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.cfg.Timeout)
		defer cancel()
	}

	return client.BlockNumber(ctx)
}

// VerifyBlock compares primary data against golden source data.
func (c *EVMClient) VerifyBlock(ctx context.Context, primary *BlockData) (*VerificationResult, error) {
	if primary == nil {
		return nil, fmt.Errorf("primary block data is nil")
	}

	golden, err := c.GetBlockData(ctx, primary.BlockNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to get golden source data: %w", err)
	}

	result := Verify(primary, golden)

	// Log verification result
	if result.Verified {
		c.logger.Debug("block verified",
			"block_number", primary.BlockNumber,
			"block_hash", truncateHash(primary.BlockHash),
		)
	} else {
		c.logger.Error("block verification FAILED",
			"block_number", primary.BlockNumber,
			"primary_hash", truncateHash(primary.BlockHash),
			"golden_hash", truncateHash(golden.BlockHash),
			"errors", len(result.Errors),
		)
	}

	return result, nil
}

// chainName converts a chain enum to a string.
func chainName(chain protov1.Chain) string {
	switch chain {
	case protov1.Chain_CHAIN_ETHEREUM:
		return "ethereum"
	case protov1.Chain_CHAIN_POLYGON:
		return "polygon"
	case protov1.Chain_CHAIN_ARBITRUM:
		return "arbitrum"
	case protov1.Chain_CHAIN_OPTIMISM:
		return "optimism"
	case protov1.Chain_CHAIN_BASE:
		return "base"
	case protov1.Chain_CHAIN_AVALANCHE:
		return "avalanche"
	case protov1.Chain_CHAIN_BSC:
		return "bsc"
	default:
		return "unknown"
	}
}

// maskURL hides sensitive parts of RPC URLs for logging.
func maskURL(url string) string {
	if idx := strings.Index(url, "@"); idx > 0 {
		return url[:strings.Index(url, "://")+3] + "***@" + url[idx+1:]
	}
	return url
}

// truncateHash safely truncates a hash for logging.
func truncateHash(hash string) string {
	if len(hash) > 16 {
		return hash[:16] + "..."
	}
	return hash
}
