package evm

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
)

type Client struct {
	cfg    *RPCConfig
	logger *slog.Logger

	mu        sync.RWMutex
	client    *ethclient.Client
	rpcClient *rpc.Client
	isWS      bool
	connected bool

	reconnectCh chan struct{}
}

func NewClient(cfg *RPCConfig, logger *slog.Logger) *Client {
	return &Client{
		cfg:         cfg,
		logger:      logger.With("component", "evm-client"),
		reconnectCh: make(chan struct{}, 1),
	}
}

func (c *Client) Connect(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.isWS = strings.HasPrefix(c.cfg.URL, "ws://") || strings.HasPrefix(c.cfg.URL, "wss://")

	c.logger.Info("connecting to RPC",
		"url", c.cfg.URL,
		"is_websocket", c.isWS,
	)

	var err error
	for attempt := 0; attempt <= c.cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			c.logger.Info("retrying connection", "attempt", attempt)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(c.cfg.RetryInterval):
			}
		}

		c.rpcClient, err = rpc.DialContext(ctx, c.cfg.URL)
		if err != nil {
			c.logger.Warn("connection failed", "error", err, "attempt", attempt)
			continue
		}

		c.client = ethclient.NewClient(c.rpcClient)
		c.connected = true

		_, err = c.client.ChainID(ctx)
		if err != nil {
			c.logger.Warn("chain ID check failed", "error", err)
			c.client.Close()
			c.connected = false
			continue
		}

		c.logger.Info("connected successfully")
		return nil
	}

	return fmt.Errorf("failed to connect after %d attempts: %w", c.cfg.MaxRetries, err)
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.client != nil {
		c.client.Close()
		c.connected = false
	}
	return nil
}

func (c *Client) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.connected
}

func (c *Client) ChainID(ctx context.Context) (*big.Int, error) {
	c.mu.RLock()
	client := c.client
	c.mu.RUnlock()

	if client == nil {
		return nil, fmt.Errorf("not connected")
	}
	return client.ChainID(ctx)
}

func (c *Client) BlockNumber(ctx context.Context) (uint64, error) {
	c.mu.RLock()
	client := c.client
	c.mu.RUnlock()

	if client == nil {
		return 0, fmt.Errorf("not connected")
	}
	return client.BlockNumber(ctx)
}

func (c *Client) BlockByNumber(ctx context.Context, number *big.Int) (*types.Block, error) {
	c.mu.RLock()
	client := c.client
	c.mu.RUnlock()

	if client == nil {
		return nil, fmt.Errorf("not connected")
	}
	return client.BlockByNumber(ctx, number)
}

func (c *Client) HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error) {
	c.mu.RLock()
	client := c.client
	c.mu.RUnlock()

	if client == nil {
		return nil, fmt.Errorf("not connected")
	}
	return client.HeaderByNumber(ctx, number)
}

func (c *Client) SubscribeNewHead(ctx context.Context, ch chan<- *types.Header) (ethereum.Subscription, error) {
	c.mu.RLock()
	client := c.client
	isWS := c.isWS
	c.mu.RUnlock()

	if client == nil {
		return nil, fmt.Errorf("not connected")
	}
	if !isWS {
		return nil, fmt.Errorf("subscriptions require WebSocket connection")
	}

	return client.SubscribeNewHead(ctx, ch)
}

func (c *Client) FilterLogs(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
	c.mu.RLock()
	client := c.client
	c.mu.RUnlock()

	if client == nil {
		return nil, fmt.Errorf("not connected")
	}
	return client.FilterLogs(ctx, query)
}

func (c *Client) SubscribeFilterLogs(ctx context.Context, query ethereum.FilterQuery, ch chan types.Log) (ethereum.Subscription, error) {
	c.mu.RLock()
	client := c.client
	isWS := c.isWS
	c.mu.RUnlock()

	if client == nil {
		return nil, fmt.Errorf("not connected")
	}
	if !isWS {
		return nil, fmt.Errorf("subscriptions require WebSocket connection")
	}

	return client.SubscribeFilterLogs(ctx, query, ch)
}

func (c *Client) TransactionReceipt(ctx context.Context, txHash common.Hash) (*types.Receipt, error) {
	c.mu.RLock()
	client := c.client
	c.mu.RUnlock()

	if client == nil {
		return nil, fmt.Errorf("not connected")
	}
	return client.TransactionReceipt(ctx, txHash)
}

func (c *Client) IsWebSocket() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.isWS
}
