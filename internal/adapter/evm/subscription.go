package evm

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/marko911/project-pulse/internal/adapter"
)

// BlockSubscriber manages block and log subscriptions.
type BlockSubscriber struct {
	client *Client
	cfg    *ProcessingConfig
	logger *slog.Logger

	mu              sync.RWMutex
	latestBlock     uint64
	processedBlocks map[uint64]bool

	// Channels
	events chan<- adapter.Event
}

// NewBlockSubscriber creates a new block subscription manager.
func NewBlockSubscriber(client *Client, cfg *ProcessingConfig, logger *slog.Logger) *BlockSubscriber {
	return &BlockSubscriber{
		client:          client,
		cfg:             cfg,
		logger:          logger.With("component", "block-subscriber"),
		processedBlocks: make(map[uint64]bool),
	}
}

// Subscribe starts the block subscription and emits events.
func (s *BlockSubscriber) Subscribe(ctx context.Context, events chan<- adapter.Event) error {
	s.events = events

	if s.client.IsWebSocket() {
		return s.subscribeWS(ctx)
	}
	return s.pollHTTP(ctx)
}

// subscribeWS uses WebSocket subscription for real-time blocks.
func (s *BlockSubscriber) subscribeWS(ctx context.Context) error {
	s.logger.Info("starting WebSocket block subscription")

	headerCh := make(chan *types.Header, 100)
	sub, err := s.client.SubscribeNewHead(ctx, headerCh)
	if err != nil {
		return fmt.Errorf("subscribe new head: %w", err)
	}
	defer sub.Unsubscribe()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case err := <-sub.Err():
			s.logger.Error("subscription error", "error", err)
			return fmt.Errorf("subscription error: %w", err)

		case header := <-headerCh:
			if err := s.processHeader(ctx, header); err != nil {
				s.logger.Error("failed to process header", "error", err, "block", header.Number.Uint64())
			}
		}
	}
}

// pollHTTP polls for new blocks via HTTP RPC.
func (s *BlockSubscriber) pollHTTP(ctx context.Context) error {
	s.logger.Info("starting HTTP polling for blocks")

	// Get starting block
	var startBlock uint64
	if s.cfg.StartBlock > 0 {
		startBlock = s.cfg.StartBlock
	} else {
		current, err := s.client.BlockNumber(ctx)
		if err != nil {
			return fmt.Errorf("get current block: %w", err)
		}
		startBlock = current
	}

	s.mu.Lock()
	s.latestBlock = startBlock
	s.mu.Unlock()

	s.logger.Info("starting from block", "block", startBlock)

	ticker := time.NewTicker(1 * time.Second) // Poll every second
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-ticker.C:
			if err := s.pollNewBlocks(ctx); err != nil {
				s.logger.Error("poll error", "error", err)
			}
		}
	}
}

// pollNewBlocks checks for and processes new blocks.
func (s *BlockSubscriber) pollNewBlocks(ctx context.Context) error {
	current, err := s.client.BlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("get block number: %w", err)
	}

	s.mu.RLock()
	lastProcessed := s.latestBlock
	s.mu.RUnlock()

	if current <= lastProcessed {
		return nil
	}

	// Process each new block
	for blockNum := lastProcessed + 1; blockNum <= current; blockNum++ {
		header, err := s.client.HeaderByNumber(ctx, big.NewInt(int64(blockNum)))
		if err != nil {
			return fmt.Errorf("get header %d: %w", blockNum, err)
		}

		if err := s.processHeader(ctx, header); err != nil {
			return fmt.Errorf("process header %d: %w", blockNum, err)
		}
	}

	return nil
}

// processHeader processes a block header and fetches logs.
func (s *BlockSubscriber) processHeader(ctx context.Context, header *types.Header) error {
	blockNum := header.Number.Uint64()

	s.logger.Debug("processing block",
		"block", blockNum,
		"hash", header.Hash().Hex(),
	)

	// Update latest block
	s.mu.Lock()
	if blockNum > s.latestBlock {
		s.latestBlock = blockNum
	}
	s.processedBlocks[blockNum] = true
	s.mu.Unlock()

	// Fetch logs for this block
	logs, err := s.fetchBlockLogs(ctx, blockNum)
	if err != nil {
		return fmt.Errorf("fetch logs: %w", err)
	}

	// Convert logs to events
	for i, log := range logs {
		event := s.logToEvent(header, &log, uint32(i))
		select {
		case s.events <- event:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

// fetchBlockLogs fetches all logs for a specific block.
func (s *BlockSubscriber) fetchBlockLogs(ctx context.Context, blockNum uint64) ([]types.Log, error) {
	query := ethereum.FilterQuery{
		FromBlock: big.NewInt(int64(blockNum)),
		ToBlock:   big.NewInt(int64(blockNum)),
	}

	// Apply address filter if configured
	if len(s.cfg.WatchAddresses) > 0 {
		addresses := make([]common.Address, len(s.cfg.WatchAddresses))
		for i, addr := range s.cfg.WatchAddresses {
			addresses[i] = common.HexToAddress(addr)
		}
		query.Addresses = addresses
	}

	return s.client.FilterLogs(ctx, query)
}

// logToEvent converts an EVM log to the canonical event format.
func (s *BlockSubscriber) logToEvent(header *types.Header, log *types.Log, eventIndex uint32) adapter.Event {
	// Extract accounts from log topics (topic[0] is event signature)
	accounts := make([]string, 0)
	accounts = append(accounts, log.Address.Hex())
	for i := 1; i < len(log.Topics); i++ {
		// Topics might be addresses (20 bytes padded to 32)
		accounts = append(accounts, log.Topics[i].Hex())
	}

	return adapter.Event{
		Chain:           "ethereum", // Will be set by adapter based on config
		CommitmentLevel: "processed",
		BlockNumber:     log.BlockNumber,
		BlockHash:       log.BlockHash.Hex(),
		TxHash:          log.TxHash.Hex(),
		TxIndex:         uint32(log.TxIndex),
		EventIndex:      eventIndex,
		EventType:       "log",
		Accounts:        accounts,
		Timestamp:       int64(header.Time),
		Payload:         log.Data,
		ProgramID:       log.Address.Hex(),
		NativeValue:     0,
	}
}

// GetLatestBlock returns the latest processed block number.
func (s *BlockSubscriber) GetLatestBlock() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.latestBlock
}

// LogSubscriber handles real-time log subscriptions.
type LogSubscriber struct {
	client *Client
	cfg    *ProcessingConfig
	logger *slog.Logger
}

// NewLogSubscriber creates a new log subscription manager.
func NewLogSubscriber(client *Client, cfg *ProcessingConfig, logger *slog.Logger) *LogSubscriber {
	return &LogSubscriber{
		client: client,
		cfg:    cfg,
		logger: logger.With("component", "log-subscriber"),
	}
}

// Subscribe starts the log subscription for specific addresses/topics.
func (ls *LogSubscriber) Subscribe(ctx context.Context, events chan<- types.Log) error {
	if !ls.client.IsWebSocket() {
		return fmt.Errorf("log subscription requires WebSocket connection")
	}

	ls.logger.Info("starting log subscription")

	query := ethereum.FilterQuery{}

	// Add address filter
	if len(ls.cfg.WatchAddresses) > 0 {
		addresses := make([]common.Address, len(ls.cfg.WatchAddresses))
		for i, addr := range ls.cfg.WatchAddresses {
			addresses[i] = common.HexToAddress(addr)
		}
		query.Addresses = addresses
	}

	logCh := make(chan types.Log, 1000)
	sub, err := ls.client.SubscribeFilterLogs(ctx, query, logCh)
	if err != nil {
		return fmt.Errorf("subscribe filter logs: %w", err)
	}
	defer sub.Unsubscribe()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case err := <-sub.Err():
			ls.logger.Error("log subscription error", "error", err)
			return fmt.Errorf("log subscription error: %w", err)

		case log := <-logCh:
			select {
			case events <- log:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}
