package evm

import (
	"context"
	"encoding/json"
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
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/mirador/pulse/internal/adapter"
)

// Adapter ingests events from EVM-compatible blockchains and publishes them
// to the message broker in canonical format.
type Adapter struct {
	cfg    *Config
	logger *slog.Logger

	// RPC clients
	client   *ethclient.Client
	wsClient *ethclient.Client

	// Broker producer
	producer *kgo.Client

	// Subscriptions
	blockSub ethereum.Subscription
	logSub   ethereum.Subscription

	// Block tracking
	mu             sync.RWMutex
	latestBlock    uint64
	processedBlock uint64
	confirmedBlock uint64
	finalizedBlock uint64

	// Metrics
	blocksProcessed uint64
	eventsEmitted   uint64

	// Lifecycle
	cancel context.CancelFunc
}

// NewAdapter creates a new EVM adapter instance.
func NewAdapter(cfg *Config, logger *slog.Logger) (*Adapter, error) {
	if cfg.RPC.URL == "" {
		return nil, fmt.Errorf("RPC URL is required")
	}

	adapter := &Adapter{
		cfg:    cfg,
		logger: logger.With("component", "evm-adapter", "chain", cfg.Chain),
	}

	return adapter, nil
}

// Run starts the adapter and blocks until context is cancelled.
func (a *Adapter) Run(ctx context.Context) error {
	a.logger.Info("starting adapter",
		"chain_id", a.cfg.ChainID,
		"rpc_url", a.cfg.RPC.URL,
		"ws_url", a.cfg.RPC.WSURL,
		"start_block", a.cfg.Processing.StartBlock,
	)

	// Connect to RPC endpoint
	if err := a.connect(ctx); err != nil {
		return fmt.Errorf("connect to RPC: %w", err)
	}
	defer a.disconnect()

	// TODO: Connect to message broker
	if err := a.connectBroker(ctx); err != nil {
		return fmt.Errorf("connect to broker: %w", err)
	}

	// Start processing loops
	errCh := make(chan error, 3)

	// Block subscription loop
	go func() {
		errCh <- a.subscribeBlocks(ctx)
	}()

	// Commitment level tracker
	go func() {
		errCh <- a.trackCommitmentLevels(ctx)
	}()

	// Metrics reporter
	go func() {
		errCh <- a.reportMetrics(ctx)
	}()

	// Wait for error or context cancellation
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		a.logger.Info("adapter shutting down")
		return ctx.Err()
	}
}

// connect establishes connection to the RPC endpoint.
func (a *Adapter) connect(ctx context.Context) error {
	var err error

	// Connect HTTP client for queries
	a.logger.Info("connecting to HTTP RPC endpoint", "url", a.cfg.RPC.URL)
	a.client, err = ethclient.DialContext(ctx, a.cfg.RPC.URL)
	if err != nil {
		return fmt.Errorf("dial HTTP RPC: %w", err)
	}

	// Verify chain ID
	chainID, err := a.client.ChainID(ctx)
	if err != nil {
		return fmt.Errorf("get chain ID: %w", err)
	}
	if chainID.Uint64() != a.cfg.ChainID {
		return fmt.Errorf("chain ID mismatch: expected %d, got %d", a.cfg.ChainID, chainID.Uint64())
	}
	a.logger.Info("verified chain ID", "chain_id", chainID)

	// Connect WebSocket client for subscriptions
	if a.cfg.RPC.WSURL != "" {
		a.logger.Info("connecting to WebSocket RPC endpoint", "url", a.cfg.RPC.WSURL)
		a.wsClient, err = ethclient.DialContext(ctx, a.cfg.RPC.WSURL)
		if err != nil {
			a.logger.Warn("failed to connect WebSocket, falling back to polling", "error", err)
		}
	}

	// Get current block
	header, err := a.client.HeaderByNumber(ctx, nil)
	if err != nil {
		return fmt.Errorf("get latest block: %w", err)
	}

	a.mu.Lock()
	a.latestBlock = header.Number.Uint64()
	a.mu.Unlock()

	a.logger.Info("connected to RPC", "latest_block", header.Number.Uint64())
	return nil
}

// disconnect closes all RPC and broker connections.
func (a *Adapter) disconnect() {
	if a.blockSub != nil {
		a.blockSub.Unsubscribe()
	}
	if a.logSub != nil {
		a.logSub.Unsubscribe()
	}
	if a.wsClient != nil {
		a.wsClient.Close()
	}
	if a.client != nil {
		a.client.Close()
	}
	if a.producer != nil {
		a.producer.Flush(context.Background())
		a.producer.Close()
	}
	a.logger.Info("disconnected from RPC and broker")
}

// connectBroker establishes connection to the message broker using franz-go.
func (a *Adapter) connectBroker(ctx context.Context) error {
	a.logger.Info("connecting to message broker",
		"addresses", a.cfg.Broker.Addresses,
		"topic_prefix", a.cfg.Broker.TopicPrefix,
	)

	// Normalize broker addresses
	brokerList := make([]string, len(a.cfg.Broker.Addresses))
	for i, addr := range a.cfg.Broker.Addresses {
		brokerList[i] = strings.TrimSpace(addr)
	}

	// Create Kafka producer for raw-events topic
	producer, err := kgo.NewClient(
		kgo.SeedBrokers(brokerList...),
		kgo.MaxProduceRequestsInflightPerBroker(1),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
		kgo.RecordRetries(5),
	)
	if err != nil {
		return fmt.Errorf("create kafka producer: %w", err)
	}

	a.producer = producer
	a.logger.Info("connected to message broker",
		"brokers", brokerList,
	)
	return nil
}

// subscribeBlocks subscribes to new blocks and processes them.
func (a *Adapter) subscribeBlocks(ctx context.Context) error {
	a.logger.Info("starting block subscription")

	// Try WebSocket subscription first
	if a.wsClient != nil {
		return a.subscribeBlocksWS(ctx)
	}

	// Fall back to polling
	return a.pollBlocks(ctx)
}

// subscribeBlocksWS uses WebSocket subscription for new blocks.
func (a *Adapter) subscribeBlocksWS(ctx context.Context) error {
	headers := make(chan *types.Header, 100)
	sub, err := a.wsClient.SubscribeNewHead(ctx, headers)
	if err != nil {
		a.logger.Warn("WebSocket subscription failed, falling back to polling", "error", err)
		return a.pollBlocks(ctx)
	}
	a.blockSub = sub
	a.logger.Info("subscribed to new headers via WebSocket")

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-sub.Err():
			a.logger.Error("block subscription error", "error", err)
			return a.pollBlocks(ctx) // Fall back to polling
		case header := <-headers:
			if err := a.processBlock(ctx, header); err != nil {
				a.logger.Error("failed to process block", "block", header.Number, "error", err)
			}
		}
	}
}

// pollBlocks uses HTTP polling for new blocks.
func (a *Adapter) pollBlocks(ctx context.Context) error {
	a.logger.Info("polling for new blocks")
	ticker := time.NewTicker(time.Duration(a.cfg.RPC.BlockPollInterval) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			header, err := a.client.HeaderByNumber(ctx, nil)
			if err != nil {
				a.logger.Error("failed to get latest block", "error", err)
				continue
			}

			a.mu.RLock()
			lastBlock := a.latestBlock
			a.mu.RUnlock()

			if header.Number.Uint64() > lastBlock {
				if err := a.processBlock(ctx, header); err != nil {
					a.logger.Error("failed to process block", "block", header.Number, "error", err)
				}
			}
		}
	}
}

// processBlock processes a new block and its logs.
func (a *Adapter) processBlock(ctx context.Context, header *types.Header) error {
	blockNum := header.Number.Uint64()

	a.logger.Debug("processing block",
		"number", blockNum,
		"hash", header.Hash().Hex(),
		"tx_count", header.GasUsed,
	)

	// Update latest block
	a.mu.Lock()
	a.latestBlock = blockNum
	a.blocksProcessed++
	a.mu.Unlock()

	// Fetch logs for this block
	logs, err := a.fetchBlockLogs(ctx, blockNum)
	if err != nil {
		return fmt.Errorf("fetch logs: %w", err)
	}

	// Process each log
	for _, log := range logs {
		event := a.logToEvent(header, &log)

		// Publish event to broker
		if err := a.publishEvent(ctx, event); err != nil {
			a.logger.Error("failed to publish event",
				"block", blockNum,
				"tx", log.TxHash.Hex(),
				"error", err,
			)
			continue
		}

		a.mu.Lock()
		a.eventsEmitted++
		a.mu.Unlock()

		a.logger.Debug("emitted event",
			"block", blockNum,
			"tx", log.TxHash.Hex(),
			"address", log.Address.Hex(),
			"topics", len(log.Topics),
		)
	}

	return nil
}

// publishEvent publishes an adapter event to the raw-events Kafka topic.
func (a *Adapter) publishEvent(ctx context.Context, event adapter.Event) error {
	if a.producer == nil {
		return fmt.Errorf("producer not initialized")
	}

	// Serialize event to JSON
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	// Generate partition key based on strategy
	partitionKey := a.generatePartitionKey(&event)

	// Create Kafka record
	record := &kgo.Record{
		Topic: "raw-events",
		Key:   []byte(partitionKey),
		Value: data,
		Headers: []kgo.RecordHeader{
			{Key: "chain", Value: []byte(event.Chain)},
			{Key: "block_number", Value: []byte(fmt.Sprintf("%d", event.BlockNumber))},
			{Key: "event_type", Value: []byte(event.EventType)},
		},
	}

	// Produce synchronously for reliability
	results := a.producer.ProduceSync(ctx, record)
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("produce: %w", err)
	}

	return nil
}

// generatePartitionKey creates a partition key based on the configured strategy.
func (a *Adapter) generatePartitionKey(event *adapter.Event) string {
	switch a.cfg.Broker.PartitionKeyStrategy {
	case "chain_block":
		return fmt.Sprintf("%s:%d", event.Chain, event.BlockNumber)
	case "account":
		if len(event.Accounts) > 0 {
			return event.Accounts[0]
		}
		return event.Chain
	case "event_type":
		return event.EventType
	case "round_robin":
		return "" // Empty key = round-robin distribution
	default:
		return fmt.Sprintf("%s:%d", event.Chain, event.BlockNumber)
	}
}

// fetchBlockLogs retrieves all logs for a specific block.
func (a *Adapter) fetchBlockLogs(ctx context.Context, blockNum uint64) ([]types.Log, error) {
	query := ethereum.FilterQuery{
		FromBlock: big.NewInt(int64(blockNum)),
		ToBlock:   big.NewInt(int64(blockNum)),
	}

	// Add address filter if configured
	if len(a.cfg.Subscription.Addresses) > 0 {
		addresses := make([]common.Address, len(a.cfg.Subscription.Addresses))
		for i, addr := range a.cfg.Subscription.Addresses {
			addresses[i] = common.HexToAddress(addr)
		}
		query.Addresses = addresses
	}

	// Add topic filter if configured
	if len(a.cfg.Subscription.Topics) > 0 {
		topics := make([][]common.Hash, len(a.cfg.Subscription.Topics))
		for i, topicGroup := range a.cfg.Subscription.Topics {
			hashes := make([]common.Hash, len(topicGroup))
			for j, topic := range topicGroup {
				hashes[j] = common.HexToHash(topic)
			}
			topics[i] = hashes
		}
		query.Topics = topics
	}

	return a.client.FilterLogs(ctx, query)
}

// logToEvent converts an Ethereum log to an adapter.Event.
func (a *Adapter) logToEvent(header *types.Header, log *types.Log) adapter.Event {
	topics := make([]string, len(log.Topics))
	for i, topic := range log.Topics {
		topics[i] = topic.Hex()
	}

	return adapter.Event{
		Chain:           a.cfg.Chain,
		CommitmentLevel: "processed", // Will be updated based on block depth
		BlockNumber:     log.BlockNumber,
		BlockHash:       log.BlockHash.Hex(),
		TxHash:          log.TxHash.Hex(),
		TxIndex:         uint32(log.TxIndex),
		EventIndex:      uint32(log.Index),
		EventType:       "log",
		Accounts:        []string{log.Address.Hex()},
		Timestamp:       int64(header.Time),
		Payload:         log.Data,
		ProgramID:       log.Address.Hex(),
	}
}

// trackCommitmentLevels tracks block confirmations for different commitment levels.
func (a *Adapter) trackCommitmentLevels(ctx context.Context) error {
	a.logger.Info("starting commitment level tracker",
		"processed_depth", a.cfg.Processing.ProcessedDepth,
		"confirmed_depth", a.cfg.Processing.ConfirmedDepth,
		"finalized_depth", a.cfg.Processing.FinalizedDepth,
	)

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			a.updateCommitmentLevels()
		}
	}
}

// updateCommitmentLevels calculates current commitment level boundaries.
func (a *Adapter) updateCommitmentLevels() {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.latestBlock == 0 {
		return
	}

	// Calculate commitment levels based on depth
	if a.latestBlock > a.cfg.Processing.ProcessedDepth {
		a.processedBlock = a.latestBlock - a.cfg.Processing.ProcessedDepth
	}
	if a.latestBlock > a.cfg.Processing.ConfirmedDepth {
		a.confirmedBlock = a.latestBlock - a.cfg.Processing.ConfirmedDepth
	}
	if a.latestBlock > a.cfg.Processing.FinalizedDepth {
		a.finalizedBlock = a.latestBlock - a.cfg.Processing.FinalizedDepth
	}
}

// reportMetrics periodically logs adapter metrics.
func (a *Adapter) reportMetrics(ctx context.Context) error {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			a.mu.RLock()
			a.logger.Info("adapter metrics",
				"latest_block", a.latestBlock,
				"processed_block", a.processedBlock,
				"confirmed_block", a.confirmedBlock,
				"finalized_block", a.finalizedBlock,
				"blocks_processed", a.blocksProcessed,
				"events_emitted", a.eventsEmitted,
			)
			a.mu.RUnlock()
		}
	}
}

// GetLatestBlock returns the latest observed block number.
func (a *Adapter) GetLatestBlock() uint64 {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.latestBlock
}

// GetMetrics returns current adapter metrics.
func (a *Adapter) GetMetrics() AdapterMetrics {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return AdapterMetrics{
		Chain:           a.cfg.Chain,
		ChainID:         a.cfg.ChainID,
		LatestBlock:     a.latestBlock,
		ProcessedBlock:  a.processedBlock,
		ConfirmedBlock:  a.confirmedBlock,
		FinalizedBlock:  a.finalizedBlock,
		BlocksProcessed: a.blocksProcessed,
		EventsEmitted:   a.eventsEmitted,
	}
}

// AdapterMetrics contains current adapter statistics.
type AdapterMetrics struct {
	Chain           string
	ChainID         uint64
	LatestBlock     uint64
	ProcessedBlock  uint64
	ConfirmedBlock  uint64
	FinalizedBlock  uint64
	BlocksProcessed uint64
	EventsEmitted   uint64
}

// Ensure Adapter implements adapter.Adapter interface.
var _ adapter.Adapter = (*Adapter)(nil)

// Name returns the adapter identifier.
func (a *Adapter) Name() string {
	return a.cfg.Chain
}

// Start begins streaming events to the provided channel.
func (a *Adapter) Start(ctx context.Context, events chan<- adapter.Event) error {
	a.logger.Info("starting event stream",
		"chain", a.cfg.Chain,
		"chain_id", a.cfg.ChainID,
	)

	// For now, run the adapter's internal loop
	return a.Run(ctx)
}

// Stop gracefully shuts down the adapter.
func (a *Adapter) Stop(ctx context.Context) error {
	a.logger.Info("stopping adapter")
	a.disconnect()
	return nil
}

// Health returns the current health status of the adapter connection.
func (a *Adapter) Health(ctx context.Context) error {
	if a.client == nil {
		return fmt.Errorf("RPC client not connected")
	}

	// Try to get latest block number as health check
	_, err := a.client.BlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}

	return nil
}
