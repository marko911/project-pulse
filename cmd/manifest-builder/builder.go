package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/mirador/pulse/internal/platform/storage"
	protov1 "github.com/mirador/pulse/pkg/proto/v1"
)

// BuilderConfig holds configuration for the manifest builder.
type BuilderConfig struct {
	Brokers       []string
	Topics        []string
	ConsumerGroup string
	PollInterval  time.Duration
	FlushInterval time.Duration
	MetricsAddr   string
}

// blockKey uniquely identifies a block for aggregation.
type blockKey struct {
	chain       protov1.Chain
	blockNumber uint64
}

// blockAggregator accumulates events for a single block.
type blockAggregator struct {
	chain          protov1.Chain
	blockNumber    uint64
	blockHash      string
	parentHash     string
	blockTimestamp time.Time
	events         []*protov1.CanonicalEvent
	txSet          map[string]bool
	lastUpdated    time.Time
}

// Builder aggregates finalized events into manifests.
type Builder struct {
	cfg    BuilderConfig
	logger *slog.Logger
	db     *storage.DB
	repo   *storage.ManifestRepository

	client *kgo.Client

	// Block aggregation
	mu          sync.RWMutex
	aggregators map[blockKey]*blockAggregator

	// Metrics
	metricsServer     *http.Server
	manifestsCreated  int64
	eventsProcessed   int64
	blocksAggregated  int64
}

// NewBuilder creates a new manifest builder.
func NewBuilder(cfg BuilderConfig, db *storage.DB, logger *slog.Logger) (*Builder, error) {
	// Create Kafka consumer
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.ConsumerGroup),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.FetchMaxWait(cfg.PollInterval),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}

	return &Builder{
		cfg:         cfg,
		logger:      logger,
		db:          db,
		repo:        storage.NewManifestRepository(db),
		client:      client,
		aggregators: make(map[blockKey]*blockAggregator),
	}, nil
}

// Run starts the manifest builder loop.
func (b *Builder) Run(ctx context.Context) error {
	b.logger.Info("starting manifest builder")

	// Start metrics server
	if b.cfg.MetricsAddr != "" {
		go b.startMetricsServer()
	}

	// Start flush ticker
	flushTicker := time.NewTicker(b.cfg.FlushInterval)
	defer flushTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-flushTicker.C:
			if err := b.flushReadyManifests(ctx); err != nil {
				b.logger.Error("flush error", "error", err)
			}

		default:
		}

		// Poll for records
		fetches := b.client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return fmt.Errorf("kafka client closed")
		}

		if errs := fetches.Errors(); len(errs) > 0 {
			for _, err := range errs {
				b.logger.Error("fetch error",
					"topic", err.Topic,
					"partition", err.Partition,
					"error", err.Err,
				)
			}
			continue
		}

		// Process each record
		fetches.EachRecord(func(record *kgo.Record) {
			if err := b.processRecord(record); err != nil {
				b.logger.Error("process record error",
					"topic", record.Topic,
					"offset", record.Offset,
					"error", err,
				)
			}
		})

		// Commit offsets
		if err := b.client.CommitUncommittedOffsets(ctx); err != nil {
			b.logger.Error("commit error", "error", err)
		}
	}
}

// processRecord handles a single Kafka record.
func (b *Builder) processRecord(record *kgo.Record) error {
	var event protov1.CanonicalEvent
	if err := json.Unmarshal(record.Value, &event); err != nil {
		return fmt.Errorf("unmarshal event: %w", err)
	}

	// Only process finalized events
	if event.CommitmentLevel != protov1.CommitmentLevel_COMMITMENT_LEVEL_FINALIZED {
		return nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	key := blockKey{chain: event.Chain, blockNumber: event.BlockNumber}

	agg, exists := b.aggregators[key]
	if !exists {
		agg = &blockAggregator{
			chain:          event.Chain,
			blockNumber:    event.BlockNumber,
			blockHash:      event.BlockHash,
			parentHash:     "", // Will be set from block event
			blockTimestamp: event.Timestamp,
			events:         make([]*protov1.CanonicalEvent, 0),
			txSet:          make(map[string]bool),
			lastUpdated:    time.Now(),
		}
		b.aggregators[key] = agg
	}

	// Add event to aggregator
	eventCopy := event
	agg.events = append(agg.events, &eventCopy)
	agg.txSet[event.TxHash] = true
	agg.lastUpdated = time.Now()

	b.eventsProcessed++

	b.logger.Debug("aggregated event",
		"chain", event.Chain,
		"block", event.BlockNumber,
		"event_id", event.EventId,
		"events_count", len(agg.events),
	)

	return nil
}

// flushReadyManifests builds and persists manifests for blocks that are ready.
// A block is ready if it hasn't received new events for a configurable period.
func (b *Builder) flushReadyManifests(ctx context.Context) error {
	b.mu.Lock()
	readyBlocks := make([]*blockAggregator, 0)
	readyKeys := make([]blockKey, 0)

	// Find blocks that haven't been updated recently
	cutoff := time.Now().Add(-b.cfg.FlushInterval)
	for key, agg := range b.aggregators {
		if agg.lastUpdated.Before(cutoff) && len(agg.events) > 0 {
			readyBlocks = append(readyBlocks, agg)
			readyKeys = append(readyKeys, key)
		}
	}

	// Remove ready blocks from aggregators
	for _, key := range readyKeys {
		delete(b.aggregators, key)
	}
	b.mu.Unlock()

	if len(readyBlocks) == 0 {
		return nil
	}

	b.logger.Info("flushing manifests", "count", len(readyBlocks))

	// Build and save manifests
	for _, agg := range readyBlocks {
		manifest := b.buildManifest(agg)

		if err := b.repo.Save(ctx, manifest); err != nil {
			b.logger.Error("failed to save manifest",
				"chain", manifest.Chain,
				"block", manifest.BlockNumber,
				"error", err,
			)
			continue
		}

		b.manifestsCreated++
		b.blocksAggregated++

		b.logger.Info("created manifest",
			"chain", manifest.Chain,
			"block", manifest.BlockNumber,
			"tx_count", manifest.EmittedTxCount,
			"event_count", manifest.EmittedEventCount,
			"hash", manifest.EventIdsHash[:16],
		)
	}

	return nil
}

// buildManifest creates a manifest from an aggregator.
func (b *Builder) buildManifest(agg *blockAggregator) *protov1.Manifest {
	// Sort events deterministically for hash computation
	sortedEvents := make([]*protov1.CanonicalEvent, len(agg.events))
	copy(sortedEvents, agg.events)
	sort.Slice(sortedEvents, func(i, j int) bool {
		if sortedEvents[i].TxIndex != sortedEvents[j].TxIndex {
			return sortedEvents[i].TxIndex < sortedEvents[j].TxIndex
		}
		return sortedEvents[i].EventIndex < sortedEvents[j].EventIndex
	})

	// Compute event IDs hash
	eventIdsHash := b.computeEventIdsHash(sortedEvents)

	// Extract parent hash from first event if available
	parentHash := ""
	// Parent hash would typically come from block metadata; for now use empty

	return &protov1.Manifest{
		Chain:              agg.chain,
		BlockNumber:        agg.blockNumber,
		BlockHash:          agg.blockHash,
		ParentHash:         parentHash,
		ExpectedTxCount:    uint32(len(agg.txSet)),     // Actual count from events
		ExpectedEventCount: uint32(len(agg.events)),   // Actual count from events
		EmittedTxCount:     uint32(len(agg.txSet)),
		EmittedEventCount:  uint32(len(agg.events)),
		EventIdsHash:       eventIdsHash,
		SourcesUsed:        []string{"kafka-consumer"},
		BlockTimestamp:     agg.blockTimestamp,
		IngestedAt:         time.Now().UTC(),
		ManifestCreatedAt:  time.Now().UTC(),
	}
}

// computeEventIdsHash computes a deterministic hash of all event IDs.
func (b *Builder) computeEventIdsHash(events []*protov1.CanonicalEvent) string {
	hasher := sha256.New()

	for _, event := range events {
		hasher.Write([]byte(event.EventId))
		hasher.Write([]byte{0}) // Null separator
	}

	return "sha256:" + hex.EncodeToString(hasher.Sum(nil))
}

// startMetricsServer starts the HTTP metrics endpoint.
func (b *Builder) startMetricsServer() {
	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
	})

	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		b.mu.RLock()
		pendingBlocks := len(b.aggregators)
		b.mu.RUnlock()

		metrics := map[string]interface{}{
			"manifests_created":  b.manifestsCreated,
			"events_processed":   b.eventsProcessed,
			"blocks_aggregated":  b.blocksAggregated,
			"pending_blocks":     pendingBlocks,
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(metrics)
	})

	mux.HandleFunc("/pending", func(w http.ResponseWriter, r *http.Request) {
		b.mu.RLock()
		pending := make([]map[string]interface{}, 0)
		for key, agg := range b.aggregators {
			pending = append(pending, map[string]interface{}{
				"chain":        key.chain,
				"block":        key.blockNumber,
				"event_count":  len(agg.events),
				"tx_count":     len(agg.txSet),
				"last_updated": agg.lastUpdated,
			})
		}
		b.mu.RUnlock()

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(pending)
	})

	b.metricsServer = &http.Server{
		Addr:    b.cfg.MetricsAddr,
		Handler: mux,
	}

	b.logger.Info("starting metrics server", "addr", b.cfg.MetricsAddr)
	if err := b.metricsServer.ListenAndServe(); err != http.ErrServerClosed {
		b.logger.Error("metrics server error", "error", err)
	}
}

// Shutdown gracefully shuts down the builder.
func (b *Builder) Shutdown(ctx context.Context) error {
	b.logger.Info("shutting down manifest builder")

	// Flush any remaining manifests
	if err := b.flushAllManifests(ctx); err != nil {
		b.logger.Error("final flush error", "error", err)
	}

	// Shutdown metrics server
	if b.metricsServer != nil {
		if err := b.metricsServer.Shutdown(ctx); err != nil {
			b.logger.Error("metrics server shutdown error", "error", err)
		}
	}

	// Close Kafka client
	b.client.Close()

	return nil
}

// flushAllManifests flushes all pending manifests regardless of age.
func (b *Builder) flushAllManifests(ctx context.Context) error {
	b.mu.Lock()
	allBlocks := make([]*blockAggregator, 0, len(b.aggregators))
	for _, agg := range b.aggregators {
		if len(agg.events) > 0 {
			allBlocks = append(allBlocks, agg)
		}
	}
	b.aggregators = make(map[blockKey]*blockAggregator)
	b.mu.Unlock()

	if len(allBlocks) == 0 {
		return nil
	}

	b.logger.Info("final flush of manifests", "count", len(allBlocks))

	for _, agg := range allBlocks {
		manifest := b.buildManifest(agg)

		if err := b.repo.Save(ctx, manifest); err != nil {
			b.logger.Error("failed to save manifest on shutdown",
				"chain", manifest.Chain,
				"block", manifest.BlockNumber,
				"error", err,
			)
			continue
		}

		b.manifestsCreated++
	}

	return nil
}
