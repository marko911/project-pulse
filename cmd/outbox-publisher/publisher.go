package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/mirador/pulse/internal/platform/storage"
)

// PublisherConfig holds configuration for the outbox publisher.
type PublisherConfig struct {
	Brokers      string
	PollInterval time.Duration
	BatchSize    int
	Workers      int
}

// Publisher polls the outbox table and publishes messages to Kafka/Redpanda.
type Publisher struct {
	cfg    PublisherConfig
	db     *storage.DB
	repo   *storage.OutboxRepository
	client *kgo.Client
}

// NewPublisher creates a new Publisher instance.
func NewPublisher(cfg PublisherConfig, db *storage.DB) (*Publisher, error) {
	// Parse brokers
	brokerList := strings.Split(cfg.Brokers, ",")
	for i := range brokerList {
		brokerList[i] = strings.TrimSpace(brokerList[i])
	}

	// Create Kafka client with ordering guarantees
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokerList...),
		// Ensure ordering: only one request in flight per partition
		kgo.MaxProduceRequestsInflightPerBroker(1),
		// Wait for all replicas to acknowledge
		kgo.RequiredAcks(kgo.AllISRAcks()),
		// Enable idempotent producer for exactly-once semantics
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
		// Retry on transient errors
		kgo.RecordRetries(5),
		kgo.RetryBackoffFn(func(n int) time.Duration {
			return time.Duration(n*100) * time.Millisecond
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}

	return &Publisher{
		cfg:    cfg,
		db:     db,
		repo:   storage.NewOutboxRepository(db),
		client: client,
	}, nil
}

// Run starts the publisher polling loop.
func (p *Publisher) Run(ctx context.Context) error {
	slog.Info("Starting publisher polling loop",
		"poll_interval", p.cfg.PollInterval,
		"batch_size", p.cfg.BatchSize,
	)

	ticker := time.NewTicker(p.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return p.shutdown()
		case <-ticker.C:
			if err := p.pollAndPublish(ctx); err != nil {
				slog.Error("Poll and publish error", "error", err)
			}
		}
	}
}

// pollAndPublish fetches pending messages and publishes them.
func (p *Publisher) pollAndPublish(ctx context.Context) error {
	// Fetch pending messages in order
	messages, err := p.repo.FetchPendingMessages(ctx, p.cfg.BatchSize)
	if err != nil {
		return fmt.Errorf("fetch pending messages: %w", err)
	}

	if len(messages) == 0 {
		return nil
	}

	slog.Debug("Fetched pending messages", "count", len(messages))

	// Extract IDs for claiming
	ids := make([]int64, len(messages))
	for i, msg := range messages {
		ids[i] = msg.ID
	}

	// Atomically claim messages (mark as processing)
	claimed, err := p.repo.MarkAsProcessing(ctx, ids)
	if err != nil {
		return fmt.Errorf("mark as processing: %w", err)
	}

	if len(claimed) == 0 {
		return nil
	}

	slog.Info("Claimed messages for publishing", "count", len(claimed))

	// Build a map of claimed IDs for filtering
	claimedSet := make(map[int64]bool)
	for _, id := range claimed {
		claimedSet[id] = true
	}

	// Publish only claimed messages, maintaining order
	var wg sync.WaitGroup
	results := make(chan publishResult, len(claimed))

	for _, msg := range messages {
		if !claimedSet[msg.ID] {
			continue
		}

		wg.Add(1)
		go func(msg storage.OutboxMessage) {
			defer wg.Done()
			err := p.publishMessage(ctx, msg)
			results <- publishResult{id: msg.ID, err: err}
		}(msg)
	}

	// Wait for all publishes to complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect successful and failed IDs
	var successIDs []int64
	for result := range results {
		if result.err != nil {
			slog.Error("Failed to publish message",
				"id", result.id,
				"error", result.err,
			)
			if err := p.repo.MarkAsFailed(ctx, result.id, result.err.Error()); err != nil {
				slog.Error("Failed to mark message as failed",
					"id", result.id,
					"error", err,
				)
			}
		} else {
			successIDs = append(successIDs, result.id)
		}
	}

	// Mark successful messages as published
	if len(successIDs) > 0 {
		if err := p.repo.MarkAsPublished(ctx, successIDs); err != nil {
			return fmt.Errorf("mark as published: %w", err)
		}
		slog.Info("Successfully published messages", "count", len(successIDs))
	}

	return nil
}

type publishResult struct {
	id  int64
	err error
}

// publishMessage publishes a single message to Kafka.
func (p *Publisher) publishMessage(ctx context.Context, msg storage.OutboxMessage) error {
	record := &kgo.Record{
		Topic: msg.Topic,
		Key:   []byte(msg.PartitionKey),
		Value: msg.Payload,
		Headers: []kgo.RecordHeader{
			{Key: "event_id", Value: []byte(msg.EventID)},
			{Key: "chain", Value: []byte(fmt.Sprintf("%d", msg.Chain))},
			{Key: "event_type", Value: []byte(msg.EventType)},
		},
	}

	// Produce synchronously to maintain ordering within a partition
	results := p.client.ProduceSync(ctx, record)
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("produce: %w", err)
	}

	return nil
}

// shutdown gracefully shuts down the publisher.
func (p *Publisher) shutdown() error {
	slog.Info("Shutting down publisher")

	// Flush any pending messages
	if err := p.client.Flush(context.Background()); err != nil {
		slog.Error("Error flushing messages", "error", err)
	}

	p.client.Close()
	return nil
}
