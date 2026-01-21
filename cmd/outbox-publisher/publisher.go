package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	pnats "github.com/marko911/project-pulse/internal/platform/nats"
	"github.com/marko911/project-pulse/internal/platform/storage"
)

type PublisherConfig struct {
	Brokers      string
	PollInterval time.Duration
	BatchSize    int
	Workers      int
	NATSUrl     string
	NATSEnabled bool
}

type Publisher struct {
	cfg    PublisherConfig
	db     *storage.DB
	repo   *storage.OutboxRepository
	client *kgo.Client
	nats *pnats.Client
}

func NewPublisher(ctx context.Context, cfg PublisherConfig, db *storage.DB) (*Publisher, error) {
	brokerList := strings.Split(cfg.Brokers, ",")
	for i := range brokerList {
		brokerList[i] = strings.TrimSpace(brokerList[i])
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokerList...),
		kgo.MaxProduceRequestsInflightPerBroker(1),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
		kgo.RecordRetries(5),
		kgo.RetryBackoffFn(func(n int) time.Duration {
			return time.Duration(n*100) * time.Millisecond
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}

	p := &Publisher{
		cfg:    cfg,
		db:     db,
		repo:   storage.NewOutboxRepository(db),
		client: client,
	}

	if cfg.NATSEnabled && cfg.NATSUrl != "" {
		natsCfg := pnats.DefaultConfig()
		natsCfg.URL = cfg.NATSUrl
		natsCfg.Name = "outbox-publisher"

		natsClient, err := pnats.Connect(ctx, natsCfg)
		if err != nil {
			client.Close()
			return nil, fmt.Errorf("nats connect: %w", err)
		}
		p.nats = natsClient

		streamCfg := pnats.DefaultCanonicalEventsStreamConfig()
		if _, err := pnats.EnsureStream(ctx, natsClient.JetStream(), streamCfg); err != nil {
			client.Close()
			natsClient.Close()
			return nil, fmt.Errorf("ensure nats stream: %w", err)
		}

		slog.Info("NATS JetStream initialized", "url", cfg.NATSUrl, "stream", streamCfg.Name)
	}

	return p, nil
}

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

func (p *Publisher) pollAndPublish(ctx context.Context) error {
	messages, err := p.repo.FetchPendingMessages(ctx, p.cfg.BatchSize)
	if err != nil {
		return fmt.Errorf("fetch pending messages: %w", err)
	}

	if len(messages) == 0 {
		return nil
	}

	slog.Debug("Fetched pending messages", "count", len(messages))

	ids := make([]int64, len(messages))
	for i, msg := range messages {
		ids[i] = msg.ID
	}

	claimed, err := p.repo.MarkAsProcessing(ctx, ids)
	if err != nil {
		return fmt.Errorf("mark as processing: %w", err)
	}

	if len(claimed) == 0 {
		return nil
	}

	slog.Info("Claimed messages for publishing", "count", len(claimed))

	claimedSet := make(map[int64]bool)
	for _, id := range claimed {
		claimedSet[id] = true
	}

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

	go func() {
		wg.Wait()
		close(results)
	}()

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

	results := p.client.ProduceSync(ctx, record)
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("kafka produce: %w", err)
	}

	if p.nats != nil {
		if err := p.publishToNATS(ctx, msg); err != nil {
			slog.Warn("NATS publish failed",
				"event_id", msg.EventID,
				"error", err,
			)
		}
	}

	return nil
}

func (p *Publisher) publishToNATS(ctx context.Context, msg storage.OutboxMessage) error {
	chainName := chainIDToName(msg.Chain)
	subject := pnats.SubjectForEvent(chainName, msg.EventType)

	_, err := p.nats.JetStream().Publish(ctx, subject, msg.Payload)
	if err != nil {
		return fmt.Errorf("jetstream publish: %w", err)
	}

	return nil
}

func chainIDToName(chainID int16) string {
	switch chainID {
	case 1:
		return "solana"
	case 2:
		return "ethereum"
	case 3:
		return "polygon"
	case 4:
		return "arbitrum"
	case 5:
		return "optimism"
	case 6:
		return "base"
	case 7:
		return "avalanche"
	case 8:
		return "bsc"
	default:
		return fmt.Sprintf("chain-%d", chainID)
	}
}

func (p *Publisher) shutdown() error {
	slog.Info("Shutting down publisher")

	if err := p.client.Flush(context.Background()); err != nil {
		slog.Error("Error flushing Kafka messages", "error", err)
	}

	p.client.Close()

	if p.nats != nil {
		if err := p.nats.Close(); err != nil {
			slog.Error("Error closing NATS connection", "error", err)
		}
	}

	return nil
}
