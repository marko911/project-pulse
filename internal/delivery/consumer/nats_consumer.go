// Package consumer provides NATS JetStream consumer for real-time event fanout.
package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	pnats "github.com/mirador/pulse/internal/platform/nats"
	protov1 "github.com/mirador/pulse/pkg/proto/v1"
)

// Router defines the interface for routing events to WebSocket clients.
type Router interface {
	Route(event *protov1.CanonicalEvent) error
	RouteBatch(ctx context.Context, events []*protov1.CanonicalEvent) error
}

// NATSConsumerConfig holds configuration for the NATS consumer.
type NATSConsumerConfig struct {
	NATSURL      string        // NATS server URL
	StreamName   string        // JetStream stream name
	ConsumerName string        // Durable consumer name
	BatchSize    int           // Messages to fetch per batch
	FetchTimeout time.Duration // Timeout for batch fetch
}

// DefaultNATSConsumerConfig returns sensible defaults.
func DefaultNATSConsumerConfig() NATSConsumerConfig {
	return NATSConsumerConfig{
		NATSURL:      "nats://localhost:4222",
		StreamName:   "CANONICAL_EVENTS",
		ConsumerName: "api-gateway-fanout",
		BatchSize:    100,
		FetchTimeout: 5 * time.Second,
	}
}

// NATSConsumer reads events from NATS JetStream and routes them to WebSocket clients.
type NATSConsumer struct {
	cfg      NATSConsumerConfig
	client   *pnats.Client
	consumer jetstream.Consumer
	router   Router
	logger   *slog.Logger

	mu      sync.Mutex
	running bool
	done    chan struct{}
}

// NewNATSConsumer creates a new NATS JetStream consumer.
func NewNATSConsumer(ctx context.Context, cfg NATSConsumerConfig, router Router, logger *slog.Logger) (*NATSConsumer, error) {
	if logger == nil {
		logger = slog.Default()
	}

	// Connect to NATS
	natsCfg := pnats.DefaultConfig()
	natsCfg.URL = cfg.NATSURL
	natsCfg.Name = cfg.ConsumerName

	client, err := pnats.Connect(ctx, natsCfg)
	if err != nil {
		return nil, fmt.Errorf("nats connect: %w", err)
	}

	// Ensure stream exists
	streamCfg := pnats.DefaultCanonicalEventsStreamConfig()
	if cfg.StreamName != "" {
		streamCfg.Name = cfg.StreamName
	}

	stream, err := pnats.EnsureStream(ctx, client.JetStream(), streamCfg)
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("ensure stream: %w", err)
	}

	// Create durable consumer
	consumerCfg := pnats.DefaultFanoutConsumerConfig(cfg.ConsumerName)
	consumer, err := pnats.EnsureConsumer(ctx, stream, consumerCfg)
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("ensure consumer: %w", err)
	}

	logger.Info("NATS consumer initialized",
		"url", cfg.NATSURL,
		"stream", streamCfg.Name,
		"consumer", cfg.ConsumerName,
	)

	return &NATSConsumer{
		cfg:      cfg,
		client:   client,
		consumer: consumer,
		router:   router,
		logger:   logger,
		done:     make(chan struct{}),
	}, nil
}

// Start begins consuming events from NATS and routing them.
func (nc *NATSConsumer) Start(ctx context.Context) error {
	nc.mu.Lock()
	if nc.running {
		nc.mu.Unlock()
		return fmt.Errorf("consumer already running")
	}
	nc.running = true
	nc.mu.Unlock()

	nc.logger.Info("Starting NATS consumer loop",
		"batch_size", nc.cfg.BatchSize,
		"fetch_timeout", nc.cfg.FetchTimeout,
	)

	for {
		select {
		case <-ctx.Done():
			nc.logger.Info("NATS consumer stopping")
			return nil
		case <-nc.done:
			nc.logger.Info("NATS consumer stopped")
			return nil
		default:
			if err := nc.fetchAndRoute(ctx); err != nil {
				nc.logger.Error("Fetch and route error", "error", err)
				// Brief backoff on error
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(100 * time.Millisecond):
				}
			}
		}
	}
}

// fetchAndRoute fetches a batch of messages and routes them to WebSocket clients.
func (nc *NATSConsumer) fetchAndRoute(ctx context.Context) error {
	// Fetch batch of messages
	msgs, err := nc.consumer.Fetch(nc.cfg.BatchSize, jetstream.FetchMaxWait(nc.cfg.FetchTimeout))
	if err != nil {
		// Timeout is normal when no messages available
		if err == context.DeadlineExceeded {
			return nil
		}
		return fmt.Errorf("fetch messages: %w", err)
	}

	var events []*protov1.CanonicalEvent
	var msgRefs []jetstream.Msg

	for msg := range msgs.Messages() {
		event, err := nc.deserializeEvent(msg.Data())
		if err != nil {
			nc.logger.Warn("Failed to deserialize event",
				"subject", msg.Subject(),
				"error", err,
			)
			// Nak to retry later
			msg.Nak()
			continue
		}

		events = append(events, event)
		msgRefs = append(msgRefs, msg)
	}

	if err := msgs.Error(); err != nil {
		nc.logger.Warn("Message iteration error", "error", err)
	}

	if len(events) == 0 {
		return nil
	}

	// Route batch to WebSocket clients
	if err := nc.router.RouteBatch(ctx, events); err != nil {
		nc.logger.Error("Route batch failed", "count", len(events), "error", err)
		// Nak all messages for retry
		for _, msg := range msgRefs {
			msg.Nak()
		}
		return err
	}

	// Ack all successfully routed messages
	for _, msg := range msgRefs {
		if err := msg.Ack(); err != nil {
			nc.logger.Warn("Failed to ack message", "error", err)
		}
	}

	nc.logger.Debug("Routed event batch", "count", len(events))
	return nil
}

// deserializeEvent converts JSON message data to CanonicalEvent.
func (nc *NATSConsumer) deserializeEvent(data []byte) (*protov1.CanonicalEvent, error) {
	var event protov1.CanonicalEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return nil, fmt.Errorf("unmarshal event: %w", err)
	}
	return &event, nil
}

// Stop gracefully stops the consumer.
func (nc *NATSConsumer) Stop() error {
	nc.mu.Lock()
	defer nc.mu.Unlock()

	if !nc.running {
		return nil
	}

	nc.running = false
	close(nc.done)

	if nc.client != nil {
		return nc.client.Close()
	}

	return nil
}

// IsRunning returns true if the consumer is actively running.
func (nc *NATSConsumer) IsRunning() bool {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	return nc.running
}
