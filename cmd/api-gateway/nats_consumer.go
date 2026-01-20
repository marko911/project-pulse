package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	pnats "github.com/marko911/project-pulse/internal/platform/nats"
	protov1 "github.com/marko911/project-pulse/pkg/proto/v1"
)

// NATSConsumer subscribes to NATS JetStream and routes events to WebSocket clients.
type NATSConsumer struct {
	client   *pnats.Client
	stream   jetstream.Stream
	consumer jetstream.Consumer
	logger   *slog.Logger

	// Callback for routing events
	onEvent func(*protov1.CanonicalEvent) error

	// Worker pool configuration
	workers int
	jobCh   chan jetstream.Msg
	wg      sync.WaitGroup
	done    chan struct{}
}

// NATSConsumerConfig holds configuration for the NATS consumer.
type NATSConsumerConfig struct {
	URL          string
	ConsumerName string
	Workers      int // Number of concurrent message processing workers (default: 4)
	Logger       *slog.Logger
	OnEvent      func(*protov1.CanonicalEvent) error
}

// NewNATSConsumer creates a new NATS JetStream consumer for event fanout.
func NewNATSConsumer(ctx context.Context, cfg NATSConsumerConfig) (*NATSConsumer, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 4 // Default to 4 workers
	}

	natsCfg := pnats.DefaultConfig()
	natsCfg.URL = cfg.URL
	natsCfg.Name = "api-gateway-consumer"

	client, err := pnats.Connect(ctx, natsCfg)
	if err != nil {
		return nil, err
	}

	// Ensure the stream exists
	streamCfg := pnats.DefaultCanonicalEventsStreamConfig()
	stream, err := pnats.EnsureStream(ctx, client.JetStream(), streamCfg)
	if err != nil {
		client.Close()
		return nil, err
	}

	// Create consumer for this gateway instance
	consumerCfg := pnats.DefaultFanoutConsumerConfig(cfg.ConsumerName)
	consumer, err := pnats.EnsureConsumer(ctx, stream, consumerCfg)
	if err != nil {
		client.Close()
		return nil, err
	}

	cfg.Logger.Info("NATS consumer initialized",
		"url", cfg.URL,
		"stream", streamCfg.Name,
		"consumer", cfg.ConsumerName,
		"workers", cfg.Workers,
	)

	return &NATSConsumer{
		client:   client,
		stream:   stream,
		consumer: consumer,
		logger:   cfg.Logger,
		onEvent:  cfg.OnEvent,
		workers:  cfg.Workers,
		jobCh:    make(chan jetstream.Msg, cfg.Workers*10), // Buffered job queue
		done:     make(chan struct{}),
	}, nil
}

// Start begins consuming events from NATS JetStream.
func (c *NATSConsumer) Start(ctx context.Context) error {
	c.logger.Info("starting NATS event consumption", "workers", c.workers)

	// Create a message iterator for the consumer
	msgIter, err := c.consumer.Messages()
	if err != nil {
		return err
	}

	// Start worker pool for concurrent message processing
	for i := 0; i < c.workers; i++ {
		c.wg.Add(1)
		go c.worker(ctx, i)
	}

	// Stop iterator on context cancellation or done signal
	go func() {
		select {
		case <-ctx.Done():
		case <-c.done:
		}
		msgIter.Stop()
	}()

	// Dispatcher: fetch messages and distribute to workers
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-c.done:
				return
			default:
				msg, err := msgIter.Next()
				if err != nil {
					if ctx.Err() != nil {
						return // Context cancelled
					}
					c.logger.Error("error fetching message", "error", err)
					time.Sleep(100 * time.Millisecond)
					continue
				}

				// Dispatch to worker pool
				select {
				case c.jobCh <- msg:
				case <-ctx.Done():
					return
				case <-c.done:
					return
				}
			}
		}
	}()

	return nil
}

// worker processes messages from the job channel.
func (c *NATSConsumer) worker(ctx context.Context, id int) {
	defer c.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.done:
			return
		case msg, ok := <-c.jobCh:
			if !ok {
				return
			}

			if err := c.handleMessage(ctx, msg); err != nil {
				c.logger.Error("error handling message",
					"worker", id,
					"subject", msg.Subject(),
					"error", err,
				)
				msg.Nak() // Negative acknowledgment for redelivery
			} else {
				msg.Ack()
			}
		}
	}
}

// handleMessage processes a single NATS message.
func (c *NATSConsumer) handleMessage(ctx context.Context, msg jetstream.Msg) error {
	// Deserialize the event from the message
	var event NATSEvent
	if err := json.Unmarshal(msg.Data(), &event); err != nil {
		c.logger.Error("failed to unmarshal event", "error", err)
		return nil // Don't retry malformed messages
	}

	// Convert to CanonicalEvent
	canonicalEvent := &protov1.CanonicalEvent{
		EventId:         event.EventID,
		Chain:           event.Chain,
		CommitmentLevel: event.CommitmentLevel,
		BlockNumber:     event.BlockNumber,
		BlockHash:       event.BlockHash,
		TxHash:          event.TxHash,
		EventType:       event.EventType,
		Accounts:        event.Accounts,
		Timestamp:       event.Timestamp,
		Payload:         event.Payload,
		ReorgAction:     event.ReorgAction,
	}

	// Route the event
	if c.onEvent != nil {
		return c.onEvent(canonicalEvent)
	}

	return nil
}

// Close shuts down the NATS consumer gracefully.
func (c *NATSConsumer) Close() error {
	// Signal workers to stop
	close(c.done)
	// Wait for workers to finish
	c.wg.Wait()
	// Close the job channel
	close(c.jobCh)
	// Close NATS connection
	return c.client.Close()
}

// NATSEvent represents the event structure published to NATS.
type NATSEvent struct {
	EventID         string                   `json:"event_id"`
	Chain           protov1.Chain            `json:"chain"`
	CommitmentLevel protov1.CommitmentLevel  `json:"commitment_level"`
	BlockNumber     uint64                   `json:"block_number"`
	BlockHash       string                   `json:"block_hash"`
	TxHash          string                   `json:"tx_hash"`
	EventType       string                   `json:"event_type"`
	Accounts        []string                 `json:"accounts"`
	Timestamp       time.Time                `json:"timestamp"`
	Payload         []byte                   `json:"payload"`
	ReorgAction     protov1.ReorgAction      `json:"reorg_action"`
	PublishedAt     time.Time                `json:"published_at"`
}
