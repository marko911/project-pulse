package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	pnats "github.com/marko911/project-pulse/internal/platform/nats"
	protov1 "github.com/marko911/project-pulse/pkg/proto/v1"
)

type Router interface {
	Route(event *protov1.CanonicalEvent) error
	RouteBatch(ctx context.Context, events []*protov1.CanonicalEvent) error
}

type NATSConsumerConfig struct {
	NATSURL      string
	StreamName   string
	ConsumerName string
	BatchSize    int
	FetchTimeout time.Duration
}

func DefaultNATSConsumerConfig() NATSConsumerConfig {
	return NATSConsumerConfig{
		NATSURL:      "nats://localhost:4222",
		StreamName:   "CANONICAL_EVENTS",
		ConsumerName: "api-gateway-fanout",
		BatchSize:    100,
		FetchTimeout: 5 * time.Second,
	}
}

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

func NewNATSConsumer(ctx context.Context, cfg NATSConsumerConfig, router Router, logger *slog.Logger) (*NATSConsumer, error) {
	if logger == nil {
		logger = slog.Default()
	}

	natsCfg := pnats.DefaultConfig()
	natsCfg.URL = cfg.NATSURL
	natsCfg.Name = cfg.ConsumerName

	client, err := pnats.Connect(ctx, natsCfg)
	if err != nil {
		return nil, fmt.Errorf("nats connect: %w", err)
	}

	streamCfg := pnats.DefaultCanonicalEventsStreamConfig()
	if cfg.StreamName != "" {
		streamCfg.Name = cfg.StreamName
	}

	stream, err := pnats.EnsureStream(ctx, client.JetStream(), streamCfg)
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("ensure stream: %w", err)
	}

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
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(100 * time.Millisecond):
				}
			}
		}
	}
}

func (nc *NATSConsumer) fetchAndRoute(ctx context.Context) error {
	msgs, err := nc.consumer.Fetch(nc.cfg.BatchSize, jetstream.FetchMaxWait(nc.cfg.FetchTimeout))
	if err != nil {
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

	if err := nc.router.RouteBatch(ctx, events); err != nil {
		nc.logger.Error("Route batch failed", "count", len(events), "error", err)
		for _, msg := range msgRefs {
			msg.Nak()
		}
		return err
	}

	for _, msg := range msgRefs {
		if err := msg.Ack(); err != nil {
			nc.logger.Warn("Failed to ack message", "error", err)
		}
	}

	nc.logger.Debug("Routed event batch", "count", len(events))
	return nil
}

func (nc *NATSConsumer) deserializeEvent(data []byte) (*protov1.CanonicalEvent, error) {
	var event protov1.CanonicalEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return nil, fmt.Errorf("unmarshal event: %w", err)
	}
	return &event, nil
}

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

func (nc *NATSConsumer) IsRunning() bool {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	return nc.running
}
