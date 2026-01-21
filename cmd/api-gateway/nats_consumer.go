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

type NATSConsumer struct {
	client   *pnats.Client
	stream   jetstream.Stream
	consumer jetstream.Consumer
	logger   *slog.Logger

	onEvent func(*protov1.CanonicalEvent) error

	workers int
	jobCh   chan jetstream.Msg
	wg      sync.WaitGroup
	done    chan struct{}
}

type NATSConsumerConfig struct {
	URL          string
	ConsumerName string
	Workers      int
	Logger       *slog.Logger
	OnEvent      func(*protov1.CanonicalEvent) error
}

func NewNATSConsumer(ctx context.Context, cfg NATSConsumerConfig) (*NATSConsumer, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 4
	}

	natsCfg := pnats.DefaultConfig()
	natsCfg.URL = cfg.URL
	natsCfg.Name = "api-gateway-consumer"

	client, err := pnats.Connect(ctx, natsCfg)
	if err != nil {
		return nil, err
	}

	streamCfg := pnats.DefaultCanonicalEventsStreamConfig()
	stream, err := pnats.EnsureStream(ctx, client.JetStream(), streamCfg)
	if err != nil {
		client.Close()
		return nil, err
	}

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
		jobCh:    make(chan jetstream.Msg, cfg.Workers*10),
		done:     make(chan struct{}),
	}, nil
}

func (c *NATSConsumer) Start(ctx context.Context) error {
	c.logger.Info("starting NATS event consumption", "workers", c.workers)

	msgIter, err := c.consumer.Messages()
	if err != nil {
		return err
	}

	for i := 0; i < c.workers; i++ {
		c.wg.Add(1)
		go c.worker(ctx, i)
	}

	go func() {
		select {
		case <-ctx.Done():
		case <-c.done:
		}
		msgIter.Stop()
	}()

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
						return
					}
					c.logger.Error("error fetching message", "error", err)
					time.Sleep(100 * time.Millisecond)
					continue
				}

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
				msg.Nak()
			} else {
				msg.Ack()
			}
		}
	}
}

func (c *NATSConsumer) handleMessage(ctx context.Context, msg jetstream.Msg) error {
	var event NATSEvent
	if err := json.Unmarshal(msg.Data(), &event); err != nil {
		c.logger.Error("failed to unmarshal event", "error", err)
		return nil
	}

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

	if c.onEvent != nil {
		return c.onEvent(canonicalEvent)
	}

	return nil
}

func (c *NATSConsumer) Close() error {
	close(c.done)
	c.wg.Wait()
	close(c.jobCh)
	return c.client.Close()
}

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
