package nats

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type StreamConfig struct {
	Name        string
	Subjects    []string
	Retention   jetstream.RetentionPolicy
	MaxAge      time.Duration
	MaxMsgs     int64
	MaxBytes    int64
	Replicas    int
	Description string
}

func DefaultCanonicalEventsStreamConfig() StreamConfig {
	return StreamConfig{
		Name:        "CANONICAL_EVENTS",
		Subjects:    []string{"events.canonical.>"},
		Retention:   jetstream.InterestPolicy,
		MaxAge:      24 * time.Hour,
		MaxMsgs:     0,
		MaxBytes:    10 * 1024 * 1024 * 1024,
		Replicas:    1,
		Description: "Canonical blockchain events for WebSocket fanout",
	}
}

func EnsureStream(ctx context.Context, js jetstream.JetStream, cfg StreamConfig) (jetstream.Stream, error) {
	streamCfg := jetstream.StreamConfig{
		Name:        cfg.Name,
		Subjects:    cfg.Subjects,
		Retention:   cfg.Retention,
		MaxAge:      cfg.MaxAge,
		MaxMsgs:     cfg.MaxMsgs,
		MaxBytes:    cfg.MaxBytes,
		Replicas:    cfg.Replicas,
		Description: cfg.Description,
		Storage:     jetstream.FileStorage,
		Discard:     jetstream.DiscardOld,
	}

	stream, err := js.CreateOrUpdateStream(ctx, streamCfg)
	if err != nil {
		return nil, fmt.Errorf("ensure stream %s: %w", cfg.Name, err)
	}

	return stream, nil
}

type ConsumerConfig struct {
	Name          string
	Durable       bool
	FilterSubject string
	DeliverPolicy jetstream.DeliverPolicy
	AckPolicy     jetstream.AckPolicy
	AckWait       time.Duration
	MaxDeliver    int
	MaxAckPending int
}

func DefaultFanoutConsumerConfig(name string) ConsumerConfig {
	return ConsumerConfig{
		Name:          name,
		Durable:       true,
		FilterSubject: "",
		DeliverPolicy: jetstream.DeliverNewPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       30 * time.Second,
		MaxDeliver:    3,
		MaxAckPending: 1000,
	}
}

func EnsureConsumer(ctx context.Context, stream jetstream.Stream, cfg ConsumerConfig) (jetstream.Consumer, error) {
	consumerCfg := jetstream.ConsumerConfig{
		Name:          cfg.Name,
		Durable:       cfg.Name,
		DeliverPolicy: cfg.DeliverPolicy,
		AckPolicy:     cfg.AckPolicy,
		AckWait:       cfg.AckWait,
		MaxDeliver:    cfg.MaxDeliver,
		MaxAckPending: cfg.MaxAckPending,
	}

	if cfg.FilterSubject != "" {
		consumerCfg.FilterSubject = cfg.FilterSubject
	}

	consumer, err := stream.CreateOrUpdateConsumer(ctx, consumerCfg)
	if err != nil {
		return nil, fmt.Errorf("ensure consumer %s: %w", cfg.Name, err)
	}

	return consumer, nil
}

func SubjectForEvent(chain string, eventType string) string {
	return fmt.Sprintf("events.canonical.%s.%s", chain, eventType)
}

func SubjectForChain(chain string) string {
	return fmt.Sprintf("events.canonical.%s.>", chain)
}
