package nats

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// StreamConfig defines the configuration for a JetStream stream.
type StreamConfig struct {
	Name        string          // Stream name (e.g., "CANONICAL_EVENTS")
	Subjects    []string        // Subjects to capture (e.g., ["events.canonical.>"])
	Retention   jetstream.RetentionPolicy
	MaxAge      time.Duration   // Maximum message age (0 = unlimited)
	MaxMsgs     int64           // Maximum messages per subject (0 = unlimited)
	MaxBytes    int64           // Maximum stream size in bytes (0 = unlimited)
	Replicas    int             // Number of replicas (1 for dev, 3 for prod)
	Description string
}

// DefaultCanonicalEventsStreamConfig returns the stream configuration for canonical events fanout.
func DefaultCanonicalEventsStreamConfig() StreamConfig {
	return StreamConfig{
		Name:        "CANONICAL_EVENTS",
		Subjects:    []string{"events.canonical.>"},
		Retention:   jetstream.InterestPolicy, // Only retain while consumers are interested
		MaxAge:      24 * time.Hour,           // Retain for 24h for replay capability
		MaxMsgs:     0,                        // Unlimited
		MaxBytes:    10 * 1024 * 1024 * 1024,  // 10GB max
		Replicas:    1,                        // Single replica for dev
		Description: "Canonical blockchain events for WebSocket fanout",
	}
}

// EnsureStream creates or updates a JetStream stream with the given configuration.
// This is idempotent - safe to call multiple times.
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

// ConsumerConfig defines the configuration for a JetStream consumer.
type ConsumerConfig struct {
	Name          string             // Consumer name (must be unique per stream)
	Durable       bool               // Persist consumer state across restarts
	FilterSubject string             // Optional subject filter within stream
	DeliverPolicy jetstream.DeliverPolicy
	AckPolicy     jetstream.AckPolicy
	AckWait       time.Duration      // Time to wait for acknowledgment
	MaxDeliver    int                // Maximum delivery attempts (-1 = unlimited)
	MaxAckPending int                // Maximum outstanding unacknowledged messages
}

// DefaultFanoutConsumerConfig returns consumer configuration for WebSocket fanout.
func DefaultFanoutConsumerConfig(name string) ConsumerConfig {
	return ConsumerConfig{
		Name:          name,
		Durable:       true,
		FilterSubject: "", // All subjects in stream
		DeliverPolicy: jetstream.DeliverNewPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       30 * time.Second,
		MaxDeliver:    3,
		MaxAckPending: 1000,
	}
}

// EnsureConsumer creates or updates a durable consumer on the given stream.
func EnsureConsumer(ctx context.Context, stream jetstream.Stream, cfg ConsumerConfig) (jetstream.Consumer, error) {
	consumerCfg := jetstream.ConsumerConfig{
		Name:          cfg.Name,
		Durable:       cfg.Name, // Durable name = consumer name
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

// SubjectForEvent returns the NATS subject for a canonical event.
// Format: events.canonical.<chain>.<event_type>
func SubjectForEvent(chain string, eventType string) string {
	return fmt.Sprintf("events.canonical.%s.%s", chain, eventType)
}

// SubjectForChain returns the wildcard subject for all events on a chain.
// Format: events.canonical.<chain>.>
func SubjectForChain(chain string) string {
	return fmt.Sprintf("events.canonical.%s.>", chain)
}
