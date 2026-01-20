package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"

	protov1 "github.com/marko911/project-pulse/pkg/proto/v1"
)

// RouterConfig holds configuration for the trigger router.
type RouterConfig struct {
	Brokers       string
	InputTopic    string
	OutputTopic   string
	ConsumerGroup string
	RedisAddr     string
	RedisPassword string
	RedisDB       int
	RedisPrefix   string
	Workers       int
}

// Router routes canonical events to function invocations based on triggers.
type Router struct {
	cfg      RouterConfig
	consumer *kgo.Client
	producer *kgo.Client
	redis    *redis.Client
	triggers *TriggerManager

	wg sync.WaitGroup
}

// NewRouter creates a new Router instance.
func NewRouter(ctx context.Context, cfg RouterConfig) (*Router, error) {
	// Parse brokers
	brokerList := strings.Split(cfg.Brokers, ",")
	for i := range brokerList {
		brokerList[i] = strings.TrimSpace(brokerList[i])
	}

	// Create Kafka consumer
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokerList...),
		kgo.ConsumerGroup(cfg.ConsumerGroup),
		kgo.ConsumeTopics(cfg.InputTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka consumer: %w", err)
	}

	// Create Kafka producer
	producer, err := kgo.NewClient(
		kgo.SeedBrokers(brokerList...),
		kgo.MaxProduceRequestsInflightPerBroker(1),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
		kgo.RecordRetries(5),
	)
	if err != nil {
		consumer.Close()
		return nil, fmt.Errorf("create kafka producer: %w", err)
	}

	// Create Redis client
	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})

	// Verify Redis connection
	if err := redisClient.Ping(ctx).Err(); err != nil {
		consumer.Close()
		producer.Close()
		return nil, fmt.Errorf("redis ping: %w", err)
	}

	slog.Info("Connected to Redis", "addr", cfg.RedisAddr)

	return &Router{
		cfg:      cfg,
		consumer: consumer,
		producer: producer,
		redis:    redisClient,
		triggers: NewTriggerManager(redisClient, cfg.RedisPrefix),
	}, nil
}

// Run starts the router processing loop.
func (r *Router) Run(ctx context.Context) error {
	slog.Info("Starting trigger router",
		"input_topic", r.cfg.InputTopic,
		"output_topic", r.cfg.OutputTopic,
		"workers", r.cfg.Workers,
	)

	// Start worker pool
	eventCh := make(chan *kgo.Record, r.cfg.Workers*10)

	for i := 0; i < r.cfg.Workers; i++ {
		r.wg.Add(1)
		go r.worker(ctx, i, eventCh)
	}

	// Consume events
	for {
		select {
		case <-ctx.Done():
			close(eventCh)
			r.wg.Wait()
			return r.shutdown()
		default:
		}

		fetches := r.consumer.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if e.Err == context.Canceled {
					continue
				}
				slog.Error("Fetch error", "topic", e.Topic, "partition", e.Partition, "error", e.Err)
			}
			continue
		}

		fetches.EachRecord(func(record *kgo.Record) {
			select {
			case eventCh <- record:
			case <-ctx.Done():
			}
		})

		// Commit offsets periodically
		if err := r.consumer.CommitUncommittedOffsets(ctx); err != nil {
			slog.Error("Commit error", "error", err)
		}
	}
}

// worker processes events and routes them to function invocations.
func (r *Router) worker(ctx context.Context, id int, eventCh <-chan *kgo.Record) {
	defer r.wg.Done()

	slog.Debug("Worker started", "worker_id", id)

	for record := range eventCh {
		if err := r.processEvent(ctx, record); err != nil {
			slog.Error("Failed to process event",
				"worker_id", id,
				"offset", record.Offset,
				"error", err,
			)
		}
	}

	slog.Debug("Worker stopped", "worker_id", id)
}

// processEvent matches an event against triggers and enqueues invocations.
func (r *Router) processEvent(ctx context.Context, record *kgo.Record) error {
	// Deserialize the canonical event
	var event protov1.CanonicalEvent
	if err := json.Unmarshal(record.Value, &event); err != nil {
		return fmt.Errorf("unmarshal event: %w", err)
	}

	// Find matching triggers
	triggers, err := r.triggers.Match(ctx, &event)
	if err != nil {
		return fmt.Errorf("match triggers: %w", err)
	}

	if len(triggers) == 0 {
		slog.Debug("No matching triggers",
			"event_id", event.EventId,
			"event_type", event.EventType,
		)
		return nil
	}

	slog.Debug("Found matching triggers",
		"event_id", event.EventId,
		"trigger_count", len(triggers),
	)

	// Create invocations for each matching trigger
	for _, trigger := range triggers {
		invocation := &FunctionInvocation{
			InvocationID: generateInvocationID(),
			FunctionID:   trigger.FunctionID,
			TriggerID:    trigger.ID,
			TenantID:     trigger.TenantID,
			Event:        &event,
			CreatedAt:    time.Now(),
		}

		if err := r.enqueueInvocation(ctx, invocation); err != nil {
			slog.Error("Failed to enqueue invocation",
				"invocation_id", invocation.InvocationID,
				"function_id", invocation.FunctionID,
				"error", err,
			)
			continue
		}

		slog.Info("Enqueued function invocation",
			"invocation_id", invocation.InvocationID,
			"function_id", invocation.FunctionID,
			"trigger_id", trigger.ID,
			"event_id", event.EventId,
		)
	}

	return nil
}

// enqueueInvocation publishes an invocation to the function-invocations topic.
func (r *Router) enqueueInvocation(ctx context.Context, inv *FunctionInvocation) error {
	data, err := json.Marshal(inv)
	if err != nil {
		return fmt.Errorf("marshal invocation: %w", err)
	}

	record := &kgo.Record{
		Topic: r.cfg.OutputTopic,
		Key:   []byte(inv.FunctionID), // Partition by function for ordering
		Value: data,
		Headers: []kgo.RecordHeader{
			{Key: "invocation_id", Value: []byte(inv.InvocationID)},
			{Key: "function_id", Value: []byte(inv.FunctionID)},
			{Key: "tenant_id", Value: []byte(inv.TenantID)},
		},
	}

	results := r.producer.ProduceSync(ctx, record)
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("produce: %w", err)
	}

	return nil
}

// shutdown gracefully shuts down the router.
func (r *Router) shutdown() error {
	slog.Info("Shutting down router")

	// Final commit
	if err := r.consumer.CommitUncommittedOffsets(context.Background()); err != nil {
		slog.Error("Final commit error", "error", err)
	}

	// Flush producer
	if err := r.producer.Flush(context.Background()); err != nil {
		slog.Error("Producer flush error", "error", err)
	}

	r.consumer.Close()
	r.producer.Close()
	r.redis.Close()

	return nil
}

// generateInvocationID creates a unique invocation ID.
func generateInvocationID() string {
	return fmt.Sprintf("inv_%d_%d", time.Now().UnixNano(), time.Now().Nanosecond()%1000)
}
