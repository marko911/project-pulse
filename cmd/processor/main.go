// Command processor runs the core event processing service.
//
// This service consumes raw blockchain events from adapters, normalizes them
// to the canonical CanonicalEvent protobuf format, and publishes them to the
// output stream for downstream consumers.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/marko911/project-pulse/internal/adapter"
	"github.com/marko911/project-pulse/internal/processor"
)

func main() {
	// Configuration flags
	brokerEndpoint := flag.String("broker", getEnv("BROKER_ENDPOINT", "localhost:9092"), "Redpanda/Kafka broker endpoint")
	inputTopic := flag.String("input-topic", getEnv("INPUT_TOPIC", "raw-events"), "Topic to consume raw events from")
	outputTopic := flag.String("output-topic", getEnv("OUTPUT_TOPIC", "canonical-events"), "Topic to publish canonical events to")
	workers := flag.Int("workers", getEnvInt("WORKER_COUNT", 4), "Number of processing workers")
	consumerGroup := flag.String("consumer-group", getEnv("CONSUMER_GROUP", "processor"), "Kafka consumer group name")
	partitionCount := flag.Int("partition-count", getEnvInt("PARTITION_COUNT", 0), "Number of partitions for output topics (0 = broker default)")
	partitionKeyStrategy := flag.String("partition-key-strategy", getEnv("PARTITION_KEY_STRATEGY", "chain_block"), "Partition key strategy: chain_block, account, event_type, round_robin")
	logLevel := flag.String("log-level", getEnv("LOG_LEVEL", "info"), "Log level: debug, info, warn, error")
	flag.Parse()

	// Setup structured logging
	var level slog.Level
	switch *logLevel {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	}))
	slog.SetDefault(logger)

	logger.Info("starting processor",
		"broker", *brokerEndpoint,
		"input_topic", *inputTopic,
		"output_topic", *outputTopic,
		"workers", *workers,
		"consumer_group", *consumerGroup,
		"partition_count", *partitionCount,
		"partition_key_strategy", *partitionKeyStrategy,
	)

	// Create processor configuration
	cfg := processor.Config{
		WorkerCount:          *workers,
		BufferSize:           10000,
		BrokerEndpoint:       *brokerEndpoint,
		InputTopic:           *inputTopic,
		OutputTopic:          *outputTopic,
		ConsumerGroup:        *consumerGroup,
		PartitionCount:       *partitionCount,
		PartitionKeyStrategy: *partitionKeyStrategy,
	}

	// Setup context with signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		logger.Info("received shutdown signal", "signal", sig)
		cancel()
	}()

	// Initialize processor service
	svc, err := newProcessorService(ctx, cfg)
	if err != nil {
		logger.Error("failed to initialize processor", "error", err)
		os.Exit(1)
	}

	logger.Info("processor running, waiting for events...")

	// Run processing loop
	if err := svc.Run(ctx); err != nil && err != context.Canceled {
		logger.Error("processor error", "error", err)
		os.Exit(1)
	}

	// Graceful shutdown
	logger.Info("processor shutdown complete")
}

// processorService encapsulates the processor's Kafka consumer, producer, and normalization logic.
type processorService struct {
	cfg        processor.Config
	consumer   *kgo.Client
	producer   *kgo.Client
	normalizer *processor.NormalizerRegistry
	wg         sync.WaitGroup
}

// newProcessorService initializes a new processor service with Kafka connections.
func newProcessorService(ctx context.Context, cfg processor.Config) (*processorService, error) {
	brokerList := strings.Split(cfg.BrokerEndpoint, ",")
	for i := range brokerList {
		brokerList[i] = strings.TrimSpace(brokerList[i])
	}

	// Create Kafka consumer for raw-events
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

	// Create Kafka producer for canonical-events
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

	slog.Info("connected to kafka",
		"brokers", cfg.BrokerEndpoint,
		"consumer_group", cfg.ConsumerGroup,
		"input_topic", cfg.InputTopic,
		"output_topic", cfg.OutputTopic,
	)

	return &processorService{
		cfg:        cfg,
		consumer:   consumer,
		producer:   producer,
		normalizer: processor.NewNormalizerRegistry(),
	}, nil
}

// Run starts the processing loop with worker pool.
func (s *processorService) Run(ctx context.Context) error {
	// Create work channel
	eventCh := make(chan *kgo.Record, s.cfg.WorkerCount*10)

	// Start worker pool
	for i := 0; i < s.cfg.WorkerCount; i++ {
		s.wg.Add(1)
		go s.worker(ctx, i, eventCh)
	}

	// Consume loop
	for {
		select {
		case <-ctx.Done():
			close(eventCh)
			s.wg.Wait()
			return s.shutdown()
		default:
		}

		fetches := s.consumer.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if e.Err == context.Canceled {
					continue
				}
				slog.Error("fetch error", "topic", e.Topic, "partition", e.Partition, "error", e.Err)
			}
			continue
		}

		fetches.EachRecord(func(record *kgo.Record) {
			select {
			case eventCh <- record:
			case <-ctx.Done():
			}
		})

		// Commit offsets after processing
		if err := s.consumer.CommitUncommittedOffsets(ctx); err != nil && err != context.Canceled {
			slog.Error("commit error", "error", err)
		}
	}
}

// worker processes raw events and produces canonical events.
func (s *processorService) worker(ctx context.Context, id int, eventCh <-chan *kgo.Record) {
	defer s.wg.Done()
	slog.Debug("worker started", "worker_id", id)

	for record := range eventCh {
		if err := s.processEvent(ctx, record); err != nil {
			slog.Error("failed to process event",
				"worker_id", id,
				"offset", record.Offset,
				"error", err,
			)
		}
	}

	slog.Debug("worker stopped", "worker_id", id)
}

// processEvent normalizes a raw event and publishes the canonical event.
func (s *processorService) processEvent(ctx context.Context, record *kgo.Record) error {
	// Deserialize raw adapter event
	var event adapter.Event
	if err := json.Unmarshal(record.Value, &event); err != nil {
		return fmt.Errorf("unmarshal raw event: %w", err)
	}

	// Normalize to canonical format
	canonical, err := s.normalizer.Normalize(ctx, event)
	if err != nil {
		return fmt.Errorf("normalize event: %w", err)
	}

	// Serialize canonical event
	data, err := json.Marshal(canonical)
	if err != nil {
		return fmt.Errorf("marshal canonical event: %w", err)
	}

	// Generate partition key based on strategy
	partitionKey := s.generatePartitionKey(&event)

	// Produce to output topic
	outRecord := &kgo.Record{
		Topic: s.cfg.OutputTopic,
		Key:   []byte(partitionKey),
		Value: data,
		Headers: []kgo.RecordHeader{
			{Key: "event_id", Value: []byte(canonical.EventId)},
			{Key: "chain", Value: []byte(event.Chain)},
			{Key: "event_type", Value: []byte(event.EventType)},
		},
	}

	results := s.producer.ProduceSync(ctx, outRecord)
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("produce canonical event: %w", err)
	}

	slog.Debug("processed event",
		"event_id", canonical.EventId,
		"chain", event.Chain,
		"block", event.BlockNumber,
		"event_type", event.EventType,
	)

	return nil
}

// generatePartitionKey creates a partition key based on the configured strategy.
func (s *processorService) generatePartitionKey(event *adapter.Event) string {
	switch s.cfg.PartitionKeyStrategy {
	case "chain_block":
		return fmt.Sprintf("%s:%d", event.Chain, event.BlockNumber)
	case "account":
		if len(event.Accounts) > 0 {
			return event.Accounts[0]
		}
		return event.Chain
	case "event_type":
		return event.EventType
	case "round_robin":
		return "" // Empty key = round-robin distribution
	default:
		return fmt.Sprintf("%s:%d", event.Chain, event.BlockNumber)
	}
}

// shutdown gracefully shuts down the processor service.
func (s *processorService) shutdown() error {
	slog.Info("shutting down processor service")

	// Final commit
	if err := s.consumer.CommitUncommittedOffsets(context.Background()); err != nil {
		slog.Error("final commit error", "error", err)
	}

	// Flush producer
	if err := s.producer.Flush(context.Background()); err != nil {
		slog.Error("producer flush error", "error", err)
	}

	s.consumer.Close()
	s.producer.Close()

	return nil
}

// getEnv returns environment variable value or default.
func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

// getEnvInt returns environment variable as int or default.
func getEnvInt(key string, defaultVal int) int {
	if val := os.Getenv(key); val != "" {
		var result int
		if _, err := fmt.Sscanf(val, "%d", &result); err == nil {
			return result
		}
	}
	return defaultVal
}
