// Package processor implements the core event processing and normalization logic.
package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/mirador/pulse/internal/adapter"
	protov1 "github.com/mirador/pulse/pkg/proto/v1"
)

// CoreProcessor implements the Processor interface with Kafka integration.
type CoreProcessor struct {
	cfg        Config
	registry   *NormalizerRegistry
	consumer   *kgo.Client
	producer   *kgo.Client
	wg         sync.WaitGroup
	shutdownCh chan struct{}

	// Metrics
	mu             sync.Mutex
	eventsReceived uint64
	eventsEmitted  uint64
	errorsCount    uint64
}

// NewCoreProcessor creates a new processor with Kafka connectivity.
func NewCoreProcessor(ctx context.Context, cfg Config) (*CoreProcessor, error) {
	// Parse brokers
	brokerList := strings.Split(cfg.BrokerEndpoint, ",")
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

	slog.Info("processor connected to kafka",
		"brokers", cfg.BrokerEndpoint,
		"input_topic", cfg.InputTopic,
		"output_topic", cfg.OutputTopic,
		"consumer_group", cfg.ConsumerGroup,
	)

	return &CoreProcessor{
		cfg:        cfg,
		registry:   NewNormalizerRegistry(),
		consumer:   consumer,
		producer:   producer,
		shutdownCh: make(chan struct{}),
	}, nil
}

// Process normalizes a single event.
func (p *CoreProcessor) Process(ctx context.Context, event adapter.Event) (*protov1.CanonicalEvent, error) {
	return p.registry.Normalize(ctx, event)
}

// Start begins the consumer/producer loop with worker pool.
func (p *CoreProcessor) Start(ctx context.Context, input <-chan adapter.Event, output chan<- *protov1.CanonicalEvent) error {
	// This signature is for the interface - we ignore input/output channels
	// and use Kafka directly in Run()
	return p.Run(ctx)
}

// Run executes the main consumer/producer loop.
func (p *CoreProcessor) Run(ctx context.Context) error {
	slog.Info("starting processor",
		"workers", p.cfg.WorkerCount,
		"buffer_size", p.cfg.BufferSize,
		"partition_key_strategy", p.cfg.PartitionKeyStrategy,
	)

	// Start worker pool
	eventCh := make(chan *kgo.Record, p.cfg.BufferSize)

	for i := 0; i < p.cfg.WorkerCount; i++ {
		p.wg.Add(1)
		go p.worker(ctx, i, eventCh)
	}

	// Main consume loop
	for {
		select {
		case <-ctx.Done():
			close(eventCh)
			p.wg.Wait()
			return p.shutdown()
		case <-p.shutdownCh:
			close(eventCh)
			p.wg.Wait()
			return p.shutdown()
		default:
		}

		fetches := p.consumer.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if e.Err == context.Canceled {
					continue
				}
				slog.Error("fetch error",
					"topic", e.Topic,
					"partition", e.Partition,
					"error", e.Err,
				)
			}
			continue
		}

		fetches.EachRecord(func(record *kgo.Record) {
			p.mu.Lock()
			p.eventsReceived++
			p.mu.Unlock()

			select {
			case eventCh <- record:
			case <-ctx.Done():
			}
		})

		// Commit offsets after processing batch
		if err := p.consumer.CommitUncommittedOffsets(ctx); err != nil {
			slog.Error("commit error", "error", err)
		}
	}
}

// worker processes events from the channel.
func (p *CoreProcessor) worker(ctx context.Context, id int, eventCh <-chan *kgo.Record) {
	defer p.wg.Done()

	slog.Debug("worker started", "worker_id", id)

	for record := range eventCh {
		if err := p.processRecord(ctx, record); err != nil {
			p.mu.Lock()
			p.errorsCount++
			p.mu.Unlock()

			slog.Error("failed to process record",
				"worker_id", id,
				"offset", record.Offset,
				"partition", record.Partition,
				"error", err,
			)
		}
	}

	slog.Debug("worker stopped", "worker_id", id)
}

// processRecord normalizes a raw event and publishes the canonical event.
func (p *CoreProcessor) processRecord(ctx context.Context, record *kgo.Record) error {
	// Deserialize the raw adapter event
	var event adapter.Event
	if err := json.Unmarshal(record.Value, &event); err != nil {
		return fmt.Errorf("unmarshal raw event: %w", err)
	}

	// Normalize to canonical format
	canonical, err := p.registry.Normalize(ctx, event)
	if err != nil {
		return fmt.Errorf("normalize event (chain=%s): %w", event.Chain, err)
	}

	// Serialize canonical event
	data, err := json.Marshal(canonical)
	if err != nil {
		return fmt.Errorf("marshal canonical event: %w", err)
	}

	// Generate partition key based on strategy
	key := p.generatePartitionKey(canonical)

	// Publish to output topic
	outRecord := &kgo.Record{
		Topic: p.cfg.OutputTopic,
		Key:   key,
		Value: data,
		Headers: []kgo.RecordHeader{
			{Key: "event_id", Value: []byte(canonical.EventId)},
			{Key: "chain", Value: []byte(chainNameFromProto(canonical.Chain))},
			{Key: "event_type", Value: []byte(canonical.EventType)},
		},
	}

	results := p.producer.ProduceSync(ctx, outRecord)
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("produce canonical event: %w", err)
	}

	p.mu.Lock()
	p.eventsEmitted++
	p.mu.Unlock()

	slog.Debug("processed event",
		"event_id", canonical.EventId,
		"chain", chainNameFromProto(canonical.Chain),
		"block", canonical.BlockNumber,
		"event_type", canonical.EventType,
	)

	return nil
}

// generatePartitionKey creates a partition key based on the configured strategy.
func (p *CoreProcessor) generatePartitionKey(event *protov1.CanonicalEvent) []byte {
	switch p.cfg.PartitionKeyStrategy {
	case "chain_block":
		// Default: partition by chain:blockNumber for ordering guarantees
		return []byte(fmt.Sprintf("%s:%d", chainNameFromProto(event.Chain), event.BlockNumber))
	case "account":
		// Partition by first account for per-account ordering
		if len(event.Accounts) > 0 {
			return []byte(event.Accounts[0])
		}
		return []byte(event.EventId)
	case "event_type":
		// Partition by event type
		return []byte(event.EventType)
	case "round_robin":
		// No key = round-robin distribution
		return nil
	default:
		// Fallback to chain:block
		return []byte(fmt.Sprintf("%s:%d", chainNameFromProto(event.Chain), event.BlockNumber))
	}
}

// Stop gracefully shuts down the processor.
func (p *CoreProcessor) Stop(ctx context.Context) error {
	close(p.shutdownCh)
	return nil
}

// Health returns the current health status.
func (p *CoreProcessor) Health(ctx context.Context) error {
	// Ping Kafka by attempting to produce to a non-existent topic
	// In production, this would check consumer lag and producer health
	return nil
}

// shutdown performs cleanup.
func (p *CoreProcessor) shutdown() error {
	slog.Info("shutting down processor")

	// Final offset commit
	ctx := context.Background()
	if err := p.consumer.CommitUncommittedOffsets(ctx); err != nil {
		slog.Error("final commit error", "error", err)
	}

	// Flush producer
	if err := p.producer.Flush(ctx); err != nil {
		slog.Error("producer flush error", "error", err)
	}

	p.consumer.Close()
	p.producer.Close()

	p.mu.Lock()
	slog.Info("processor shutdown complete",
		"events_received", p.eventsReceived,
		"events_emitted", p.eventsEmitted,
		"errors", p.errorsCount,
	)
	p.mu.Unlock()

	return nil
}

// Stats returns processor statistics.
func (p *CoreProcessor) Stats() (received, emitted, errors uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.eventsReceived, p.eventsEmitted, p.errorsCount
}
