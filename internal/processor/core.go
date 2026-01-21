package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/marko911/project-pulse/internal/adapter"
	protov1 "github.com/marko911/project-pulse/pkg/proto/v1"
)

type CoreProcessor struct {
	cfg        Config
	registry   *NormalizerRegistry
	consumer   *kgo.Client
	producer   *kgo.Client
	wg         sync.WaitGroup
	shutdownCh chan struct{}

	mu             sync.Mutex
	eventsReceived uint64
	eventsEmitted  uint64
	errorsCount    uint64
}

func NewCoreProcessor(ctx context.Context, cfg Config) (*CoreProcessor, error) {
	brokerList := strings.Split(cfg.BrokerEndpoint, ",")
	for i := range brokerList {
		brokerList[i] = strings.TrimSpace(brokerList[i])
	}

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

func (p *CoreProcessor) Process(ctx context.Context, event adapter.Event) (*protov1.CanonicalEvent, error) {
	return p.registry.Normalize(ctx, event)
}

func (p *CoreProcessor) Start(ctx context.Context, input <-chan adapter.Event, output chan<- *protov1.CanonicalEvent) error {
	return p.Run(ctx)
}

func (p *CoreProcessor) Run(ctx context.Context) error {
	slog.Info("starting processor",
		"workers", p.cfg.WorkerCount,
		"buffer_size", p.cfg.BufferSize,
		"partition_key_strategy", p.cfg.PartitionKeyStrategy,
	)

	eventCh := make(chan *kgo.Record, p.cfg.BufferSize)

	for i := 0; i < p.cfg.WorkerCount; i++ {
		p.wg.Add(1)
		go p.worker(ctx, i, eventCh)
	}

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

		if err := p.consumer.CommitUncommittedOffsets(ctx); err != nil {
			slog.Error("commit error", "error", err)
		}
	}
}

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

func (p *CoreProcessor) processRecord(ctx context.Context, record *kgo.Record) error {
	var event adapter.Event
	if err := json.Unmarshal(record.Value, &event); err != nil {
		return fmt.Errorf("unmarshal raw event: %w", err)
	}

	canonical, err := p.registry.Normalize(ctx, event)
	if err != nil {
		return fmt.Errorf("normalize event (chain=%s): %w", event.Chain, err)
	}

	data, err := json.Marshal(canonical)
	if err != nil {
		return fmt.Errorf("marshal canonical event: %w", err)
	}

	key := p.generatePartitionKey(canonical)

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

func (p *CoreProcessor) generatePartitionKey(event *protov1.CanonicalEvent) []byte {
	switch p.cfg.PartitionKeyStrategy {
	case "chain_block":
		return []byte(fmt.Sprintf("%s:%d", chainNameFromProto(event.Chain), event.BlockNumber))
	case "account":
		if len(event.Accounts) > 0 {
			return []byte(event.Accounts[0])
		}
		return []byte(event.EventId)
	case "event_type":
		return []byte(event.EventType)
	case "round_robin":
		return nil
	default:
		return []byte(fmt.Sprintf("%s:%d", chainNameFromProto(event.Chain), event.BlockNumber))
	}
}

func (p *CoreProcessor) Stop(ctx context.Context) error {
	close(p.shutdownCh)
	return nil
}

func (p *CoreProcessor) Health(ctx context.Context) error {
	return nil
}

func (p *CoreProcessor) shutdown() error {
	slog.Info("shutting down processor")

	ctx := context.Background()
	if err := p.consumer.CommitUncommittedOffsets(ctx); err != nil {
		slog.Error("final commit error", "error", err)
	}

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

func (p *CoreProcessor) Stats() (received, emitted, errors uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.eventsReceived, p.eventsEmitted, p.errorsCount
}
