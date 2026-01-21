package backfill

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	protov1 "github.com/marko911/project-pulse/pkg/proto/v1"
)

type OrchestratorConfig struct {
	Brokers []string

	GapEventsTopic string

	BackfillRequestsTopic string

	BackfillResultsTopic string

	ConsumerGroup string

	MaxRetries int

	DefaultPriority BackfillPriority

	MaxConcurrentBackfills int
}

func DefaultOrchestratorConfig() OrchestratorConfig {
	return OrchestratorConfig{
		GapEventsTopic:         "gap-events",
		BackfillRequestsTopic:  "backfill-requests",
		BackfillResultsTopic:   "backfill-results",
		ConsumerGroup:          "backfill-orchestrator",
		MaxRetries:             3,
		DefaultPriority:        BackfillPriorityNormal,
		MaxConcurrentBackfills: 10,
	}
}

type Orchestrator struct {
	cfg    OrchestratorConfig
	logger *slog.Logger

	consumer *kgo.Client
	producer *kgo.Client

	mu       sync.RWMutex
	pending  map[string]*BackfillTracker
	active   int
	stats    OrchestratorStats
	shutdown bool
}

type OrchestratorStats struct {
	GapsReceived      uint64
	BackfillsCreated  uint64
	BackfillsComplete uint64
	BackfillsFailed   uint64
	TotalBlocksFilled uint64
}

func NewOrchestrator(cfg OrchestratorConfig, logger *slog.Logger) (*Orchestrator, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("no brokers configured")
	}
	if logger == nil {
		logger = slog.Default()
	}

	return &Orchestrator{
		cfg:     cfg,
		logger:  logger.With("component", "backfill-orchestrator"),
		pending: make(map[string]*BackfillTracker),
	}, nil
}

func (o *Orchestrator) Start(ctx context.Context) error {
	consumerOpts := []kgo.Opt{
		kgo.SeedBrokers(o.cfg.Brokers...),
		kgo.ConsumerGroup(o.cfg.ConsumerGroup),
		kgo.ConsumeTopics(o.cfg.GapEventsTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	}

	if o.cfg.BackfillResultsTopic != "" {
		consumerOpts = append(consumerOpts,
			kgo.ConsumeTopics(o.cfg.BackfillResultsTopic))
	}

	consumer, err := kgo.NewClient(consumerOpts...)
	if err != nil {
		return fmt.Errorf("create consumer: %w", err)
	}
	o.consumer = consumer

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(o.cfg.Brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		consumer.Close()
		return fmt.Errorf("create producer: %w", err)
	}
	o.producer = producer

	o.logger.Info("backfill orchestrator started",
		"gap_topic", o.cfg.GapEventsTopic,
		"backfill_topic", o.cfg.BackfillRequestsTopic,
	)

	return nil
}

func (o *Orchestrator) Run(ctx context.Context) error {
	if o.consumer == nil {
		return fmt.Errorf("orchestrator not started")
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		fetches := o.consumer.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}

		errs := fetches.Errors()
		for _, err := range errs {
			o.logger.Error("fetch error",
				"topic", err.Topic,
				"partition", err.Partition,
				"error", err.Err,
			)
		}

		fetches.EachRecord(func(record *kgo.Record) {
			if err := o.processRecord(ctx, record); err != nil {
				o.logger.Error("process record error",
					"error", err,
					"topic", record.Topic,
					"offset", record.Offset,
				)
			}
		})
	}
}

func (o *Orchestrator) processRecord(ctx context.Context, record *kgo.Record) error {
	switch record.Topic {
	case o.cfg.GapEventsTopic:
		return o.handleGapEvent(ctx, record)
	case o.cfg.BackfillResultsTopic:
		return o.handleBackfillResult(ctx, record)
	default:
		o.logger.Warn("unknown topic", "topic", record.Topic)
		return nil
	}
}

func (o *Orchestrator) handleGapEvent(ctx context.Context, record *kgo.Record) error {
	var gap GapEvent
	if err := json.Unmarshal(record.Value, &gap); err != nil {
		o.logger.Warn("failed to parse gap event", "error", err)
		return nil
	}

	o.mu.Lock()
	o.stats.GapsReceived++
	o.mu.Unlock()

	o.logger.Info("received gap event",
		"chain", gap.Chain,
		"expected", gap.ExpectedBlock,
		"received", gap.ReceivedBlock,
		"gap_size", gap.GapSize,
	)

	request := o.createBackfillRequest(&gap)

	if err := o.publishBackfillRequest(ctx, request); err != nil {
		return fmt.Errorf("publish backfill request: %w", err)
	}

	o.trackRequest(request)

	o.logger.Info("backfill request created",
		"request_id", request.RequestID,
		"chain", request.Chain,
		"start", request.StartBlock,
		"end", request.EndBlock,
	)

	return nil
}

func (o *Orchestrator) createBackfillRequest(gap *GapEvent) *BackfillRequest {
	requestID := uuid.New().String()

	return &BackfillRequest{
		RequestID:       requestID,
		Chain:           gap.Chain,
		CommitmentLevel: protov1.CommitmentLevel(gap.CommitmentLevel),
		StartBlock:      gap.ExpectedBlock,
		EndBlock:        gap.ReceivedBlock - 1,
		Priority:        o.cfg.DefaultPriority,
		SourceGapID:     gap.EventID,
		RequestedAt:     time.Now(),
		RetryCount:      0,
		MaxRetries:      o.cfg.MaxRetries,
	}
}

func (o *Orchestrator) publishBackfillRequest(ctx context.Context, request *BackfillRequest) error {
	data, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}

	record := &kgo.Record{
		Topic: o.cfg.BackfillRequestsTopic,
		Key:   []byte(fmt.Sprintf("%d:%d", request.Chain, request.StartBlock)),
		Value: data,
	}

	results := o.producer.ProduceSync(ctx, record)
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("produce: %w", err)
	}

	o.mu.Lock()
	o.stats.BackfillsCreated++
	o.mu.Unlock()

	return nil
}

func (o *Orchestrator) trackRequest(request *BackfillRequest) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.pending[request.RequestID] = &BackfillTracker{
		RequestID:   request.RequestID,
		Request:     request,
		Status:      BackfillStatusPending,
		StartedAt:   time.Now(),
		LastUpdated: time.Now(),
	}
	o.active++
}

func (o *Orchestrator) handleBackfillResult(ctx context.Context, record *kgo.Record) error {
	var result BackfillResult
	if err := json.Unmarshal(record.Value, &result); err != nil {
		o.logger.Warn("failed to parse backfill result", "error", err)
		return nil
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	tracker, exists := o.pending[result.RequestID]
	if !exists {
		o.logger.Warn("received result for unknown request", "request_id", result.RequestID)
		return nil
	}

	tracker.Status = result.Status
	tracker.LastUpdated = time.Now()

	switch result.Status {
	case BackfillStatusCompleted:
		o.stats.BackfillsComplete++
		o.stats.TotalBlocksFilled += result.BlocksProcessed
		delete(o.pending, result.RequestID)
		o.active--

		o.logger.Info("backfill completed",
			"request_id", result.RequestID,
			"blocks_processed", result.BlocksProcessed,
			"duration", result.Duration,
		)

	case BackfillStatusFailed:
		if tracker.Request.RetryCount < tracker.Request.MaxRetries {
			o.logger.Warn("backfill failed, retrying",
				"request_id", result.RequestID,
				"retry", tracker.Request.RetryCount+1,
				"error", result.Error,
			)
			tracker.Request.RetryCount++
			tracker.Status = BackfillStatusRetrying
			go o.retryBackfill(ctx, tracker.Request)
		} else {
			o.stats.BackfillsFailed++
			delete(o.pending, result.RequestID)
			o.active--

			o.logger.Error("backfill failed permanently",
				"request_id", result.RequestID,
				"error", result.Error,
			)
		}
	}

	return nil
}

func (o *Orchestrator) retryBackfill(ctx context.Context, request *BackfillRequest) {
	time.Sleep(time.Duration(request.RetryCount) * time.Second)

	if err := o.publishBackfillRequest(ctx, request); err != nil {
		o.logger.Error("failed to retry backfill",
			"request_id", request.RequestID,
			"error", err,
		)
	}
}

func (o *Orchestrator) GetStats() OrchestratorStats {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.stats
}

func (o *Orchestrator) GetPendingCount() int {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return len(o.pending)
}

func (o *Orchestrator) GetActiveCount() int {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.active
}

func (o *Orchestrator) Shutdown(ctx context.Context) error {
	o.mu.Lock()
	o.shutdown = true
	o.mu.Unlock()

	o.logger.Info("shutting down backfill orchestrator")

	if o.consumer != nil {
		o.consumer.Close()
	}
	if o.producer != nil {
		o.producer.Close()
	}

	return nil
}
