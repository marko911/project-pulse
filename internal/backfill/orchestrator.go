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

	protov1 "github.com/mirador/pulse/pkg/proto/v1"
)

// OrchestratorConfig configures the backfill orchestrator.
type OrchestratorConfig struct {
	// Brokers is the list of Kafka/Redpanda brokers.
	Brokers []string

	// GapEventsTopic is the topic to consume gap events from.
	GapEventsTopic string

	// BackfillRequestsTopic is the topic to publish backfill requests to.
	BackfillRequestsTopic string

	// BackfillResultsTopic is the topic to consume backfill results from (optional).
	BackfillResultsTopic string

	// ConsumerGroup is the consumer group ID.
	ConsumerGroup string

	// MaxRetries is the maximum number of retries for a backfill request.
	MaxRetries int

	// DefaultPriority is the default priority for backfill requests.
	DefaultPriority BackfillPriority

	// MaxConcurrentBackfills limits how many backfills can run simultaneously.
	MaxConcurrentBackfills int
}

// DefaultOrchestratorConfig returns sensible defaults.
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

// Orchestrator coordinates backfill operations in response to gap events.
type Orchestrator struct {
	cfg    OrchestratorConfig
	logger *slog.Logger

	consumer *kgo.Client
	producer *kgo.Client

	mu       sync.RWMutex
	pending  map[string]*BackfillTracker // requestID -> tracker
	active   int                         // number of active backfills
	stats    OrchestratorStats
	shutdown bool
}

// OrchestratorStats tracks orchestrator metrics.
type OrchestratorStats struct {
	GapsReceived      uint64
	BackfillsCreated  uint64
	BackfillsComplete uint64
	BackfillsFailed   uint64
	TotalBlocksFilled uint64
}

// NewOrchestrator creates a new backfill orchestrator.
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

// Start initializes Kafka clients and begins processing.
func (o *Orchestrator) Start(ctx context.Context) error {
	// Create consumer for gap events
	consumerOpts := []kgo.Opt{
		kgo.SeedBrokers(o.cfg.Brokers...),
		kgo.ConsumerGroup(o.cfg.ConsumerGroup),
		kgo.ConsumeTopics(o.cfg.GapEventsTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	}

	// Also consume backfill results if configured
	if o.cfg.BackfillResultsTopic != "" {
		consumerOpts = append(consumerOpts,
			kgo.ConsumeTopics(o.cfg.BackfillResultsTopic))
	}

	consumer, err := kgo.NewClient(consumerOpts...)
	if err != nil {
		return fmt.Errorf("create consumer: %w", err)
	}
	o.consumer = consumer

	// Create producer for backfill requests
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

// Run processes gap events and orchestrates backfills until context is cancelled.
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

// processRecord handles a single Kafka record.
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

// handleGapEvent processes a gap event and creates a backfill request.
func (o *Orchestrator) handleGapEvent(ctx context.Context, record *kgo.Record) error {
	var gap GapEvent
	if err := json.Unmarshal(record.Value, &gap); err != nil {
		o.logger.Warn("failed to parse gap event", "error", err)
		return nil // Don't return error - skip malformed events
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

	// Create backfill request for the gap
	request := o.createBackfillRequest(&gap)

	// Publish backfill request
	if err := o.publishBackfillRequest(ctx, request); err != nil {
		return fmt.Errorf("publish backfill request: %w", err)
	}

	// Track the pending request
	o.trackRequest(request)

	o.logger.Info("backfill request created",
		"request_id", request.RequestID,
		"chain", request.Chain,
		"start", request.StartBlock,
		"end", request.EndBlock,
	)

	return nil
}

// createBackfillRequest creates a BackfillRequest from a GapEvent.
func (o *Orchestrator) createBackfillRequest(gap *GapEvent) *BackfillRequest {
	// Generate a unique request ID
	requestID := uuid.New().String()

	return &BackfillRequest{
		RequestID:       requestID,
		Chain:           gap.Chain,
		CommitmentLevel: protov1.CommitmentLevel(gap.CommitmentLevel),
		StartBlock:      gap.ExpectedBlock,
		EndBlock:        gap.ReceivedBlock - 1, // Exclusive of received block
		Priority:        o.cfg.DefaultPriority,
		SourceGapID:     gap.EventID,
		RequestedAt:     time.Now(),
		RetryCount:      0,
		MaxRetries:      o.cfg.MaxRetries,
	}
}

// publishBackfillRequest publishes a backfill request to Kafka.
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

// trackRequest adds a request to the pending tracker.
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

// handleBackfillResult processes a backfill result.
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
		// Check if we should retry
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

// retryBackfill re-publishes a backfill request for retry.
func (o *Orchestrator) retryBackfill(ctx context.Context, request *BackfillRequest) {
	// Add a small delay before retry
	time.Sleep(time.Duration(request.RetryCount) * time.Second)

	if err := o.publishBackfillRequest(ctx, request); err != nil {
		o.logger.Error("failed to retry backfill",
			"request_id", request.RequestID,
			"error", err,
		)
	}
}

// GetStats returns current orchestrator statistics.
func (o *Orchestrator) GetStats() OrchestratorStats {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.stats
}

// GetPendingCount returns the number of pending backfill requests.
func (o *Orchestrator) GetPendingCount() int {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return len(o.pending)
}

// GetActiveCount returns the number of active backfills.
func (o *Orchestrator) GetActiveCount() int {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.active
}

// Shutdown gracefully shuts down the orchestrator.
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
