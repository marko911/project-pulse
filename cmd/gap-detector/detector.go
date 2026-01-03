package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	protov1 "github.com/mirador/pulse/pkg/proto/v1"
)

// DetectorConfig holds configuration for the gap detector.
type DetectorConfig struct {
	Brokers       []string
	Topics        []string
	ConsumerGroup string
	PollInterval  time.Duration
	StateDir      string
	AlertWebhook  string
	MetricsAddr   string
	MaxGapBlocks  int64
}

// GapEvent represents a detected gap in block sequence.
type GapEvent struct {
	Chain           protov1.Chain `json:"chain"`
	CommitmentLevel int16         `json:"commitment_level"`
	ExpectedBlock   uint64        `json:"expected_block"`
	ReceivedBlock   uint64        `json:"received_block"`
	GapSize         uint64        `json:"gap_size"`
	DetectedAt      time.Time     `json:"detected_at"`
	Topic           string        `json:"topic"`
}

// Detector monitors finalized topics for gaps in block sequences.
// It implements a fail-closed design: if a gap is detected, it halts processing.
type Detector struct {
	cfg    DetectorConfig
	logger *slog.Logger

	client *kgo.Client
	state  *ChainState

	mu       sync.Mutex
	halted   bool
	haltErr  error
	gapCount int64

	// Metrics
	metricsServer *http.Server
	blocksScanned int64
	lastBlockTime map[chainKey]time.Time
}

// chainKey identifies a unique chain + commitment level combination.
type chainKey struct {
	Chain           protov1.Chain
	CommitmentLevel int16
}

// NewDetector creates a new gap detector instance.
func NewDetector(cfg DetectorConfig, logger *slog.Logger) (*Detector, error) {
	// Create chain state tracker
	state := NewChainState()

	d := &Detector{
		cfg:           cfg,
		logger:        logger,
		state:         state,
		lastBlockTime: make(map[chainKey]time.Time),
	}

	return d, nil
}

// Run starts the gap detector and blocks until context is cancelled or halt occurs.
func (d *Detector) Run(ctx context.Context) error {
	// Initialize Kafka client
	opts := []kgo.Opt{
		kgo.SeedBrokers(d.cfg.Brokers...),
		kgo.ConsumerGroup(d.cfg.ConsumerGroup),
		kgo.ConsumeTopics(d.cfg.Topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("create kafka client: %w", err)
	}
	d.client = client

	// Start metrics server
	if d.cfg.MetricsAddr != "" {
		go d.runMetricsServer()
	}

	d.logger.Info("gap detector started, monitoring topics", "topics", d.cfg.Topics)

	// Main consumption loop
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Check if halted
		if d.IsHalted() {
			d.logger.Error("detector halted due to gap", "error", d.haltErr)
			return d.haltErr
		}

		// Fetch records
		fetches := d.client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				d.logger.Error("fetch error", "topic", e.Topic, "partition", e.Partition, "error", e.Err)
			}
			continue
		}

		// Process each record
		fetches.EachRecord(func(record *kgo.Record) {
			if err := d.processRecord(ctx, record); err != nil {
				d.logger.Error("process record error", "error", err, "offset", record.Offset)
			}
		})

		// Commit offsets if not halted
		if !d.IsHalted() {
			if err := d.client.CommitUncommittedOffsets(ctx); err != nil {
				d.logger.Error("commit error", "error", err)
			}
		}
	}
}

// processRecord processes a single Kafka record and checks for gaps.
func (d *Detector) processRecord(ctx context.Context, record *kgo.Record) error {
	// Parse the canonical event from the record
	var event protov1.CanonicalEvent
	if err := json.Unmarshal(record.Value, &event); err != nil {
		// Log but don't halt on parse errors - could be schema evolution
		d.logger.Warn("failed to parse event", "error", err, "offset", record.Offset)
		return nil
	}

	key := chainKey{
		Chain:           event.Chain,
		CommitmentLevel: int16(event.CommitmentLevel),
	}

	// Check for gap
	gapEvent := d.state.CheckAndUpdate(key.Chain, key.CommitmentLevel, event.BlockNumber, record.Topic)

	if gapEvent != nil {
		d.handleGap(ctx, gapEvent)
	}

	// Update metrics
	d.mu.Lock()
	d.blocksScanned++
	d.lastBlockTime[key] = time.Now()
	d.mu.Unlock()

	d.logger.Debug("processed block",
		"chain", event.Chain,
		"block", event.BlockNumber,
		"commitment", event.CommitmentLevel,
	)

	return nil
}

// handleGap handles a detected gap - logs, alerts, and halts.
func (d *Detector) handleGap(ctx context.Context, gap *GapEvent) {
	d.logger.Error("GAP DETECTED - HALTING",
		"chain", gap.Chain,
		"expected_block", gap.ExpectedBlock,
		"received_block", gap.ReceivedBlock,
		"gap_size", gap.GapSize,
		"commitment_level", gap.CommitmentLevel,
		"topic", gap.Topic,
	)

	// Check if gap exceeds threshold
	if d.cfg.MaxGapBlocks > 0 && int64(gap.GapSize) <= d.cfg.MaxGapBlocks {
		d.logger.Warn("gap within threshold, not halting",
			"gap_size", gap.GapSize,
			"threshold", d.cfg.MaxGapBlocks,
		)
		return
	}

	// Send alert webhook if configured
	if d.cfg.AlertWebhook != "" {
		go d.sendAlert(gap)
	}

	// FAIL-CLOSED: Halt the detector
	d.halt(fmt.Errorf("gap detected: chain=%d expected=%d received=%d gap=%d",
		gap.Chain, gap.ExpectedBlock, gap.ReceivedBlock, gap.GapSize))
}

// halt stops the detector in a fail-closed manner.
func (d *Detector) halt(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.halted {
		return
	}

	d.halted = true
	d.haltErr = err
	d.gapCount++
}

// IsHalted returns true if the detector is halted due to a gap.
func (d *Detector) IsHalted() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.halted
}

// sendAlert sends a gap alert to the configured webhook.
func (d *Detector) sendAlert(gap *GapEvent) {
	payload, err := json.Marshal(map[string]interface{}{
		"type":      "gap_detected",
		"severity":  "critical",
		"gap":       gap,
		"service":   "gap-detector",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		d.logger.Error("failed to marshal alert", "error", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.cfg.AlertWebhook, nil)
	if err != nil {
		d.logger.Error("failed to create alert request", "error", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	// Create body with payload
	req.Body = http.NoBody
	req.ContentLength = int64(len(payload))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		d.logger.Error("failed to send alert", "error", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		d.logger.Error("alert webhook returned error", "status", resp.StatusCode)
	} else {
		d.logger.Info("gap alert sent successfully")
	}
}

// runMetricsServer runs the HTTP metrics server.
func (d *Detector) runMetricsServer() {
	mux := http.NewServeMux()

	// Health endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		d.mu.Lock()
		halted := d.halted
		d.mu.Unlock()

		if halted {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprintf(w, `{"status":"halted","reason":"gap_detected"}`)
			return
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"status":"healthy"}`)
	})

	// Metrics endpoint
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		d.mu.Lock()
		metrics := map[string]interface{}{
			"blocks_scanned": d.blocksScanned,
			"gap_count":      d.gapCount,
			"halted":         d.halted,
			"chains":         d.state.GetStats(),
		}
		d.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(metrics)
	})

	// State endpoint - shows current tracking state
	mux.HandleFunc("/state", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(d.state.GetState())
	})

	d.metricsServer = &http.Server{
		Addr:    d.cfg.MetricsAddr,
		Handler: mux,
	}

	d.logger.Info("starting metrics server", "addr", d.cfg.MetricsAddr)
	if err := d.metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		d.logger.Error("metrics server error", "error", err)
	}
}

// Shutdown gracefully shuts down the detector.
func (d *Detector) Shutdown(ctx context.Context) error {
	d.logger.Info("shutting down gap detector")

	// Shutdown metrics server
	if d.metricsServer != nil {
		if err := d.metricsServer.Shutdown(ctx); err != nil {
			d.logger.Error("metrics server shutdown error", "error", err)
		}
	}

	// Close Kafka client
	if d.client != nil {
		d.client.Close()
	}

	return nil
}
