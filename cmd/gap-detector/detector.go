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

	protov1 "github.com/marko911/project-pulse/pkg/proto/v1"
)

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

type GapEvent struct {
	Chain           protov1.Chain `json:"chain"`
	CommitmentLevel int16         `json:"commitment_level"`
	ExpectedBlock   uint64        `json:"expected_block"`
	ReceivedBlock   uint64        `json:"received_block"`
	GapSize         uint64        `json:"gap_size"`
	DetectedAt      time.Time     `json:"detected_at"`
	Topic           string        `json:"topic"`
}

type Detector struct {
	cfg    DetectorConfig
	logger *slog.Logger

	client *kgo.Client
	state  *ChainState

	mu       sync.Mutex
	halted   bool
	haltErr  error
	gapCount int64

	metricsServer *http.Server
	blocksScanned int64
	lastBlockTime map[chainKey]time.Time
}

type chainKey struct {
	Chain           protov1.Chain
	CommitmentLevel int16
}

func NewDetector(cfg DetectorConfig, logger *slog.Logger) (*Detector, error) {
	state := NewChainState()

	d := &Detector{
		cfg:           cfg,
		logger:        logger,
		state:         state,
		lastBlockTime: make(map[chainKey]time.Time),
	}

	return d, nil
}

func (d *Detector) Run(ctx context.Context) error {
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

	if d.cfg.MetricsAddr != "" {
		go d.runMetricsServer()
	}

	d.logger.Info("gap detector started, monitoring topics", "topics", d.cfg.Topics)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if d.IsHalted() {
			d.logger.Error("detector halted due to gap", "error", d.haltErr)
			return d.haltErr
		}

		fetches := d.client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				d.logger.Error("fetch error", "topic", e.Topic, "partition", e.Partition, "error", e.Err)
			}
			continue
		}

		fetches.EachRecord(func(record *kgo.Record) {
			if err := d.processRecord(ctx, record); err != nil {
				d.logger.Error("process record error", "error", err, "offset", record.Offset)
			}
		})

		if !d.IsHalted() {
			if err := d.client.CommitUncommittedOffsets(ctx); err != nil {
				d.logger.Error("commit error", "error", err)
			}
		}
	}
}

func (d *Detector) processRecord(ctx context.Context, record *kgo.Record) error {
	var event protov1.CanonicalEvent
	if err := json.Unmarshal(record.Value, &event); err != nil {
		d.logger.Warn("failed to parse event", "error", err, "offset", record.Offset)
		return nil
	}

	key := chainKey{
		Chain:           event.Chain,
		CommitmentLevel: int16(event.CommitmentLevel),
	}

	gapEvent := d.state.CheckAndUpdate(key.Chain, key.CommitmentLevel, event.BlockNumber, record.Topic)

	if gapEvent != nil {
		d.handleGap(ctx, gapEvent)
	}

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

func (d *Detector) handleGap(ctx context.Context, gap *GapEvent) {
	d.logger.Error("GAP DETECTED - HALTING",
		"chain", gap.Chain,
		"expected_block", gap.ExpectedBlock,
		"received_block", gap.ReceivedBlock,
		"gap_size", gap.GapSize,
		"commitment_level", gap.CommitmentLevel,
		"topic", gap.Topic,
	)

	if d.cfg.MaxGapBlocks > 0 && int64(gap.GapSize) <= d.cfg.MaxGapBlocks {
		d.logger.Warn("gap within threshold, not halting",
			"gap_size", gap.GapSize,
			"threshold", d.cfg.MaxGapBlocks,
		)
		return
	}

	if d.cfg.AlertWebhook != "" {
		go d.sendAlert(gap)
	}

	d.halt(fmt.Errorf("gap detected: chain=%d expected=%d received=%d gap=%d",
		gap.Chain, gap.ExpectedBlock, gap.ReceivedBlock, gap.GapSize))
}

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

func (d *Detector) IsHalted() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.halted
}

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

func (d *Detector) runMetricsServer() {
	mux := http.NewServeMux()

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

func (d *Detector) Shutdown(ctx context.Context) error {
	d.logger.Info("shutting down gap detector")

	if d.metricsServer != nil {
		if err := d.metricsServer.Shutdown(ctx); err != nil {
			d.logger.Error("metrics server shutdown error", "error", err)
		}
	}

	if d.client != nil {
		d.client.Close()
	}

	return nil
}
