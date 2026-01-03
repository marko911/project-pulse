package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/mirador/pulse/internal/platform/goldensource"
	"github.com/mirador/pulse/internal/platform/storage"
	protov1 "github.com/mirador/pulse/pkg/proto/v1"
)

// ReconcilerConfig holds configuration for the reconciler.
type ReconcilerConfig struct {
	// ReconcileInterval is how often to check for blocks needing reconciliation
	ReconcileInterval time.Duration

	// BatchSize is the max number of blocks to reconcile per cycle
	BatchSize int

	// LookbackBlocks is how far back to look for unreconciled blocks
	LookbackBlocks uint64

	// FailClosed halts on any reconciliation mismatch
	FailClosed bool

	// MetricsAddr for HTTP metrics endpoint
	MetricsAddr string
}

// DefaultReconcilerConfig returns sensible defaults.
func DefaultReconcilerConfig() ReconcilerConfig {
	return ReconcilerConfig{
		ReconcileInterval: 30 * time.Second,
		BatchSize:         100,
		LookbackBlocks:    1000,
		FailClosed:        true,
		MetricsAddr:       ":9093",
	}
}

// ReconciliationResult represents the outcome of reconciling a single block.
type ReconciliationResult struct {
	Chain              protov1.Chain
	BlockNumber        uint64
	BlockHash          string
	Matched            bool
	Errors             []string
	LocalManifest      *storage.ManifestRecord
	GoldenData         *goldensource.BlockData
	ReconciledAt       time.Time
}

// Reconciler compares local manifests with golden source data.
type Reconciler struct {
	cfg      ReconcilerConfig
	logger   *slog.Logger
	db       *storage.DB
	repo     *storage.ManifestRepository
	verifier *goldensource.Verifier

	mu            sync.RWMutex
	halted        bool
	haltReason    string
	lastReconcile time.Time
	stats         *ReconcilerStats

	metricsServer *http.Server
}

// ReconcilerStats tracks reconciliation statistics.
type ReconcilerStats struct {
	BlocksReconciled   int64
	BlocksMatched      int64
	BlocksMismatched   int64
	BlocksSkipped      int64
	GoldenSourceErrors int64
	LastReconciledBlock uint64
	LastReconciledAt   time.Time
}

// NewReconciler creates a new reconciler.
func NewReconciler(cfg ReconcilerConfig, db *storage.DB, verifier *goldensource.Verifier, logger *slog.Logger) *Reconciler {
	return &Reconciler{
		cfg:      cfg,
		logger:   logger,
		db:       db,
		repo:     storage.NewManifestRepository(db),
		verifier: verifier,
		stats:    &ReconcilerStats{},
	}
}

// Run starts the reconciliation loop.
func (r *Reconciler) Run(ctx context.Context) error {
	r.logger.Info("starting reconciler",
		"interval", r.cfg.ReconcileInterval,
		"batch_size", r.cfg.BatchSize,
		"fail_closed", r.cfg.FailClosed,
	)

	// Start metrics server
	if r.cfg.MetricsAddr != "" {
		go r.startMetricsServer()
	}

	ticker := time.NewTicker(r.cfg.ReconcileInterval)
	defer ticker.Stop()

	// Run initial reconciliation
	if err := r.reconcileCycle(ctx); err != nil {
		if r.cfg.FailClosed && r.halted {
			return fmt.Errorf("reconciler halted: %s", r.haltReason)
		}
		r.logger.Error("initial reconcile cycle error", "error", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-ticker.C:
			r.mu.RLock()
			halted := r.halted
			r.mu.RUnlock()

			if halted && r.cfg.FailClosed {
				r.logger.Warn("reconciler halted, skipping cycle", "reason", r.haltReason)
				continue
			}

			if err := r.reconcileCycle(ctx); err != nil {
				if r.cfg.FailClosed && r.halted {
					return fmt.Errorf("reconciler halted: %s", r.haltReason)
				}
				r.logger.Error("reconcile cycle error", "error", err)
			}
		}
	}
}

// reconcileCycle performs a single reconciliation pass.
func (r *Reconciler) reconcileCycle(ctx context.Context) error {
	r.mu.Lock()
	r.lastReconcile = time.Now()
	r.mu.Unlock()

	// Get chains with registered golden source clients
	chains := r.getActiveChains()
	if len(chains) == 0 {
		r.logger.Debug("no golden source clients registered, skipping cycle")
		return nil
	}

	for _, chain := range chains {
		if err := r.reconcileChain(ctx, chain); err != nil {
			return err
		}
	}

	return nil
}

// reconcileChain reconciles manifests for a single chain.
func (r *Reconciler) reconcileChain(ctx context.Context, chain protov1.Chain) error {
	// Get latest manifest block
	latestBlock, err := r.repo.GetLatestBlock(ctx, chain)
	if err != nil {
		return fmt.Errorf("get latest block for chain %v: %w", chain, err)
	}

	if latestBlock == 0 {
		r.logger.Debug("no manifests found for chain", "chain", chain)
		return nil
	}

	// Calculate range to reconcile
	fromBlock := uint64(0)
	if latestBlock > r.cfg.LookbackBlocks {
		fromBlock = latestBlock - r.cfg.LookbackBlocks
	}

	// Get manifests in range
	manifests, err := r.repo.GetBlockRange(ctx, chain, fromBlock, latestBlock)
	if err != nil {
		return fmt.Errorf("get block range: %w", err)
	}

	if len(manifests) == 0 {
		return nil
	}

	// Limit to batch size
	if len(manifests) > r.cfg.BatchSize {
		manifests = manifests[len(manifests)-r.cfg.BatchSize:]
	}

	r.logger.Info("reconciling blocks",
		"chain", chain,
		"from", manifests[0].BlockNumber,
		"to", manifests[len(manifests)-1].BlockNumber,
		"count", len(manifests),
	)

	// Reconcile each manifest
	for _, manifest := range manifests {
		result, err := r.reconcileManifest(ctx, &manifest)
		if err != nil {
			r.mu.Lock()
			r.stats.GoldenSourceErrors++
			r.mu.Unlock()

			r.logger.Error("failed to reconcile manifest",
				"chain", chain,
				"block", manifest.BlockNumber,
				"error", err,
			)
			continue
		}

		r.mu.Lock()
		r.stats.BlocksReconciled++
		r.stats.LastReconciledBlock = uint64(manifest.BlockNumber)
		r.stats.LastReconciledAt = time.Now()

		if result.Matched {
			r.stats.BlocksMatched++
		} else {
			r.stats.BlocksMismatched++

			// Fail-closed behavior
			if r.cfg.FailClosed {
				r.halted = true
				r.haltReason = fmt.Sprintf("mismatch at block %d: %v",
					manifest.BlockNumber, result.Errors)
				r.mu.Unlock()

				r.logger.Error("RECONCILIATION MISMATCH - HALTING",
					"chain", chain,
					"block", manifest.BlockNumber,
					"errors", result.Errors,
				)

				return fmt.Errorf("fail-closed: reconciliation mismatch at block %d",
					manifest.BlockNumber)
			}
		}
		r.mu.Unlock()

		if result.Matched {
			r.logger.Debug("block reconciled successfully",
				"chain", chain,
				"block", manifest.BlockNumber,
			)
		} else {
			r.logger.Warn("block reconciliation mismatch",
				"chain", chain,
				"block", manifest.BlockNumber,
				"errors", result.Errors,
			)
		}
	}

	return nil
}

// reconcileManifest compares a single manifest against golden source.
func (r *Reconciler) reconcileManifest(ctx context.Context, manifest *storage.ManifestRecord) (*ReconciliationResult, error) {
	chain := protov1.Chain(manifest.Chain)

	// Build primary block data from manifest
	primary := &goldensource.BlockData{
		Chain:            chain,
		BlockNumber:      uint64(manifest.BlockNumber),
		BlockHash:        manifest.BlockHash,
		ParentHash:       manifest.ParentHash,
		TransactionCount: uint32(manifest.EmittedTxCount),
		Timestamp:        manifest.BlockTimestamp,
		FetchedAt:        manifest.IngestedAt,
	}

	// Verify against golden source
	verifyResult, err := r.verifier.VerifyBlock(ctx, primary)
	if err != nil {
		return nil, fmt.Errorf("verify block: %w", err)
	}

	// Skip if verification was skipped (sampling or unavailable)
	if verifyResult == nil {
		r.mu.Lock()
		r.stats.BlocksSkipped++
		r.mu.Unlock()
		return &ReconciliationResult{
			Chain:       chain,
			BlockNumber: uint64(manifest.BlockNumber),
			Matched:     true, // Skipped counts as matched
		}, nil
	}

	result := &ReconciliationResult{
		Chain:         chain,
		BlockNumber:   uint64(manifest.BlockNumber),
		BlockHash:     manifest.BlockHash,
		Matched:       verifyResult.Verified,
		LocalManifest: manifest,
		GoldenData:    verifyResult.Golden,
		ReconciledAt:  time.Now(),
	}

	// Collect error messages
	for _, e := range verifyResult.Errors {
		result.Errors = append(result.Errors, e.Error())
	}

	return result, nil
}

// getActiveChains returns chains with registered golden source clients.
func (r *Reconciler) getActiveChains() []protov1.Chain {
	// For now, check which chains we have manifests for
	// In production, this would come from the verifier's registered clients
	chains := []protov1.Chain{
		protov1.Chain_CHAIN_ETHEREUM,
		protov1.Chain_CHAIN_SOLANA,
	}

	// Filter to chains with actual data
	active := make([]protov1.Chain, 0)
	for _, chain := range chains {
		count, err := r.repo.CountByChain(context.Background(), chain)
		if err == nil && count > 0 {
			active = append(active, chain)
		}
	}

	return active
}

// IsHalted returns whether the reconciler is halted.
func (r *Reconciler) IsHalted() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.halted
}

// HaltReason returns the reason for the halt.
func (r *Reconciler) HaltReason() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.haltReason
}

// ResolveHalt clears the halt state after issue resolution.
func (r *Reconciler) ResolveHalt(resolution string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.halted {
		return fmt.Errorf("reconciler is not halted")
	}

	r.logger.Info("reconciler halt resolved",
		"previous_reason", r.haltReason,
		"resolution", resolution,
	)

	r.halted = false
	r.haltReason = ""
	return nil
}

// Stats returns current reconciliation statistics.
func (r *Reconciler) Stats() ReconcilerStats {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return *r.stats
}

// startMetricsServer starts the HTTP metrics endpoint.
func (r *Reconciler) startMetricsServer() {
	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, req *http.Request) {
		r.mu.RLock()
		halted := r.halted
		reason := r.haltReason
		r.mu.RUnlock()

		status := map[string]interface{}{
			"status": "healthy",
			"halted": halted,
		}
		if halted {
			status["status"] = "halted"
			status["reason"] = reason
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			w.WriteHeader(http.StatusOK)
		}
		json.NewEncoder(w).Encode(status)
	})

	mux.HandleFunc("/metrics", func(w http.ResponseWriter, req *http.Request) {
		r.mu.RLock()
		stats := *r.stats
		halted := r.halted
		lastReconcile := r.lastReconcile
		r.mu.RUnlock()

		metrics := map[string]interface{}{
			"blocks_reconciled":     stats.BlocksReconciled,
			"blocks_matched":        stats.BlocksMatched,
			"blocks_mismatched":     stats.BlocksMismatched,
			"blocks_skipped":        stats.BlocksSkipped,
			"golden_source_errors":  stats.GoldenSourceErrors,
			"last_reconciled_block": stats.LastReconciledBlock,
			"last_reconciled_at":    stats.LastReconciledAt,
			"last_cycle_at":         lastReconcile,
			"halted":                halted,
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(metrics)
	})

	mux.HandleFunc("/resolve", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "POST required", http.StatusMethodNotAllowed)
			return
		}

		var body struct {
			Resolution string `json:"resolution"`
		}
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			http.Error(w, "invalid body", http.StatusBadRequest)
			return
		}

		if err := r.ResolveHalt(body.Resolution); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "resolved"})
	})

	r.metricsServer = &http.Server{
		Addr:    r.cfg.MetricsAddr,
		Handler: mux,
	}

	r.logger.Info("starting metrics server", "addr", r.cfg.MetricsAddr)
	if err := r.metricsServer.ListenAndServe(); err != http.ErrServerClosed {
		r.logger.Error("metrics server error", "error", err)
	}
}

// Shutdown gracefully shuts down the reconciler.
func (r *Reconciler) Shutdown(ctx context.Context) error {
	r.logger.Info("shutting down reconciler")

	if r.metricsServer != nil {
		if err := r.metricsServer.Shutdown(ctx); err != nil {
			r.logger.Error("metrics server shutdown error", "error", err)
		}
	}

	return nil
}
