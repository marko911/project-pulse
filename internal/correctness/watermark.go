package correctness

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	protov1 "github.com/mirador/pulse/pkg/proto/v1"
)

// Common errors for watermark operations.
var (
	ErrWatermarkHalted     = errors.New("watermark advancement halted")
	ErrWatermarkRegression = errors.New("watermark regression not allowed")
	ErrNoCorrectnessCheck  = errors.New("block not verified for correctness")
)

// HaltSource identifies what component triggered a halt.
type HaltSource string

const (
	HaltSourceGapDetector   HaltSource = "gap_detector"
	HaltSourceReconciler    HaltSource = "reconciler"
	HaltSourceManualHold    HaltSource = "manual_hold"
	HaltSourceExternalCheck HaltSource = "external_check"
)

// HaltCondition represents a halt state from a specific source.
type HaltCondition struct {
	Source      HaltSource
	Reason      string
	Chain       protov1.Chain
	BlockNumber uint64
	DetectedAt  time.Time
}

// WatermarkState represents the current state of the finalized watermark.
type WatermarkState struct {
	Chain               protov1.Chain
	FinalizedWatermark  uint64    // Highest verified block
	PendingWatermark    uint64    // Highest block awaiting verification
	LastAdvancedAt      time.Time
	Halted              bool
	HaltConditions      []HaltCondition
}

// CorrectnessChecker defines the interface for checking block correctness.
type CorrectnessChecker interface {
	// IsBlockVerified returns true if the block has passed all correctness checks.
	IsBlockVerified(ctx context.Context, chain protov1.Chain, blockNumber uint64) (bool, error)
}

// WatermarkController manages the fail-closed watermark policy.
// The watermark represents the highest block that has been verified for correctness.
// Advancement halts when any correctness failure is detected.
type WatermarkController struct {
	cfg    WatermarkConfig
	logger *slog.Logger

	mu        sync.RWMutex
	states    map[protov1.Chain]*WatermarkState
	halts     map[HaltSource]*HaltCondition
	checker   CorrectnessChecker
	callbacks []WatermarkCallback
}

// WatermarkConfig configures the watermark controller.
type WatermarkConfig struct {
	// FailClosed halts watermark advancement on any correctness failure.
	// Default: true
	FailClosed bool

	// RequireVerification requires explicit correctness verification before advancing.
	// If false, watermark advances optimistically.
	// Default: true (Milestone 3 requirement)
	RequireVerification bool

	// MaxPendingBlocks limits how far ahead pending can be from finalized.
	// This prevents runaway processing when verification is slow.
	// Default: 1000
	MaxPendingBlocks uint64
}

// DefaultWatermarkConfig returns sensible defaults for production.
func DefaultWatermarkConfig() WatermarkConfig {
	return WatermarkConfig{
		FailClosed:          true,
		RequireVerification: true,
		MaxPendingBlocks:    1000,
	}
}

// WatermarkCallback is called when watermark state changes.
type WatermarkCallback func(ctx context.Context, chain protov1.Chain, state *WatermarkState) error

// NewWatermarkController creates a new watermark controller.
func NewWatermarkController(cfg WatermarkConfig, logger *slog.Logger) *WatermarkController {
	if logger == nil {
		logger = slog.Default()
	}

	return &WatermarkController{
		cfg:    cfg,
		logger: logger.With("component", "watermark-controller"),
		states: make(map[protov1.Chain]*WatermarkState),
		halts:  make(map[HaltSource]*HaltCondition),
	}
}

// SetCorrectnessChecker sets the checker for block verification.
func (w *WatermarkController) SetCorrectnessChecker(checker CorrectnessChecker) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.checker = checker
}

// OnWatermarkChange registers a callback for watermark changes.
func (w *WatermarkController) OnWatermarkChange(cb WatermarkCallback) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.callbacks = append(w.callbacks, cb)
}

// InitializeChain sets the initial watermark for a chain.
func (w *WatermarkController) InitializeChain(chain protov1.Chain, startBlock uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.states[chain] = &WatermarkState{
		Chain:               chain,
		FinalizedWatermark:  startBlock,
		PendingWatermark:    startBlock,
		LastAdvancedAt:      time.Now(),
		Halted:              false,
		HaltConditions:      make([]HaltCondition, 0),
	}

	w.logger.Info("initialized watermark",
		"chain", chain,
		"start_block", startBlock,
	)
}

// GetWatermark returns the current finalized watermark for a chain.
func (w *WatermarkController) GetWatermark(chain protov1.Chain) uint64 {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if state := w.states[chain]; state != nil {
		return state.FinalizedWatermark
	}
	return 0
}

// GetState returns the full watermark state for a chain.
func (w *WatermarkController) GetState(chain protov1.Chain) *WatermarkState {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if state := w.states[chain]; state != nil {
		// Return a copy to prevent race conditions
		copy := *state
		copy.HaltConditions = make([]HaltCondition, len(state.HaltConditions))
		for i, h := range state.HaltConditions {
			copy.HaltConditions[i] = h
		}
		return &copy
	}
	return nil
}

// AdvanceWatermark attempts to advance the finalized watermark.
// Returns an error if advancement is halted or verification fails.
func (w *WatermarkController) AdvanceWatermark(ctx context.Context, chain protov1.Chain, toBlock uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	state := w.states[chain]
	if state == nil {
		return fmt.Errorf("chain %v not initialized", chain)
	}

	// Check for global halts
	if w.cfg.FailClosed && len(w.halts) > 0 {
		return fmt.Errorf("%w: %d halt conditions active", ErrWatermarkHalted, len(w.halts))
	}

	// Check for chain-specific halts
	if state.Halted {
		return fmt.Errorf("%w: chain %v has %d halt conditions",
			ErrWatermarkHalted, chain, len(state.HaltConditions))
	}

	// Prevent regression
	if toBlock < state.FinalizedWatermark {
		return fmt.Errorf("%w: current=%d requested=%d",
			ErrWatermarkRegression, state.FinalizedWatermark, toBlock)
	}

	// Already at or past this block
	if toBlock <= state.FinalizedWatermark {
		return nil
	}

	// Check pending limit
	if toBlock > state.FinalizedWatermark+w.cfg.MaxPendingBlocks {
		return fmt.Errorf("would exceed max pending blocks: %d > %d",
			toBlock-state.FinalizedWatermark, w.cfg.MaxPendingBlocks)
	}

	// Verify correctness if required
	if w.cfg.RequireVerification && w.checker != nil {
		verified, err := w.checker.IsBlockVerified(ctx, chain, toBlock)
		if err != nil {
			return fmt.Errorf("correctness check failed: %w", err)
		}
		if !verified {
			// Update pending but not finalized
			if toBlock > state.PendingWatermark {
				state.PendingWatermark = toBlock
			}
			return fmt.Errorf("%w: block %d", ErrNoCorrectnessCheck, toBlock)
		}
	}

	// Advance the watermark
	oldWatermark := state.FinalizedWatermark
	state.FinalizedWatermark = toBlock
	state.PendingWatermark = toBlock
	state.LastAdvancedAt = time.Now()

	w.logger.Info("watermark advanced",
		"chain", chain,
		"from", oldWatermark,
		"to", toBlock,
	)

	// Notify callbacks (copy to release lock)
	callbacks := make([]WatermarkCallback, len(w.callbacks))
	copy(callbacks, w.callbacks)
	stateCopy := *state

	go func() {
		for _, cb := range callbacks {
			if err := cb(ctx, chain, &stateCopy); err != nil {
				w.logger.Error("watermark callback failed", "error", err)
			}
		}
	}()

	return nil
}

// Halt registers a halt condition from a correctness component.
func (w *WatermarkController) Halt(source HaltSource, chain protov1.Chain, blockNumber uint64, reason string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	condition := HaltCondition{
		Source:      source,
		Reason:      reason,
		Chain:       chain,
		BlockNumber: blockNumber,
		DetectedAt:  time.Now(),
	}

	// Register global halt
	w.halts[source] = &condition

	// Register chain-specific halt
	if state := w.states[chain]; state != nil {
		state.Halted = true
		state.HaltConditions = append(state.HaltConditions, condition)
	}

	w.logger.Error("WATERMARK HALTED",
		"source", source,
		"chain", chain,
		"block", blockNumber,
		"reason", reason,
	)
}

// ResolveHalt clears a halt condition from a specific source.
func (w *WatermarkController) ResolveHalt(source HaltSource, resolution string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	halt, exists := w.halts[source]
	if !exists {
		return fmt.Errorf("no halt condition from source: %s", source)
	}

	chain := halt.Chain

	// Remove global halt
	delete(w.halts, source)

	// Update chain state
	if state := w.states[chain]; state != nil {
		// Remove this halt condition
		remaining := make([]HaltCondition, 0)
		for _, h := range state.HaltConditions {
			if h.Source != source {
				remaining = append(remaining, h)
			}
		}
		state.HaltConditions = remaining

		// Clear halted flag if no conditions remain
		if len(remaining) == 0 {
			state.Halted = false
		}
	}

	w.logger.Info("halt resolved",
		"source", source,
		"chain", chain,
		"resolution", resolution,
		"remaining_halts", len(w.halts),
	)

	return nil
}

// IsHalted returns whether any halt conditions are active.
func (w *WatermarkController) IsHalted() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return len(w.halts) > 0
}

// GetHaltConditions returns all active halt conditions.
func (w *WatermarkController) GetHaltConditions() []HaltCondition {
	w.mu.RLock()
	defer w.mu.RUnlock()

	conditions := make([]HaltCondition, 0, len(w.halts))
	for _, h := range w.halts {
		conditions = append(conditions, *h)
	}
	return conditions
}

// Stats returns current watermark statistics.
func (w *WatermarkController) Stats() map[string]interface{} {
	w.mu.RLock()
	defer w.mu.RUnlock()

	chainStats := make(map[string]interface{})
	for chain, state := range w.states {
		chainStats[chainNameFromProto(chain)] = map[string]interface{}{
			"finalized_watermark": state.FinalizedWatermark,
			"pending_watermark":   state.PendingWatermark,
			"last_advanced_at":    state.LastAdvancedAt,
			"halted":              state.Halted,
			"halt_count":          len(state.HaltConditions),
		}
	}

	haltInfo := make([]map[string]interface{}, 0, len(w.halts))
	for source, h := range w.halts {
		haltInfo = append(haltInfo, map[string]interface{}{
			"source":      string(source),
			"chain":       chainNameFromProto(h.Chain),
			"block":       h.BlockNumber,
			"reason":      h.Reason,
			"detected_at": h.DetectedAt,
		})
	}

	return map[string]interface{}{
		"chains":     chainStats,
		"halted":     len(w.halts) > 0,
		"halt_count": len(w.halts),
		"halts":      haltInfo,
		"config": map[string]interface{}{
			"fail_closed":          w.cfg.FailClosed,
			"require_verification": w.cfg.RequireVerification,
			"max_pending_blocks":   w.cfg.MaxPendingBlocks,
		},
	}
}

// IntegrateGapDetector wires up a gap detector to trigger halts.
func (w *WatermarkController) IntegrateGapDetector(detector *GapDetector) {
	detector.OnGap(func(ctx context.Context, event *GapEvent) error {
		chain := protoChainFromName(event.Chain)
		w.Halt(HaltSourceGapDetector, chain, event.ExpectedBlock,
			fmt.Sprintf("gap detected: missing blocks %d-%d",
				event.ExpectedBlock, event.ReceivedBlock-1))
		return nil
	})

	w.logger.Info("integrated gap detector with watermark controller")
}

// protoChainFromName converts a chain name string to protobuf Chain enum.
func protoChainFromName(name string) protov1.Chain {
	switch name {
	case "ethereum":
		return protov1.Chain_CHAIN_ETHEREUM
	case "solana":
		return protov1.Chain_CHAIN_SOLANA
	case "polygon":
		return protov1.Chain_CHAIN_POLYGON
	case "arbitrum":
		return protov1.Chain_CHAIN_ARBITRUM
	case "optimism":
		return protov1.Chain_CHAIN_OPTIMISM
	case "base":
		return protov1.Chain_CHAIN_BASE
	case "avalanche":
		return protov1.Chain_CHAIN_AVALANCHE
	case "bsc":
		return protov1.Chain_CHAIN_BSC
	default:
		return protov1.Chain_CHAIN_UNSPECIFIED
	}
}
