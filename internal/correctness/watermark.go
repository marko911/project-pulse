package correctness

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	protov1 "github.com/marko911/project-pulse/pkg/proto/v1"
)

var (
	ErrWatermarkHalted     = errors.New("watermark advancement halted")
	ErrWatermarkRegression = errors.New("watermark regression not allowed")
	ErrNoCorrectnessCheck  = errors.New("block not verified for correctness")
)

type HaltSource string

const (
	HaltSourceGapDetector   HaltSource = "gap_detector"
	HaltSourceReconciler    HaltSource = "reconciler"
	HaltSourceManualHold    HaltSource = "manual_hold"
	HaltSourceExternalCheck HaltSource = "external_check"
)

type HaltCondition struct {
	Source      HaltSource
	Reason      string
	Chain       protov1.Chain
	BlockNumber uint64
	DetectedAt  time.Time
}

type WatermarkState struct {
	Chain               protov1.Chain
	FinalizedWatermark  uint64
	PendingWatermark    uint64
	LastAdvancedAt      time.Time
	Halted              bool
	HaltConditions      []HaltCondition
}

type CorrectnessChecker interface {
	IsBlockVerified(ctx context.Context, chain protov1.Chain, blockNumber uint64) (bool, error)
}

type WatermarkController struct {
	cfg    WatermarkConfig
	logger *slog.Logger

	mu        sync.RWMutex
	states    map[protov1.Chain]*WatermarkState
	halts     map[HaltSource]*HaltCondition
	checker   CorrectnessChecker
	callbacks []WatermarkCallback
}

type WatermarkConfig struct {
	FailClosed bool

	RequireVerification bool

	MaxPendingBlocks uint64
}

func DefaultWatermarkConfig() WatermarkConfig {
	return WatermarkConfig{
		FailClosed:          true,
		RequireVerification: true,
		MaxPendingBlocks:    1000,
	}
}

type WatermarkCallback func(ctx context.Context, chain protov1.Chain, state *WatermarkState) error

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

func (w *WatermarkController) SetCorrectnessChecker(checker CorrectnessChecker) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.checker = checker
}

func (w *WatermarkController) OnWatermarkChange(cb WatermarkCallback) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.callbacks = append(w.callbacks, cb)
}

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

func (w *WatermarkController) GetWatermark(chain protov1.Chain) uint64 {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if state := w.states[chain]; state != nil {
		return state.FinalizedWatermark
	}
	return 0
}

func (w *WatermarkController) GetState(chain protov1.Chain) *WatermarkState {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if state := w.states[chain]; state != nil {
		copy := *state
		copy.HaltConditions = make([]HaltCondition, len(state.HaltConditions))
		for i, h := range state.HaltConditions {
			copy.HaltConditions[i] = h
		}
		return &copy
	}
	return nil
}

func (w *WatermarkController) AdvanceWatermark(ctx context.Context, chain protov1.Chain, toBlock uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	state := w.states[chain]
	if state == nil {
		return fmt.Errorf("chain %v not initialized", chain)
	}

	if w.cfg.FailClosed && len(w.halts) > 0 {
		return fmt.Errorf("%w: %d halt conditions active", ErrWatermarkHalted, len(w.halts))
	}

	if state.Halted {
		return fmt.Errorf("%w: chain %v has %d halt conditions",
			ErrWatermarkHalted, chain, len(state.HaltConditions))
	}

	if toBlock < state.FinalizedWatermark {
		return fmt.Errorf("%w: current=%d requested=%d",
			ErrWatermarkRegression, state.FinalizedWatermark, toBlock)
	}

	if toBlock <= state.FinalizedWatermark {
		return nil
	}

	if toBlock > state.FinalizedWatermark+w.cfg.MaxPendingBlocks {
		return fmt.Errorf("would exceed max pending blocks: %d > %d",
			toBlock-state.FinalizedWatermark, w.cfg.MaxPendingBlocks)
	}

	if w.cfg.RequireVerification && w.checker != nil {
		verified, err := w.checker.IsBlockVerified(ctx, chain, toBlock)
		if err != nil {
			return fmt.Errorf("correctness check failed: %w", err)
		}
		if !verified {
			if toBlock > state.PendingWatermark {
				state.PendingWatermark = toBlock
			}
			return fmt.Errorf("%w: block %d", ErrNoCorrectnessCheck, toBlock)
		}
	}

	oldWatermark := state.FinalizedWatermark
	state.FinalizedWatermark = toBlock
	state.PendingWatermark = toBlock
	state.LastAdvancedAt = time.Now()

	w.logger.Info("watermark advanced",
		"chain", chain,
		"from", oldWatermark,
		"to", toBlock,
	)

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

	w.halts[source] = &condition

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

func (w *WatermarkController) ResolveHalt(source HaltSource, resolution string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	halt, exists := w.halts[source]
	if !exists {
		return fmt.Errorf("no halt condition from source: %s", source)
	}

	chain := halt.Chain

	delete(w.halts, source)

	if state := w.states[chain]; state != nil {
		remaining := make([]HaltCondition, 0)
		for _, h := range state.HaltConditions {
			if h.Source != source {
				remaining = append(remaining, h)
			}
		}
		state.HaltConditions = remaining

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

func (w *WatermarkController) IsHalted() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return len(w.halts) > 0
}

func (w *WatermarkController) GetHaltConditions() []HaltCondition {
	w.mu.RLock()
	defer w.mu.RUnlock()

	conditions := make([]HaltCondition, 0, len(w.halts))
	for _, h := range w.halts {
		conditions = append(conditions, *h)
	}
	return conditions
}

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
