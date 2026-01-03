package correctness

import (
	"context"
	"errors"
	"testing"
	"time"

	protov1 "github.com/mirador/pulse/pkg/proto/v1"
)

// mockCorrectnessChecker implements CorrectnessChecker for testing.
type mockCorrectnessChecker struct {
	verified map[uint64]bool
	err      error
}

func newMockChecker() *mockCorrectnessChecker {
	return &mockCorrectnessChecker{
		verified: make(map[uint64]bool),
	}
}

func (m *mockCorrectnessChecker) IsBlockVerified(ctx context.Context, chain protov1.Chain, blockNumber uint64) (bool, error) {
	if m.err != nil {
		return false, m.err
	}
	return m.verified[blockNumber], nil
}

func (m *mockCorrectnessChecker) SetVerified(block uint64, verified bool) {
	m.verified[block] = verified
}

func TestDefaultWatermarkConfig(t *testing.T) {
	cfg := DefaultWatermarkConfig()

	if !cfg.FailClosed {
		t.Error("expected FailClosed to be true")
	}
	if !cfg.RequireVerification {
		t.Error("expected RequireVerification to be true")
	}
	if cfg.MaxPendingBlocks != 1000 {
		t.Errorf("expected MaxPendingBlocks 1000, got %d", cfg.MaxPendingBlocks)
	}
}

func TestWatermarkController_Initialize(t *testing.T) {
	cfg := DefaultWatermarkConfig()
	w := NewWatermarkController(cfg, nil)

	chain := protov1.Chain_CHAIN_ETHEREUM
	w.InitializeChain(chain, 100)

	watermark := w.GetWatermark(chain)
	if watermark != 100 {
		t.Errorf("expected watermark 100, got %d", watermark)
	}

	state := w.GetState(chain)
	if state == nil {
		t.Fatal("expected non-nil state")
	}
	if state.FinalizedWatermark != 100 {
		t.Errorf("expected FinalizedWatermark 100, got %d", state.FinalizedWatermark)
	}
	if state.PendingWatermark != 100 {
		t.Errorf("expected PendingWatermark 100, got %d", state.PendingWatermark)
	}
	if state.Halted {
		t.Error("expected not halted initially")
	}
}

func TestWatermarkController_GetWatermark_Uninitialized(t *testing.T) {
	cfg := DefaultWatermarkConfig()
	w := NewWatermarkController(cfg, nil)

	watermark := w.GetWatermark(protov1.Chain_CHAIN_ETHEREUM)
	if watermark != 0 {
		t.Errorf("expected 0 for uninitialized chain, got %d", watermark)
	}
}

func TestWatermarkController_Advance_NoVerification(t *testing.T) {
	cfg := DefaultWatermarkConfig()
	cfg.RequireVerification = false // Disable for this test
	w := NewWatermarkController(cfg, nil)

	chain := protov1.Chain_CHAIN_ETHEREUM
	w.InitializeChain(chain, 100)

	ctx := context.Background()
	err := w.AdvanceWatermark(ctx, chain, 150)
	if err != nil {
		t.Fatalf("advance failed: %v", err)
	}

	watermark := w.GetWatermark(chain)
	if watermark != 150 {
		t.Errorf("expected watermark 150, got %d", watermark)
	}
}

func TestWatermarkController_Advance_WithVerification(t *testing.T) {
	cfg := DefaultWatermarkConfig()
	w := NewWatermarkController(cfg, nil)

	checker := newMockChecker()
	checker.SetVerified(150, true)
	w.SetCorrectnessChecker(checker)

	chain := protov1.Chain_CHAIN_ETHEREUM
	w.InitializeChain(chain, 100)

	ctx := context.Background()
	err := w.AdvanceWatermark(ctx, chain, 150)
	if err != nil {
		t.Fatalf("advance failed: %v", err)
	}

	watermark := w.GetWatermark(chain)
	if watermark != 150 {
		t.Errorf("expected watermark 150, got %d", watermark)
	}
}

func TestWatermarkController_Advance_UnverifiedBlock(t *testing.T) {
	cfg := DefaultWatermarkConfig()
	w := NewWatermarkController(cfg, nil)

	checker := newMockChecker()
	// Block 150 is NOT verified
	w.SetCorrectnessChecker(checker)

	chain := protov1.Chain_CHAIN_ETHEREUM
	w.InitializeChain(chain, 100)

	ctx := context.Background()
	err := w.AdvanceWatermark(ctx, chain, 150)
	if err == nil {
		t.Fatal("expected error for unverified block")
	}
	if !errors.Is(err, ErrNoCorrectnessCheck) {
		t.Errorf("expected ErrNoCorrectnessCheck, got %v", err)
	}

	// Watermark should not have advanced
	watermark := w.GetWatermark(chain)
	if watermark != 100 {
		t.Errorf("expected watermark still 100, got %d", watermark)
	}

	// But pending should have updated
	state := w.GetState(chain)
	if state.PendingWatermark != 150 {
		t.Errorf("expected pending 150, got %d", state.PendingWatermark)
	}
}

func TestWatermarkController_Advance_Regression(t *testing.T) {
	cfg := DefaultWatermarkConfig()
	cfg.RequireVerification = false
	w := NewWatermarkController(cfg, nil)

	chain := protov1.Chain_CHAIN_ETHEREUM
	w.InitializeChain(chain, 100)

	ctx := context.Background()
	err := w.AdvanceWatermark(ctx, chain, 50)
	if err == nil {
		t.Fatal("expected error for regression")
	}
	if !errors.Is(err, ErrWatermarkRegression) {
		t.Errorf("expected ErrWatermarkRegression, got %v", err)
	}
}

func TestWatermarkController_Halt(t *testing.T) {
	cfg := DefaultWatermarkConfig()
	w := NewWatermarkController(cfg, nil)

	chain := protov1.Chain_CHAIN_ETHEREUM
	w.InitializeChain(chain, 100)

	// Trigger halt
	w.Halt(HaltSourceGapDetector, chain, 101, "missing block 101")

	if !w.IsHalted() {
		t.Error("expected controller to be halted")
	}

	conditions := w.GetHaltConditions()
	if len(conditions) != 1 {
		t.Fatalf("expected 1 halt condition, got %d", len(conditions))
	}
	if conditions[0].Source != HaltSourceGapDetector {
		t.Errorf("expected source gap_detector, got %s", conditions[0].Source)
	}

	// State should also be halted
	state := w.GetState(chain)
	if !state.Halted {
		t.Error("expected chain state to be halted")
	}
}

func TestWatermarkController_Halt_BlocksAdvance(t *testing.T) {
	cfg := DefaultWatermarkConfig()
	cfg.RequireVerification = false
	w := NewWatermarkController(cfg, nil)

	chain := protov1.Chain_CHAIN_ETHEREUM
	w.InitializeChain(chain, 100)

	// Trigger halt
	w.Halt(HaltSourceReconciler, chain, 101, "mismatch at block 101")

	// Try to advance
	ctx := context.Background()
	err := w.AdvanceWatermark(ctx, chain, 150)
	if err == nil {
		t.Fatal("expected error when halted")
	}
	if !errors.Is(err, ErrWatermarkHalted) {
		t.Errorf("expected ErrWatermarkHalted, got %v", err)
	}

	// Watermark should not have changed
	watermark := w.GetWatermark(chain)
	if watermark != 100 {
		t.Errorf("expected watermark still 100, got %d", watermark)
	}
}

func TestWatermarkController_ResolveHalt(t *testing.T) {
	cfg := DefaultWatermarkConfig()
	cfg.RequireVerification = false
	w := NewWatermarkController(cfg, nil)

	chain := protov1.Chain_CHAIN_ETHEREUM
	w.InitializeChain(chain, 100)

	// Trigger halt
	w.Halt(HaltSourceGapDetector, chain, 101, "gap detected")

	if !w.IsHalted() {
		t.Error("expected halted")
	}

	// Resolve
	err := w.ResolveHalt(HaltSourceGapDetector, "backfill completed")
	if err != nil {
		t.Fatalf("resolve failed: %v", err)
	}

	if w.IsHalted() {
		t.Error("expected not halted after resolution")
	}

	// Should be able to advance now
	ctx := context.Background()
	err = w.AdvanceWatermark(ctx, chain, 150)
	if err != nil {
		t.Fatalf("advance failed after resolution: %v", err)
	}
}

func TestWatermarkController_ResolveHalt_NotHalted(t *testing.T) {
	cfg := DefaultWatermarkConfig()
	w := NewWatermarkController(cfg, nil)

	err := w.ResolveHalt(HaltSourceGapDetector, "test")
	if err == nil {
		t.Error("expected error for non-existent halt")
	}
}

func TestWatermarkController_MultipleHalts(t *testing.T) {
	cfg := DefaultWatermarkConfig()
	w := NewWatermarkController(cfg, nil)

	chain := protov1.Chain_CHAIN_ETHEREUM
	w.InitializeChain(chain, 100)

	// Multiple halts
	w.Halt(HaltSourceGapDetector, chain, 101, "gap")
	w.Halt(HaltSourceReconciler, chain, 102, "mismatch")

	conditions := w.GetHaltConditions()
	if len(conditions) != 2 {
		t.Fatalf("expected 2 halt conditions, got %d", len(conditions))
	}

	// Resolve one
	w.ResolveHalt(HaltSourceGapDetector, "fixed")

	// Still halted
	if !w.IsHalted() {
		t.Error("expected still halted with one remaining condition")
	}

	conditions = w.GetHaltConditions()
	if len(conditions) != 1 {
		t.Fatalf("expected 1 remaining condition, got %d", len(conditions))
	}

	// Resolve remaining
	w.ResolveHalt(HaltSourceReconciler, "fixed")

	if w.IsHalted() {
		t.Error("expected not halted after all resolutions")
	}
}

func TestWatermarkController_MaxPendingBlocks(t *testing.T) {
	cfg := DefaultWatermarkConfig()
	cfg.MaxPendingBlocks = 10
	cfg.RequireVerification = false
	w := NewWatermarkController(cfg, nil)

	chain := protov1.Chain_CHAIN_ETHEREUM
	w.InitializeChain(chain, 100)

	ctx := context.Background()

	// Within limit
	err := w.AdvanceWatermark(ctx, chain, 105)
	if err != nil {
		t.Fatalf("expected success within limit: %v", err)
	}

	// Exceeds limit (100 + 10 = 110, trying 120)
	err = w.AdvanceWatermark(ctx, chain, 120)
	if err == nil {
		t.Error("expected error for exceeding max pending")
	}
}

func TestWatermarkController_Stats(t *testing.T) {
	cfg := DefaultWatermarkConfig()
	w := NewWatermarkController(cfg, nil)

	chain := protov1.Chain_CHAIN_ETHEREUM
	w.InitializeChain(chain, 100)

	w.Halt(HaltSourceGapDetector, chain, 101, "test gap")

	stats := w.Stats()

	if !stats["halted"].(bool) {
		t.Error("expected halted in stats")
	}
	if stats["halt_count"].(int) != 1 {
		t.Errorf("expected halt_count 1, got %d", stats["halt_count"])
	}

	chainStats := stats["chains"].(map[string]interface{})
	if len(chainStats) == 0 {
		t.Error("expected chain stats")
	}
}

func TestWatermarkController_Callback(t *testing.T) {
	cfg := DefaultWatermarkConfig()
	cfg.RequireVerification = false
	w := NewWatermarkController(cfg, nil)

	chain := protov1.Chain_CHAIN_ETHEREUM
	w.InitializeChain(chain, 100)

	callbackCalled := make(chan bool, 1)
	w.OnWatermarkChange(func(ctx context.Context, c protov1.Chain, state *WatermarkState) error {
		if c == chain && state.FinalizedWatermark == 150 {
			callbackCalled <- true
		}
		return nil
	})

	ctx := context.Background()
	w.AdvanceWatermark(ctx, chain, 150)

	select {
	case <-callbackCalled:
		// Success
	case <-time.After(1 * time.Second):
		t.Error("callback not called within timeout")
	}
}

func TestWatermarkController_IntegrateGapDetector(t *testing.T) {
	cfg := DefaultWatermarkConfig()
	w := NewWatermarkController(cfg, nil)

	chain := protov1.Chain_CHAIN_ETHEREUM
	w.InitializeChain(chain, 100)

	// Create gap detector
	gdCfg := DefaultGapDetectorConfig()
	gd := NewGapDetector(gdCfg, nil)

	// Integrate
	w.IntegrateGapDetector(gd)

	// Simulate gap detection
	ctx := context.Background()
	gd.processBlock(ctx, "ethereum", protov1.CommitmentLevel_COMMITMENT_LEVEL_FINALIZED, 100, "hash100")
	gd.processBlock(ctx, "ethereum", protov1.CommitmentLevel_COMMITMENT_LEVEL_FINALIZED, 105, "hash105") // Gap!

	// Watermark controller should be halted
	if !w.IsHalted() {
		t.Error("expected watermark controller to be halted after gap detection")
	}

	conditions := w.GetHaltConditions()
	if len(conditions) == 0 {
		t.Fatal("expected halt conditions")
	}
	if conditions[0].Source != HaltSourceGapDetector {
		t.Errorf("expected gap_detector source, got %s", conditions[0].Source)
	}
}

func TestHaltSource_Values(t *testing.T) {
	sources := []HaltSource{
		HaltSourceGapDetector,
		HaltSourceReconciler,
		HaltSourceManualHold,
		HaltSourceExternalCheck,
	}

	for _, s := range sources {
		if s == "" {
			t.Error("expected non-empty halt source")
		}
	}
}

func TestProtoChainFromName(t *testing.T) {
	tests := []struct {
		name     string
		expected protov1.Chain
	}{
		{"ethereum", protov1.Chain_CHAIN_ETHEREUM},
		{"solana", protov1.Chain_CHAIN_SOLANA},
		{"polygon", protov1.Chain_CHAIN_POLYGON},
		{"arbitrum", protov1.Chain_CHAIN_ARBITRUM},
		{"optimism", protov1.Chain_CHAIN_OPTIMISM},
		{"base", protov1.Chain_CHAIN_BASE},
		{"avalanche", protov1.Chain_CHAIN_AVALANCHE},
		{"bsc", protov1.Chain_CHAIN_BSC},
		{"unknown", protov1.Chain_CHAIN_UNSPECIFIED},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := protoChainFromName(tt.name)
			if result != tt.expected {
				t.Errorf("protoChainFromName(%s) = %v, want %v", tt.name, result, tt.expected)
			}
		})
	}
}
