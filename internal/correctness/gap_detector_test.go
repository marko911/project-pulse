package correctness

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	protov1 "github.com/marko911/project-pulse/pkg/proto/v1"
)

func TestGapDetector_SequentialBlocks(t *testing.T) {
	detector := NewGapDetector(GapDetectorConfig{
		FailClosed: false,
	}, nil)

	ctx := context.Background()

	// Process sequential blocks
	for i := uint64(100); i <= 105; i++ {
		event := &protov1.CanonicalEvent{
			Chain:           protov1.Chain_CHAIN_ETHEREUM,
			CommitmentLevel: protov1.CommitmentLevel_COMMITMENT_LEVEL_FINALIZED,
			BlockNumber:     i,
			BlockHash:       "0xabc123",
		}

		gapEvent, err := detector.ProcessEvent(ctx, event)
		if err != nil {
			t.Fatalf("unexpected error at block %d: %v", i, err)
		}
		if gapEvent != nil {
			t.Fatalf("unexpected gap detected at block %d", i)
		}
	}

	// Verify state
	state := detector.GetChainState("ethereum", protov1.CommitmentLevel_COMMITMENT_LEVEL_FINALIZED)
	if state == nil {
		t.Fatal("expected chain state to exist")
	}
	if state.LastSeenBlock != 105 {
		t.Errorf("expected last seen block 105, got %d", state.LastSeenBlock)
	}
	if state.GapsDetected != 0 {
		t.Errorf("expected 0 gaps, got %d", state.GapsDetected)
	}
}

func TestGapDetector_GapDetection(t *testing.T) {
	detector := NewGapDetector(GapDetectorConfig{
		FailClosed: false, // Don't halt for this test
	}, nil)

	ctx := context.Background()

	// Process block 100
	event1 := &protov1.CanonicalEvent{
		Chain:           protov1.Chain_CHAIN_ETHEREUM,
		CommitmentLevel: protov1.CommitmentLevel_COMMITMENT_LEVEL_FINALIZED,
		BlockNumber:     100,
		BlockHash:       "0xblock100",
	}
	_, _ = detector.ProcessEvent(ctx, event1)

	// Process block 105 (skipping 101-104)
	event2 := &protov1.CanonicalEvent{
		Chain:           protov1.Chain_CHAIN_ETHEREUM,
		CommitmentLevel: protov1.CommitmentLevel_COMMITMENT_LEVEL_FINALIZED,
		BlockNumber:     105,
		BlockHash:       "0xblock105",
	}

	gapEvent, err := detector.ProcessEvent(ctx, event2)
	if err != nil {
		t.Fatalf("unexpected error (fail-closed=false): %v", err)
	}
	if gapEvent == nil {
		t.Fatal("expected gap event to be returned")
	}

	// Verify gap event details
	if gapEvent.ExpectedBlock != 101 {
		t.Errorf("expected block 101, got %d", gapEvent.ExpectedBlock)
	}
	if gapEvent.ReceivedBlock != 105 {
		t.Errorf("received block should be 105, got %d", gapEvent.ReceivedBlock)
	}
	if gapEvent.GapSize != 4 {
		t.Errorf("gap size should be 4, got %d", gapEvent.GapSize)
	}
	if len(gapEvent.MissingBlocks) != 4 {
		t.Errorf("missing blocks count should be 4, got %d", len(gapEvent.MissingBlocks))
	}

	// Verify missing blocks are correct
	expectedMissing := []uint64{101, 102, 103, 104}
	for i, block := range gapEvent.MissingBlocks {
		if block != expectedMissing[i] {
			t.Errorf("missing block %d: expected %d, got %d", i, expectedMissing[i], block)
		}
	}
}

func TestGapDetector_FailClosed(t *testing.T) {
	detector := NewGapDetector(GapDetectorConfig{
		FailClosed: true,
	}, nil)

	ctx := context.Background()

	// Process block 100
	event1 := &protov1.CanonicalEvent{
		Chain:           protov1.Chain_CHAIN_ETHEREUM,
		CommitmentLevel: protov1.CommitmentLevel_COMMITMENT_LEVEL_FINALIZED,
		BlockNumber:     100,
		BlockHash:       "0xblock100",
	}
	_, _ = detector.ProcessEvent(ctx, event1)

	// Process block 105 (gap)
	event2 := &protov1.CanonicalEvent{
		Chain:           protov1.Chain_CHAIN_ETHEREUM,
		CommitmentLevel: protov1.CommitmentLevel_COMMITMENT_LEVEL_FINALIZED,
		BlockNumber:     105,
		BlockHash:       "0xblock105",
	}

	gapEvent, err := detector.ProcessEvent(ctx, event2)
	if err == nil {
		t.Fatal("expected error when gap detected in fail-closed mode")
	}
	if gapEvent == nil {
		t.Fatal("expected gap event even with error")
	}

	// Verify halted state
	if !detector.IsHalted() {
		t.Error("detector should be halted")
	}
	if detector.HaltReason() == "" {
		t.Error("halt reason should be set")
	}

	// Further processing should fail
	event3 := &protov1.CanonicalEvent{
		Chain:           protov1.Chain_CHAIN_ETHEREUM,
		CommitmentLevel: protov1.CommitmentLevel_COMMITMENT_LEVEL_FINALIZED,
		BlockNumber:     106,
		BlockHash:       "0xblock106",
	}
	_, err = detector.ProcessEvent(ctx, event3)
	if err == nil {
		t.Error("expected error when processing while halted")
	}
}

func TestGapDetector_MultiChain(t *testing.T) {
	detector := NewGapDetector(GapDetectorConfig{
		FailClosed: false,
	}, nil)

	ctx := context.Background()

	// Process Ethereum blocks
	for i := uint64(100); i <= 103; i++ {
		event := &protov1.CanonicalEvent{
			Chain:           protov1.Chain_CHAIN_ETHEREUM,
			CommitmentLevel: protov1.CommitmentLevel_COMMITMENT_LEVEL_FINALIZED,
			BlockNumber:     i,
			BlockHash:       "0xeth",
		}
		_, _ = detector.ProcessEvent(ctx, event)
	}

	// Process Solana slots
	for i := uint64(200); i <= 203; i++ {
		event := &protov1.CanonicalEvent{
			Chain:           protov1.Chain_CHAIN_SOLANA,
			CommitmentLevel: protov1.CommitmentLevel_COMMITMENT_LEVEL_FINALIZED,
			BlockNumber:     i,
			BlockHash:       "0xsol",
		}
		_, _ = detector.ProcessEvent(ctx, event)
	}

	// Verify both chains tracked independently
	ethState := detector.GetChainState("ethereum", protov1.CommitmentLevel_COMMITMENT_LEVEL_FINALIZED)
	if ethState == nil || ethState.LastSeenBlock != 103 {
		t.Errorf("ethereum last block should be 103")
	}

	solState := detector.GetChainState("solana", protov1.CommitmentLevel_COMMITMENT_LEVEL_FINALIZED)
	if solState == nil || solState.LastSeenBlock != 203 {
		t.Errorf("solana last block should be 203")
	}
}

func TestGapDetector_SkipsNonFinalized(t *testing.T) {
	detector := NewGapDetector(GapDetectorConfig{
		FailClosed: true,
	}, nil)

	ctx := context.Background()

	// Process confirmed (non-finalized) events - should be skipped
	for i := uint64(100); i <= 105; i++ {
		event := &protov1.CanonicalEvent{
			Chain:           protov1.Chain_CHAIN_ETHEREUM,
			CommitmentLevel: protov1.CommitmentLevel_COMMITMENT_LEVEL_CONFIRMED,
			BlockNumber:     i,
			BlockHash:       "0xconfirmed",
		}

		gapEvent, err := detector.ProcessEvent(ctx, event)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if gapEvent != nil {
			t.Fatal("should not detect gaps for non-finalized events")
		}
	}

	// No state should be tracked for confirmed level
	state := detector.GetChainState("ethereum", protov1.CommitmentLevel_COMMITMENT_LEVEL_CONFIRMED)
	if state != nil {
		t.Error("should not track non-finalized events")
	}
}

func TestGapDetector_SkipsReorgEvents(t *testing.T) {
	detector := NewGapDetector(GapDetectorConfig{
		FailClosed: true,
	}, nil)

	ctx := context.Background()

	// First block
	event1 := &protov1.CanonicalEvent{
		Chain:           protov1.Chain_CHAIN_ETHEREUM,
		CommitmentLevel: protov1.CommitmentLevel_COMMITMENT_LEVEL_FINALIZED,
		BlockNumber:     100,
		ReorgAction:     protov1.ReorgAction_REORG_ACTION_NORMAL,
	}
	_, _ = detector.ProcessEvent(ctx, event1)

	// Retraction event for block 101 (should be skipped)
	retract := &protov1.CanonicalEvent{
		Chain:           protov1.Chain_CHAIN_ETHEREUM,
		CommitmentLevel: protov1.CommitmentLevel_COMMITMENT_LEVEL_FINALIZED,
		BlockNumber:     101,
		ReorgAction:     protov1.ReorgAction_REORG_ACTION_RETRACT,
	}
	gapEvent, err := detector.ProcessEvent(ctx, retract)
	if err != nil {
		t.Fatalf("reorg events should not cause errors: %v", err)
	}
	if gapEvent != nil {
		t.Error("reorg events should not cause gap detection")
	}

	// Block 102 should still be fine (101 was skipped due to retraction)
	event2 := &protov1.CanonicalEvent{
		Chain:           protov1.Chain_CHAIN_ETHEREUM,
		CommitmentLevel: protov1.CommitmentLevel_COMMITMENT_LEVEL_FINALIZED,
		BlockNumber:     101,
		ReorgAction:     protov1.ReorgAction_REORG_ACTION_NORMAL,
	}
	_, _ = detector.ProcessEvent(ctx, event2)
}

func TestGapDetector_Callback(t *testing.T) {
	detector := NewGapDetector(GapDetectorConfig{
		FailClosed: false,
	}, nil)

	var callbackCalled atomic.Int32
	var receivedGap *GapEvent

	detector.OnGap(func(ctx context.Context, event *GapEvent) error {
		callbackCalled.Add(1)
		receivedGap = event
		return nil
	})

	ctx := context.Background()

	// Block 100
	event1 := &protov1.CanonicalEvent{
		Chain:           protov1.Chain_CHAIN_ETHEREUM,
		CommitmentLevel: protov1.CommitmentLevel_COMMITMENT_LEVEL_FINALIZED,
		BlockNumber:     100,
	}
	_, _ = detector.ProcessEvent(ctx, event1)

	// Block 103 (gap of 2)
	event2 := &protov1.CanonicalEvent{
		Chain:           protov1.Chain_CHAIN_ETHEREUM,
		CommitmentLevel: protov1.CommitmentLevel_COMMITMENT_LEVEL_FINALIZED,
		BlockNumber:     103,
	}
	_, _ = detector.ProcessEvent(ctx, event2)

	if callbackCalled.Load() != 1 {
		t.Errorf("callback should be called once, got %d", callbackCalled.Load())
	}
	if receivedGap == nil {
		t.Fatal("callback should receive gap event")
	}
	if receivedGap.GapSize != 2 {
		t.Errorf("gap size should be 2, got %d", receivedGap.GapSize)
	}
}

func TestGapDetector_ResolveGap(t *testing.T) {
	detector := NewGapDetector(GapDetectorConfig{
		FailClosed: true,
	}, nil)

	ctx := context.Background()

	// Block 100
	event1 := &protov1.CanonicalEvent{
		Chain:           protov1.Chain_CHAIN_ETHEREUM,
		CommitmentLevel: protov1.CommitmentLevel_COMMITMENT_LEVEL_FINALIZED,
		BlockNumber:     100,
	}
	_, _ = detector.ProcessEvent(ctx, event1)

	// Block 105 (gap) - causes halt
	event2 := &protov1.CanonicalEvent{
		Chain:           protov1.Chain_CHAIN_ETHEREUM,
		CommitmentLevel: protov1.CommitmentLevel_COMMITMENT_LEVEL_FINALIZED,
		BlockNumber:     105,
	}
	_, _ = detector.ProcessEvent(ctx, event2)

	if !detector.IsHalted() {
		t.Fatal("detector should be halted")
	}

	// Resolve the gap
	err := detector.ResolveGap("ethereum", protov1.CommitmentLevel_COMMITMENT_LEVEL_FINALIZED, 105)
	if err != nil {
		t.Fatalf("resolve gap error: %v", err)
	}

	if detector.IsHalted() {
		t.Error("detector should resume after gap resolution")
	}

	// Should be able to process new blocks now
	event3 := &protov1.CanonicalEvent{
		Chain:           protov1.Chain_CHAIN_ETHEREUM,
		CommitmentLevel: protov1.CommitmentLevel_COMMITMENT_LEVEL_FINALIZED,
		BlockNumber:     106,
	}
	_, err = detector.ProcessEvent(ctx, event3)
	if err != nil {
		t.Errorf("should be able to process after resolution: %v", err)
	}
}

func TestGapDetector_DuplicateBlocks(t *testing.T) {
	detector := NewGapDetector(GapDetectorConfig{
		FailClosed: true,
	}, nil)

	ctx := context.Background()

	// Process blocks 100, 101, 102
	for i := uint64(100); i <= 102; i++ {
		event := &protov1.CanonicalEvent{
			Chain:           protov1.Chain_CHAIN_ETHEREUM,
			CommitmentLevel: protov1.CommitmentLevel_COMMITMENT_LEVEL_FINALIZED,
			BlockNumber:     i,
		}
		_, _ = detector.ProcessEvent(ctx, event)
	}

	// Re-send block 101 (duplicate)
	duplicate := &protov1.CanonicalEvent{
		Chain:           protov1.Chain_CHAIN_ETHEREUM,
		CommitmentLevel: protov1.CommitmentLevel_COMMITMENT_LEVEL_FINALIZED,
		BlockNumber:     101,
	}
	gapEvent, err := detector.ProcessEvent(ctx, duplicate)
	if err != nil {
		t.Errorf("duplicate blocks should not cause errors: %v", err)
	}
	if gapEvent != nil {
		t.Error("duplicate blocks should not cause gap detection")
	}

	// State should still show block 102 as last seen
	state := detector.GetChainState("ethereum", protov1.CommitmentLevel_COMMITMENT_LEVEL_FINALIZED)
	if state.LastSeenBlock != 102 {
		t.Errorf("last seen should still be 102, got %d", state.LastSeenBlock)
	}
}

func TestGapDetector_Stats(t *testing.T) {
	detector := NewGapDetector(GapDetectorConfig{
		FailClosed: false,
	}, nil)

	ctx := context.Background()

	// Process some blocks
	for i := uint64(100); i <= 105; i++ {
		event := &protov1.CanonicalEvent{
			Chain:           protov1.Chain_CHAIN_ETHEREUM,
			CommitmentLevel: protov1.CommitmentLevel_COMMITMENT_LEVEL_FINALIZED,
			BlockNumber:     i,
		}
		_, _ = detector.ProcessEvent(ctx, event)
	}

	stats := detector.Stats()
	if stats == nil {
		t.Fatal("stats should not be nil")
	}

	chains, ok := stats["chains"].(map[string]interface{})
	if !ok {
		t.Fatal("stats should contain chains")
	}

	if len(chains) == 0 {
		t.Error("should have at least one chain tracked")
	}

	if stats["halted"] != false {
		t.Error("should not be halted")
	}
}

func TestGapDetector_NilEvent(t *testing.T) {
	detector := NewGapDetector(DefaultGapDetectorConfig(), nil)
	ctx := context.Background()

	_, err := detector.ProcessEvent(ctx, nil)
	if err == nil {
		t.Error("expected error for nil event")
	}
}

func TestDefaultGapDetectorConfig(t *testing.T) {
	cfg := DefaultGapDetectorConfig()

	if !cfg.FailClosed {
		t.Error("default should have fail-closed enabled")
	}
	if cfg.MaxAllowedGap != 1 {
		t.Errorf("default max allowed gap should be 1, got %d", cfg.MaxAllowedGap)
	}
	if cfg.AlertOnFirstBlock {
		t.Error("default should not alert on first block")
	}
}

func BenchmarkGapDetector_ProcessEvent(b *testing.B) {
	detector := NewGapDetector(GapDetectorConfig{
		FailClosed: false,
	}, nil)

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		event := &protov1.CanonicalEvent{
			Chain:           protov1.Chain_CHAIN_ETHEREUM,
			CommitmentLevel: protov1.CommitmentLevel_COMMITMENT_LEVEL_FINALIZED,
			BlockNumber:     uint64(i),
			BlockHash:       "0xbenchmark",
			Timestamp:       time.Now(),
		}
		_, _ = detector.ProcessEvent(ctx, event)
	}
}
