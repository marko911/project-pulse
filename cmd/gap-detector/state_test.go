package main

import (
	"testing"

	protov1 "github.com/marko911/project-pulse/pkg/proto/v1"
)

func TestChainState_FirstBlock(t *testing.T) {
	state := NewChainState()

	gap := state.CheckAndUpdate(protov1.Chain_CHAIN_ETHEREUM, 3, 100, "test-topic")
	if gap != nil {
		t.Errorf("expected no gap for first block, got: %+v", gap)
	}

	lastBlock, exists := state.GetLastBlock(protov1.Chain_CHAIN_ETHEREUM, 3)
	if !exists {
		t.Error("expected chain to exist after first block")
	}
	if lastBlock != 100 {
		t.Errorf("expected last block 100, got %d", lastBlock)
	}
}

func TestChainState_ConsecutiveBlocks(t *testing.T) {
	state := NewChainState()

	blocks := []uint64{100, 101, 102, 103, 104}
	for _, block := range blocks {
		gap := state.CheckAndUpdate(protov1.Chain_CHAIN_ETHEREUM, 3, block, "test-topic")
		if gap != nil {
			t.Errorf("expected no gap for consecutive block %d, got: %+v", block, gap)
		}
	}

	lastBlock, _ := state.GetLastBlock(protov1.Chain_CHAIN_ETHEREUM, 3)
	if lastBlock != 104 {
		t.Errorf("expected last block 104, got %d", lastBlock)
	}
}

func TestChainState_GapDetection(t *testing.T) {
	state := NewChainState()

	state.CheckAndUpdate(protov1.Chain_CHAIN_ETHEREUM, 3, 100, "test-topic")

	gap := state.CheckAndUpdate(protov1.Chain_CHAIN_ETHEREUM, 3, 102, "test-topic")
	if gap == nil {
		t.Fatal("expected gap to be detected")
	}

	if gap.ExpectedBlock != 101 {
		t.Errorf("expected ExpectedBlock 101, got %d", gap.ExpectedBlock)
	}
	if gap.ReceivedBlock != 102 {
		t.Errorf("expected ReceivedBlock 102, got %d", gap.ReceivedBlock)
	}
	if gap.GapSize != 1 {
		t.Errorf("expected GapSize 1, got %d", gap.GapSize)
	}
	if gap.Chain != protov1.Chain_CHAIN_ETHEREUM {
		t.Errorf("expected Chain ETHEREUM, got %d", gap.Chain)
	}
}

func TestChainState_LargeGap(t *testing.T) {
	state := NewChainState()

	state.CheckAndUpdate(protov1.Chain_CHAIN_SOLANA, 3, 1000, "test-topic")

	gap := state.CheckAndUpdate(protov1.Chain_CHAIN_SOLANA, 3, 1010, "test-topic")
	if gap == nil {
		t.Fatal("expected gap to be detected")
	}

	if gap.GapSize != 9 {
		t.Errorf("expected GapSize 9, got %d", gap.GapSize)
	}
}

func TestChainState_DuplicateBlock(t *testing.T) {
	state := NewChainState()

	state.CheckAndUpdate(protov1.Chain_CHAIN_ETHEREUM, 3, 100, "test-topic")
	state.CheckAndUpdate(protov1.Chain_CHAIN_ETHEREUM, 3, 101, "test-topic")
	state.CheckAndUpdate(protov1.Chain_CHAIN_ETHEREUM, 3, 102, "test-topic")

	gap := state.CheckAndUpdate(protov1.Chain_CHAIN_ETHEREUM, 3, 101, "test-topic")
	if gap != nil {
		t.Errorf("expected no gap for duplicate block, got: %+v", gap)
	}

	lastBlock, _ := state.GetLastBlock(protov1.Chain_CHAIN_ETHEREUM, 3)
	if lastBlock != 102 {
		t.Errorf("expected last block 102 after duplicate, got %d", lastBlock)
	}
}

func TestChainState_MultipleChains(t *testing.T) {
	state := NewChainState()

	state.CheckAndUpdate(protov1.Chain_CHAIN_ETHEREUM, 3, 100, "eth-topic")
	state.CheckAndUpdate(protov1.Chain_CHAIN_SOLANA, 3, 50000, "sol-topic")
	state.CheckAndUpdate(protov1.Chain_CHAIN_POLYGON, 3, 200, "poly-topic")

	gap := state.CheckAndUpdate(protov1.Chain_CHAIN_ETHEREUM, 3, 101, "eth-topic")
	if gap != nil {
		t.Error("expected no gap for ethereum")
	}

	gap = state.CheckAndUpdate(protov1.Chain_CHAIN_SOLANA, 3, 50001, "sol-topic")
	if gap != nil {
		t.Error("expected no gap for solana")
	}

	gap = state.CheckAndUpdate(protov1.Chain_CHAIN_POLYGON, 3, 205, "poly-topic")
	if gap == nil {
		t.Fatal("expected gap for polygon")
	}
	if gap.Chain != protov1.Chain_CHAIN_POLYGON {
		t.Errorf("expected gap on polygon, got chain %d", gap.Chain)
	}
	if gap.GapSize != 4 {
		t.Errorf("expected gap size 4, got %d", gap.GapSize)
	}
}

func TestChainState_DifferentCommitmentLevels(t *testing.T) {
	state := NewChainState()

	state.CheckAndUpdate(protov1.Chain_CHAIN_ETHEREUM, 1, 100, "topic")
	state.CheckAndUpdate(protov1.Chain_CHAIN_ETHEREUM, 2, 95, "topic")
	state.CheckAndUpdate(protov1.Chain_CHAIN_ETHEREUM, 3, 90, "topic")

	lastProcessed, _ := state.GetLastBlock(protov1.Chain_CHAIN_ETHEREUM, 1)
	lastConfirmed, _ := state.GetLastBlock(protov1.Chain_CHAIN_ETHEREUM, 2)
	lastFinalized, _ := state.GetLastBlock(protov1.Chain_CHAIN_ETHEREUM, 3)

	if lastProcessed != 100 {
		t.Errorf("expected processed=100, got %d", lastProcessed)
	}
	if lastConfirmed != 95 {
		t.Errorf("expected confirmed=95, got %d", lastConfirmed)
	}
	if lastFinalized != 90 {
		t.Errorf("expected finalized=90, got %d", lastFinalized)
	}
}

func TestChainState_GetStats(t *testing.T) {
	state := NewChainState()

	state.CheckAndUpdate(protov1.Chain_CHAIN_ETHEREUM, 3, 100, "topic")
	state.CheckAndUpdate(protov1.Chain_CHAIN_ETHEREUM, 3, 101, "topic")
	state.CheckAndUpdate(protov1.Chain_CHAIN_ETHEREUM, 3, 105, "topic")

	stats := state.GetStats()

	if stats["chain_count"].(int) != 1 {
		t.Errorf("expected chain_count=1, got %v", stats["chain_count"])
	}
	if stats["total_blocks"].(int64) != 3 {
		t.Errorf("expected total_blocks=3, got %v", stats["total_blocks"])
	}
	if stats["total_gaps"].(int64) != 1 {
		t.Errorf("expected total_gaps=1, got %v", stats["total_gaps"])
	}
}

func TestChainState_GetState(t *testing.T) {
	state := NewChainState()

	state.CheckAndUpdate(protov1.Chain_CHAIN_ETHEREUM, 3, 100, "topic")
	state.CheckAndUpdate(protov1.Chain_CHAIN_SOLANA, 3, 200, "topic")

	snapshots := state.GetState()

	if len(snapshots) != 2 {
		t.Errorf("expected 2 snapshots, got %d", len(snapshots))
	}

	for _, snap := range snapshots {
		if snap.Tracker == nil {
			t.Error("expected tracker in snapshot")
		}
		if !snap.Tracker.Initialized {
			t.Error("expected tracker to be initialized")
		}
	}
}

func TestChainState_Reset(t *testing.T) {
	state := NewChainState()

	state.CheckAndUpdate(protov1.Chain_CHAIN_ETHEREUM, 3, 100, "topic")
	state.CheckAndUpdate(protov1.Chain_CHAIN_SOLANA, 3, 200, "topic")

	state.Reset()

	snapshots := state.GetState()
	if len(snapshots) != 0 {
		t.Errorf("expected 0 snapshots after reset, got %d", len(snapshots))
	}

	_, exists := state.GetLastBlock(protov1.Chain_CHAIN_ETHEREUM, 3)
	if exists {
		t.Error("expected chain to not exist after reset")
	}
}

func TestChainState_ConcurrentAccess(t *testing.T) {
	state := NewChainState()

	done := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		go func(chain protov1.Chain, start uint64) {
			for j := uint64(0); j < 100; j++ {
				state.CheckAndUpdate(chain, 3, start+j, "topic")
			}
			done <- true
		}(protov1.Chain(i%8+1), uint64(i*1000))
	}

	for i := 0; i < 10; i++ {
		<-done
	}

	stats := state.GetStats()
	if stats["chain_count"].(int) == 0 {
		t.Error("expected some chains to be tracked")
	}
}
