package correctness

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"

	"github.com/mirador/pulse/internal/adapter"
	protov1 "github.com/mirador/pulse/pkg/proto/v1"
)

func TestNewReorgDetector(t *testing.T) {
	cfg := DefaultReorgDetectorConfig()
	detector := NewReorgDetector(cfg, nil)

	if detector == nil {
		t.Fatal("NewReorgDetector returned nil")
	}

	if detector.maxTracked != 1000 {
		t.Errorf("expected maxTracked=1000, got %d", detector.maxTracked)
	}
}

func TestReorgDetector_FirstBlock(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	detector := NewReorgDetector(DefaultReorgDetectorConfig(), logger)

	block := &BlockInfo{
		Number:     100,
		Hash:       "0xabc123def456abc123def456abc123def456abc123def456abc123def456abc1",
		ParentHash: "0x000000000000000000000000000000000000000000000000000000000000000",
		Timestamp:  1000,
	}

	reorg, err := detector.ProcessBlock(ctx, block)
	if err != nil {
		t.Fatalf("ProcessBlock failed: %v", err)
	}
	if reorg != nil {
		t.Error("first block should not trigger reorg")
	}

	head := detector.GetChainHead("default")
	if head == nil {
		t.Fatal("chain head should be set")
	}
	if head.Number != 100 {
		t.Errorf("expected head number 100, got %d", head.Number)
	}
}

func TestReorgDetector_NormalChainExtension(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	detector := NewReorgDetector(DefaultReorgDetectorConfig(), logger)

	// First block
	block1 := &BlockInfo{
		Number:     100,
		Hash:       "0xblock100_hash_0000000000000000000000000000000000000000000000",
		ParentHash: "0xblock99_hash_00000000000000000000000000000000000000000000000",
		Timestamp:  1000,
	}
	detector.ProcessBlock(ctx, block1)

	// Second block extending the chain
	block2 := &BlockInfo{
		Number:     101,
		Hash:       "0xblock101_hash_0000000000000000000000000000000000000000000000",
		ParentHash: "0xblock100_hash_0000000000000000000000000000000000000000000000", // Points to block1
		Timestamp:  1012,
	}

	reorg, err := detector.ProcessBlock(ctx, block2)
	if err != nil {
		t.Fatalf("ProcessBlock failed: %v", err)
	}
	if reorg != nil {
		t.Error("normal chain extension should not trigger reorg")
	}

	head := detector.GetChainHead("default")
	if head.Number != 101 {
		t.Errorf("expected head number 101, got %d", head.Number)
	}
}

func TestReorgDetector_DetectsReorg(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	detector := NewReorgDetector(DefaultReorgDetectorConfig(), logger)

	// Build a chain: 100 -> 101 -> 102
	blocks := []*BlockInfo{
		{Number: 100, Hash: "0xblock100_aaaa000000000000000000000000000000000000000000000", ParentHash: "0xblock99_0000000000000000000000000000000000000000000000000", Timestamp: 1000},
		{Number: 101, Hash: "0xblock101_aaaa000000000000000000000000000000000000000000000", ParentHash: "0xblock100_aaaa000000000000000000000000000000000000000000000", Timestamp: 1012},
		{Number: 102, Hash: "0xblock102_aaaa000000000000000000000000000000000000000000000", ParentHash: "0xblock101_aaaa000000000000000000000000000000000000000000000", Timestamp: 1024},
	}

	for _, b := range blocks {
		_, err := detector.ProcessBlock(ctx, b)
		if err != nil {
			t.Fatalf("ProcessBlock failed: %v", err)
		}
	}

	// Now introduce a reorg: new block 102 that forks from block 100
	forkBlock := &BlockInfo{
		Number:     102,
		Hash:       "0xblock102_bbbb000000000000000000000000000000000000000000000", // Different hash
		ParentHash: "0xblock100_aaaa000000000000000000000000000000000000000000000", // Forks from block 100
		Timestamp:  1024,
	}

	reorg, err := detector.ProcessBlock(ctx, forkBlock)
	if err != nil {
		t.Fatalf("ProcessBlock failed: %v", err)
	}

	if reorg == nil {
		t.Fatal("expected reorg to be detected")
	}

	if reorg.ForkPoint != 100 {
		t.Errorf("expected fork point 100, got %d", reorg.ForkPoint)
	}

	if reorg.Depth != 2 {
		t.Errorf("expected depth 2, got %d", reorg.Depth)
	}

	// Should have orphaned blocks 101 and 102
	if len(reorg.OrphanedBlocks) != 2 {
		t.Errorf("expected 2 orphaned blocks, got %d", len(reorg.OrphanedBlocks))
	}
}

func TestReorgDetector_DuplicateBlock(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	detector := NewReorgDetector(DefaultReorgDetectorConfig(), logger)

	block := &BlockInfo{
		Number:     100,
		Hash:       "0xblock100_hash_0000000000000000000000000000000000000000000000",
		ParentHash: "0xblock99_hash_00000000000000000000000000000000000000000000000",
		Timestamp:  1000,
	}

	// Process same block twice
	detector.ProcessBlock(ctx, block)
	reorg, err := detector.ProcessBlock(ctx, block)

	if err != nil {
		t.Fatalf("ProcessBlock failed: %v", err)
	}
	if reorg != nil {
		t.Error("duplicate block should not trigger reorg")
	}
}

func TestReorgDetector_MultiChain(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	detector := NewReorgDetector(DefaultReorgDetectorConfig(), logger)

	// Process blocks for ethereum
	ethBlock := &BlockInfo{
		Number:     100,
		Hash:       "0xeth_block100_0000000000000000000000000000000000000000000000",
		ParentHash: "0xeth_block99_00000000000000000000000000000000000000000000000",
		Timestamp:  1000,
	}
	detector.ProcessBlockForChain(ctx, "ethereum", ethBlock)

	// Process blocks for solana
	solBlock := &BlockInfo{
		Number:     50000,
		Hash:       "sol_block50000_000000000000000000000000000000000000000000000",
		ParentHash: "sol_block49999_000000000000000000000000000000000000000000000",
		Timestamp:  1000,
	}
	detector.ProcessBlockForChain(ctx, "solana", solBlock)

	// Verify each chain has separate head
	ethHead := detector.GetChainHead("ethereum")
	solHead := detector.GetChainHead("solana")

	if ethHead.Number != 100 {
		t.Errorf("ethereum head should be 100, got %d", ethHead.Number)
	}
	if solHead.Number != 50000 {
		t.Errorf("solana head should be 50000, got %d", solHead.Number)
	}
}

func TestReorgDetector_ReorgCallback(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	detector := NewReorgDetector(DefaultReorgDetectorConfig(), logger)

	var callbackEvent *ReorgEvent
	detector.OnReorg(func(ctx context.Context, event *ReorgEvent) error {
		callbackEvent = event
		return nil
	})

	// Build chain: 99 -> 100 -> 101
	blocks := []*BlockInfo{
		{Number: 99, Hash: "0xblock99_0000000000000000000000000000000000000000000000000", ParentHash: "0xblock98_0000000000000000000000000000000000000000000000000", Timestamp: 988},
		{Number: 100, Hash: "0xblock100_aaaa000000000000000000000000000000000000000000000", ParentHash: "0xblock99_0000000000000000000000000000000000000000000000000", Timestamp: 1000},
		{Number: 101, Hash: "0xblock101_aaaa000000000000000000000000000000000000000000000", ParentHash: "0xblock100_aaaa000000000000000000000000000000000000000000000", Timestamp: 1012},
	}

	for _, b := range blocks {
		detector.ProcessBlock(ctx, b)
	}

	// Trigger reorg: new block 101 that forks from block 99 (instead of 100)
	forkBlock := &BlockInfo{
		Number:     101,
		Hash:       "0xblock101_bbbb000000000000000000000000000000000000000000000",
		ParentHash: "0xblock99_0000000000000000000000000000000000000000000000000", // Fork from block 99
		Timestamp:  1012,
	}
	detector.ProcessBlock(ctx, forkBlock)

	if callbackEvent == nil {
		t.Fatal("callback should have been called")
	}
	if callbackEvent.ForkPoint != 99 {
		t.Errorf("expected fork point 99, got %d", callbackEvent.ForkPoint)
	}
}

func TestReorgDetector_GetBlockByHash(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	detector := NewReorgDetector(DefaultReorgDetectorConfig(), logger)

	hash := "0xblock100_hash_0000000000000000000000000000000000000000000000"
	block := &BlockInfo{
		Number:     100,
		Hash:       hash,
		ParentHash: "0xblock99_hash_00000000000000000000000000000000000000000000000",
		Timestamp:  1000,
	}
	detector.ProcessBlock(ctx, block)

	found := detector.GetBlockByHash("default", hash)
	if found == nil {
		t.Fatal("should find block by hash")
	}
	if found.Number != 100 {
		t.Errorf("expected block 100, got %d", found.Number)
	}

	notFound := detector.GetBlockByHash("default", "0xnonexistent000000000000000000000000000000000000000000000")
	if notFound != nil {
		t.Error("should not find nonexistent block")
	}
}

func TestReorgDetector_GetBlockByHeight(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	detector := NewReorgDetector(DefaultReorgDetectorConfig(), logger)

	block := &BlockInfo{
		Number:     100,
		Hash:       "0xblock100_hash_0000000000000000000000000000000000000000000000",
		ParentHash: "0xblock99_hash_00000000000000000000000000000000000000000000000",
		Timestamp:  1000,
	}
	detector.ProcessBlock(ctx, block)

	found := detector.GetBlockByHeight("default", 100)
	if found == nil {
		t.Fatal("should find block by height")
	}

	notFound := detector.GetBlockByHeight("default", 999)
	if notFound != nil {
		t.Error("should not find nonexistent block")
	}
}

func TestReorgDetector_ProcessAdapterEvent(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	detector := NewReorgDetector(DefaultReorgDetectorConfig(), logger)

	event := adapter.Event{
		Chain:           "ethereum",
		CommitmentLevel: "finalized",
		BlockNumber:     100,
		BlockHash:       "0xblock100_hash_0000000000000000000000000000000000000000000000",
		ParentHash:      "0xblock99_hash_00000000000000000000000000000000000000000000000",
		EventType:       "block",
		Timestamp:       1000,
	}

	reorg, err := detector.ProcessAdapterEvent(ctx, event)
	if err != nil {
		t.Fatalf("ProcessAdapterEvent failed: %v", err)
	}
	if reorg != nil {
		t.Error("first block should not trigger reorg")
	}

	head := detector.GetChainHead("ethereum")
	if head == nil {
		t.Fatal("should have chain head")
	}
	if head.Number != 100 {
		t.Errorf("expected head 100, got %d", head.Number)
	}
}

func TestReorgDetector_ProcessAdapterEvent_SkipsNonBlocks(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	detector := NewReorgDetector(DefaultReorgDetectorConfig(), logger)

	// Log event without block hash should be skipped
	event := adapter.Event{
		Chain:     "ethereum",
		EventType: "log",
		BlockHash: "",
	}

	reorg, err := detector.ProcessAdapterEvent(ctx, event)
	if err != nil {
		t.Fatalf("ProcessAdapterEvent failed: %v", err)
	}
	if reorg != nil {
		t.Error("non-block event should not trigger reorg")
	}

	head := detector.GetChainHead("ethereum")
	if head != nil {
		t.Error("non-block event should not set chain head")
	}
}

func TestReorgDetector_CreateRetractionEvents(t *testing.T) {
	detector := NewReorgDetector(DefaultReorgDetectorConfig(), nil)

	reorg := &ReorgEvent{
		Chain:          "ethereum",
		ForkPoint:      100,
		OrphanedBlocks: []uint64{101, 102},
		OrphanedHashes: []string{"0xhash101", "0xhash102"},
		Depth:          2,
	}

	originalEvents := []*protov1.CanonicalEvent{
		{EventId: "evt_1", BlockNumber: 101, Chain: protov1.Chain_CHAIN_ETHEREUM},
		{EventId: "evt_2", BlockNumber: 101, Chain: protov1.Chain_CHAIN_ETHEREUM},
		{EventId: "evt_3", BlockNumber: 102, Chain: protov1.Chain_CHAIN_ETHEREUM},
		{EventId: "evt_4", BlockNumber: 100, Chain: protov1.Chain_CHAIN_ETHEREUM}, // Not orphaned
	}

	retractions := detector.CreateRetractionEvents(reorg, originalEvents)

	if len(retractions) != 3 {
		t.Errorf("expected 3 retractions, got %d", len(retractions))
	}

	for _, r := range retractions {
		if r.ReorgAction != protov1.ReorgAction_REORG_ACTION_RETRACT {
			t.Error("retraction should have RETRACT action")
		}
	}
}

func TestReorgDetector_Stats(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	detector := NewReorgDetector(DefaultReorgDetectorConfig(), logger)

	// Add some blocks
	for i := uint64(100); i < 105; i++ {
		block := &BlockInfo{
			Number:     i,
			Hash:       fmt.Sprintf("0xblock%d_hash000000000000000000000000000000000000000000000", i),
			ParentHash: fmt.Sprintf("0xblock%d_hash000000000000000000000000000000000000000000000", i-1),
			Timestamp:  int64(1000 + i*12),
		}
		detector.ProcessBlock(ctx, block)
	}

	stats := detector.Stats()
	chains, ok := stats["chains"].(map[string]interface{})
	if !ok {
		t.Fatal("stats should have chains")
	}

	defaultChain, ok := chains["default"].(map[string]interface{})
	if !ok {
		t.Fatal("stats should have default chain")
	}

	if defaultChain["head_number"].(uint64) != 104 {
		t.Errorf("expected head number 104, got %v", defaultChain["head_number"])
	}
}

func TestReorgDetector_PrunesOldBlocks(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	cfg := ReorgDetectorConfig{
		MaxTrackedBlocks: 10, // Small limit for testing
		MinConfirmations: 3,
	}
	detector := NewReorgDetector(cfg, logger)

	// Add 20 blocks
	for i := uint64(100); i < 120; i++ {
		block := &BlockInfo{
			Number:     i,
			Hash:       fmt.Sprintf("0xblock%d_hash000000000000000000000000000000000000000000000", i),
			ParentHash: fmt.Sprintf("0xblock%d_hash000000000000000000000000000000000000000000000", i-1),
			Timestamp:  int64(1000 + i*12),
		}
		detector.ProcessBlock(ctx, block)
	}

	// Old blocks should be pruned
	oldBlock := detector.GetBlockByHeight("default", 100)
	if oldBlock != nil {
		t.Error("old block should have been pruned")
	}

	// Recent blocks should still exist
	recentBlock := detector.GetBlockByHeight("default", 115)
	if recentBlock == nil {
		t.Error("recent block should still exist")
	}
}

func TestReorgDetector_NilBlock(t *testing.T) {
	ctx := context.Background()
	detector := NewReorgDetector(DefaultReorgDetectorConfig(), nil)

	_, err := detector.ProcessBlock(ctx, nil)
	if err == nil {
		t.Error("should error on nil block")
	}
}
