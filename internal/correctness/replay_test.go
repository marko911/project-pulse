package correctness

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/marko911/project-pulse/internal/adapter"
	"github.com/marko911/project-pulse/internal/adapter/replay"
	protov1 "github.com/marko911/project-pulse/pkg/proto/v1"
)

func TestDeterministicReplayWithReorg(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	tempDir, err := os.MkdirTemp("", "reorg-test-fixtures-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	fixtures := createReorgFixtures()
	for i, fixture := range fixtures {
		filename := filepath.Join(tempDir, fixture.filename)
		data, err := json.MarshalIndent(fixture.data, "", "  ")
		if err != nil {
			t.Fatalf("failed to marshal fixture %d: %v", i, err)
		}
		if err := os.WriteFile(filename, data, 0644); err != nil {
			t.Fatalf("failed to write fixture %d: %v", i, err)
		}
	}

	fileSource := replay.NewFileSource(replay.FileSourceConfig{
		Chain:       "evm",
		FixturesDir: tempDir,
		Loop:        false,
	}, logger)

	detector := NewReorgDetector(DefaultReorgDetectorConfig(), logger)

	var allEvents []adapter.Event
	var reorgEvents []*ReorgEvent

	detector.OnReorg(func(ctx context.Context, event *ReorgEvent) error {
		reorgEvents = append(reorgEvents, event)
		return nil
	})

	eventCh := make(chan adapter.Event, 100)

	go func() {
		defer close(eventCh)
		if err := fileSource.Stream(ctx, eventCh); err != nil {
			t.Logf("stream error: %v", err)
		}
	}()

	for event := range eventCh {
		allEvents = append(allEvents, event)

		if event.EventType == "block" && event.BlockHash != "" {
			_, err := detector.ProcessAdapterEvent(ctx, event)
			if err != nil {
				t.Fatalf("ProcessAdapterEvent failed: %v", err)
			}
		}
	}

	if len(allEvents) < 4 {
		t.Errorf("expected at least 4 events, got %d", len(allEvents))
	}

	if len(reorgEvents) != 1 {
		t.Fatalf("expected 1 reorg event, got %d", len(reorgEvents))
	}

	reorg := reorgEvents[0]
	t.Logf("Reorg detected: fork_point=%d, depth=%d, orphaned=%v",
		reorg.ForkPoint, reorg.Depth, reorg.OrphanedBlocks)

	if reorg.ForkPoint != 100 {
		t.Errorf("expected fork point 100, got %d", reorg.ForkPoint)
	}

	orphanedSet := make(map[uint64]bool)
	for _, b := range reorg.OrphanedBlocks {
		orphanedSet[b] = true
	}
	if !orphanedSet[101] || !orphanedSet[102] {
		t.Errorf("expected orphaned blocks to include 101 and 102, got %v", reorg.OrphanedBlocks)
	}
}

func TestReorgEventSequence(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	detector := NewReorgDetector(DefaultReorgDetectorConfig(), logger)

	canonicalBlocks := []*BlockInfo{
		{Number: 100, Hash: "0xblock100_aaaa000000000000000000000000000000000000000000000", ParentHash: "0xblock99_0000000000000000000000000000000000000000000000000", Timestamp: 1000},
		{Number: 101, Hash: "0xblock101_aaaa000000000000000000000000000000000000000000000", ParentHash: "0xblock100_aaaa000000000000000000000000000000000000000000000", Timestamp: 1012},
		{Number: 102, Hash: "0xblock102_aaaa000000000000000000000000000000000000000000000", ParentHash: "0xblock101_aaaa000000000000000000000000000000000000000000000", Timestamp: 1024},
	}

	for _, block := range canonicalBlocks {
		_, err := detector.ProcessBlock(ctx, block)
		if err != nil {
			t.Fatalf("failed to process block %d: %v", block.Number, err)
		}
	}

	canonicalEvents := []*protov1.CanonicalEvent{
		{EventId: "evt_100_1", BlockNumber: 100, TxIndex: 0, EventIndex: 0, Chain: protov1.Chain_CHAIN_ETHEREUM},
		{EventId: "evt_101_1", BlockNumber: 101, TxIndex: 0, EventIndex: 0, Chain: protov1.Chain_CHAIN_ETHEREUM},
		{EventId: "evt_101_2", BlockNumber: 101, TxIndex: 1, EventIndex: 0, Chain: protov1.Chain_CHAIN_ETHEREUM},
		{EventId: "evt_102_1", BlockNumber: 102, TxIndex: 0, EventIndex: 0, Chain: protov1.Chain_CHAIN_ETHEREUM},
	}

	forkBlock := &BlockInfo{
		Number:     102,
		Hash:       "0xblock102_bbbb000000000000000000000000000000000000000000000",
		ParentHash: "0xblock100_aaaa000000000000000000000000000000000000000000000",
		Timestamp:  1030,
	}

	reorg, err := detector.ProcessBlock(ctx, forkBlock)
	if err != nil {
		t.Fatalf("ProcessBlock for fork failed: %v", err)
	}

	if reorg == nil {
		t.Fatal("expected reorg to be detected")
	}

	retractions := detector.CreateRetractionEvents(reorg, canonicalEvents)

	if len(retractions) != 3 {
		t.Errorf("expected 3 retractions, got %d", len(retractions))
	}

	for _, r := range retractions {
		if r.ReorgAction != protov1.ReorgAction_REORG_ACTION_RETRACT {
			t.Errorf("expected RETRACT action, got %v", r.ReorgAction)
		}
		expectedSuffix := ":retract"
		if len(r.EventId) < len(expectedSuffix) || r.EventId[len(r.EventId)-len(expectedSuffix):] != expectedSuffix {
			t.Errorf("retraction event ID should end with :retract, got %s", r.EventId)
		}
	}

	newCanonicalEvents := []*protov1.CanonicalEvent{
		{EventId: "evt_102b_1", BlockNumber: 102, TxIndex: 0, EventIndex: 0, Chain: protov1.Chain_CHAIN_ETHEREUM},
		{EventId: "evt_102b_2", BlockNumber: 102, TxIndex: 1, EventIndex: 0, Chain: protov1.Chain_CHAIN_ETHEREUM},
	}

	replacements := detector.CreateReplacementEvents(reorg, retractions, newCanonicalEvents)

	if len(replacements) != 2 {
		t.Errorf("expected 2 replacements, got %d", len(replacements))
	}

	for _, r := range replacements {
		if r.ReorgAction != protov1.ReorgAction_REORG_ACTION_REPLACE {
			t.Errorf("expected REPLACE action, got %v", r.ReorgAction)
		}
	}

	t.Logf("Reorg sequence verified: %d retractions, %d replacements",
		len(retractions), len(replacements))
}

func TestOutboxSequenceOrdering(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	detector := NewReorgDetector(DefaultReorgDetectorConfig(), logger)

	var eventSequence []string

	detector.OnReorg(func(ctx context.Context, event *ReorgEvent) error {
		eventSequence = append(eventSequence, "REORG_DETECTED")
		return nil
	})

	blocks := []*BlockInfo{
		{Number: 100, Hash: "0xh100_000000000000000000000000000000000000000000000000", ParentHash: "0xh99_0000000000000000000000000000000000000000000000000", Timestamp: 1000},
		{Number: 101, Hash: "0xh101_000000000000000000000000000000000000000000000000", ParentHash: "0xh100_000000000000000000000000000000000000000000000000", Timestamp: 1012},
		{Number: 102, Hash: "0xh102_000000000000000000000000000000000000000000000000", ParentHash: "0xh101_000000000000000000000000000000000000000000000000", Timestamp: 1024},
	}

	for _, block := range blocks {
		_, err := detector.ProcessBlock(ctx, block)
		if err != nil {
			t.Fatalf("failed to process block %d: %v", block.Number, err)
		}
		eventSequence = append(eventSequence, "BLOCK_"+string(rune('0'+block.Number-100+48)))
	}

	forkBlock := &BlockInfo{
		Number:     102,
		Hash:       "0xhFORK_0000000000000000000000000000000000000000000000",
		ParentHash: "0xh100_000000000000000000000000000000000000000000000000",
		Timestamp:  1030,
	}
	_, err := detector.ProcessBlock(ctx, forkBlock)
	if err != nil {
		t.Fatalf("ProcessBlock for fork failed: %v", err)
	}
	eventSequence = append(eventSequence, "FORK_BLOCK")

	t.Logf("Event sequence: %v", eventSequence)

	reorgIdx := -1
	for i, e := range eventSequence {
		if e == "REORG_DETECTED" {
			reorgIdx = i
			break
		}
	}

	if reorgIdx == -1 {
		t.Error("REORG_DETECTED not found in sequence")
	}

	if reorgIdx < 3 {
		t.Error("REORG_DETECTED should come after initial blocks")
	}
}

type fixtureEntry struct {
	filename string
	data     interface{}
}

func createReorgFixtures() []fixtureEntry {
	now := time.Now()

	return []fixtureEntry{
		{
			filename: "block_100.json",
			data: map[string]interface{}{
				"chain":      "evm",
				"type":       "block",
				"recorded_at": now.Add(-3 * time.Minute).Format(time.RFC3339),
				"data": map[string]interface{}{
					"number":       100,
					"hash":         "0xblock100_aaaa000000000000000000000000000000000000000000000",
					"parent_hash":  "0xblock99_0000000000000000000000000000000000000000000000000",
					"timestamp":    now.Add(-3 * time.Minute).Unix(),
					"gas_limit":    30000000,
					"gas_used":     15000000,
					"transactions": []string{"tx100_1", "tx100_2"},
				},
			},
		},
		{
			filename: "block_101.json",
			data: map[string]interface{}{
				"chain":       "evm",
				"type":        "block",
				"recorded_at": now.Add(-2 * time.Minute).Format(time.RFC3339),
				"data": map[string]interface{}{
					"number":       101,
					"hash":         "0xblock101_aaaa000000000000000000000000000000000000000000000",
					"parent_hash":  "0xblock100_aaaa000000000000000000000000000000000000000000000",
					"timestamp":    now.Add(-2 * time.Minute).Unix(),
					"gas_limit":    30000000,
					"gas_used":     12000000,
					"transactions": []string{"tx101_1"},
				},
			},
		},
		{
			filename: "block_102.json",
			data: map[string]interface{}{
				"chain":       "evm",
				"type":        "block",
				"recorded_at": now.Add(-1 * time.Minute).Format(time.RFC3339),
				"data": map[string]interface{}{
					"number":       102,
					"hash":         "0xblock102_aaaa000000000000000000000000000000000000000000000",
					"parent_hash":  "0xblock101_aaaa000000000000000000000000000000000000000000000",
					"timestamp":    now.Add(-1 * time.Minute).Unix(),
					"gas_limit":    30000000,
					"gas_used":     18000000,
					"transactions": []string{"tx102_1", "tx102_2", "tx102_3"},
				},
			},
		},
		{
			filename: "block_102_reorg.json",
			data: map[string]interface{}{
				"chain":       "evm",
				"type":        "block",
				"recorded_at": now.Format(time.RFC3339),
				"data": map[string]interface{}{
					"number":       102,
					"hash":         "0xblock102_bbbb000000000000000000000000000000000000000000000",
					"parent_hash":  "0xblock100_aaaa000000000000000000000000000000000000000000000",
					"timestamp":    now.Unix(),
					"gas_limit":    30000000,
					"gas_used":     20000000,
					"transactions": []string{"tx102b_1", "tx102b_2"},
				},
			},
		},
	}
}
