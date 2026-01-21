package correctness

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/marko911/project-pulse/internal/adapter"
	"github.com/marko911/project-pulse/internal/adapter/replay"
	protov1 "github.com/marko911/project-pulse/pkg/proto/v1"
)

func TestReorgDetection_Integration(t *testing.T) {
	fixturesDir := findFixturesDir(t)
	reorgFixturesDir := filepath.Join(fixturesDir, "reorg_test")

	if _, err := os.Stat(reorgFixturesDir); os.IsNotExist(err) {
		t.Skip("Reorg fixtures not found at:", reorgFixturesDir)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	source := replay.NewFileSource(replay.FileSourceConfig{
		Chain:         "",
		FixturesDir:   reorgFixturesDir,
		Loop:          false,
		PlaybackSpeed: 0,
	}, logger)

	detector := NewReorgDetector(DefaultReorgDetectorConfig(), logger)

	var detectedReorgs []*ReorgEvent
	detector.OnReorg(func(ctx context.Context, event *ReorgEvent) error {
		t.Logf("Reorg detected: fork_point=%d, depth=%d, orphaned=%v, new=%v",
			event.ForkPoint, event.Depth, event.OrphanedBlocks, event.NewBlocks)
		detectedReorgs = append(detectedReorgs, event)
		return nil
	})

	events := make(chan adapter.Event, 100)

	streamErr := make(chan error, 1)
	go func() {
		streamErr <- source.Stream(ctx, events)
		close(events)
	}()

	var processedEvents []adapter.Event
	for event := range events {
		processedEvents = append(processedEvents, event)

		reorg, err := detector.ProcessAdapterEvent(ctx, event)
		if err != nil {
			t.Errorf("ProcessAdapterEvent failed: %v", err)
			continue
		}

		if reorg != nil {
			t.Logf("Direct reorg return: %+v", reorg)
		}
	}

	if err := <-streamErr; err != nil {
		t.Fatalf("Stream error: %v", err)
	}

	t.Logf("Processed %d events", len(processedEvents))
	if len(processedEvents) < 3 {
		t.Fatalf("Expected at least 3 events, got %d", len(processedEvents))
	}

	if len(detectedReorgs) == 0 {
		t.Log("No reorgs detected via callback, checking if detection worked via return value...")
	}

	head := detector.GetChainHead("evm")
	if head == nil {
		t.Fatal("No head block tracked for evm chain")
	}

	t.Logf("Final head: number=%d, hash=%s", head.Number, head.Hash[:20])

	if head.Number != 102 {
		t.Errorf("Expected head at block 102, got %d", head.Number)
	}

	block101 := detector.GetBlockByHeight("evm", 101)
	if block101 == nil {
		t.Fatal("Block 101 not found in chain")
	}

	expectedHash := "0xccc101000000000000000000000000000000000000000000000000000000"
	if block101.Hash != expectedHash {
		t.Errorf("Block 101 has wrong hash after reorg.\nExpected: %s\nGot: %s",
			expectedHash, block101.Hash)
	}

	t.Logf("Reorg detection test passed: block 101 correctly points to reorged chain")
}

func TestReorgRetractReplace_Integration(t *testing.T) {
	_ = context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	detector := NewReorgDetector(DefaultReorgDetectorConfig(), logger)

	originalEvents := []*protov1.CanonicalEvent{
		{
			EventId:     "evt-101-0",
			Chain:       protov1.Chain_CHAIN_ETHEREUM,
			BlockNumber: 101,
			BlockHash:   "0xbbb101000000000000000000000000000000000000000000000000000000",
			TxHash:      "0xtx101_original",
			TxIndex:     0,
			EventIndex:  0,
			EventType:   "Transfer",
			ReorgAction: protov1.ReorgAction_REORG_ACTION_NORMAL,
		},
	}

	newEvents := []*protov1.CanonicalEvent{
		{
			EventId:     "evt-101-0-new",
			Chain:       protov1.Chain_CHAIN_ETHEREUM,
			BlockNumber: 101,
			BlockHash:   "0xccc101000000000000000000000000000000000000000000000000000000",
			TxHash:      "0xtx101_reorg",
			TxIndex:     0,
			EventIndex:  0,
			EventType:   "Transfer",
			ReorgAction: protov1.ReorgAction_REORG_ACTION_NORMAL,
		},
	}

	reorg := &ReorgEvent{
		Chain:          "ethereum",
		ForkPoint:      100,
		OrphanedBlocks: []uint64{101},
		OrphanedHashes: []string{"0xbbb101000000000000000000000000000000000000000000000000000000"},
		NewBlocks:      []uint64{101},
		NewHashes:      []string{"0xccc101000000000000000000000000000000000000000000000000000000"},
		Depth:          1,
	}

	retractions, replacements := detector.EmitReorgEvents(reorg, originalEvents, newEvents)

	if len(retractions) != 1 {
		t.Fatalf("Expected 1 retraction, got %d", len(retractions))
	}

	retraction := retractions[0]
	if retraction.ReorgAction != protov1.ReorgAction_REORG_ACTION_RETRACT {
		t.Errorf("Expected RETRACT action, got %v", retraction.ReorgAction)
	}
	if retraction.EventId != "evt-101-0:retract" {
		t.Errorf("Expected retraction ID 'evt-101-0:retract', got '%s'", retraction.EventId)
	}
	t.Logf("Retraction event: %s (action=%v)", retraction.EventId, retraction.ReorgAction)

	if len(replacements) != 1 {
		t.Fatalf("Expected 1 replacement, got %d", len(replacements))
	}

	replacement := replacements[0]
	if replacement.ReorgAction != protov1.ReorgAction_REORG_ACTION_REPLACE {
		t.Errorf("Expected REPLACE action, got %v", replacement.ReorgAction)
	}
	if replacement.EventId != "evt-101-0-new:replace" {
		t.Errorf("Expected replacement ID 'evt-101-0-new:replace', got '%s'", replacement.EventId)
	}
	t.Logf("Replacement event: %s (action=%v, replaces=%s)",
		replacement.EventId, replacement.ReorgAction, replacement.ReplacesEventId)

	t.Log("Retract/Replace event generation test passed")
}

func TestOutboxSequence_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping database integration test in short mode")
	}

	t.Log("Outbox sequence test would verify database integration")
	t.Log("Skipping: requires running database")
}

func findFixturesDir(t *testing.T) string {
	candidates := []string{
		"../../fixtures",
		"../../../fixtures",
		"fixtures",
	}

	for _, dir := range candidates {
		if info, err := os.Stat(dir); err == nil && info.IsDir() {
			absPath, _ := filepath.Abs(dir)
			t.Logf("Found fixtures at: %s", absPath)
			return absPath
		}
	}

	wd, _ := os.Getwd()
	t.Logf("Working directory: %s", wd)

	for dir := wd; dir != "/"; dir = filepath.Dir(dir) {
		fixturesPath := filepath.Join(dir, "fixtures")
		if info, err := os.Stat(fixturesPath); err == nil && info.IsDir() {
			return fixturesPath
		}
	}

	t.Fatal("Could not find fixtures directory")
	return ""
}
