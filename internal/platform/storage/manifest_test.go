package storage

import (
	"encoding/json"
	"testing"
	"time"

	protov1 "github.com/marko911/project-pulse/pkg/proto/v1"
)

func TestManifestRecord_ToProto(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	sourcesJSON, _ := json.Marshal([]string{"rpc-primary", "rpc-secondary"})

	record := &ManifestRecord{
		ID:                 1,
		Chain:              int16(protov1.Chain_CHAIN_ETHEREUM),
		BlockNumber:        12345678,
		BlockHash:          "0xabc123",
		ParentHash:         "0xdef456",
		ExpectedTxCount:    100,
		ExpectedEventCount: 250,
		EmittedTxCount:     100,
		EmittedEventCount:  250,
		EventIdsHash:       "sha256:abcdef",
		SourcesUsed:        sourcesJSON,
		BlockTimestamp:     now,
		IngestedAt:         now,
		ManifestCreatedAt:  now,
	}

	proto, err := record.ToProto()
	if err != nil {
		t.Fatalf("ToProto failed: %v", err)
	}

	if proto.Chain != protov1.Chain_CHAIN_ETHEREUM {
		t.Errorf("expected Chain ETHEREUM, got %v", proto.Chain)
	}
	if proto.BlockNumber != 12345678 {
		t.Errorf("expected BlockNumber 12345678, got %d", proto.BlockNumber)
	}
	if proto.BlockHash != "0xabc123" {
		t.Errorf("expected BlockHash 0xabc123, got %s", proto.BlockHash)
	}
	if proto.ParentHash != "0xdef456" {
		t.Errorf("expected ParentHash 0xdef456, got %s", proto.ParentHash)
	}
	if proto.ExpectedTxCount != 100 {
		t.Errorf("expected ExpectedTxCount 100, got %d", proto.ExpectedTxCount)
	}
	if proto.ExpectedEventCount != 250 {
		t.Errorf("expected ExpectedEventCount 250, got %d", proto.ExpectedEventCount)
	}
	if proto.EmittedTxCount != 100 {
		t.Errorf("expected EmittedTxCount 100, got %d", proto.EmittedTxCount)
	}
	if proto.EmittedEventCount != 250 {
		t.Errorf("expected EmittedEventCount 250, got %d", proto.EmittedEventCount)
	}
	if proto.EventIdsHash != "sha256:abcdef" {
		t.Errorf("expected EventIdsHash sha256:abcdef, got %s", proto.EventIdsHash)
	}
	if len(proto.SourcesUsed) != 2 {
		t.Errorf("expected 2 sources, got %d", len(proto.SourcesUsed))
	}
	if proto.SourcesUsed[0] != "rpc-primary" {
		t.Errorf("expected first source rpc-primary, got %s", proto.SourcesUsed[0])
	}
}

func TestManifestRecord_ToProto_EmptySources(t *testing.T) {
	sourcesJSON, _ := json.Marshal([]string{})

	record := &ManifestRecord{
		ID:                 1,
		Chain:              int16(protov1.Chain_CHAIN_SOLANA),
		BlockNumber:        999,
		BlockHash:          "0x123",
		ParentHash:         "0x456",
		ExpectedTxCount:    0,
		ExpectedEventCount: 0,
		EmittedTxCount:     0,
		EmittedEventCount:  0,
		EventIdsHash:       "",
		SourcesUsed:        sourcesJSON,
		BlockTimestamp:     time.Now(),
		IngestedAt:         time.Now(),
		ManifestCreatedAt:  time.Now(),
	}

	proto, err := record.ToProto()
	if err != nil {
		t.Fatalf("ToProto failed: %v", err)
	}

	if len(proto.SourcesUsed) != 0 {
		t.Errorf("expected 0 sources, got %d", len(proto.SourcesUsed))
	}
}

func TestManifestRecord_ToProto_InvalidJSON(t *testing.T) {
	record := &ManifestRecord{
		SourcesUsed: []byte("invalid json"),
	}

	_, err := record.ToProto()
	if err == nil {
		t.Error("expected error for invalid JSON, got nil")
	}
}

func TestManifestRecord_Mismatch(t *testing.T) {
	// Test detecting correctness mismatch
	record := &ManifestRecord{
		ExpectedTxCount:    100,
		ExpectedEventCount: 250,
		EmittedTxCount:     99, // Mismatch!
		EmittedEventCount:  250,
	}

	hasTxMismatch := record.ExpectedTxCount != record.EmittedTxCount
	hasEventMismatch := record.ExpectedEventCount != record.EmittedEventCount

	if !hasTxMismatch {
		t.Error("expected tx mismatch to be detected")
	}
	if hasEventMismatch {
		t.Error("expected no event mismatch")
	}
}
