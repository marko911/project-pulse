package main

import (
	"testing"
	"time"

	protov1 "github.com/mirador/pulse/pkg/proto/v1"
)

func TestComputeEventIdsHash_Empty(t *testing.T) {
	b := &Builder{}
	hash := b.computeEventIdsHash([]*protov1.CanonicalEvent{})

	if hash == "" {
		t.Error("expected non-empty hash for empty events")
	}
	if len(hash) < 10 {
		t.Errorf("expected sha256 prefixed hash, got %s", hash)
	}
	if hash[:7] != "sha256:" {
		t.Errorf("expected sha256: prefix, got %s", hash[:7])
	}
}

func TestComputeEventIdsHash_Deterministic(t *testing.T) {
	b := &Builder{}

	events := []*protov1.CanonicalEvent{
		{EventId: "event-1"},
		{EventId: "event-2"},
		{EventId: "event-3"},
	}

	hash1 := b.computeEventIdsHash(events)
	hash2 := b.computeEventIdsHash(events)

	if hash1 != hash2 {
		t.Errorf("expected deterministic hash, got %s and %s", hash1, hash2)
	}
}

func TestComputeEventIdsHash_OrderMatters(t *testing.T) {
	b := &Builder{}

	events1 := []*protov1.CanonicalEvent{
		{EventId: "event-1"},
		{EventId: "event-2"},
	}
	events2 := []*protov1.CanonicalEvent{
		{EventId: "event-2"},
		{EventId: "event-1"},
	}

	hash1 := b.computeEventIdsHash(events1)
	hash2 := b.computeEventIdsHash(events2)

	if hash1 == hash2 {
		t.Error("expected different hashes for different order")
	}
}

func TestBuildManifest(t *testing.T) {
	b := &Builder{}

	now := time.Now().UTC()
	agg := &blockAggregator{
		chain:          protov1.Chain_CHAIN_ETHEREUM,
		blockNumber:    12345,
		blockHash:      "0xabc123",
		blockTimestamp: now,
		events: []*protov1.CanonicalEvent{
			{EventId: "evt-1", TxHash: "tx-1", TxIndex: 0, EventIndex: 0},
			{EventId: "evt-2", TxHash: "tx-1", TxIndex: 0, EventIndex: 1},
			{EventId: "evt-3", TxHash: "tx-2", TxIndex: 1, EventIndex: 0},
		},
		txSet: map[string]bool{
			"tx-1": true,
			"tx-2": true,
		},
	}

	manifest := b.buildManifest(agg)

	if manifest.Chain != protov1.Chain_CHAIN_ETHEREUM {
		t.Errorf("expected Chain ETHEREUM, got %v", manifest.Chain)
	}
	if manifest.BlockNumber != 12345 {
		t.Errorf("expected BlockNumber 12345, got %d", manifest.BlockNumber)
	}
	if manifest.BlockHash != "0xabc123" {
		t.Errorf("expected BlockHash 0xabc123, got %s", manifest.BlockHash)
	}
	if manifest.EmittedTxCount != 2 {
		t.Errorf("expected EmittedTxCount 2, got %d", manifest.EmittedTxCount)
	}
	if manifest.EmittedEventCount != 3 {
		t.Errorf("expected EmittedEventCount 3, got %d", manifest.EmittedEventCount)
	}
	if manifest.EventIdsHash == "" {
		t.Error("expected non-empty EventIdsHash")
	}
	if len(manifest.SourcesUsed) != 1 || manifest.SourcesUsed[0] != "kafka-consumer" {
		t.Errorf("expected SourcesUsed [kafka-consumer], got %v", manifest.SourcesUsed)
	}
}

func TestBuildManifest_EventsSortedByTxAndEventIndex(t *testing.T) {
	b := &Builder{}

	// Events out of order
	agg := &blockAggregator{
		chain:          protov1.Chain_CHAIN_SOLANA,
		blockNumber:    999,
		blockHash:      "hash",
		blockTimestamp: time.Now(),
		events: []*protov1.CanonicalEvent{
			{EventId: "evt-3", TxIndex: 1, EventIndex: 0},
			{EventId: "evt-1", TxIndex: 0, EventIndex: 0},
			{EventId: "evt-2", TxIndex: 0, EventIndex: 1},
		},
		txSet: map[string]bool{"tx": true},
	}

	manifest := b.buildManifest(agg)

	// The hash should be deterministic regardless of input order
	// because buildManifest sorts events before hashing

	agg2 := &blockAggregator{
		chain:          protov1.Chain_CHAIN_SOLANA,
		blockNumber:    999,
		blockHash:      "hash",
		blockTimestamp: time.Now(),
		events: []*protov1.CanonicalEvent{
			{EventId: "evt-1", TxIndex: 0, EventIndex: 0},
			{EventId: "evt-2", TxIndex: 0, EventIndex: 1},
			{EventId: "evt-3", TxIndex: 1, EventIndex: 0},
		},
		txSet: map[string]bool{"tx": true},
	}

	manifest2 := b.buildManifest(agg2)

	if manifest.EventIdsHash != manifest2.EventIdsHash {
		t.Errorf("expected same hash for same events in different order, got %s and %s",
			manifest.EventIdsHash, manifest2.EventIdsHash)
	}
}

func TestBlockKey_Equality(t *testing.T) {
	key1 := blockKey{chain: protov1.Chain_CHAIN_ETHEREUM, blockNumber: 100}
	key2 := blockKey{chain: protov1.Chain_CHAIN_ETHEREUM, blockNumber: 100}
	key3 := blockKey{chain: protov1.Chain_CHAIN_SOLANA, blockNumber: 100}
	key4 := blockKey{chain: protov1.Chain_CHAIN_ETHEREUM, blockNumber: 101}

	if key1 != key2 {
		t.Error("expected same keys to be equal")
	}
	if key1 == key3 {
		t.Error("expected different chains to be unequal")
	}
	if key1 == key4 {
		t.Error("expected different block numbers to be unequal")
	}
}
