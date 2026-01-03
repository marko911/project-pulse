# Phase 1 Testing & Verification Strategy

This document defines the testing layers and formal verification approaches for Milestone 1 (Ingestion & Canonical Stream MVP).

## Overview

Project Pulse's correctness guarantees require rigorous testing beyond standard unit tests. This strategy implements seven testing layers that together provide **provable data correctness**.

| Layer | Purpose | Run Frequency |
|-------|---------|---------------|
| Unit Tests | Function-level correctness | Every commit |
| Property-Based Tests | Invariant verification | Every commit |
| Fixture Replay | Real chain data validation | Every commit |
| Integration Tests | Full pipeline verification | PR merge |
| Reconciliation | Production data integrity | Continuous |
| Chaos Tests | Failure scenario handling | Weekly / pre-release |
| Dual-Source Verification | Provider independence | Daily (prod) |

---

## Layer 1: Unit Tests (Standard TDD)

### What to Test

- Normalizer functions (Solana raw → CanonicalEvent, EVM log → CanonicalEvent)
- Deterministic event ID generation
- Protobuf serialization/deserialization
- Individual adapter parsing logic
- Block hash and parent hash handling

### Tools

- Go's built-in `testing` package
- `github.com/stretchr/testify` for assertions

### Example

```go
package normalizer

import (
    "testing"
    "github.com/stretchr/testify/assert"
)

func TestEventID_Deterministic(t *testing.T) {
    event := CanonicalEvent{
        Chain:       "solana",
        BlockNumber: 12345,
        TxHash:      "abc123def456...",
        LogIndex:    0,
    }

    id1 := GenerateEventID(event)
    id2 := GenerateEventID(event)

    assert.Equal(t, id1, id2, "Event IDs must be deterministic")
}

func TestEventID_UniqueAcrossChains(t *testing.T) {
    solanaEvent := CanonicalEvent{Chain: "solana", BlockNumber: 100, TxHash: "abc", LogIndex: 0}
    ethEvent := CanonicalEvent{Chain: "ethereum", BlockNumber: 100, TxHash: "abc", LogIndex: 0}

    assert.NotEqual(t, GenerateEventID(solanaEvent), GenerateEventID(ethEvent),
        "Same tx on different chains must have different IDs")
}

func TestNormalize_SolanaTransaction(t *testing.T) {
    raw := loadTestFixture("solana_tx_simple.json")

    events, err := NormalizeSolanaTransaction(raw)

    assert.NoError(t, err)
    assert.Len(t, events, 3, "Expected 3 events from this transaction")
    assert.Equal(t, "solana", events[0].Chain)
    assert.Equal(t, uint64(250000000), events[0].BlockNumber)
}
```

### Coverage Requirements

- Target: >80% coverage for `internal/` packages
- Critical paths (event ordering, deduplication): 100% coverage required

---

## Layer 2: Recorded Fixture Replay

This is the **most critical testing layer** for correctness. Record real blockchain data, replay through adapters, and verify outputs match expected results.

### Directory Structure

```
fixtures/
├── solana/
│   ├── mainnet/
│   │   ├── block_250000000.json          # Raw Geyser response
│   │   ├── block_250000000.expected.json # Expected CanonicalEvents
│   │   ├── block_250000001.json
│   │   └── block_250000001.expected.json
│   ├── reorg_scenarios/
│   │   ├── simple_reorg/
│   │   │   ├── blocks_before.json        # Chain state before reorg
│   │   │   ├── blocks_after.json         # Chain state after reorg
│   │   │   └── expected_corrections.json # Expected tombstones + replacements
│   │   └── deep_reorg/
│   │       └── ...
│   └── edge_cases/
│       ├── empty_block.json
│       ├── max_instructions_block.json
│       └── failed_transaction.json
├── ethereum/
│   ├── mainnet/
│   │   ├── block_19000000.json
│   │   └── block_19000000.expected.json
│   └── edge_cases/
│       ├── empty_block.json
│       ├── reverted_tx_with_logs.json
│       └── max_logs_block.json
├── bsc/
├── avalanche/
├── arbitrum/
└── base/
```

### Fixture Recording Tool

Create a tool to capture fixtures from live chains:

```go
// cmd/fixture-recorder/main.go
package main

import (
    "encoding/json"
    "flag"
    "os"
)

func main() {
    chain := flag.String("chain", "solana", "Chain to record from")
    startBlock := flag.Uint64("start", 0, "Starting block/slot")
    count := flag.Int("count", 10, "Number of blocks to record")
    output := flag.String("output", "./fixtures", "Output directory")
    flag.Parse()

    recorder := NewFixtureRecorder(*chain, getEndpoint(*chain))

    for i := 0; i < *count; i++ {
        blockNum := *startBlock + uint64(i)

        // Record raw response
        raw, err := recorder.FetchRawBlock(blockNum)
        handleErr(err)
        writeJSON(*output, fmt.Sprintf("block_%d.json", blockNum), raw)

        // Generate expected output
        expected, err := recorder.NormalizeAndCapture(raw)
        handleErr(err)
        writeJSON(*output, fmt.Sprintf("block_%d.expected.json", blockNum), expected)
    }
}
```

### Replay Test Harness

```go
package replay_test

import (
    "encoding/json"
    "os"
    "path/filepath"
    "testing"

    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
)

func TestSolanaAdapter_ReplayAllFixtures(t *testing.T) {
    fixtures, err := filepath.Glob("../fixtures/solana/mainnet/block_*.json")
    require.NoError(t, err)
    require.NotEmpty(t, fixtures, "No fixtures found")

    adapter := NewSolanaAdapter(testConfig)

    for _, fixturePath := range fixtures {
        // Skip .expected.json files
        if strings.Contains(fixturePath, ".expected") {
            continue
        }

        t.Run(filepath.Base(fixturePath), func(t *testing.T) {
            // Load raw fixture
            raw := loadFixture(t, fixturePath)

            // Load expected output
            expectedPath := strings.Replace(fixturePath, ".json", ".expected.json", 1)
            expected := loadExpected(t, expectedPath)

            // Run through adapter
            actual, err := adapter.Normalize(raw)
            require.NoError(t, err)

            // Verify count matches
            assert.Equal(t, len(expected), len(actual), "Event count mismatch")

            // Verify each event matches exactly
            for i, exp := range expected {
                assert.Equal(t, exp.EventID, actual[i].EventID, "EventID mismatch at index %d", i)
                assert.Equal(t, exp.BlockHash, actual[i].BlockHash, "BlockHash mismatch at index %d", i)
                assert.Equal(t, exp.TxHash, actual[i].TxHash, "TxHash mismatch at index %d", i)
                assert.Equal(t, exp.LogIndex, actual[i].LogIndex, "LogIndex mismatch at index %d", i)
                assert.Equal(t, exp.EventType, actual[i].EventType, "EventType mismatch at index %d", i)
                // Compare full payload
                assert.JSONEq(t, string(exp.Payload), string(actual[i].Payload), "Payload mismatch at index %d", i)
            }
        })
    }
}

func TestReorgScenario_SimpleReorg(t *testing.T) {
    scenario := loadReorgScenario(t, "solana/reorg_scenarios/simple_reorg")

    adapter := NewSolanaAdapter(testConfig)

    // Process blocks before reorg
    eventsBefore, _ := adapter.ProcessBlocks(scenario.BlocksBefore)

    // Simulate reorg detection
    eventsAfter, corrections := adapter.ProcessReorg(scenario.BlocksBefore, scenario.BlocksAfter)

    // Verify corrections match expected
    assert.Equal(t, len(scenario.ExpectedCorrections.Tombstones), countTombstones(corrections))
    assert.Equal(t, len(scenario.ExpectedCorrections.Replacements), countReplacements(corrections))
}
```

---

## Layer 3: Property-Based Testing (Invariants)

Use property-based testing to verify invariants hold for **any valid input**, not just hand-picked test cases.

### Tools

- `github.com/flyingmutant/rapid` (recommended for Go)
- Alternative: `github.com/leanovate/gopter`

### Critical Invariants

| Invariant | Description | Priority |
|-----------|-------------|----------|
| **Determinism** | Same input → same event ID, always | P0 |
| **Ordering** | Events within block maintain log_index order | P0 |
| **Completeness** | Block with N logs → exactly N CanonicalEvents | P0 |
| **Parent Chain** | Block N's parent_hash == Block N-1's hash | P0 |
| **No Duplicates** | No two events share the same event_id | P0 |
| **Schema Validity** | All events pass Protobuf schema validation | P0 |
| **Timestamp Monotonicity** | Block timestamps are non-decreasing | P1 |

### Implementation

```go
package invariants_test

import (
    "testing"

    "pgregory.net/rapid"
)

// Generator for random valid events
func genCanonicalEvent(t *rapid.T) CanonicalEvent {
    return CanonicalEvent{
        Chain:       rapid.SampledFrom([]string{"solana", "ethereum", "bsc", "avalanche", "arbitrum", "base"}).Draw(t, "chain"),
        BlockNumber: rapid.Uint64Range(1, 1_000_000_000).Draw(t, "blockNumber"),
        BlockHash:   rapid.StringMatching(`[a-f0-9]{64}`).Draw(t, "blockHash"),
        TxHash:      rapid.StringMatching(`[a-f0-9]{64}`).Draw(t, "txHash"),
        LogIndex:    rapid.Uint32Range(0, 10000).Draw(t, "logIndex"),
        EventType:   rapid.SampledFrom([]string{"transfer", "swap", "mint", "burn"}).Draw(t, "eventType"),
        Commitment:  rapid.SampledFrom([]string{"processed", "confirmed", "finalized"}).Draw(t, "commitment"),
        Timestamp:   rapid.Int64Range(1600000000, 2000000000).Draw(t, "timestamp"),
    }
}

func TestProperty_EventIDDeterminism(t *testing.T) {
    rapid.Check(t, func(t *rapid.T) {
        event := genCanonicalEvent(t)

        id1 := GenerateEventID(event)
        id2 := GenerateEventID(event)

        if id1 != id2 {
            t.Fatalf("Non-deterministic ID generation: %s != %s for event %+v", id1, id2, event)
        }
    })
}

func TestProperty_EventIDUniqueness(t *testing.T) {
    rapid.Check(t, func(t *rapid.T) {
        event1 := genCanonicalEvent(t)
        event2 := genCanonicalEvent(t)

        // If any field differs, IDs must differ
        if event1.Chain != event2.Chain ||
           event1.BlockNumber != event2.BlockNumber ||
           event1.TxHash != event2.TxHash ||
           event1.LogIndex != event2.LogIndex {

            id1 := GenerateEventID(event1)
            id2 := GenerateEventID(event2)

            if id1 == id2 {
                t.Fatalf("ID collision: %s for different events:\n%+v\n%+v", id1, event1, event2)
            }
        }
    })
}

func TestProperty_BlockOrderingPreserved(t *testing.T) {
    rapid.Check(t, func(t *rapid.T) {
        // Generate a sequence of events from same block
        blockNum := rapid.Uint64Range(1, 1_000_000).Draw(t, "blockNum")
        numEvents := rapid.IntRange(1, 100).Draw(t, "numEvents")

        events := make([]CanonicalEvent, numEvents)
        for i := 0; i < numEvents; i++ {
            events[i] = genCanonicalEvent(t)
            events[i].BlockNumber = blockNum
            events[i].LogIndex = uint32(i)
        }

        // Shuffle and sort
        shuffled := shuffleEvents(events)
        sorted := SortEventsByLogIndex(shuffled)

        // Verify order restored
        for i := 0; i < len(sorted)-1; i++ {
            if sorted[i].LogIndex >= sorted[i+1].LogIndex {
                t.Fatalf("Ordering violated: event %d (log_index=%d) >= event %d (log_index=%d)",
                    i, sorted[i].LogIndex, i+1, sorted[i+1].LogIndex)
            }
        }
    })
}

func TestProperty_ParentChainContinuity(t *testing.T) {
    rapid.Check(t, func(t *rapid.T) {
        numBlocks := rapid.IntRange(2, 50).Draw(t, "numBlocks")

        blocks := generateValidBlockChain(t, numBlocks)

        // Verify parent hash chain
        for i := 1; i < len(blocks); i++ {
            if blocks[i].ParentHash != blocks[i-1].BlockHash {
                t.Fatalf("Chain break at block %d: parent=%s, prev_hash=%s",
                    blocks[i].BlockNumber, blocks[i].ParentHash, blocks[i-1].BlockHash)
            }
        }
    })
}
```

---

## Layer 4: Integration Tests (Docker Compose)

Test the full pipeline with real containers but controlled inputs.

### Test Environment

```yaml
# docker-compose.test.yml
version: '3.8'

services:
  redpanda:
    image: redpandadata/redpanda:v24.1.1
    command:
      - redpanda start
      - --smp 1
      - --memory 1G
      - --overprovisioned
      - --node-id 0
      - --kafka-addr PLAINTEXT://0.0.0.0:9092
    ports:
      - "9092:9092"
    healthcheck:
      test: ["CMD", "rpk", "cluster", "health"]
      interval: 5s
      timeout: 3s
      retries: 5

  timescaledb:
    image: timescale/timescaledb:latest-pg15
    environment:
      POSTGRES_USER: pulse
      POSTGRES_PASSWORD: pulse_test
      POSTGRES_DB: pulse_test
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U pulse"]
      interval: 5s
      timeout: 3s
      retries: 5

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 5

  # Mock Solana Geyser that replays fixtures
  mock-geyser:
    build:
      context: ./test/mock-geyser
    volumes:
      - ./fixtures/solana:/fixtures:ro
    environment:
      FIXTURE_DIR: /fixtures/mainnet
      REPLAY_SPEED: 10  # 10x speed for faster tests
    ports:
      - "10000:10000"

  # Mock EVM RPC that replays fixtures
  mock-evm:
    build:
      context: ./test/mock-evm
    volumes:
      - ./fixtures/ethereum:/fixtures:ro
    environment:
      FIXTURE_DIR: /fixtures/mainnet
      CHAIN_ID: 1
    ports:
      - "8545:8545"
```

### Integration Test Implementation

```go
package integration_test

import (
    "context"
    "database/sql"
    "testing"
    "time"

    "github.com/stretchr/testify/require"
    "github.com/testcontainers/testcontainers-go/modules/compose"
)

type TestContext struct {
    Compose *compose.ComposeStack
    DB      *sql.DB
    Redis   *redis.Client
}

func setupTestEnvironment(t *testing.T) *TestContext {
    ctx := context.Background()

    comp, err := compose.NewDockerCompose("docker-compose.test.yml")
    require.NoError(t, err)

    err = comp.Up(ctx, compose.Wait(true))
    require.NoError(t, err)

    t.Cleanup(func() {
        comp.Down(ctx)
    })

    // Connect to TimescaleDB
    db, err := sql.Open("postgres", "postgres://pulse:pulse_test@localhost:5432/pulse_test?sslmode=disable")
    require.NoError(t, err)

    // Connect to Redis
    rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

    return &TestContext{Compose: comp, DB: db, Redis: rdb}
}

func TestIntegration_SolanaFullPipeline(t *testing.T) {
    if testing.Short() {
        t.Skip("Skipping integration test in short mode")
    }

    ctx := setupTestEnvironment(t)

    // Start Solana adapter pointing to mock-geyser
    adapter := startSolanaAdapter(t, "localhost:10000")
    defer adapter.Stop()

    // Start processor
    processor := startProcessor(t)
    defer processor.Stop()

    // Wait for events to flow through pipeline
    require.Eventually(t, func() bool {
        count := queryEventCount(ctx.DB, "solana")
        return count >= 100  // Expect at least 100 events from fixtures
    }, 30*time.Second, 500*time.Millisecond, "Events did not flow through pipeline")

    // Verify against expected
    events := queryEvents(ctx.DB, "solana", 250000000, 250000010)
    expected := loadExpectedIntegration(t, "solana/integration_expected.json")

    require.Equal(t, len(expected), len(events), "Event count mismatch")

    for i, exp := range expected {
        require.Equal(t, exp.EventID, events[i].EventID, "EventID mismatch at %d", i)
    }
}

func TestIntegration_NoGapsNoDuplicates(t *testing.T) {
    if testing.Short() {
        t.Skip("Skipping integration test in short mode")
    }

    ctx := setupTestEnvironment(t)

    // Run full pipeline
    runFullPipeline(t, ctx, 60*time.Second)

    // Check for gaps
    gaps := findGaps(ctx.DB, "solana")
    require.Empty(t, gaps, "Found gaps in block sequence: %v", gaps)

    // Check for duplicates
    duplicates := findDuplicates(ctx.DB)
    require.Empty(t, duplicates, "Found duplicate event IDs: %v", duplicates)
}
```

---

## Layer 5: Correctness Proofs (Manifests + Reconciliation)

This layer provides **formal mathematical proof** of data integrity for finalized data.

### Manifest Structure

```go
// internal/manifest/types.go
package manifest

import "time"

// BlockManifest represents a cryptographic commitment to block contents
type BlockManifest struct {
    // Identity
    Chain       string `json:"chain"`
    BlockNumber uint64 `json:"block_number"`
    BlockHash   string `json:"block_hash"`
    ParentHash  string `json:"parent_hash"`

    // Counts (for Tier 2 reconciliation)
    TxCount          uint32 `json:"tx_count"`
    LogCount         uint32 `json:"log_count"`         // EVM only
    InstructionCount uint32 `json:"instruction_count"` // Solana only
    EventCount       uint32 `json:"event_count"`       // Our canonical events

    // Cryptographic proofs (for Tier 3 reconciliation)
    EventIDsHash    string `json:"event_ids_hash"`    // SHA256 of sorted event IDs
    EventPayloadHash string `json:"event_payload_hash"` // SHA256 of all payloads

    // Metadata
    Commitment   string    `json:"commitment"`    // processed/confirmed/finalized
    IngestedAt   time.Time `json:"ingested_at"`
    FinalizedAt  time.Time `json:"finalized_at,omitempty"`

    // Reconciliation status
    ReconciliationStatus string    `json:"reconciliation_status"` // pending/verified/suspect
    LastReconciled       time.Time `json:"last_reconciled,omitempty"`
}

// ComputeEventIDsHash generates deterministic hash of event IDs
func ComputeEventIDsHash(events []CanonicalEvent) string {
    // Sort by event ID for determinism
    sorted := make([]string, len(events))
    for i, e := range events {
        sorted[i] = e.EventID
    }
    sort.Strings(sorted)

    // Hash concatenated IDs
    h := sha256.New()
    for _, id := range sorted {
        h.Write([]byte(id))
    }
    return hex.EncodeToString(h.Sum(nil))
}
```

### Three-Tier Reconciliation

```go
// internal/reconciliation/reconciler.go
package reconciliation

import (
    "context"
    "fmt"
)

type Reconciler struct {
    db           *sql.DB
    rpcProviders map[string]RPCProvider // Independent RPC sources
    metrics      *ReconciliationMetrics
}

// Tier1_HeaderContinuity verifies parent hash chain is unbroken
// Cost: Cheap (local DB query only)
// Run: Continuously
func (r *Reconciler) Tier1_HeaderContinuity(ctx context.Context, chain string, fromBlock, toBlock uint64) error {
    manifests, err := r.loadManifests(ctx, chain, fromBlock, toBlock)
    if err != nil {
        return fmt.Errorf("load manifests: %w", err)
    }

    for i := 1; i < len(manifests); i++ {
        prev := manifests[i-1]
        curr := manifests[i]

        // Verify sequence
        if curr.BlockNumber != prev.BlockNumber+1 {
            return &ReconciliationError{
                Tier:    1,
                Type:    "gap",
                Chain:   chain,
                Block:   prev.BlockNumber + 1,
                Message: fmt.Sprintf("missing block between %d and %d", prev.BlockNumber, curr.BlockNumber),
            }
        }

        // Verify parent hash
        if curr.ParentHash != prev.BlockHash {
            return &ReconciliationError{
                Tier:    1,
                Type:    "chain_break",
                Chain:   chain,
                Block:   curr.BlockNumber,
                Message: fmt.Sprintf("parent hash mismatch: expected %s, got %s", prev.BlockHash, curr.ParentHash),
            }
        }
    }

    r.metrics.Tier1Verified.WithLabelValues(chain).Add(float64(len(manifests)))
    return nil
}

// Tier2_CountMatching verifies counts against independent RPC source
// Cost: Medium (requires RPC calls)
// Run: Hourly or on-demand
func (r *Reconciler) Tier2_CountMatching(ctx context.Context, chain string, blockNum uint64) error {
    manifest, err := r.loadManifest(ctx, chain, blockNum)
    if err != nil {
        return err
    }

    // Get block from independent source
    provider := r.rpcProviders[chain]
    block, err := provider.GetBlock(ctx, blockNum)
    if err != nil {
        return fmt.Errorf("fetch independent block: %w", err)
    }

    // Verify transaction count
    if block.TxCount != manifest.TxCount {
        return &ReconciliationError{
            Tier:    2,
            Type:    "tx_count_mismatch",
            Chain:   chain,
            Block:   blockNum,
            Message: fmt.Sprintf("tx count: expected %d, got %d", block.TxCount, manifest.TxCount),
        }
    }

    // Verify log/instruction count (chain-specific)
    if chain == "solana" {
        if block.InstructionCount != manifest.InstructionCount {
            return &ReconciliationError{
                Tier:    2,
                Type:    "instruction_count_mismatch",
                Chain:   chain,
                Block:   blockNum,
                Message: fmt.Sprintf("instruction count: expected %d, got %d", block.InstructionCount, manifest.InstructionCount),
            }
        }
    } else {
        if block.LogCount != manifest.LogCount {
            return &ReconciliationError{
                Tier:    2,
                Type:    "log_count_mismatch",
                Chain:   chain,
                Block:   blockNum,
                Message: fmt.Sprintf("log count: expected %d, got %d", block.LogCount, manifest.LogCount),
            }
        }
    }

    r.metrics.Tier2Verified.WithLabelValues(chain).Inc()
    return nil
}

// Tier3_HashReconstruction fully reconstructs and verifies event hashes
// Cost: Expensive (requires full event reconstruction)
// Run: Sampled (1% of blocks) or on-demand for suspect blocks
func (r *Reconciler) Tier3_HashReconstruction(ctx context.Context, chain string, blockNum uint64) error {
    manifest, err := r.loadManifest(ctx, chain, blockNum)
    if err != nil {
        return err
    }

    // Load all events for this block
    events, err := r.loadEvents(ctx, chain, blockNum)
    if err != nil {
        return err
    }

    // Verify event count
    if len(events) != int(manifest.EventCount) {
        return &ReconciliationError{
            Tier:    3,
            Type:    "event_count_mismatch",
            Chain:   chain,
            Block:   blockNum,
            Message: fmt.Sprintf("event count: expected %d, got %d", manifest.EventCount, len(events)),
        }
    }

    // Reconstruct and verify event IDs hash
    computedHash := ComputeEventIDsHash(events)
    if computedHash != manifest.EventIDsHash {
        return &ReconciliationError{
            Tier:    3,
            Type:    "event_hash_mismatch",
            Chain:   chain,
            Block:   blockNum,
            Message: fmt.Sprintf("event IDs hash: expected %s, got %s", manifest.EventIDsHash, computedHash),
        }
    }

    r.metrics.Tier3Verified.WithLabelValues(chain).Inc()
    return nil
}
```

### Watermark Halt Policy

```go
// internal/reconciliation/watermark.go
package reconciliation

// WatermarkManager controls the finalized watermark advancement
type WatermarkManager struct {
    db      *sql.DB
    alerter Alerter
}

// AdvanceWatermark attempts to advance the finalized watermark
// Returns error and HALTS if any blocks are suspect
func (w *WatermarkManager) AdvanceWatermark(ctx context.Context, chain string, targetBlock uint64) error {
    currentWatermark, err := w.getCurrentWatermark(ctx, chain)
    if err != nil {
        return err
    }

    // Check for suspect blocks in range
    suspectBlocks, err := w.findSuspectBlocks(ctx, chain, currentWatermark, targetBlock)
    if err != nil {
        return err
    }

    if len(suspectBlocks) > 0 {
        // HALT - do not advance watermark
        w.alerter.Alert(AlertCritical, "Watermark halted due to suspect blocks", map[string]interface{}{
            "chain":          chain,
            "current":        currentWatermark,
            "target":         targetBlock,
            "suspect_blocks": suspectBlocks,
            "runbook":        "https://runbooks.internal/correctness-incident",
        })

        return &WatermarkHaltError{
            Chain:         chain,
            Current:       currentWatermark,
            Target:        targetBlock,
            SuspectBlocks: suspectBlocks,
        }
    }

    // Safe to advance
    return w.setWatermark(ctx, chain, targetBlock)
}
```

---

## Layer 6: Chaos/Fault Injection Tests

Test failure scenarios to verify exactly-once delivery and fail-closed behavior.

### Scenarios

| Scenario | What It Tests | Expected Behavior |
|----------|---------------|-------------------|
| Adapter crash mid-block | Exactly-once, no partial blocks | Resume from last committed offset |
| Redpanda partition unavailable | Backpressure, retry logic | Buffer and retry, no data loss |
| TimescaleDB connection drop | Transactional outbox integrity | Outbox preserves uncommitted events |
| Network latency spike | Timeout handling | Graceful degradation |
| Clock skew | Timestamp handling | Events ordered by chain timestamp, not local |
| Processor OOM kill | Recovery without data loss | Resume from last checkpoint |

### Implementation

```go
package chaos_test

import (
    "context"
    "testing"
    "time"

    "github.com/docker/docker/api/types/container"
    "github.com/docker/docker/client"
)

func TestChaos_AdapterCrashMidBlock(t *testing.T) {
    if testing.Short() {
        t.Skip("Skipping chaos test in short mode")
    }

    ctx := setupTestEnvironment(t)
    docker, _ := client.NewClientWithOpts(client.FromEnv)

    // Start pipeline
    adapter := startSolanaAdapter(t, ctx)
    processor := startProcessor(t, ctx)

    // Let it process some blocks
    waitForBlocks(ctx, 5)

    // Get current event count
    countBefore := queryEventCount(ctx.DB, "solana")

    // CHAOS: Kill adapter container abruptly
    err := docker.ContainerKill(context.Background(), "adapter-solana", "SIGKILL")
    require.NoError(t, err)

    // Wait a moment
    time.Sleep(2 * time.Second)

    // Restart adapter
    adapter = startSolanaAdapter(t, ctx)

    // Let it recover and process more blocks
    waitForBlocks(ctx, 10)

    // VERIFY: No gaps, no duplicates
    gaps := findGaps(ctx.DB, "solana")
    require.Empty(t, gaps, "Found gaps after crash recovery: %v", gaps)

    duplicates := findDuplicates(ctx.DB)
    require.Empty(t, duplicates, "Found duplicates after crash recovery: %v", duplicates)

    // Verify event count increased (processing continued)
    countAfter := queryEventCount(ctx.DB, "solana")
    require.Greater(t, countAfter, countBefore, "No new events after recovery")
}

func TestChaos_RedpandaPartitionUnavailable(t *testing.T) {
    ctx := setupTestEnvironment(t)

    // Start pipeline
    startFullPipeline(t, ctx)
    waitForBlocks(ctx, 5)

    // CHAOS: Pause Redpanda container (simulates network partition)
    pauseContainer(t, "redpanda")

    // Adapter should buffer and not crash
    time.Sleep(10 * time.Second)
    require.True(t, isContainerRunning(t, "adapter-solana"), "Adapter crashed during Redpanda outage")

    // Resume Redpanda
    unpauseContainer(t, "redpanda")

    // Wait for recovery
    time.Sleep(10 * time.Second)

    // Verify no data loss
    gaps := findGaps(ctx.DB, "solana")
    require.Empty(t, gaps, "Found gaps after Redpanda recovery")
}

func TestChaos_TimescaleConnectionDrop(t *testing.T) {
    ctx := setupTestEnvironment(t)

    // Start pipeline
    startFullPipeline(t, ctx)
    waitForBlocks(ctx, 5)

    countBefore := queryEventCount(ctx.DB, "solana")

    // CHAOS: Drop all DB connections
    forceDisconnectDB(t, ctx)

    // Let events accumulate in outbox
    time.Sleep(10 * time.Second)

    // Restore DB
    restoreDBConnections(t, ctx)

    // Wait for outbox to drain
    time.Sleep(10 * time.Second)

    // Verify outbox is empty and events were delivered
    outboxCount := queryOutboxCount(ctx.DB)
    require.Zero(t, outboxCount, "Outbox not drained after DB recovery")

    countAfter := queryEventCount(ctx.DB, "solana")
    require.Greater(t, countAfter, countBefore, "Events lost during DB outage")
}

// Network latency injection using tc (traffic control)
func TestChaos_NetworkLatencySpike(t *testing.T) {
    ctx := setupTestEnvironment(t)

    // Start pipeline
    startFullPipeline(t, ctx)

    // Measure baseline latency
    baselineP99 := measureP99Latency(ctx, 10*time.Second)

    // CHAOS: Add 500ms latency to Redpanda
    execInContainer(t, "redpanda", []string{
        "tc", "qdisc", "add", "dev", "eth0", "root", "netem", "delay", "500ms",
    })

    // Measure latency under stress
    stressP99 := measureP99Latency(ctx, 10*time.Second)

    // Remove latency
    execInContainer(t, "redpanda", []string{
        "tc", "qdisc", "del", "dev", "eth0", "root",
    })

    // Verify no data loss despite latency
    gaps := findGaps(ctx.DB, "solana")
    require.Empty(t, gaps, "Found gaps during latency spike")

    // Log latency impact for analysis
    t.Logf("Baseline P99: %v, Stress P99: %v", baselineP99, stressP99)
}
```

---

## Layer 7: Dual-Source Divergence Detection

Run adapters against two independent RPC providers and verify outputs are identical.

```go
package dualsource_test

import (
    "testing"

    "github.com/stretchr/testify/require"
)

func TestDualSource_SolanaProviderAgreement(t *testing.T) {
    if testing.Short() {
        t.Skip("Skipping dual-source test in short mode")
    }

    // Block range to verify
    fromBlock := uint64(250000000)
    toBlock := uint64(250000100)

    // Run adapter with Provider A (e.g., Helius)
    eventsA := runSolanaAdapter(t, Config{
        Provider: "helius",
        Endpoint: os.Getenv("SOLANA_HELIUS_ENDPOINT"),
    }, fromBlock, toBlock)

    // Run adapter with Provider B (e.g., Triton)
    eventsB := runSolanaAdapter(t, Config{
        Provider: "triton",
        Endpoint: os.Getenv("SOLANA_TRITON_ENDPOINT"),
    }, fromBlock, toBlock)

    // Verify identical output
    require.Equal(t, len(eventsA), len(eventsB),
        "Event count differs between providers: Helius=%d, Triton=%d",
        len(eventsA), len(eventsB))

    for i := range eventsA {
        require.Equal(t, eventsA[i].EventID, eventsB[i].EventID,
            "EventID divergence at index %d:\nHelius: %s\nTriton: %s",
            i, eventsA[i].EventID, eventsB[i].EventID)
    }

    t.Logf("Verified %d events match across providers", len(eventsA))
}

func TestDualSource_EVMProviderAgreement(t *testing.T) {
    // Similar test for EVM chains with Alchemy vs QuickNode
    // ...
}
```

---

## Running Tests

### Makefile Targets

```makefile
# Run all unit tests
test-unit:
	go test -v -short ./...

# Run property-based tests
test-property:
	go test -v -run "TestProperty" ./...

# Run fixture replay tests
test-fixtures:
	go test -v -run "TestReplay" ./internal/adapter/...

# Run integration tests (requires Docker)
test-integration:
	docker-compose -f docker-compose.test.yml up -d
	go test -v -tags=integration ./test/integration/...
	docker-compose -f docker-compose.test.yml down

# Run chaos tests (requires Docker)
test-chaos:
	docker-compose -f docker-compose.test.yml up -d
	go test -v -tags=chaos -timeout=30m ./test/chaos/...
	docker-compose -f docker-compose.test.yml down

# Run all tests
test-all: test-unit test-property test-fixtures test-integration

# Generate coverage report
coverage:
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
```

### CI Pipeline

```yaml
# .github/workflows/test.yml
name: Tests

on: [push, pull_request]

jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: '1.22'
      - run: make test-unit
      - run: make test-property
      - run: make test-fixtures

  integration-tests:
    runs-on: ubuntu-latest
    needs: unit-tests
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: '1.22'
      - run: make test-integration

  chaos-tests:
    runs-on: ubuntu-latest
    needs: integration-tests
    if: github.ref == 'refs/heads/main'
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: '1.22'
      - run: make test-chaos
```

---

## Correctness Claim

After implementing all testing layers, Project Pulse can make the following verifiable claim:

> **Correctness Guarantee for Finalized Data**
>
> For any block range [A..B] in the finalized stream where `reconciliation_status = "verified"`:
>
> 1. **Header Continuity:** Parent hash chain is unbroken (Tier 1)
> 2. **Count Matching:** Event counts match independent RPC source (Tier 2)
> 3. **Hash Verification:** Event ID hash matches reconstructed hash (Tier 3, sampled)
> 4. **No Gaps:** No missing blocks in sequence
> 5. **No Duplicates:** No duplicate event IDs
> 6. **Schema Validity:** All events pass Protobuf schema validation
>
> This constitutes a **mathematical proof of data completeness and correctness** backed by:
> - Cryptographic commitments (SHA-256 hashes in manifests)
> - Independent verification (dual-source reconciliation)
> - Continuous monitoring (real-time gap detection)
>
> Verification artifacts are available via:
> - `GET /api/v1/manifests?chain=<chain>&from=<block>&to=<block>`
> - `GET /api/v1/reconciliation/status?chain=<chain>`
> - `GET /health/correctness`

---

## Summary

| Test Layer | Proves | Automation |
|------------|--------|------------|
| Unit Tests | Functions work correctly | CI on every commit |
| Property Tests | Invariants hold for all inputs | CI on every commit |
| Fixture Replay | Real chain data normalizes correctly | CI on every commit |
| Integration | Full pipeline works end-to-end | CI on PR merge |
| Reconciliation | Production data matches chain state | Continuous in prod |
| Chaos Tests | System survives failures gracefully | Weekly / pre-release |
| Dual-Source | No provider-specific bugs | Daily in prod |
