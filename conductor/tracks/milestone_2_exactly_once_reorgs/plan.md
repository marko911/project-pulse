# Track Plan: Milestone 2 - Exactly-Once & Reorg Corrections

## Phase 1: Test Infrastructure (Simulators)
- [ ] Task: Fixture Recorder Tool
    - [ ] Subtask: Create `cmd/fixture-recorder/` to fetch blocks/logs from live chains and save to JSON.
    - [ ] Subtask: Implement fetching logic for Solana (GetBlock) and EVM (GetBlock/GetLogs).
- [ ] Task: Replay Harness Interfaces
    - [ ] Subtask: Refactor Adapters to use a `Source` interface (real RPC vs. Replay).
    - [ ] Subtask: Implement `FileSource` that streams events from `fixtures/` directory.

## Phase 2: Transactional Outbox
- [ ] Task: Database Schema Update
    - [ ] Subtask: Create migration for `events` (canonical) and `outbox` tables.
    - [ ] Subtask: Implement `Outbox` repository for atomic upsert of Event + OutboxMsg.
- [ ] Task: Outbox Publisher Service
    - [ ] Subtask: Create `cmd/outbox-publisher/`.
    - [ ] Subtask: Implement polling/CDC loop to read from `outbox` table and publish to Redpanda.
    - [ ] Subtask: Ensure strict ordering by `event_id` (or sequence number).

## Phase 3: Subscription & Routing
- [ ] Task: Redis Subscription Store
    - [ ] Subtask: Implement `SubscriptionManager` to store client interest sets (chain, address, event_type).
- [ ] Task: Routing Logic
    - [ ] Subtask: Implement matching logic to filter canonical events against active subscriptions.
    - [ ] Subtask: Optimize for high-concurrency (pipelining/batching).

## Phase 4: Reorg Detection & Handling
- [ ] Task: Reorg Detector Logic
    - [ ] Subtask: Implement block parent hash verification.
    - [ ] Subtask: Detect "fork" conditions where local head != remote head.
- [ ] Task: Tombstone & Replacement
    - [ ] Subtask: Define `Retract` and `Replace` event payloads in Protobuf.
    - [ ] Subtask: Implement logic to emit "Retract" events for orphaned blocks.
    - [ ] Subtask: Implement logic to emit "Replace" events for the new canonical chain.

## Phase 5: Verification
- [ ] Task: Deterministic Replay Test
    - [ ] Subtask: Run `fixture-recorder` to capture a known reorg (or simulate one).
    - [ ] Subtask: Run `adapter` with `FileSource` and verify Outbox contains correct Retract/Replace sequence.
