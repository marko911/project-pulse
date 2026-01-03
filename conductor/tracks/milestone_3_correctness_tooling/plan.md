# Track Plan: Milestone 3 - Correctness Tooling

## Phase 1: Gap Detection
- [ ] Task: Gap Detector Service
    - [ ] Subtask: Create `cmd/gap-detector/`.
    - [ ] Subtask: Implement real-time monitoring of Redpanda finalized topics.
    - [ ] Subtask: Implement state tracking (last seen block) per chain/commitment.
    - [ ] Subtask: Emit alerts/metrics when a gap is detected.

## Phase 2: Manifest Generation
- [ ] Task: Manifest Repository & Schema
    - [ ] Subtask: Create SQL migration for `manifests` table.
    - [ ] Subtask: Implement `ManifestRepository` in `internal/platform/storage`.
- [ ] Task: Manifest Builder Service
    - [ ] Subtask: Create `cmd/manifest-builder/`.
    - [ ] Subtask: Implement logic to aggregate event counts and compute `event_ids_hash` for finalized blocks.
    - [ ] Subtask: Write manifests to database.

## Phase 3: Reconciliation
- [ ] Task: Independent Source Client (Golden Source)
    - [ ] Subtask: Implement a lightweight client that calls a secondary RPC provider.
- [ ] Task: Reconciliation Service
    - [ ] Subtask: Create `cmd/reconciler/`.
    - [ ] Subtask: Implement "Tier 1" reconciliation (header continuity).
    - [ ] Subtask: Implement "Tier 2" reconciliation (event count comparison).
    - [ ] Subtask: Implement "Tier 3" reconciliation (sampling/full hash check).

## Phase 4: Automated Recovery
- [ ] Task: Backfill Orchestrator
    - [ ] Subtask: Implement service to receive "GapFound" events.
    - [ ] Subtask: Trigger targeted range requests to adapters via a `backfill` topic.
- [ ] Task: Fail-Closed Watermark Policy
    - [ ] Subtask: Implement logic to halt the "Global Finalized Watermark" if a gap is detected or reconciliation fails.

## Phase 5: Verification
- [ ] Task: Correctness Dashboard/API
    - [ ] Subtask: Expose `/health/correctness` and `/manifest` endpoints.
- [ ] Task: Chaos Testing (Gap Recovery)
    - [ ] Subtask: Use `fixture-recorder` to skip a block; verify Gap Detector finds it and Backfill Orchestrator restores it.
