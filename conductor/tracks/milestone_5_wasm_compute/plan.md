# Track Plan: Milestone 5 - Phase 2: WASM Compute Platform

## Phase 1: Trigger Router & Invocation Queue
- [ ] Task: Trigger Router Service
    - [ ] Subtask: Create `cmd/trigger-router/`.
    - [ ] Subtask: Implement logic to match events against user-defined triggers (store triggers in Redis).
    - [ ] Subtask: Enqueue invocations to a new Kafka topic `function-invocations`.
- [ ] Task: Invocation Queue Setup
    - [ ] Subtask: Configure `function-invocations` topic in Redpanda with appropriate partitioning.

## Phase 2: WASM Runtime Host (Wasmtime)
- [ ] Task: WASM Runtime Service
    - [ ] Subtask: Create `cmd/wasm-host/`.
    - [ ] Subtask: Integrate `bytecodealliance/wasmtime-go` for executing WASM modules.
    - [ ] Subtask: Implement `Host SDK` functions (host functions exposed to WASM) for logging and basic KV access.
    - [ ] Subtask: Implement module loading (from S3/MinIO) and caching.
- [ ] Task: Resource Enforcement
    - [ ] Subtask: Implement epoch-based interruption for timeout enforcement.
    - [ ] Subtask: Enforce memory limits per instance.

## Phase 3: State Layer (Durable Objects & KV)
- [ ] Task: Per-Tenant KV Store
    - [ ] Subtask: Implement a Redis-backed KV API exposed to the WASM runtime via host functions.
    - [ ] Subtask: Enforce tenant isolation using key prefixes.
- [ ] Task: Durable Object Prototype
    - [ ] Subtask: Implement single-threaded actor model coordination (using Redis locking or similar).
    - [ ] Subtask: Expose `DO` state accessors to WASM.

## Phase 4: Developer Experience (CLI & Deploy)
- [ ] Task: Deployment CLI (`pulse`)
    - [ ] Subtask: Implement `pulse deploy <function.wasm> <manifest.yaml>`.
    - [ ] Subtask: Create API endpoint `POST /api/v1/functions` to receive uploads and store in MinIO.
- [ ] Task: Function Registry
    - [ ] Subtask: Create database tables for `functions`, `triggers`, and `deployments`.

## Phase 5: Metering & Billing Hooks
- [ ] Task: Usage Metering
    - [ ] Subtask: Track execution time (wall + CPU) and memory usage per invocation.
    - [ ] Subtask: Emit usage metrics to a `billing-events` topic or table.
