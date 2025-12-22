# Track Plan: Milestone 1 - Ingestion & Canonical Stream MVP

## Phase 1: Repository & Environment Setup
- [ ] Task: Initialize Project Repository
    - [ ] Subtask: Run `go mod init <module_name>` to initialize the Go module.
    - [ ] Subtask: Create standard directory structure (`cmd/`, `internal/`, `pkg/`, `deployments/`, `api/proto/`).
    - [ ] Subtask: Create a `Makefile` with standard commands (`build`, `test`, `lint`, `proto`).
- [ ] Task: Docker Compose Environment
    - [ ] Subtask: Create `docker-compose.yml` defining services for Redpanda, Postgres (TimescaleDB), and Redis.
    - [ ] Subtask: Verify all services spin up and are accessible.
- [ ] Task: Conductor - User Manual Verification 'Phase 1: Repository & Environment Setup' (Protocol in workflow.md)

## Phase 2: Canonical Schema Definition
- [ ] Task: Define Protobuf Schema
    - [ ] Subtask: Create `api/proto/v1/event.proto`.
    - [ ] Subtask: Define `CanonicalEvent` message with fields: `chain`, `commitment`, `block_number`, `block_hash`, `tx_hash`, `event_type`, `payload`, `timestamp`.
    - [ ] Subtask: Configure `buf` or `protoc` generation in `Makefile`.
    - [ ] Subtask: Generate Go code from the proto definition.
- [ ] Task: Conductor - User Manual Verification 'Phase 2: Canonical Schema Definition' (Protocol in workflow.md)

## Phase 3: Adapter Implementation (Solana)
- [ ] Task: Solana Adapter Scaffold
    - [ ] Subtask: Create `cmd/adapter-solana/` and `internal/adapter/solana/`.
    - [ ] Subtask: Implement main entrypoint that loads configuration (env vars).
- [ ] Task: Connect to Geyser gRPC
    - [ ] Subtask: Implement a gRPC client to connect to a Solana Yellowstone Geyser endpoint.
    - [ ] Subtask: Implement a subscription mechanism to receive block/transaction updates.
    - [ ] Subtask: Log received events to stdout for verification.
- [ ] Task: Conductor - User Manual Verification 'Phase 3: Adapter Implementation (Solana)' (Protocol in workflow.md)

## Phase 4: Adapter Implementation (EVM)
- [ ] Task: EVM Adapter Scaffold
    - [ ] Subtask: Create `cmd/adapter-evm/` and `internal/adapter/evm/`.
    - [ ] Subtask: Implement main entrypoint that loads configuration.
- [ ] Task: Connect to EVM RPC
    - [ ] Subtask: Implement a standard `ethclient` or WebSocket connection to an EVM chain.
    - [ ] Subtask: Implement log subscription (`eth_subscribe` "logs").
    - [ ] Subtask: Log received logs to stdout for verification.
- [ ] Task: Conductor - User Manual Verification 'Phase 4: Adapter Implementation (EVM)' (Protocol in workflow.md)

## Phase 5: Core Processing & Normalization
- [ ] Task: Processor Scaffold
    - [ ] Subtask: Create `cmd/processor/` and `internal/processor/`.
- [ ] Task: Normalization Logic
    - [ ] Subtask: Implement a `Normalizer` interface.
    - [ ] Subtask: Implement specific normalization logic for Solana events -> `CanonicalEvent`.
    - [ ] Subtask: Implement specific normalization logic for EVM logs -> `CanonicalEvent`.
    - [ ] Subtask: Write unit tests to verify mapping of fields.
- [ ] Task: Conductor - User Manual Verification 'Phase 5: Core Processing & Normalization' (Protocol in workflow.md)
