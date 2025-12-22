# Track Spec: Milestone 1 - Ingestion & Canonical Stream MVP

## Goal
Establish the foundational infrastructure for Mirador. This includes setting up the Go repository structure, defining the canonical Protobuf schema, implementing the basic Solana and EVM adapters, and creating the core processor to normalize events into a standard format.

## Scope
- **Repo Structure:** Implement the standard `cmd/`, `internal/`, `pkg/` layout.
- **Protobuf Schema:** Define the `CanonicalEvent` proto with all required fields (chain, commitment, block info, event data).
- **Solana Adapter:** Basic ingestion from Yellowstone/Geyser gRPC.
- **EVM Adapter:** Basic ingestion from an EVM RPC/WebSocket.
- **Processing Core:** Normalize raw events into `CanonicalEvent` structs.
- **Docker Compose:** Setup local development environment with Redpanda and Postgres.

## Success Criteria
- [ ] Repo structure is established and `go mod` is initialized.
- [ ] Protobuf definitions compile to Go code.
- [ ] Docker Compose spins up Redpanda and Postgres successfully.
- [ ] Solana Adapter can connect to a mock/testnet Geyser and log raw events.
- [ ] EVM Adapter can connect to a mock/testnet RPC and log raw logs.
- [ ] Core processor successfully unmarshals a raw event and remarshals it to the canonical Protobuf format.
