# Product Guide: Mirador

## Initial Concept
Mirador is a high-throughput, ultra-low-latency blockchain data platform that ingests real-time events from Solana and EVM chains. It normalizes and enriches these events into a canonical stream and delivers them to customers via WebSockets, gRPC streaming, REST, and webhooks. Key differentiators include reorg-aware correction semantics, "exactly-once" delivery guarantees within commitment levels, and provable completeness for finalized data.

## Target Audience
The primary users for Mirador are:
- **DeFi Developers:** Teams building protocols or trading bots that require ultra-low latency data for critical operations like liquidations, arbitrage, and real-time market making.
- **Compute-proximal Platforms:** Applications that need to run custom logic (WASM/V8) directly adjacent to the data stream to minimize network hops and latency.

## Core Value Proposition
- **Ultra-Low Latency:** Optimized for sub-50ms end-to-end latency from block production to client delivery.
- **Data Integrity:** Guarantees "exactly-once" delivery and handles chain reorganizations gracefully with clear commitment levels (processed, confirmed, finalized).
- **Compute-at-Edge:** Enables users to deploy serverless functions that execute immediately upon event ingestion, eliminating the latency of external webhooks or polling.

## Key Features (Phase 1)
- **Multi-Chain Ingestion:** Support for high-volume Solana (Yellowstone/Geyser) and EVM chain events.
- **Canonical Event Stream:** A unified, normalized schema for events across different blockchains.
- **Flexible Delivery:** High-concurrency WebSockets, gRPC streaming, and reliable webhooks.
- **Tiered Storage:** Hot storage (TimescaleDB) for recent data and cold storage (Parquet/BigQuery) for analytics.
- **Correctness Tooling:** Real-time gap detection and manifest-based reconciliation for finalized data.

## Key Features (Phase 2)
- **User-Deployed Compute:** A WASM-based serverless environment for running user code triggered by data events.
- **Stateful Functions:** Support for per-tenant KV stores and durable objects to manage state across invocations.
