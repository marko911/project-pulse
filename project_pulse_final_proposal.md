# Project Pulse Architecture Proposal

Project Pulse is a high-throughput, ultra-low-latency blockchain data platform that ingests real-time events from Solana and EVM chains, normalizes and enriches them into a canonical event stream, and delivers them to customers via WebSockets, gRPC streaming, REST, and webhooks—while guaranteeing reorg-aware correction semantics and “exactly-once” delivery within each commitment level, plus provable completeness for finalized data through manifests and continuous reconciliation. It also provides tiered storage (hot for recent query + state, cold for long-term archive/analytics) and, in Phase 2, enables customers to deploy their own low-latency compute modules (WASM/V8 serverless functions with per-tenant quotas and durable state) so their logic can run next to the data stream for minimal latency.

## 1. Goals and Non-Goals

### 1.1 Goals (Phase 1)

1. **Ultra-low latency real-time ingestion + delivery**
   - Solana sustained ingestion > 50k events/sec; EVM > 1k events/sec.
   - p99 end-to-end latency < 50ms (block production → client delivery).
   - 100k+ concurrent WebSocket connections with <10ms fanout latency.
2. **Exactly-once delivery (within commitment level)**
   - Deterministic event IDs + idempotent sinks + transactional outbox + strict offset commit discipline.
3. **Reorg-aware correctness**
   - Separate commitment-level streams (processed/confirmed/finalized).
   - Correction events (tombstones/retractions + replacements).
4. **Provable completeness for finalized**
   - Manifest-based audit trail + reconciliation + fail-closed watermark policies.
5. **Architectural “no-regret” decisions for Phase 2 compute**
   - Keep the hot path and state layer compatible with a WASM/V8 compute runtime.

### 1.2 Goals (Phase 2)

1. **User-deployed compute “at the data”**
   - Event-triggered serverless functions (WASM sandbox or V8 isolates).
   - Per-tenant quotas, metering, isolation, noisy-neighbor protection.
2. **Stateful compute**
   - Per-tenant KV store.
   - Durable objects (single-threaded actor model) for coordination/state.

### 1.3 Non-Goals (initially)

- Multi-region active/active. Phase 2 will be active/passive first.
- General-purpose stream processing platform (we focus on Project Pulse’s canonical schema + delivery + compute).

---

## 2. System Overview (Hybrid Reference Architecture)

### 2.1 High-Level Data Flow

```
┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                                   MIRADOR PLATFORM                                                       │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

   ┌───────────────────────────────┐                                ┌───────────────────────────────┐
   │            SOLANA             │                                │              EVM              │
   │  Yellowstone / Geyser gRPC    │                                │   WS / RPC (multiple chains)  │
   └───────────────┬───────────────┘                                └───────────────┬───────────────┘
                   │                                                            │
                   ▼                                                            ▼
        ┌───────────────────────┐                                   ┌───────────────────────┐
        │     Solana Adapter     │                                   │      EVM Adapter(s)   │
        │  parse, normalize,     │                                   │  parse, normalize,    │
        │  commitment mapping    │                                   │  commitment mapping   │
        └───────────────┬───────┘                                   └───────────────┬───────┘
                        │                                                           │
                        ├──────────────────────────────────────┬────────────────────┘
                        │                                      │
                        ▼                                      ▼
            ┌───────────────────────┐                ┌─────────────────────────────┐
            │  Backfill Orchestrator │                │  Optional Quorum Producer   │
            │  targeted ranges only  │                │  (EVM integrity mode)       │
            └───────────────┬───────┘                └───────────────┬─────────────┘
                            │                                       (side-input; emits distinct `events.<chain>.<lvl>.quorum` stream)
                            │
                            ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                             BROKER BACKBONE (Redpanda/Kafka)                                             │
│                                                                                                                          │
│   events.<chain>.processed    events.<chain>.confirmed    events.<chain>.finalized     backfill.events.<chain>            │
│                                                                                                                          │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                                  PROCESSING CORE                                                         │
│  - schema validate (protobuf)                                                                                             │
│  - deterministic event_id                                                                                                 │
│  - idempotent upserts + transactional outbox                                                                               │
│  - reorg detection + retract/replace (tombstones/corrections)                                                              │
│  - enrichment (prices/labels/tokens)                                                                                       │
│  - windowed aggregates                                                                                                    │
│  - cross-chain correlation                                                                                                 │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌───────────────────────────────────────────────┐     ┌─────────────────────────────┐     ┌───────────────────────────────┐
│                HOT STORAGE (SQL)              │     │        HOT STATE (Redis)     │     │  SQL Tables: Events +         │
│   Timescale/Postgres (24–72h canonical)      │     │  subs, quotas, correlation   │     │  Outbox + Manifests           │
│   - events (unique on event_id)              │     │  heads/watermarks            │     │  (finalized audit trail)      │
└───────────────────────────────┬──────────────┘     └───────────────┬─────────────┘     └───────────────┬───────────────┘
                                │                                    │                                   │
                                └──────────────────────────┬─────────┴───────────────┬───────────────────┘
                                                           │                         │
                                                           ▼                         ▼
                                           ┌──────────────────────────────┐   ┌──────────────────────────────────┐
                                           │        Outbox Publisher       │   │         Correctness Tooling       │
                                           │  reads Outbox table + emits   │   │  (always-on; uses finalized)      │
                                           └──────────────┬───────────────┘   │                                  │
                                                          │                   │  Gap Detector (real-time)         │
                                                          ├──────────────►    │  Manifest Builder (finalized;      │
                                                          │   (direct, no NATS)│    writes manifests → SQL tables) │
                                                          │                   │  Reconciliation Service            │
                                                          │                   │  Divergence checks (dual-source)   │
                                                          │                   │  Targeted backfill triggers        │
                                                          │                   └───────────────────┬───────────────┘
                                                          │                                   │
                                                          │                                   │ (side-input)
                                                          │                                   ▼
                                                          │                     ┌───────────────────────────────┐
                                                          │                     │ Independent Source ("Golden") │
                                                          │                     │  - separate RPC/provider      │
                                                          │                     │  - optional own node          │
                                                          │                     └───────────────────────────────┘
                                                          │
                                                          ▼
                                           ┌───────────────────────────┐
                                           │ Optional Fanout Spine     │
                                           │      NATS JetStream       │
                                           │  replayable fanout bus    │
                                           └───────────────┬───────────┘
                                                           │
                                                           ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                                    DELIVERY APIS                                                         │
│                                                                                                                          │
│  ┌───────────────────────────────┐    ┌───────────────────────────────┐    ┌────────────────────────────────────┐      │
│  │ WebSocket Gateway (100k+)     │    │ gRPC Streaming Service         │    │ Webhook Dispatcher + DLQ           │      │
│  │ - stateless gateways          │    │ - flow control/backpressure    │    │ - retries, signatures, replay      │      │
│  │ - subs stored in Redis        │    │                               │    │                                    │      │
│  └───────────────┬───────────────┘    └───────────────┬───────────────┘    └───────────────┬────────────────────┘      │
│                  │                                    │                                    │                             │
│                  └───────────────────────┬────────────┴───────────────┬────────────────────┘                             │
│                                          │                            │                                                  │
│                                          ▼                            ▼                                                  │
│                                    ┌──────────────────────────────────────────────┐                                      │
│                                    │               REST / Admin API                │                                      │
│                                    │  - query hot store                             │                                      │
│                                    │  - manifests + reconciliation status           │                                      │
│                                    │  - tenants/keys/limits                          │                                      │
│                                    └──────────────────────────────────────────────┘                                      │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                             COLD STORAGE / ANALYTICS                                                     │
│   GCS Parquet (hourly partitions, compaction)  ─────────►  BigQuery (analytics / ad hoc queries)                          │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘


┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                           PHASE 2: COMPUTE AT THE DATA (WASM-first)                                      │
│                                                                                                                          │
│  ┌──────────────────────────────┐      ┌──────────────────────────────┐      ┌──────────────────────────────────┐      │
│  │ Trigger Router               │─────►│ Invocation Queue (MVP: Kafka)  │─────►│ WASM Runtime Hosts (Wasmtime)     │      │
│  │ - compiled predicates        │      │ - per-tenant concurrency      │      │ - epoch interruption/timeouts     │      │
│  │ - event/schedule/webhook     │      │ - rate limits                 │      │ - host SDK (KV, durable objs)     │      │
│  └───────────────┬──────────────┘      └───────────────┬──────────────┘      └───────────────┬──────────────────┘      │
│                  │                                      │                                      │                         │
│                  ▼                                      ▼                                      ▼                         │
│  ┌──────────────────────────────┐      ┌──────────────────────────────┐      ┌──────────────────────────────────┐      │
│  │ Per-tenant KV (Redis first)  │      │ Durable Objects (actor model) │      │ Metering + Quotas + Billing Hooks │      │
│  │ - namespaces per tenant      │      │ - single-threaded per key     │      │ - cpu_ms, wall_ms, mem_peak       │      │
│  └──────────────────────────────┘      └──────────────────────────────┘      └──────────────────────────────────┘      │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

```

---

## 3. Architecture Decisions (Why this hybrid)

### 3.1 “Spec-truth” components (from GPT)

- **Manifest Builder + Manifest API** for finalized correctness audit.
- **Reconciliation Service** with fail-closed watermark policy.
- **Commitment-level topic separation** (processed/confirmed/finalized).
- **Exactly-once strategy**: deterministic IDs + idempotent sinks + transactional outbox.
- **WASM-first compute platform** with durable objects + per-tenant quotas.

### 3.2 “3-person team buildability” components (from Claude)

- **Local-first dev**: Docker Compose stack that mirrors production.
- **Environment parity via interfaces**: protocol-compatible local services (MinIO/Redpanda/etc).
- **Progressive deployment**: local → single VM → managed services → Kubernetes.
- **Repo/project structure + Makefile workflow**: one-command setup, standard entrypoints.

### 3.3 “Hardening extras” components (from Gemini)

- **Dedicated real-time Gap Detector** service and targeted backfill trigger path.
- **Chaos testing**: adopt Chaos Mesh once Kubernetes is in use; use lightweight fault injection pre-K8s.

---

## 4. Core Data Model and APIs

### 4.1 Canonical Event Schema

- Protobuf canonical schema with strict evolution policy (compat checks in CI).
- Event fields must include:
  - `chain`, `commitment_level`, `block_number/slot`, `block_hash`, `tx_hash/signature`
  - `event_type`, `account(s)`, `timestamp`, `payload`
  - `event_id` (deterministic, stable)
  - `reorg_action` (normal | retract | replace), `version`

### 4.2 Subscription Filters

Support multi-field subscription filters:

- chains, event_types, accounts, min_value_usd, commitment
- server-side compilation into efficient predicates
- per-connection subscription sets stored in Redis (durable across gateway restarts)

### 4.3 Delivery APIs

- **WebSocket**: real-time streaming with backpressure and per-tenant limits.
- **gRPC streaming**: high-throughput integrations.
- **Webhooks**: async delivery with retries + DLQ and signed payloads.
- **REST**: point lookups, range scans, admin, manifests, reconciliation status.

---

## 5. Exactly-Once + Reorg Semantics

### 5.1 Deterministic Event IDs

- Compute `event_id` from chain + commitment + block/slot + tx + event-local index (+ schema version).
- The goal: duplicates across sources are safe; replays are idempotent.

### 5.2 Transactional Outbox (durable fanout intent)

- In a single DB transaction:
  1. upsert canonical event row(s)
  2. insert outbox message(s) keyed by `event_id`
- Outbox publisher reliably emits to NATS/WS/gRPC/webhooks.
- Consumer offsets are committed **only after** durable write success.

### 5.3 Reorg Corrections

- Processed/confirmed are best-effort real-time; finalized is canonical.
- On reorg:
  - emit tombstone/retract events for orphaned block events
  - emit replacements for new canonical block
- Clients are expected to handle correction events via SDK helpers.

---

## 6. Correctness Contract (Required)

This section is the non-negotiable contract for engineering, operations, and external claims.

### 6.1 Definitions

- **Commitment Levels**:
  - `processed`: lowest latency, may change.
  - `confirmed`: medium confidence, may change.
  - `finalized`: canonical, used for “provable completeness” claims.
- **Event Completeness**:
  - For a given commitment stream and block range, all canonical events implied by chain data are present.
- **Exactly-once (within commitment)**:
  - For each commitment stream, each canonical event is delivered once to downstream consumers, despite failures/retries.
- **Gap**:
  - A missing block/slot in a continuous finalized sequence, or missing required events for a finalized block.

### 6.2 Contractual Invariants (Finalized Stream)

For every finalized block/slot in a contiguous range [A..B]:

1. **Manifest exists** for each block/slot.
2. **Header continuity** holds: parent hash links form a continuous chain.
3. **No gaps** in the range (or explicit documented chain halt condition).
4. **Counts match**: manifest counts (tx/log/instruction/event) match independent source(s).
5. **Event hash matches**: `event_ids_hash` matches reconstruction on a defined sample policy (and can be increased to 100% during incidents).
6. **Schema validity**: every event passes schema validation (reject to DLQ is a correctness incident).
7. **Durable watermark policy**: the “finalized watermark” does not advance past suspect/gap blocks.

### 6.3 Contractual Invariants (Processed/Confirmed Streams)

- Best-effort low-latency delivery is the priority.
- Correction events may be emitted at any time to retract/replace.
- “No drops” is still required operationally, but completeness is not externally claimed.

### 6.4 Ordering Contract

- Global ordering across all partitions is not guaranteed.
- Guaranteed canonical ordering within a block/slot uses:
  - `(slot/block_number, tx_index, event_local_index)` when available
- If a stream cannot reliably produce `tx_index`, the ordering contract must be explicitly downgraded for that stream type.

### 6.5 Failure Policies (Fail Closed)

If finalized reconciliation fails:

- Mark affected blocks “suspect” and halt advancing finalized watermark.
- Trigger targeted backfill.
- Alert on-call with runbook link.
- Produce a “correctness status” API endpoint used by internal and (optionally) external clients.

### 6.6 Audit Endpoints

Expose:

- `GET /manifest/{chain}/{block_or_slot}`
- `GET /manifest/{chain}/range?from=&to=`
- `GET /reconciliation/status?chain=&from=&to=`
- `GET /health/correctness` (watermarks, suspect blocks, last pass)

---

## 7. Correctness Tooling

### 7.1 Gap Detector (real-time)

- Consumes finalized topics and tracks last seen block/slot per chain.
- On gap detection:
  - raises high priority alert
  - triggers targeted backfill for missing block(s)
- Must be lightweight and always-on.

### 7.2 Manifest Builder (finalized)

For each finalized block/slot:

- store `(chain, height/slot, hash, parent_hash, expected_counts, event_count_emitted, event_ids_hash, ingested_at, sources_used)`

### 7.3 Reconciliation Service

Runs continuously in tiers:

1. Header chain continuity (cheap, continuous)
2. Count reconciliation (medium cost, continuous or periodic)
3. Full event hash reconstruction (strongest; sample policy adjustable; 100% in staging)

### 7.4 Dual-source divergence signals

- Optionally ingest from two independent providers (especially for EVM).
- Differences are signals; duplicates are safe due to deterministic IDs.

---

## 8. Storage Strategy

### 8.1 Hot Store (TimescaleDB)

- Retention: 24–72 hours
- Indices: chain, event_type, accounts, height, timestamp
- Upserts on `event_id` unique key

### 8.2 Warm Cache (Redis)

- Subscription routing state
- Latest head/finalized per chain
- Rate limiting + quotas
- Cross-chain correlation pending state (TTL cleanup)

### 8.3 Cold Store (GCS Parquet + BigQuery)

- Hourly partitioned parquet → daily compaction
- BigQuery as query/analytics surface

---

## 9. Delivery and Fanout

### 9.1 WebSocket Gateway (100k+)

- Stateless WS gateways behind load balancer
- Connection/session auth + per-tenant limits
- Subscription sets stored in Redis
- Fanout via:
  - direct Redis pub/sub (small scale), or
  - NATS JetStream spine (recommended for replayable fanout & multi-gateway scaling)

### 9.2 gRPC Streaming

- Efficient server push
- Backpressure + flow control
- Recommended for backend integrations

### 9.3 Webhooks

- Durable queue + retries + exponential backoff
- DLQ per tenant + replay tooling
- Signed payloads

---

## 10. Phase 2 Compute Platform (WASM-first)

### 10.1 User-facing Model

- Users deploy functions that trigger on:
  - event match
  - schedule
  - webhook
- Runtime:
  - **WASM sandbox** (default) or **V8 isolates** (future option)
- Limits:
  - CPU time, memory, wall-time timeout, concurrency

### 10.2 Core Components

1. **Trigger Router**
   - Matches events to trigger definitions (compiled predicates)
   - Enqueues invocations
2. **WASM Runtime Host**
   - Wasmtime-based execution
   - Host SDK (logging, metrics, KV, durable object calls)
   - Resource enforcement (epoch interruption / timeouts)
3. **State Layer**
   - Per-tenant KV (Redis-backed initially)
   - Durable Objects (actor model):
     - single-threaded per key
     - ordered message processing
     - persistent state in Redis (and optional SQL later)
4. **Metering + Billing Hooks**
   - Record per invocation: cpu_ms, wall_ms, mem_peak, bytes_in/out
   - Enforce quotas and rate limits

### 10.3 Deployment UX

- CLI + GitOps model:
  - `mirador deploy` uploads wasm module + manifest (triggers, env, limits)
  - versioned releases and rollbacks
- Local dev:
  - run functions against recorded fixtures and replayed block ranges

---

## 11. Security, Multi-Tenancy, and Abuse Prevention

- API keys for all client access; JWT sessions optional for UI.
- Per-tenant rate limits and quotas for:
  - subscriptions
  - webhook endpoints
  - function invocations
- Isolation boundaries:
  - logical tenant isolation in storage namespaces
  - hard resource enforcement at compute runtime
  - network egress policy for user functions (deny by default, allowlist optional)

---

## 12. Observability and Operations

### 12.1 Metrics

Expose (per component and chain):

- throughput, latency p50/p95/p99/p99.9
- error rates
- queue depths / lag (Kafka, NATS)
- watermarks (adapter/broker/processor/storage/delivery)
- correctness signals (gaps, suspect blocks, reconciliation failures)

### 12.2 Tracing

- End-to-end trace IDs propagated adapter → broker → processor → delivery.
- Sampling 1% default; 100% for errors.

### 12.3 Logging

- JSON structured logs with correlation IDs.
- Central aggregation.

### 12.4 Runbooks + SLOs

- Error budget alerting
- Runbook links embedded in critical alerts
- “Correctness incident” playbook (halt watermark, backfill, verify, resume)

---

## 13. Deployment Plan (Progressive Staging)

### Stage 1: Local Development (Day 1)

- Docker Compose:
  - Redpanda, Postgres/Timescale, Redis, NATS (optional), MinIO (optional), Prometheus/Grafana/Jaeger
- Recorded fixtures + deterministic replay tests.

### Stage 2: Single VM (Early customers)

- Same compose file on a beefy VM
- External RPC endpoints, TLS, backups

### Stage 3: Managed Services (Growth)

- Migrate stateful services first:
  - Cloud SQL (HA + PITR)
  - Memorystore (HA)
  - GCS
- Keep compute + adapters as containers

### Stage 4: Kubernetes (Scale)

- GKE (standard or autopilot)
- KEDA autoscaling by lag/queue depth
- Optional NATS JetStream spine for fanout

### Resilience/Chaos

- Pre-K8s: toxiproxy/netem scripts in CI
- On K8s: Chaos Mesh experiments (network latency, pod kills, node drains)

---

## 14. Implementation Plan (3-Person Team)

### Milestone 0 (Week 0–1): Repo + Dev Stack

- Adopt repo layout (cmd/, internal/, pkg/, migrations/, deployments/, terraform/)
- Docker Compose stack + Makefile workflow
- Protobuf schema + CI compatibility checks
- Recorded fixture format + replay harness

### Milestone 1 (Week 2–4): Ingestion + Canonical Stream

- Solana adapter (Yellowstone) + EVM adapter
- Topic strategy: processed/confirmed/finalized + backfill
- Processor core: deterministic IDs, normalize, write to Timescale
- Baseline WS + gRPC streaming for a small set of event types
- Basic auth + rate limiting

### Milestone 2 (Week 5–6): Exactly-Once + Reorg Corrections

- Transactional outbox and outbox publisher
- Strict offset commit discipline
- Reorg detection + correction/tombstone emission
- Subscription routing in Redis

### Milestone 3 (Week 7–8): Correctness Tooling (Must-have)

- Gap Detector (real-time)
- Manifest Builder (finalized)
- Reconciliation service tier 1 + tier 2
- Watermark halt policy + correctness status endpoints
- Backfill orchestrator (targeted)

### Milestone 4 (Week 9–10): Performance + WS Scale

- Partition tuning and consumer concurrency
- WS gateway horizontal scaling pattern
- Optional NATS JetStream spine
- Load tests at target throughput + latency SLO dashboards

### Milestone 5 (Phase 2 Kickoff): WASM Runtime MVP

- Trigger router + WASM host
- KV API + quota enforcement + metering
- Durable object prototype (actor keyed by object id)
- Developer CLI for deploy/version/rollback

---

## 15. Risks and Mitigations (Top 5)

1. **Solana throughput causing latency spikes**
   - heavy partitioning, allocation reduction, backpressure, targeted batching
2. **Exactly-once bugs under failure**
   - outbox discipline, strict offset commits, chaos/fault injection tests
3. **Reorg correction complexity**
   - explicit reorg schema + SDK helpers + contract tests
4. **100k WS fanout**
   - stateless gateways + Redis state + NATS JetStream spine
5. **Compute platform isolation**
   - WASM-first, epoch interruption, egress controls, per-tenant quotas

---

## Appendix A: Recommended Repo Structure (Starting Point)

Use the Claude-style structure with separate cmd entrypoints and internal interfaces:

- `cmd/` for services: adapters, processor, api-gateway, manifest-builder, reconciler, gap-detector
- `internal/platform/` for interfaces (queue/cache/storage/pubsub)
- `deployments/docker/` + `deployments/k8s/` + `deployments/terraform/`

---

## Appendix B: “Correctness Report” (External Claim Template)

When communicating externally, claims should be limited to:

> For the finalized stream, for any block range [A..B] reported as “verified,” Project Pulse can provide manifests for each block and show reconciliation passing (no gaps, continuity, and count/hash agreement), with full schema validity.
