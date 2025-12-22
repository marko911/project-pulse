# Tech Stack: Mirador

## Core Backend
- **Language:** Go (primary for adapters, processor, and delivery APIs).
- **Communication:** gRPC (internal service-to-service), Protobuf (canonical schema).
- **Messaging:** Redpanda (broker backbone for event streams), NATS JetStream (low-latency fanout spine for WebSockets).

## Compute Platform (Phase 2)
- **Runtime:** WASM (Wasmtime) for user-deployed functions.
- **State:** Durable Objects (actor model) with Redis-backed persistence.

## Data Storage
- **Hot Store:** TimescaleDB (PostgreSQL-based) for real-time queryable events (24-72h retention).
- **Hot State:** Redis for subscriptions, rate limits, and cross-chain correlation.
- **Cold Store:** Google Cloud Storage (GCS) for hourly Parquet partitions.
- **Analytics:** BigQuery for historical ad-hoc queries and analytics.

## Infrastructure & DevOps
- **Provisioning:** Terraform (managed infrastructure).
- **Orchestration:** Kubernetes (GKE) for production; Docker Compose for local development parity.
- **Observability:** Prometheus/Grafana (metrics), Jaeger (tracing), Structured JSON logging.
- **Testing:** Chaos Mesh (fault injection), recorded fixture replay harness.
