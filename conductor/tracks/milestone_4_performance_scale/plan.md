# Track Plan: Milestone 4 - Performance & WebSocket Scale

## Phase 1: Partitioning & Concurrency
- [ ] Task: Kafka/Redpanda Partition Tuning
    - [ ] Subtask: Analyze current partition strategy and increase partitions for high-volume topics (e.g., `events.solana.*`).
    - [ ] Subtask: Update `adapter` and `processor` configuration to utilize increased partitions.
- [ ] Task: Concurrent Consumption Optimization
    - [ ] Subtask: Profile `processor` throughput.
    - [ ] Subtask: Implement worker pools for parallel event processing within consumers if needed.

## Phase 2: WebSocket Gateway Scaling
- [ ] Task: Redis-based Subscription Routing
    - [ ] Subtask: Implement storage of active subscriptions in Redis (allowing stateless gateways).
    - [ ] Subtask: Update `api-gateway` to read/write subscriptions from Redis.
- [ ] Task: Distributed Fanout (NATS JetStream)
    - [ ] Subtask: Set up NATS JetStream (local/dev).
    - [ ] Subtask: Implement `outbox-publisher` to NATS bridge.
    - [ ] Subtask: Update `api-gateway` to consume from NATS for client delivery.
- [ ] Task: Horizontal Scaling Support
    - [ ] Subtask: Ensure `api-gateway` instances can run in parallel without conflict.

## Phase 3: Load Testing & SLO Verification
- [ ] Task: Load Test Suite
    - [ ] Subtask: Create k6 or similar load generation scripts (simulating 10k+ connections).
    - [ ] Subtask: Simulate high-throughput ingestion bursts.
- [ ] Task: Observability & SLOs
    - [ ] Subtask: Create Grafana dashboard for "Performance & SLOs".
    - [ ] Subtask: Define alerts for p99 latency > 50ms and error rates.
