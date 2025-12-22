# Product Guidelines: Mirador

## Engineering Philosophy
- **Performance First:** Every architectural decision must be weighed against its impact on latency and throughput. Allocations on the hot path should be minimized.
- **Correctness is Non-Negotiable:** "Exactly-once" delivery and data completeness are the foundation of trust. We fail closed rather than serving corrupt or incomplete data.
- **Observability:** If you can't measure it, you can't optimize it. Metrics, tracing, and structured logging are first-class citizens, not afterthoughts.
- **Simplicity in Complexity:** While the problem domain is complex, our internal abstractions and interfaces should be as simple and composable as possible (e.g., standard "Adapter" and "Processor" interfaces).

## Code Quality Standards
- **Test-Driven Development (TDD):** Critical paths, especially around event ordering and deduplication, must be covered by deterministic replay tests.
- **Strict Typing:** Use strong typing to prevent runtime errors. Protobuf schemas serve as the contract between services.
- **Documentation:** Code should be self-documenting where possible, but complex algorithms (e.g., reorg handling) require inline explanation and architectural decision records (ADRs).
- **Review Protocol:** All PRs require review from at least one peer, with a focus on potential race conditions and performance regressions.

## Design Principles
- **Loose Coupling:** Services interact via defined interfaces (gRPC/PubSub), allowing independent scaling and deployment.
- **Idempotency:** All write operations must be idempotent to support safe retries and replayability.
- **Tenant Isolation:** In Phase 2, user code must be strictly sandboxed (WASM) to prevent "noisy neighbor" issues and ensure security.
