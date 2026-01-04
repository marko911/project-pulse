# Track Plan: Milestone 7 - Implementation Hardening

## Phase 1: WASM Runtime Completion
- [ ] Task: WASM Input/Output Logic
    - [ ] Subtask: Implement `input` passing in `internal/wasm/runtime.go`.
        - **Context:** Currently, the `Execute` function accepts an `input []byte` but does nothing with it.
        - **Requirement:** Allocate memory inside the WASM instance (using `malloc` if available, or a pre-defined buffer), write the input bytes, and pass the pointer/length to the entry function or make it available via a host function `get_input`.
- [ ] Task: WASM Host Kafka Integration
    - [ ] Subtask: Implement `consumeInvocations` in `internal/wasm/host.go`.
        - **Context:** The function currently just waits for context cancellation.
        - **Requirement:** Use `franz-go` to consume `function-invocations` topic, unmarshal JSON, and send `InvocationRequest` to the worker channel.
    - [ ] Subtask: Implement `publishResults` in `internal/wasm/host.go`.
        - **Context:** The function currently logs results but doesn't publish.
        - **Requirement:** Use `franz-go` to publish `InvocationResult` JSON to the `function-results` topic.

## Phase 2: EVM Adapter Connectivity
- [ ] Task: EVM Adapter Broker Connection
    - [ ] Subtask: Implement `ConnectToBroker` in `internal/adapter/evm/adapter.go`.
        - **Context:** The EVM adapter connects to the RPC but dumps events to `/dev/null` (logs).
        - **Requirement:** Initialize a `kgo.Client`, and in the event loop, marshal events to the canonical JSON/Protobuf format and produce them to the `raw-events` topic.

## Phase 3: Processor Service Implementation
- [ ] Task: Processor Core Logic
    - [ ] Subtask: Implement `cmd/processor/main.go` initialization.
        - **Context:** The main function is a skeleton with TODOs.
        - **Requirement:** Initialize the `internal/processor` logic.
    - [ ] Subtask: Implement Consumer/Producer loop in `internal/processor`.
        - **Context:** The processor needs to be the bridge between `raw-events` and `canonical-events`.
        - **Requirement:** Consume `raw-events`, run the `Normalize` function (already implemented?), and produce to `canonical-events`.

## Phase 4: API Gateway Security
- [ ] Task: WebSocket Security
    - [ ] Subtask: Configure CORS/Allowed Origins in `cmd/api-gateway/websocket.go`.
        - **Context:** Currently hardcoded or open.
        - **Requirement:** specific config flag for allowed origins.
