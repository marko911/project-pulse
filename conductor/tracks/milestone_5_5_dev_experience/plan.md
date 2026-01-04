# Track Plan: Milestone 5.5 - Developer Experience Polish

## Phase 1: CLI Trigger Management
- [ ] Task: CLI Trigger Commands
    - [ ] Subtask: Implement `mirador triggers create <function_id> --event <type>`.
    - [ ] Subtask: Implement `mirador triggers list`.
    - [ ] Subtask: Implement `mirador triggers delete <id>`.
- [ ] Task: API Trigger Endpoints
    - [ ] Subtask: Update `api-gateway` to expose POST/GET/DELETE endpoints for `/api/v1/triggers`.

## Phase 2: Observability & Watch
- [ ] Task: CLI Logs Command
    - [ ] Subtask: Implement `mirador logs <function_id>`.
    - [ ] Subtask: Stream logs from `billing-events` or `function-results` topic (via API Gateway WebSocket or SSE).
- [ ] Task: Local Dev Watch Mode
    - [ ] Subtask: Implement `mirador dev` that watches for file changes, rebuilds WASM, and redeploys.

## Phase 3: Documentation & Examples
- [ ] Task: Update README
    - [ ] Subtask: Add "Getting Started" guide with the new CLI commands.
    - [ ] Subtask: Document the `hello-world` example.
