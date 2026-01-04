# How Mirador Works

This document explains how Mirador processes blockchain events and executes your code, written for developers who want a clear mental model of the system.

---

## The Big Picture

Say you want low-latency access to Solana transactions—maybe you're building a trading bot, tracking whale movements, or monitoring a specific program. Instead of running your own RPC node and polling, you deploy a small piece of code (a WASM module) to Mirador, and it executes your code every time a matching event arrives.

Here's what that looks like:

```
You write code → Deploy it → Create a trigger → Events flow in → Your code runs
```

The whole point is: your code runs *inside* the platform, right next to the data stream, so there's no network hop to your servers for every event.

---

## Your Side: Writing and Deploying Functions

### Step 1: Write Your Function

You write a Go program that will receive blockchain events. It's compiled to WebAssembly (WASM), which is a portable binary format that runs in a secure sandbox.

```go
func main() {
    // Read the incoming event
    eventData := getInput()

    // Parse it, do your logic
    var event CanonicalEvent
    json.Unmarshal(eventData, &event)

    // Log what you found
    log(1, fmt.Sprintf("Got transaction: %s", event.TxHash))

    // Optionally return some output
    setOutput(`{"processed": true}`)
}
```

Your function has access to a few host functions provided by the platform:
- `get_input()` - Read the event that triggered your function
- `log(level, message)` - Write logs (visible in `mirador logs`)
- `output(data)` - Return data from your function

### Step 2: Compile to WASM

```bash
GOOS=wasip1 GOARCH=wasm go build -o myfunction.wasm .
```

This produces a `.wasm` file—a sandboxed binary that can't access your filesystem, network, or anything else unless explicitly allowed.

### Step 3: Deploy

```bash
mirador deploy myfunction.wasm --name my-function
```

Behind the scenes:
1. The CLI uploads your `.wasm` file to object storage (MinIO locally, GCS in production)
2. It registers the function in the database with metadata (name, checksum, version)
3. You get back a function ID like `fn_1234567890`

### Step 4: Create a Trigger

```bash
mirador triggers create \
  --function-id fn_1234567890 \
  --name solana-watcher \
  --event-type transaction \
  --filter-chain 1
```

This tells the platform: "Every time a Solana transaction comes in, run my function."

You can filter more specifically:
- `--filter-chain 1` (Solana) or `2` (Ethereum), etc.
- `--filter-addr <address>` to watch specific accounts/contracts
- `--event-type transfer` to only match certain event types

---

## The Platform Side: What Happens to Your Events

Now let's trace what happens when a Solana transaction occurs on the network.

### Component 1: Adapters (The Ears)

**What it does:** Connects to blockchain nodes and listens for new data.

The **Solana Adapter** maintains a WebSocket connection to Solana RPC nodes (devnet for local dev, or a Geyser gRPC connection for mainnet performance). When a new transaction is confirmed, it receives the raw data.

The adapter's job is *normalization*—taking chain-specific data and converting it to a common format called a `CanonicalEvent`:

```json
{
  "event_id": "evt_abc123",
  "chain": 1,
  "block_number": 234567890,
  "tx_hash": "5xYz...",
  "event_type": "transaction",
  "timestamp": "2024-01-15T12:00:00Z",
  "payload": "<base64-encoded raw data>"
}
```

This normalization is crucial—it means your WASM function receives the same structure whether the event came from Solana, Ethereum, or any other chain.

**Publishes to:** `canonical-events` Kafka topic

### Component 2: Redpanda (The Nervous System)

**What it does:** Reliably moves events between components.

Redpanda is a Kafka-compatible message broker. Think of it as a highly durable queue system. When the adapter publishes an event, it's written to disk and replicated before being acknowledged. This means:

- Events aren't lost if a component crashes
- Multiple consumers can read the same stream independently
- We can replay events if needed

The key topics:
- `canonical-events` - All normalized blockchain events
- `function-invocations` - "Please run this function with this event"
- `function-results` - "Here's what happened when we ran it"

### Component 3: Trigger Router (The Matchmaker)

**What it does:** Decides which functions should run for each event.

The Trigger Router consumes from `canonical-events` and, for each event:

1. Looks up matching triggers in Redis (fast in-memory cache)
2. For each match, creates an invocation request
3. Publishes to `function-invocations` topic

The matching is based on the filters you set when creating the trigger:
- Chain matches? ✓
- Event type matches? ✓
- Address filter matches (if specified)? ✓

If you have 10 triggers all watching Solana transactions, one event will create 10 invocation requests.

**Reads from:** `canonical-events`
**Publishes to:** `function-invocations`

### Component 4: WASM Host (The Executor)

**What it does:** Actually runs your code.

The WASM Host is where your function executes. When it receives an invocation request:

1. **Load the module:** Fetches your `.wasm` file from object storage (cached in memory for speed)
2. **Create a sandbox:** Instantiates a fresh WASM environment with strict memory limits
3. **Inject the event:** Makes the event data available via `get_input()`
4. **Execute:** Runs your `main()` function with a CPU time limit
5. **Collect results:** Captures any output and logs
6. **Publish results:** Sends execution status to `function-results` topic

The sandbox is important—your code can't:
- Access the filesystem
- Make network calls (unless we add host functions for it)
- Consume unlimited memory or CPU
- Affect other functions running on the same host

Each invocation is isolated. If your code crashes, it doesn't affect anything else.

**Reads from:** `function-invocations`
**Publishes to:** `function-results`, `billing-events`

### Component 5: Function API (The Control Plane)

**What it does:** Manages your functions and triggers.

This is what the `mirador` CLI talks to. It handles:
- Uploading WASM modules to storage
- CRUD operations for functions and triggers
- Querying invocation logs
- Syncing trigger configuration to Redis for the Trigger Router

**Stores in:** TimescaleDB (functions, triggers, logs), MinIO (WASM binaries), Redis (trigger cache)

---

## The Complete Flow (Putting It Together)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  BLOCKCHAIN                                                                 │
│  ┌─────────────────┐                                                        │
│  │ Solana Network  │                                                        │
│  └────────┬────────┘                                                        │
│           │ WebSocket/Geyser                                                │
│           ▼                                                                 │
│  ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐       │
│  │ Solana Adapter  │────▶│    Redpanda     │────▶│ Trigger Router  │       │
│  │ (normalizes)    │     │ canonical-events│     │ (matches)       │       │
│  └─────────────────┘     └─────────────────┘     └────────┬────────┘       │
│                                                           │                 │
│                          ┌─────────────────┐              │                 │
│                          │    Redpanda     │◀─────────────┘                 │
│                          │func-invocations │                                │
│                          └────────┬────────┘                                │
│                                   │                                         │
│                                   ▼                                         │
│  ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐       │
│  │     MinIO       │────▶│   WASM Host     │────▶│    Redpanda     │       │
│  │ (wasm modules)  │     │ (executes code) │     │ function-results│       │
│  └─────────────────┘     └─────────────────┘     └─────────────────┘       │
│                                   │                                         │
│                                   ▼                                         │
│                          ┌─────────────────┐                                │
│                          │   Your Logs     │                                │
│                          │ (mirador logs)  │                                │
│                          └─────────────────┘                                │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Timeline of a single event:**

1. **T+0ms:** Solana confirms a transaction
2. **T+~50ms:** Adapter receives it via WebSocket, normalizes, publishes to Kafka
3. **T+~55ms:** Trigger Router receives event, finds your trigger, creates invocation
4. **T+~60ms:** WASM Host receives invocation, loads your module (cached), executes
5. **T+~65ms:** Your function runs, logs output, completes
6. **T+~70ms:** Results published, visible in `mirador logs`

Total latency from chain to your code: **~60-100ms** depending on network conditions.

---

## Storage Layers

### Hot Storage (TimescaleDB)
Recent data that needs to be queryable—functions, triggers, invocation logs. TimescaleDB is PostgreSQL optimized for time-series data, so queries like "show me the last 100 invocations" are fast.

### Cache (Redis)
Things that need to be looked up on every event—active triggers, subscription routing. The Trigger Router checks Redis for every incoming event, so this needs to be sub-millisecond.

### Object Storage (MinIO/GCS)
Large blobs—your WASM modules. Fetched once and cached in memory by the WASM Host.

### Event Log (Redpanda)
The durable backbone. Every event flows through Kafka topics. This gives us replay capability, decouples components, and ensures nothing is lost.

---

## Scaling

### Local Development
Everything runs as single instances in Docker Compose. Fine for development and testing.

### Production
- **Adapters:** One per chain, or sharded by account ranges for high-volume chains
- **Trigger Router:** Horizontally scaled, partitioned by event key
- **WASM Host:** Horizontally scaled, each instance handles a partition of invocations
- **Redpanda:** Multi-node cluster with replication
- **TimescaleDB:** Primary with read replicas

The Kafka partition strategy ensures events for the same function are processed in order, while allowing parallel processing across functions.

---

## Why WASM?

We chose WebAssembly for user code because:

1. **Security:** WASM runs in a strict sandbox by default. No filesystem, no network, no syscalls unless explicitly provided by host functions.

2. **Speed:** WASM modules instantiate in microseconds (vs. seconds for containers). This enables running user code on every event without adding latency.

3. **Portability:** Write in Go, Rust, AssemblyScript, or anything that compiles to WASM. The platform doesn't care.

4. **Resource Control:** We can limit memory (e.g., 128MB) and CPU time (e.g., 5 seconds) per invocation. Runaway code gets terminated cleanly.

---

## What Your Code Receives

When your function runs, it receives a `CanonicalEvent` as JSON:

```json
{
  "event_id": "evt_solana_abc123",
  "chain": 1,
  "block_number": 234567890,
  "tx_hash": "5xYz...",
  "event_type": "transaction",
  "timestamp": "2024-01-15T12:00:00Z",
  "payload": "eyJyZXN1bHQiOnsi..."
}
```

The `payload` is base64-encoded chain-specific data. For Solana transactions, it contains the full transaction logs. Your code decodes and processes this.

See the [hello-world example](examples/hello-world/) for a complete implementation that parses Solana payloads and identifies programs and actions.

---

## Summary

1. **You deploy** a WASM function via the CLI
2. **You create a trigger** that defines which events should invoke it
3. **Adapters** pull data from blockchain nodes and normalize it
4. **Redpanda** moves events reliably between components
5. **Trigger Router** matches events to your triggers
6. **WASM Host** executes your code in a secure sandbox
7. **Results** flow back for logging and metrics

Your code runs inside the platform, right next to the data. No webhooks, no polling, no running your own infrastructure. Just deploy and react to events.
