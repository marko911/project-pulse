# Hello World WASM Function

This example demonstrates a WASM function that processes Solana transaction events, decodes the payload, and identifies known programs and actions.

## What This Function Does

When triggered by a Solana transaction event, this function:

1. Parses the incoming canonical event JSON
2. Decodes the base64-encoded payload (Solana logs)
3. Extracts program invocations from the logs
4. Identifies known programs (Token Program, Jupiter, Drift, etc.)
5. Detects action types (transfers, swaps, mints, burns)
6. Logs a summary of the transaction

## Prerequisites

- **Go 1.21+** with WASM support (`GOOS=wasip1`)
- Running Pulse platform (`make up`)
- Built CLI (`go build -o bin/pulse ./cmd/pulse`)

## Build

```bash
# From this directory
GOOS=wasip1 GOARCH=wasm go build -o hello.wasm .

# Or from the repository root
cd examples/hello-world && GOOS=wasip1 GOARCH=wasm go build -o hello.wasm .
```

**Note:** We use standard Go with `GOOS=wasip1` target, not TinyGo. This provides full Go standard library support including `encoding/json` and `encoding/base64`.

## Deploy

```bash
# From repository root
./bin/pulse deploy examples/hello-world/hello.wasm --name hello-world
```

Note the function ID returned (e.g., `fn_1234567890`).

## Create a Trigger

```bash
# Trigger on all Solana transactions
./bin/pulse triggers create \
  --function-id <function-id> \
  --name solana-watcher \
  --event-type transaction \
  --filter-chain 1
```

## Verify Execution

Watch the WASM host logs to see your function processing events:

```bash
docker compose -f deployments/docker/docker-compose.yml logs -f wasm-host 2>&1 | grep "source.*wasm"
```

You should see output like:

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📦 Solana Transaction @ Slot 123456
🔗 Signature: 5xYz...AbCd
📋 Programs:
   • Token Program
   • Jupiter v6
⚡ Actions:
   💸 Token Transfer
   🔄 Swap
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

## WASM Host Functions

The WASM runtime provides these host functions for your module:

| Function | Signature | Description |
|----------|-----------|-------------|
| `log` | `(level i32, ptr i32, len i32)` | Log a message (level: 0=debug, 1=info, 2=warn, 3=error) |
| `output` | `(ptr i32, len i32)` | Set the function's output (returned to caller) |
| `get_input_len` | `() -> i32` | Get the length of the input event data |
| `get_input` | `(ptr i32, len i32) -> i32` | Read the input event data into a buffer |

## Input Format

The function receives a `CanonicalEvent` as JSON:

```json
{
  "event_id": "evt_123",
  "chain": 1,
  "block_number": 123456,
  "tx_hash": "5xYz...",
  "event_type": "transaction",
  "timestamp": "2024-01-01T12:00:00Z",
  "payload": "<base64-encoded-data>"
}
```

For Solana transactions, the `payload` contains base64-encoded JSON with the transaction logs.

## Customizing

Edit `main.go` to add your own logic:

```go
// Add more known programs
var programNames = map[string]string{
    "YourProgram111111111111111111111111111111111": "My Custom Program",
}

// Add more action detection
if strings.Contains(msg, "your_instruction") {
    actions = append(actions, "🎯 My Custom Action")
}
```

Rebuild and redeploy:

```bash
GOOS=wasip1 GOARCH=wasm go build -o hello.wasm .
./bin/pulse deploy hello.wasm --name hello-world
```
