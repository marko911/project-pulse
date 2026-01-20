# Local Platform Verification Guide

This guide details the steps to verify the end-to-end functionality of the Pulse platform running locally with Docker Compose.

**Goal:** Confirm that an event flows from ingestion (real Solana devnet) -> Trigger Router -> WASM Host -> Execution Log.

## 1. Prerequisites

Ensure the full stack is running:

```bash
make up
```

Verify all services are healthy:

```bash
docker compose -f deployments/docker/docker-compose.yml ps
```

All services should show `healthy` or `running` status.

## 2. Build the CLI

```bash
go build -o bin/pulse ./cmd/pulse
```

## 3. Build and Deploy the Test Function

```bash
# Build the WASM module (uses standard Go with WASI target)
cd examples/hello-world
GOOS=wasip1 GOARCH=wasm go build -o hello.wasm .
cd ../..

# Deploy to the platform
./bin/pulse deploy examples/hello-world/hello.wasm --name hello-world
```

**Note the function ID returned** (e.g., `fn_1767510883808726320`).

## 4. Create a Trigger

Use the CLI to create a trigger that invokes your function on Solana transactions:

```bash
./bin/pulse triggers create \
  --function-id <function-id> \
  --name solana-watcher \
  --event-type transaction \
  --filter-chain 1
```

This tells the system: "When any Solana transaction arrives, run this function."

## 5. Verify Execution

### Method A: Watch Live Logs (Recommended)

The Solana adapter is connected to devnet, which has continuous activity. Just watch the logs:

```bash
# Watch WASM execution logs
docker compose -f deployments/docker/docker-compose.yml logs -f wasm-host 2>&1 | grep "source.*wasm"
```

**Success condition:** You see formatted output showing Solana transactions with programs and actions detected.

Example output:
```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📦 Solana Transaction @ Slot 123456
🔗 Signature: 5xYz...AbCd
📋 Programs:
   • Token Program
   • System Program
⚡ Actions:
   💸 Token Transfer
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

### Method B: Force an Event (If Devnet is Quiet)

Manually inject a canonical event into Redpanda:

```bash
# Start producer
docker exec -i pulse-redpanda rpk topic produce canonical-events
```

Paste this JSON (single line) and press Enter, then Ctrl+D:

```json
{"event_id":"manual-test-1","chain":1,"commitment_level":2,"block_number":1000,"tx_hash":"manual-tx","event_type":"transaction","timestamp":"2026-01-04T12:00:00Z","payload":"eyJyZXN1bHQiOnsiY29udGV4dCI6eyJzbG90IjoxMDAwfSwidmFsdWUiOnsic2lnbmF0dXJlIjoibWFudWFsLXR4IiwiZXJyIjpudWxsLCJsb2dzIjpbIlByb2dyYW0gMTExMTExMTExMTExMTExMTExMTExMTExMTExMTExMTEgaW52b2tlIFsxXSIsIlByb2dyYW0gbG9nOiBIZWxsbyBmcm9tIHRlc3QiLCJQcm9ncmFtIDExMTExMTExMTExMTExMTExMTExMTExMTExMTExMTExIHN1Y2Nlc3MiXX19fQ=="}
```

Check the wasm-host logs immediately after.

## 6. View Invocation Logs via CLI

```bash
# List all invocations for your function
./bin/pulse logs <function-id>

# Show only errors
./bin/pulse logs <function-id> --errors

# Limit to recent entries
./bin/pulse logs <function-id> --limit 5
```

## 7. Verify Function Status

```bash
# List all deployed functions
./bin/pulse functions list

# Get details for a specific function
./bin/pulse functions get <function-id>
```

## Troubleshooting

### No events appearing

1. **Check adapter is receiving events:**
   ```bash
   docker compose -f deployments/docker/docker-compose.yml logs adapter-solana | tail -20
   ```
   You should see messages about received transactions.

2. **Check trigger-router is matching:**
   ```bash
   docker compose -f deployments/docker/docker-compose.yml logs trigger-router | grep -i "match\|trigger"
   ```
   Look for "Found matching triggers" messages.

3. **Check function exists in Redis:**
   ```bash
   docker exec pulse-redis redis-cli KEYS "triggers:*"
   ```

### WASM module not found

```bash
# Check MinIO has the module
docker exec pulse-minio mc ls local/wasm-modules/

# Verify the function is registered
./bin/pulse functions list
```

### Trigger not matching

Verify the trigger configuration:
```bash
./bin/pulse triggers list
./bin/pulse triggers get <trigger-id>
```

Ensure `event_type` and `filter_chain` match the incoming events.

### Redis connection errors

```bash
# Check Redis is running
docker exec pulse-redis redis-cli ping
# Should return: PONG
```

### Database issues

```bash
# Check TimescaleDB is ready
docker exec pulse-timescaledb pg_isready -U pulse

# Check function is in database
docker exec pulse-timescaledb psql -U pulse -d pulse -c "SELECT id, name, status FROM functions;"
```

## Clean Start

If things are in a bad state, restart fresh:

```bash
make down
docker volume prune -f  # Warning: removes all unused volumes
make up
```

Then repeat from step 2.
