# Load Testing Suite

Project Pulse - Milestone 4: Performance & WebSocket Scale

This directory contains load testing tools for validating system performance under various conditions.

## Prerequisites

- [k6](https://k6.io/docs/get-started/installation/) - For HTTP and WebSocket load tests
- Go 1.22+ - For the event generator
- Running Pulse infrastructure (API Gateway, NATS, Redis)

## Quick Start

### 1. Start Infrastructure

```bash
# Start local development environment
docker-compose up -d
```

### 2. Run Smoke Tests

```bash
# HTTP endpoints smoke test
k6 run k6/http_endpoints.js

# WebSocket fanout smoke test
k6 run k6/websocket_fanout.js
```

### 3. Run Event Generator

```bash
cd generator
go mod tidy
go run . -rate 1000 -duration 60s
```

## k6 Test Scenarios

All k6 tests support multiple scenarios via the `SCENARIO` environment variable:

| Scenario | Description | VUs | Duration |
|----------|-------------|-----|----------|
| `smoke` | Quick validation | 10 | 1m |
| `load` | Normal expected load | 100 | 5m |
| `stress` | Find breaking point | 100→2000 | ~20m |
| `spike` | Sudden traffic surge | 100→2000→100 | ~6m |
| `soak` | Extended duration | 200 | 30m |

### Usage Examples

```bash
# Smoke test (default)
k6 run k6/http_endpoints.js

# Load test
k6 run -e SCENARIO=load k6/http_endpoints.js

# Stress test with custom URL
k6 run -e SCENARIO=stress -e HTTP_URL=http://prod:8080 k6/http_endpoints.js

# WebSocket stress test
k6 run -e SCENARIO=stress k6/websocket_fanout.js
```

## Test Files

### k6/http_endpoints.js

Tests REST API performance:
- `/health` - Health check
- `/ready` - Readiness probe
- `/api/v1/correctness/status` - Correctness status
- `/api/v1/correctness/watermark` - Watermark monitoring
- `/api/v1/correctness/halts` - Halt events
- `/api/v1/correctness/gaps` - Gap detection
- `/api/v1/manifests/{chain}` - Manifest queries
- `/api/v1/manifests/{chain}/{block}` - Block manifest

**Metrics:**
- `http_health_latency_ms` - Health endpoint latency
- `http_ready_latency_ms` - Ready endpoint latency
- `http_correctness_status_latency_ms` - Status endpoint latency
- `http_manifest_latency_ms` - Manifest query latency
- `http_error_rate` - Overall error rate

### k6/websocket_fanout.js

Tests WebSocket connection handling and event delivery:
- Connection establishment
- Subscription creation (simple/moderate/complex filters)
- Event delivery latency
- Subscription management (list, unsubscribe)
- Connection lifecycle

**Metrics:**
- `ws_connections_total` - Total connection attempts
- `ws_connection_errors_total` - Connection failures
- `ws_event_latency_ms` - Event delivery latency
- `ws_subscription_latency_ms` - Subscription creation latency
- `ws_active_connections` - Current connection count
- `ws_error_rate` - Overall error rate

### generator/main.go

High-throughput event generator for NATS JetStream:
- Configurable event rate (events/sec)
- Multi-chain support
- Burst mode for spike testing
- Realistic event payload generation

**Flags:**
```
-nats string      NATS server URL (default "nats://localhost:4222")
-rate int         Events per second (default 1000)
-duration dur     Test duration (default 1m)
-chains string    Comma-separated chains (default "ethereum,solana")
-burst            Enable burst mode
-burst-ratio      Burst rate multiplier (default 10.0)
-burst-period     Time between bursts (default 30s)
-stream string    JetStream stream name (default "CANONICAL_EVENTS")
-workers int      Publisher workers (default 4)
```

## Performance Targets (SLOs)

| Metric | Target |
|--------|--------|
| End-to-end latency (p99) | < 50ms |
| End-to-end latency (p95) | < 25ms |
| Error rate | < 0.1% |
| Throughput baseline | 10k events/sec |
| WebSocket connections | 10k+ concurrent |

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `HTTP_URL` | API Gateway HTTP URL | `http://localhost:8080` |
| `WS_URL` | WebSocket URL | `ws://localhost:8080/ws` |
| `SCENARIO` | Test scenario name | `smoke` |

## Output and Reporting

### k6 Output

```bash
# JSON output
k6 run --out json=results.json k6/http_endpoints.js

# InfluxDB output (for Grafana)
k6 run --out influxdb=http://localhost:8086/k6 k6/http_endpoints.js

# Cloud output (k6 Cloud)
k6 cloud k6/http_endpoints.js
```

### Generator Metrics

The generator logs metrics every 5 seconds:
```json
{
  "level": "INFO",
  "msg": "metrics",
  "published": 5000,
  "rate_per_sec": 1000,
  "errors": 0,
  "bytes_sent_mb": 2,
  "avg_latency_ms": 1.5
}
```

## Troubleshooting

### WebSocket Connection Failures

1. Check API Gateway is running: `curl http://localhost:8080/health`
2. Verify WebSocket endpoint: `wscat -c ws://localhost:8080/ws`
3. Check Redis connectivity for subscriptions

### Event Generator Failures

1. Verify NATS is running: `nats server check`
2. Check stream exists: `nats stream info CANONICAL_EVENTS`
3. Review NATS connection settings

### High Latency

1. Check Redis subscription matching performance
2. Verify NATS consumer configuration
3. Review API Gateway connection pool settings

## Extending Tests

### Adding Custom Scenarios

Edit `k6/config.js` to add new scenarios:

```javascript
export const config = {
  scenarios: {
    custom: {
      stages: [
        { duration: '5m', target: 500 },
        { duration: '10m', target: 500 },
        { duration: '5m', target: 0 },
      ],
    },
  },
};
```

### Adding Custom Metrics

```javascript
import { Trend } from 'k6/metrics';

const customLatency = new Trend('custom_operation_latency_ms');

export default function () {
  const start = Date.now();
  // ... operation ...
  customLatency.add(Date.now() - start);
}
```
