#!/bin/bash
# Burst Load Test Runner
# Project Pulse - Milestone 4: Performance & WebSocket Scale
#
# Orchestrates a full burst load test:
# 1. Starts the event generator with burst mode
# 2. Runs k6 burst simulation tests
# 3. Collects and reports results
#
# Usage:
#   ./run_burst_test.sh                     # Default configuration
#   ./run_burst_test.sh --rate 5000         # Custom event rate
#   ./run_burst_test.sh --burst-ratio 15    # 15x burst multiplier
#   ./run_burst_test.sh --duration 5m       # 5 minute test

set -euo pipefail

# Configuration with defaults
RATE="${RATE:-1000}"
BURST_RATIO="${BURST_RATIO:-10}"
BURST_PERIOD="${BURST_PERIOD:-30s}"
DURATION="${DURATION:-3m}"
NATS_URL="${NATS_URL:-nats://localhost:4222}"
HTTP_URL="${HTTP_URL:-http://localhost:8080}"
WS_URL="${WS_URL:-ws://localhost:8080/ws}"
OUTPUT_DIR="${OUTPUT_DIR:-./results}"
CHAINS="${CHAINS:-ethereum,solana}"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --rate)
            RATE="$2"
            shift 2
            ;;
        --burst-ratio)
            BURST_RATIO="$2"
            shift 2
            ;;
        --burst-period)
            BURST_PERIOD="$2"
            shift 2
            ;;
        --duration)
            DURATION="$2"
            shift 2
            ;;
        --nats-url)
            NATS_URL="$2"
            shift 2
            ;;
        --http-url)
            HTTP_URL="$2"
            shift 2
            ;;
        --ws-url)
            WS_URL="$2"
            shift 2
            ;;
        --output-dir)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --chains)
            CHAINS="$2"
            shift 2
            ;;
        -h|--help)
            echo "Burst Load Test Runner"
            echo ""
            echo "Usage: $0 [options]"
            echo ""
            echo "Options:"
            echo "  --rate NUM           Base event rate per second (default: 1000)"
            echo "  --burst-ratio NUM    Burst multiplier (default: 10)"
            echo "  --burst-period DUR   Period between bursts (default: 30s)"
            echo "  --duration DUR       Total test duration (default: 3m)"
            echo "  --nats-url URL       NATS server URL (default: nats://localhost:4222)"
            echo "  --http-url URL       HTTP API URL (default: http://localhost:8080)"
            echo "  --ws-url URL         WebSocket URL (default: ws://localhost:8080/ws)"
            echo "  --output-dir DIR     Results output directory (default: ./results)"
            echo "  --chains LIST        Comma-separated chains (default: ethereum,solana)"
            echo "  -h, --help           Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOAD_DIR="$(dirname "$SCRIPT_DIR")"
GENERATOR_DIR="$LOAD_DIR/generator"
K6_DIR="$LOAD_DIR/k6"

# Create output directory
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULT_DIR="$OUTPUT_DIR/burst_test_$TIMESTAMP"
mkdir -p "$RESULT_DIR"

echo "=========================================="
echo "Pulse Burst Load Test"
echo "=========================================="
echo "Configuration:"
echo "  Rate:         $RATE events/sec"
echo "  Burst Ratio:  ${BURST_RATIO}x"
echo "  Burst Period: $BURST_PERIOD"
echo "  Duration:     $DURATION"
echo "  NATS URL:     $NATS_URL"
echo "  HTTP URL:     $HTTP_URL"
echo "  WS URL:       $WS_URL"
echo "  Chains:       $CHAINS"
echo "  Output:       $RESULT_DIR"
echo "=========================================="

# Check prerequisites
check_prereq() {
    if ! command -v $1 &> /dev/null; then
        echo "Warning: $1 not found. Some tests may be skipped."
        return 1
    fi
    return 0
}

# Cleanup function
cleanup() {
    echo ""
    echo "Cleaning up..."
    if [[ -n "${GENERATOR_PID:-}" ]]; then
        kill "$GENERATOR_PID" 2>/dev/null || true
        wait "$GENERATOR_PID" 2>/dev/null || true
    fi
    echo "Cleanup complete."
}
trap cleanup EXIT

# Pre-flight health check
echo ""
echo "Pre-flight checks..."

if curl -sf "$HTTP_URL/health" > /dev/null 2>&1; then
    echo "✓ API Gateway is healthy"
else
    echo "✗ API Gateway not responding at $HTTP_URL/health"
    echo "  Please ensure the API gateway is running."
    exit 1
fi

# Check NATS connectivity
if check_prereq nats; then
    if nats server check --server="$NATS_URL" 2>/dev/null; then
        echo "✓ NATS server is healthy"
    else
        echo "⚠ NATS server check failed (may still work)"
    fi
fi

echo ""
echo "Starting burst load test..."
echo ""

# Build and start event generator
echo "Building event generator..."
cd "$GENERATOR_DIR"
go build -o event-generator . 2>&1

echo "Starting event generator with burst mode..."
./event-generator \
    -nats "$NATS_URL" \
    -rate "$RATE" \
    -duration "$DURATION" \
    -chains "$CHAINS" \
    -burst \
    -burst-ratio "$BURST_RATIO" \
    -burst-period "$BURST_PERIOD" \
    > "$RESULT_DIR/generator.log" 2>&1 &
GENERATOR_PID=$!
echo "Generator started (PID: $GENERATOR_PID)"

# Wait for generator to connect
sleep 2

# Run k6 tests if available
if check_prereq k6; then
    echo ""
    echo "Starting k6 burst simulation..."

    cd "$K6_DIR"
    k6 run \
        -e HTTP_URL="$HTTP_URL" \
        -e WS_URL="$WS_URL" \
        -e BURST_RATIO="$BURST_RATIO" \
        --out json="$RESULT_DIR/k6_results.json" \
        burst_simulation.js 2>&1 | tee "$RESULT_DIR/k6_output.log"

    echo ""
    echo "k6 test complete"
else
    echo ""
    echo "k6 not available, waiting for generator to complete..."
    wait "$GENERATOR_PID" || true
fi

# Wait for generator to finish
echo ""
echo "Waiting for event generator to complete..."
wait "$GENERATOR_PID" || true
GENERATOR_PID=""

# Generate summary
echo ""
echo "=========================================="
echo "Test Complete"
echo "=========================================="
echo ""
echo "Results saved to: $RESULT_DIR"
echo ""

# Parse generator log for summary
if [[ -f "$RESULT_DIR/generator.log" ]]; then
    echo "Generator Summary:"
    grep -E '"msg":"(generation complete|metrics)"' "$RESULT_DIR/generator.log" | tail -5
    echo ""
fi

# Parse k6 results if available
if [[ -f "$RESULT_DIR/k6_results.json" ]]; then
    echo "k6 Summary:"
    if check_prereq jq; then
        jq -r '.metric | select(.type == "trend") | "\(.data.name): p95=\(.data.value.p_95 // "N/A")ms, p99=\(.data.value.p_99 // "N/A")ms"' \
            "$RESULT_DIR/k6_results.json" 2>/dev/null | head -10 || echo "  (see $RESULT_DIR/k6_output.log for details)"
    else
        echo "  (install jq for JSON parsing, see $RESULT_DIR/k6_output.log)"
    fi
    echo ""
fi

echo "=========================================="
echo "Files generated:"
ls -la "$RESULT_DIR"
echo "=========================================="
