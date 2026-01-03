#!/bin/bash
# High-Throughput Ingestion Burst Test
# Project Pulse - Milestone 4: Performance & WebSocket Scale
#
# Runs the event generator in various burst configurations to test
# ingestion pipeline resilience.
#
# Usage:
#   ./run_ingestion_burst.sh                    # Default test
#   ./run_ingestion_burst.sh sustained         # Sustained high load
#   ./run_ingestion_burst.sh spike             # Spike traffic pattern
#   ./run_ingestion_burst.sh gradual           # Gradual ramp up

set -euo pipefail

# Configuration
NATS_URL="${NATS_URL:-nats://localhost:4222}"
CHAINS="${CHAINS:-ethereum,solana,polygon}"
OUTPUT_DIR="${OUTPUT_DIR:-./results}"

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GENERATOR_DIR="$(dirname "$SCRIPT_DIR")/generator"

# Build generator
echo "Building event generator..."
cd "$GENERATOR_DIR"
go build -o event-generator . 2>&1

# Test mode
MODE="${1:-burst}"

echo "=========================================="
echo "Ingestion Burst Test: $MODE"
echo "=========================================="

case "$MODE" in
    burst)
        # Default burst test: 5000 events/sec with 10x bursts every 30s
        echo "Mode: Standard burst (5k base, 10x bursts)"
        ./event-generator \
            -nats "$NATS_URL" \
            -rate 5000 \
            -duration 3m \
            -chains "$CHAINS" \
            -burst \
            -burst-ratio 10 \
            -burst-period 30s \
            -workers 8
        ;;

    sustained)
        # Sustained high load: 20k events/sec for 5 minutes
        echo "Mode: Sustained high load (20k events/sec)"
        ./event-generator \
            -nats "$NATS_URL" \
            -rate 20000 \
            -duration 5m \
            -chains "$CHAINS" \
            -workers 16
        ;;

    spike)
        # Spike test: 1k base with 50x spikes
        echo "Mode: Spike (1k base, 50x spikes)"
        ./event-generator \
            -nats "$NATS_URL" \
            -rate 1000 \
            -duration 5m \
            -chains "$CHAINS" \
            -burst \
            -burst-ratio 50 \
            -burst-period 60s \
            -workers 8
        ;;

    gradual)
        # Gradual ramp test: multiple runs with increasing rate
        echo "Mode: Gradual ramp up"
        for rate in 1000 2000 5000 10000 20000; do
            echo ""
            echo "Testing rate: $rate events/sec"
            ./event-generator \
                -nats "$NATS_URL" \
                -rate "$rate" \
                -duration 30s \
                -chains "$CHAINS" \
                -workers 8
            sleep 5
        done
        ;;

    stress)
        # Stress test: find breaking point
        echo "Mode: Stress test (finding limits)"
        echo "Starting at 10k, increasing until errors..."

        for rate in 10000 25000 50000 75000 100000; do
            echo ""
            echo "Testing rate: $rate events/sec"
            if ! timeout 60s ./event-generator \
                -nats "$NATS_URL" \
                -rate "$rate" \
                -duration 30s \
                -chains "$CHAINS" \
                -workers 16 2>&1 | tee /tmp/stress_$rate.log; then
                echo "Rate $rate caused issues"
                break
            fi
            sleep 10
        done
        ;;

    *)
        echo "Unknown mode: $MODE"
        echo ""
        echo "Available modes:"
        echo "  burst     - Standard burst test (5k base, 10x bursts)"
        echo "  sustained - Sustained high load (20k events/sec)"
        echo "  spike     - Spike traffic (1k base, 50x spikes)"
        echo "  gradual   - Gradual ramp up (1k -> 20k)"
        echo "  stress    - Stress test (find breaking point)"
        exit 1
        ;;
esac

echo ""
echo "=========================================="
echo "Test Complete"
echo "=========================================="
