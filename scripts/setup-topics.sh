#!/bin/bash
# Setup Kafka/Redpanda topics for Pulse
#
# Usage: ./setup-topics.sh [broker_addr]
# Default broker: localhost:9092

set -euo pipefail

BROKER="${1:-localhost:9092}"

echo "Setting up Kafka topics on $BROKER..."

# Topic configurations
# Function invocations topic - partitioned by function ID for ordering
# Each partition handles invocations for a subset of functions
create_topic() {
    local name=$1
    local partitions=$2
    local replication=$3
    local retention_ms=${4:-604800000}  # 7 days default

    echo "Creating topic: $name (partitions=$partitions, replication=$replication)"

    rpk topic create "$name" \
        --brokers "$BROKER" \
        --partitions "$partitions" \
        --replicas "$replication" \
        --topic-config "retention.ms=$retention_ms" \
        --topic-config "cleanup.policy=delete" \
        2>/dev/null || echo "  Topic $name already exists or creation failed"
}

# Core streaming topics
create_topic "raw-events" 12 1 604800000           # Raw blockchain events
create_topic "canonical-events" 24 1 604800000     # Normalized canonical events

# WASM function execution topics
create_topic "function-invocations" 16 1 86400000  # Function invocation requests (1 day retention)
create_topic "function-results" 8 1 259200000      # Function execution results (3 days)

# Billing and metering
create_topic "billing-events" 4 1 2592000000       # Billing/usage events (30 days)

# Dead letter queues
create_topic "dlq-function-invocations" 4 1 604800000  # Failed invocations

echo ""
echo "Topic setup complete. Current topics:"
rpk topic list --brokers "$BROKER"
