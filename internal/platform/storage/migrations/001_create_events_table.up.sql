-- Migration: Create canonical events table
-- This table stores normalized blockchain events in a time-series optimized format

CREATE TABLE IF NOT EXISTS events (
    -- Primary identifier (composite for uniqueness)
    event_id        TEXT NOT NULL,

    -- Chain identification
    chain           SMALLINT NOT NULL,

    -- Block context
    block_number    BIGINT NOT NULL,
    block_hash      TEXT NOT NULL,
    block_timestamp TIMESTAMPTZ NOT NULL,

    -- Transaction context
    tx_hash         TEXT NOT NULL,
    tx_index        INTEGER NOT NULL,
    event_index     INTEGER NOT NULL,

    -- Event classification
    event_type      TEXT NOT NULL,
    program_id      TEXT,

    -- Associated accounts (stored as JSONB array for flexible querying)
    accounts        JSONB NOT NULL DEFAULT '[]',

    -- Event payload (chain-specific data)
    payload         BYTEA,

    -- Finality tracking
    commitment_level SMALLINT NOT NULL DEFAULT 1,

    -- Reorg handling
    reorg_action    SMALLINT NOT NULL DEFAULT 1,
    replaces_event_id TEXT,

    -- Value transfer (if applicable)
    native_value    BIGINT NOT NULL DEFAULT 0,

    -- Schema versioning for forward compatibility
    schema_version  INTEGER NOT NULL DEFAULT 1,

    -- Timestamps
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Convert to TimescaleDB hypertable for time-series optimization
SELECT create_hypertable('events', 'block_timestamp',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Unique constraint required by TimescaleDB (must include partitioning key)
CREATE UNIQUE INDEX IF NOT EXISTS idx_events_unique_id
    ON events (event_id, block_timestamp);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_events_chain_block
    ON events (chain, block_number DESC);

CREATE INDEX IF NOT EXISTS idx_events_chain_tx
    ON events (chain, tx_hash);

CREATE INDEX IF NOT EXISTS idx_events_event_type
    ON events (chain, event_type);

CREATE INDEX IF NOT EXISTS idx_events_program_id
    ON events (chain, program_id)
    WHERE program_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_events_commitment
    ON events (chain, commitment_level, block_number DESC);

CREATE INDEX IF NOT EXISTS idx_events_accounts
    ON events USING GIN (accounts);

CREATE INDEX IF NOT EXISTS idx_events_ingested
    ON events (ingested_at DESC);

-- Partial index for reorg-related events
CREATE INDEX IF NOT EXISTS idx_events_reorg
    ON events (chain, replaces_event_id)
    WHERE reorg_action != 1;
