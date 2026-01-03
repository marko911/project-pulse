-- Migration: Create manifests table for correctness audit trail
-- Manifests track the expected vs emitted counts for finalized blocks,
-- providing the foundation for correctness verification.

CREATE TABLE IF NOT EXISTS manifests (
    -- Primary identification
    id              BIGSERIAL,

    -- Chain and block context
    chain           SMALLINT NOT NULL,
    block_number    BIGINT NOT NULL,
    block_hash      TEXT NOT NULL,
    parent_hash     TEXT NOT NULL,

    -- Correctness verification counts
    expected_tx_count    INTEGER NOT NULL,
    expected_event_count INTEGER NOT NULL,
    emitted_tx_count     INTEGER NOT NULL,
    emitted_event_count  INTEGER NOT NULL,

    -- Integrity hash of all event IDs in deterministic order
    event_ids_hash  TEXT NOT NULL,

    -- Sources used to build this manifest (JSON array of source names)
    sources_used    JSONB NOT NULL DEFAULT '[]',

    -- Timestamps
    block_timestamp     TIMESTAMPTZ NOT NULL,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    manifest_created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Convert to TimescaleDB hypertable for time-series optimization
SELECT create_hypertable('manifests', 'block_timestamp',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Unique constraint: one manifest per chain/block (includes partitioning key)
CREATE UNIQUE INDEX IF NOT EXISTS idx_manifests_chain_block_unique
    ON manifests (chain, block_number, block_timestamp);

-- Index for fast lookups by chain and block number
CREATE INDEX IF NOT EXISTS idx_manifests_chain_block
    ON manifests (chain, block_number DESC);

-- Index for finding manifests by block hash (for reorg detection)
CREATE INDEX IF NOT EXISTS idx_manifests_block_hash
    ON manifests (chain, block_hash);

-- Index for finding recent manifests
CREATE INDEX IF NOT EXISTS idx_manifests_created
    ON manifests (manifest_created_at DESC);

-- Index for correctness mismatches (where expected != emitted)
CREATE INDEX IF NOT EXISTS idx_manifests_mismatch
    ON manifests (chain, block_number)
    WHERE expected_tx_count != emitted_tx_count
       OR expected_event_count != emitted_event_count;
