-- Migration: Create transactional outbox table
-- Implements the Transactional Outbox pattern for guaranteed event delivery

CREATE TABLE IF NOT EXISTS outbox (
    -- Monotonically increasing ID for strict ordering
    id              BIGSERIAL PRIMARY KEY,

    -- Reference to the canonical event
    event_id        TEXT NOT NULL,

    -- Target topic for publishing
    topic           TEXT NOT NULL DEFAULT 'canonical-events',

    -- Partition key (typically chain + some identifier for ordering)
    partition_key   TEXT NOT NULL,

    -- Serialized event payload (protobuf or JSON)
    payload         BYTEA NOT NULL,

    -- Event metadata for routing
    chain           SMALLINT NOT NULL,
    event_type      TEXT NOT NULL,

    -- Processing state
    status          TEXT NOT NULL DEFAULT 'pending',
    -- Values: 'pending', 'processing', 'published', 'failed'

    -- Retry handling
    retry_count     INTEGER NOT NULL DEFAULT 0,
    max_retries     INTEGER NOT NULL DEFAULT 5,
    last_error      TEXT,

    -- Timestamps
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at    TIMESTAMPTZ,
    published_at    TIMESTAMPTZ,

    -- Constraints
    CONSTRAINT outbox_status_check
        CHECK (status IN ('pending', 'processing', 'published', 'failed'))
);

-- Index for the publisher to find pending messages in order
CREATE INDEX IF NOT EXISTS idx_outbox_pending
    ON outbox (id ASC)
    WHERE status = 'pending';

-- Index for finding messages to retry
CREATE INDEX IF NOT EXISTS idx_outbox_failed
    ON outbox (created_at ASC)
    WHERE status = 'failed' AND retry_count < max_retries;

-- Index for cleanup of old published messages
CREATE INDEX IF NOT EXISTS idx_outbox_published
    ON outbox (published_at ASC)
    WHERE status = 'published';

-- Index for correlation with events table
CREATE INDEX IF NOT EXISTS idx_outbox_event_id
    ON outbox (event_id);

-- Index for topic-based queries
CREATE INDEX IF NOT EXISTS idx_outbox_topic
    ON outbox (topic, chain);

-- Function to clean up old published messages (call periodically)
CREATE OR REPLACE FUNCTION cleanup_outbox(retention_interval INTERVAL DEFAULT '7 days')
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM outbox
    WHERE status = 'published'
      AND published_at < NOW() - retention_interval;

    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;
