-- Migration: Create function registry tables for WASM compute platform
-- These tables store function metadata, triggers, and deployment history.

-- Functions table: stores function definitions
CREATE TABLE IF NOT EXISTS functions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       TEXT NOT NULL,
    name            TEXT NOT NULL,
    description     TEXT,

    -- Runtime configuration
    runtime_version TEXT NOT NULL DEFAULT '1.0',
    max_memory_mb   INTEGER NOT NULL DEFAULT 128,
    max_cpu_ms      INTEGER NOT NULL DEFAULT 5000,

    -- Current deployment info
    current_version INTEGER NOT NULL DEFAULT 0,
    module_hash     TEXT,
    module_size     INTEGER,

    -- Status
    status          TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'disabled', 'deleted')),

    -- Timestamps
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Unique name per tenant
    UNIQUE(tenant_id, name)
);

-- Indexes for functions
CREATE INDEX IF NOT EXISTS idx_functions_tenant ON functions (tenant_id);
CREATE INDEX IF NOT EXISTS idx_functions_status ON functions (status) WHERE status != 'deleted';
CREATE INDEX IF NOT EXISTS idx_functions_updated ON functions (updated_at DESC);

-- Triggers table: maps events to function invocations
CREATE TABLE IF NOT EXISTS triggers (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    function_id     UUID NOT NULL REFERENCES functions(id) ON DELETE CASCADE,
    tenant_id       TEXT NOT NULL,
    name            TEXT NOT NULL,

    -- Event matching criteria
    event_type      TEXT NOT NULL,       -- e.g., 'blockchain.event', 'http.request'
    filter_chain    SMALLINT,            -- Chain ID filter (null = all)
    filter_address  TEXT,                -- Contract address filter
    filter_topic    TEXT,                -- Event topic/signature filter
    filter_json     JSONB,               -- Additional filter criteria

    -- Execution configuration
    priority        INTEGER NOT NULL DEFAULT 100,
    max_retries     INTEGER NOT NULL DEFAULT 3,
    timeout_ms      INTEGER NOT NULL DEFAULT 5000,

    -- Status
    status          TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'disabled', 'deleted')),

    -- Timestamps
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes for triggers
CREATE INDEX IF NOT EXISTS idx_triggers_function ON triggers (function_id);
CREATE INDEX IF NOT EXISTS idx_triggers_tenant ON triggers (tenant_id);
CREATE INDEX IF NOT EXISTS idx_triggers_event_type ON triggers (event_type, status) WHERE status = 'active';
CREATE INDEX IF NOT EXISTS idx_triggers_chain ON triggers (filter_chain, status) WHERE status = 'active';
CREATE INDEX IF NOT EXISTS idx_triggers_address ON triggers (filter_address, status) WHERE status = 'active' AND filter_address IS NOT NULL;

-- Deployments table: tracks deployment history
CREATE TABLE IF NOT EXISTS deployments (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    function_id     UUID NOT NULL REFERENCES functions(id) ON DELETE CASCADE,
    tenant_id       TEXT NOT NULL,
    version         INTEGER NOT NULL,

    -- Module info
    module_hash     TEXT NOT NULL,
    module_size     INTEGER NOT NULL,
    module_path     TEXT NOT NULL,       -- S3/MinIO path

    -- Build info
    source_hash     TEXT,                -- Hash of source code if available
    build_log       TEXT,

    -- Deployment metadata
    deployed_by     TEXT,                -- User/agent that deployed
    deployment_note TEXT,

    -- Status
    status          TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'superseded', 'rollback', 'failed')),

    -- Timestamps
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    activated_at    TIMESTAMPTZ,

    -- Unique version per function
    UNIQUE(function_id, version)
);

-- Indexes for deployments
CREATE INDEX IF NOT EXISTS idx_deployments_function ON deployments (function_id, version DESC);
CREATE INDEX IF NOT EXISTS idx_deployments_tenant ON deployments (tenant_id);
CREATE INDEX IF NOT EXISTS idx_deployments_status ON deployments (function_id, status) WHERE status = 'active';
CREATE INDEX IF NOT EXISTS idx_deployments_created ON deployments (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_deployments_hash ON deployments (module_hash);

-- Invocations table: tracks function execution history (hypertable for time-series)
CREATE TABLE IF NOT EXISTS invocations (
    id              UUID DEFAULT gen_random_uuid(),
    function_id     UUID NOT NULL,
    trigger_id      UUID,
    tenant_id       TEXT NOT NULL,
    deployment_id   UUID NOT NULL,

    -- Request info
    request_id      TEXT NOT NULL,
    input_hash      TEXT,
    input_size      INTEGER,

    -- Execution results
    success         BOOLEAN NOT NULL,
    output_hash     TEXT,
    output_size     INTEGER,
    error_message   TEXT,

    -- Metrics
    duration_ms     INTEGER NOT NULL,
    memory_bytes    BIGINT,
    cpu_time_ms     INTEGER,

    -- Timing
    started_at      TIMESTAMPTZ NOT NULL,
    completed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (id, completed_at)
);

-- Convert to TimescaleDB hypertable
SELECT create_hypertable('invocations', 'completed_at',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Indexes for invocations
CREATE INDEX IF NOT EXISTS idx_invocations_function ON invocations (function_id, completed_at DESC);
CREATE INDEX IF NOT EXISTS idx_invocations_tenant ON invocations (tenant_id, completed_at DESC);
CREATE INDEX IF NOT EXISTS idx_invocations_trigger ON invocations (trigger_id, completed_at DESC) WHERE trigger_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_invocations_request ON invocations (request_id);
CREATE INDEX IF NOT EXISTS idx_invocations_errors ON invocations (function_id, completed_at DESC) WHERE success = false;
