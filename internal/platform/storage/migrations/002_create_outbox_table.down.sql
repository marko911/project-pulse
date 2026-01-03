-- Rollback: Drop outbox table and associated objects
DROP FUNCTION IF EXISTS cleanup_outbox;
DROP TABLE IF EXISTS outbox CASCADE;
