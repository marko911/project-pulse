-- Rollback: Drop manifests table and indexes

DROP INDEX IF EXISTS idx_manifests_mismatch;
DROP INDEX IF EXISTS idx_manifests_created;
DROP INDEX IF EXISTS idx_manifests_block_hash;
DROP INDEX IF EXISTS idx_manifests_chain_block;
DROP INDEX IF EXISTS idx_manifests_chain_block_unique;

DROP TABLE IF EXISTS manifests;
