-- Rollback: Drop function registry tables

DROP TABLE IF EXISTS invocations;
DROP TABLE IF EXISTS deployments;
DROP TABLE IF EXISTS triggers;
DROP TABLE IF EXISTS functions;
