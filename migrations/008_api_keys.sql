-- =============================================================================
-- Migration 008: B2B API keys table
-- Stores client API keys for authenticating requests to protected endpoints.
--
-- Safe to run multiple times — uses CREATE TABLE IF NOT EXISTS.
-- =============================================================================

CREATE TABLE IF NOT EXISTS api_keys (
    id          BIGSERIAL    PRIMARY KEY,
    key         VARCHAR(64)  NOT NULL UNIQUE,
    client_name VARCHAR(128) NOT NULL,
    active      BOOLEAN      NOT NULL DEFAULT true,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_api_keys_key ON api_keys (key);

SELECT 'migration 008 complete — api_keys table created' AS status;
