-- ─────────────────────────────────────────────────────────────────────────────
-- Migration 012 — Add sport column to pipeline_run_log
-- Run date: 2026-04-30
--
-- Adds a nullable VARCHAR(8) sport column to the shared pipeline_run_log table
-- so that MLB, NFL, and NHL pipeline runs can be distinguished in the log.
-- Existing rows remain valid (sport = NULL for legacy MLB and NFL entries).
-- ─────────────────────────────────────────────────────────────────────────────

-- ── UPGRADE ──────────────────────────────────────────────────────────────────
ALTER TABLE pipeline_run_log
    ADD COLUMN IF NOT EXISTS sport VARCHAR(8);

COMMENT ON COLUMN pipeline_run_log.sport IS 'Sport identifier: MLB | NFL | NHL. NULL for legacy rows.';

-- ── DOWNGRADE (run manually to reverse) ──────────────────────────────────────
-- ALTER TABLE pipeline_run_log DROP COLUMN IF EXISTS sport;
