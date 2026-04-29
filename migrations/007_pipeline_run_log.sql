-- =============================================================================
-- Migration 007: Pipeline run log table
-- Records every execution of the Axiom daily pipeline — success, failure,
-- and no-data runs — for observability and debugging.
--
-- Safe to run multiple times — uses CREATE TABLE IF NOT EXISTS.
-- =============================================================================

CREATE TABLE IF NOT EXISTS pipeline_run_log (
    id               BIGSERIAL PRIMARY KEY,
    run_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    target_date      DATE        NOT NULL,
    status           VARCHAR(16) NOT NULL,   -- 'success' | 'error' | 'no_data'
    pitchers_scored  INTEGER     NOT NULL DEFAULT 0,
    games_processed  INTEGER     NOT NULL DEFAULT 0,
    elapsed_seconds  FLOAT,
    error_message    TEXT,
    dry_run          BOOLEAN     NOT NULL DEFAULT false
);

CREATE INDEX IF NOT EXISTS ix_pipeline_run_log_run_at     ON pipeline_run_log (run_at);
CREATE INDEX IF NOT EXISTS ix_pipeline_run_log_target_date ON pipeline_run_log (target_date);

SELECT 'migration 007 complete — pipeline_run_log table created' AS status;
