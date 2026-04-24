-- =============================================================================
-- Migration 006: Merlin Simulation Engine output columns
-- Adds Monte Carlo simulation results (N=2000) to model_outputs_daily.
--
-- Safe to run multiple times — all statements use IF NOT EXISTS / ADD COLUMN IF NOT EXISTS.
-- =============================================================================

-- ── Simulation output columns on model_outputs_daily
ALTER TABLE model_outputs_daily ADD COLUMN IF NOT EXISTS sim_median_hits      FLOAT;
ALTER TABLE model_outputs_daily ADD COLUMN IF NOT EXISTS sim_median_ks        FLOAT;
ALTER TABLE model_outputs_daily ADD COLUMN IF NOT EXISTS sim_over_pct_hits    FLOAT;
ALTER TABLE model_outputs_daily ADD COLUMN IF NOT EXISTS sim_under_pct_hits   FLOAT;
ALTER TABLE model_outputs_daily ADD COLUMN IF NOT EXISTS sim_p5_hits          FLOAT;
ALTER TABLE model_outputs_daily ADD COLUMN IF NOT EXISTS sim_p95_hits         FLOAT;
ALTER TABLE model_outputs_daily ADD COLUMN IF NOT EXISTS sim_over_pct_ks      FLOAT;
ALTER TABLE model_outputs_daily ADD COLUMN IF NOT EXISTS sim_under_pct_ks     FLOAT;
ALTER TABLE model_outputs_daily ADD COLUMN IF NOT EXISTS sim_p5_ks            FLOAT;
ALTER TABLE model_outputs_daily ADD COLUMN IF NOT EXISTS sim_p95_ks           FLOAT;
ALTER TABLE model_outputs_daily ADD COLUMN IF NOT EXISTS sim_confidence_hits  VARCHAR(16);
ALTER TABLE model_outputs_daily ADD COLUMN IF NOT EXISTS sim_confidence_ks    VARCHAR(16);
ALTER TABLE model_outputs_daily ADD COLUMN IF NOT EXISTS sim_kill_streak_prob FLOAT;

-- ── Verify
SELECT 'migration 006 complete — simulation columns added to model_outputs_daily' AS status;
