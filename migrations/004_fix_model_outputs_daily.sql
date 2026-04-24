-- ─────────────────────────────────────────────────────────────
-- Axiom DB Migration 004 — Fix model_outputs_daily
--
-- Adds ALL missing columns to model_outputs_daily that the
-- pipeline code references. Safe to run multiple times
-- (uses ADD COLUMN IF NOT EXISTS throughout).
--
-- Run this from your Mac terminal with the proxy running:
--   PGPASSWORD='AxiomGTMVelo2026!' psql \
--     "host=127.0.0.1 port=5434 dbname=axiom_db user=axiom_user sslmode=disable" \
--     -f migrations/004_fix_model_outputs_daily.sql
-- ─────────────────────────────────────────────────────────────

-- PFF columns (Pitcher Form Factor)
ALTER TABLE model_outputs_daily
    ADD COLUMN IF NOT EXISTS pff_score          FLOAT       DEFAULT 0.0,
    ADD COLUMN IF NOT EXISTS pff_label          VARCHAR(32) DEFAULT 'NEUTRAL';

-- Risk profile columns (auto-computed by pipeline each day)
ALTER TABLE model_outputs_daily
    ADD COLUMN IF NOT EXISTS risk_score         INTEGER     DEFAULT 0,
    ADD COLUMN IF NOT EXISTS risk_tier          VARCHAR(16) DEFAULT 'LOW',
    ADD COLUMN IF NOT EXISTS risk_flags         VARCHAR(256),
    ADD COLUMN IF NOT EXISTS combo_risk         BOOLEAN     DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS season_era_tier    VARCHAR(16) DEFAULT 'NORMAL',
    ADD COLUMN IF NOT EXISTS park_extreme       BOOLEAN     DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS park_hits_multiplier FLOAT     DEFAULT 1.0;

-- Verify all columns now exist
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'model_outputs_daily'
  AND column_name IN (
    'pff_score','pff_label',
    'risk_score','risk_tier','risk_flags','combo_risk',
    'season_era_tier','park_extreme','park_hits_multiplier'
  )
ORDER BY column_name;
