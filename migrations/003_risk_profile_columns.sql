-- ─────────────────────────────────────────────────────────────
-- Axiom DB Migration — Risk Profile Columns
-- Run this ONCE against the Cloud SQL axiom_db database
-- before deploying the updated code.
--
-- New columns added to model_outputs_daily:
--   risk_score, risk_tier, risk_flags, combo_risk,
--   season_era_tier, park_extreme, park_hits_multiplier
--
-- Also expands pff_label from VARCHAR(16) to VARCHAR(32)
-- to accommodate labels like "HOT/BOOM-BUST/VEL"
-- ─────────────────────────────────────────────────────────────

-- Risk profile columns (auto-computed by pipeline each day)
ALTER TABLE model_outputs_daily
    ADD COLUMN IF NOT EXISTS risk_score         INTEGER     DEFAULT 0,
    ADD COLUMN IF NOT EXISTS risk_tier          VARCHAR(16) DEFAULT 'LOW',
    ADD COLUMN IF NOT EXISTS risk_flags         VARCHAR(256),
    ADD COLUMN IF NOT EXISTS combo_risk         BOOLEAN     DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS season_era_tier    VARCHAR(16) DEFAULT 'NORMAL',
    ADD COLUMN IF NOT EXISTS park_extreme       BOOLEAN     DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS park_hits_multiplier FLOAT     DEFAULT 1.0;

-- Widen pff_label to hold compound labels (e.g. "HOT/BOOM-BUST/VEL")
ALTER TABLE model_outputs_daily
    ALTER COLUMN pff_label TYPE VARCHAR(32);

-- Verify
SELECT column_name, data_type, character_maximum_length
FROM information_schema.columns
WHERE table_name = 'model_outputs_daily'
  AND column_name IN (
    'risk_score','risk_tier','risk_flags','combo_risk',
    'season_era_tier','park_extreme','park_hits_multiplier','pff_label'
  )
ORDER BY column_name;
