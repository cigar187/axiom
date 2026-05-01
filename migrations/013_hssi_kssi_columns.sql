-- Migration 013 — Add HSSI/KSSI columns alongside existing HUSI/KUSI columns
--
-- Strategy: additive-only. No existing columns are renamed or dropped.
-- The live database retains all husi/kusi columns for backward compatibility
-- with Tiltbox and any other consumers reading those fields.
-- New hssi/kssi columns are written by the updated pipeline going forward.
--
-- Tables affected:
--   model_outputs_daily   — 8 new index score columns
--   ml_training_samples   — 2 new formula output columns
--   ml_model_outputs      — 10 new ML prediction / comparison columns
--   axiom_pitcher_stats   — 4 new proprietary layer columns

-- ── UPGRADE ──────────────────────────────────────────────────────────────────

-- Table 1: model_outputs_daily
ALTER TABLE model_outputs_daily ADD COLUMN IF NOT EXISTS hssi FLOAT;
ALTER TABLE model_outputs_daily ADD COLUMN IF NOT EXISTS kssi FLOAT;
ALTER TABLE model_outputs_daily ADD COLUMN IF NOT EXISTS hssi_base FLOAT;
ALTER TABLE model_outputs_daily ADD COLUMN IF NOT EXISTS kssi_base FLOAT;
ALTER TABLE model_outputs_daily ADD COLUMN IF NOT EXISTS hssi_interaction FLOAT;
ALTER TABLE model_outputs_daily ADD COLUMN IF NOT EXISTS kssi_interaction FLOAT;
ALTER TABLE model_outputs_daily ADD COLUMN IF NOT EXISTS hssi_volatility FLOAT;
ALTER TABLE model_outputs_daily ADD COLUMN IF NOT EXISTS kssi_volatility FLOAT;

-- Table 2: ml_training_samples
ALTER TABLE ml_training_samples ADD COLUMN IF NOT EXISTS formula_hssi FLOAT;
ALTER TABLE ml_training_samples ADD COLUMN IF NOT EXISTS formula_kssi FLOAT;

-- Table 3: ml_model_outputs
ALTER TABLE ml_model_outputs ADD COLUMN IF NOT EXISTS ml_hssi FLOAT;
ALTER TABLE ml_model_outputs ADD COLUMN IF NOT EXISTS ml_kssi FLOAT;
ALTER TABLE ml_model_outputs ADD COLUMN IF NOT EXISTS ml_hssi_grade VARCHAR(4);
ALTER TABLE ml_model_outputs ADD COLUMN IF NOT EXISTS ml_kssi_grade VARCHAR(4);
ALTER TABLE ml_model_outputs ADD COLUMN IF NOT EXISTS hssi_delta FLOAT;
ALTER TABLE ml_model_outputs ADD COLUMN IF NOT EXISTS kssi_delta FLOAT;
ALTER TABLE ml_model_outputs ADD COLUMN IF NOT EXISTS hssi_divergence VARCHAR(16);
ALTER TABLE ml_model_outputs ADD COLUMN IF NOT EXISTS kssi_divergence VARCHAR(16);
ALTER TABLE ml_model_outputs ADD COLUMN IF NOT EXISTS consensus_hssi_grade VARCHAR(8);
ALTER TABLE ml_model_outputs ADD COLUMN IF NOT EXISTS consensus_kssi_grade VARCHAR(8);

-- Table 4: axiom_pitcher_stats
ALTER TABLE axiom_pitcher_stats ADD COLUMN IF NOT EXISTS axiom_hssi_avg FLOAT;
ALTER TABLE axiom_pitcher_stats ADD COLUMN IF NOT EXISTS axiom_kssi_avg FLOAT;
ALTER TABLE axiom_pitcher_stats ADD COLUMN IF NOT EXISTS axiom_hssi_trend FLOAT;
ALTER TABLE axiom_pitcher_stats ADD COLUMN IF NOT EXISTS axiom_kssi_trend FLOAT;

-- ── DOWNGRADE (reverse order — run manually if rollback needed) ───────────────
-- NOTE: Only removes the NEW columns added in this migration.
-- Existing husi/kusi columns are NOT touched by downgrade.

-- ALTER TABLE axiom_pitcher_stats DROP COLUMN IF EXISTS axiom_kssi_trend;
-- ALTER TABLE axiom_pitcher_stats DROP COLUMN IF EXISTS axiom_hssi_trend;
-- ALTER TABLE axiom_pitcher_stats DROP COLUMN IF EXISTS axiom_kssi_avg;
-- ALTER TABLE axiom_pitcher_stats DROP COLUMN IF EXISTS axiom_hssi_avg;

-- ALTER TABLE ml_model_outputs DROP COLUMN IF EXISTS consensus_kssi_grade;
-- ALTER TABLE ml_model_outputs DROP COLUMN IF EXISTS consensus_hssi_grade;
-- ALTER TABLE ml_model_outputs DROP COLUMN IF EXISTS kssi_divergence;
-- ALTER TABLE ml_model_outputs DROP COLUMN IF EXISTS hssi_divergence;
-- ALTER TABLE ml_model_outputs DROP COLUMN IF EXISTS kssi_delta;
-- ALTER TABLE ml_model_outputs DROP COLUMN IF EXISTS hssi_delta;
-- ALTER TABLE ml_model_outputs DROP COLUMN IF EXISTS ml_kssi_grade;
-- ALTER TABLE ml_model_outputs DROP COLUMN IF EXISTS ml_hssi_grade;
-- ALTER TABLE ml_model_outputs DROP COLUMN IF EXISTS ml_kssi;
-- ALTER TABLE ml_model_outputs DROP COLUMN IF EXISTS ml_hssi;

-- ALTER TABLE ml_training_samples DROP COLUMN IF EXISTS formula_kssi;
-- ALTER TABLE ml_training_samples DROP COLUMN IF EXISTS formula_hssi;

-- ALTER TABLE model_outputs_daily DROP COLUMN IF EXISTS kssi_volatility;
-- ALTER TABLE model_outputs_daily DROP COLUMN IF EXISTS hssi_volatility;
-- ALTER TABLE model_outputs_daily DROP COLUMN IF EXISTS kssi_interaction;
-- ALTER TABLE model_outputs_daily DROP COLUMN IF EXISTS hssi_interaction;
-- ALTER TABLE model_outputs_daily DROP COLUMN IF EXISTS kssi_base;
-- ALTER TABLE model_outputs_daily DROP COLUMN IF EXISTS hssi_base;
-- ALTER TABLE model_outputs_daily DROP COLUMN IF EXISTS kssi;
-- ALTER TABLE model_outputs_daily DROP COLUMN IF EXISTS hssi;
