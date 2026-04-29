-- =============================================================================
-- Migration 009: Entropy Filter columns
-- Adds hits_entropy, ks_entropy, entropy_label to model_outputs_daily.
-- Computed after both Engine 1 (formula) and the ML model have run.
--
-- hits_entropy  = |Engine 1 projected_hits − ML projected_hits|
-- ks_entropy    = |Engine 1 projected_ks   − ML projected_ks|
-- entropy_label = ALIGNED / DIVERGING / HIGH_ENTROPY
--
-- Safe to run multiple times — uses ADD COLUMN IF NOT EXISTS.
-- =============================================================================

ALTER TABLE model_outputs_daily ADD COLUMN IF NOT EXISTS hits_entropy   FLOAT;
ALTER TABLE model_outputs_daily ADD COLUMN IF NOT EXISTS ks_entropy     FLOAT;
ALTER TABLE model_outputs_daily ADD COLUMN IF NOT EXISTS entropy_label  VARCHAR(16);

SELECT 'migration 009 complete — entropy filter columns added to model_outputs_daily' AS status;
