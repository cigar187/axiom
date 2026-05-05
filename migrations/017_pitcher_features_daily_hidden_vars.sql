-- ── Migration 017: Add hidden variable columns to pitcher_features_daily
-- ── These fields are already computed in PitcherFeatureSet and written to
--    model_outputs_daily. This migration surfaces them in pitcher_features_daily
--    for diagnostic queries and ML training.
--
-- UPGRADE

ALTER TABLE pitcher_features_daily ADD COLUMN IF NOT EXISTS catcher_strike_rate FLOAT;
ALTER TABLE pitcher_features_daily ADD COLUMN IF NOT EXISTS tfi_rest_hours FLOAT;
ALTER TABLE pitcher_features_daily ADD COLUMN IF NOT EXISTS tfi_tz_shift FLOAT;
ALTER TABLE pitcher_features_daily ADD COLUMN IF NOT EXISTS vaa_degrees FLOAT;
ALTER TABLE pitcher_features_daily ADD COLUMN IF NOT EXISTS extension_ft FLOAT;

-- Confirm
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'pitcher_features_daily'
  AND column_name IN ('catcher_strike_rate', 'tfi_rest_hours', 'tfi_tz_shift', 'vaa_degrees', 'extension_ft')
ORDER BY column_name;

-- DOWNGRADE (run manually if needed)
-- ALTER TABLE pitcher_features_daily DROP COLUMN IF EXISTS catcher_strike_rate;
-- ALTER TABLE pitcher_features_daily DROP COLUMN IF EXISTS tfi_rest_hours;
-- ALTER TABLE pitcher_features_daily DROP COLUMN IF EXISTS tfi_tz_shift;
-- ALTER TABLE pitcher_features_daily DROP COLUMN IF EXISTS vaa_degrees;
-- ALTER TABLE pitcher_features_daily DROP COLUMN IF EXISTS extension_ft;
