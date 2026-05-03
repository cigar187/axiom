-- Migration 016 — Add ks_misses and hits_misses to pitcher_warning_flags
--
-- Strategy: additive-only. Adds two integer columns to record how many of
-- the last 3 starts a pitcher missed their sim median for Ks and hits.
--
-- Tables affected:
--   pitcher_warning_flags   — two new columns

-- ── UPGRADE ──────────────────────────────────────────────────────────────────

ALTER TABLE pitcher_warning_flags
    ADD COLUMN IF NOT EXISTS ks_misses   INTEGER,
    ADD COLUMN IF NOT EXISTS hits_misses INTEGER;

-- ── CONFIRM ──────────────────────────────────────────────────────────────────

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'pitcher_warning_flags'
ORDER BY ordinal_position;

-- ── DOWNGRADE (run manually if rollback needed) ───────────────────────────────

-- ALTER TABLE pitcher_warning_flags
--     DROP COLUMN IF EXISTS ks_misses,
--     DROP COLUMN IF EXISTS hits_misses;
