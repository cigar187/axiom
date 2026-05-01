-- Migration 014 — Add game total and moneyline columns to the games table
--
-- Strategy: additive-only. No existing columns are renamed or dropped.
-- These three columns capture the pre-game market lines from The Rundown API
-- and feed the GTS (Game Total Score) modifier in the HSSI/KSSI scoring engines.
--
-- Tables affected:
--   games   — 3 new market line columns

-- ── UPGRADE ──────────────────────────────────────────────────────────────────

ALTER TABLE games ADD COLUMN IF NOT EXISTS game_total FLOAT;
ALTER TABLE games ADD COLUMN IF NOT EXISTS home_moneyline INTEGER;
ALTER TABLE games ADD COLUMN IF NOT EXISTS away_moneyline INTEGER;

-- ── CONFIRM ──────────────────────────────────────────────────────────────────

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'games'
  AND column_name IN ('game_total', 'home_moneyline', 'away_moneyline')
ORDER BY column_name;

-- ── DOWNGRADE (run manually if rollback needed) ───────────────────────────────

-- ALTER TABLE games DROP COLUMN IF EXISTS away_moneyline;
-- ALTER TABLE games DROP COLUMN IF EXISTS home_moneyline;
-- ALTER TABLE games DROP COLUMN IF EXISTS game_total;
