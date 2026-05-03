-- Migration 015 — Add pitcher_warning_flags table
--
-- Strategy: additive-only. Creates a new table to record post-game
-- performance flags when a pitcher's actual results fall below their
-- Merlin simulation floor for Ks or hits allowed.
--
-- Tables affected:
--   pitcher_warning_flags   — new table

-- ── UPGRADE ──────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS pitcher_warning_flags (
    id               SERIAL PRIMARY KEY,
    pitcher_id       INTEGER,
    pitcher_name     VARCHAR(100)  NOT NULL,
    game_date        DATE          NOT NULL,
    actual_ks        FLOAT,
    floor_ks         FLOAT,
    actual_hits      FLOAT,
    floor_hits       FLOAT,
    flag_type        VARCHAR(20)   NOT NULL,
    created_at       TIMESTAMP     DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pwf_game_date
    ON pitcher_warning_flags (game_date);

CREATE INDEX IF NOT EXISTS idx_pwf_pitcher_id
    ON pitcher_warning_flags (pitcher_id);

CREATE INDEX IF NOT EXISTS idx_pwf_flag_type
    ON pitcher_warning_flags (flag_type);

-- ── CONFIRM ──────────────────────────────────────────────────────────────────

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'pitcher_warning_flags'
ORDER BY ordinal_position;

-- ── DOWNGRADE (run manually if rollback needed) ───────────────────────────────

-- DROP TABLE IF EXISTS pitcher_warning_flags;
