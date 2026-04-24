-- ─────────────────────────────────────────────────────────────
-- Axiom DB Migration 005 — ML Engine Tables
--
-- Creates ml_training_samples and ml_model_outputs if they do not
-- exist, then safely adds any columns that may be missing from
-- older versions. Safe to run multiple times (idempotent).
--
-- Run from your Mac terminal with the proxy running in Tab 1:
--   PGPASSWORD='AxiomGTMVelo2026!' psql \
--     "host=127.0.0.1 port=5434 dbname=axiom_db user=axiom_user sslmode=disable" \
--     -f migrations/005_ml_tables.sql
-- ─────────────────────────────────────────────────────────────

-- ── Table 1: ml_training_samples ──────────────────────────────
CREATE TABLE IF NOT EXISTS ml_training_samples (
    id                  BIGSERIAL PRIMARY KEY,
    pitcher_id          VARCHAR(32)  NOT NULL,
    game_id             VARCHAR(32)  NOT NULL,
    game_date           DATE         NOT NULL,

    -- Formula block scores (inputs to ML)
    owc_score           FLOAT,
    pcs_score           FLOAT,
    ens_score           FLOAT,
    ops_score           FLOAT,
    uhs_score           FLOAT,
    dsc_score           FLOAT,
    ocr_score           FLOAT,
    pmr_score           FLOAT,
    per_score           FLOAT,
    kop_score           FLOAT,
    uks_score           FLOAT,
    tlr_score           FLOAT,

    -- Raw stats
    season_h9           FLOAT,
    season_k9           FLOAT,
    expected_ip         FLOAT,
    bullpen_fatigue_opp FLOAT        DEFAULT 0.0,
    bullpen_fatigue_own FLOAT        DEFAULT 0.0,
    ens_park            FLOAT,
    ens_temp            FLOAT,
    ens_air             FLOAT,

    -- Formula outputs
    formula_husi        FLOAT,
    formula_kusi        FLOAT,
    formula_proj_hits   FLOAT,
    formula_proj_ks     FLOAT,

    -- Actual outcomes (labeled after game completes)
    actual_hits         FLOAT,
    actual_ks           FLOAT,
    actual_ip           FLOAT,
    is_complete         BOOLEAN      DEFAULT FALSE,

    -- PFF
    pff_score           FLOAT        DEFAULT 0.0,
    pff_label           VARCHAR(32)  DEFAULT 'NEUTRAL',

    -- Risk profile
    risk_score          INTEGER      DEFAULT 0,
    risk_tier           VARCHAR(16)  DEFAULT 'LOW',
    risk_flags          VARCHAR(256),
    combo_risk          BOOLEAN      DEFAULT FALSE,
    season_era_tier     VARCHAR(16)  DEFAULT 'NORMAL',
    park_extreme        BOOLEAN      DEFAULT FALSE,
    park_hits_multiplier FLOAT       DEFAULT 1.0,

    -- Hidden variable inputs (SKUs #14, #37, #38)
    catcher_strike_rate FLOAT,
    tfi_rest_hours      FLOAT,
    tfi_tz_shift        INTEGER,
    vaa_degrees         FLOAT,
    extension_ft        FLOAT,

    -- First-inning detail
    first_inning_hits   FLOAT,
    first_inning_ks     FLOAT,

    created_at          TIMESTAMPTZ  DEFAULT NOW(),
    updated_at          TIMESTAMPTZ  DEFAULT NOW(),

    CONSTRAINT uq_ml_pitcher_game UNIQUE (pitcher_id, game_id)
);

CREATE INDEX IF NOT EXISTS ix_ml_training_game_date ON ml_training_samples (game_date);
CREATE INDEX IF NOT EXISTS ix_ml_training_pitcher_id ON ml_training_samples (pitcher_id);

-- Add any columns missing from older table versions
ALTER TABLE ml_training_samples ADD COLUMN IF NOT EXISTS pff_score           FLOAT        DEFAULT 0.0;
ALTER TABLE ml_training_samples ADD COLUMN IF NOT EXISTS pff_label           VARCHAR(32)  DEFAULT 'NEUTRAL';
ALTER TABLE ml_training_samples ADD COLUMN IF NOT EXISTS risk_score          INTEGER      DEFAULT 0;
ALTER TABLE ml_training_samples ADD COLUMN IF NOT EXISTS risk_tier           VARCHAR(16)  DEFAULT 'LOW';
ALTER TABLE ml_training_samples ADD COLUMN IF NOT EXISTS risk_flags          VARCHAR(256);
ALTER TABLE ml_training_samples ADD COLUMN IF NOT EXISTS combo_risk          BOOLEAN      DEFAULT FALSE;
ALTER TABLE ml_training_samples ADD COLUMN IF NOT EXISTS season_era_tier     VARCHAR(16)  DEFAULT 'NORMAL';
ALTER TABLE ml_training_samples ADD COLUMN IF NOT EXISTS park_extreme        BOOLEAN      DEFAULT FALSE;
ALTER TABLE ml_training_samples ADD COLUMN IF NOT EXISTS park_hits_multiplier FLOAT       DEFAULT 1.0;
ALTER TABLE ml_training_samples ADD COLUMN IF NOT EXISTS catcher_strike_rate FLOAT;
ALTER TABLE ml_training_samples ADD COLUMN IF NOT EXISTS tfi_rest_hours      FLOAT;
ALTER TABLE ml_training_samples ADD COLUMN IF NOT EXISTS tfi_tz_shift        INTEGER;
ALTER TABLE ml_training_samples ADD COLUMN IF NOT EXISTS vaa_degrees         FLOAT;
ALTER TABLE ml_training_samples ADD COLUMN IF NOT EXISTS extension_ft        FLOAT;
ALTER TABLE ml_training_samples ADD COLUMN IF NOT EXISTS first_inning_hits   FLOAT;
ALTER TABLE ml_training_samples ADD COLUMN IF NOT EXISTS first_inning_ks     FLOAT;


-- ── Table 2: ml_model_outputs ─────────────────────────────────
CREATE TABLE IF NOT EXISTS ml_model_outputs (
    id                    BIGSERIAL PRIMARY KEY,
    pitcher_id            VARCHAR(32)  NOT NULL,
    game_id               VARCHAR(32)  NOT NULL,
    game_date             DATE         NOT NULL,

    -- ML predictions
    ml_proj_hits          FLOAT,
    ml_proj_ks            FLOAT,
    ml_husi               FLOAT,
    ml_kusi               FLOAT,
    ml_husi_grade         VARCHAR(4),
    ml_kusi_grade         VARCHAR(4),

    -- Comparison to formula
    husi_delta            FLOAT,
    kusi_delta            FLOAT,
    husi_divergence       VARCHAR(16),
    kusi_divergence       VARCHAR(16),
    consensus_husi_grade  VARCHAR(8),
    consensus_kusi_grade  VARCHAR(8),

    -- Model metadata
    model_version         VARCHAR(32),
    training_samples      INTEGER,
    mae_hits              FLOAT,
    mae_ks                FLOAT,

    created_at            TIMESTAMPTZ  DEFAULT NOW(),
    updated_at            TIMESTAMPTZ  DEFAULT NOW(),

    CONSTRAINT uq_ml_output_pitcher_game UNIQUE (pitcher_id, game_id)
);

CREATE INDEX IF NOT EXISTS ix_ml_outputs_game_date ON ml_model_outputs (game_date);

-- Add any columns missing from older table versions
ALTER TABLE ml_model_outputs ADD COLUMN IF NOT EXISTS consensus_husi_grade  VARCHAR(8);
ALTER TABLE ml_model_outputs ADD COLUMN IF NOT EXISTS consensus_kusi_grade  VARCHAR(8);
ALTER TABLE ml_model_outputs ADD COLUMN IF NOT EXISTS model_version         VARCHAR(32);
ALTER TABLE ml_model_outputs ADD COLUMN IF NOT EXISTS training_samples      INTEGER;
ALTER TABLE ml_model_outputs ADD COLUMN IF NOT EXISTS mae_hits              FLOAT;
ALTER TABLE ml_model_outputs ADD COLUMN IF NOT EXISTS mae_ks                FLOAT;


-- ── Verification ──────────────────────────────────────────────
SELECT 'ml_training_samples columns:' AS info, COUNT(*) AS col_count
FROM information_schema.columns
WHERE table_name = 'ml_training_samples';

SELECT 'ml_model_outputs columns:' AS info, COUNT(*) AS col_count
FROM information_schema.columns
WHERE table_name = 'ml_model_outputs';
