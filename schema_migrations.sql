-- ─────────────────────────────────────────────────────────────
-- Axiom Database Schema
-- Database: axiom_db (Google Cloud SQL — PostgreSQL)
-- Project:  axiom-gtmvelo
-- Run this once to create all tables.
-- ─────────────────────────────────────────────────────────────

-- Create the database (run as superuser if not already created)
-- CREATE DATABASE axiom_db;

-- Connect to axiom_db before running the rest of this file.
-- \c axiom_db

-- ─────────────────────────────────────────────────────────────
-- 1. games
-- One row per MLB game per day.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS games (
    game_id                  VARCHAR(32) PRIMARY KEY,
    game_date                DATE        NOT NULL,
    home_team                VARCHAR(64) NOT NULL,
    away_team                VARCHAR(64) NOT NULL,
    park                     VARCHAR(128),
    -- Weather
    temperature_f            FLOAT,
    wind_speed_mph           FLOAT,
    wind_direction           VARCHAR(64),
    is_dome                  BOOLEAN     DEFAULT FALSE,
    roof_type                VARCHAR(32),
    weather_condition        VARCHAR(64),
    -- Umpire assignment
    home_plate_umpire_id     VARCHAR(32),
    home_plate_umpire_name   VARCHAR(128),
    -- Status
    status                   VARCHAR(32) DEFAULT 'scheduled',
    created_at               TIMESTAMPTZ DEFAULT NOW(),
    updated_at               TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_games_date ON games (game_date);


-- ─────────────────────────────────────────────────────────────
-- 2. probable_pitchers
-- One row per probable starter per game.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS probable_pitchers (
    id             BIGSERIAL   PRIMARY KEY,
    pitcher_id     VARCHAR(32) NOT NULL,
    game_id        VARCHAR(32) NOT NULL REFERENCES games(game_id),
    team_id        VARCHAR(32) NOT NULL,
    pitcher_name   VARCHAR(128) NOT NULL,
    handedness     VARCHAR(4),
    confirmed_flag BOOLEAN     DEFAULT FALSE,
    created_at     TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_pitcher_game UNIQUE (pitcher_id, game_id)
);

CREATE INDEX IF NOT EXISTS idx_probable_pitchers_pitcher ON probable_pitchers (pitcher_id);


-- ─────────────────────────────────────────────────────────────
-- 3. sportsbook_props
-- Prop lines from The Rundown API.
-- market_type: 'strikeouts' | 'hits_allowed'
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sportsbook_props (
    prop_id      BIGSERIAL    PRIMARY KEY,
    game_id      VARCHAR(32)  NOT NULL REFERENCES games(game_id),
    pitcher_id   VARCHAR(32)  NOT NULL,
    sportsbook   VARCHAR(64)  NOT NULL,
    market_type  VARCHAR(32)  NOT NULL,
    line         FLOAT        NOT NULL,
    over_odds    FLOAT,
    under_odds   FLOAT,
    timestamp    TIMESTAMPTZ  DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_props_pitcher ON sportsbook_props (pitcher_id);
CREATE INDEX IF NOT EXISTS idx_props_game ON sportsbook_props (game_id);


-- ─────────────────────────────────────────────────────────────
-- 4. pitcher_features_daily
-- Every normalized 0-100 feature score stored for full audit trail.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pitcher_features_daily (
    id                     BIGSERIAL    PRIMARY KEY,
    pitcher_id             VARCHAR(32)  NOT NULL,
    game_id                VARCHAR(32)  NOT NULL REFERENCES games(game_id),
    game_date              DATE         NOT NULL,

    -- HUSI block scores
    owc_score              FLOAT,
    owc_babip              FLOAT,
    owc_hh                 FLOAT,
    owc_bar                FLOAT,
    owc_ld                 FLOAT,
    owc_xba                FLOAT,
    owc_bot3               FLOAT,
    owc_topheavy           FLOAT,

    pcs_score              FLOAT,
    pcs_gb                 FLOAT,
    pcs_soft               FLOAT,
    pcs_bara               FLOAT,
    pcs_hha                FLOAT,
    pcs_xbaa               FLOAT,
    pcs_xwobaa             FLOAT,
    pcs_cmd                FLOAT,
    pcs_reg                FLOAT,

    ens_score              FLOAT,
    ens_park               FLOAT,
    ens_windin             FLOAT,
    ens_temp               FLOAT,
    ens_air                FLOAT,
    ens_roof               FLOAT,
    ens_of                 FLOAT,
    ens_inf                FLOAT,

    ops_score              FLOAT,
    ops_pcap               FLOAT,
    ops_hook               FLOAT,
    ops_traffic            FLOAT,
    ops_tto                FLOAT,
    ops_bpen               FLOAT,
    ops_inj                FLOAT,
    ops_trend              FLOAT,
    ops_fat                FLOAT,

    uhs_score              FLOAT,
    uhs_cstr               FLOAT,
    uhs_zone               FLOAT,
    uhs_early              FLOAT,
    uhs_weak               FLOAT,

    dsc_score              FLOAT,
    dsc_def                FLOAT,
    dsc_infdef             FLOAT,
    dsc_ofdef              FLOAT,
    dsc_catch              FLOAT,
    dsc_align              FLOAT,

    -- KUSI block scores
    ocr_score              FLOAT,
    ocr_k                  FLOAT,
    ocr_con                FLOAT,
    ocr_zcon               FLOAT,
    ocr_disc               FLOAT,
    ocr_2s                 FLOAT,
    ocr_foul               FLOAT,
    ocr_dec                FLOAT,

    pmr_score              FLOAT,
    pmr_p1                 FLOAT,
    pmr_p2                 FLOAT,
    pmr_put                FLOAT,
    pmr_run                FLOAT,
    pmr_top6               FLOAT,
    pmr_plat               FLOAT,

    per_score              FLOAT,
    per_ppa                FLOAT,
    per_bb                 FLOAT,
    per_fps                FLOAT,
    per_deep               FLOAT,
    per_putw               FLOAT,
    per_cmdd               FLOAT,
    per_velo               FLOAT,

    kop_score              FLOAT,
    kop_pcap               FLOAT,
    kop_hook               FLOAT,
    kop_tto                FLOAT,
    kop_bpen               FLOAT,
    kop_pat                FLOAT,
    kop_inj                FLOAT,
    kop_fat                FLOAT,

    uks_score              FLOAT,
    uks_tight              FLOAT,
    uks_cstrl              FLOAT,
    uks_2exp               FLOAT,
    uks_count              FLOAT,

    tlr_score              FLOAT,
    tlr_top4k              FLOAT,
    tlr_top6c              FLOAT,
    tlr_vet                FLOAT,
    tlr_top2               FLOAT,

    -- Data quality
    lineup_confirmed       BOOLEAN      DEFAULT FALSE,
    umpire_confirmed       BOOLEAN      DEFAULT FALSE,
    bullpen_data_available BOOLEAN      DEFAULT FALSE,
    data_quality_flag      VARCHAR(32)  DEFAULT 'partial',

    created_at             TIMESTAMPTZ  DEFAULT NOW(),

    CONSTRAINT uq_feat_pitcher_game UNIQUE (pitcher_id, game_id)
);

CREATE INDEX IF NOT EXISTS idx_features_date ON pitcher_features_daily (game_date);
CREATE INDEX IF NOT EXISTS idx_features_pitcher ON pitcher_features_daily (pitcher_id);


-- ─────────────────────────────────────────────────────────────
-- 5. model_outputs_daily
-- Final HUSI / KUSI scores and projections.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS model_outputs_daily (
    id                     BIGSERIAL    PRIMARY KEY,
    pitcher_id             VARCHAR(32)  NOT NULL,
    game_id                VARCHAR(32)  NOT NULL REFERENCES games(game_id),
    game_date              DATE         NOT NULL,
    market_type            VARCHAR(32)  NOT NULL,   -- 'hits_allowed' | 'strikeouts'

    -- Index scores
    husi                   FLOAT,
    kusi                   FLOAT,
    husi_base              FLOAT,
    kusi_base              FLOAT,
    husi_interaction       FLOAT,
    kusi_interaction       FLOAT,
    husi_volatility        FLOAT,
    kusi_volatility        FLOAT,

    -- Projections
    base_hits              FLOAT,
    base_ks                FLOAT,
    projected_hits         FLOAT,
    projected_ks           FLOAT,

    -- Prop line snapshot
    sportsbook             VARCHAR(64),
    line                   FLOAT,
    under_odds             FLOAT,
    implied_under_prob     FLOAT,

    -- Edge and grade
    stat_edge              FLOAT,
    grade                  VARCHAR(8),
    confidence             VARCHAR(16),
    notes                  TEXT,
    data_quality_flag      VARCHAR(32),

    created_at             TIMESTAMPTZ  DEFAULT NOW(),

    CONSTRAINT uq_output_pitcher_game_market UNIQUE (pitcher_id, game_id, market_type)
);

CREATE INDEX IF NOT EXISTS idx_outputs_date ON model_outputs_daily (game_date);
CREATE INDEX IF NOT EXISTS idx_outputs_pitcher ON model_outputs_daily (pitcher_id);
CREATE INDEX IF NOT EXISTS idx_outputs_edge ON model_outputs_daily (stat_edge DESC NULLS LAST);


-- ─────────────────────────────────────────────────────────────
-- 6. backtest_results
-- Stores actual outcomes for backtesting and model tuning.
-- Populated after games are final (separate backfill job).
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS backtest_results (
    id                BIGSERIAL    PRIMARY KEY,
    pitcher_id        VARCHAR(32)  NOT NULL,
    game_id           VARCHAR(32)  NOT NULL,
    market_type       VARCHAR(32)  NOT NULL,
    sportsbook_line   FLOAT,
    model_projection  FLOAT,
    final_index       FLOAT,
    actual_result     FLOAT,       -- actual stat: hits allowed or Ks recorded
    result_win_loss   VARCHAR(8),  -- 'WIN' | 'LOSS' | 'PUSH'
    closing_line      FLOAT,
    closing_odds      FLOAT,
    recorded_at       TIMESTAMPTZ  DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_backtest_pitcher ON backtest_results (pitcher_id);
CREATE INDEX IF NOT EXISTS idx_backtest_game ON backtest_results (game_id);


-- ─────────────────────────────────────────────────────────────
-- 7. umpire_profiles
-- Updated once daily by the umpire scraper.
-- Until the scraper is live, this table is empty and scores default to 50 (neutral).
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS umpire_profiles (
    umpire_id           VARCHAR(32)  PRIMARY KEY,
    umpire_name         VARCHAR(128) NOT NULL,
    called_strike_rate  FLOAT,      -- normalized 0-100
    zone_accuracy       FLOAT,      -- normalized 0-100
    favor_direction     VARCHAR(16),
    sample_games        INTEGER,
    last_updated        TIMESTAMPTZ
);


-- ─────────────────────────────────────────────────────────────
-- 8. ml_training_samples
-- One row per pitcher per game. Populated by the ML trainer
-- immediately after the formula engine scores pitchers.
-- actual_hits / actual_ks are filled in post-game via the labeler.
-- This is the growing dataset the ML engine learns from.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ml_training_samples (
    id                  BIGSERIAL    PRIMARY KEY,
    pitcher_id          VARCHAR(32)  NOT NULL,
    game_id             VARCHAR(32)  NOT NULL,
    game_date           DATE         NOT NULL,

    -- Feature inputs (formula block scores, 0-100)
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
    bullpen_fatigue_opp FLOAT  DEFAULT 0.0,
    bullpen_fatigue_own FLOAT  DEFAULT 0.0,
    ens_park            FLOAT,
    ens_temp            FLOAT,
    ens_air             FLOAT,

    -- Formula engine outputs (ML uses these as context inputs)
    formula_husi        FLOAT,
    formula_kusi        FLOAT,
    formula_proj_hits   FLOAT,
    formula_proj_ks     FLOAT,

    -- Actual outcomes — NULL until game completes (these are training labels)
    actual_hits         FLOAT,
    actual_ks           FLOAT,
    actual_ip           FLOAT,
    is_complete         BOOLEAN      DEFAULT FALSE,

    -- PFF snapshot at time of start
    pff_score           FLOAT        DEFAULT 0.0,
    pff_label           VARCHAR(16)  DEFAULT 'NEUTRAL',

    -- First-inning performance (cold/hot start pattern)
    first_inning_hits   FLOAT,
    first_inning_ks     FLOAT,

    created_at          TIMESTAMPTZ  DEFAULT NOW(),
    updated_at          TIMESTAMPTZ  DEFAULT NOW(),

    CONSTRAINT uq_ml_pitcher_game UNIQUE (pitcher_id, game_id)
);

CREATE INDEX IF NOT EXISTS idx_ml_samples_date     ON ml_training_samples (game_date);
CREATE INDEX IF NOT EXISTS idx_ml_samples_complete ON ml_training_samples (is_complete);


-- ─────────────────────────────────────────────────────────────
-- 9. ml_model_outputs
-- ML engine predictions stored alongside formula predictions.
-- Enables direct comparison: Formula HUSI vs ML HUSI per pitcher per day.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ml_model_outputs (
    id                    BIGSERIAL    PRIMARY KEY,
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

    -- Comparison to formula engine
    husi_delta            FLOAT,         -- ml_husi - formula_husi
    kusi_delta            FLOAT,
    husi_divergence       VARCHAR(16),   -- ALIGNED | SLIGHT_DIFF | DIVERGENT | CONFLICT
    kusi_divergence       VARCHAR(16),
    consensus_husi_grade  VARCHAR(8),    -- grade only when both engines agree; 'SPLIT' otherwise
    consensus_kusi_grade  VARCHAR(8),

    -- Model metadata
    model_version         VARCHAR(32),
    training_samples      INTEGER,
    mae_hits              FLOAT,
    mae_ks                FLOAT,

    created_at            TIMESTAMPTZ  DEFAULT NOW(),

    CONSTRAINT uq_ml_output_pitcher_game UNIQUE (pitcher_id, game_id)
);

CREATE INDEX IF NOT EXISTS idx_ml_outputs_date ON ml_model_outputs (game_date);

