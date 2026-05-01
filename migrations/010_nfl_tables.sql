-- =============================================================================
-- Migration 010: NFL QB tables
-- Adds four new tables for the NFL scoring system (QPYI / QTDI).
-- All tables are prefixed nfl_ — zero collision risk with existing MLB tables.
--
--   nfl_games              — one row per NFL game per week
--   nfl_qb_starters        — one row per QB starting assignment per week
--   nfl_qb_features_daily  — intermediate block scores, one row per QB per week
--   nfl_model_outputs_daily — final scored output, one row per QB per market
--
-- Safe to run multiple times — uses CREATE TABLE IF NOT EXISTS.
-- Reversible — see DOWNGRADE section at the bottom of this file.
-- =============================================================================


-- =============================================================================
-- UPGRADE
-- =============================================================================

-- ── nfl_games ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS nfl_games (
    id          BIGSERIAL    PRIMARY KEY,
    game_id     VARCHAR(64)  NOT NULL,
    game_date   DATE         NOT NULL,
    season_year INTEGER      NOT NULL,
    week        INTEGER      NOT NULL,
    home_team   VARCHAR(64)  NOT NULL,
    away_team   VARCHAR(64)  NOT NULL,
    stadium     VARCHAR(128),
    surface     VARCHAR(32),
    is_dome     BOOLEAN      NOT NULL DEFAULT false,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT uq_nfl_games_game_id UNIQUE (game_id)
);

CREATE INDEX IF NOT EXISTS ix_nfl_games_game_id   ON nfl_games (game_id);
CREATE INDEX IF NOT EXISTS ix_nfl_games_game_date ON nfl_games (game_date);

-- ── nfl_qb_starters ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS nfl_qb_starters (
    id                  BIGSERIAL    PRIMARY KEY,
    game_id             VARCHAR(64)  NOT NULL REFERENCES nfl_games (game_id),
    qb_name             VARCHAR(128) NOT NULL,
    team                VARCHAR(64)  NOT NULL,
    opponent            VARCHAR(64)  NOT NULL,
    is_home             BOOLEAN      NOT NULL DEFAULT true,
    injury_designation  VARCHAR(32),
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT uq_nfl_starter_game_qb UNIQUE (game_id, qb_name)
);

CREATE INDEX IF NOT EXISTS ix_nfl_qb_starters_game_id ON nfl_qb_starters (game_id);

-- ── nfl_qb_features_daily ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS nfl_qb_features_daily (
    id           BIGSERIAL    PRIMARY KEY,
    game_id      VARCHAR(64)  NOT NULL REFERENCES nfl_games (game_id),
    qb_name      VARCHAR(128) NOT NULL,
    team         VARCHAR(64)  NOT NULL,
    week         INTEGER      NOT NULL,
    season_year  INTEGER      NOT NULL,
    -- QPYI block scores (0–100 each)
    osw_score    FLOAT,
    qsr_score    FLOAT,
    gsp_score    FLOAT,
    scb_score    FLOAT,
    pdr_score    FLOAT,
    ens_score    FLOAT,
    dsr_score    FLOAT,
    rct_score    FLOAT,
    -- QTDI-specific block scores (0–100 each)
    ord_score    FLOAT,
    qtr_score    FLOAT,
    gsp_td_score FLOAT,
    scb_td_score FLOAT,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT uq_nfl_features_game_qb UNIQUE (game_id, qb_name)
);

CREATE INDEX IF NOT EXISTS ix_nfl_qb_features_daily_game_id ON nfl_qb_features_daily (game_id);
CREATE INDEX IF NOT EXISTS ix_nfl_qb_features_daily_qb_name ON nfl_qb_features_daily (qb_name);

-- ── nfl_model_outputs_daily ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS nfl_model_outputs_daily (
    id              BIGSERIAL    PRIMARY KEY,
    game_id         VARCHAR(64)  NOT NULL REFERENCES nfl_games (game_id),
    qb_name         VARCHAR(128) NOT NULL,
    team            VARCHAR(64)  NOT NULL,
    opponent        VARCHAR(64)  NOT NULL,
    week            INTEGER      NOT NULL,
    season_year     INTEGER      NOT NULL,
    market          VARCHAR(32)  NOT NULL,   -- 'passing_yards' | 'touchdowns'
    qpyi_score      FLOAT,
    qtdi_score      FLOAT,
    grade           VARCHAR(8),
    projected_value FLOAT,
    prop_line       FLOAT,
    edge            FLOAT,                   -- projected_value minus prop_line
    signal_tag      VARCHAR(32),
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT uq_nfl_output_game_qb_market UNIQUE (game_id, qb_name, market)
);

CREATE INDEX IF NOT EXISTS ix_nfl_model_outputs_daily_game_id    ON nfl_model_outputs_daily (game_id);
CREATE INDEX IF NOT EXISTS ix_nfl_model_outputs_daily_qb_name    ON nfl_model_outputs_daily (qb_name);
CREATE INDEX IF NOT EXISTS ix_nfl_model_outputs_daily_week       ON nfl_model_outputs_daily (season_year, week);

SELECT 'migration 010 complete — nfl_games, nfl_qb_starters, nfl_qb_features_daily, nfl_model_outputs_daily created' AS status;


-- =============================================================================
-- DOWNGRADE  (run this block manually to roll back migration 010)
-- Drop tables in reverse dependency order to respect foreign key constraints.
-- =============================================================================

-- DROP TABLE IF EXISTS nfl_model_outputs_daily;
-- DROP TABLE IF EXISTS nfl_qb_features_daily;
-- DROP TABLE IF EXISTS nfl_qb_starters;
-- DROP TABLE IF EXISTS nfl_games;
