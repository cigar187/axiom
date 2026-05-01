-- =============================================================================
-- Migration 011: NHL tables
-- Adds five new tables for the NHL scoring system (GSAI / PPSI).
-- All tables are prefixed nhl_ — zero collision risk with MLB/NFL tables.
--
--   nhl_games                 — one row per NHL game
--   nhl_game_rosters          — one row per player per game
--   nhl_goalie_features_daily — GSAI block scores, one row per goalie per game
--   nhl_skater_features_daily — PPSI block scores, one row per skater per game
--   nhl_model_outputs_daily   — final scored output, one row per player per market
--
-- Safe to run multiple times — uses CREATE TABLE IF NOT EXISTS.
-- Reversible — see DOWNGRADE section at the bottom of this file.
-- =============================================================================


-- =============================================================================
-- UPGRADE
-- =============================================================================

-- ── nhl_games ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS nhl_games (
    id                  BIGSERIAL    PRIMARY KEY,
    game_id             VARCHAR(32)  NOT NULL,
    game_date           DATE         NOT NULL,
    season_year         INTEGER      NOT NULL,
    series_game_number  INTEGER      NOT NULL DEFAULT 0,
    home_team           VARCHAR(16)  NOT NULL,
    away_team           VARCHAR(16)  NOT NULL,
    home_series_wins    INTEGER      NOT NULL DEFAULT 0,
    away_series_wins    INTEGER      NOT NULL DEFAULT 0,
    venue               VARCHAR(128),
    is_playoff          BOOLEAN      NOT NULL DEFAULT true,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT uq_nhl_games_game_id UNIQUE (game_id)
);

CREATE INDEX IF NOT EXISTS ix_nhl_games_game_id   ON nhl_games (game_id);
CREATE INDEX IF NOT EXISTS ix_nhl_games_game_date ON nhl_games (game_date);

-- ── nhl_game_rosters ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS nhl_game_rosters (
    id                  BIGSERIAL    PRIMARY KEY,
    game_id             VARCHAR(32)  NOT NULL REFERENCES nhl_games (game_id),
    player_id           INTEGER      NOT NULL,
    player_name         VARCHAR(128) NOT NULL,
    team                VARCHAR(16)  NOT NULL,
    opponent            VARCHAR(16)  NOT NULL,
    position            VARCHAR(8)   NOT NULL,
    is_home             BOOLEAN      NOT NULL,
    line_number         INTEGER,
    pp_unit             INTEGER,
    injury_designation  VARCHAR(32),
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT uq_nhl_roster_game_player UNIQUE (game_id, player_id)
);

CREATE INDEX IF NOT EXISTS ix_nhl_game_rosters_game_id   ON nhl_game_rosters (game_id);
CREATE INDEX IF NOT EXISTS ix_nhl_game_rosters_player_id ON nhl_game_rosters (player_id);

-- ── nhl_goalie_features_daily ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS nhl_goalie_features_daily (
    id              BIGSERIAL    PRIMARY KEY,
    game_id         VARCHAR(32)  NOT NULL REFERENCES nhl_games (game_id),
    player_id       INTEGER      NOT NULL,
    player_name     VARCHAR(128) NOT NULL,
    team            VARCHAR(16)  NOT NULL,
    -- GSAI block scores (0–100 each)
    gsai_score      FLOAT,
    gss_score       FLOAT,    -- Goalie Save Suppression   29%
    osq_score       FLOAT,    -- Opponent Shooting Quality  24%
    top_score       FLOAT,    -- Tactical / Operational     18%
    gen_score       FLOAT,    -- Game Environment           16%
    rfs_score       FLOAT,    -- Referee Flow Score          8%
    tsc_score       FLOAT,    -- Team Structure & Coverage   5%
    projected_shots FLOAT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_nhl_goalie_feat_game_player UNIQUE (game_id, player_id)
);

CREATE INDEX IF NOT EXISTS ix_nhl_goalie_features_daily_game_id   ON nhl_goalie_features_daily (game_id);
CREATE INDEX IF NOT EXISTS ix_nhl_goalie_features_daily_player_id ON nhl_goalie_features_daily (player_id);

-- ── nhl_skater_features_daily ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS nhl_skater_features_daily (
    id                BIGSERIAL    PRIMARY KEY,
    game_id           VARCHAR(32)  NOT NULL REFERENCES nhl_games (game_id),
    player_id         INTEGER      NOT NULL,
    player_name       VARCHAR(128) NOT NULL,
    team              VARCHAR(16)  NOT NULL,
    -- PPSI block scores (0–100 each)
    ppsi_score        FLOAT,
    osr_score         FLOAT,    -- Opponent Scoring Resistance 28%
    pmr_score         FLOAT,    -- Player Matchup Rating       22%
    per_score         FLOAT,    -- Player Efficiency Rating    18%
    pop_score         FLOAT,    -- Points Operational          14%
    rps_score         FLOAT,    -- Referee PP Score            10%
    tld_score         FLOAT,    -- Top-Line Deployment          8%
    -- Projections
    projected_pts     FLOAT,
    projected_sog     FLOAT,
    projected_goals   FLOAT,
    projected_assists FLOAT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_nhl_skater_feat_game_player UNIQUE (game_id, player_id)
);

CREATE INDEX IF NOT EXISTS ix_nhl_skater_features_daily_game_id   ON nhl_skater_features_daily (game_id);
CREATE INDEX IF NOT EXISTS ix_nhl_skater_features_daily_player_id ON nhl_skater_features_daily (player_id);

-- ── nhl_model_outputs_daily ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS nhl_model_outputs_daily (
    id                        BIGSERIAL    PRIMARY KEY,
    game_id                   VARCHAR(32)  NOT NULL REFERENCES nhl_games (game_id),
    player_id                 INTEGER      NOT NULL,
    player_name               VARCHAR(128) NOT NULL,
    team                      VARCHAR(16)  NOT NULL,
    opponent                  VARCHAR(16)  NOT NULL,
    position                  VARCHAR(8)   NOT NULL,
    market                    VARCHAR(32)  NOT NULL,  -- points | goals | assists | shots_on_goal | shots_faced
    -- Index scores
    gsai_score                FLOAT,
    ppsi_score                FLOAT,
    grade                     VARCHAR(8),
    -- Projection and prop line
    projected_value           FLOAT        NOT NULL,
    prop_line                 FLOAT,
    edge                      FLOAT,                  -- projected_value minus prop_line
    signal_tag                VARCHAR(32),
    -- ML engine
    ml_projection             FLOAT,
    ml_signal                 VARCHAR(16),             -- ALIGNED | LEAN | SPLIT
    playoff_discount_applied  BOOLEAN      NOT NULL DEFAULT true,
    created_at                TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT uq_nhl_output_game_player_market UNIQUE (game_id, player_id, market)
);

CREATE INDEX IF NOT EXISTS ix_nhl_model_outputs_daily_game_id   ON nhl_model_outputs_daily (game_id);
CREATE INDEX IF NOT EXISTS ix_nhl_model_outputs_daily_player_id ON nhl_model_outputs_daily (player_id);
CREATE INDEX IF NOT EXISTS ix_nhl_model_outputs_daily_market    ON nhl_model_outputs_daily (market);

SELECT 'migration 011 complete — nhl_games, nhl_game_rosters, nhl_goalie_features_daily, nhl_skater_features_daily, nhl_model_outputs_daily created' AS status;


-- =============================================================================
-- DOWNGRADE  (run this block manually to roll back migration 011)
-- Drop tables in reverse dependency order to respect foreign key constraints.
-- =============================================================================

-- DROP TABLE IF EXISTS nhl_model_outputs_daily;
-- DROP TABLE IF EXISTS nhl_skater_features_daily;
-- DROP TABLE IF EXISTS nhl_goalie_features_daily;
-- DROP TABLE IF EXISTS nhl_game_rosters;
-- DROP TABLE IF EXISTS nhl_games;
