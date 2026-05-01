"""
SQLAlchemy ORM models for the Axiom database (axiom_db).

MLB tables:
  - games
  - probable_pitchers
  - sportsbook_props
  - pitcher_features_daily
  - model_outputs_daily
  - backtest_results
  - umpire_profiles
  - statcast_pitcher_cache
  - axiom_pitcher_stats
  - axiom_game_lineup
  - pipeline_run_log
  - api_keys

NFL tables (nfl_ prefix — zero collision risk with MLB tables):
  - nfl_games
  - nfl_qb_starters
  - nfl_qb_features_daily
  - nfl_model_outputs_daily

NHL tables (nhl_ prefix — zero collision risk with MLB/NFL tables):
  - nhl_games
  - nhl_game_rosters
  - nhl_goalie_features_daily
  - nhl_skater_features_daily
  - nhl_model_outputs_daily
"""
import uuid
from datetime import date, datetime
from typing import Optional

from sqlalchemy import (
    BigInteger, Boolean, Date, DateTime, Float, ForeignKey,
    Integer, String, Text, UniqueConstraint, func,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base


# ─────────────────────────────────────────────────────────────
# games
# ─────────────────────────────────────────────────────────────
class Game(Base):
    __tablename__ = "games"

    game_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    game_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    home_team: Mapped[str] = mapped_column(String(64), nullable=False)
    away_team: Mapped[str] = mapped_column(String(64), nullable=False)
    park: Mapped[Optional[str]] = mapped_column(String(128))

    # Weather
    temperature_f: Mapped[Optional[float]] = mapped_column(Float)
    wind_speed_mph: Mapped[Optional[float]] = mapped_column(Float)
    wind_direction: Mapped[Optional[str]] = mapped_column(String(64))
    is_dome: Mapped[bool] = mapped_column(Boolean, default=False)
    roof_type: Mapped[Optional[str]] = mapped_column(String(32))
    weather_condition: Mapped[Optional[str]] = mapped_column(String(64))

    # Umpire
    home_plate_umpire_id: Mapped[Optional[str]] = mapped_column(String(32))
    home_plate_umpire_name: Mapped[Optional[str]] = mapped_column(String(128))

    # Market lines (from The Rundown)
    game_total:      Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    home_moneyline:  Mapped[Optional[int]]   = mapped_column(Integer, nullable=True)
    away_moneyline:  Mapped[Optional[int]]   = mapped_column(Integer, nullable=True)

    status: Mapped[str] = mapped_column(String(32), default="scheduled")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    probable_pitchers: Mapped[list["ProbablePitcher"]] = relationship(back_populates="game")
    props: Mapped[list["SportsbookProp"]] = relationship(back_populates="game")
    outputs: Mapped[list["ModelOutputDaily"]] = relationship(back_populates="game")


# ─────────────────────────────────────────────────────────────
# probable_pitchers
# ─────────────────────────────────────────────────────────────
class ProbablePitcher(Base):
    __tablename__ = "probable_pitchers"
    __table_args__ = (UniqueConstraint("pitcher_id", "game_id", name="uq_pitcher_game"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    pitcher_id: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    game_id: Mapped[str] = mapped_column(String(32), ForeignKey("games.game_id"), nullable=False)
    team_id: Mapped[str] = mapped_column(String(32), nullable=False)
    pitcher_name: Mapped[str] = mapped_column(String(128), nullable=False)
    handedness: Mapped[Optional[str]] = mapped_column(String(4))
    confirmed_flag: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    game: Mapped["Game"] = relationship(back_populates="probable_pitchers")
    features: Mapped[list["PitcherFeaturesDaily"]] = relationship(back_populates="pitcher_record")
    outputs: Mapped[list["ModelOutputDaily"]] = relationship(back_populates="pitcher_record")


# ─────────────────────────────────────────────────────────────
# sportsbook_props
# ─────────────────────────────────────────────────────────────
class SportsbookProp(Base):
    __tablename__ = "sportsbook_props"

    prop_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    game_id: Mapped[str] = mapped_column(String(32), ForeignKey("games.game_id"), nullable=False)
    pitcher_id: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    sportsbook: Mapped[str] = mapped_column(String(64), nullable=False)
    market_type: Mapped[str] = mapped_column(String(32), nullable=False)  # "hits_allowed" | "strikeouts"
    line: Mapped[float] = mapped_column(Float, nullable=False)
    over_odds: Mapped[Optional[float]] = mapped_column(Float)
    under_odds: Mapped[Optional[float]] = mapped_column(Float)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    game: Mapped["Game"] = relationship(back_populates="props")


# ─────────────────────────────────────────────────────────────
# pitcher_features_daily
# Stores every normalized 0-100 feature score for full audit trail
# ─────────────────────────────────────────────────────────────
class PitcherFeaturesDaily(Base):
    __tablename__ = "pitcher_features_daily"
    __table_args__ = (UniqueConstraint("pitcher_id", "game_id", name="uq_feat_pitcher_game"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    pitcher_id: Mapped[str] = mapped_column(String(32), ForeignKey("probable_pitchers.pitcher_id"), nullable=False)
    game_id: Mapped[str] = mapped_column(String(32), ForeignKey("games.game_id"), nullable=False)
    game_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)

    # ── HUSI block scores (0-100)
    owc_score: Mapped[Optional[float]] = mapped_column(Float)
    owc_babip: Mapped[Optional[float]] = mapped_column(Float)
    owc_hh: Mapped[Optional[float]] = mapped_column(Float)
    owc_bar: Mapped[Optional[float]] = mapped_column(Float)
    owc_ld: Mapped[Optional[float]] = mapped_column(Float)
    owc_xba: Mapped[Optional[float]] = mapped_column(Float)
    owc_bot3: Mapped[Optional[float]] = mapped_column(Float)
    owc_topheavy: Mapped[Optional[float]] = mapped_column(Float)

    pcs_score: Mapped[Optional[float]] = mapped_column(Float)
    pcs_gb: Mapped[Optional[float]] = mapped_column(Float)
    pcs_soft: Mapped[Optional[float]] = mapped_column(Float)
    pcs_bara: Mapped[Optional[float]] = mapped_column(Float)
    pcs_hha: Mapped[Optional[float]] = mapped_column(Float)
    pcs_xbaa: Mapped[Optional[float]] = mapped_column(Float)
    pcs_xwobaa: Mapped[Optional[float]] = mapped_column(Float)
    pcs_cmd: Mapped[Optional[float]] = mapped_column(Float)
    pcs_reg: Mapped[Optional[float]] = mapped_column(Float)

    ens_score: Mapped[Optional[float]] = mapped_column(Float)
    ens_park: Mapped[Optional[float]] = mapped_column(Float)
    ens_windin: Mapped[Optional[float]] = mapped_column(Float)
    ens_temp: Mapped[Optional[float]] = mapped_column(Float)
    ens_air: Mapped[Optional[float]] = mapped_column(Float)
    ens_roof: Mapped[Optional[float]] = mapped_column(Float)
    ens_of: Mapped[Optional[float]] = mapped_column(Float)
    ens_inf: Mapped[Optional[float]] = mapped_column(Float)

    ops_score: Mapped[Optional[float]] = mapped_column(Float)
    ops_pcap: Mapped[Optional[float]] = mapped_column(Float)
    ops_hook: Mapped[Optional[float]] = mapped_column(Float)
    ops_traffic: Mapped[Optional[float]] = mapped_column(Float)
    ops_tto: Mapped[Optional[float]] = mapped_column(Float)
    ops_bpen: Mapped[Optional[float]] = mapped_column(Float)
    ops_inj: Mapped[Optional[float]] = mapped_column(Float)
    ops_trend: Mapped[Optional[float]] = mapped_column(Float)
    ops_fat: Mapped[Optional[float]] = mapped_column(Float)

    uhs_score: Mapped[Optional[float]] = mapped_column(Float)
    uhs_cstr: Mapped[Optional[float]] = mapped_column(Float)
    uhs_zone: Mapped[Optional[float]] = mapped_column(Float)
    uhs_early: Mapped[Optional[float]] = mapped_column(Float)
    uhs_weak: Mapped[Optional[float]] = mapped_column(Float)

    dsc_score: Mapped[Optional[float]] = mapped_column(Float)
    dsc_def: Mapped[Optional[float]] = mapped_column(Float)
    dsc_infdef: Mapped[Optional[float]] = mapped_column(Float)
    dsc_ofdef: Mapped[Optional[float]] = mapped_column(Float)
    dsc_catch: Mapped[Optional[float]] = mapped_column(Float)
    dsc_align: Mapped[Optional[float]] = mapped_column(Float)

    # ── KUSI block scores (0-100)
    ocr_score: Mapped[Optional[float]] = mapped_column(Float)
    ocr_k: Mapped[Optional[float]] = mapped_column(Float)
    ocr_con: Mapped[Optional[float]] = mapped_column(Float)
    ocr_zcon: Mapped[Optional[float]] = mapped_column(Float)
    ocr_disc: Mapped[Optional[float]] = mapped_column(Float)
    ocr_2s: Mapped[Optional[float]] = mapped_column(Float)
    ocr_foul: Mapped[Optional[float]] = mapped_column(Float)
    ocr_dec: Mapped[Optional[float]] = mapped_column(Float)

    pmr_score: Mapped[Optional[float]] = mapped_column(Float)
    pmr_p1: Mapped[Optional[float]] = mapped_column(Float)
    pmr_p2: Mapped[Optional[float]] = mapped_column(Float)
    pmr_put: Mapped[Optional[float]] = mapped_column(Float)
    pmr_run: Mapped[Optional[float]] = mapped_column(Float)
    pmr_top6: Mapped[Optional[float]] = mapped_column(Float)
    pmr_plat: Mapped[Optional[float]] = mapped_column(Float)

    per_score: Mapped[Optional[float]] = mapped_column(Float)
    per_ppa: Mapped[Optional[float]] = mapped_column(Float)
    per_bb: Mapped[Optional[float]] = mapped_column(Float)
    per_fps: Mapped[Optional[float]] = mapped_column(Float)
    per_deep: Mapped[Optional[float]] = mapped_column(Float)
    per_putw: Mapped[Optional[float]] = mapped_column(Float)
    per_cmdd: Mapped[Optional[float]] = mapped_column(Float)
    per_velo: Mapped[Optional[float]] = mapped_column(Float)

    kop_score: Mapped[Optional[float]] = mapped_column(Float)
    kop_pcap: Mapped[Optional[float]] = mapped_column(Float)
    kop_hook: Mapped[Optional[float]] = mapped_column(Float)
    kop_tto: Mapped[Optional[float]] = mapped_column(Float)
    kop_bpen: Mapped[Optional[float]] = mapped_column(Float)
    kop_pat: Mapped[Optional[float]] = mapped_column(Float)
    kop_inj: Mapped[Optional[float]] = mapped_column(Float)
    kop_fat: Mapped[Optional[float]] = mapped_column(Float)

    uks_score: Mapped[Optional[float]] = mapped_column(Float)
    uks_tight: Mapped[Optional[float]] = mapped_column(Float)
    uks_cstrl: Mapped[Optional[float]] = mapped_column(Float)
    uks_2exp: Mapped[Optional[float]] = mapped_column(Float)
    uks_count: Mapped[Optional[float]] = mapped_column(Float)

    tlr_score: Mapped[Optional[float]] = mapped_column(Float)
    tlr_top4k: Mapped[Optional[float]] = mapped_column(Float)
    tlr_top6c: Mapped[Optional[float]] = mapped_column(Float)
    tlr_vet: Mapped[Optional[float]] = mapped_column(Float)
    tlr_top2: Mapped[Optional[float]] = mapped_column(Float)

    # Data quality
    lineup_confirmed: Mapped[bool] = mapped_column(Boolean, default=False)
    umpire_confirmed: Mapped[bool] = mapped_column(Boolean, default=False)
    bullpen_data_available: Mapped[bool] = mapped_column(Boolean, default=False)
    data_quality_flag: Mapped[Optional[str]] = mapped_column(String(32), default="partial")

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    pitcher_record: Mapped["ProbablePitcher"] = relationship(
        back_populates="features",
        primaryjoin="PitcherFeaturesDaily.pitcher_id == ProbablePitcher.pitcher_id",
        foreign_keys="[PitcherFeaturesDaily.pitcher_id]",
    )


# ─────────────────────────────────────────────────────────────
# model_outputs_daily
# ─────────────────────────────────────────────────────────────
class ModelOutputDaily(Base):
    __tablename__ = "model_outputs_daily"
    __table_args__ = (
        UniqueConstraint("pitcher_id", "game_id", "market_type", name="uq_output_pitcher_game_market"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    pitcher_id: Mapped[str] = mapped_column(String(32), ForeignKey("probable_pitchers.pitcher_id"), nullable=False, index=True)
    game_id: Mapped[str] = mapped_column(String(32), ForeignKey("games.game_id"), nullable=False)
    game_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    market_type: Mapped[str] = mapped_column(String(32), nullable=False)

    # Index scores
    hssi: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    husi: Mapped[Optional[float]] = mapped_column(Float)
    kssi: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    kusi: Mapped[Optional[float]] = mapped_column(Float)
    hssi_base: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    husi_base: Mapped[Optional[float]] = mapped_column(Float)
    kssi_base: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    kusi_base: Mapped[Optional[float]] = mapped_column(Float)
    hssi_interaction: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    husi_interaction: Mapped[Optional[float]] = mapped_column(Float)
    kssi_interaction: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    kusi_interaction: Mapped[Optional[float]] = mapped_column(Float)
    hssi_volatility: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    husi_volatility: Mapped[Optional[float]] = mapped_column(Float)
    kssi_volatility: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    kusi_volatility: Mapped[Optional[float]] = mapped_column(Float)

    # Projections
    base_hits: Mapped[Optional[float]] = mapped_column(Float)
    base_ks: Mapped[Optional[float]] = mapped_column(Float)
    projected_hits: Mapped[Optional[float]] = mapped_column(Float)
    projected_ks: Mapped[Optional[float]] = mapped_column(Float)

    # Prop line
    sportsbook: Mapped[Optional[str]] = mapped_column(String(64))
    line: Mapped[Optional[float]] = mapped_column(Float)
    under_odds: Mapped[Optional[float]] = mapped_column(Float)
    implied_under_prob: Mapped[Optional[float]] = mapped_column(Float)

    # Edge
    stat_edge: Mapped[Optional[float]] = mapped_column(Float)
    grade: Mapped[Optional[str]] = mapped_column(String(8))
    confidence: Mapped[Optional[str]] = mapped_column(String(16))
    notes: Mapped[Optional[str]] = mapped_column(Text)
    data_quality_flag: Mapped[Optional[str]] = mapped_column(String(32))

    # PFF + Risk profile (auto-computed by pipeline each day)
    pff_score: Mapped[Optional[float]] = mapped_column(Float, default=0.0)
    pff_label: Mapped[Optional[str]] = mapped_column(String(32), default="NEUTRAL")
    risk_score: Mapped[Optional[int]] = mapped_column(Integer, default=0)
    risk_tier: Mapped[Optional[str]] = mapped_column(String(16), default="LOW")
    risk_flags: Mapped[Optional[str]] = mapped_column(String(256))
    combo_risk: Mapped[Optional[bool]] = mapped_column(Boolean, default=False)
    season_era_tier: Mapped[Optional[str]] = mapped_column(String(16), default="NORMAL")
    park_extreme: Mapped[Optional[bool]] = mapped_column(Boolean, default=False)
    park_hits_multiplier: Mapped[Optional[float]] = mapped_column(Float, default=1.0)

    # ── Merlin Simulation Engine outputs (N=2000 Monte Carlo)
    sim_median_hits: Mapped[Optional[float]] = mapped_column(Float)
    sim_median_ks: Mapped[Optional[float]] = mapped_column(Float)
    sim_over_pct_hits: Mapped[Optional[float]] = mapped_column(Float)
    sim_under_pct_hits: Mapped[Optional[float]] = mapped_column(Float)
    sim_p5_hits: Mapped[Optional[float]] = mapped_column(Float)
    sim_p95_hits: Mapped[Optional[float]] = mapped_column(Float)
    sim_over_pct_ks: Mapped[Optional[float]] = mapped_column(Float)
    sim_under_pct_ks: Mapped[Optional[float]] = mapped_column(Float)
    sim_p5_ks: Mapped[Optional[float]] = mapped_column(Float)
    sim_p95_ks: Mapped[Optional[float]] = mapped_column(Float)
    sim_confidence_hits: Mapped[Optional[str]] = mapped_column(String(16))
    sim_confidence_ks: Mapped[Optional[str]] = mapped_column(String(16))
    sim_kill_streak_prob: Mapped[Optional[float]] = mapped_column(Float)

    # ── Entropy Filter — agreement between Engine 1 (formula) and Engine 2 (ML)
    hits_entropy:  Mapped[Optional[float]] = mapped_column(Float)
    ks_entropy:    Mapped[Optional[float]] = mapped_column(Float)
    entropy_label: Mapped[Optional[str]]  = mapped_column(String(16))

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    game: Mapped["Game"] = relationship(back_populates="outputs")
    pitcher_record: Mapped["ProbablePitcher"] = relationship(
        back_populates="outputs",
        primaryjoin="ModelOutputDaily.pitcher_id == ProbablePitcher.pitcher_id",
        foreign_keys="[ModelOutputDaily.pitcher_id]",
    )


# ─────────────────────────────────────────────────────────────
# backtest_results
# ─────────────────────────────────────────────────────────────
class BacktestResult(Base):
    __tablename__ = "backtest_results"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    pitcher_id: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    game_id: Mapped[str] = mapped_column(String(32), nullable=False)
    market_type: Mapped[str] = mapped_column(String(32), nullable=False)
    sportsbook_line: Mapped[Optional[float]] = mapped_column(Float)
    model_projection: Mapped[Optional[float]] = mapped_column(Float)
    final_index: Mapped[Optional[float]] = mapped_column(Float)
    actual_result: Mapped[Optional[float]] = mapped_column(Float)
    result_win_loss: Mapped[Optional[str]] = mapped_column(String(8))
    closing_line: Mapped[Optional[float]] = mapped_column(Float)
    closing_odds: Mapped[Optional[float]] = mapped_column(Float)
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


# ─────────────────────────────────────────────────────────────
# umpire_profiles
# ─────────────────────────────────────────────────────────────
class UmpireProfile(Base):
    __tablename__ = "umpire_profiles"

    umpire_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    umpire_name: Mapped[str] = mapped_column(String(128), nullable=False)
    called_strike_rate: Mapped[Optional[float]] = mapped_column(Float)
    zone_accuracy: Mapped[Optional[float]] = mapped_column(Float)
    favor_direction: Mapped[Optional[str]] = mapped_column(String(16))
    sample_games: Mapped[Optional[int]] = mapped_column(Integer)
    last_updated: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))


# ─────────────────────────────────────────────────────────────
# ml_training_samples
# One row per pitcher per game. Starts with formula outputs as inputs;
# actual_hits / actual_ks filled in once the game is complete.
# This is the growing dataset the ML engine learns from.
# ─────────────────────────────────────────────────────────────
class MLTrainingSample(Base):
    __tablename__ = "ml_training_samples"
    __table_args__ = (UniqueConstraint("pitcher_id", "game_id", name="uq_ml_pitcher_game"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    pitcher_id: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    game_id: Mapped[str] = mapped_column(String(32), nullable=False)
    game_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)

    # ── Feature inputs (block scores, 0-100 each)
    owc_score: Mapped[Optional[float]] = mapped_column(Float)
    pcs_score: Mapped[Optional[float]] = mapped_column(Float)
    ens_score: Mapped[Optional[float]] = mapped_column(Float)
    ops_score: Mapped[Optional[float]] = mapped_column(Float)
    uhs_score: Mapped[Optional[float]] = mapped_column(Float)
    dsc_score: Mapped[Optional[float]] = mapped_column(Float)
    ocr_score: Mapped[Optional[float]] = mapped_column(Float)
    pmr_score: Mapped[Optional[float]] = mapped_column(Float)
    per_score: Mapped[Optional[float]] = mapped_column(Float)
    kop_score: Mapped[Optional[float]] = mapped_column(Float)
    uks_score: Mapped[Optional[float]] = mapped_column(Float)
    tlr_score: Mapped[Optional[float]] = mapped_column(Float)

    # ── Raw stats
    season_h9: Mapped[Optional[float]] = mapped_column(Float)
    season_k9: Mapped[Optional[float]] = mapped_column(Float)
    expected_ip: Mapped[Optional[float]] = mapped_column(Float)
    bullpen_fatigue_opp: Mapped[Optional[float]] = mapped_column(Float, default=0.0)
    bullpen_fatigue_own: Mapped[Optional[float]] = mapped_column(Float, default=0.0)
    ens_park: Mapped[Optional[float]] = mapped_column(Float)
    ens_temp: Mapped[Optional[float]] = mapped_column(Float)
    ens_air: Mapped[Optional[float]] = mapped_column(Float)

    # ── Formula engine outputs (used as ML inputs — ML learns when to agree/disagree)
    formula_hssi: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    formula_husi: Mapped[Optional[float]] = mapped_column(Float)
    formula_kssi: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    formula_kusi: Mapped[Optional[float]] = mapped_column(Float)
    formula_proj_hits: Mapped[Optional[float]] = mapped_column(Float)
    formula_proj_ks: Mapped[Optional[float]] = mapped_column(Float)

    # ── Actual outcomes (NULL until game completes — this is the training label)
    actual_hits: Mapped[Optional[float]] = mapped_column(Float)
    actual_ks: Mapped[Optional[float]] = mapped_column(Float)
    actual_ip: Mapped[Optional[float]] = mapped_column(Float)
    is_complete: Mapped[bool] = mapped_column(Boolean, default=False)

    # ── PFF snapshot at time of start (for hot/cold start pattern learning)
    pff_score: Mapped[Optional[float]] = mapped_column(Float, default=0.0)
    pff_label: Mapped[Optional[str]] = mapped_column(String(32), default="NEUTRAL")

    # ── Risk Profile (computed daily by pipeline, served via /v1/risk/today)
    # Automatically populated — no manual commands needed.
    risk_score: Mapped[Optional[int]] = mapped_column(Integer, default=0)
    risk_tier: Mapped[Optional[str]] = mapped_column(String(16), default="LOW")      # HIGH / MODERATE / LOW
    risk_flags: Mapped[Optional[str]] = mapped_column(String(256))                   # pipe-delimited flag names
    combo_risk: Mapped[Optional[bool]] = mapped_column(Boolean, default=False)        # True when 3+ flags active
    season_era_tier: Mapped[Optional[str]] = mapped_column(String(16), default="NORMAL")  # NORMAL / STRUGGLING / DISASTER
    park_extreme: Mapped[Optional[bool]] = mapped_column(Boolean, default=False)      # True for Coors, Chase, etc.
    park_hits_multiplier: Mapped[Optional[float]] = mapped_column(Float, default=1.0) # direct park multiplier applied

    # ── Hidden variable inputs (SKUs #14, #37, #38) — collected daily
    # The ML uses these to detect when formula residuals correlate with travel/catcher/velo
    catcher_strike_rate: Mapped[Optional[float]] = mapped_column(Float)
    tfi_rest_hours: Mapped[Optional[float]] = mapped_column(Float)
    tfi_tz_shift: Mapped[Optional[int]] = mapped_column(Integer)
    vaa_degrees: Mapped[Optional[float]] = mapped_column(Float)
    extension_ft: Mapped[Optional[float]] = mapped_column(Float)

    # ── First-inning performance (cold start detection)
    # ML uses these to learn which pitchers habitually struggle early
    first_inning_hits: Mapped[Optional[float]] = mapped_column(Float)
    first_inning_ks: Mapped[Optional[float]] = mapped_column(Float)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


# ─────────────────────────────────────────────────────────────
# ml_model_outputs
# ML engine predictions stored alongside formula predictions.
# The key table for the "comparison" view in the API and UI.
# ─────────────────────────────────────────────────────────────
class MLModelOutput(Base):
    __tablename__ = "ml_model_outputs"
    __table_args__ = (UniqueConstraint("pitcher_id", "game_id", name="uq_ml_output_pitcher_game"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    pitcher_id: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    game_id: Mapped[str] = mapped_column(String(32), nullable=False)
    game_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)

    # ── ML predictions
    ml_proj_hits: Mapped[Optional[float]] = mapped_column(Float)
    ml_proj_ks: Mapped[Optional[float]] = mapped_column(Float)
    ml_hssi: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ml_husi: Mapped[Optional[float]] = mapped_column(Float)
    ml_kssi: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ml_kusi: Mapped[Optional[float]] = mapped_column(Float)
    ml_hssi_grade: Mapped[Optional[str]] = mapped_column(String(4), nullable=True)
    ml_husi_grade: Mapped[Optional[str]] = mapped_column(String(4))
    ml_kssi_grade: Mapped[Optional[str]] = mapped_column(String(4), nullable=True)
    ml_kusi_grade: Mapped[Optional[str]] = mapped_column(String(4))

    # ── Comparison to formula
    hssi_delta: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    husi_delta: Mapped[Optional[float]] = mapped_column(Float)   # ml - formula
    kssi_delta: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    kusi_delta: Mapped[Optional[float]] = mapped_column(Float)
    hssi_divergence: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)
    husi_divergence: Mapped[Optional[str]] = mapped_column(String(16))   # ALIGNED/DIVERGENT/CONFLICT
    kssi_divergence: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)
    kusi_divergence: Mapped[Optional[str]] = mapped_column(String(16))
    consensus_hssi_grade: Mapped[Optional[str]] = mapped_column(String(8), nullable=True)
    consensus_husi_grade: Mapped[Optional[str]] = mapped_column(String(8))
    consensus_kssi_grade: Mapped[Optional[str]] = mapped_column(String(8), nullable=True)
    consensus_kusi_grade: Mapped[Optional[str]] = mapped_column(String(8))

    # ── Model metadata
    model_version: Mapped[Optional[str]] = mapped_column(String(32))
    training_samples: Mapped[Optional[int]] = mapped_column(Integer)
    mae_hits: Mapped[Optional[float]] = mapped_column(Float)
    mae_ks: Mapped[Optional[float]] = mapped_column(Float)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


# ─────────────────────────────────────────────────────────────
# statcast_pitcher_cache
# ─────────────────────────────────────────────────────────────
# Every stat Axiom fetches from Baseball Savant is written here.
# This is Axiom's insurance policy: if Baseball Savant goes down,
# we fall back to this table. Over time it becomes our proprietary
# historical Statcast dataset — no external dependency needed.
# ─────────────────────────────────────────────────────────────
class StatcastPitcherCache(Base):
    __tablename__ = "statcast_pitcher_cache"
    __table_args__ = (UniqueConstraint("pitcher_id", "season", name="uq_statcast_pitcher_season"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    pitcher_id: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    season: Mapped[str] = mapped_column(String(8), nullable=False)
    player_name: Mapped[Optional[str]] = mapped_column(String(128))

    # ── Statcast metrics (season-to-date at time of last fetch)
    swstr_pct: Mapped[Optional[float]] = mapped_column(Float)
    hard_hit_pct: Mapped[Optional[float]] = mapped_column(Float)
    gb_pct: Mapped[Optional[float]] = mapped_column(Float)
    innings_pitched: Mapped[Optional[float]] = mapped_column(Float)

    # ── Provenance
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    data_source: Mapped[str] = mapped_column(String(32), default="baseball_savant")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


# ─────────────────────────────────────────────────────────────
# axiom_pitcher_stats
# ─────────────────────────────────────────────────────────────
# Axiom's proprietary growing pitcher dataset.
# Aggregates stats from ALL sources (MLB Stats API + Statcast + future)
# into one canonical row per pitcher per season, updated daily by the pipeline.
#
# Over time this becomes Axiom's owned historical database:
#   Year 1: mirrors external APIs
#   Year 3: starts filling gaps external APIs miss
#   Year 5+: becomes the primary source that external APIs supplement
#
# This is what makes Axiom defensible as a business.
# ─────────────────────────────────────────────────────────────
class AxiomPitcherStats(Base):
    __tablename__ = "axiom_pitcher_stats"
    __table_args__ = (UniqueConstraint("pitcher_id", "season", name="uq_axiom_pitcher_season"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    pitcher_id: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    season: Mapped[str] = mapped_column(String(8), nullable=False)
    player_name: Mapped[Optional[str]] = mapped_column(String(128), index=True)
    team_id: Mapped[Optional[str]] = mapped_column(String(32))

    # ── MLB Stats API layer
    season_era: Mapped[Optional[float]] = mapped_column(Float)
    season_k_per_9: Mapped[Optional[float]] = mapped_column(Float)
    season_h_per_9: Mapped[Optional[float]] = mapped_column(Float)
    season_bb_per_9: Mapped[Optional[float]] = mapped_column(Float)
    season_k_pct: Mapped[Optional[float]] = mapped_column(Float)
    season_go_ao: Mapped[Optional[float]] = mapped_column(Float)
    avg_ip_per_start: Mapped[Optional[float]] = mapped_column(Float)
    mlb_service_years: Mapped[Optional[float]] = mapped_column(Float)
    games_started: Mapped[Optional[int]] = mapped_column(Integer)
    total_ip: Mapped[Optional[float]] = mapped_column(Float)

    # ── Statcast layer
    season_swstr_pct: Mapped[Optional[float]] = mapped_column(Float)
    season_hard_hit_pct: Mapped[Optional[float]] = mapped_column(Float)
    season_gb_pct: Mapped[Optional[float]] = mapped_column(Float)

    # ── Axiom proprietary layer (our formula outputs — built by us, owned by us)
    axiom_hssi_avg: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    axiom_husi_avg: Mapped[Optional[float]] = mapped_column(Float)
    axiom_kssi_avg: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    axiom_kusi_avg: Mapped[Optional[float]] = mapped_column(Float)
    axiom_hssi_trend: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    axiom_husi_trend: Mapped[Optional[float]] = mapped_column(Float)
    axiom_kssi_trend: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    axiom_kusi_trend: Mapped[Optional[float]] = mapped_column(Float)
    axiom_starts_scored: Mapped[Optional[int]] = mapped_column(Integer)

    # ── Data provenance timestamps
    mlb_stats_last_updated: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    statcast_last_updated: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


# ─────────────────────────────────────────────────────────────
# axiom_game_lineup — Axiom's proprietary batter vault
#
# Stores every batter's stats for every game we score.
# This is Axiom's ownership of lineup data — if MLB Stats API
# goes down we have historical lineup profiles to fall back on.
#
# Also powers lineup fluidity analysis: the K-rate spread between
# the top and bottom of the batting order tells the simulation how
# likely a manager is to pinch-hit in late innings (TTO3).
# ─────────────────────────────────────────────────────────────
class AxiomGameLineup(Base):
    __tablename__ = "axiom_game_lineup"
    __table_args__ = (
        UniqueConstraint("game_id", "team_id", "batter_id", name="uq_game_team_batter"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    # ── Game context
    game_id: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    team_id: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    game_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    season: Mapped[str] = mapped_column(String(8), nullable=False)
    side: Mapped[Optional[str]] = mapped_column(String(8))  # "home" or "away"
    lineup_confirmed: Mapped[bool] = mapped_column(Boolean, default=False)

    # ── Individual batter identity
    batter_id: Mapped[str] = mapped_column(String(32), nullable=False)
    batter_name: Mapped[Optional[str]] = mapped_column(String(128))
    batting_order: Mapped[Optional[int]] = mapped_column(Integer)  # 1-9 slot

    # ── Season hitting stats (captured at game-day snapshot)
    k_rate: Mapped[Optional[float]] = mapped_column(Float)       # strikeout rate per AB (%)
    k_per_pa: Mapped[Optional[float]] = mapped_column(Float)     # strikeout rate per PA (%)
    bb_rate: Mapped[Optional[float]] = mapped_column(Float)      # walk rate per PA (%)
    avg: Mapped[Optional[float]] = mapped_column(Float)          # batting average
    obp: Mapped[Optional[float]] = mapped_column(Float)          # on-base percentage
    slg: Mapped[Optional[float]] = mapped_column(Float)          # slugging percentage
    at_bats: Mapped[Optional[int]] = mapped_column(Integer)      # sample size

    # ── SKU #39 — Swing Plane Collision: batter swing profile (Baseball Savant bat-tracking)
    # Stored here for longitudinal analysis: over multiple seasons Axiom builds its own
    # historical batter swing-profile dataset, independent of Baseball Savant availability.
    avg_attack_angle: Mapped[Optional[float]] = mapped_column(Float)  # degrees, ideal 5-20°
    swing_tilt: Mapped[Optional[float]] = mapped_column(Float)        # degrees, higher = steeper plane

    # ── Axiom-computed lineup fluidity metrics (for simulation)
    # lineup_slot_danger: normalized danger score for this batting slot (0-100).
    # High score = this batter is dangerous = pitcher must work hard here.
    # Used by the simulation to weight TTO3 pinch-hitter probability.
    lineup_slot_danger: Mapped[Optional[float]] = mapped_column(Float)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


# ─────────────────────────────────────────────────────────────
# pipeline_run_log — records every pipeline execution
# ─────────────────────────────────────────────────────────────
class PipelineRunLog(Base):
    __tablename__ = "pipeline_run_log"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    run_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)
    sport: Mapped[Optional[str]] = mapped_column(String(8))  # "MLB" | "NFL" | "NHL" — nullable for backward compat
    target_date: Mapped[date] = mapped_column(Date, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)  # "success" | "error" | "no_data"
    pitchers_scored: Mapped[int] = mapped_column(Integer, default=0)
    games_processed: Mapped[int] = mapped_column(Integer, default=0)
    elapsed_seconds: Mapped[Optional[float]] = mapped_column(Float)
    error_message: Mapped[Optional[str]] = mapped_column(Text)
    dry_run: Mapped[bool] = mapped_column(Boolean, default=False)


# ─────────────────────────────────────────────────────────────
# api_keys — B2B client authentication keys
# ─────────────────────────────────────────────────────────────
class ApiKey(Base):
    __tablename__ = "api_keys"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    key: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True)
    client_name: Mapped[str] = mapped_column(String(128), nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


# ═════════════════════════════════════════════════════════════
# NFL TABLES
# All prefixed with nfl_ — zero collision risk with MLB tables.
# ═════════════════════════════════════════════════════════════

# ─────────────────────────────────────────────────────────────
# nfl_games — one row per NFL game
# ─────────────────────────────────────────────────────────────
class NFLGame(Base):
    __tablename__ = "nfl_games"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    game_id: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True)
    game_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    season_year: Mapped[int] = mapped_column(Integer, nullable=False)
    week: Mapped[int] = mapped_column(Integer, nullable=False)
    home_team: Mapped[str] = mapped_column(String(64), nullable=False)
    away_team: Mapped[str] = mapped_column(String(64), nullable=False)
    stadium: Mapped[Optional[str]] = mapped_column(String(128))
    surface: Mapped[Optional[str]] = mapped_column(String(32))
    is_dome: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    starters: Mapped[list["NFLQBStarter"]] = relationship(back_populates="game")
    features: Mapped[list["NFLQBFeaturesDaily"]] = relationship(back_populates="game")
    outputs: Mapped[list["NFLModelOutputDaily"]] = relationship(back_populates="game")


# ─────────────────────────────────────────────────────────────
# nfl_qb_starters — one row per QB starting assignment per week
# ─────────────────────────────────────────────────────────────
class NFLQBStarter(Base):
    __tablename__ = "nfl_qb_starters"
    __table_args__ = (UniqueConstraint("game_id", "qb_name", name="uq_nfl_starter_game_qb"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    game_id: Mapped[str] = mapped_column(String(64), ForeignKey("nfl_games.game_id"), nullable=False, index=True)
    qb_name: Mapped[str] = mapped_column(String(128), nullable=False)
    team: Mapped[str] = mapped_column(String(64), nullable=False)
    opponent: Mapped[str] = mapped_column(String(64), nullable=False)
    is_home: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    injury_designation: Mapped[Optional[str]] = mapped_column(String(32))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    game: Mapped["NFLGame"] = relationship(back_populates="starters")


# ─────────────────────────────────────────────────────────────
# nfl_qb_features_daily — intermediate block scores, one row per QB per week
# ─────────────────────────────────────────────────────────────
class NFLQBFeaturesDaily(Base):
    __tablename__ = "nfl_qb_features_daily"
    __table_args__ = (UniqueConstraint("game_id", "qb_name", name="uq_nfl_features_game_qb"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    game_id: Mapped[str] = mapped_column(String(64), ForeignKey("nfl_games.game_id"), nullable=False, index=True)
    qb_name: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    team: Mapped[str] = mapped_column(String(64), nullable=False)
    week: Mapped[int] = mapped_column(Integer, nullable=False)
    season_year: Mapped[int] = mapped_column(Integer, nullable=False)

    # ── QPYI block scores (0–100 each)
    osw_score: Mapped[Optional[float]] = mapped_column(Float)
    qsr_score: Mapped[Optional[float]] = mapped_column(Float)
    gsp_score: Mapped[Optional[float]] = mapped_column(Float)
    scb_score: Mapped[Optional[float]] = mapped_column(Float)
    pdr_score: Mapped[Optional[float]] = mapped_column(Float)
    ens_score: Mapped[Optional[float]] = mapped_column(Float)
    dsr_score: Mapped[Optional[float]] = mapped_column(Float)
    rct_score: Mapped[Optional[float]] = mapped_column(Float)

    # ── QTDI-specific block scores (0–100 each)
    ord_score:    Mapped[Optional[float]] = mapped_column(Float)
    qtr_score:    Mapped[Optional[float]] = mapped_column(Float)
    gsp_td_score: Mapped[Optional[float]] = mapped_column(Float)
    scb_td_score: Mapped[Optional[float]] = mapped_column(Float)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    game: Mapped["NFLGame"] = relationship(back_populates="features")


# ─────────────────────────────────────────────────────────────
# nfl_model_outputs_daily — final scored output, one row per QB per market per week
# ─────────────────────────────────────────────────────────────
class NFLModelOutputDaily(Base):
    __tablename__ = "nfl_model_outputs_daily"
    __table_args__ = (
        UniqueConstraint("game_id", "qb_name", "market", name="uq_nfl_output_game_qb_market"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    game_id: Mapped[str] = mapped_column(String(64), ForeignKey("nfl_games.game_id"), nullable=False, index=True)
    qb_name: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    team: Mapped[str] = mapped_column(String(64), nullable=False)
    opponent: Mapped[str] = mapped_column(String(64), nullable=False)
    week: Mapped[int] = mapped_column(Integer, nullable=False)
    season_year: Mapped[int] = mapped_column(Integer, nullable=False)
    market: Mapped[str] = mapped_column(String(32), nullable=False)  # "passing_yards" | "touchdowns"

    # ── Index scores
    qpyi_score: Mapped[Optional[float]] = mapped_column(Float)
    qtdi_score: Mapped[Optional[float]] = mapped_column(Float)
    grade: Mapped[Optional[str]] = mapped_column(String(8))

    # ── Projection
    projected_value: Mapped[Optional[float]] = mapped_column(Float)
    prop_line: Mapped[Optional[float]] = mapped_column(Float)
    edge: Mapped[Optional[float]] = mapped_column(Float)  # projected_value minus prop_line
    signal_tag: Mapped[Optional[str]] = mapped_column(String(32))

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    game: Mapped["NFLGame"] = relationship(back_populates="outputs")


# ══ NHL Tables ═══════════════════════════════════════════════════════════════
# All prefixed with nhl_ — zero collision risk with MLB/NFL tables.
# ═════════════════════════════════════════════════════════════════════════════

# ─────────────────────────────────────────────────────────────
# nhl_games — one row per NHL game
# ─────────────────────────────────────────────────────────────
class NHLGame(Base):
    __tablename__ = "nhl_games"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    game_id: Mapped[str] = mapped_column(String(32), nullable=False, unique=True, index=True)
    game_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    season_year: Mapped[int] = mapped_column(Integer, nullable=False)
    series_game_number: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    home_team: Mapped[str] = mapped_column(String(16), nullable=False)
    away_team: Mapped[str] = mapped_column(String(16), nullable=False)
    home_series_wins: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    away_series_wins: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    venue: Mapped[Optional[str]] = mapped_column(String(128))
    is_playoff: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    rosters: Mapped[list["NHLGameRoster"]] = relationship(back_populates="game")
    goalie_features: Mapped[list["NHLGoalieFeaturesDaily"]] = relationship(back_populates="game")
    skater_features: Mapped[list["NHLSkaterFeaturesDaily"]] = relationship(back_populates="game")
    outputs: Mapped[list["NHLModelOutputDaily"]] = relationship(back_populates="game")


# ─────────────────────────────────────────────────────────────
# nhl_game_rosters — one row per player per game
# ─────────────────────────────────────────────────────────────
class NHLGameRoster(Base):
    __tablename__ = "nhl_game_rosters"
    __table_args__ = (UniqueConstraint("game_id", "player_id", name="uq_nhl_roster_game_player"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    game_id: Mapped[str] = mapped_column(String(32), ForeignKey("nhl_games.game_id"), nullable=False, index=True)
    player_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    player_name: Mapped[str] = mapped_column(String(128), nullable=False)
    team: Mapped[str] = mapped_column(String(16), nullable=False)
    opponent: Mapped[str] = mapped_column(String(16), nullable=False)
    position: Mapped[str] = mapped_column(String(8), nullable=False)
    is_home: Mapped[bool] = mapped_column(Boolean, nullable=False)
    line_number: Mapped[Optional[int]] = mapped_column(Integer)
    pp_unit: Mapped[Optional[int]] = mapped_column(Integer)
    injury_designation: Mapped[Optional[str]] = mapped_column(String(32))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    game: Mapped["NHLGame"] = relationship(back_populates="rosters")


# ─────────────────────────────────────────────────────────────
# nhl_goalie_features_daily — GSAI block scores, one row per goalie per game
# ─────────────────────────────────────────────────────────────
class NHLGoalieFeaturesDaily(Base):
    __tablename__ = "nhl_goalie_features_daily"
    __table_args__ = (UniqueConstraint("game_id", "player_id", name="uq_nhl_goalie_feat_game_player"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    game_id: Mapped[str] = mapped_column(String(32), ForeignKey("nhl_games.game_id"), nullable=False, index=True)
    player_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    player_name: Mapped[str] = mapped_column(String(128), nullable=False)
    team: Mapped[str] = mapped_column(String(16), nullable=False)

    # ── GSAI block scores (0–100 each)
    gsai_score: Mapped[Optional[float]] = mapped_column(Float)
    gss_score: Mapped[Optional[float]] = mapped_column(Float)   # Goalie Save Suppression   29%
    osq_score: Mapped[Optional[float]] = mapped_column(Float)   # Opponent Shooting Quality  24%
    top_score: Mapped[Optional[float]] = mapped_column(Float)   # Tactical / Operational     18%
    gen_score: Mapped[Optional[float]] = mapped_column(Float)   # Game Environment           16%
    rfs_score: Mapped[Optional[float]] = mapped_column(Float)   # Referee Flow Score          8%
    tsc_score: Mapped[Optional[float]] = mapped_column(Float)   # Team Structure & Coverage   5%

    projected_shots: Mapped[Optional[float]] = mapped_column(Float)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    game: Mapped["NHLGame"] = relationship(back_populates="goalie_features")


# ─────────────────────────────────────────────────────────────
# nhl_skater_features_daily — PPSI block scores, one row per skater per game
# ─────────────────────────────────────────────────────────────
class NHLSkaterFeaturesDaily(Base):
    __tablename__ = "nhl_skater_features_daily"
    __table_args__ = (UniqueConstraint("game_id", "player_id", name="uq_nhl_skater_feat_game_player"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    game_id: Mapped[str] = mapped_column(String(32), ForeignKey("nhl_games.game_id"), nullable=False, index=True)
    player_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    player_name: Mapped[str] = mapped_column(String(128), nullable=False)
    team: Mapped[str] = mapped_column(String(16), nullable=False)

    # ── PPSI block scores (0–100 each)
    ppsi_score: Mapped[Optional[float]] = mapped_column(Float)
    osr_score: Mapped[Optional[float]] = mapped_column(Float)   # Opponent Scoring Resistance 28%
    pmr_score: Mapped[Optional[float]] = mapped_column(Float)   # Player Matchup Rating       22%
    per_score: Mapped[Optional[float]] = mapped_column(Float)   # Player Efficiency Rating    18%
    pop_score: Mapped[Optional[float]] = mapped_column(Float)   # Points Operational          14%
    rps_score: Mapped[Optional[float]] = mapped_column(Float)   # Referee PP Score            10%
    tld_score: Mapped[Optional[float]] = mapped_column(Float)   # Top-Line Deployment          8%

    # ── Projections
    projected_pts: Mapped[Optional[float]] = mapped_column(Float)
    projected_sog: Mapped[Optional[float]] = mapped_column(Float)
    projected_goals: Mapped[Optional[float]] = mapped_column(Float)
    projected_assists: Mapped[Optional[float]] = mapped_column(Float)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    game: Mapped["NHLGame"] = relationship(back_populates="skater_features")


# ─────────────────────────────────────────────────────────────
# nhl_model_outputs_daily — final scored output, one row per player per market per game
# ─────────────────────────────────────────────────────────────
class NHLModelOutputDaily(Base):
    __tablename__ = "nhl_model_outputs_daily"
    __table_args__ = (
        UniqueConstraint("game_id", "player_id", "market", name="uq_nhl_output_game_player_market"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    game_id: Mapped[str] = mapped_column(String(32), ForeignKey("nhl_games.game_id"), nullable=False, index=True)
    player_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    player_name: Mapped[str] = mapped_column(String(128), nullable=False)
    team: Mapped[str] = mapped_column(String(16), nullable=False)
    opponent: Mapped[str] = mapped_column(String(16), nullable=False)
    position: Mapped[str] = mapped_column(String(8), nullable=False)
    market: Mapped[str] = mapped_column(String(32), nullable=False)  # points | goals | assists | shots_on_goal | shots_faced

    # ── Index scores
    gsai_score: Mapped[Optional[float]] = mapped_column(Float)
    ppsi_score: Mapped[Optional[float]] = mapped_column(Float)
    grade: Mapped[Optional[str]] = mapped_column(String(8))

    # ── Projection and prop line
    projected_value: Mapped[float] = mapped_column(Float, nullable=False)
    prop_line: Mapped[Optional[float]] = mapped_column(Float)
    edge: Mapped[Optional[float]] = mapped_column(Float)        # projected_value minus prop_line
    signal_tag: Mapped[Optional[str]] = mapped_column(String(32))

    # ── ML engine
    ml_projection: Mapped[Optional[float]] = mapped_column(Float)
    ml_signal: Mapped[Optional[str]] = mapped_column(String(16))  # ALIGNED | LEAN | SPLIT

    playoff_discount_applied: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    game: Mapped["NHLGame"] = relationship(back_populates="outputs")
