"""
SQLAlchemy ORM models for the Axiom database (axiom_db).

Tables:
  - games
  - probable_pitchers
  - sportsbook_props
  - pitcher_features_daily
  - model_outputs_daily
  - backtest_results
  - umpire_profiles
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
    husi: Mapped[Optional[float]] = mapped_column(Float)
    kusi: Mapped[Optional[float]] = mapped_column(Float)
    husi_base: Mapped[Optional[float]] = mapped_column(Float)
    kusi_base: Mapped[Optional[float]] = mapped_column(Float)
    husi_interaction: Mapped[Optional[float]] = mapped_column(Float)
    kusi_interaction: Mapped[Optional[float]] = mapped_column(Float)
    husi_volatility: Mapped[Optional[float]] = mapped_column(Float)
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
    formula_husi: Mapped[Optional[float]] = mapped_column(Float)
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
    ml_husi: Mapped[Optional[float]] = mapped_column(Float)
    ml_kusi: Mapped[Optional[float]] = mapped_column(Float)
    ml_husi_grade: Mapped[Optional[str]] = mapped_column(String(4))
    ml_kusi_grade: Mapped[Optional[str]] = mapped_column(String(4))

    # ── Comparison to formula
    husi_delta: Mapped[Optional[float]] = mapped_column(Float)   # ml - formula
    kusi_delta: Mapped[Optional[float]] = mapped_column(Float)
    husi_divergence: Mapped[Optional[str]] = mapped_column(String(16))   # ALIGNED/DIVERGENT/CONFLICT
    kusi_divergence: Mapped[Optional[str]] = mapped_column(String(16))
    consensus_husi_grade: Mapped[Optional[str]] = mapped_column(String(8))
    consensus_kusi_grade: Mapped[Optional[str]] = mapped_column(String(8))

    # ── Model metadata
    model_version: Mapped[Optional[str]] = mapped_column(String(32))
    training_samples: Mapped[Optional[int]] = mapped_column(Integer)
    mae_hits: Mapped[Optional[float]] = mapped_column(Float)
    mae_ks: Mapped[Optional[float]] = mapped_column(Float)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
