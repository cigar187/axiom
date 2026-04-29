"""
Pydantic schemas for API request/response validation.
These are the shapes of data going in and out of every API endpoint.
"""
from datetime import date, datetime
from typing import Optional
from pydantic import BaseModel, Field


# ─────────────────────────────────────────────────────────────
# Shared sub-schemas
# ─────────────────────────────────────────────────────────────

class PropLine(BaseModel):
    sportsbook: Optional[str] = None
    market_type: Optional[str] = None
    line: Optional[float] = None
    over_odds: Optional[float] = None
    under_odds: Optional[float] = None
    implied_under_prob: Optional[float] = None


class HUSIDetail(BaseModel):
    husi: Optional[float] = None
    husi_base: Optional[float] = None
    husi_interaction: Optional[float] = None
    husi_volatility: Optional[float] = None
    grade: Optional[str] = None


class KUSIDetail(BaseModel):
    kusi: Optional[float] = None
    kusi_base: Optional[float] = None
    kusi_interaction: Optional[float] = None
    kusi_volatility: Optional[float] = None
    grade: Optional[str] = None


# ─────────────────────────────────────────────────────────────
# /v1/pitchers/today  — full ranked table row
# ─────────────────────────────────────────────────────────────

class PitcherTodayRow(BaseModel):
    date: date
    game: str
    pitcher: str
    pitcher_id: str
    team: str                          # abbreviation e.g. "LAD"
    team_name: Optional[str] = None   # full name e.g. "Los Angeles Dodgers"
    opponent: str
    opponent_name: Optional[str] = None
    handedness: Optional[str] = None

    # Hits
    hits_line: Optional[float] = None
    hits_under_odds: Optional[float] = None
    hits_implied_under_prob: Optional[float] = None
    base_hits: Optional[float] = None
    projected_hits: Optional[float] = None
    hits_edge: Optional[float] = None
    husi: Optional[float] = None
    husi_grade: Optional[str] = None

    # Strikeouts
    k_line: Optional[float] = None
    k_under_odds: Optional[float] = None
    k_implied_under_prob: Optional[float] = None
    base_ks: Optional[float] = None
    projected_ks: Optional[float] = None
    k_edge: Optional[float] = None
    kusi: Optional[float] = None
    kusi_grade: Optional[str] = None

    # Shared
    interaction_boost_husi: Optional[float] = None
    interaction_boost_kusi: Optional[float] = None
    volatility_penalty_husi: Optional[float] = None
    volatility_penalty_kusi: Optional[float] = None
    confidence: Optional[str] = None
    notes: Optional[str] = None
    data_quality_flag: Optional[str] = None

    # ML Engine 2 comparison (None if ML hasn't trained yet)
    ml_husi: Optional[float] = None
    ml_kusi: Optional[float] = None
    ml_husi_grade: Optional[str] = None
    ml_kusi_grade: Optional[str] = None
    ml_proj_hits: Optional[float] = None
    ml_proj_ks: Optional[float] = None
    husi_signal: Optional[str] = None   # ALIGNED / SLIGHT_DIFF / DIVERGENT / CONFLICT
    kusi_signal: Optional[str] = None

    # Entropy Filter — agreement between Engine 1 (formula) and Engine 2 (ML)
    hits_entropy:  Optional[float] = None  # |formula projected_hits − ml projected_hits|
    ks_entropy:    Optional[float] = None  # |formula projected_ks   − ml projected_ks|
    entropy_label: Optional[str]  = None  # ALIGNED / DIVERGING / HIGH_ENTROPY

    # B2B product tags — maps each field name to its Axiom SKU number
    # SKU #27=HUSI  #28=KUSI  #29=ML-ENGINE  #30=ENS  #31=UMP
    # #32=BFS  #33=PFF  #34=MGS  #35=PROPS  #36=PITCHER-PROFILE
    # #37=CATCHER-FRAMING  #14=TFI  #38=VAA-EXT
    product_tags: dict = {}

    # SKU #37 — Catcher Framing
    catcher_name: Optional[str] = None
    catcher_strike_rate: Optional[float] = None
    catcher_framing_label: Optional[str] = None
    catcher_kusi_adj: Optional[float] = None

    # SKU #14 — Travel & Fatigue Index
    tfi_rest_hours: Optional[float] = None
    tfi_tz_shift: Optional[int] = None
    tfi_getaway_day: Optional[bool] = None
    tfi_cross_timezone: Optional[bool] = None
    tfi_penalty_pct: Optional[float] = None
    tfi_label: Optional[str] = None

    # SKU #38 — VAA & Extension
    vaa_degrees: Optional[float] = None
    extension_ft: Optional[float] = None
    vaa_flat: Optional[bool] = None
    extension_elite: Optional[bool] = None

    # Risk profile (from daily risk scorer)
    risk_score: Optional[int] = None
    risk_tier: Optional[str] = None
    risk_flags: Optional[str] = None
    pff_score: Optional[float] = None
    pff_label: Optional[str] = None

    # ── Merlin Simulation Engine (N=2000) outputs
    # Populated after the daily pipeline runs the Monte Carlo simulation.
    # None = simulation has not run yet for this pitcher.
    sim_median_hits: Optional[float] = None     # most likely hits result
    sim_median_ks: Optional[float] = None       # most likely Ks result
    sim_over_pct_hits: Optional[float] = None   # % of runs: proj_hits > book line
    sim_under_pct_hits: Optional[float] = None  # % of runs: proj_hits < book line
    sim_p5_hits: Optional[float] = None         # 5th percentile — Shutdown Floor
    sim_p95_hits: Optional[float] = None        # 95th percentile — Shelling Ceiling
    sim_over_pct_ks: Optional[float] = None     # % of runs: proj_ks > book line
    sim_under_pct_ks: Optional[float] = None    # % of runs: proj_ks < book line
    sim_p5_ks: Optional[float] = None           # 5th percentile — Shutdown Floor
    sim_p95_ks: Optional[float] = None          # 95th percentile — Kill Streak Ceiling
    sim_confidence_hits: Optional[str] = None   # HIGH_OVER / HIGH_UNDER / LEAN_OVER / LEAN_UNDER / SPLIT
    sim_confidence_ks: Optional[str] = None
    sim_kill_streak_prob: Optional[float] = None  # % of runs where proj_ks >= 10


class PitchersTodayResponse(BaseModel):
    date: date
    generated_at: datetime
    pitcher_count: int
    pitchers: list[PitcherTodayRow]
    # Full Axiom product catalog included for B2B client reference
    catalog_url: str = "/v1/products"


# ─────────────────────────────────────────────────────────────
# /v1/rankings/today  — sorted by strongest under signal
# ─────────────────────────────────────────────────────────────

class RankingRow(BaseModel):
    rank: int
    pitcher: str
    pitcher_id: str
    team: str
    opponent: str
    market_type: str        # "strikeouts" | "hits_allowed"
    line: Optional[float] = None
    under_odds: Optional[float] = None
    projection: Optional[float] = None
    edge: Optional[float] = None
    index_score: Optional[float] = None   # HUSI or KUSI depending on market
    grade: Optional[str] = None
    confidence: Optional[str] = None
    data_quality_flag: Optional[str] = None


class RankingsTodayResponse(BaseModel):
    date: date
    generated_at: datetime
    rankings: list[RankingRow]


# ─────────────────────────────────────────────────────────────
# /v1/pitchers/{id}/profile  — deep dive on one pitcher
# ─────────────────────────────────────────────────────────────

class BlockScores(BaseModel):
    """All individual feature block scores stored for full transparency."""
    owc: Optional[float] = None
    pcs: Optional[float] = None
    ens: Optional[float] = None
    ops: Optional[float] = None
    uhs: Optional[float] = None
    dsc: Optional[float] = None
    ocr: Optional[float] = None
    pmr: Optional[float] = None
    per: Optional[float] = None
    kop: Optional[float] = None
    uks: Optional[float] = None
    tlr: Optional[float] = None


class PitcherProfile(BaseModel):
    pitcher_id: str
    pitcher_name: str
    team: str
    opponent: str
    game_id: str
    game_date: date
    handedness: Optional[str] = None
    lineup_confirmed: bool = False
    umpire_confirmed: bool = False
    data_quality_flag: Optional[str] = None

    husi: HUSIDetail
    kusi: KUSIDetail
    block_scores: BlockScores

    hits_prop: Optional[PropLine] = None
    k_prop: Optional[PropLine] = None

    base_hits: Optional[float] = None
    projected_hits: Optional[float] = None
    base_ks: Optional[float] = None
    projected_ks: Optional[float] = None

    hits_edge: Optional[float] = None
    k_edge: Optional[float] = None
    notes: Optional[str] = None


# ─────────────────────────────────────────────────────────────
# /v1/tasks/run-daily  — pipeline trigger
# ─────────────────────────────────────────────────────────────

class RunDailyRequest(BaseModel):
    target_date: Optional[date] = Field(
        default=None,
        description="Date to run pipeline for. Defaults to today if omitted.",
    )
    dry_run: bool = Field(
        default=False,
        description="If true, scores are computed but NOT saved to the database.",
    )


class RunDailyResponse(BaseModel):
    status: str
    target_date: date
    pitchers_scored: int
    dry_run: bool
    message: str
    elapsed_seconds: Optional[float] = None


# ─────────────────────────────────────────────────────────────
# /health
# ─────────────────────────────────────────────────────────────

class HealthResponse(BaseModel):
    status: str = "ok"
    service: str = "axiom-engine"
    version: str = "1.0.0"
    db: str = "unknown"
    last_pipeline_run: Optional[str] = None
    pitchers_scored_today: Optional[int] = None
