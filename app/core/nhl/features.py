"""
NHL Feature Sets — the data contracts between the live data fetcher and
the GSAUI / PPUI scoring engines.

Convention (mirrors the baseball PitcherFeatureSet):
  - All block sub-scores are already normalized to 0-100
  - 50 = neutral (no edge either way)
  - >50 favors the UNDER (fewer shots / fewer points)
  - <50 favors the OVER
  - None fields fall back to 50 inside the engines
"""

from dataclasses import dataclass
from typing import Optional


# ──────────────────────────────────────────────────────────────────────────────
# Shared game context (used by both engines)
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class NHLGameContext:
    game_id: str = ""
    game_date: str = ""
    home_team: str = ""
    away_team: str = ""
    venue: str = ""
    series_game_number: int = 0        # 1–7 for playoffs
    home_series_wins: int = 0
    away_series_wins: int = 0

    # Back-to-back and rest (key environment variable)
    home_b2b: bool = False             # home team played yesterday
    away_b2b: bool = False
    home_rest_days: int = 2            # days since last game
    away_rest_days: int = 2

    # Referee crew (limited public data — defaults to neutral)
    referee_crew_pp_per_game: Optional[float] = None   # avg PP calls this crew makes


# ──────────────────────────────────────────────────────────────────────────────
# Goalie Feature Set  →  feeds GSAUI
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class NHLGoalieFeatureSet:
    # Identity
    player_id: int = 0
    player_name: str = ""
    team: str = ""
    is_home: bool = False
    is_confirmed_starter: bool = True

    # Base rate (drives projected shots — analogous to blended_h_per_9)
    avg_shots_faced_per_game: float = 29.0   # season average shots on goal faced

    # Game context (injected from NHLGameContext)
    ctx: Optional[NHLGameContext] = None

    # ── OSQ: Opponent Shooting Quality (27%)
    # High score = opponent is a WEAK shooter = fewer quality shots → under
    osq_shots_pg: Optional[float] = None       # opponent avg shots/game (reverse: high shots = low score)
    osq_shooting_pct: Optional[float] = None   # opponent shooting% (reverse)
    osq_pp_pct: Optional[float] = None         # opponent PP% (reverse)
    osq_high_danger_rate: Optional[float] = None  # opponent high-danger chance rate (reverse)
    osq_series_momentum: Optional[float] = None   # series context: opponent desperate / not
    osq_xgf_per_60: Optional[float] = None    # opponent xGF/60 if available (reverse)

    # ── GSS: Goalie Save Suppression (26%)
    # High score = goalie is elite at stopping pucks
    gss_sv_pct: Optional[float] = None          # season save% (normal: higher = better)
    gss_gsax: Optional[float] = None            # goals saved above expected (normal)
    gss_hd_sv_pct: Optional[float] = None       # high-danger save% (normal)
    gss_playoff_sv_pct: Optional[float] = None  # playoff-specific save% this postseason
    gss_rebound_control: Optional[float] = None # rebound control rate (normal: fewer rebounds = better)
    gss_consistency: Optional[float] = None     # start-to-start consistency score

    # ── GEN: Game Environment (16%)
    # High score = environment favors fewer shots (rested team, dome, home ice)
    gen_is_home: Optional[float] = None          # home ice advantage (normal)
    gen_rest_days: Optional[float] = None        # days of rest (normal)
    gen_b2b_penalty: Optional[float] = None      # B2B fatigue (reverse: being on B2B = lower score)
    gen_series_game: Optional[float] = None      # game number in series fatigue
    gen_opponent_b2b: Optional[float] = None     # opponent on B2B (normal: tired opponent = better for under)

    # ── TOP: Tactical / Operational (18%)
    # High score = favorable conditions for the goalie
    top_starter_prob: Optional[float] = None     # probability this is the confirmed starter
    top_pk_pct: Optional[float] = None           # own team PK% (normal: good PK = fewer quality shots)
    top_coach_defensive: Optional[float] = None  # coach defensive scheme rating
    top_injury_status: Optional[float] = None    # own team health (normal)
    top_opponent_pp_rate: Optional[float] = None # opponent PP opportunities per game (reverse)

    # ── RFS: Referee Flow Score (8%)
    # High score = ref crew calls fewer penalties → fewer PP shots
    rfs_crew_pp_rate: Optional[float] = None     # ref crew avg PPs called per game (reverse)
    rfs_home_bias: Optional[float] = None        # ref crew home team penalty differential

    # ── TSC: Team Structure & Coverage (5%)
    # High score = defense suppresses shots
    tsc_blocks_pg: Optional[float] = None        # team shot blocks per game (normal)
    tsc_cf_pct: Optional[float] = None           # team Corsi For% (normal: >50 = possession dominant)
    tsc_dzone_exit_pct: Optional[float] = None   # defensive zone exit% (normal)


# ──────────────────────────────────────────────────────────────────────────────
# Skater Feature Set  →  feeds PPUI
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class NHLSkaterFeatureSet:
    # Identity
    player_id: int = 0
    player_name: str = ""
    team: str = ""
    position: str = ""      # C, LW, RW, D
    is_home: bool = False
    line_number: int = 1    # 1–4 (forward lines) or 1–3 (D pairs)
    pp_unit: int = 1        # 1 or 2 (power play unit)

    # Base rates (drive projections — all three are bettable markets)
    avg_points_per_game: float = 0.5    # season points/game
    avg_shots_per_game: float = 2.5     # season shots on goal/game
    avg_shooting_pct: float = 0.105     # season shooting percentage (goals / shots)

    # Game context
    ctx: Optional[NHLGameContext] = None

    # ── OSR: Opponent Scoring Resistance (28%)
    # High score = opponent ALLOWS scoring → easier to get points
    osr_goals_against_pg: Optional[float] = None   # opponent GA/game (normal: high GA = easier)
    osr_sv_pct_against: Optional[float] = None     # opponent goalie save% (reverse: high sv% = hard)
    osr_shots_against_pg: Optional[float] = None   # opponent shots against per game (normal)
    osr_pk_pct_against: Optional[float] = None     # opponent PK% (reverse: good PK = harder)
    osr_hd_chances_against: Optional[float] = None # opponent high-danger chances against per game (normal)
    osr_xga_per_60: Optional[float] = None         # opponent xGA/60 (normal: higher xGA = easier)

    # ── PMR: Player Matchup Rating (22%)
    # High score = favorable individual matchup
    pmr_shooting_pct: Optional[float] = None       # player shooting% (normal)
    pmr_opp_goalie_sv_pct: Optional[float] = None  # opposing goalie sv% (reverse)
    pmr_zone_start_pct: Optional[float] = None     # offensive zone start% (normal: more OZ starts = more chances)
    pmr_opp_goalie_gsax: Optional[float] = None    # opposing goalie GSAx (reverse: hot goalie = harder)
    pmr_shot_location: Optional[float] = None      # player's shot quality (slot% / high-danger rate, normal)

    # ── PER: Player Efficiency Rating (18%)
    # High score = player generates a lot of scoring chances efficiently
    per_shots_pg: Optional[float] = None           # shots on goal per game (normal)
    per_points_pg: Optional[float] = None          # points per game rate (normal)
    per_primary_pts_pg: Optional[float] = None     # primary points per game (goals + primary assists, normal)
    per_ixg_per_60: Optional[float] = None         # individual xG per 60 (normal)
    per_shooting_talent: Optional[float] = None    # cumulative G / cumulative xG skill score (normal)

    # ── POP: Points Operational (14%)
    # High score = coach deploys player heavily with prime opportunities
    pop_toi_pg: Optional[float] = None             # average TOI per game (normal)
    pop_pp_toi_pg: Optional[float] = None          # PP time on ice per game (normal)
    pop_linemate_quality: Optional[float] = None   # quality of linemates (normal)
    pop_injury_linemates: Optional[float] = None   # are top linemates healthy? (normal)

    # ── RPS: Referee PP Score (10%)
    # High score = ref crew calls many PPs → more point chances for PP1 players
    rps_crew_pp_rate: Optional[float] = None       # ref crew avg PPs per game (normal for PP players)
    rps_player_draw_rate: Optional[float] = None   # player penalty-drawing tendency (normal)

    # ── TLD: Top-Line Deployment (8%)
    # High score = player is a heavy-usage top-line contributor
    tld_toi_percentile: Optional[float] = None     # player's TOI rank on team (normal)
    tld_line_position: Optional[float] = None      # line # converted to score (1st=100, 4th=0)
    tld_pp1_status: Optional[float] = None         # on PP1? (100=yes, 0=no)
