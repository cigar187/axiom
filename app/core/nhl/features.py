"""
NHL Feature Sets — the data contracts between the live data fetchers and
the GSAI / PPSI scoring engines.

The scoring engines only READ from these dataclasses.
Data fetchers only WRITE to these dataclasses.
This hard boundary means you can swap any data source without touching
the scoring logic — the same guarantee as PitcherFeatureSet and QBFeatureSet.

Convention (mirrors PitcherFeatureSet and QBFeatureSet):
  - All block sub-scores are already normalized to 0–100 before entering here
  - 50  = neutral (no edge either way)
  - >50 = favors the UNDER (fewer shots / fewer points)
  - <50 = favors the OVER
  - None fields fall back to 50 (neutral) inside the engines

Architecture:
  GSAI_base = 0.29×GSS + 0.24×OSQ + 0.18×TOP + 0.16×GEN + 0.08×RFS + 0.05×TSC
  PPSI_base = 0.28×OSR + 0.22×PMR + 0.18×PER + 0.14×POP + 0.10×RPS + 0.08×TLD

GSAI Blocks (6 blocks — NHLGoalieFeatureSet):
  GSS  Goalie Save Suppression          29%   6 inputs
  OSQ  Opponent Shooting Quality        24%   6 inputs
  TOP  Tactical / Operational           18%   5 inputs
  GEN  Game Environment                 16%   5 inputs
  RFS  Referee Flow Score                8%   2 inputs
  TSC  Team Structure & Coverage         5%   3 inputs

PPSI Blocks (6 blocks — NHLSkaterFeatureSet):
  OSR  Opponent Scoring Resistance      28%   6 inputs
  PMR  Player Matchup Rating            22%   5 inputs
  PER  Player Efficiency Rating         18%   5 inputs
  POP  Points Operational               14%   4 inputs
  RPS  Referee PP Score                 10%   2 inputs
  TLD  Top-Line Deployment               8%   3 inputs

Formula version: v1.0 — complete and closed.
"""

from dataclasses import dataclass
from typing import Optional


# ──────────────────────────────────────────────────────────────────────────────
# Shared game context (used by both engines)
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class NHLGameContext:
    game_id: str = ""
    game_date: str = ""          # YYYY-MM-DD
    home_team: str = ""          # team abbreviation, e.g. "TBL"
    away_team: str = ""          # team abbreviation, e.g. "FLA"
    venue: str = ""

    # Playoff series state
    series_game_number: int = 0  # 1–7 (0 = regular season)
    home_series_wins: int = 0
    away_series_wins: int = 0

    # Back-to-back and rest — the most impactful schedule variable
    home_b2b: bool = False        # home team played yesterday
    away_b2b: bool = False
    home_rest_days: int = 2       # calendar days since last game
    away_rest_days: int = 2

    # Referee crew (sourced from Warren Sharp / NHL refs historical data)
    referee_crew_pp_per_game: Optional[float] = None  # avg PP calls this crew makes per game


# ──────────────────────────────────────────────────────────────────────────────
# Goalie Feature Set  →  feeds GSAI
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class NHLGoalieFeatureSet:

    # ── Identity ──────────────────────────────────────────────────────────────
    player_id: int = 0
    player_name: str = ""
    team: str = ""               # team abbreviation, e.g. "TBL"
    opponent: str = ""           # opposing team abbreviation, e.g. "FLA"
    is_home: bool = False
    is_confirmed_starter: bool = True  # GV1 fires when False — highest single risk factor

    # ── Operational flag ──────────────────────────────────────────────────────
    # GV7 fires when False — never bet a goalie with no data
    gss_data_available: bool = True

    # ── Game context (injected from NHLGameContext) ───────────────────────────
    ctx: Optional[NHLGameContext] = None

    # ── Prop lines (filled by odds fetcher — The Rundown API) ─────────────────
    shots_line: Optional[float] = None           # shots-faced O/U line
    shots_over_odds: Optional[float] = None
    shots_under_odds: Optional[float] = None

    # ── Base rate (drives projected shots — sourced from OPPONENT shots-for/game)
    # Blended: 70% playoff shots-against avg / 30% regular season shots-against avg.
    # Sourced from OPPONENT shots-for per game — NOT the goalie's individual season stats.
    # A goalie who always faces 32 shots because their team always gives up 32 shots
    # is a team problem, not an individual goalie problem.
    avg_shots_faced_per_game: float = 29.0

    # ─────────────────────────────────────────────────────────────────────────
    # BLOCK 1 — GSS: Goalie Save Suppression  (GSAI 29%)
    # How elite is this goalie at stopping shots regardless of opponent?
    # High score = elite shot stopper → favors UNDER.
    #
    # Sources:
    #   gss_sv_pct          → NHL API / MoneyPuck
    #   gss_gsax            → Evolving Hockey
    #   gss_hd_sv_pct       → MoneyPuck
    #   gss_playoff_sv_pct  → NHL API (playoff game log)
    #   gss_rebound_control → MoneyPuck
    #   gss_consistency     → Evolving Hockey
    # ─────────────────────────────────────────────────────────────────────────
    gss_sv_pct: Optional[float] = None           # overall save% this season (higher = better)
    gss_gsax: Optional[float] = None             # goals saved above expected (higher = better)
    gss_hd_sv_pct: Optional[float] = None        # high-danger save% (higher = better)
    gss_playoff_sv_pct: Optional[float] = None   # save% in this postseason specifically
    gss_rebound_control: Optional[float] = None  # rebound control rate (fewer rebounds = better)
    gss_consistency: Optional[float] = None      # start-to-start variance in save% (higher = more consistent)

    # ─────────────────────────────────────────────────────────────────────────
    # BLOCK 2 — OSQ: Opponent Shooting Quality  (GSAI 24%)
    # How dangerous is the offense this goalie is facing tonight?
    # High score = weak opponent offense → favors UNDER.
    # All inputs are INVERTED — high opponent shots/pp/xGF = low OSQ score.
    #
    # Sources:
    #   osq_shots_pg        → NHL API
    #   osq_shooting_pct    → NHL API
    #   osq_pp_pct          → NHL API
    #   osq_high_danger_rate → MoneyPuck
    #   osq_series_momentum → NHL API (series win/loss record)
    #   osq_xgf_per_60      → Evolving Hockey
    # ─────────────────────────────────────────────────────────────────────────
    osq_shots_pg: Optional[float] = None          # opponent avg shots-for/game (inverted: high shots = low score)
    osq_shooting_pct: Optional[float] = None      # opponent team shooting% (inverted)
    osq_pp_pct: Optional[float] = None            # opponent power play% (inverted)
    osq_high_danger_rate: Optional[float] = None  # opponent high-danger chance rate (inverted)
    osq_series_momentum: Optional[float] = None   # win/loss trend in current series (inverted: momentum against = low score)
    osq_xgf_per_60: Optional[float] = None        # opponent expected goals for/60 (inverted)

    # ─────────────────────────────────────────────────────────────────────────
    # BLOCK 3 — TOP: Tactical / Operational  (GSAI 18%)
    # How is this goalie being deployed tonight?
    # High score = favorable conditions → favors UNDER.
    #
    # Sources:
    #   top_starter_prob       → Daily Faceoff / official NHL injury report
    #   top_pk_pct             → NHL API
    #   top_coach_defensive    → manual coaching tendency data
    #   top_injury_status      → official NHL injury report
    #   top_opponent_pp_rate   → NHL API (inverted: high opp PP rate = low score)
    # ─────────────────────────────────────────────────────────────────────────
    top_starter_prob: Optional[float] = None       # confirmed starter probability (100=confirmed, 0=backup)
    top_pk_pct: Optional[float] = None             # own team penalty kill% (higher = fewer quality PP shots faced)
    top_coach_defensive: Optional[float] = None    # coach defensive system rating (higher = more shot suppression)
    top_injury_status: Optional[float] = None      # own team defensive health (100=fully healthy, 0=key D out)
    top_opponent_pp_rate: Optional[float] = None   # opponent PP attempts per game (inverted: high rate = low score)

    # ─────────────────────────────────────────────────────────────────────────
    # BLOCK 4 — GEN: Game Environment  (GSAI 16%)
    # What does the game context look like tonight?
    # High score = environment suppresses shot volume → favors UNDER.
    #
    # Game 7 note: gen_series_game triggers GV6 (-1.5) automatically in the
    # volatility engine — both teams play cautiously under elimination pressure.
    #
    # Sources: NHL schedule data
    # ─────────────────────────────────────────────────────────────────────────
    gen_is_home: Optional[float] = None           # home ice advantage (100=home, 0=away)
    gen_rest_days: Optional[float] = None         # days of rest before this game (higher = more rested)
    gen_b2b_penalty: Optional[float] = None       # goalie's own team on B2B (inverted: B2B = low score)
    gen_series_game: Optional[float] = None       # series game number as a score (Game 7 = max volatility via GV6)
    gen_opponent_b2b: Optional[float] = None      # opponent on B2B (normal: tired opponent = better for UNDER)

    # ─────────────────────────────────────────────────────────────────────────
    # BLOCK 5 — RFS: Referee Flow Score  (GSAI 8%)
    # How will the officials affect power play volume tonight?
    # High score = low-PP crew → fewer high-danger power play shots → UNDER.
    #
    # Sources:
    #   rfs_crew_pp_rate → Warren Sharp / NHL referee historical data (inverted)
    #   rfs_home_bias    → NHL referee historical data
    # ─────────────────────────────────────────────────────────────────────────
    rfs_crew_pp_rate: Optional[float] = None      # ref crew avg PPs called/game (inverted: high PP crew = low score)
    rfs_home_bias: Optional[float] = None         # crew tendency to favor home team on calls

    # ─────────────────────────────────────────────────────────────────────────
    # BLOCK 6 — TSC: Team Structure & Coverage  (GSAI 5%)
    # How well does the defensive structure protect this goalie?
    # High score = defense suppresses shot quality → favors UNDER.
    #
    # Sources:
    #   tsc_blocks_pg      → NHL API
    #   tsc_cf_pct         → Natural Stat Trick (Corsi For%)
    #   tsc_dzone_exit_pct → Corey Sznajder / manual tracking
    # ─────────────────────────────────────────────────────────────────────────
    tsc_blocks_pg: Optional[float] = None          # team shot blocks per game (higher = better for UNDER)
    tsc_cf_pct: Optional[float] = None             # Corsi For% — shot attempt control (>50 = possession dominant)
    tsc_dzone_exit_pct: Optional[float] = None     # defensive zone exit success rate (higher = cleaner exits)


# ──────────────────────────────────────────────────────────────────────────────
# Skater Feature Set  →  feeds PPSI
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class NHLSkaterFeatureSet:

    # ── Identity ──────────────────────────────────────────────────────────────
    player_id: int = 0
    player_name: str = ""
    team: str = ""               # team abbreviation, e.g. "TBL"
    opponent: str = ""           # opposing team abbreviation, e.g. "FLA"
    position: str = ""           # C, LW, RW, D
    is_home: bool = False
    line_number: int = 1         # 1–4 (forward lines) or 1–3 (D pairs)
    pp_unit: int = 1             # 1 or 2 (power play unit; 1 = elite PP deployment)

    # ── Game context (injected from NHLGameContext) ───────────────────────────
    ctx: Optional[NHLGameContext] = None

    # ── Prop lines (filled by odds fetcher — The Rundown API) ─────────────────
    points_line: Optional[float] = None
    points_over_odds: Optional[float] = None
    points_under_odds: Optional[float] = None
    goals_line: Optional[float] = None
    goals_over_odds: Optional[float] = None
    goals_under_odds: Optional[float] = None
    assists_line: Optional[float] = None
    assists_over_odds: Optional[float] = None
    assists_under_odds: Optional[float] = None
    sog_line: Optional[float] = None              # shots on goal
    sog_over_odds: Optional[float] = None
    sog_under_odds: Optional[float] = None

    # ── Base rates (Bayesian-blended: 70% playoff / 30% regular season)
    # Small samples are shrunk toward league averages — the same logic as
    # blended_h_per_9 / blended_k_per_9 in the MLB engine.
    avg_points_per_game: float = 0.5              # blended pts/game (league avg default)
    avg_shots_per_game: float = 2.5               # blended shots on goal/game
    avg_shooting_pct: float = 0.105               # blended shooting% (goals / shots)

    # ─────────────────────────────────────────────────────────────────────────
    # BLOCK 1 — OSR: Opponent Scoring Resistance  (PPSI 28%)
    # How leaky is the defense and goalie this skater is facing tonight?
    # High score = opponent ALLOWS scoring → easier for this player to produce.
    #
    # Sources:
    #   osr_goals_against_pg      → NHL API
    #   osr_sv_pct_against        → NHL API / MoneyPuck (inverted: high sv% = hard to score)
    #   osr_shots_against_pg      → NHL API
    #   osr_pk_pct_against        → NHL API (inverted: good PK = harder to score)
    #   osr_hd_chances_against    → MoneyPuck
    #   osr_xga_per_60            → Evolving Hockey
    # ─────────────────────────────────────────────────────────────────────────
    osr_goals_against_pg: Optional[float] = None    # opponent GA/game (higher = leakier = higher score)
    osr_sv_pct_against: Optional[float] = None      # opponent goalie save% (inverted: high sv% = low score)
    osr_shots_against_pg: Optional[float] = None    # opponent shots-against/game (higher = easier matchup)
    osr_pk_pct_against: Optional[float] = None      # opponent PK% (inverted: elite PK = low score)
    osr_hd_chances_against: Optional[float] = None  # opponent high-danger chances against rate (higher = more open)
    osr_xga_per_60: Optional[float] = None          # opponent expected goals against/60 (higher = more leaky)

    # ─────────────────────────────────────────────────────────────────────────
    # BLOCK 2 — PMR: Player Matchup Rating  (PPSI 22%)
    # How favorable is this specific player vs this specific goalie tonight?
    # High score = favorable individual matchup.
    #
    # Sources:
    #   pmr_shooting_pct       → NHL API
    #   pmr_opp_goalie_sv_pct  → NHL API / MoneyPuck (inverted)
    #   pmr_zone_start_pct     → Natural Stat Trick
    #   pmr_opp_goalie_gsax    → Evolving Hockey (inverted: hot goalie = low score)
    #   pmr_shot_location      → MoneyPuck (high-danger shot rate)
    # ─────────────────────────────────────────────────────────────────────────
    pmr_shooting_pct: Optional[float] = None         # player career shooting% (higher = better finisher)
    pmr_opp_goalie_sv_pct: Optional[float] = None    # opposing goalie sv% this season (inverted)
    pmr_zone_start_pct: Optional[float] = None       # offensive zone start% (higher = more chances)
    pmr_opp_goalie_gsax: Optional[float] = None      # opposing goalie goals saved above expected (inverted: hot goalie = low score)
    pmr_shot_location: Optional[float] = None        # player's shot location quality — slot% / high-danger rate

    # ─────────────────────────────────────────────────────────────────────────
    # BLOCK 3 — PER: Player Efficiency Rating  (PPSI 18%)
    # How efficiently does this player generate quality scoring chances?
    # High score = elite chance creator.
    #
    # Sources:
    #   per_shots_pg          → NHL API
    #   per_points_pg         → NHL API
    #   per_primary_pts_pg    → Evolving Hockey (goals + first assists only)
    #   per_ixg_per_60        → Evolving Hockey
    #   per_shooting_talent   → MoneyPuck (career G/xG ratio — true talent above luck)
    # ─────────────────────────────────────────────────────────────────────────
    per_shots_pg: Optional[float] = None             # shots on goal per game this season
    per_points_pg: Optional[float] = None            # points per game rate this season
    per_primary_pts_pg: Optional[float] = None       # primary points/game — goals + first assists only
    per_ixg_per_60: Optional[float] = None           # individual expected goals per 60 minutes
    per_shooting_talent: Optional[float] = None      # career G/xG ratio — shooting talent above raw luck

    # ─────────────────────────────────────────────────────────────────────────
    # BLOCK 4 — POP: Points Operational  (PPSI 14%)
    # How is this player being deployed tonight?
    # High score = heavy deployment with prime linemates.
    #
    # PV1 fires when pop_injury_linemates < 35 — line chemistry disruption is
    # the single biggest scoring risk factor in the model (-2.5 penalty).
    #
    # Sources:
    #   pop_toi_pg             → NHL API
    #   pop_pp_toi_pg          → NHL API
    #   pop_linemate_quality   → NHL API (combined points/game of linemates)
    #   pop_injury_linemates   → official NHL injury report
    # ─────────────────────────────────────────────────────────────────────────
    pop_toi_pg: Optional[float] = None               # average time on ice per game
    pop_pp_toi_pg: Optional[float] = None            # power play time on ice per game
    pop_linemate_quality: Optional[float] = None     # quality of linemates — their combined pts/game
    pop_injury_linemates: Optional[float] = None     # key linemates healthy? (100=all healthy, 0=key linemate out)

    # ─────────────────────────────────────────────────────────────────────────
    # BLOCK 5 — RPS: Referee PP Score  (PPSI 10%)
    # How much will the officials create power play opportunities for this player?
    # High score = high-PP crew + player who draws penalties → more PP points.
    #
    # Sources:
    #   rps_crew_pp_rate     → Warren Sharp / NHL referee historical data
    #   rps_player_draw_rate → NHL API (personal penalty-drawing rate)
    # ─────────────────────────────────────────────────────────────────────────
    rps_crew_pp_rate: Optional[float] = None          # ref crew avg PPs called/game (high = more PP chances)
    rps_player_draw_rate: Optional[float] = None      # this player's personal penalty-drawing tendency

    # ─────────────────────────────────────────────────────────────────────────
    # BLOCK 6 — TLD: Top-Line Deployment  (PPSI 8%)
    # Is this a prime-deployment top-line contributor?
    # High score = elite TOI, first line, first power play unit.
    #
    # Sources:
    #   tld_toi_percentile → NHL API (TOI rank on own team)
    #   tld_line_position  → coaching lineup data (1st line=100, 4th line=0)
    #   tld_pp1_status     → coaching lineup data (PP1=100, PP2/not on PP=0)
    # ─────────────────────────────────────────────────────────────────────────
    tld_toi_percentile: Optional[float] = None        # player TOI rank on their own team (100=top TOI)
    tld_line_position: Optional[float] = None         # line number converted to score (1st=100, 4th=0)
    tld_pp1_status: Optional[float] = None            # on first power play unit? (100=yes, 0=no/PP2)
