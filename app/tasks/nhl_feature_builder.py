"""
nhl_feature_builder.py — NHL feature set builder and pipeline orchestrator.

Migrated from nhl_report.py (the working live playoff report) into the standard
Axiom pipeline architecture.  All 18 norm_* functions are copied exactly — they
are calibrated to real NHL league ranges and must not be changed.

Responsibilities:
  1. Normalize raw NHL API values into 0–100 sub-scores (18 norm_* functions)
  2. Build NHLGoalieFeatureSet from a structured goalie dict + team-level stats
  3. Build NHLSkaterFeatureSet from a structured skater dict + team-level stats
  4. Orchestrate a full scoring run for all games via build_all_feature_sets()

Data flow:
  build_game_contexts()          ← nhl_schedule.py
        ↓
  get_team_stats()               ← nhl_stats.py
  get_roster_from_boxscore()     ← nhl_schedule.py
  get_all_player_stats_for_game()← nhl_stats.py
        ↓
  build_goalie_feature_set()     ← this file
  build_skater_feature_set()     ← this file
        ↓
  compute_gsai() / compute_ppsi()← app/core/nhl/gsai.py, ppsi.py
  train_player_ml()              ← nhl_stats.py

Usage:
  Called by nhl_pipeline.py after game contexts are built.
  Can also be run standalone: python -m app.tasks.nhl_feature_builder
"""

import difflib

from app.core.nhl.features import NHLGameContext, NHLGoalieFeatureSet, NHLSkaterFeatureSet
from app.core.nhl.gsai import compute_gsai
from app.core.nhl.ppsi import compute_ppsi
from app.services.nhl_schedule import (
    build_game_contexts,
    get_roster_from_boxscore,
    get_roster_from_pregame,
)
from app.services.nhl_stats import (
    extract_goalie_season_stats,
    extract_player_season_stats,
    extract_playoff_stats,
    get_all_player_stats_for_game,
    get_team_stats,
    train_player_ml,
)
from app.utils.logging import get_logger

log = get_logger("nhl_feature_builder")

# League-average save% used as the fallback when a goalie has no API data
_FALLBACK_SV_PCT = 0.910
_LEAGUE_AVG_SV   = 0.907


# ──────────────────────────────────────────────────────────────────────────────
# Internal helper
# ──────────────────────────────────────────────────────────────────────────────

def _clamp(v: float) -> float:
    return max(0.0, min(100.0, v))


# ──────────────────────────────────────────────────────────────────────────────
# Normalizer library — migrated exactly from nhl_report.py
# All ranges are calibrated to real NHL league data; do not change them.
# ──────────────────────────────────────────────────────────────────────────────

def norm_linear(value: float, low: float, high: float, direction: str = "normal") -> float:
    """Map value from [low,high] to [0,100]. direction='reverse' flips the scale."""
    if high == low:
        return 50.0
    score = _clamp((value - low) / (high - low) * 100.0)
    return score if direction == "normal" else 100.0 - score


def norm_sv_pct(sv: float) -> float:
    """NHL save% range: .870-.940, avg .910"""
    return norm_linear(sv, 0.870, 0.940, "normal")

def norm_gsax(gsax: float) -> float:
    """GSAx range per season: -15 to +15 for starters, playoff scale shorter"""
    return norm_linear(gsax, -8.0, 8.0, "normal")

def norm_shots_pg_for(shots: float) -> float:
    """Shots on goal FOR per game — high = dangerous. Range 25-38."""
    return norm_linear(shots, 25.0, 38.0, "reverse")  # high shots = bad for goalie under

def norm_shots_pg_against(shots: float) -> float:
    """Shots allowed per game — high = easy opponent. Range 25-38."""
    return norm_linear(shots, 25.0, 38.0, "normal")   # high shots against opp = easier to score

def norm_pp_pct(pp: float) -> float:
    """Power play% range: 0.13-0.30"""
    return norm_linear(pp, 0.13, 0.30, "reverse")  # high PP% opponent = harder for goalie

def norm_hd_rate(hd_rate: float) -> float:
    """High-danger chance rate — high = dangerous opponent. Range 8-18 per game."""
    return norm_linear(hd_rate, 8.0, 18.0, "reverse")

def norm_ga_pg(ga: float) -> float:
    """Goals against per game for skater OSR block. High = easier to score. Range 2.5-4.5."""
    return norm_linear(ga, 2.5, 4.5, "normal")

def norm_pk_pct(pk: float) -> float:
    """PK% range: 0.72-0.90"""
    return norm_linear(pk, 0.72, 0.90, "reverse")  # high PK% = harder to score on PP

def norm_rest_days(days: int) -> float:
    """0 days (B2B) = 20; 1 day = 50; 2 days = 70; 3+ days = 85"""
    if days == 0: return 20.0
    if days == 1: return 50.0
    if days == 2: return 70.0
    return 85.0

def norm_home_ice() -> float:
    return 65.0   # home ice = mild edge (NHL home win rate ~54%)

def norm_series_game(game_num: int) -> float:
    """Later games = more fatigue. G7 = most pressure/fatigue."""
    return norm_linear(game_num, 1, 7, "reverse")

def norm_toi_pg(toi: float) -> float:
    """Average TOI per game. Range 10-28 min."""
    return norm_linear(toi, 10.0, 28.0, "normal")

def norm_pp_toi(pp_toi: float) -> float:
    """PP TOI per game. Range 0-4 min."""
    return norm_linear(pp_toi, 0.0, 4.0, "normal")

def norm_pts_pg(pts: float) -> float:
    """Points per game. Range 0-1.5."""
    return norm_linear(pts, 0.0, 1.5, "normal")

def norm_shots_player_pg(shots: float) -> float:
    """Player shots per game. Range 1-5."""
    return norm_linear(shots, 1.0, 5.0, "normal")

def norm_shooting_pct(sh_pct: float) -> float:
    """Shooting% range 0.05-0.25"""
    return norm_linear(sh_pct, 0.05, 0.25, "normal")

def norm_blocks_pg(blocks: float) -> float:
    """Team shot blocks per game. Range 8-20."""
    return norm_linear(blocks, 8.0, 20.0, "normal")

def norm_cf_pct(cf: float) -> float:
    """Corsi For%. Range 42-58."""
    return norm_linear(cf, 42.0, 58.0, "normal")

def norm_line_position(line: int) -> float:
    """Line 1=100, 2=67, 3=33, 4=0"""
    return max(0.0, (4 - line) * 33.3)

def norm_gsax_opp(gsax: float) -> float:
    """For skater PMR — opposing goalie GSAx. High GSAx = bad for skater."""
    return norm_linear(gsax, -5.0, 10.0, "reverse")


# ──────────────────────────────────────────────────────────────────────────────
# Private helpers
# ──────────────────────────────────────────────────────────────────────────────

def _parse_toi_seconds(toi_str: str) -> int:
    """Parse 'MM:SS' to total seconds. Returns 0 on failure."""
    try:
        parts = str(toi_str).split(":")
        return int(parts[0]) * 60 + int(parts[1])
    except Exception:
        return 0


def _player_name_str(player: dict) -> str:
    """Extract a display name from an enriched player dict."""
    name_obj = player.get("name", {})
    if isinstance(name_obj, dict):
        return name_obj.get("default", f"player_{player.get('playerId', '?')}")
    return str(name_obj) if name_obj else f"player_{player.get('playerId', '?')}"


def _assign_lines(
    fwds: list[dict],
    defs: list[dict],
) -> tuple[list[dict], list[dict]]:
    """
    Sort forwards and defensemen by boxscore TOI and assign line numbers and
    estimated PP units.

    Forwards: sorted by TOI desc; positions 1-3 = line 1, 4-6 = line 2, etc.
    Defense:  sorted by TOI desc; positions 1-2 = line 1, 3-4 = line 2, etc.
    PP unit:  line 1 players get pp_unit=1, all others get pp_unit=2.

    This is an approximation when coaching lineup data is not available.
    """
    fwds_sorted = sorted(fwds, key=lambda p: _parse_toi_seconds(p.get("toi", "0:00")), reverse=True)
    defs_sorted = sorted(defs, key=lambda p: _parse_toi_seconds(p.get("toi", "0:00")), reverse=True)

    for i, p in enumerate(fwds_sorted):
        line = i // 3 + 1
        p["line"]    = min(line, 4)
        p["pp_unit"] = 1 if line == 1 else 2

    for i, p in enumerate(defs_sorted):
        line = i // 2 + 1
        p["line"]    = min(line, 3)
        p["pp_unit"] = 1 if line == 1 else 2

    return fwds_sorted, defs_sorted


def _extract_team_aggregates(club_stats: dict) -> dict:
    """
    Derive team-level aggregate stats from a club-stats/now API response.

    What is directly computable from club-stats/now:
      shots_pg — sum of all skater shots / max games played
      sv_pct   — primary starter save percentage
      ga_pg    — primary starter goals against per game

    What falls back to neutral NHL midpoints (not in the public club-stats API):
      pp_pct    0.215 → midpoint of 0.13–0.30 → normalizes to 50.0
      pk_pct    0.810 → midpoint of 0.72–0.90 → normalizes to 50.0
      blocks_pg 14.0  → midpoint of 8–20       → normalizes to 50.0
      cf_pct    50.0  → midpoint of 42–58       → normalizes to 50.0
      hd_pg     13.0  → midpoint of 8–18        → normalizes to 50.0
    """
    skaters = club_stats.get("skaters", [])
    goalies = club_stats.get("goalies", [])

    total_shots = sum(int(s.get("shots", 0) or 0) for s in skaters)
    max_gp      = max((int(s.get("gamesPlayed", 0) or 0) for s in skaters), default=0)
    shots_pg    = total_shots / max_gp if max_gp > 0 else 29.0

    starters = sorted(goalies, key=lambda g: int(g.get("gamesStarted", 0) or 0), reverse=True)
    if starters:
        g0     = starters[0]
        sv_pct = float(g0.get("savePercentage", _FALLBACK_SV_PCT) or _FALLBACK_SV_PCT)
        gp_g   = int(g0.get("gamesPlayed", 1) or 1)
        ga_pg  = float(g0.get("goalsAgainst", 0) or 0) / gp_g
    else:
        sv_pct = _FALLBACK_SV_PCT
        ga_pg  = 2.90

    return {
        "shots_pg":  round(shots_pg, 1),
        "sv_pct":    sv_pct,
        "ga_pg":     round(ga_pg, 2),
        "pp_pct":    0.215,   # neutral midpoint — not in public NHL API
        "pk_pct":    0.810,   # neutral midpoint — not in public NHL API
        "blocks_pg": 14.0,    # neutral midpoint — not in public NHL API
        "cf_pct":    50.0,    # neutral midpoint — not in public NHL API
        "hd_pg":     13.0,    # neutral midpoint — not in public NHL API
    }


# ──────────────────────────────────────────────────────────────────────────────
# Feature set builders
# ──────────────────────────────────────────────────────────────────────────────

def build_goalie_feature_set(
    goalie_dict: dict,
    game_context: NHLGameContext,
    team_stats: dict,
    opponent_stats: dict,
    is_playoff: bool = True,
) -> NHLGoalieFeatureSet:
    """
    Build a complete NHLGoalieFeatureSet from structured API data.

    Args:
        goalie_dict:    Enriched player dict from get_all_player_stats_for_game().
                        Must carry: playerId, name, stats (landing page), is_home,
                        is_goalie=True, and optionally starter (bool).
        game_context:   NHLGameContext for this game.
        team_stats:     club-stats/now dict for the goalie's own team.
        opponent_stats: club-stats/now dict for the opposing team.
        is_playoff:     Accepted for API consistency; not used internally since
                        GSAI is a deterministic formula (ML discount is applied
                        in build_all_feature_sets via train_player_ml).

    Note on shots-faced base rate:
        avg_shots_faced_per_game = opponent shots-for per game.
        This equals the shots the goalie actually faces and is the correct base
        for the GSAI projection formula — not the goalie's individual season stat.

    Note on gss_data_available:
        Set to False when the goalie has 0 games played in the API (no season data).
        GV7 (-2.0 volatility penalty) fires automatically in compute_gsai().
        Never default to neutral on a goalie with no data.
    """
    player_id   = int(goalie_dict.get("playerId", 0))
    player_name = _player_name_str(goalie_dict)
    is_home     = goalie_dict.get("is_home", True)
    team        = game_context.home_team if is_home else game_context.away_team
    opponent    = game_context.away_team if is_home else game_context.home_team

    landing    = goalie_dict.get("stats") or {}
    reg_stats  = extract_goalie_season_stats(landing)
    po_stats   = extract_playoff_stats(landing)

    opp_agg = _extract_team_aggregates(opponent_stats)
    own_agg = _extract_team_aggregates(team_stats)

    opp_shots_pg  = opp_agg["shots_pg"]    # opponent shots FOR per game = goalie shots faced
    opp_pp_pct    = opp_agg["pp_pct"]
    opp_hd_rate   = opp_agg["hd_pg"]
    own_pk_pct    = own_agg["pk_pct"]
    own_blocks_pg = own_agg["blocks_pg"]
    own_cf_pct    = own_agg["cf_pct"]

    # B2B and rest days
    if is_home:
        on_b2b  = game_context.home_b2b
        rest    = game_context.home_rest_days
        opp_b2b = game_context.away_b2b
        series_score_own = game_context.home_series_wins
        series_score_opp = game_context.away_series_wins
    else:
        on_b2b  = game_context.away_b2b
        rest    = game_context.away_rest_days
        opp_b2b = game_context.home_b2b
        series_score_own = game_context.away_series_wins
        series_score_opp = game_context.home_series_wins

    # ── Save percentage — API first, computed fallback, then league average
    gp         = int(reg_stats.get("gamesPlayed", 0) or 0)
    sv_pct_raw = reg_stats.get("savePctg", None)
    if sv_pct_raw is None:
        sa = int(reg_stats.get("shotsAgainst", 0) or 0)
        ga = int(reg_stats.get("goalsAgainst", 0) or 0)
        sv_pct_raw = (1.0 - ga / sa) if sa > 0 else _FALLBACK_SV_PCT
    sv_pct_raw = float(sv_pct_raw or _FALLBACK_SV_PCT)

    # gss_data_available drives GV7 (-2.0) in the volatility engine
    gss_data_available = (gp > 0)

    # ── Playoff save%
    po_sv_raw = po_stats.get("savePctg", None)
    if po_sv_raw is None:
        po_sa = int(po_stats.get("shotsAgainst", 0) or 0)
        po_ga = int(po_stats.get("goalsAgainst", 0) or 0)
        po_sv_raw = (1.0 - po_ga / po_sa) if po_sa > 0 else sv_pct_raw

    # ── GSAx proxy: (sv% − league_avg) × avg shots faced per game
    avg_shots          = opp_shots_pg
    gsax_per_game_proxy = (sv_pct_raw - _LEAGUE_AVG_SV) * avg_shots

    # ── High-danger sv% estimate (league average HDSV% ≈ .810)
    hd_sv_est = max(0.760, 0.810 + (sv_pct_raw - _LEAGUE_AVG_SV) * 0.8)

    # ── Series momentum (opponent urgency)
    opp_series_lead = series_score_opp - series_score_own
    if opp_series_lead > 0:
        momentum = 35.0   # opponent leads → confident, may generate more shots
    elif opp_series_lead < 0:
        momentum = 45.0   # opponent behind → desperate
    else:
        momentum = 50.0   # tied

    return NHLGoalieFeatureSet(
        player_id=player_id,
        player_name=player_name,
        team=team,
        opponent=opponent,
        is_home=is_home,
        is_confirmed_starter=bool(goalie_dict.get("starter", True)),
        gss_data_available=gss_data_available,
        avg_shots_faced_per_game=avg_shots,
        ctx=game_context,

        # GSS — Goalie Save Suppression
        gss_sv_pct=norm_sv_pct(sv_pct_raw),
        gss_gsax=norm_gsax(gsax_per_game_proxy),
        gss_hd_sv_pct=norm_linear(hd_sv_est, 0.760, 0.870, "normal"),
        gss_playoff_sv_pct=norm_sv_pct(float(po_sv_raw)),
        gss_rebound_control=50.0,               # not in public API
        gss_consistency=55.0 if gp >= 10 else 45.0,

        # OSQ — Opponent Shooting Quality (all inverted — high opponent = low score)
        osq_shots_pg=norm_shots_pg_for(opp_shots_pg),
        osq_shooting_pct=norm_pp_pct(opp_pp_pct),
        osq_pp_pct=norm_pp_pct(opp_pp_pct),
        osq_high_danger_rate=norm_hd_rate(opp_hd_rate),
        osq_series_momentum=momentum,
        osq_xgf_per_60=50.0,                    # not in public API

        # GEN — Game Environment
        gen_is_home=norm_home_ice() if is_home else 100.0 - norm_home_ice(),
        gen_rest_days=norm_rest_days(rest),
        gen_b2b_penalty=20.0 if on_b2b else 70.0,
        gen_series_game=norm_series_game(game_context.series_game_number),
        gen_opponent_b2b=70.0 if opp_b2b else 50.0,

        # TOP — Tactical / Operational
        top_starter_prob=90.0 if goalie_dict.get("starter", True) else 50.0,
        top_pk_pct=norm_pk_pct(own_pk_pct),
        top_coach_defensive=50.0,               # not in public API
        top_injury_status=60.0,                 # not in public API
        top_opponent_pp_rate=norm_pp_pct(opp_pp_pct),

        # RFS — Referee Flow Score
        rfs_crew_pp_rate=50.0,                  # neutral (no public crew data)
        rfs_home_bias=50.0,

        # TSC — Team Structure & Coverage
        tsc_blocks_pg=norm_blocks_pg(own_blocks_pg),
        tsc_cf_pct=norm_cf_pct(own_cf_pct),
        tsc_dzone_exit_pct=50.0,                # not in public API
    )


def build_skater_feature_set(
    skater_dict: dict,
    game_context: NHLGameContext,
    team_stats: dict,
    opponent_stats: dict,
    goalie_sv_pct: float = _FALLBACK_SV_PCT,
    is_playoff: bool = True,
) -> NHLSkaterFeatureSet:
    """
    Build a complete NHLSkaterFeatureSet from structured API data.

    Args:
        skater_dict:    Enriched player dict from get_all_player_stats_for_game().
                        Must carry: playerId, name, position, stats (landing page),
                        edge (EDGE tracking dict), is_home, line, pp_unit.
        game_context:   NHLGameContext for this game.
        team_stats:     club-stats/now dict for the skater's own team.
        opponent_stats: club-stats/now dict for the opposing team.
        goalie_sv_pct:  Opposing goalie save percentage — from _extract_team_aggregates
                        on opponent_stats. Used for pmr_opp_goalie_sv_pct and
                        pmr_opp_goalie_gsax proxy.
        is_playoff:     Accepted for API consistency; GSAI/PPSI are deterministic.
                        ML discount flows via train_player_ml() in the orchestrator.

    EDGE data integration (April 2026):
        NHL EDGE percentile rankings replace normalization guesses for:
          PMR: pmr_zone_start_pct  ← burst22_pct (speed creates O-zone entries)
               pmr_shot_location   ← oz_time_pct (sustained puck possession)
          PER: per_shooting_talent ← EDGE shot-speed percentile + API shooting%
          POP: pop_linemate_quality← max_speed_pct (skating quality of line)
    """
    player_id   = int(skater_dict.get("playerId", 0))
    player_name = _player_name_str(skater_dict)
    is_home     = skater_dict.get("is_home", True)
    position    = skater_dict.get("position", "C")
    line        = int(skater_dict.get("line", 2))
    pp_unit     = int(skater_dict.get("pp_unit", 2))
    team        = game_context.home_team if is_home else game_context.away_team
    opponent    = game_context.away_team if is_home else game_context.home_team

    landing   = skater_dict.get("stats") or {}
    reg_stats = extract_player_season_stats(landing)
    edge      = skater_dict.get("edge") or {}

    gp = int(reg_stats.get("gamesPlayed", 0) or 0)

    # ── API fallbacks for players with no season data (injured, call-up, etc.)
    if gp == 0:
        _pts_defaults = {
            ("C",  1): 0.90, ("C",  2): 0.60, ("C",  3): 0.35, ("C",  4): 0.15,
            ("LW", 1): 0.80, ("LW", 2): 0.55, ("LW", 3): 0.30, ("LW", 4): 0.12,
            ("RW", 1): 0.80, ("RW", 2): 0.55, ("RW", 3): 0.30, ("RW", 4): 0.12,
            ("D",  1): 0.60, ("D",  2): 0.40, ("D",  3): 0.20,
        }
        _sog_defaults = {
            ("C",  1): 3.0, ("C",  2): 2.5, ("C",  3): 2.0, ("C",  4): 1.5,
            ("LW", 1): 3.2, ("LW", 2): 2.6, ("LW", 3): 2.0, ("LW", 4): 1.4,
            ("RW", 1): 3.2, ("RW", 2): 2.6, ("RW", 3): 2.0, ("RW", 4): 1.4,
            ("D",  1): 2.0, ("D",  2): 1.6, ("D",  3): 1.2,
        }
        key      = (position, min(line, 4 if position != "D" else 3))
        pts_pg   = _pts_defaults.get(key, 0.40)
        shots_pg = _sog_defaults.get(key, 2.0)
        goals    = 0
        shots    = int(shots_pg)
    else:
        goals   = int(reg_stats.get("goals", 0) or 0)
        assists = int(reg_stats.get("assists", 0) or 0)
        points  = int(reg_stats.get("points", goals + assists))
        shots   = int(reg_stats.get("shots", 0) or 0) or max(1, gp)
        pts_pg   = points / gp
        shots_pg = shots / gp

    # ── TOI parsing
    toi_total = reg_stats.get("timeOnIce", "0:00") or "0:00"
    toi_pg    = (_parse_toi_seconds(toi_total) / 60.0) / gp if gp > 0 else 18.0

    sh_pct_raw    = (goals / shots) if shots > 0 else 0.10
    pp_toi_est    = 2.5 if pp_unit == 1 else (1.2 if pp_unit == 2 else 0.0)
    primary_pts_pg = pts_pg * 0.65

    # ── EDGE percentiles (already on 0–100 scale from nhl_stats.py)
    burst22_pct  = edge.get("burst22_pct",      50.0)
    avg_shot_pct = edge.get("avg_shot_spd_pct", 50.0)
    oz_time_pct  = edge.get("oz_time_pct",      50.0)
    max_speed_pct = edge.get("max_speed_pct",   50.0)

    # ── Shooting talent: blend traditional shooting% with EDGE shot-speed percentile
    api_talent   = norm_linear(sh_pct_raw, 0.05, 0.20, "normal")
    talent_score = api_talent * 0.60 + avg_shot_pct * 0.40

    # ── Opposing goalie GSAx proxy from sv%
    opp_goalie_gsax_pg = (goalie_sv_pct - _LEAGUE_AVG_SV) * 30.0

    # ── Opponent aggregates for OSR block
    opp_agg = _extract_team_aggregates(opponent_stats)
    opp_ga_pg           = opp_agg["ga_pg"]
    opp_pk_pct          = opp_agg["pk_pct"]
    opp_shots_against_pg = opp_agg["shots_pg"]   # mirrors nhl_report.py run_report() mapping
    opp_hd_chances      = opp_agg["hd_pg"]

    on_b2b = game_context.home_b2b if is_home else game_context.away_b2b

    return NHLSkaterFeatureSet(
        player_id=player_id,
        player_name=player_name,
        team=team,
        opponent=opponent,
        position=position,
        is_home=is_home,
        line_number=line,
        pp_unit=pp_unit,
        avg_points_per_game=pts_pg,
        avg_shots_per_game=shots_pg,
        avg_shooting_pct=sh_pct_raw,
        ctx=game_context,

        # OSR — Opponent Scoring Resistance
        osr_goals_against_pg=norm_ga_pg(opp_ga_pg),
        osr_sv_pct_against=norm_sv_pct(goalie_sv_pct),
        osr_shots_against_pg=norm_shots_pg_against(opp_shots_against_pg),
        osr_pk_pct_against=norm_pk_pct(opp_pk_pct),
        osr_hd_chances_against=norm_hd_rate(opp_hd_chances),
        osr_xga_per_60=50.0,                    # not in public API

        # PMR — Player Matchup Rating
        pmr_shooting_pct=norm_shooting_pct(sh_pct_raw),
        pmr_opp_goalie_sv_pct=norm_sv_pct(goalie_sv_pct),
        pmr_zone_start_pct=burst22_pct,         # EDGE: speed burst percentile
        pmr_opp_goalie_gsax=norm_gsax_opp(opp_goalie_gsax_pg),
        pmr_shot_location=oz_time_pct,          # EDGE: OZ time percentile

        # PER — Player Efficiency Rating
        per_shots_pg=norm_shots_player_pg(shots_pg),
        per_points_pg=norm_pts_pg(pts_pg),
        per_primary_pts_pg=norm_pts_pg(primary_pts_pg * 1.5),
        per_ixg_per_60=50.0,                    # not in public API
        per_shooting_talent=talent_score,       # EDGE-blended

        # POP — Points Operational
        pop_toi_pg=norm_toi_pg(toi_pg),
        pop_pp_toi_pg=norm_pp_toi(pp_toi_est),
        pop_linemate_quality=max_speed_pct,     # EDGE: max speed percentile
        pop_injury_linemates=60.0,              # not in public API

        # RPS — Referee PP Score
        rps_crew_pp_rate=50.0,                  # neutral (no public crew data)
        rps_player_draw_rate=55.0 if position == "C" else 50.0,

        # TLD — Top-Line Deployment
        tld_toi_percentile=norm_toi_pg(toi_pg),
        tld_line_position=norm_line_position(line),
        tld_pp1_status=90.0 if pp_unit == 1 else 10.0,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Signal utility
# ──────────────────────────────────────────────────────────────────────────────

def compute_signal(formula_pts: float, ml_pts: float) -> str:
    """
    Compare the PPSI formula projection against the ML projection.

    ALIGNED  — both engines within 10% of each other
    LEAN     — 10–25% difference (mild divergence, worth noting)
    SPLIT    — >25% difference (strong divergence, investigate before betting)
    """
    if formula_pts <= 0 or ml_pts <= 0:
        return "SPLIT"
    diff = abs(formula_pts - ml_pts) / max(formula_pts, ml_pts)
    if diff <= 0.10:
        return "ALIGNED"
    if diff <= 0.25:
        return "LEAN"
    return "SPLIT"


# ──────────────────────────────────────────────────────────────────────────────
# Orchestrator
# ──────────────────────────────────────────────────────────────────────────────

def build_all_feature_sets(
    game_contexts:   list,
    is_playoff:      bool = True,
    game_lines_data: dict = None,
) -> list[tuple]:
    """
    Score every goalie and skater across a list of game contexts.

    For each game:
      1. Fetch club-stats for both teams.
      2. Pull the boxscore roster (forwards, defense, goalies) for each team.
      3. Assign estimated line numbers and PP units from boxscore TOI.
      4. Enrich the full roster with player landing pages and EDGE data.
      5. Build NHLGoalieFeatureSet → run compute_gsai().
      6. Build NHLSkaterFeatureSet → run compute_ppsi() + train_player_ml().

    Returns:
        List of (player_dict, feature_set, score_result) tuples.
        score_result for skaters includes a "ml" key with ML projections or None.
        Players that fail to score are logged at WARNING and excluded from results.

    Args:
        game_contexts:   List of NHLGameContext objects (from build_game_contexts()).
        is_playoff:      When True, the 12% playoff discount is applied to all ML
                         projections via train_player_ml().
        game_lines_data: Dict of game line data from The Rundown (keyed by event_id).
                         Provides game_total, home_moneyline, away_moneyline per game.
    """
    game_lines_data = game_lines_data or {}
    results:   list[tuple] = []
    n_goalies  = 0
    n_skaters  = 0

    for ctx in game_contexts:
        game_id   = ctx.game_id
        home_team = ctx.home_team
        away_team = ctx.away_team
        b2b_both  = ctx.home_b2b and ctx.away_b2b

        log.info("nhl_feature_builder: scoring game",
                 matchup=f"{away_team}@{home_team}",
                 game_id=game_id, series_game=ctx.series_game_number)

        # ── Match game lines by fuzzy home team name (0.82 threshold) ─────────
        game_gl: dict = {}
        for gl in game_lines_data.values():
            rd_home = (gl.get("home_team_name") or "").lower()
            if rd_home and difflib.SequenceMatcher(None, rd_home, home_team.lower()).ratio() >= 0.82:
                game_gl = gl
                break

        # ── Team stats (club-stats/now) ────────────────────────────────────────
        home_club_stats = get_team_stats(home_team)
        away_club_stats = get_team_stats(away_team)

        # Pre-compute opposing goalie sv_pct for each side
        home_agg = _extract_team_aggregates(home_club_stats)
        away_agg = _extract_team_aggregates(away_club_stats)

        # ── Boxscore rosters (fall back to play-by-play rosterSpots for FUT games) ──
        h_fwds, h_defs, h_gols = get_roster_from_boxscore(int(game_id), home_team)
        a_fwds, a_defs, a_gols = get_roster_from_boxscore(int(game_id), away_team)

        if not (h_fwds or h_defs or h_gols or a_fwds or a_defs or a_gols):
            log.warning(
                "nhl_feature_builder: boxscore empty — fetching rosterSpots from play-by-play",
                game_id=game_id, matchup=f"{away_team}@{home_team}",
            )
            (h_fwds, h_defs, h_gols), (a_fwds, a_defs, a_gols) = \
                get_roster_from_pregame(int(game_id))

        if not (h_fwds or h_defs or h_gols or a_fwds or a_defs or a_gols):
            log.warning(
                "nhl_feature_builder: rosterSpots also empty — skipping game",
                game_id=game_id, matchup=f"{away_team}@{home_team}",
            )
            continue

        # ── Assign lines and tag each player with team and home/away ──────────
        h_fwds, h_defs = _assign_lines(h_fwds, h_defs)
        a_fwds, a_defs = _assign_lines(a_fwds, a_defs)

        for p in h_fwds + h_defs + h_gols:
            p["is_home"]     = True
            p["team_abbrev"] = home_team
        for p in a_fwds + a_defs + a_gols:
            p["is_home"]     = False
            p["team_abbrev"] = away_team

        home_roster = h_fwds + h_defs + h_gols
        away_roster = a_fwds + a_defs + a_gols

        # ── Enrich with stats from NHL API ────────────────────────────────────
        home_enriched = get_all_player_stats_for_game(home_roster, is_playoff=is_playoff)
        away_enriched = get_all_player_stats_for_game(away_roster, is_playoff=is_playoff)

        # ── Score each player ─────────────────────────────────────────────────
        for player in home_enriched + away_enriched:
            p_name    = _player_name_str(player)
            p_id      = int(player.get("playerId", 0))
            p_is_home = player.get("is_home", True)
            is_goalie = player.get("is_goalie", False)

            own_club_stats = home_club_stats if p_is_home else away_club_stats
            opp_club_stats = away_club_stats if p_is_home else home_club_stats
            opp_sv_pct     = away_agg["sv_pct"] if p_is_home else home_agg["sv_pct"]

            if is_goalie:
                try:
                    fs     = build_goalie_feature_set(player, ctx, own_club_stats, opp_club_stats, is_playoff)
                    fs.game_total     = game_gl.get("game_total")
                    fs.home_moneyline = game_gl.get("home_moneyline")
                    fs.away_moneyline = game_gl.get("away_moneyline")
                    score  = compute_gsai(fs, b2b_both=b2b_both)
                    results.append((player, fs, score))
                    n_goalies += 1
                    log.info(
                        "nhl_feature_builder: goalie scored",
                        name=p_name,
                        team=player.get("team_abbrev", "?"),
                        gsai=score["gsai"],
                        grade=score["grade"],
                        proj_shots=score["projected_shots"],
                    )
                except Exception as exc:
                    log.warning("nhl_feature_builder: failed to score goalie",
                                name=p_name, player_id=p_id, error=str(exc))

            else:
                try:
                    fs     = build_skater_feature_set(
                        player, ctx, own_club_stats, opp_club_stats, opp_sv_pct, is_playoff
                    )
                    fs.game_total     = game_gl.get("game_total")
                    fs.home_moneyline = game_gl.get("home_moneyline")
                    fs.away_moneyline = game_gl.get("away_moneyline")
                    score  = compute_ppsi(fs)

                    # ML engine — train per player and attach result
                    ml = train_player_ml(p_id, p_name, p_is_home, is_playoff=is_playoff)
                    score["ml"] = ml

                    results.append((player, fs, score))
                    n_skaters += 1
                    log.info(
                        "nhl_feature_builder: skater scored",
                        name=p_name,
                        team=player.get("team_abbrev", "?"),
                        ppsi=score["ppsi"],
                        grade=score["grade"],
                        proj_pts=score["projected_points"],
                    )
                except Exception as exc:
                    log.warning("nhl_feature_builder: failed to score skater",
                                name=p_name, player_id=p_id, error=str(exc))

    log.info("nhl_feature_builder: run complete",
             total_goalies=n_goalies, total_skaters=n_skaters,
             total_results=len(results))
    return results


# ──────────────────────────────────────────────────────────────────────────────
# Standalone runner — full pipeline test
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\n=== NHL Feature Builder — Live Pipeline Test ===\n")

    print("Fetching today's game contexts...")
    contexts = build_game_contexts()

    if not contexts:
        print("No NHL games found today — cannot run pipeline test.")
    else:
        print(f"Found {len(contexts)} game(s). Running scoring pipeline...\n")

        results = build_all_feature_sets(contexts, is_playoff=True)

        if not results:
            print("No players scored — boxscore may be empty (game not started yet).")
        else:
            goalies = [(p, fs, s) for p, fs, s in results if isinstance(fs, NHLGoalieFeatureSet)]
            skaters = [(p, fs, s) for p, fs, s in results if isinstance(fs, NHLSkaterFeatureSet)]

            print(f"\n{'─' * 70}")
            print(f"  GOALIES — GSAI Summary ({len(goalies)} scored)")
            print(f"{'─' * 70}")
            print(f"  {'GOALIE':<26} {'TEAM':<5} {'GSAI':>6} {'GRADE':>5} "
                  f"{'BASE-SH':>8} {'PROJ-SH':>8}")
            for player, _, score in goalies:
                print(f"  {_player_name_str(player):<26} "
                      f"{player.get('team_abbrev','?'):<5} "
                      f"{score['gsai']:>6.1f} "
                      f"{score['grade']:>5} "
                      f"{score['base_shots']:>8.1f} "
                      f"{score['projected_shots']:>8.1f}")

            print(f"\n{'─' * 70}")
            print(f"  SKATERS — PPSI Summary ({len(skaters)} scored)")
            print(f"{'─' * 70}")
            print(f"  {'PLAYER':<22} {'TEAM':<5} {'POS':<3} {'PPSI':>5} {'GRD':>4} "
                  f"{'PROJ-PTS':>8} {'PROJ-SOG':>9} {'ML-PTS':>7} {'SIGNAL'}")
            for player, _, score in skaters:
                ml      = score.get("ml") or {}
                if ml and ml.get("ml_active"):
                    ml_pts = f"{ml['ml_proj_points']:.2f}"
                    signal = compute_signal(score["projected_points"], ml["ml_proj_points"])
                else:
                    ml_pts = "—"
                    signal = "—"
                print(f"  {_player_name_str(player):<22} "
                      f"{player.get('team_abbrev','?'):<5} "
                      f"{player.get('position','?'):<3} "
                      f"{score['ppsi']:>5.1f} "
                      f"{score['grade']:>4} "
                      f"{score['projected_points']:>8.2f} "
                      f"{score['projected_sog']:>9.1f} "
                      f"{ml_pts:>7} "
                      f"{signal}")

    print("\nnhl_feature_builder pipeline test complete.")
