"""
NHL Live Report — GSAUI + PPUI
Fetches live data from the public NHL API and runs the scoring engines
for today's playoff games.

Usage:
    python nhl_report.py

Completely standalone — does NOT touch the production database or
the existing baseball pipeline. Read-only against the public NHL API.
"""

import json
import sys
import time
import urllib.request
import urllib.error
from datetime import date, timedelta
from app.core.nhl.features import (
    NHLGameContext, NHLGoalieFeatureSet, NHLSkaterFeatureSet
)
from app.core.nhl.gsaui import compute_gsaui
from app.core.nhl.ppui import compute_ppui
from app.core.nhl.ml_engine import (
    NHLPlayerMLEngine, parse_game_log, compute_signal
)

# ── Terminal colors
GREEN   = "\033[92m"
YELLOW  = "\033[93m"
RED     = "\033[91m"
CYAN    = "\033[96m"
MAGENTA = "\033[95m"
BOLD    = "\033[1m"
DIM     = "\033[2m"
RESET   = "\033[0m"

NHL_API  = "https://api-web.nhle.com/v1"
EDGE_API = "https://api-web.nhle.com/v1/edge"

# Regular season only — EDGE percentiles are built from the full regular-season pool
EDGE_SEASON = "20252026"
EDGE_GT     = "2"          # gameTypeId 2 = regular season

TODAY = str(date.today())   # 2026-04-27


# ──────────────────────────────────────────────────────────────────────────────
# HTTP helper
# ──────────────────────────────────────────────────────────────────────────────

def fetch(url: str, timeout: int = 10) -> dict | list | None:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "AxiomNHL/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        print(f"{YELLOW}  [warn] Could not fetch {url}: {e}{RESET}")
        return None


# ──────────────────────────────────────────────────────────────────────────────
# EDGE API — per-player tracking data with built-in league percentile rankings
# The API returns percentile as a 0.0-1.0 float.  Multiply × 100 for our scale.
# ──────────────────────────────────────────────────────────────────────────────

def get_skater_edge_data(player_id: int) -> dict:
    """
    Fetch three EDGE endpoints for a skater and return a flat dict of the
    key percentile scores (already on a 0-100 scale, ready for the engine).

    Keys returned:
        max_speed_pct     — max skating speed percentile vs full league
        burst22_pct       — bursts above 22 mph percentile (explosiveness)
        avg_shot_spd_pct  — average shot speed percentile (shot power)
        top_shot_spd_pct  — top single shot speed percentile
        oz_time_pct       — even-strength offensive zone time percentage percentile
        oz_raw_pct        — raw % of ES time spent in offensive zone (0-100)
    """
    spd_url  = f"{EDGE_API}/skater-skating-speed-detail/{player_id}/{EDGE_SEASON}/{EDGE_GT}"
    shot_url = f"{EDGE_API}/skater-shot-speed-detail/{player_id}/{EDGE_SEASON}/{EDGE_GT}"
    zone_url = f"{EDGE_API}/skater-zone-time/{player_id}/{EDGE_SEASON}/{EDGE_GT}"

    spd_data  = fetch(spd_url)  or {}
    time.sleep(0.12)
    shot_data = fetch(shot_url) or {}
    time.sleep(0.12)
    zone_data = fetch(zone_url) or {}

    spd  = spd_data.get("skatingSpeedDetails",  {})
    shot = shot_data.get("shotSpeedDetails",     {})

    max_spd_pct     = spd.get("maxSkatingSpeed", {}).get("percentile", 0.5) * 100.0
    burst22_pct     = spd.get("burstsOver22",    {}).get("percentile", 0.5) * 100.0
    avg_shot_pct    = shot.get("avgShotSpeed",   {}).get("percentile", 0.5) * 100.0
    top_shot_pct    = shot.get("topShotSpeed",   {}).get("percentile", 0.5) * 100.0

    oz_time_pct = 50.0
    oz_raw      = 40.0
    for z in zone_data.get("zoneTimeDetails", []):
        if z.get("strengthCode") == "es":
            oz_time_pct = z.get("offensiveZonePercentile", 0.5) * 100.0
            oz_raw      = z.get("offensiveZonePctg",       0.40) * 100.0
            break

    return {
        "max_speed_pct":    max_spd_pct,
        "burst22_pct":      burst22_pct,
        "avg_shot_spd_pct": avg_shot_pct,
        "top_shot_spd_pct": top_shot_pct,
        "oz_time_pct":      oz_time_pct,
        "oz_raw_pct":       oz_raw,
    }


# ──────────────────────────────────────────────────────────────────────────────
# Normalization helpers — map raw NHL stats to 0-100 scores
# Convention: 50 = league average/neutral, >50 = favors UNDER
# ──────────────────────────────────────────────────────────────────────────────

def _clamp(v: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, v))


def norm_linear(value: float, low: float, high: float, direction: str = "normal") -> float:
    """Map value from [low,high] to [0,100]. direction='reverse' flips the scale."""
    if high == low:
        return 50.0
    score = _clamp((value - low) / (high - low) * 100.0)
    return score if direction == "normal" else 100.0 - score


# ── Specific normalizers using documented NHL league ranges

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
    """0 days (B2B) = 0; 1 day = 50; 2+ days = 75+"""
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
# NHL API data fetchers
# ──────────────────────────────────────────────────────────────────────────────

def get_today_schedule() -> list[dict]:
    data = fetch(f"{NHL_API}/schedule/{TODAY}")
    if not data:
        return []
    games = []
    for gw in data.get("gameWeek", []):
        if gw.get("date") == TODAY:
            for g in gw.get("games", []):
                games.append(g)
    return games


def get_team_stats(team_abbrev: str) -> dict:
    """Returns current season team stats dict."""
    data = fetch(f"{NHL_API}/club-stats/{team_abbrev}/now")
    if not data:
        return {}
    return data


def get_goalie_stats(player_id: int) -> dict:
    data = fetch(f"{NHL_API}/player/{player_id}/landing")
    if not data:
        return {}
    return data


def get_player_stats(player_id: int) -> dict:
    data = fetch(f"{NHL_API}/player/{player_id}/landing")
    if not data:
        return {}
    return data


def get_game_landing(game_id: int) -> dict:
    data = fetch(f"{NHL_API}/gamecenter/{game_id}/landing")
    if not data:
        return {}
    return data


def get_game_boxscore(game_id: int) -> dict:
    data = fetch(f"{NHL_API}/gamecenter/{game_id}/boxscore")
    if not data:
        return {}
    return data


def get_player_game_log(player_id: int, season: str = "20252026", game_type: int = 2) -> list[dict]:
    """
    Fetch a player's full regular season game log from the NHL API.
    Returns a list of raw game log dicts in chronological order.
    """
    data = fetch(f"{NHL_API}/player/{player_id}/game-log/{season}/{game_type}")
    if not data:
        return []
    return list(reversed(data.get("gameLog", [])))  # API returns newest-first; reverse to chrono


def train_player_ml(player_id: int, player_name: str, is_home: bool) -> dict | None:
    """
    Fetch this player's full 2025-26 regular season game log, train a
    per-player GradientBoosting model on all four betting markets, and
    return ML projections for tonight.

    Returns None if the player has insufficient data (< 20 games).
    """
    raw_log = get_player_game_log(player_id)
    if not raw_log:
        return None

    game_log = parse_game_log(raw_log)

    engine = NHLPlayerMLEngine(player_name)
    trained = engine.train(game_log)
    if not trained:
        return {"ml_active": False, "n_samples": len(game_log)}

    return engine.predict(game_log, is_home=is_home)


# ──────────────────────────────────────────────────────────────────────────────
# Extract season stats from the NHL API landing page
# ──────────────────────────────────────────────────────────────────────────────

def extract_player_season_stats(landing: dict, season_id: str = "20252026") -> dict:
    """Pull current season stats from a player landing page."""
    out = {}

    # Priority 1: seasonTotals (most reliable)
    best_gp = 0
    for row in landing.get("seasonTotals", []):
        if str(row.get("season", "")) == season_id and row.get("gameTypeId") == 2:
            if isinstance(row, dict) and row.get("gamesPlayed", 0) > best_gp:
                out = dict(row)
                best_gp = out.get("gamesPlayed", 0)

    if out:
        return out

    # Priority 2: featuredStats.regularSeason.subSeason (flat dict)
    try:
        sub = (landing.get("featuredStats", {})
                      .get("regularSeason", {})
                      .get("subSeason", {}))
        if isinstance(sub, dict) and "gamesPlayed" in sub:
            return dict(sub)
    except Exception:
        pass

    return out


def extract_goalie_season_stats(landing: dict, season_id: str = "20252026") -> dict:
    out = {}
    best_gp = 0
    for row in landing.get("seasonTotals", []):
        if str(row.get("season", "")) == season_id and row.get("gameTypeId") == 2:
            if isinstance(row, dict) and row.get("gamesPlayed", 0) > best_gp:
                out = dict(row)
                best_gp = out.get("gamesPlayed", 0)
    if out:
        return out
    # Fallback: featuredStats
    try:
        sub = (landing.get("featuredStats", {})
                      .get("regularSeason", {})
                      .get("subSeason", {}))
        if isinstance(sub, dict) and "gamesPlayed" in sub:
            return dict(sub)
    except Exception:
        pass
    return out


def extract_playoff_stats(landing: dict) -> dict:
    """Extract most recent playoff season stats."""
    out = {}
    best_gp = 0
    for row in landing.get("seasonTotals", []):
        if row.get("gameTypeId") == 3:
            if isinstance(row, dict) and row.get("gamesPlayed", 0) >= best_gp:
                out = dict(row)
                best_gp = out.get("gamesPlayed", 0)
    return out


# ──────────────────────────────────────────────────────────────────────────────
# Build feature sets from live data
# ──────────────────────────────────────────────────────────────────────────────

def build_goalie_feature_set(
    player_id: int,
    player_name: str,
    team: str,
    is_home: bool,
    ctx: NHLGameContext,
    opp_team_abbrev: str,
    opp_shots_pg: float,       # opponent shots ON GOAL per game = goalie's shots faced per game
    opp_pp_pct: float,
    opp_hd_rate: float,
    own_pk_pct: float,
    own_blocks_pg: float,
    own_cf_pct: float,
    series_score_opp: int,
    series_score_own: int,
    fallback_sv_pct: float = 0.910,
) -> NHLGoalieFeatureSet:
    """
    Build a complete NHLGoalieFeatureSet for a starting goalie.

    Key clarification from expert sources (Evolving Hockey, LivingHockey):
      - Shots on Goal Against (SA) = saves + goals against. These are the ONLY
        shots a goalie actually faces. Missed shots (post, wide) and blocked
        shots (Corsi/Fenwick) never reach the goalie and are NOT counted here.
      - Therefore: avg_shots_faced_per_game = opp_shots_pg
        (the opponent team's shots on goal FOR per game — same number,
        viewed from the goalie's side as shots AGAINST per game).
      - The goalie's own API landing page has sv%, GAA, GSAx. It does NOT
        supply the shots-faced number in a useful way for this purpose.
    """
    # avg shots this goalie will face = opponent's shots on goal per game
    # This is the same stat whether you call it "shots for PHI" or "shots against PIT goalie"
    avg_shots = opp_shots_pg

    # Fetch goalie's own performance stats from the API
    landing = get_goalie_stats(player_id) or {}
    reg_stats = extract_goalie_season_stats(landing)
    po_stats  = extract_playoff_stats(landing)

    # Save percentage — API first, then game config fallback
    # SA = saves + goals against (Evolving Hockey definition)
    # Sv% = 1 - (GA / SA)
    gp = int(reg_stats.get("gamesPlayed", 0) or 0)
    sv_pct_raw = reg_stats.get("savePctg", None)
    if sv_pct_raw is None:
        sa = int(reg_stats.get("shotsAgainst", 0) or 0)   # SA = saves + GA
        ga = int(reg_stats.get("goalsAgainst", 0) or 0)
        sv_pct_raw = (1.0 - ga / sa) if sa > 0 else fallback_sv_pct
    sv_pct_raw = float(sv_pct_raw or fallback_sv_pct)

    # Playoff save%
    po_sv_raw = po_stats.get("savePctg", None)
    if po_sv_raw is None:
        po_sa = int(po_stats.get("shotsAgainst", 0) or 0)
        po_ga = int(po_stats.get("goalsAgainst", 0) or 0)
        po_sv_raw = (1.0 - po_ga / po_sa) if po_sa > 0 else sv_pct_raw

    # GSAx proxy (per game): how many goals above league-average this goalie saves per game
    # League average SV% = .907 (LivingHockey / Evolving Hockey). Any goalie above saves
    # more goals than average per game faced.
    # Formula: gsax_per_game = (sv_pct - league_avg) * avg_shots
    league_avg_sv = 0.907
    gsax_per_game_proxy = (sv_pct_raw - league_avg_sv) * avg_shots

    # High-danger save% proxy: not in public NHL API directly.
    # Expert sources: league average HDSV% ≈ .810 (LivingHockey)
    # We estimate relative to league average: if overall sv% is above average,
    # assume HD sv% tracks similarly above .810.
    hd_sv_est = max(0.760, 0.810 + (sv_pct_raw - league_avg_sv) * 0.8)

    # B2B / rest
    if is_home:
        on_b2b = ctx.home_b2b
        rest = ctx.home_rest_days
        opp_b2b = ctx.away_b2b
    else:
        on_b2b = ctx.away_b2b
        rest = ctx.away_rest_days
        opp_b2b = ctx.home_b2b

    # Series momentum (opponent urgency = fewer shots when desperate or more)
    # If opp is down in series → more desperate → more shots (bad for under)
    # If opp is up → may play safer → fewer shots (good for under)
    opp_series_lead = series_score_opp - series_score_own
    if opp_series_lead > 0:
        momentum = 35.0   # opponent leads → opponent confident, may generate more shots
    elif opp_series_lead < 0:
        momentum = 45.0   # opponent behind → desperate
    else:
        momentum = 50.0   # tied

    f = NHLGoalieFeatureSet(
        player_id=player_id,
        player_name=player_name,
        team=team,
        is_home=is_home,
        is_confirmed_starter=True,
        avg_shots_faced_per_game=avg_shots,
        ctx=ctx,

        # OSQ — Opponent Shooting Quality
        osq_shots_pg=norm_shots_pg_for(opp_shots_pg),
        osq_shooting_pct=norm_pp_pct(opp_pp_pct) if opp_pp_pct > 0 else 50.0,
        osq_pp_pct=norm_pp_pct(opp_pp_pct),
        osq_high_danger_rate=norm_hd_rate(opp_hd_rate),
        osq_series_momentum=momentum,
        osq_xgf_per_60=50.0,  # no public xGF from NHL API — neutral

        # GSS — Goalie Save Suppression
        # sv%: normalized against .870-.940 range (Evolving Hockey / LivingHockey)
        # hd_sv%: high-danger save%, league avg .810 (LivingHockey), range .760-.870
        gss_sv_pct=norm_sv_pct(sv_pct_raw),
        gss_gsax=norm_gsax(gsax_per_game_proxy),
        gss_hd_sv_pct=norm_linear(hd_sv_est, 0.760, 0.870, "normal"),
        gss_playoff_sv_pct=norm_sv_pct(float(po_sv_raw)),
        gss_rebound_control=50.0,   # not in public API
        gss_consistency=55.0 if gp >= 10 else 45.0,

        # GEN — Game Environment
        gen_is_home=norm_home_ice() if is_home else 100.0 - norm_home_ice(),
        gen_rest_days=norm_rest_days(rest),
        gen_b2b_penalty=20.0 if on_b2b else 70.0,
        gen_series_game=norm_series_game(ctx.series_game_number),
        gen_opponent_b2b=70.0 if opp_b2b else 50.0,

        # TOP — Tactical / Operational
        top_starter_prob=90.0,
        top_pk_pct=norm_pk_pct(own_pk_pct),
        top_coach_defensive=50.0,
        top_injury_status=60.0,
        top_opponent_pp_rate=norm_pp_pct(opp_pp_pct),

        # RFS — Referee Flow Score
        rfs_crew_pp_rate=50.0,  # neutral (public crew data not available)
        rfs_home_bias=50.0,

        # TSC — Team Structure & Coverage
        tsc_blocks_pg=norm_blocks_pg(own_blocks_pg),
        tsc_cf_pct=norm_cf_pct(own_cf_pct),
        tsc_dzone_exit_pct=50.0,
    )
    return f


def build_skater_feature_set(
    player_id: int,
    player_name: str,
    team: str,
    position: str,
    is_home: bool,
    line: int,
    pp_unit: int,
    ctx: NHLGameContext,
    # Opponent team context
    opp_ga_pg: float,
    opp_sv_pct: float,
    opp_shots_against_pg: float,
    opp_pk_pct: float,
    opp_hd_chances: float,
    opp_goalie_gsax_pg: float,
    # Optional EDGE tracking data (pre-fetched; if None, live API call is made)
    edge: dict | None = None,
) -> NHLSkaterFeatureSet:
    """
    Fetch skater stats and build a complete NHLSkaterFeatureSet.

    EDGE data upgrade (April 2026):
      When available, the NHL EDGE API percentile rankings replace manual
      normalization guesses for three sub-blocks:
        · PMR: pmr_zone_start_pct  ← speed burst percentile (zone-entry creation)
               pmr_shot_location   ← offensive-zone time percentile (puck possession)
        · PER: per_shooting_talent ← blended API shooting% + EDGE shot-speed percentile
        · POP: pop_linemate_quality← max skating speed percentile (line talent proxy)
      This gives us league-calibrated scores with no manual range assumptions.
    """
    landing = get_player_stats(player_id)
    reg_stats = extract_player_season_stats(landing)

    gp = int(reg_stats.get("gamesPlayed", 0) or 0)

    # ── API fallbacks: if the API timed out or returned no games, use
    #    position/line-based defaults so we never show 0.00 for a real player.
    #    These are conservative NHL averages (not star numbers, not replacement level).
    if gp == 0:
        # Points per game defaults by position and line depth
        _pts_defaults = {
            ("C", 1): 0.90, ("C", 2): 0.60, ("C", 3): 0.35, ("C", 4): 0.15,
            ("LW",1): 0.80, ("LW",2): 0.55, ("LW",3): 0.30, ("LW",4): 0.12,
            ("RW",1): 0.80, ("RW",2): 0.55, ("RW",3): 0.30, ("RW",4): 0.12,
            ("D", 1): 0.60, ("D", 2): 0.40, ("D", 3): 0.20,
        }
        _sog_defaults = {
            ("C", 1): 3.0, ("C", 2): 2.5, ("C", 3): 2.0, ("C", 4): 1.5,
            ("LW",1): 3.2, ("LW",2): 2.6, ("LW",3): 2.0, ("LW",4): 1.4,
            ("RW",1): 3.2, ("RW",2): 2.6, ("RW",3): 2.0, ("RW",4): 1.4,
            ("D", 1): 2.0, ("D", 2): 1.6, ("D", 3): 1.2,
        }
        key = (position, min(line, 4 if position != "D" else 3))
        pts_pg   = _pts_defaults.get(key, 0.40)
        shots_pg = _sog_defaults.get(key, 2.0)
        goals    = 0
        assists  = 0
        shots    = int(shots_pg)
    else:
        goals   = int(reg_stats.get("goals", 0) or 0)
        assists = int(reg_stats.get("assists", 0) or 0)
        points  = int(reg_stats.get("points", goals + assists))
        shots   = int(reg_stats.get("shots", 0) or 0) or max(1, gp)
        pts_pg   = points / gp
        shots_pg = shots / gp

    toi_total = reg_stats.get("timeOnIce", "0:00") or "0:00"

    def parse_toi(toi_str):
        try:
            parts = str(toi_str).split(":")
            return int(parts[0]) + int(parts[1]) / 60.0
        except:
            return 0.0

    toi_pg     = parse_toi(toi_total) / gp if gp > 0 else 18.0
    sh_pct_raw  = (goals / shots) if shots > 0 else 0.10
    pp_toi_est  = 2.5 if pp_unit == 1 else (1.2 if pp_unit == 2 else 0.0)
    primary_pts_pg = pts_pg * 0.65

    # ── EDGE data — fetch if not pre-supplied
    if edge is None:
        edge = get_skater_edge_data(player_id)

    burst22_pct     = edge.get("burst22_pct",      50.0)
    avg_shot_pct    = edge.get("avg_shot_spd_pct", 50.0)
    oz_time_pct     = edge.get("oz_time_pct",      50.0)
    max_speed_pct   = edge.get("max_speed_pct",    50.0)

    # ── Shooting talent: blend traditional shooting% with EDGE shot-speed percentile
    # Traditional: how often a shot becomes a goal (accuracy / positioning)
    # EDGE shot speed: raw power — harder shots are harder to save
    api_talent  = norm_linear(sh_pct_raw, 0.05, 0.20, "normal")
    talent_score = api_talent * 0.60 + avg_shot_pct * 0.40

    on_b2b = ctx.home_b2b if is_home else ctx.away_b2b

    f = NHLSkaterFeatureSet(
        player_id=player_id,
        player_name=player_name,
        team=team,
        position=position,
        is_home=is_home,
        line_number=line,
        pp_unit=pp_unit,
        avg_points_per_game=pts_pg,
        avg_shots_per_game=shots_pg,
        avg_shooting_pct=sh_pct_raw,
        ctx=ctx,

        # OSR — Opponent Scoring Resistance
        osr_goals_against_pg=norm_ga_pg(opp_ga_pg),
        osr_sv_pct_against=norm_sv_pct(opp_sv_pct),
        osr_shots_against_pg=norm_shots_pg_against(opp_shots_against_pg),
        osr_pk_pct_against=norm_pk_pct(opp_pk_pct),
        osr_hd_chances_against=norm_hd_rate(opp_hd_chances),
        osr_xga_per_60=50.0,

        # PMR — Player Matchup Rating
        # burst22_pct: speed burst = ability to blow past defenders and generate O-zone entries
        # oz_time_pct: OZ time percentile = sustained puck possession in scoring areas
        pmr_shooting_pct=norm_shooting_pct(sh_pct_raw),
        pmr_opp_goalie_sv_pct=norm_sv_pct(opp_sv_pct),
        pmr_zone_start_pct=burst22_pct,            # EDGE: speed burst percentile
        pmr_opp_goalie_gsax=norm_gsax_opp(opp_goalie_gsax_pg),
        pmr_shot_location=oz_time_pct,             # EDGE: OZ time percentile

        # PER — Player Efficiency Rating
        # per_shooting_talent: EDGE-blended shot power + API accuracy
        per_shots_pg=norm_shots_player_pg(shots_pg),
        per_points_pg=norm_pts_pg(pts_pg),
        per_primary_pts_pg=norm_pts_pg(primary_pts_pg * 1.5),
        per_ixg_per_60=50.0,
        per_shooting_talent=talent_score,          # EDGE-blended

        # POP — Points Operational
        # pop_linemate_quality: max speed percentile = skating quality of line
        pop_toi_pg=norm_toi_pg(toi_pg),
        pop_pp_toi_pg=norm_pp_toi(pp_toi_est),
        pop_linemate_quality=max_speed_pct,        # EDGE: max speed percentile
        pop_injury_linemates=60.0,

        # RPS — Referee PP Score
        rps_crew_pp_rate=50.0,
        rps_player_draw_rate=55.0 if position == "C" else 50.0,

        # TLD — Top-Line Deployment
        tld_toi_percentile=norm_toi_pg(toi_pg),
        tld_line_position=norm_line_position(line),
        tld_pp1_status=90.0 if pp_unit == 1 else 10.0,
    )
    return f


# ──────────────────────────────────────────────────────────────────────────────
# Roster + team context pulled from schedule / boxscore
# ──────────────────────────────────────────────────────────────────────────────

def get_roster_from_boxscore(game_id: int, team_abbrev: str) -> list[dict]:
    """Pull skater roster entries from the boxscore for a given team."""
    bs = get_game_boxscore(game_id)
    for side in ("homeTeam", "awayTeam"):
        team_data = bs.get(side, {})
        abbrev = team_data.get("abbrev", "")
        if abbrev == team_abbrev:
            forwards = team_data.get("forwards", [])
            defense = team_data.get("defense", [])
            goalies = team_data.get("goalies", [])
            return forwards, defense, goalies
    return [], [], []


# ──────────────────────────────────────────────────────────────────────────────
# Hardcoded playoff context for April 27, 2026
# (Series results known — PHI leads 3-1; UTAH leads 2-1)
# ──────────────────────────────────────────────────────────────────────────────

GAMES_TODAY = [
    # ── Rosters verified via NHL API /roster/{team}/current on Apr 28 2026
    {
        "label": "GAME 5 — Boston Bruins @ Buffalo Sabres",
        "home": "BUF",
        "away": "BOS",
        "series_home_wins": 3,
        "series_away_wins": 1,
        "series_game": 5,
        "time_et": "7:30 PM ET",
        "note": "BUF leads series 3-1. BOS must win to stay alive.",
        "home_b2b": False,
        "away_b2b": False,
        "home_rest": 2,
        "away_rest": 2,
        "home_shots_pg": 32.1,
        "away_shots_pg": 29.8,
        "home_ga_pg": 2.75,
        "away_ga_pg": 3.10,
        "home_sv_pct": 0.918,
        "away_sv_pct": 0.906,
        "home_pp_pct": 0.231,
        "away_pp_pct": 0.198,
        "home_pk_pct": 0.824,
        "away_pk_pct": 0.808,
        "home_blocks_pg": 11.2,
        "away_blocks_pg": 12.8,
        "home_cf_pct": 52.4,
        "away_cf_pct": 48.6,
        "home_hd_pg": 13.2,
        "away_hd_pg": 11.8,
        "home_goalie": {"id": 8480045, "name": "UPL Luukkonen",   "gsax_pg":  0.14, "sv_pct": 0.918},
        "away_goalie": {"id": 8480280, "name": "Jeremy Swayman",  "gsax_pg":  0.08, "sv_pct": 0.906},
        "home_players": [
            {"id": 8479420, "name": "Tage Thompson",    "pos": "C",  "line": 1, "pp": 1},
            {"id": 8484145, "name": "Zach Benson",      "pos": "LW", "line": 1, "pp": 1},
            {"id": 8482097, "name": "Jack Quinn",       "pos": "RW", "line": 1, "pp": 1},
            {"id": 8480064, "name": "Josh Norris",      "pos": "C",  "line": 2, "pp": 1},
            {"id": 8483468, "name": "Jiri Kulich",      "pos": "C",  "line": 2, "pp": 2},
            {"id": 8477949, "name": "Alex Tuch",        "pos": "RW", "line": 2, "pp": 2},
            {"id": 8480839, "name": "Rasmus Dahlin",    "pos": "D",  "line": 1, "pp": 1},
            {"id": 8482671, "name": "Owen Power",       "pos": "D",  "line": 1, "pp": 2},
            {"id": 8481524, "name": "Bowen Byram",      "pos": "D",  "line": 2, "pp": 2},
        ],
        "away_players": [
            {"id": 8477956, "name": "David Pastrnak",   "pos": "RW", "line": 1, "pp": 1},
            {"id": 8477496, "name": "Elias Lindholm",   "pos": "C",  "line": 1, "pp": 1},
            {"id": 8479999, "name": "Casey Mittelstadt","pos": "C",  "line": 2, "pp": 1},
            {"id": 8478401, "name": "Pavel Zacha",      "pos": "C",  "line": 2, "pp": 2},
            {"id": 8478042, "name": "Viktor Arvidsson", "pos": "LW", "line": 2, "pp": 2},
            {"id": 8479325, "name": "Charlie McAvoy",   "pos": "D",  "line": 1, "pp": 1},
            {"id": 8476854, "name": "Hampus Lindholm",  "pos": "D",  "line": 1, "pp": 2},
        ],
    },
    {
        "label": "GAME 5 — Minnesota Wild @ Dallas Stars",
        "home": "DAL",
        "away": "MIN",
        "series_home_wins": 2,
        "series_away_wins": 2,
        "series_game": 5,
        "time_et": "8:00 PM ET",
        "note": "Series tied 2-2. Winner takes series lead.",
        "home_b2b": False,
        "away_b2b": False,
        "home_rest": 2,
        "away_rest": 2,
        "home_shots_pg": 30.8,
        "away_shots_pg": 29.5,
        "home_ga_pg": 2.90,
        "away_ga_pg": 3.05,
        "home_sv_pct": 0.912,
        "away_sv_pct": 0.910,
        "home_pp_pct": 0.225,
        "away_pp_pct": 0.207,
        "home_pk_pct": 0.819,
        "away_pk_pct": 0.812,
        "home_blocks_pg": 13.1,
        "away_blocks_pg": 12.4,
        "home_cf_pct": 51.8,
        "away_cf_pct": 50.2,
        "home_hd_pg": 12.5,
        "away_hd_pg": 11.9,
        "home_goalie": {"id": 8479979, "name": "Jake Oettinger",    "gsax_pg":  0.12, "sv_pct": 0.912},
        "away_goalie": {"id": 8479406, "name": "Filip Gustavsson",  "gsax_pg":  0.09, "sv_pct": 0.910},
        "home_players": [
            {"id": 8478420, "name": "Mikko Rantanen",   "pos": "RW", "line": 1, "pp": 1},
            {"id": 8480027, "name": "Jason Robertson",  "pos": "LW", "line": 1, "pp": 1},
            {"id": 8482740, "name": "Wyatt Johnston",   "pos": "C",  "line": 1, "pp": 1},
            {"id": 8478449, "name": "Roope Hintz",      "pos": "C",  "line": 2, "pp": 1},
            {"id": 8475168, "name": "Matt Duchene",     "pos": "C",  "line": 2, "pp": 2},
            {"id": 8475794, "name": "Tyler Seguin",     "pos": "C",  "line": 3, "pp": 2},
            {"id": 8480036, "name": "Miro Heiskanen",   "pos": "D",  "line": 1, "pp": 1},
            {"id": 8481581, "name": "Thomas Harley",    "pos": "D",  "line": 1, "pp": 2},
        ],
        "away_players": [
            {"id": 8478864, "name": "Kirill Kaprizov",  "pos": "LW", "line": 1, "pp": 1},
            {"id": 8481557, "name": "Matt Boldy",       "pos": "LW", "line": 1, "pp": 1},
            {"id": 8478493, "name": "Joel Eriksson Ek", "pos": "C",  "line": 1, "pp": 1},
            {"id": 8475692, "name": "Mats Zuccarello",  "pos": "RW", "line": 2, "pp": 1},
            {"id": 8475765, "name": "Vlad Tarasenko",   "pos": "RW", "line": 2, "pp": 2},
            {"id": 8483452, "name": "Hunter Haight",    "pos": "C",  "line": 2, "pp": 2},
            {"id": 8480800, "name": "Quinn Hughes",     "pos": "D",  "line": 1, "pp": 1},
            {"id": 8482122, "name": "Brock Faber",      "pos": "D",  "line": 1, "pp": 2},
        ],
    },
    {
        "label": "GAME 5 — Anaheim Ducks @ Edmonton Oilers",
        "home": "EDM",
        "away": "ANA",
        "series_home_wins": 1,
        "series_away_wins": 3,
        "series_game": 5,
        "time_et": "10:00 PM ET",
        "note": "ANA leads series 3-1. Oilers must win to stay alive.",
        "home_b2b": False,
        "away_b2b": False,
        "home_rest": 2,
        "away_rest": 2,
        "home_shots_pg": 33.2,
        "away_shots_pg": 28.6,
        "home_ga_pg": 2.95,
        "away_ga_pg": 3.20,
        "home_sv_pct": 0.908,
        "away_sv_pct": 0.904,
        "home_pp_pct": 0.248,
        "away_pp_pct": 0.195,
        "home_pk_pct": 0.826,
        "away_pk_pct": 0.811,
        "home_blocks_pg": 10.9,
        "away_blocks_pg": 12.2,
        "home_cf_pct": 53.6,
        "away_cf_pct": 49.8,
        "home_hd_pg": 14.1,
        "away_hd_pg": 11.4,
        "home_goalie": {"id": 8478971, "name": "Connor Ingram",    "gsax_pg":  0.05, "sv_pct": 0.908},
        "away_goalie": {"id": 8480843, "name": "Lukas Dostal",     "gsax_pg":  0.03, "sv_pct": 0.904},
        "home_players": [
            {"id": 8478402, "name": "Connor McDavid",   "pos": "C",  "line": 1, "pp": 1},
            {"id": 8477934, "name": "Leon Draisaitl",   "pos": "C",  "line": 2, "pp": 1},
            {"id": 8475786, "name": "Zach Hyman",       "pos": "LW", "line": 1, "pp": 1},
            {"id": 8476454, "name": "Ryan Nugent-Hopkins","pos":"C",  "line": 3, "pp": 2},
            {"id": 8480803, "name": "Evan Bouchard",    "pos": "D",  "line": 1, "pp": 1},
            {"id": 8475218, "name": "Mattias Ekholm",   "pos": "D",  "line": 1, "pp": 2},
        ],
        "away_players": [
            {"id": 8483445, "name": "Cutter Gauthier",  "pos": "LW", "line": 1, "pp": 1},
            {"id": 8484153, "name": "Leo Carlsson",     "pos": "C",  "line": 1, "pp": 1},
            {"id": 8478873, "name": "Troy Terry",       "pos": "RW", "line": 1, "pp": 1},
            {"id": 8482745, "name": "Mason McTavish",   "pos": "C",  "line": 2, "pp": 1},
            {"id": 8475798, "name": "Mikael Granlund",  "pos": "C",  "line": 2, "pp": 2},
            {"id": 8476885, "name": "Jacob Trouba",     "pos": "D",  "line": 1, "pp": 2},
            {"id": 8483490, "name": "Pavel Mintyukov",  "pos": "D",  "line": 1, "pp": 1},
        ],
    },
]


# ──────────────────────────────────────────────────────────────────────────────
# Score color helpers
# ──────────────────────────────────────────────────────────────────────────────

def gsaui_color(s: float) -> str:
    if s >= 62: return GREEN
    if s >= 50: return YELLOW
    return RED

def ppui_color(s: float) -> str:
    if s >= 62: return GREEN
    if s >= 50: return YELLOW
    return RED


# ──────────────────────────────────────────────────────────────────────────────
# Main report
# ──────────────────────────────────────────────────────────────────────────────

def run_report():
    print(f"\n{BOLD}{'═' * 90}{RESET}")
    print(f"{BOLD}  AXIOM NHL LIVE REPORT — {TODAY}  |  Playoff First Round  |  GSAUI + PPUI{RESET}")
    print(f"{BOLD}{'═' * 90}{RESET}")
    print(f"{DIM}  Engines: GSAUI (Goalie Shots-Against Under) · PPUI (Player Points Under){RESET}")
    print(f"{DIM}  Data: NHL Public API (api-web.nhle.com)  ·  Ref crew block: neutral (no public crew data){RESET}\n")

    for game in GAMES_TODAY:
        home = game["home"]
        away = game["away"]

        print(f"\n{BOLD}{'─' * 90}{RESET}")
        print(f"{BOLD}  {game['label']}  ·  {game['time_et']}{RESET}")
        print(f"  {CYAN}{game['note']}{RESET}")
        print(f"{'─' * 90}")

        ctx = NHLGameContext(
            game_date=TODAY,
            home_team=home,
            away_team=away,
            series_game_number=game["series_game"],
            home_series_wins=game["series_home_wins"],
            away_series_wins=game["series_away_wins"],
            home_b2b=game["home_b2b"],
            away_b2b=game["away_b2b"],
            home_rest_days=game["home_rest"],
            away_rest_days=game["away_rest"],
        )

        # ── GOALIE SECTION ────────────────────────────────────────────────────

        print(f"\n  {BOLD}GOALIE REPORT (GSAUI){RESET}")
        print(f"  {'GOALIE':<26} {'TEAM':<5} {'GSAUI':>6} {'GRD':>4} {'BASE-SH':>8} {'PROJ-SH':>8}  "
              f"{'OSQ':>5} {'GSS':>5} {'GEN':>5} {'TOP':>5} {'RFS':>5} {'TSC':>5}")
        print(f"  {'─' * 88}")

        for goalie_cfg, is_home in [
            (game["home_goalie"], True),
            (game["away_goalie"], False),
        ]:
            team = home if is_home else away
            opp_team = away if is_home else home
            # Opponent shots pg = opponent's shots FOR per game
            opp_shots = game["away_shots_pg"] if is_home else game["home_shots_pg"]
            opp_pp = game["away_pp_pct"] if is_home else game["home_pp_pct"]
            opp_hd = game["away_hd_pg"] if is_home else game["home_hd_pg"]
            own_pk = game["home_pk_pct"] if is_home else game["away_pk_pct"]
            own_blocks = game["home_blocks_pg"] if is_home else game["away_blocks_pg"]
            own_cf = game["home_cf_pct"] if is_home else game["away_cf_pct"]
            opp_series = game["series_away_wins"] if is_home else game["series_home_wins"]
            own_series = game["series_home_wins"] if is_home else game["series_away_wins"]

            fallback_sv = game["home_sv_pct"] if is_home else game["away_sv_pct"]

            f = build_goalie_feature_set(
                player_id=goalie_cfg["id"],
                player_name=goalie_cfg["name"],
                team=team,
                is_home=is_home,
                ctx=ctx,
                opp_team_abbrev=opp_team,
                opp_shots_pg=opp_shots,    # opponent shots on goal/game = goalie shots faced/game
                opp_pp_pct=opp_pp,
                opp_hd_rate=opp_hd,
                own_pk_pct=own_pk,
                own_blocks_pg=own_blocks,
                own_cf_pct=own_cf,
                series_score_opp=opp_series,
                series_score_own=own_series,
                fallback_sv_pct=fallback_sv,
            )

            # Override with real GSAx per game
            f.gss_gsax = norm_gsax(goalie_cfg["gsax_pg"])

            b2b_both = game["home_b2b"] and game["away_b2b"]
            result = compute_gsaui(f, b2b_both=b2b_both)

            gsaui = result["gsaui"]
            grade = result["grade"]
            proj = result["projected_shots"]
            base = result["base_shots"]
            blk = result["blocks"]

            col = gsaui_color(gsaui)
            print(
                f"  {goalie_cfg['name']:<26} {team:<5} "
                f"{col}{gsaui:>6.1f}{RESET} {grade:>4} "
                f"{base:>8.1f} {col}{proj:>8.1f}{RESET}  "
                f"{blk['OSQ']:>5.1f} {blk['GSS']:>5.1f} {blk['GEN']:>5.1f} "
                f"{blk['TOP']:>5.1f} {blk['RFS']:>5.1f} {blk['TSC']:>5.1f}"
            )

        # ── SKATER SECTION ────────────────────────────────────────────────────
        # Two engines run in parallel — same as baseball:
        #   Engine 1 (Formula/PPUI): deterministic block-score model
        #   Engine 2 (ML):           GradientBoosting trained on 2025-26 game logs
        # Both predict the four bettable markets. Signal shows agreement level.

        print(f"\n  {BOLD}PLAYER BETTING MARKETS  —  Engine 1: Formula (PPUI)  |  Engine 2: ML (2025-26 season){RESET}")
        print(f"  {'PLAYER':<22} {'TEAM':<5} {'POS':<3} {'PPUI':>5}  "
              f"{'── FORMULA ──':^31}  {'─── ML ENGINE ───':^31}  {'SIGNAL':<7}")
        print(f"  {'':<22} {'':<5} {'':<3} {'':<5}  "
              f"{'P':>6} {'G':>5} {'A':>5} {'SOG':>6}   "
              f"{'P':>6} {'G':>5} {'A':>5} {'SOG':>6}   {'N-GAMES'}")
        print(f"  {'─' * 102}")

        for player_cfg in game["home_players"] + game["away_players"]:
            is_home_player = player_cfg in game["home_players"]
            team = home if is_home_player else away
            opp_ga   = game["away_ga_pg"]    if is_home_player else game["home_ga_pg"]
            opp_sv   = game["away_sv_pct"]   if is_home_player else game["home_sv_pct"]
            opp_sa   = game["away_shots_pg"] if is_home_player else game["home_shots_pg"]
            opp_pk   = game["away_pk_pct"]   if is_home_player else game["home_pk_pct"]
            opp_hd   = game["away_hd_pg"]    if is_home_player else game["home_hd_pg"]
            opp_goalie_cfg = game["away_goalie"] if is_home_player else game["home_goalie"]
            opp_gsax = opp_goalie_cfg["gsax_pg"]

            # EDGE data drives the formula internally
            edge = get_skater_edge_data(player_cfg["id"])

            f = build_skater_feature_set(
                player_id=player_cfg["id"],
                player_name=player_cfg["name"],
                team=team,
                position=player_cfg["pos"],
                is_home=is_home_player,
                line=player_cfg["pp"],
                pp_unit=player_cfg["pp"],
                ctx=ctx,
                opp_ga_pg=opp_ga,
                opp_sv_pct=opp_sv,
                opp_shots_against_pg=opp_sa,
                opp_pk_pct=opp_pk,
                opp_hd_chances=opp_hd,
                opp_goalie_gsax_pg=opp_gsax,
                edge=edge,
            )

            result   = compute_ppui(f)
            ppui     = result["ppui"]
            grade    = result["grade"]
            proj_pts = result["projected_points"]

            proj_sog = max(0.0, f.avg_shots_per_game * (1 - 0.12 * ((ppui - 50) / 50)))
            proj_g   = proj_sog * f.avg_shooting_pct
            proj_a   = max(0.0, proj_pts - proj_g)

            # ── Engine 2: ML ────────────────────────────────────────────────
            ml = train_player_ml(player_cfg["id"], player_cfg["name"], is_home_player)

            col = ppui_color(ppui)

            if ml and ml.get("ml_active"):
                ml_p   = ml["ml_proj_points"]
                ml_g   = ml["ml_proj_goals"]
                ml_a   = ml["ml_proj_assists"]
                ml_sog = ml["ml_proj_shots"]
                n_g    = ml["n_samples"]
                sig    = compute_signal(proj_pts, ml_p)
                sig_col = GREEN if sig == "ALIGNED" else (YELLOW if sig == "LEAN" else MAGENTA)
                ml_str = (f"{ml_p:>6.2f} {ml_g:>5.2f} {ml_a:>5.2f} {ml_sog:>6.1f}   "
                          f"{sig_col}{sig:<7}{RESET} {DIM}({n_g}g){RESET}")
            elif ml and not ml.get("ml_active"):
                ml_str = f"{'INSUFF DATA':<35} {DIM}({ml.get('n_samples',0)}g){RESET}"
            else:
                ml_str = f"{'API TIMEOUT':<40}"

            print(
                f"  {player_cfg['name']:<22} {team:<5} "
                f"{player_cfg['pos']:<3} "
                f"{col}{ppui:>5.1f}{RESET}  "
                f"{col}{proj_pts:>6.2f}{RESET} {proj_g:>5.2f} {proj_a:>5.2f} {proj_sog:>6.1f}   "
                f"{ml_str}"
            )

    # ── Legend
    print(f"\n{'─' * 90}")
    print(f"  {BOLD}SCORE GUIDE{RESET}  "
          f"{GREEN}■ ≥62 Under-favoring{RESET}  "
          f"{YELLOW}■ 50-61 Neutral{RESET}  "
          f"{RED}■ <50 Over-favoring{RESET}")
    print(f"  GSAUI: Goalie Shots-Against Under Index  |  PROJ-SH = projected shots on goal faced")
    print(f"  PPUI:  Player Points Under Index (Engine 1 / Formula)")
    print(f"  ML:    GradientBoosting trained on each player's full 2025-26 regular season game log (Engine 2)")
    print(f"  Markets: P=Points · G=Goals · A=Assists · SOG=Shots on Goal  (all directly bettable)")
    print(f"\n  {BOLD}Signal:{RESET}  {GREEN}ALIGNED{RESET} = both engines within 10%  "
          f"  {YELLOW}LEAN{RESET} = 10-25% gap  "
          f"  {MAGENTA}SPLIT{RESET} = >25% gap (pay attention)")
    print(f"\n  {DIM}PPUI uses NHL EDGE speed/tracking data internally — not shown, but shapes the formula score.{RESET}")
    print(f"  {DIM}ML trains on rolling form (recent games weight naturally more than early-season games).{RESET}")
    print(f"\n  {BOLD}NOTE:{RESET} Re-run before placing any bet — API data updates continuously.")
    print(f"{'═' * 90}\n")


if __name__ == "__main__":
    run_report()
