"""
GSAI — Goalie Shots-Against Index
Scoring engine for NHL goalie shots-on-goal under props.

Formula (mirrors HUSI block architecture):

  GSAI_base = 0.29*GSS + 0.24*OSQ + 0.18*TOP + 0.16*GEN + 0.08*RFS + 0.05*TSC

  OSQ (Opponent Shooting Quality):
    0.20*shots_pg + 0.18*shooting_pct + 0.18*pp_pct + 0.16*high_danger_rate
    + 0.14*series_momentum + 0.14*xgf_per_60

  GSS (Goalie Save Suppression):
    0.26*sv_pct + 0.22*gsax + 0.20*hd_sv_pct + 0.16*playoff_sv_pct
    + 0.10*rebound_control + 0.06*consistency

  GEN (Game Environment):
    0.30*is_home + 0.28*rest_days + 0.24*b2b_penalty + 0.12*series_game
    + 0.06*opponent_b2b

  TOP (Tactical / Operational):
    0.30*starter_prob + 0.22*pk_pct + 0.20*coach_defensive
    + 0.16*injury_status + 0.12*opponent_pp_rate

  RFS (Referee Flow Score):
    0.60*crew_pp_rate + 0.40*home_bias

  TSC (Team Structure & Coverage):
    0.40*blocks_pg + 0.35*cf_pct + 0.25*dzone_exit_pct

Interaction boosts (capped at 8.0):
  G1: GSS > 70 and OSQ > 65           → +2.0  (elite goalie vs weak shooter)
  G2: GEN rest_days > 70 and TOP > 65 → +1.5  (rested + strong deployment)
  G3: TSC > 65 and OSQ > 60           → +1.0  (defense limits quality shots)
  G4: RFS < 40 and TOP pp_rate > 65   → -1.5  (high-PP crew + opponent PP dangerous)
  G5: B2B penalty active (both teams)  → +1.0  (both tired = lower shot quality)

Volatility penalties (capped at 8.0):
  GV1: backup starter (not confirmed)  → -3.0
  GV2: opponent on hot streak          → -1.5
  GV3: goalie facing B2B (own team)    → -2.0
  GV4: high-danger shot rate vs goalie → -1.5
  GV5: referee crew known high-PP      → -1.0
  GV6: Game 7 elimination game         → -1.5
  GV7: GSS data missing                → -2.0

Final: GSAI = clamp(GSAI_base + interaction - volatility, 0, 100)
Projected shots: projected_shots = base_shots * (1 - 0.22 * ((GSAI - 50) / 50))
Capped: 15–50 shots.
"""

import logging

from app.core.nhl.features import NHLGoalieFeatureSet

NEUTRAL = 50.0

logger = logging.getLogger(__name__)


def _f(val, fallback: float = NEUTRAL) -> float:
    return val if val is not None else fallback


# ─────────────────────────────────────────────────────────────
# Block scorers
# ─────────────────────────────────────────────────────────────

def score_osq(f: NHLGoalieFeatureSet) -> float:
    """OSQ — Opponent Shooting Quality (0-100). High = weak opponent = fewer quality shots."""
    shots    = _f(f.osq_shots_pg)
    sh_pct   = _f(f.osq_shooting_pct)
    pp_pct   = _f(f.osq_pp_pct)
    hd_rate  = _f(f.osq_high_danger_rate)
    momentum = _f(f.osq_series_momentum)
    xgf      = _f(f.osq_xgf_per_60)

    return (
        0.20 * shots    +
        0.18 * sh_pct   +
        0.18 * pp_pct   +
        0.16 * hd_rate  +
        0.14 * momentum +
        0.14 * xgf
    )


def score_gss(f: NHLGoalieFeatureSet) -> float:
    """GSS — Goalie Save Suppression (0-100). High = elite shot stopper."""
    sv       = _f(f.gss_sv_pct)
    gsax     = _f(f.gss_gsax)
    hd_sv    = _f(f.gss_hd_sv_pct)
    po_sv    = _f(f.gss_playoff_sv_pct)
    rebound  = _f(f.gss_rebound_control)
    consist  = _f(f.gss_consistency)

    return (
        0.26 * sv       +
        0.22 * gsax     +
        0.20 * hd_sv    +
        0.16 * po_sv    +
        0.10 * rebound  +
        0.06 * consist
    )


def score_gen(f: NHLGoalieFeatureSet) -> float:
    """GEN — Game Environment (0-100). High = environment suppresses shot volume."""
    is_home   = _f(f.gen_is_home)
    rest      = _f(f.gen_rest_days)
    b2b       = _f(f.gen_b2b_penalty)
    ser_game  = _f(f.gen_series_game)
    opp_b2b   = _f(f.gen_opponent_b2b)

    return (
        0.30 * is_home  +
        0.28 * rest     +
        0.24 * b2b      +
        0.12 * ser_game +
        0.06 * opp_b2b
    )


def score_top(f: NHLGoalieFeatureSet) -> float:
    """TOP — Tactical / Operational (0-100). High = goalie in favorable deployment."""
    starter   = _f(f.top_starter_prob)
    pk        = _f(f.top_pk_pct)
    coach     = _f(f.top_coach_defensive)
    injury    = _f(f.top_injury_status)
    opp_pp    = _f(f.top_opponent_pp_rate)

    return (
        0.30 * starter  +
        0.22 * pk       +
        0.20 * coach    +
        0.16 * injury   +
        0.12 * opp_pp
    )


def score_rfs(f: NHLGoalieFeatureSet) -> float:
    """RFS — Referee Flow Score (0-100). High = low-PP crew = fewer power-play shots."""
    crew    = _f(f.rfs_crew_pp_rate)
    bias    = _f(f.rfs_home_bias)

    return (
        0.60 * crew +
        0.40 * bias
    )


def score_tsc(f: NHLGoalieFeatureSet) -> float:
    """TSC — Team Structure & Coverage (0-100). High = defense limits shot quality."""
    blocks  = _f(f.tsc_blocks_pg)
    cf      = _f(f.tsc_cf_pct)
    dzone   = _f(f.tsc_dzone_exit_pct)

    return (
        0.40 * blocks +
        0.35 * cf     +
        0.25 * dzone
    )


# ─────────────────────────────────────────────────────────────
# Interaction boosts
# ─────────────────────────────────────────────────────────────

def compute_gsai_interaction(
    osq: float, gss: float, gen: float, top: float,
    rfs: float, tsc: float,
    b2b_both: bool = False,
    opp_pp_rate_raw: float = NEUTRAL,
) -> float:
    boost = 0.0

    # G1: Elite goalie vs weak shooter
    if gss > 70 and osq > 65:
        boost += 2.0

    # G2: Rested + strong deployment
    if gen > 70 and top > 65:
        boost += 1.5

    # G3: Defense limits quality shots
    if tsc > 65 and osq > 60:
        boost += 1.0

    # G4: High-PP crew + dangerous opponent PP (hurts under)
    if rfs < 40 and opp_pp_rate_raw > 65:
        boost -= 1.5

    # G5: Both teams on B2B → lower shot quality → slight under boost
    if b2b_both:
        boost += 1.0

    return max(-8.0, min(8.0, boost))


# ─────────────────────────────────────────────────────────────
# Volatility penalties
# ─────────────────────────────────────────────────────────────

def compute_gsai_volatility(
    f: NHLGoalieFeatureSet,
    gss: float,
    gen: float,
    rfs: float,
) -> float:
    penalty = 0.0

    # GV1: Backup / unconfirmed starter
    if not f.is_confirmed_starter:
        penalty += 3.0

    # GV2: Opponent hot streak (series lead + momentum against)
    momentum = _f(f.osq_series_momentum)
    if momentum < 35:  # opponent has strong momentum
        penalty += 1.5

    # GV3: Goalie on B2B (own team)
    b2b_val = _f(f.gen_b2b_penalty)
    if b2b_val < 35:  # low score = on B2B = fatigued
        penalty += 2.0

    # GV4: High-danger shots rate above neutral
    hd = _f(f.osq_high_danger_rate)
    if hd < 35:  # low OSQ hd = opponent has high-danger threats
        penalty += 1.5

    # GV5: Known high-PP crew
    if rfs < 40:
        penalty += 1.0

    # GV6: Game 7 elimination game — both teams play more defensively but
    # unpredictably; maximum series volatility regardless of shot model
    series_game = f.gen_series_game
    if series_game is not None and series_game >= 7:
        penalty += 1.5
        logger.info("GV6 triggered — Game 7 elimination game")

    # GV7: GSS data unavailable — never treat a goalie with no data as neutral
    if not f.gss_data_available:
        penalty += 2.0
        logger.info("GV7 triggered — GSS data missing, applying volatility penalty")

    return min(penalty, 8.0)


# ─────────────────────────────────────────────────────────────
# Main GSAI compute function
# ─────────────────────────────────────────────────────────────

def _clamp(v: float) -> float:
    return max(0.0, min(100.0, v))


def gsai_grade(score: float) -> str:
    if score >= 65: return "A+"
    if score >= 58: return "A"
    if score >= 52: return "B"
    if score >= 46: return "C"
    return "D"


def compute_gsai(
    f: NHLGoalieFeatureSet,
    b2b_both: bool = False,
    silent: bool = False,
) -> dict:
    """
    Compute GSAI for a goalie and return a results dict.

    Returns:
        {
            "gsai": float (0-100),
            "grade": str,
            "projected_shots": float,
            "blocks": { OSQ, GSS, GEN, TOP, RFS, TSC },
            "interaction": float,
            "volatility": float,
        }
    """
    osq = score_osq(f)
    gss = score_gss(f)
    gen = score_gen(f)
    top = score_top(f)
    rfs = score_rfs(f)
    tsc = score_tsc(f)

    gsai_base = (
        0.29 * gss +
        0.24 * osq +
        0.18 * top +
        0.16 * gen +
        0.08 * rfs +
        0.05 * tsc
    )

    interaction = compute_gsai_interaction(
        osq=osq, gss=gss, gen=gen, top=top, rfs=rfs, tsc=tsc,
        b2b_both=b2b_both,
        opp_pp_rate_raw=_f(f.top_opponent_pp_rate),
    )

    volatility = compute_gsai_volatility(f, gss=gss, gen=gen, rfs=rfs)

    gsai_raw = gsai_base + interaction - volatility
    gsai = _clamp(gsai_raw)
    grade = gsai_grade(gsai)

    # Projected shots — sensitivity 0.22, capped 15–50
    base = f.avg_shots_faced_per_game
    projected_shots = base * (1.0 - 0.22 * ((gsai - 50.0) / 50.0))
    projected_shots = round(max(15.0, min(50.0, projected_shots)), 1)

    return {
        "gsai": round(gsai, 1),
        "grade": grade,
        "projected_shots": projected_shots,
        "base_shots": round(base, 1),
        "blocks": {
            "OSQ": round(osq, 1),
            "GSS": round(gss, 1),
            "GEN": round(gen, 1),
            "TOP": round(top, 1),
            "RFS": round(rfs, 1),
            "TSC": round(tsc, 1),
        },
        "interaction": round(interaction, 2),
        "volatility": round(volatility, 2),
        "gsai_base": round(gsai_base, 2),
    }
