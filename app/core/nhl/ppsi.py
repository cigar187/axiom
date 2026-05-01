"""
PPSI — Player Points Scoring Index
Scoring engine for NHL player points (goals + assists) under props.

Formula (mirrors KUSI block architecture):

  PPSI_base = 0.28*OSR + 0.22*PMR + 0.18*PER + 0.14*POP + 0.10*RPS + 0.08*TLD

  OSR (Opponent Scoring Resistance):
    0.22*goals_against_pg + 0.20*sv_pct_against + 0.18*shots_against_pg
    + 0.16*pk_pct_against + 0.14*hd_chances_against + 0.10*xga_per_60

  PMR (Player Matchup Rating):
    0.28*shooting_pct + 0.26*opp_goalie_sv_pct + 0.22*zone_start_pct
    + 0.14*opp_goalie_gsax + 0.10*shot_location

  PER (Player Efficiency Rating):
    0.24*shots_pg + 0.22*points_pg + 0.20*primary_pts_pg
    + 0.20*ixg_per_60 + 0.14*shooting_talent

  POP (Points Operational):
    0.30*toi_pg + 0.30*pp_toi_pg + 0.22*linemate_quality + 0.18*injury_linemates

  RPS (Referee PP Score):
    0.55*crew_pp_rate + 0.45*player_draw_rate

  TLD (Top-Line Deployment):
    0.40*toi_percentile + 0.35*line_position + 0.25*pp1_status

Interaction boosts (capped at 7.0):
  P1: OSR > 65 and PER > 65          → +2.0  (weak opponent + high-efficiency player)
  P2: TLD pp1_status = 100 and RPS > 65 → +1.5 (PP1 player + high-PP crew)
  P3: PMR > 70 and PER > 65          → +1.5  (favorable matchup + efficient scorer)
  P4: OSR < 35 and PMR < 40          → -1.5  (stingy opponent + hot goalie)

Volatility penalties (capped at 7.0):
  PV1: key linemate injured            → -2.5
  PV2: player on B2B fatigue           → -1.5
  PV3: opponent hot goalie (high GSAx) → -2.0
  PV4: player in scoring slump         → -1.0
  PV5: Game 7 elimination game         → -1.0

Final: PPSI = clamp(PPSI_base + interaction - volatility, 0, 100)
Projections:
  projected_pts     = base_pts * (1 - 0.22 * ((PPSI - 50) / 50))   cap 0–5
  projected_sog     = base_sog * (1 - 0.12 * ((PPSI - 50) / 50))   cap 0–12
  projected_goals   = projected_sog * player_shooting_pct            cap 0–3
  projected_assists = max(0, projected_pts - projected_goals)        cap 0–4
"""

import logging

from app.core.nhl.features import NHLSkaterFeatureSet

NEUTRAL = 50.0

logger = logging.getLogger(__name__)


def _f(val, fallback: float = NEUTRAL) -> float:
    return val if val is not None else fallback


# ─────────────────────────────────────────────────────────────
# Block scorers
# ─────────────────────────────────────────────────────────────

def score_osr(f: NHLSkaterFeatureSet) -> float:
    """OSR — Opponent Scoring Resistance (0-100). High = opponent is soft → easier to score."""
    ga_pg   = _f(f.osr_goals_against_pg)
    sv_pct  = _f(f.osr_sv_pct_against)
    sa_pg   = _f(f.osr_shots_against_pg)
    pk_pct  = _f(f.osr_pk_pct_against)
    hd      = _f(f.osr_hd_chances_against)
    xga     = _f(f.osr_xga_per_60)

    return (
        0.22 * ga_pg  +
        0.20 * sv_pct +
        0.18 * sa_pg  +
        0.16 * pk_pct +
        0.14 * hd     +
        0.10 * xga
    )


def score_pmr(f: NHLSkaterFeatureSet) -> float:
    """PMR — Player Matchup Rating (0-100). High = favorable individual matchup."""
    sh_pct     = _f(f.pmr_shooting_pct)
    opp_sv     = _f(f.pmr_opp_goalie_sv_pct)
    zone       = _f(f.pmr_zone_start_pct)
    opp_gsax   = _f(f.pmr_opp_goalie_gsax)
    shot_loc   = _f(f.pmr_shot_location)

    return (
        0.28 * sh_pct   +
        0.26 * opp_sv   +
        0.22 * zone     +
        0.14 * opp_gsax +
        0.10 * shot_loc
    )


def score_per(f: NHLSkaterFeatureSet) -> float:
    """PER — Player Efficiency Rating (0-100). High = generates lots of quality chances."""
    shots   = _f(f.per_shots_pg)
    pts     = _f(f.per_points_pg)
    pri_pts = _f(f.per_primary_pts_pg)
    ixg     = _f(f.per_ixg_per_60)
    talent  = _f(f.per_shooting_talent)

    return (
        0.24 * shots   +
        0.22 * pts     +
        0.20 * pri_pts +
        0.20 * ixg     +
        0.14 * talent
    )


def score_pop(f: NHLSkaterFeatureSet) -> float:
    """POP — Points Operational (0-100). High = coach gives heavy prime deployment."""
    toi       = _f(f.pop_toi_pg)
    pp_toi    = _f(f.pop_pp_toi_pg)
    linemates = _f(f.pop_linemate_quality)
    lm_health = _f(f.pop_injury_linemates)

    return (
        0.30 * toi       +
        0.30 * pp_toi    +
        0.22 * linemates +
        0.18 * lm_health
    )


def score_rps(f: NHLSkaterFeatureSet) -> float:
    """RPS — Referee PP Score (0-100). High = crew calls many PPs = more point chances."""
    crew   = _f(f.rps_crew_pp_rate)
    player = _f(f.rps_player_draw_rate)

    return (
        0.55 * crew   +
        0.45 * player
    )


def score_tld(f: NHLSkaterFeatureSet) -> float:
    """TLD — Top-Line Deployment (0-100). High = heavy-usage top producer."""
    toi_pct   = _f(f.tld_toi_percentile)
    line_pos  = _f(f.tld_line_position)
    pp1       = _f(f.tld_pp1_status)

    return (
        0.40 * toi_pct  +
        0.35 * line_pos +
        0.25 * pp1
    )


# ─────────────────────────────────────────────────────────────
# Interaction boosts
# ─────────────────────────────────────────────────────────────

def compute_ppsi_interaction(
    osr: float, pmr: float, per: float,
    pop: float, rps: float, tld: float,
    pp1_status: float = NEUTRAL,
) -> float:
    boost = 0.0

    # P1: Weak opponent + high-efficiency player
    if osr > 65 and per > 65:
        boost += 2.0

    # P2: PP1 player + high-PP crew
    if pp1_status >= 80 and rps > 65:
        boost += 1.5

    # P3: Favorable matchup + efficient scorer
    if pmr > 70 and per > 65:
        boost += 1.5

    # P4: Stingy opponent + hot goalie (hurts points)
    if osr < 35 and pmr < 40:
        boost -= 1.5

    return max(-7.0, min(7.0, boost))


# ─────────────────────────────────────────────────────────────
# Volatility penalties
# ─────────────────────────────────────────────────────────────

def compute_ppsi_volatility(f: NHLSkaterFeatureSet, pmr: float) -> float:
    penalty = 0.0

    # PV1: Key linemate injured
    lm_health = _f(f.pop_injury_linemates)
    if lm_health < 35:
        penalty += 2.5

    # PV2: Player on B2B fatigue (inferred from GEN of their game context)
    if f.ctx:
        on_b2b = f.ctx.away_b2b if not f.is_home else f.ctx.home_b2b
        if on_b2b:
            penalty += 1.5

    # PV3: Opponent goalie is hot
    opp_gsax = _f(f.pmr_opp_goalie_gsax)
    if opp_gsax < 30:  # low score = high GSAx = hot goalie
        penalty += 2.0

    # PV4: Player in scoring slump (low PER)
    per_pts = _f(f.per_points_pg)
    if per_pts < 38:
        penalty += 1.0

    # PV5: Game 7 elimination game — tighter defensive play reduces individual
    # scoring floors regardless of matchup quality
    if f.ctx and f.ctx.series_game_number >= 7:
        penalty += 1.0
        logger.info("PV5 triggered — Game 7 elimination game, tighter defensive play")

    return min(penalty, 7.0)


# ─────────────────────────────────────────────────────────────
# Main PPSI compute function
# ─────────────────────────────────────────────────────────────

def _clamp(v: float) -> float:
    return max(0.0, min(100.0, v))


def ppsi_grade(score: float) -> str:
    if score >= 63: return "A+"
    if score >= 56: return "A"
    if score >= 50: return "B"
    if score >= 44: return "C"
    return "D"


def compute_ppsi(f: NHLSkaterFeatureSet, silent: bool = False) -> dict:
    """
    Compute PPSI for a skater and return a results dict.

    Returns:
        {
            "ppsi": float (0-100),
            "grade": str,
            "projected_points": float   (cap 0–5),
            "projected_sog": float      (cap 0–12),
            "projected_goals": float    (cap 0–3),
            "projected_assists": float  (cap 0–4),
            "blocks": { OSR, PMR, PER, POP, RPS, TLD },
            "interaction": float,
            "volatility": float,
        }
    """
    osr = score_osr(f)
    pmr = score_pmr(f)
    per = score_per(f)
    pop = score_pop(f)
    rps = score_rps(f)
    tld = score_tld(f)

    ppsi_base = (
        0.28 * osr +
        0.22 * pmr +
        0.18 * per +
        0.14 * pop +
        0.10 * rps +
        0.08 * tld
    )

    pp1_status = _f(f.tld_pp1_status)
    interaction = compute_ppsi_interaction(
        osr=osr, pmr=pmr, per=per, pop=pop, rps=rps, tld=tld,
        pp1_status=pp1_status,
    )

    volatility = compute_ppsi_volatility(f, pmr=pmr)

    ppsi_raw = ppsi_base + interaction - volatility
    ppsi = _clamp(ppsi_raw)
    grade = ppsi_grade(ppsi)

    base_pts          = f.avg_points_per_game
    base_sog          = f.avg_shots_per_game
    player_sh_pct     = f.avg_shooting_pct

    projected_pts = base_pts * (1.0 - 0.22 * ((ppsi - 50.0) / 50.0))
    projected_pts = round(max(0.0, min(5.0, projected_pts)), 2)

    projected_sog = base_sog * (1.0 - 0.12 * ((ppsi - 50.0) / 50.0))
    projected_sog = round(max(0.0, min(12.0, projected_sog)), 1)

    projected_goals = round(max(0.0, min(3.0, projected_sog * player_sh_pct)), 2)

    projected_assists = round(max(0.0, min(4.0, projected_pts - projected_goals)), 2)

    return {
        "ppsi": round(ppsi, 1),
        "grade": grade,
        "projected_points": projected_pts,
        "projected_sog": projected_sog,
        "projected_goals": projected_goals,
        "projected_assists": projected_assists,
        "base_points": round(base_pts, 2),
        "base_sog": round(base_sog, 2),
        "blocks": {
            "OSR": round(osr, 1),
            "PMR": round(pmr, 1),
            "PER": round(per, 1),
            "POP": round(pop, 1),
            "RPS": round(rps, 1),
            "TLD": round(tld, 1),
        },
        "interaction": round(interaction, 2),
        "volatility": round(volatility, 2),
        "ppsi_base": round(ppsi_base, 2),
    }
