"""
QPYI — QB Passing Yards Index
Scoring engine for NFL quarterback passing yards props.

Mirrors HUSI architecture exactly. Projects passing yards for one QB in one game.

Formula:
  QPYI_base = 0.23×OSW + 0.20×QSR + 0.14×GSP + 0.12×SCB + 0.10×PDR + 0.09×ENS + 0.07×DSR + 0.05×RCT

  OSW (Opponent Secondary Weakness) — 23%:
    Sub-weights PENDING — awaiting piece from user
    Inputs: osw_cb, osw_slot, osw_safety, osw_yat, osw_cmp, osw_air,
            osw_blitz, osw_press, osw_dvoa

  QSR (QB Skill Rating) — 20%:
    Sub-weights PENDING — awaiting piece from user
    Inputs: qsr_cpoe, qsr_air, qsr_pres_cmp, qsr_ttt, qsr_deep,
            qsr_offplat, qsr_mech, qsr_presnap, qsr_pa_rate, qsr_pa_cpoe

  GSP (Game Script Profile) — 14%:
    Sub-weights PENDING — awaiting piece from user
    Inputs: gsp_pcall, gsp_spread, gsp_total, gsp_snaps,
            gsp_pace, gsp_rz, gsp_oc_trend

  SCB (Supporting Cast Block) — 12%:
    Sub-weights PENDING — awaiting piece from user
    Inputs: scb_pblk, scb_sep, scb_yac, scb_te, scb_inj, scb_ryoe

  PDR (Physical Durability Rating) — 10%:
    Sub-weights PENDING — awaiting piece from user
    Inputs: pdr_sack, pdr_press, pdr_mob, pdr_hits, pdr_rest,
            pdr_snaps_prior, pdr_prac, pdr_inj, pdr_age, pdr_trend

  ENS (Environmental) — 9%:
    Sub-weights PENDING — awaiting piece from user
    Inputs: ens_dome, ens_wind, ens_temp, ens_precip,
            ens_turf, ens_alt, ens_crowd

  DSR (Defensive Scheme Rating) — 7%:
    Sub-weights PENDING — awaiting piece from user
    Inputs: dsr_zone_eff, dsr_man_eff, dsr_blitz_eff,
            dsr_dc_scheme, dsr_matchup_hist

  RCT (Referee Crew Tendencies) — 5%:
    Sub-weights PENDING — awaiting piece from user
    Inputs: rct_pi, rct_rtp, rct_hold, rct_total

Interaction boosts:
  PENDING — awaiting piece from user

Volatility penalties:
  PENDING — awaiting piece from user

Post-formula adjustments:
  PDR rest multiplier  — PENDING
  Park/turf multiplier — PENDING

Final: QPYI = clamp(QPYI_base + interaction - volatility, 0, 100)
Projected yards: PENDING — awaiting projection formula piece from user
"""

import statistics

from app.core.nfl.features import QBFeatureSet
from app.utils.normalization import clamp
from app.utils.logging import get_logger

log = get_logger("qpyi_engine")

NEUTRAL = 50.0


def _f(val, fallback: float = NEUTRAL) -> float:
    """Return val if not None, else fallback. Mirrors _f() in husi.py / kusi.py."""
    return val if val is not None else fallback


def compute_gts_modifier(f: "QBFeatureSet") -> dict:
    """
    GTS — Game Total Score modifier (NFL, passing yards).

    Calibrated for NFL game totals (league avg ≈ 46.5).
    High total → pass-heavy environment → QPYI goes UP (more yards expected).
    Low total  → run-heavy/defensive game → QPYI goes DOWN.

    Returns:
        {"score_adjustment": float, "ip_cap_adjustment": float}
    Defaults to 0.0 when game_total is not yet wired to the feature set.
    """
    game_total = getattr(f, "game_total", None)
    if game_total is None or game_total == 0.0:
        return {"score_adjustment": 0.0, "ip_cap_adjustment": 0.0}

    league_avg = 46.5
    aw = game_total / league_avg  # 1.0 = neutral

    if game_total >= 52.0:
        score_adj = (aw - 1.0) * 15.0   # high total → boost — QPYI increases
        ip_cap_adj = 0.0
    elif game_total <= 40.0:
        score_adj = (aw - 1.0) * 15.0   # low total → penalty — QPYI decreases (aw < 1.0 → negative)
        ip_cap_adj = 0.0
    else:
        score_adj = 0.0
        ip_cap_adj = 0.0

    return {
        "score_adjustment": round(score_adj, 2),
        "ip_cap_adjustment": round(ip_cap_adj, 2),
    }


# ─────────────────────────────────────────────────────────────
# Block scorers
# ─────────────────────────────────────────────────────────────

def score_osw(f: QBFeatureSet) -> float:
    """
    OSW = Opponent Secondary Weakness (0–100).
    High score = weak opposing secondary = favors QB passing production.

    Sources:
      osw_cb, osw_slot  → Pro Football Focus
      osw_safety        → Next Gen Stats / Mike Lopez
      osw_yat           → NFL Stats API
      osw_cmp           → nflfastR / Ben Baldwin
      osw_air           → Josh Hermsmeyer (FiveThirtyEight)
      osw_blitz         → Next Gen Stats / Mike Lopez
      osw_press         → Next Gen Stats / Quang Nguyen (STRAIN)
      osw_dvoa          → Aaron Schatz (Football Outsiders)

    Sub-weights: PENDING — awaiting formula piece from user.
    Placeholder equal weights used until sub-formula is received.
    """
    cb     = _f(f.osw_cb)
    slot   = _f(f.osw_slot)
    safety = _f(f.osw_safety)
    yat    = _f(f.osw_yat)
    cmp    = _f(f.osw_cmp)
    air    = _f(f.osw_air)
    blitz  = _f(f.osw_blitz)
    press  = _f(f.osw_press)
    dvoa   = _f(f.osw_dvoa)

    osw = (
        0.20 * dvoa   +
        0.16 * cb     +
        0.14 * yat    +
        0.12 * cmp    +
        0.12 * press  +
        0.10 * blitz  +
        0.08 * slot   +
        0.05 * safety +
        0.03 * air
    )
    log.debug("QPYI OSW", qb=f.player_name,
              cb=cb, slot=slot, safety=safety, yat=yat, cmp=cmp,
              air=air, blitz=blitz, press=press, dvoa=dvoa, osw=round(osw, 2))
    return clamp(osw)


def score_qsr(f: QBFeatureSet) -> float:
    """
    QSR = QB Skill Rating (0–100).
    High score = elite individual QB skill level this week.

    Sources:
      qsr_cpoe, qsr_pa_rate, qsr_pa_cpoe → nflfastR / Ben Baldwin (The Athletic)
      qsr_air                             → Josh Hermsmeyer (FiveThirtyEight)
      qsr_pres_cmp, qsr_ttt               → Next Gen Stats / Mike Lopez
      qsr_deep, qsr_offplat               → Pro Football Focus
      qsr_mech                            → Glenn Fleisig PhD (ASMI) / Gregory Rash
      qsr_presnap                         → Next Gen Stats / Cynthia Frelund (NFL Network)

    Sub-weights: PENDING — awaiting formula piece from user.
    Placeholder equal weights used until sub-formula is received.
    """
    cpoe     = _f(f.qsr_cpoe)
    air      = _f(f.qsr_air)
    pres_cmp = _f(f.qsr_pres_cmp)
    ttt      = _f(f.qsr_ttt)
    deep     = _f(f.qsr_deep)
    offplat  = _f(f.qsr_offplat)
    mech     = _f(f.qsr_mech)
    presnap  = _f(f.qsr_presnap)
    pa_rate  = _f(f.qsr_pa_rate)
    pa_cpoe  = _f(f.qsr_pa_cpoe)

    qsr = (
        0.22 * cpoe     +
        0.14 * deep     +
        0.13 * pres_cmp +
        0.12 * pa_cpoe  +
        0.10 * air      +
        0.10 * presnap  +
        0.08 * offplat  +
        0.07 * pa_rate  +
        0.02 * ttt      +
        0.02 * mech
    )
    log.debug("QPYI QSR", qb=f.player_name,
              cpoe=cpoe, air=air, pres_cmp=pres_cmp, ttt=ttt, deep=deep,
              offplat=offplat, mech=mech, presnap=presnap,
              pa_rate=pa_rate, pa_cpoe=pa_cpoe, qsr=round(qsr, 2))
    return clamp(qsr)


def score_gsp(f: QBFeatureSet) -> float:
    """
    GSP = Game Script Profile (0–100).
    High score = game script produces many passing opportunities.

    Sources:
      gsp_pcall, gsp_oc_trend → Warren Sharp (Sharp Football Analysis)
      gsp_spread, gsp_total   → Vegas lines
      gsp_snaps               → nflfastR / Brian Burke
      gsp_pace                → NFL Stats
      gsp_rz                  → NFL Stats / Dean Oliver (ESPN QBR)

    Sub-weights: PENDING — awaiting formula piece from user.
    Placeholder equal weights used until sub-formula is received.
    """
    pcall    = _f(f.gsp_pcall)
    spread   = _f(f.gsp_spread)
    total    = _f(f.gsp_total)
    snaps    = _f(f.gsp_snaps)
    pace     = _f(f.gsp_pace)
    rz       = _f(f.gsp_rz)
    oc_trend = _f(f.gsp_oc_trend)

    gsp = (
        0.25 * spread   +
        0.20 * total    +
        0.18 * pcall    +
        0.15 * snaps    +
        0.10 * pace     +
        0.08 * rz       +
        0.04 * oc_trend
    )
    log.debug("QPYI GSP", qb=f.player_name,
              pcall=pcall, spread=spread, total=total, snaps=snaps,
              pace=pace, rz=rz, oc_trend=oc_trend, gsp=round(gsp, 2))
    return clamp(gsp)


def score_scb(f: QBFeatureSet) -> float:
    """
    SCB = Supporting Cast Block (0–100).
    High score = strong supporting cast this week.

    Sources:
      scb_pblk → ESPN Next Gen Stats / Thompson Bliss (NFL Physics)
      scb_sep  → Next Gen Stats / Mike Lopez
      scb_yac  → nflfastR / Brian Burke (EPA)
      scb_te   → NFL Stats
      scb_inj  → Official NFL injury report
      scb_ryoe → Next Gen Stats / Brian Burke (EPA splits)

    Sub-weights: PENDING — awaiting formula piece from user.
    Placeholder equal weights used until sub-formula is received.
    """
    pblk = _f(f.scb_pblk)
    sep  = _f(f.scb_sep)
    yac  = _f(f.scb_yac)
    te   = _f(f.scb_te)
    inj  = _f(f.scb_inj)
    ryoe = _f(f.scb_ryoe)

    scb = (
        0.28 * pblk +
        0.25 * sep  +
        0.18 * yac  +
        0.12 * te   +
        0.10 * ryoe +
        0.07 * inj
    )
    log.debug("QPYI SCB", qb=f.player_name,
              pblk=pblk, sep=sep, yac=yac, te=te, inj=inj, ryoe=ryoe,
              scb=round(scb, 2))
    return clamp(scb)


def score_pdr(f: QBFeatureSet) -> float:
    """
    PDR = Physical Durability Rating (0–100).
    High score = QB is physically fresh, healthy, and mobile this week.
    Unique to NFL — no equivalent block exists in the MLB formula.

    Sources:
      pdr_sack        → NFL Stats / Brad Oremland (QB-TSP)
      pdr_press       → Next Gen Stats / Rishav Dutta (Cleveland Browns)
      pdr_mob         → Next Gen Stats
      pdr_hits        → NFL Injury Surveillance System / Kelly et al. (EMG study)
      pdr_rest        → NFL schedule
      pdr_snaps_prior → nflfastR play-by-play
      pdr_prac        → Official NFL injury report
      pdr_inj         → Official NFL injury report
      pdr_age         → John DeWitt PhD (NASA / Rice University)
      pdr_trend       → Andrew Patton PhD (Johns Hopkins / NFL Analytics)

    Sub-weights: PENDING — awaiting formula piece from user.
    Placeholder equal weights used until sub-formula is received.
    """
    sack        = _f(f.pdr_sack)
    press       = _f(f.pdr_press)
    mob         = _f(f.pdr_mob)
    hits        = _f(f.pdr_hits)
    rest        = _f(f.pdr_rest)
    snaps_prior = _f(f.pdr_snaps_prior)
    prac        = _f(f.pdr_prac)
    inj         = _f(f.pdr_inj)
    age         = _f(f.pdr_age)
    trend       = _f(f.pdr_trend)

    pdr = (
        0.20 * rest        +
        0.18 * inj         +
        0.14 * sack        +
        0.12 * press       +
        0.10 * prac        +
        0.09 * hits        +
        0.08 * trend       +
        0.05 * mob         +
        0.03 * age         +
        0.01 * snaps_prior
    )
    log.debug("QPYI PDR", qb=f.player_name,
              sack=sack, press=press, mob=mob, hits=hits, rest=rest,
              snaps_prior=snaps_prior, prac=prac, inj=inj, age=age,
              trend=trend, pdr=round(pdr, 2))
    return clamp(pdr)


def score_ens(f: QBFeatureSet) -> float:
    """
    ENS = Environmental (0–100).
    High score = conditions favor passing production.

    Sources:
      ens_dome                      → NFL schedule data
      ens_wind, ens_temp, ens_precip → Weather API / Thompson Bliss (NFL Physics)
      ens_turf, ens_alt             → Stadium data
      ens_crowd                     → PFF road/home splits / stadium noise ratings

    Sub-weights: PENDING — awaiting formula piece from user.
    Placeholder equal weights used until sub-formula is received.
    """
    dome   = _f(f.ens_dome)
    wind   = _f(f.ens_wind)
    temp   = _f(f.ens_temp)
    precip = _f(f.ens_precip)
    turf   = _f(f.ens_turf)
    alt    = _f(f.ens_alt)
    crowd  = _f(f.ens_crowd)

    ens = (
        0.25 * dome   +
        0.22 * wind   +
        0.18 * precip +
        0.14 * temp   +
        0.10 * turf   +
        0.08 * crowd  +
        0.03 * alt
    )
    log.debug("QPYI ENS", qb=f.player_name,
              dome=dome, wind=wind, temp=temp, precip=precip,
              turf=turf, alt=alt, crowd=crowd, ens=round(ens, 2))
    return clamp(ens)


def score_dsr(f: QBFeatureSet) -> float:
    """
    DSR = Defensive Scheme Rating (0–100).
    High score = this QB reads and attacks this specific scheme effectively.
    New block — not present in MLB formula.

    Captures the specific matchup between a QB's tendencies and this week's
    DC scheme. Josh Allen vs. Tampa-2 vs. single-high man are fundamentally
    different problems — previously buried inside OSW / QSR averages.

    Sources:
      dsr_zone_eff, dsr_matchup_hist → nflfastR / Cynthia Frelund (NFL Network)
      dsr_man_eff                    → nflfastR / Ben Baldwin
      dsr_blitz_eff                  → Next Gen Stats / Mike Lopez
      dsr_dc_scheme                  → Ted Nguyen (The 33rd Team)

    Sub-weights: PENDING — awaiting formula piece from user.
    Placeholder equal weights used until sub-formula is received.
    """
    zone_eff     = _f(f.dsr_zone_eff)
    man_eff      = _f(f.dsr_man_eff)
    blitz_eff    = _f(f.dsr_blitz_eff)
    dc_scheme    = _f(f.dsr_dc_scheme)
    matchup_hist = _f(f.dsr_matchup_hist)

    dsr = (
        0.28 * zone_eff     +
        0.22 * man_eff      +
        0.20 * blitz_eff    +
        0.18 * matchup_hist +
        0.12 * dc_scheme
    )
    log.debug("QPYI DSR", qb=f.player_name,
              zone_eff=zone_eff, man_eff=man_eff, blitz_eff=blitz_eff,
              dc_scheme=dc_scheme, matchup_hist=matchup_hist,
              dsr=round(dsr, 2))
    return clamp(dsr)


def score_rct(f: QBFeatureSet) -> float:
    """
    RCT = Referee Crew Tendencies (0–100).
    High score = this crew's tendencies favor passing volume and opportunities.

    Sources:
      rct_pi, rct_rtp  → Warren Sharp (Sharp Football Analysis)
      rct_hold, rct_total → NFL Stats

    Sub-weights: PENDING — awaiting formula piece from user.
    Placeholder equal weights used until sub-formula is received.
    """
    pi    = _f(f.rct_pi)
    rtp   = _f(f.rct_rtp)
    hold  = _f(f.rct_hold)
    total = _f(f.rct_total)

    rct = (
        0.35 * pi    +
        0.28 * rtp   +
        0.22 * hold  +
        0.15 * total
    )
    log.debug("QPYI RCT", qb=f.player_name,
              pi=pi, rtp=rtp, hold=hold, total=total, rct=round(rct, 2))
    return clamp(rct)


# ─────────────────────────────────────────────────────────────
# Interaction boosts
# ─────────────────────────────────────────────────────────────

def compute_qpyi_interaction(f: QBFeatureSet, **block_scores) -> float:
    """
    Apply QPYI interaction boosts. Returns total boost capped at [-5.0, +5.0].

    Positive boosts fire when multiple blocks compound each other's strength.
    Negative boost fires when physical vulnerability and bad conditions overlap.

    Rules:
      I1: osw >= 65 AND gsp >= 65  → +2.5  elite secondary weakness + pass-heavy script
      I2: qsr >= 65 AND scb >= 65  → +2.0  elite QB skill + elite supporting cast
      I3: ens >= 65 AND gsp >= 65  → +1.5  dome/perfect weather + pass-heavy script
      I4: dsr >= 65 AND qsr >= 65  → +1.5  elite QB reads the scheme he faces best
      I5: pdr <= 35 AND ens <= 35  → -2.0  banged-up QB in bad weather (compounding downside)
    """
    osw = block_scores.get("osw", NEUTRAL)
    qsr = block_scores.get("qsr", NEUTRAL)
    gsp = block_scores.get("gsp", NEUTRAL)
    scb = block_scores.get("scb", NEUTRAL)
    pdr = block_scores.get("pdr", NEUTRAL)
    ens = block_scores.get("ens", NEUTRAL)
    dsr = block_scores.get("dsr", NEUTRAL)

    boost = 0.0
    name  = f.player_name

    if osw >= 65 and gsp >= 65:
        boost += 2.5
        log.info("QPYI I1 triggered", qb=name, osw=osw, gsp=gsp, boost=2.5)

    if qsr >= 65 and scb >= 65:
        boost += 2.0
        log.info("QPYI I2 triggered", qb=name, qsr=qsr, scb=scb, boost=2.0)

    if ens >= 65 and gsp >= 65:
        boost += 1.5
        log.info("QPYI I3 triggered", qb=name, ens=ens, gsp=gsp, boost=1.5)

    if dsr >= 65 and qsr >= 65:
        boost += 1.5
        log.info("QPYI I4 triggered", qb=name, dsr=dsr, qsr=qsr, boost=1.5)

    if pdr <= 35 and ens <= 35:
        boost -= 2.0
        log.info("QPYI I5 triggered", qb=name, pdr=pdr, ens=ens, penalty=-2.0)

    return max(-5.0, min(5.0, boost))


# ─────────────────────────────────────────────────────────────
# Volatility penalties
# ─────────────────────────────────────────────────────────────

def compute_qpyi_volatility(f: QBFeatureSet, **block_scores) -> float:
    """
    Apply QPYI volatility penalties. Returns total penalty in [-8.0, 0.0].
    This value is ADDED to the score in compute_qpyi() — negative means reduction.

    Rules:
      V1: any block score < 30          → -1.5 per block  (extreme weakness = downside risk)
      V2: pdr < 40 AND qsr < 50         → -2.0            (hurt QB also struggling with accuracy)
      V3: ens < 35                      → -1.0            (severe weather adds projection uncertainty)
      V4: stdev(all 8 blocks) > 20      → -1.0            (highly uneven profile = less predictable)
    """
    osw = block_scores.get("osw", NEUTRAL)
    qsr = block_scores.get("qsr", NEUTRAL)
    gsp = block_scores.get("gsp", NEUTRAL)
    scb = block_scores.get("scb", NEUTRAL)
    pdr = block_scores.get("pdr", NEUTRAL)
    ens = block_scores.get("ens", NEUTRAL)
    dsr = block_scores.get("dsr", NEUTRAL)
    rct = block_scores.get("rct", NEUTRAL)

    blocks = [osw, qsr, gsp, scb, pdr, ens, dsr, rct]
    penalty = 0.0
    name    = f.player_name

    # V1: extreme weakness in any single block
    for label, score in zip(["osw", "qsr", "gsp", "scb", "pdr", "ens", "dsr", "rct"], blocks):
        if score < 30:
            penalty -= 1.5
            log.info("QPYI V1 triggered", qb=name, block=label, score=score, penalty=-1.5)

    # V2: hurt QB + accuracy issues
    if pdr < 40 and qsr < 50:
        penalty -= 2.0
        log.info("QPYI V2 triggered", qb=name, pdr=pdr, qsr=qsr, penalty=-2.0)

    # V3: severe weather
    if ens < 35:
        penalty -= 1.0
        log.info("QPYI V3 triggered", qb=name, ens=ens, penalty=-1.0)

    # V4: highly uneven profile
    if statistics.stdev(blocks) > 20:
        penalty -= 1.0
        log.info("QPYI V4 triggered", qb=name, stdev=round(statistics.stdev(blocks), 2), penalty=-1.0)

    return max(-8.0, min(0.0, penalty))


# ─────────────────────────────────────────────────────────────
# Grading
# ─────────────────────────────────────────────────────────────

def qpyi_grade(score: float) -> str:
    """
    Grade thresholds anchored to real NFL QB distributions.
    Mirrors husi_grade() / kusi_grade() pattern exactly.

    In modern football no QB realistically scores 85+ on QPYI.
    An elite QB in a dome against a soft secondary lands in the 65-75 range.
    Thresholds are set against what real QBs actually achieve:

      A+  ≥ 65  — Elite yards matchup, soft secondary, dome, pass-heavy script  (top ~10%)
      A   ≥ 58  — Very favorable — big passing day expected                      (top ~25%)
      B   ≥ 52  — Solid — productive game likely                                 (top ~45%)
      C   ≥ 46  — Neutral — average output expected                              (middle ~30%)
      D   < 46  — Unfavorable — tough defense, bad weather, or run-heavy script  (bottom ~25%)
    """
    if score >= 65:
        return "A+"
    elif score >= 58:
        return "A"
    elif score >= 52:
        return "B"
    elif score >= 46:
        return "C"
    else:
        return "D"


# ─────────────────────────────────────────────────────────────
# Projection multiplier helpers
# ─────────────────────────────────────────────────────────────

def get_pdr_rest_multiplier(pdr_rest: float) -> float:
    """
    Convert the PDR rest score (0–100) into a projected yards multiplier.
    Higher pdr_rest = more days of recovery = positive multiplier.

      >= 80  →  1.08   bye week, fully rested
      >= 65  →  1.03   normal full week rest
      >= 50  →  1.00   neutral, adequate rest
      >= 35  →  0.95   short week, some fatigue
       < 35  →  0.88   Thursday night game, severe penalty
    """
    if pdr_rest >= 80:
        return 1.08
    elif pdr_rest >= 65:
        return 1.03
    elif pdr_rest >= 50:
        return 1.00
    elif pdr_rest >= 35:
        return 0.95
    else:
        return 0.88


def get_park_turf_multiplier(ens_dome: float, ens_turf: float) -> float:
    """
    Convert dome and turf scores (0–100 each) into a projected yards multiplier.
    Dome + artificial turf = maximum passing environment.

      dome >= 70 AND turf >= 70  →  1.06   indoor artificial — maximum passing environment
      dome >= 70                 →  1.04   dome regardless of turf
      turf >= 70                 →  1.02   outdoor artificial turf
      dome < 40 AND turf < 40    →  0.97   outdoor natural grass — slight passing penalty
      otherwise                  →  1.00   neutral
    """
    if ens_dome >= 70 and ens_turf >= 70:
        return 1.06
    elif ens_dome >= 70:
        return 1.04
    elif ens_turf >= 70:
        return 1.02
    elif ens_dome < 40 and ens_turf < 40:
        return 0.97
    else:
        return 1.00


# ─────────────────────────────────────────────────────────────
# Main QPYI computation
# ─────────────────────────────────────────────────────────────

def compute_qpyi(f: QBFeatureSet, silent: bool = False) -> dict:
    """
    Compute the full QPYI score for one QB on one game day.

    Args:
        f:      QB feature set.
        silent: If True, suppress all logging (used by simulation engine).

    Returns a dict with:
      qpyi_base, qpyi_interaction, qpyi_volatility, qpyi, grade,
      projected_yards, base_yards,
      all individual block scores for database logging.

    Post-formula multipliers applied in order: PDR rest multiplier → park/turf multiplier → cap 0–500.
    """
    if not silent:
        log.info("QPYI computation starting", qb=f.player_name, game_id=f.game_id)

    # ── Score all blocks
    osw = score_osw(f)
    qsr = score_qsr(f)
    gsp = score_gsp(f)
    scb = score_scb(f)
    pdr = score_pdr(f)
    ens = score_ens(f)
    dsr = score_dsr(f)
    rct = score_rct(f)

    # ── Top-level formula
    qpyi_base = (
        0.23 * osw +
        0.20 * qsr +
        0.14 * gsp +
        0.12 * scb +
        0.10 * pdr +
        0.09 * ens +
        0.07 * dsr +
        0.05 * rct
    )

    # ── Interactions and volatility
    interaction = compute_qpyi_interaction(f,
        osw=osw, qsr=qsr, gsp=gsp, scb=scb,
        pdr=pdr, ens=ens, dsr=dsr, rct=rct)
    volatility = compute_qpyi_volatility(f,
        osw=osw, qsr=qsr, gsp=gsp, scb=scb,
        pdr=pdr, ens=ens, dsr=dsr, rct=rct)

    # volatility is 0 or negative — adding it correctly reduces the score
    qpyi_raw = qpyi_base + interaction + volatility
    qpyi = clamp(qpyi_raw)

    # ── GTS modifier (game total context)
    gts = compute_gts_modifier(f)
    qpyi = clamp(qpyi + gts["score_adjustment"])
    gts_score_adj = gts["score_adjustment"]
    gts_ip_cap_adj = gts["ip_cap_adjustment"]
    if gts_score_adj != 0.0 and not silent:
        log.info("QPYI GTS modifier applied",
                 qb=f.player_name,
                 game_total=getattr(f, "game_total", None),
                 score_adjustment=gts_score_adj)

    grade = qpyi_grade(qpyi)

    # ── Projection
    # projected_yards = base_yards × (1 − 0.21 × ((QPYI − 50) / 50))
    # Then: × PDR rest multiplier × park/turf multiplier. Capped 0–500.
    base_yards = f.blended_yards_per_game
    projected_yards = base_yards * (1.0 - 0.21 * ((qpyi - 50.0) / 50.0))

    rest_mult = get_pdr_rest_multiplier(_f(f.pdr_rest))
    projected_yards *= rest_mult

    park_mult = get_park_turf_multiplier(_f(f.ens_dome), _f(f.ens_turf))
    projected_yards *= park_mult

    projected_yards = max(0.0, min(500.0, projected_yards))

    if not silent:
        log.info("QPYI multipliers",
                 qb=f.player_name,
                 rest_mult=rest_mult,
                 park_mult=park_mult,
                 projected_yards=round(projected_yards, 1))

    if not silent:
        log.info("QPYI result",
                 qb=f.player_name,
                 qpyi_base=round(qpyi_base, 2),
                 interaction=round(interaction, 2),
                 volatility=round(volatility, 2),
                 qpyi=round(qpyi, 2),
                 grade=grade,
                 projected_yards=round(projected_yards, 1))

    return {
        # ── Scores
        "qpyi_base":        round(qpyi_base, 2),
        "qpyi_interaction": round(interaction, 2),
        "qpyi_volatility":  round(volatility, 2),
        "qpyi":             round(qpyi, 2),
        "grade":            grade,
        "gts_score_adj":    gts_score_adj,
        # ip_cap_adjustment is informational only — not yet wired to expected_ip(). Wire in a follow-up block.
        "gts_ip_cap_adj":   gts_ip_cap_adj,
        # ── Projection
        "base_yards":       round(base_yards, 1),
        "projected_yards":  round(projected_yards, 1),
        # ── Block scores (for DB logging)
        "osw": round(osw, 2),
        "qsr": round(qsr, 2),
        "gsp": round(gsp, 2),
        "scb": round(scb, 2),
        "pdr": round(pdr, 2),
        "ens": round(ens, 2),
        "dsr": round(dsr, 2),
        "rct": round(rct, 2),
    }
