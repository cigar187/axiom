"""
QTDI — QB Touchdown Index
Scoring engine for NFL quarterback touchdown props.

Mirrors KUSI architecture exactly. Projects touchdowns for one QB in one game.

Formula:
  QTDI_base = 0.24×ORD + 0.20×QTR + 0.15×GSP_TD + 0.12×SCB_TD
            + 0.10×PDR + 0.07×DSR + 0.07×ENS + 0.05×RCT

  ORD (Opponent Red Zone Defense) — 24%:
    Replaces OSW. Targets red zone defense specifically.
    Sub-weights PENDING — awaiting piece from user
    Inputs: ord_rz_yards, ord_rz_td_rate, ord_gl_stand, ord_short_yds, ord_rz_dvoa

  QTR (QB Touchdown Rate Block) — 20%:
    Replaces QSR. TD-specific efficiency rather than overall accuracy.
    Sub-weights PENDING — awaiting piece from user
    Inputs: qtr_td_per_rz, qtr_pa_td_rate, qtr_sneak, qtr_q4_td,
            qtr_third_conv, qtr_gl_carry

  GSP_TD (Game Script Profile — TD version) — 15%:
    PENDING — awaiting detail piece from user

  SCB_TD (Supporting Cast Block — TD version) — 12%:
    PENDING — awaiting detail piece from user

  PDR (Physical Durability Rating) — 10%:
    Identical to QPYI. Same inputs, same weights within block.

  DSR (Defensive Scheme Rating) — 7%:
    Identical to QPYI. Same inputs, same weights within block.

  ENS (Environmental) — 7%:
    Same inputs as QPYI. Block weight shifts from 9% to 7%.

  RCT (Referee Crew Tendencies) — 5%:
    Identical to QPYI. Same inputs, same weights within block.

Interaction boosts:
  PENDING — awaiting piece from user

Volatility penalties:
  PENDING — awaiting piece from user

Post-formula adjustments:
  PDR rest multiplier  — PENDING
  Red zone opportunity rate multiplier — PENDING

Final: QTDI = clamp(QTDI_base + interaction - volatility, 0, 100)
Projected TDs: PENDING — awaiting projection formula piece from user
"""

import statistics

from app.core.nfl.features import QBFeatureSet
from app.core.nfl.qpyi import score_pdr, score_dsr, score_ens, score_rct
from app.utils.normalization import clamp
from app.utils.logging import get_logger

log = get_logger("qtdi_engine")

NEUTRAL = 50.0


def _f(val, fallback: float = NEUTRAL) -> float:
    """Return val if not None, else fallback. Mirrors _f() in husi.py / kusi.py."""
    return val if val is not None else fallback


# ─────────────────────────────────────────────────────────────
# Block scorers — QTDI-specific
# ─────────────────────────────────────────────────────────────

def score_ord(f: QBFeatureSet) -> float:
    """
    ORD = Opponent Red Zone Defense (0–100).
    Replaces OSW for QTDI. High score = leaky red zone defense = favors TD production.

    Sources:
      ord_rz_dvoa  → Aaron Schatz (Football Outsiders — DVOA red zone splits)
      All others   → NFL Stats / Dean Oliver (ESPN QBR)

    Sub-weights: PENDING — awaiting formula piece from user.
    Placeholder equal weights used until sub-formula is received.
    """
    rz_dvoa    = _f(f.ord_rz_dvoa)
    td_rate    = _f(f.ord_td_rate)
    gl_stop    = _f(f.ord_goal_line_stop_rate)
    rz_yards   = _f(f.ord_rz_yards_allowed)
    short_rank = _f(f.ord_short_yardage_rank)

    ord_ = (
        0.30 * rz_dvoa    +
        0.28 * td_rate    +
        0.20 * gl_stop    +
        0.14 * rz_yards   +
        0.08 * short_rank
    )
    log.debug("QTDI ORD", qb=f.player_name,
              rz_dvoa=rz_dvoa, td_rate=td_rate, gl_stop=gl_stop,
              rz_yards=rz_yards, short_rank=short_rank, ord_=round(ord_, 2))
    return clamp(ord_)


def score_qtr(f: QBFeatureSet) -> float:
    """
    QTR = QB Touchdown Rate Block (0–100).
    Replaces QSR for QTDI. High score = elite TD conversion efficiency this week.

    Sources:
      qtr_pa_td_rate → nflfastR / Ben Baldwin
      All others     → NFL Stats / Dean Oliver (ESPN QBR)

    Sub-weights: PENDING — awaiting formula piece from user.
    Placeholder equal weights used until sub-formula is received.
    """
    td_per_rz   = _f(f.qtr_td_rate_per_rz_trip)
    pa_td_rate  = _f(f.qtr_pa_td_rate)
    q4_clutch   = _f(f.qtr_q4_clutch_td_rate)
    third_conv  = _f(f.qtr_third_down_conv_rate)
    sneak       = _f(f.qtr_sneak_tendency)
    gl_carry    = _f(f.qtr_goal_line_carry_rate)

    qtr = (
        0.30 * td_per_rz  +
        0.20 * pa_td_rate +
        0.18 * q4_clutch  +
        0.16 * third_conv +
        0.10 * sneak      +
        0.06 * gl_carry
    )
    log.debug("QTDI QTR", qb=f.player_name,
              td_per_rz=td_per_rz, pa_td_rate=pa_td_rate, q4_clutch=q4_clutch,
              third_conv=third_conv, sneak=sneak, gl_carry=gl_carry,
              qtr=round(qtr, 2))
    return clamp(qtr)


def score_gsp_td(f: QBFeatureSet) -> float:
    """
    GSP_TD = Game Script Profile — TD version (0–100).
    Same 7 GSP inputs as QPYI but weighted toward TD opportunity context.
    Red zone trip rate leads because TD volume is driven by red zone access.

    gsp_rz:       0.30  — red zone trip rate dominates TD opportunity
    gsp_spread:   0.25  — trailing teams score TDs to close gaps
    gsp_total:    0.20  — high-total games produce more TD volume
    gsp_pcall:    0.12  — pass-heavy OC creates more scoring opportunities
    gsp_snaps:    0.08  — more snaps = more chances to score
    gsp_pace:     0.03  — pace matters less for TDs than raw opportunities
    gsp_oc_trend: 0.02  — hot OC streak adds marginal TD edge
    """
    rz       = _f(f.gsp_rz)
    spread   = _f(f.gsp_spread)
    total    = _f(f.gsp_total)
    pcall    = _f(f.gsp_pcall)
    snaps    = _f(f.gsp_snaps)
    pace     = _f(f.gsp_pace)
    oc_trend = _f(f.gsp_oc_trend)

    gsp_td = (
        0.30 * rz       +
        0.25 * spread   +
        0.20 * total    +
        0.12 * pcall    +
        0.08 * snaps    +
        0.03 * pace     +
        0.02 * oc_trend
    )
    log.debug("QTDI GSP_TD", qb=f.player_name,
              rz=rz, spread=spread, total=total, pcall=pcall,
              snaps=snaps, pace=pace, oc_trend=oc_trend,
              gsp_td=round(gsp_td, 2))
    return clamp(gsp_td)


def score_scb_td(f: QBFeatureSet) -> float:
    """
    SCB_TD = Supporting Cast Block — TD version (0–100).
    Same 6 SCB inputs as QPYI but weighted toward red zone supporting cast.
    TE quality leads because TEs are the primary red zone target.

    scb_te:   0.32  — TE is the primary red zone threat
    scb_sep:  0.22  — open receivers convert TD opportunities
    scb_pblk: 0.18  — O-line must hold long enough to score
    scb_inj:  0.14  — healthy red zone targets are critical
    scb_yac:  0.08  — YAC matters less in tight red zone windows
    scb_ryoe: 0.06  — run game keeps defense honest at the goal line
    """
    te   = _f(f.scb_te)
    sep  = _f(f.scb_sep)
    pblk = _f(f.scb_pblk)
    inj  = _f(f.scb_inj)
    yac  = _f(f.scb_yac)
    ryoe = _f(f.scb_ryoe)

    scb_td = (
        0.32 * te   +
        0.22 * sep  +
        0.18 * pblk +
        0.14 * inj  +
        0.08 * yac  +
        0.06 * ryoe
    )
    log.debug("QTDI SCB_TD", qb=f.player_name,
              te=te, sep=sep, pblk=pblk, inj=inj, yac=yac, ryoe=ryoe,
              scb_td=round(scb_td, 2))
    return clamp(scb_td)


# ─────────────────────────────────────────────────────────────
# Interaction boosts
# ─────────────────────────────────────────────────────────────

def compute_qtdi_interaction(f: QBFeatureSet, **block_scores) -> float:
    """
    Apply QTDI interaction boosts. Returns total boost capped at [-5.0, +5.0].

    Positive boosts fire when multiple blocks compound each other's TD strength.
    Negative boost fires when physical vulnerability meets a strong red zone defense.

    Rules:
      T1: ord >= 65 AND gsp_td >= 65   → +2.5  leaky RZ defense + pass-heavy script = elite TD environment
      T2: qtr >= 65 AND scb_td >= 65   → +2.0  elite TD efficiency + elite RZ supporting cast
      T3: ord >= 65 AND qtr >= 65      → +2.0  weak RZ defense facing QB who converts trips at elite rate
      T4: dsr >= 65 AND qtr >= 65      → +1.5  QB exploits the specific scheme best in scoring situations
      T5: pdr <= 35 AND ord <= 40      → -2.0  banged-up QB facing strong RZ defense (compounding downside)
    """
    ord_  = block_scores.get("ord_",   NEUTRAL)
    qtr   = block_scores.get("qtr",    NEUTRAL)
    gsp_td = block_scores.get("gsp_td", NEUTRAL)
    scb_td = block_scores.get("scb_td", NEUTRAL)
    pdr   = block_scores.get("pdr",    NEUTRAL)
    dsr   = block_scores.get("dsr",    NEUTRAL)

    boost = 0.0
    name  = f.player_name

    if ord_ >= 65 and gsp_td >= 65:
        boost += 2.5
        log.info("QTDI T1 triggered", qb=name, ord_=ord_, gsp_td=gsp_td, boost=2.5)

    if qtr >= 65 and scb_td >= 65:
        boost += 2.0
        log.info("QTDI T2 triggered", qb=name, qtr=qtr, scb_td=scb_td, boost=2.0)

    if ord_ >= 65 and qtr >= 65:
        boost += 2.0
        log.info("QTDI T3 triggered", qb=name, ord_=ord_, qtr=qtr, boost=2.0)

    if dsr >= 65 and qtr >= 65:
        boost += 1.5
        log.info("QTDI T4 triggered", qb=name, dsr=dsr, qtr=qtr, boost=1.5)

    if pdr <= 35 and ord_ <= 40:
        boost -= 2.0
        log.info("QTDI T5 triggered", qb=name, pdr=pdr, ord_=ord_, penalty=-2.0)

    return max(-5.0, min(5.0, boost))


# ─────────────────────────────────────────────────────────────
# Volatility penalties
# ─────────────────────────────────────────────────────────────

def compute_qtdi_volatility(f: QBFeatureSet, **block_scores) -> float:
    """
    Apply QTDI volatility penalties. Returns total penalty in [-8.0, 0.0].
    This value is ADDED to the score in compute_qtdi() — negative means reduction.

    Rules:
      TV1: any block score < 30               → -1.5 per block  (extreme weakness = TD downside risk)
      TV2: pdr < 40 AND qtr < 50              → -2.0            (hurt QB who also converts TDs inefficiently)
      TV3: ord > 65 AND scb_td < 40           → -1.5            (good TD environment but weak RZ cast cancels it)
      TV4: stdev(all 8 blocks) > 20           → -1.0            (uneven profile = TD projection uncertainty)
    """
    ord_   = block_scores.get("ord_",   NEUTRAL)
    qtr    = block_scores.get("qtr",    NEUTRAL)
    gsp_td = block_scores.get("gsp_td", NEUTRAL)
    scb_td = block_scores.get("scb_td", NEUTRAL)
    pdr    = block_scores.get("pdr",    NEUTRAL)
    dsr    = block_scores.get("dsr",    NEUTRAL)
    ens    = block_scores.get("ens",    NEUTRAL)
    rct    = block_scores.get("rct",    NEUTRAL)

    blocks = [ord_, qtr, gsp_td, scb_td, pdr, dsr, ens, rct]
    labels = ["ord", "qtr", "gsp_td", "scb_td", "pdr", "dsr", "ens", "rct"]
    penalty = 0.0
    name    = f.player_name

    # TV1: extreme weakness in any single block
    for label, score in zip(labels, blocks):
        if score < 30:
            penalty -= 1.5
            log.info("QTDI TV1 triggered", qb=name, block=label, score=score, penalty=-1.5)

    # TV2: hurt QB + inefficient TD conversion
    if pdr < 40 and qtr < 50:
        penalty -= 2.0
        log.info("QTDI TV2 triggered", qb=name, pdr=pdr, qtr=qtr, penalty=-2.0)

    # TV3: good TD environment cancelled by weak red zone cast
    if ord_ > 65 and scb_td < 40:
        penalty -= 1.5
        log.info("QTDI TV3 triggered", qb=name, ord_=ord_, scb_td=scb_td, penalty=-1.5)

    # TV4: highly uneven profile
    if statistics.stdev(blocks) > 20:
        penalty -= 1.0
        log.info("QTDI TV4 triggered", qb=name, stdev=round(statistics.stdev(blocks), 2), penalty=-1.0)

    return max(-8.0, min(0.0, penalty))


# ─────────────────────────────────────────────────────────────
# Grading
# ─────────────────────────────────────────────────────────────

def qtdi_grade(score: float) -> str:
    """
    Grade thresholds anchored to real NFL QB TD distributions.
    Mirrors kusi_grade() pattern — TD thresholds start lower than QPYI yards
    thresholds because touchdowns are harder to project with certainty.
    Same logic as KUSI grading lower than HUSI in the MLB formula.

      A+  ≥ 58  — Elite TD matchup, leaky red zone D, pass-heavy script  (top ~10%)
      A   ≥ 52  — Very favorable — multiple TDs likely                    (top ~25%)
      B   ≥ 46  — Solid — 1–2 TDs realistic                              (top ~45%)
      C   ≥ 40  — Neutral — 1 TD or none, coin flip                      (middle ~30%)
      D   < 40  — Unfavorable — strong red zone D, low-scoring game       (bottom ~25%)
    """
    if score >= 58:
        return "A+"
    elif score >= 52:
        return "A"
    elif score >= 46:
        return "B"
    elif score >= 40:
        return "C"
    else:
        return "D"


# ─────────────────────────────────────────────────────────────
# Main QTDI computation
# ─────────────────────────────────────────────────────────────

def compute_qtdi(f: QBFeatureSet, silent: bool = False) -> dict:
    """
    Compute the full QTDI score for one QB on one game day.

    Args:
        f:      QB feature set.
        silent: If True, suppress all logging (used by simulation engine).

    Returns a dict with:
      qtdi_base, qtdi_interaction, qtdi_volatility, qtdi, grade,
      projected_tds, base_tds,
      all individual block scores for database logging.

    NOTE: PDR rest multiplier derivation PENDING — awaiting piece from user.
    """
    if not silent:
        log.info("QTDI computation starting", qb=f.player_name, game_id=f.game_id)

    # ── Score QTDI-specific blocks
    ord_  = score_ord(f)
    qtr   = score_qtr(f)
    gsp_td = score_gsp_td(f)    # PENDING
    scb_td = score_scb_td(f)    # PENDING

    # ── Shared blocks (re-scored independently — same inputs, QTDI weights)
    pdr = score_pdr(f)
    dsr = score_dsr(f)
    ens = score_ens(f)
    rct = score_rct(f)

    # ── Top-level formula
    qtdi_base = (
        0.24 * ord_   +
        0.20 * qtr    +
        0.15 * gsp_td +
        0.12 * scb_td +
        0.10 * pdr    +
        0.07 * dsr    +
        0.07 * ens    +
        0.05 * rct
    )

    # ── Interactions and volatility (PENDING)
    interaction = compute_qtdi_interaction(f,
        ord_=ord_, qtr=qtr, gsp_td=gsp_td, scb_td=scb_td,
        pdr=pdr, dsr=dsr, ens=ens, rct=rct)
    volatility = compute_qtdi_volatility(f,
        ord_=ord_, qtr=qtr, gsp_td=gsp_td, scb_td=scb_td,
        pdr=pdr, dsr=dsr, ens=ens, rct=rct)

    # volatility is 0 or negative — adding it correctly reduces the score
    qtdi_raw = qtdi_base + interaction + volatility
    qtdi = clamp(qtdi_raw)

    grade = qtdi_grade(qtdi)

    # ── Projection
    # projected_tds = base_tds × (1 − 0.25 × ((QTDI − 50) / 50))
    # Then adjusted by PDR rest multiplier. Capped 0–7.
    base_tds = f.blended_tds_per_game
    projected_tds = base_tds * (1.0 - 0.25 * ((qtdi - 50.0) / 50.0))

    # PDR rest multiplier (derivation PENDING — 1.0 until formula piece received)
    projected_tds *= f.pdr_rest_mult

    projected_tds = max(0.0, min(7.0, projected_tds))

    if not silent:
        log.info("QTDI result",
                 qb=f.player_name,
                 qtdi_base=round(qtdi_base, 2),
                 interaction=round(interaction, 2),
                 volatility=round(volatility, 2),
                 qtdi=round(qtdi, 2),
                 grade=grade,
                 projected_tds=round(projected_tds, 2))

    return {
        # ── Scores
        "qtdi_base":        round(qtdi_base, 2),
        "qtdi_interaction": round(interaction, 2),
        "qtdi_volatility":  round(volatility, 2),
        "qtdi":             round(qtdi, 2),
        "grade":            grade,
        # ── Projection
        "base_tds":         round(base_tds, 2),
        "projected_tds":    round(projected_tds, 2),
        # ── Block scores (for DB logging)
        "ord":    round(ord_, 2),
        "qtr":    round(qtr, 2),
        "gsp_td": round(gsp_td, 2),
        "scb_td": round(scb_td, 2),
        "pdr":    round(pdr, 2),
        "dsr":    round(dsr, 2),
        "ens":    round(ens, 2),
        "rct":    round(rct, 2),
    }
