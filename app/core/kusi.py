"""
KUSI — Strikeouts Under Score Index
Scoring engine for pitcher strikeout under props.

Formula implemented exactly as specified:

  KUSI_base = 0.28*OCR + 0.22*PMR + 0.18*PER + 0.14*KOP + 0.10*UKS + 0.08*TLR

  OCR = 0.22*K + 0.20*CON + 0.16*ZCON + 0.14*DISC + 0.12*2S + 0.10*FOUL + 0.06*DEC
  PMR = 0.22*P1 + 0.16*P2 + 0.18*PUT + 0.16*RUN + 0.16*TOP6 + 0.12*PLAT
  PER = 0.22*PPA + 0.20*BB + 0.16*FPS + 0.14*DEEP + 0.12*PUTW + 0.10*CMDD + 0.06*VELO
  KOP = 0.24*PCAP + 0.18*HOOK + 0.16*TTO + 0.12*BPEN + 0.12*PAT + 0.10*INJ + 0.08*FAT
  UKS = 0.34*TIGHT + 0.26*CSTRL + 0.22*2EXP + 0.18*COUNT
  TLR = 0.35*TOP4K + 0.30*TOP6C + 0.20*VET + 0.15*TOP2

Interaction boosts (capped at 7.0):
  K1: OCR > 70 and PER_PPA > 65  → +2.0
  K2: PMR_PUT > 70 and relies on one putaway pitch  → +1.5
  K3: lineup discipline > 65 and PER_FPS > 60  → +1.5
  K4: KOP_HOOK > 70 and KOP_BPEN > 70  → +1.5
  K5: TLR_TOP4K > 70 and sportsbook K line >= pitcher median + 1.0  → +2.0
  K6: KOP_TTO > 70 and OCR_FOUL > 65  → +1.0
  K7: UKS > 65 and weak edge-command profile  → +1.0

Volatility penalties (capped at 8.5):
  KV1: lineup uncertainty              -2.5
  KV2: umpire unknown                  -1.0
  KV3: pitcher stuff volatility high   -2.0
  KV4: recent velocity spike           -1.5
  KV5: key contact bats resting unc.   -2.0
  KV6: bullpen depleted                -1.5
  KV7: rain/weather timing unc.        -1.5
  KV8: opponent boom-bust K volatility -1.5

Final: KUSI = clamp(KUSI_base + interaction - volatility, 0, 100)
Projected Ks: projected_ks = base_ks * (1 - 0.25 * ((KUSI - 50) / 50))
"""

from app.core.features import PitcherFeatureSet
from app.utils.normalization import clamp
from app.utils.ip_window import expected_ip, ip_tier_label
from app.utils.mgs import compute_mgs
from app.utils.logging import get_logger

log = get_logger("kusi_engine")

NEUTRAL = 50.0


def _f(val, fallback: float = NEUTRAL) -> float:
    return val if val is not None else fallback


# ─────────────────────────────────────────────────────────────
# Block scorers
# ─────────────────────────────────────────────────────────────

def score_ocr(f: PitcherFeatureSet) -> float:
    """OCR = Opponent Contact Rate block (0-100)."""
    k_    = _f(f.ocr_k)
    con   = _f(f.ocr_con)
    zcon  = _f(f.ocr_zcon)
    disc  = _f(f.ocr_disc)
    s2    = _f(f.ocr_2s)
    foul  = _f(f.ocr_foul)
    dec   = _f(f.ocr_dec)

    ocr = (
        0.22 * k_   +
        0.20 * con  +
        0.16 * zcon +
        0.14 * disc +
        0.12 * s2   +
        0.10 * foul +
        0.06 * dec
    )
    log.debug("KUSI OCR", pitcher=f.pitcher_name,
              k=k_, con=con, zcon=zcon, disc=disc, s2=s2, foul=foul, dec=dec,
              ocr=round(ocr, 2))
    return clamp(ocr)


def score_pmr(f: PitcherFeatureSet) -> float:
    """PMR = Pitch Mix Rating block (0-100)."""
    p1   = _f(f.pmr_p1)
    p2   = _f(f.pmr_p2)
    put  = _f(f.pmr_put)
    run  = _f(f.pmr_run)
    top6 = _f(f.pmr_top6)
    plat = _f(f.pmr_plat)

    pmr = (
        0.22 * p1   +
        0.16 * p2   +
        0.18 * put  +
        0.16 * run  +
        0.16 * top6 +
        0.12 * plat
    )
    log.debug("KUSI PMR", pitcher=f.pitcher_name,
              p1=p1, p2=p2, put=put, run=run, top6=top6, plat=plat, pmr=round(pmr, 2))
    return clamp(pmr)


def score_per(f: PitcherFeatureSet) -> float:
    """PER = Pitcher Efficiency Rating block (0-100)."""
    ppa  = _f(f.per_ppa)
    bb   = _f(f.per_bb)
    fps  = _f(f.per_fps)
    deep = _f(f.per_deep)
    putw = _f(f.per_putw)
    cmdd = _f(f.per_cmdd)
    velo = _f(f.per_velo)

    per = (
        0.22 * ppa  +
        0.20 * bb   +
        0.16 * fps  +
        0.14 * deep +
        0.12 * putw +
        0.10 * cmdd +
        0.06 * velo
    )
    log.debug("KUSI PER", pitcher=f.pitcher_name,
              ppa=ppa, bb=bb, fps=fps, deep=deep, putw=putw, cmdd=cmdd, velo=velo,
              per=round(per, 2))
    return clamp(per)


def score_kop(f: PitcherFeatureSet) -> float:
    """KOP = K-Operational Profile block (0-100)."""
    pcap = _f(f.kop_pcap)
    hook = _f(f.kop_hook)
    tto  = _f(f.kop_tto)
    bpen = _f(f.kop_bpen)
    pat  = _f(f.kop_pat)
    inj  = _f(f.kop_inj)
    fat  = _f(f.kop_fat)

    kop = (
        0.24 * pcap +
        0.18 * hook +
        0.16 * tto  +
        0.12 * bpen +
        0.12 * pat  +
        0.10 * inj  +
        0.08 * fat
    )
    log.debug("KUSI KOP", pitcher=f.pitcher_name,
              pcap=pcap, hook=hook, tto=tto, bpen=bpen, pat=pat, inj=inj, fat=fat,
              kop=round(kop, 2))
    return clamp(kop)


def score_uks(f: PitcherFeatureSet) -> float:
    """UKS = Umpire K Score block (0-100)."""
    tight = _f(f.uks_tight)
    cstrl = _f(f.uks_cstrl)
    exp2  = _f(f.uks_2exp)
    count = _f(f.uks_count)

    uks = (
        0.34 * tight +
        0.26 * cstrl +
        0.22 * exp2  +
        0.18 * count
    )
    log.debug("KUSI UKS", pitcher=f.pitcher_name,
              tight=tight, cstrl=cstrl, exp2=exp2, count=count, uks=round(uks, 2))
    return clamp(uks)


def score_tlr(f: PitcherFeatureSet) -> float:
    """TLR = Top-Lineup Resistance block (0-100)."""
    top4k = _f(f.tlr_top4k)
    top6c = _f(f.tlr_top6c)
    vet   = _f(f.tlr_vet)
    top2  = _f(f.tlr_top2)

    tlr = (
        0.35 * top4k +
        0.30 * top6c +
        0.20 * vet   +
        0.15 * top2
    )
    log.debug("KUSI TLR", pitcher=f.pitcher_name,
              top4k=top4k, top6c=top6c, vet=vet, top2=top2, tlr=round(tlr, 2))
    return clamp(tlr)


# ─────────────────────────────────────────────────────────────
# Interaction boosts
# ─────────────────────────────────────────────────────────────

def compute_kusi_interaction(
    f: PitcherFeatureSet,
    ocr: float,
    per_ppa: float,
    pmr_put: float,
    per_fps: float,
    kop_hook: float,
    kop_bpen: float,
    tlr_top4k: float,
    kop_tto: float,
    ocr_foul: float,
    uks: float,
    uhs_zone: float = 50.0,
    pcs_cmd: float = 50.0,
    per_deep: float = 50.0,
    ocr_disc: float = 50.0,
    silent: bool = False,
) -> float:
    """
    Apply KUSI interaction boosts. Returns total boost (capped at 9.0).
    Each triggered rule is logged individually.

    Merlin v2.0 additions:
      Zone Sympathy: UHS_ZONE > 70 AND PCS_CMD > 70 → +4.0 (interactive, not additive)
      K8 Swing-and-Miss Collision: PER_DEEP > 75 AND OCR_DISC < 40 → +4.0 boost
    """
    boost = 0.0
    name = f.pitcher_name

    if ocr > 70 and per_ppa > 65:
        boost += 2.0
        if not silent:
            log.info("KUSI K1 triggered", pitcher=name, ocr=ocr, per_ppa=per_ppa, boost=2.0)

    if pmr_put > 70 and f.relies_on_one_putaway:
        boost += 1.5
        if not silent:
            log.info("KUSI K2 triggered", pitcher=name, pmr_put=pmr_put, boost=1.5)

    lineup_disc = _f(f.lineup_discipline_score)
    if lineup_disc > 65 and per_fps > 60:
        boost += 1.5
        if not silent:
            log.info("KUSI K3 triggered", pitcher=name, lineup_disc=lineup_disc, per_fps=per_fps, boost=1.5)

    if kop_hook > 70 and kop_bpen > 70:
        boost += 1.5
        if not silent:
            log.info("KUSI K4 triggered", pitcher=name, kop_hook=kop_hook, kop_bpen=kop_bpen, boost=1.5)

    # K5: top-lineup K ability + sportsbook line is above pitcher median K by >= 1.0
    if (
        tlr_top4k > 70
        and f.k_line is not None
        and f.pitcher_median_ks is not None
        and f.k_line >= f.pitcher_median_ks + 1.0
    ):
        boost += 2.0
        if not silent:
            log.info("KUSI K5 triggered", pitcher=name,
                     tlr_top4k=tlr_top4k, k_line=f.k_line, pitcher_median_ks=f.pitcher_median_ks, boost=2.0)

    if kop_tto > 70 and ocr_foul > 65:
        boost += 1.0
        if not silent:
            log.info("KUSI K6 triggered", pitcher=name, kop_tto=kop_tto, ocr_foul=ocr_foul, boost=1.0)

    if uks > 65 and f.weak_edge_command:
        boost += 1.0
        if not silent:
            log.info("KUSI K7 triggered", pitcher=name, uks=uks, weak_edge_command=f.weak_edge_command, boost=1.0)

    # ── Zone Sympathy (Merlin v2.0): same rule as HUSI — umpire zone + pitcher command
    # are interactive. When both > 70, the umpire is expanding the zone exactly where
    # the pitcher commands best → more called Ks.
    if uhs_zone > 70 and pcs_cmd > 70:
        boost += 4.0
        if not silent:
            log.info("KUSI Zone Sympathy triggered", pitcher=name,
                     uhs_zone=uhs_zone, pcs_cmd=pcs_cmd, boost=4.0)

    # ── K8 Swing-and-Miss Collision (Merlin v2.0):
    # A pitcher who goes deep into games (PER_DEEP > 75) against a lineup with
    # poor plate discipline (OCR_DISC < 40) creates a "kill streak" environment.
    # Batters chase late, the pitcher is comfortable in counts, and K totals surge.
    if per_deep > 75 and ocr_disc < 40:
        boost += 4.0
        if not silent:
            log.info("KUSI K8 Swing-and-Miss Collision triggered", pitcher=name,
                     per_deep=per_deep, ocr_disc=ocr_disc, boost=4.0)

    capped = min(boost, 9.0)  # raised cap for new Merlin rules
    if not silent:
        log.info("KUSI interaction total", pitcher=name, raw_boost=boost, capped=capped)
    return capped


# ─────────────────────────────────────────────────────────────
# Volatility penalties
# ─────────────────────────────────────────────────────────────

def compute_kusi_volatility(f: PitcherFeatureSet, silent: bool = False) -> float:
    """
    Apply KUSI volatility penalties. Returns total penalty (capped at 8.5).
    Each triggered penalty is logged individually.
    """
    penalty = 0.0
    name = f.pitcher_name

    if not f.lineup_confirmed:
        penalty += 2.5
        if not silent:
            log.info("KUSI KV1 lineup uncertainty", pitcher=name, penalty=2.5)

    if not f.umpire_confirmed:
        penalty += 1.0
        if not silent:
            log.info("KUSI KV2 umpire unknown", pitcher=name, penalty=1.0)

    if f.per_velo is not None and f.per_velo < 40:
        penalty += 2.0
        if not silent:
            log.info("KUSI KV3 stuff volatility", pitcher=name, per_velo=f.per_velo, penalty=2.0)

    if f.recent_velocity_spike:
        penalty += 1.5
        if not silent:
            log.info("KUSI KV4 velocity spike upside uncertainty", pitcher=name, penalty=1.5)

    if f.key_contact_bats_uncertain:
        penalty += 2.0
        if not silent:
            log.info("KUSI KV5 key contact bats resting", pitcher=name, penalty=2.0)

    if f.kop_bpen is not None and f.kop_bpen < 35:
        penalty += 1.5
        if not silent:
            log.info("KUSI KV6 bullpen depleted", pitcher=name, kop_bpen=f.kop_bpen, penalty=1.5)

    if f.ens_windin is not None and f.ens_windin < 35:
        penalty += 1.5
        if not silent:
            log.info("KUSI KV7 rain/weather timing uncertainty", pitcher=name, ens_windin=f.ens_windin, penalty=1.5)

    if f.opponent_boom_bust_volatility:
        penalty += 1.5
        if not silent:
            log.info("KUSI KV8 opponent boom-bust K volatility", pitcher=name, penalty=1.5)

    capped = min(penalty, 8.5)
    if not silent:
        log.info("KUSI volatility total", pitcher=name, raw_penalty=penalty, capped=capped)
    return capped


# ─────────────────────────────────────────────────────────────
# Grade
# ─────────────────────────────────────────────────────────────

def kusi_grade(score: float) -> str:
    """
    Grade thresholds are calibrated to the current data reality.
    Many features (prop lines, umpire, bullpen) default to neutral (50)
    when unavailable, compressing scores toward 50. As data matures
    and more sources are live, these thresholds will move upward.
    """
    if score >= 62:
        return "A+"
    elif score >= 57:
        return "A"
    elif score >= 52:
        return "B"
    elif score >= 47:
        return "C"
    else:
        return "D"


# ─────────────────────────────────────────────────────────────
# Main KUSI computation
# ─────────────────────────────────────────────────────────────

def compute_kusi(f: PitcherFeatureSet, silent: bool = False) -> dict:
    """
    Compute the full KUSI score for one pitcher on one game day.

    Args:
        f:      Pitcher feature set.
        silent: If True, suppress all logging. Used by SimulationEngine for 2000-iteration speed.

    Returns a dict with:
      kusi_base, kusi_interaction, kusi_volatility, kusi, grade,
      projected_ks, base_ks,
      all individual block scores for database logging.
    """
    if not silent:
        log.info("KUSI computation starting", pitcher=f.pitcher_name, game_id=f.game_id)

    # ── Block scores
    ocr = score_ocr(f)
    pmr = score_pmr(f)
    per = score_per(f)
    kop = score_kop(f)
    uks = score_uks(f)
    tlr = score_tlr(f)

    if not silent:
        log.info("KUSI block scores",
                 pitcher=f.pitcher_name,
                 OCR=round(ocr, 2), PMR=round(pmr, 2), PER=round(per, 2),
                 KOP=round(kop, 2), UKS=round(uks, 2), TLR=round(tlr, 2))

    # ── Base formula
    kusi_base = (
        0.28 * ocr +
        0.22 * pmr +
        0.18 * per +
        0.14 * kop +
        0.10 * uks +
        0.08 * tlr
    )
    if not silent:
        log.info("KUSI base", pitcher=f.pitcher_name, kusi_base=round(kusi_base, 2))

    # ── Interaction boosts (includes Zone Sympathy and K8 from Merlin v2.0)
    interaction = compute_kusi_interaction(
        f=f,
        ocr=ocr,
        per_ppa=_f(f.per_ppa),
        pmr_put=_f(f.pmr_put),
        per_fps=_f(f.per_fps),
        kop_hook=_f(f.kop_hook),
        kop_bpen=_f(f.kop_bpen),
        tlr_top4k=_f(f.tlr_top4k),
        kop_tto=_f(f.kop_tto),
        ocr_foul=_f(f.ocr_foul),
        uks=uks,
        uhs_zone=_f(f.uhs_zone),
        pcs_cmd=_f(f.pcs_cmd),
        per_deep=_f(f.per_deep),
        ocr_disc=_f(f.ocr_disc),
        silent=silent,
    )

    # ── Volatility penalties
    volatility = compute_kusi_volatility(f, silent=silent)

    # ── Final KUSI (pre-bullpen)
    kusi_raw = kusi_base + interaction - volatility
    kusi_pre = clamp(kusi_raw)

    # ── Bullpen Fatigue Adjustment
    from app.utils.bullpen import apply_bullpen_to_kusi
    kusi = apply_bullpen_to_kusi(kusi_pre, f.bullpen_fatigue_own)
    bullpen_adjustment = round(kusi - kusi_pre, 2)

    # ── Catcher Framing adjustment (SKU #37)
    kusi_pre_framing = kusi
    if f.catcher_kusi_adj != 0.0:
        kusi = kusi * (1.0 + f.catcher_kusi_adj)
        kusi = round(max(0.0, min(kusi, 100.0)), 2)
        if not silent:
            log.info("KUSI catcher framing adjustment applied",
                     pitcher=f.pitcher_name,
                     catcher=f.catcher_name,
                     strike_rate=f.catcher_strike_rate,
                     framing_label=f.catcher_framing_label,
                     kusi_adj=f.catcher_kusi_adj,
                     kusi_before=round(kusi_pre_framing, 2),
                     kusi_after=round(kusi, 2))

    # ── Extension / Perceived Velocity boost (SKU #38)
    if f.extension_elite and not silent:
        log.info("KUSI extension elite (perceived velo boost already applied in per_velo)",
                 pitcher=f.pitcher_name,
                 extension_ft=f.extension_ft)

    grade = kusi_grade(kusi)
    if not silent:
        log.info("KUSI final",
                 pitcher=f.pitcher_name,
                 kusi_base=round(kusi_base, 2),
                 interaction=round(interaction, 2),
                 volatility=round(volatility, 2),
                 kusi_pre_bullpen=round(kusi_pre, 2),
                 bullpen_bfs=f.bullpen_fatigue_own,
                 bullpen_label=f.bullpen_label_own,
                 bullpen_adjustment=bullpen_adjustment,
                 catcher=f.catcher_name,
                 catcher_framing=f.catcher_framing_label,
                 kusi=round(kusi, 2),
                 grade=grade)

    # ── Projected strikeouts (MGS-aware)
    exp_ip = expected_ip(f.avg_ip_per_start, f.mlb_service_years)
    safe_k_per_9 = min(f.season_k_per_9 or 8.0, 15.0)
    base_ks = safe_k_per_9 * (exp_ip / 9.0)
    projected_ks = base_ks * (1 - 0.25 * ((kusi - 50) / 50))

    # MGS adjustment — TTO, pitch-count fatigue, and PFF all suppress Ks in late innings
    _, mgs_ks_mult, mgs_label = compute_mgs(
        exp_ip,
        current_inning=f.mgs_inning,
        current_pitch_count=f.mgs_pitch_count,
        pff_hits_tto1_mult=f.pff_hits_tto1_mult,
        pff_ks_tto1_mult=f.pff_ks_tto1_mult,
        pff_tto_late_boost=f.pff_tto_late_boost,
        pff_label=f.pff_label,
        baserunners_l2=f.baserunners_l2_innings,
        silent=silent,
    )
    projected_ks = projected_ks * mgs_ks_mult
    projected_ks = max(0.0, min(projected_ks, 15.0))

    if not silent:
        log.info("KUSI projection",
                 pitcher=f.pitcher_name,
                 exp_ip=exp_ip,
                 ip_tier=ip_tier_label(exp_ip),
                 base_ks=round(base_ks, 2),
                 mgs_ks_mult=round(mgs_ks_mult, 3),
                 mgs_label=mgs_label,
                 projected_ks=round(projected_ks, 2))

    return {
        "kusi": round(kusi, 2),
        "kusi_base": round(kusi_base, 2),
        "kusi_interaction": round(interaction, 2),
        "kusi_volatility": round(volatility, 2),
        "grade": grade,
        "base_ks": round(base_ks, 2),
        "projected_ks": round(projected_ks, 2),
        "mgs_ks_mult": round(mgs_ks_mult, 4),
        "mgs_label": mgs_label,
        # Block scores for DB storage
        "ocr_score": round(ocr, 2),
        "pmr_score": round(pmr, 2),
        "per_score": round(per, 2),
        "kop_score": round(kop, 2),
        "uks_score": round(uks, 2),
        "tlr_score": round(tlr, 2),
    }
