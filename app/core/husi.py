"""
HUSI — Hits Under Score Index
Scoring engine for pitcher hits-allowed under props.

Formula implemented exactly as specified:

  HUSI_base = 0.27*OWC + 0.26*PCS + 0.16*ENS + 0.18*OPS + 0.08*UHS + 0.05*DSC

  OWC = 0.20*BABIP + 0.20*HH + 0.18*BAR + 0.14*LD + 0.14*XBA + 0.08*BOT3 + 0.06*TOPHEAVY
  PCS = 0.18*GB + 0.16*SOFT + 0.16*BARA + 0.14*HHA + 0.12*XBAA + 0.10*XWOBAA + 0.08*CMD + 0.06*REG
  ENS = 0.35*PARK + 0.18*WINDIN + 0.14*TEMP + 0.10*AIR + 0.07*ROOF + 0.10*OF + 0.06*INF
  OPS = 0.24*PCAP + 0.20*HOOK + 0.14*TRAFFIC + 0.14*TTO + 0.10*BPEN + 0.08*INJ + 0.06*TREND + 0.04*FAT
  UHS = 0.34*CSTR + 0.28*ZONE + 0.20*EARLY + 0.18*WEAK
  DSC = 0.40*DEF + 0.20*INFDEF + 0.15*OFDEF + 0.15*CATCH + 0.10*ALIGN

Interaction boosts (capped at 6.5):
  H1: GB > 65 and INFDEF > 65  → +1.5
  H2: fly-ball suppression > 60 and PARK > 70  → +1.5
  H3: WINDIN > 70 and LD > 60  → +1.0
  H4: HOOK > 70 and BPEN > 70  → +2.0
  H5: BOT3 > 70 and TTO > 65   → +1.0
  H6: REG > 65 and HHA > 65    → +1.0
  H7: TOPHEAVY > 70 and projected_batters_faced < 23  → +1.5

Volatility penalties (capped at 8.0):
  HV1: lineup uncertainty       -2.5
  HV2: meaningful weather unc.  -1.5
  HV3: extreme pitcher BABIP var -1.5
  HV4: poor defense             -2.0
  HV5: backup catcher           -1.0
  HV6: umpire unknown           -0.8
  HV7: consistency < 40        -2.0
  HV8: bullpen depleted longer leash -1.2

Final: HUSI = clamp(HUSI_base + interaction - volatility, 0, 100)
Projected hits: projected_hits = base_hits * (1 - 0.21 * ((HUSI - 50) / 50))
"""

from app.core.features import PitcherFeatureSet
from app.utils.normalization import clamp
from app.utils.ip_window import expected_ip, ip_tier_label
from app.utils.mgs import compute_mgs
from app.utils.logging import get_logger

log = get_logger("husi_engine")

NEUTRAL = 50.0  # default score for any missing sub-feature


def _f(val, fallback: float = NEUTRAL) -> float:
    """Return val if it is not None, else fallback."""
    return val if val is not None else fallback


# ─────────────────────────────────────────────────────────────
# Block scorers — each logs its inputs and output
# ─────────────────────────────────────────────────────────────

def score_owc(f: PitcherFeatureSet) -> float:
    """OWC = Opponent Weaknesses vs Contact (0-100)."""
    babip = _f(f.owc_babip)
    hh    = _f(f.owc_hh)
    bar   = _f(f.owc_bar)
    ld    = _f(f.owc_ld)
    xba   = _f(f.owc_xba)
    bot3  = _f(f.owc_bot3)
    th    = _f(f.owc_topheavy)

    owc = (
        0.20 * babip +
        0.20 * hh    +
        0.18 * bar   +
        0.14 * ld    +
        0.14 * xba   +
        0.08 * bot3  +
        0.06 * th
    )
    log.debug("HUSI OWC", pitcher=f.pitcher_name,
              babip=babip, hh=hh, bar=bar, ld=ld, xba=xba, bot3=bot3, topheavy=th, owc=round(owc, 2))
    return clamp(owc)


def score_pcs(f: PitcherFeatureSet) -> float:
    """PCS = Pitcher Contact Suppression (0-100)."""
    gb     = _f(f.pcs_gb)
    soft   = _f(f.pcs_soft)
    bara   = _f(f.pcs_bara)
    hha    = _f(f.pcs_hha)
    xbaa   = _f(f.pcs_xbaa)
    xwobaa = _f(f.pcs_xwobaa)
    cmd    = _f(f.pcs_cmd)
    reg    = _f(f.pcs_reg)

    pcs = (
        0.18 * gb     +
        0.16 * soft   +
        0.16 * bara   +
        0.14 * hha    +
        0.12 * xbaa   +
        0.10 * xwobaa +
        0.08 * cmd    +
        0.06 * reg
    )
    log.debug("HUSI PCS", pitcher=f.pitcher_name,
              gb=gb, soft=soft, bara=bara, hha=hha, xbaa=xbaa,
              xwobaa=xwobaa, cmd=cmd, reg=reg, pcs=round(pcs, 2))
    return clamp(pcs)


def score_ens(f: PitcherFeatureSet) -> float:
    """ENS = Environmental Score (0-100)."""
    park   = _f(f.ens_park)
    windin = _f(f.ens_windin)
    temp   = _f(f.ens_temp)
    air    = _f(f.ens_air)
    roof   = _f(f.ens_roof)
    of_    = _f(f.ens_of)
    inf    = _f(f.ens_inf)

    ens = (
        0.35 * park   +
        0.18 * windin +
        0.14 * temp   +
        0.10 * air    +
        0.07 * roof   +
        0.10 * of_    +
        0.06 * inf
    )
    log.debug("HUSI ENS", pitcher=f.pitcher_name,
              park=park, windin=windin, temp=temp, air=air, roof=roof,
              of_=of_, inf=inf, ens=round(ens, 2))
    return clamp(ens)


def score_ops(f: PitcherFeatureSet) -> float:
    """OPS = Operational Score (0-100)."""
    pcap    = _f(f.ops_pcap)
    hook    = _f(f.ops_hook)
    traffic = _f(f.ops_traffic)
    tto     = _f(f.ops_tto)
    bpen    = _f(f.ops_bpen)
    inj     = _f(f.ops_inj)
    trend   = _f(f.ops_trend)
    fat     = _f(f.ops_fat)

    ops = (
        0.24 * pcap    +
        0.20 * hook    +
        0.14 * traffic +
        0.14 * tto     +
        0.10 * bpen    +
        0.08 * inj     +
        0.06 * trend   +
        0.04 * fat
    )
    log.debug("HUSI OPS", pitcher=f.pitcher_name,
              pcap=pcap, hook=hook, traffic=traffic, tto=tto,
              bpen=bpen, inj=inj, trend=trend, fat=fat, ops=round(ops, 2))
    return clamp(ops)


def score_uhs(f: PitcherFeatureSet) -> float:
    """UHS = Umpire Hits Score (0-100)."""
    cstr  = _f(f.uhs_cstr)
    zone  = _f(f.uhs_zone)
    early = _f(f.uhs_early)
    weak  = _f(f.uhs_weak)

    uhs = (
        0.34 * cstr  +
        0.28 * zone  +
        0.20 * early +
        0.18 * weak
    )
    log.debug("HUSI UHS", pitcher=f.pitcher_name,
              cstr=cstr, zone=zone, early=early, weak=weak, uhs=round(uhs, 2))
    return clamp(uhs)


def score_dsc(f: PitcherFeatureSet) -> float:
    """DSC = Defense Score (0-100)."""
    def_   = _f(f.dsc_def)
    infdef = _f(f.dsc_infdef)
    ofdef  = _f(f.dsc_ofdef)
    catch  = _f(f.dsc_catch)
    align  = _f(f.dsc_align)

    dsc = (
        0.40 * def_   +
        0.20 * infdef +
        0.15 * ofdef  +
        0.15 * catch  +
        0.10 * align
    )
    log.debug("HUSI DSC", pitcher=f.pitcher_name,
              def_=def_, infdef=infdef, ofdef=ofdef, catch=catch, align=align, dsc=round(dsc, 2))
    return clamp(dsc)


# ─────────────────────────────────────────────────────────────
# Interaction boosts
# ─────────────────────────────────────────────────────────────

def compute_husi_interaction(
    f: PitcherFeatureSet,
    pcs_gb: float,
    dsc_infdef: float,
    fly_supp: float,
    ens_park: float,
    ens_windin: float,
    owc_ld: float,
    ops_hook: float,
    ops_bpen: float,
    owc_bot3: float,
    ops_tto: float,
    pcs_reg: float,
    pcs_hha: float,
    owc_topheavy: float,
    uhs_zone: float = 50.0,
    pcs_cmd: float = 50.0,
    ens_air: float = 50.0,
    ens_of: float = 50.0,
    silent: bool = False,
) -> float:
    """
    Apply HUSI interaction boosts. Returns total boost (capped at 8.0).
    Each triggered rule is logged individually.

    Merlin v2.0 additions:
      Zone Sympathy: UHS_ZONE > 70 AND PCS_CMD > 70 → +4.0 (interactive, not additive)
      E1 Heavy Air:  ENS_AIR > 70 AND ENS_OF > 75   → +5.0 (physics-based suppression)
    """
    boost = 0.0
    name = f.pitcher_name

    if pcs_gb > 65 and dsc_infdef > 65:
        boost += 1.5
        if not silent:
            log.info("HUSI H1 triggered", pitcher=name, pcs_gb=pcs_gb, dsc_infdef=dsc_infdef, boost=1.5)

    if fly_supp > 60 and ens_park > 70:
        boost += 1.5
        if not silent:
            log.info("HUSI H2 triggered", pitcher=name, fly_supp=fly_supp, ens_park=ens_park, boost=1.5)

    if ens_windin > 70 and owc_ld > 60:
        boost += 1.0
        if not silent:
            log.info("HUSI H3 triggered", pitcher=name, ens_windin=ens_windin, owc_ld=owc_ld, boost=1.0)

    if ops_hook > 70 and ops_bpen > 70:
        boost += 2.0
        if not silent:
            log.info("HUSI H4 triggered", pitcher=name, ops_hook=ops_hook, ops_bpen=ops_bpen, boost=2.0)

    if owc_bot3 > 70 and ops_tto > 65:
        boost += 1.0
        if not silent:
            log.info("HUSI H5 triggered", pitcher=name, owc_bot3=owc_bot3, ops_tto=ops_tto, boost=1.0)

    if pcs_reg > 65 and pcs_hha > 65:
        boost += 1.0
        if not silent:
            log.info("HUSI H6 triggered", pitcher=name, pcs_reg=pcs_reg, pcs_hha=pcs_hha, boost=1.0)

    if (
        owc_topheavy > 70
        and f.projected_batters_faced is not None
        and f.projected_batters_faced < 23
    ):
        boost += 1.5
        if not silent:
            log.info("HUSI H7 triggered", pitcher=name,
                     owc_topheavy=owc_topheavy, projected_batters_faced=f.projected_batters_faced, boost=1.5)

    # ── Zone Sympathy (Merlin v2.0): Umpire zone + pitcher command are interactive, not additive.
    # When both are elite (> 70), the effect is multiplicative — the umpire rewards exactly
    # the kind of pitch the pitcher throws best. Replace old +1.0 additive with +4.0 interactive.
    if uhs_zone > 70 and pcs_cmd > 70:
        boost += 4.0
        if not silent:
            log.info("HUSI Zone Sympathy triggered", pitcher=name,
                     uhs_zone=uhs_zone, pcs_cmd=pcs_cmd, boost=4.0)

    # ── E1 Heavy Air / Deep Park (Merlin v2.0): Dense air + large outfield = physics-based
    # hit suppression. Fly balls that would leave normal parks die in heavy/large venues.
    if ens_air > 70 and ens_of > 75:
        boost += 5.0
        if not silent:
            log.info("HUSI E1 Heavy Air triggered", pitcher=name,
                     ens_air=ens_air, ens_of=ens_of, boost=5.0)

    capped = min(boost, 8.0)  # raised cap for new Merlin rules
    if not silent:
        log.info("HUSI interaction total", pitcher=name, raw_boost=boost, capped=capped)
    return capped


# ─────────────────────────────────────────────────────────────
# Volatility penalties
# ─────────────────────────────────────────────────────────────

def compute_husi_volatility(
    f: PitcherFeatureSet,
    pcs_consistency: float,
    ops_traffic: float = 50.0,
    owc_bot3: float = 50.0,
    silent: bool = False,
) -> float:
    """
    Apply HUSI volatility penalties. Returns total penalty (capped at 9.5).
    Each triggered penalty is logged individually.

    Merlin v2.0 addition:
      H8 Pressure Cooker: OPS_TRAFFIC > 75 AND OWC_BOT3 < 40 → -3.5 HUSI Penalty.
      Models pitcher collapse when facing high-traffic innings against a weak-bottomed lineup
      (pressure builds as manager leaves starter in despite runners on base).
    """
    penalty = 0.0
    name = f.pitcher_name

    if not f.lineup_confirmed:
        penalty += 2.5
        if not silent:
            log.info("HUSI HV1 lineup uncertainty", pitcher=name, penalty=2.5)

    # HV2: weather uncertainty — treat ens_windin < 40 as meaningful weather risk
    if f.ens_windin is not None and f.ens_windin < 40:
        penalty += 1.5
        if not silent:
            log.info("HUSI HV2 weather uncertainty", pitcher=name, ens_windin=f.ens_windin, penalty=1.5)

    if f.babip_variance_high:
        penalty += 1.5
        if not silent:
            log.info("HUSI HV3 extreme BABIP variance", pitcher=name, penalty=1.5)

    # HV4: poor defense — dsc_def < 40 means below-average defense
    if f.dsc_def is not None and f.dsc_def < 40:
        penalty += 2.0
        if not silent:
            log.info("HUSI HV4 poor defense", pitcher=name, dsc_def=f.dsc_def, penalty=2.0)

    # HV5: backup catcher — dsc_catch < 35
    if f.dsc_catch is not None and f.dsc_catch < 35:
        penalty += 1.0
        if not silent:
            log.info("HUSI HV5 backup catcher downgrade", pitcher=name, dsc_catch=f.dsc_catch, penalty=1.0)

    if not f.umpire_confirmed:
        penalty += 0.8
        if not silent:
            log.info("HUSI HV6 umpire unknown", pitcher=name, penalty=0.8)

    if pcs_consistency < 40:
        penalty += 2.0
        if not silent:
            log.info("HUSI HV7 low consistency", pitcher=name, pcs_consistency=pcs_consistency, penalty=2.0)

    # HV8: bullpen depleted — ops_bpen < 35
    if f.ops_bpen is not None and f.ops_bpen < 35:
        penalty += 1.2
        if not silent:
            log.info("HUSI HV8 bullpen depleted", pitcher=name, ops_bpen=f.ops_bpen, penalty=1.2)

    # HV9: extreme hitter park (Coors, Great American, Chase, Citizens Bank, etc.) — park_score < 40
    if f.park_extreme:
        penalty += 2.5
        if not silent:
            park_score = round(f.ens_park or 50.0, 1)
            log.info("HUSI HV9 extreme hitter park", pitcher=name,
                     park=f.ens_park, park_score=park_score,
                     park_hits_multiplier=f.park_hits_multiplier, penalty=2.5)

    # HV10: struggling season ERA
    if f.season_era_tier == "STRUGGLING":
        penalty += 1.5
        if not silent:
            log.info("HUSI HV10 struggling season ERA", pitcher=name,
                     season_era=f.season_era_raw, tier="STRUGGLING", penalty=1.5)
    elif f.season_era_tier == "DISASTER":
        penalty += 3.0
        if not silent:
            log.info("HUSI HV10 disaster season ERA", pitcher=name,
                     season_era=f.season_era_raw, tier="DISASTER", penalty=3.0)

    # ── H8 Pressure Cooker (Merlin v2.0):
    # High traffic innings + weak bottom of lineup = pitcher collapse.
    # A pitcher getting beaten in high-leverage situations (TRAFFIC > 75) while the
    # lineup has a weak bottom (BOT3 < 40) means no easy outs to recover — pitcher
    # is forced to challenge better hitters in pressure spots.
    if ops_traffic > 75 and owc_bot3 < 40:
        penalty += 3.5
        if not silent:
            log.info("HUSI H8 Pressure Cooker triggered", pitcher=name,
                     ops_traffic=ops_traffic, owc_bot3=owc_bot3, penalty=3.5)

    capped = min(penalty, 9.5)  # raised cap for new Merlin H8 rule
    if not silent:
        log.info("HUSI volatility total", pitcher=name, raw_penalty=penalty, capped=capped)
    return capped


# ─────────────────────────────────────────────────────────────
# Grade
# ─────────────────────────────────────────────────────────────

def husi_grade(score: float) -> str:
    """
    Grade thresholds anchored to real MLB pitcher distributions — NOT a 0-100 fantasy scale.

    In modern baseball no starter realistically scores 85+ on HUSI. An ace in a
    pitcher-friendly park against a weak lineup lands in the 65-75 range. Thresholds
    are set against what real pitchers actually achieve:

      A+  ≥ 65  — Elite hit suppression (top ~10%): Peralta, Skenes vs. weak lineup
      A   ≥ 58  — Very favorable (top ~25%): solid suppression, good matchup
      B   ≥ 52  — Above average (top ~45%): expect below-average hits allowed
      C   ≥ 46  — Neutral (middle third): average start expected
      D   < 46  — Risky: expect elevated hits, fade for Under props
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
# Main HUSI computation
# ─────────────────────────────────────────────────────────────

def compute_husi(f: PitcherFeatureSet, silent: bool = False) -> dict:
    """
    Compute the full HUSI score for one pitcher on one game day.

    Args:
        f:      Pitcher feature set.
        silent: If True, suppress all logging. Used by SimulationEngine for 2000-iteration speed.

    Returns a dict with:
      husi_base, husi_interaction, husi_volatility, husi, grade,
      projected_hits, base_hits,
      all individual block scores for database logging.
    """
    if not silent:
        log.info("HUSI computation starting", pitcher=f.pitcher_name, game_id=f.game_id)

    # ── Block scores
    owc = score_owc(f)
    pcs = score_pcs(f)
    ens = score_ens(f)
    ops = score_ops(f)
    uhs = score_uhs(f)
    dsc = score_dsc(f)

    if not silent:
        log.info("HUSI block scores",
                 pitcher=f.pitcher_name,
                 OWC=round(owc, 2), PCS=round(pcs, 2), ENS=round(ens, 2),
                 OPS=round(ops, 2), UHS=round(uhs, 2), DSC=round(dsc, 2))

    # ── Base formula
    husi_base = (
        0.27 * owc +
        0.26 * pcs +
        0.16 * ens +
        0.18 * ops +
        0.08 * uhs +
        0.05 * dsc
    )
    if not silent:
        log.info("HUSI base", pitcher=f.pitcher_name, husi_base=round(husi_base, 2))

    # ── Interaction boosts (includes Zone Sympathy and E1 from Merlin v2.0)
    fly_supp = _f(f.fly_ball_suppression)
    interaction = compute_husi_interaction(
        f=f,
        pcs_gb=_f(f.pcs_gb),
        dsc_infdef=_f(f.dsc_infdef),
        fly_supp=fly_supp,
        ens_park=_f(f.ens_park),
        ens_windin=_f(f.ens_windin),
        owc_ld=_f(f.owc_ld),
        ops_hook=_f(f.ops_hook),
        ops_bpen=_f(f.ops_bpen),
        owc_bot3=_f(f.owc_bot3),
        ops_tto=_f(f.ops_tto),
        pcs_reg=_f(f.pcs_reg),
        pcs_hha=_f(f.pcs_hha),
        owc_topheavy=_f(f.owc_topheavy),
        uhs_zone=_f(f.uhs_zone),
        pcs_cmd=_f(f.pcs_cmd),
        ens_air=_f(f.ens_air),
        ens_of=_f(f.ens_of),
        silent=silent,
    )

    # ── Volatility penalties (includes H8 Pressure Cooker from Merlin v2.0)
    pcs_consistency = _f(f.pcs_cmd)
    volatility = compute_husi_volatility(
        f,
        pcs_consistency=pcs_consistency,
        ops_traffic=_f(f.ops_traffic),
        owc_bot3=_f(f.owc_bot3),
        silent=silent,
    )

    # ── Final HUSI (pre-bullpen)
    husi_raw = husi_base + interaction - volatility
    husi_pre = clamp(husi_raw)

    # ── Bullpen Fatigue Adjustment
    # Tired opponent bullpen = more hit opportunities = HUSI goes UP.
    # Formula: Final_HUSI = Base_HUSI × (1 + BFS_opponent)
    from app.utils.bullpen import apply_bullpen_to_husi
    husi = apply_bullpen_to_husi(husi_pre, f.bullpen_fatigue_opp)
    bullpen_adjustment = round(husi - husi_pre, 2)

    # ── Travel & Fatigue Index adjustment (SKU #14)
    # A tired/traveling pitching team gives up more hits → HUSI goes DOWN.
    # Formula: HUSI_tfi = HUSI × (1 - tfi_penalty_pct)
    from app.utils.travel_fatigue import apply_tfi_to_husi
    husi_pre_tfi = husi
    husi = apply_tfi_to_husi(husi, f.tfi_penalty_pct)
    tfi_adjustment = round(husi - husi_pre_tfi, 2)
    if not silent and f.tfi_penalty_pct > 0:
        log.info("HUSI TFI penalty applied",
                 pitcher=f.pitcher_name,
                 tfi_label=f.tfi_label,
                 rest_hours=f.tfi_rest_hours,
                 tz_shift=f.tfi_tz_shift,
                 penalty_pct=f.tfi_penalty_pct,
                 husi_before=round(husi_pre_tfi, 2),
                 husi_after=round(husi, 2))

    grade = husi_grade(husi)
    if not silent:
        log.info("HUSI final",
                 pitcher=f.pitcher_name,
                 husi_base=round(husi_base, 2),
                 interaction=round(interaction, 2),
                 volatility=round(volatility, 2),
                 husi_pre_bullpen=round(husi_pre, 2),
                 bullpen_bfs=f.bullpen_fatigue_opp,
                 bullpen_label=f.bullpen_label_opp,
                 bullpen_adjustment=bullpen_adjustment,
                 tfi_label=f.tfi_label,
                 tfi_penalty=f.tfi_penalty_pct,
                 husi=round(husi, 2),
                 grade=grade)

    # ── Projected hits (MGS-aware)
    exp_ip = expected_ip(f.avg_ip_per_start, f.mlb_service_years)
    safe_h_per_9 = min(f.season_hits_per_9 or 9.0, 15.0)
    base_hits = safe_h_per_9 * (exp_ip / 9.0)
    projected_hits = base_hits * (1 - 0.21 * ((husi - 50) / 50))

    # MGS adjustment — applies TTO curve, pitch-count fatigue, PFF form factor,
    # and TTO3 Death Trap baserunner multiplier (Merlin v2.0)
    mgs_hits_mult, _, mgs_label = compute_mgs(
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
    projected_hits = projected_hits * mgs_hits_mult

    # ── VAA Elevation Rule (Merlin v2.0 — replaces old flat penalty logic)
    # Old rule: flat VAA (> -4.5°) ALWAYS added +10% contact penalty.
    # Merlin fix: flat VAA thrown HIGH in the zone (pitch_location_high_pct > 60%)
    #   actually produces pop-ups and weak fly balls, NOT hard contact.
    #   In this case, REVERSE the penalty to a BOOST (hits suppression).
    # When pitch_location_high_pct is unavailable, fall back to the old penalty.
    if f.vaa_flat and f.vaa_contact_penalty > 0:
        vaa_pre = projected_hits
        if (
            f.pitch_location_high_pct is not None
            and f.pitch_location_high_pct > 60.0
        ):
            # VAA Elevation override: flat + high = pop-up machine → suppress hits
            projected_hits = projected_hits * (1.0 - f.vaa_contact_penalty)
            if not silent:
                log.info("HUSI VAA Elevation BOOST applied (Merlin override)",
                         pitcher=f.pitcher_name,
                         vaa_degrees=f.vaa_degrees,
                         pitch_location_high_pct=f.pitch_location_high_pct,
                         boost_pct=f.vaa_contact_penalty,
                         hits_before=round(vaa_pre, 2),
                         hits_after=round(projected_hits, 2))
        else:
            # Standard VAA flat penalty: flat low pitch = easy to track and drive
            projected_hits = projected_hits * (1.0 + f.vaa_contact_penalty)
            if not silent:
                log.info("HUSI VAA flat penalty applied",
                         pitcher=f.pitcher_name,
                         vaa_degrees=f.vaa_degrees,
                         pitch_location_high_pct=f.pitch_location_high_pct,
                         contact_penalty_pct=f.vaa_contact_penalty,
                         hits_before=round(vaa_pre, 2),
                         hits_after=round(projected_hits, 2))

    # ── Park Factor Direct Override
    if f.park_hits_multiplier is not None and f.park_hits_multiplier != 1.0:
        park_pre = projected_hits
        projected_hits = projected_hits * f.park_hits_multiplier
        if not silent:
            log.info("HUSI park factor override applied",
                     pitcher=f.pitcher_name,
                     park_score=round(f.ens_park or 50.0, 1),
                     park_multiplier=f.park_hits_multiplier,
                     park_extreme=f.park_extreme,
                     hits_before=round(park_pre, 2),
                     hits_after=round(projected_hits, 2))

    projected_hits = max(0.0, min(projected_hits, 15.0))  # hard cap

    if not silent:
        log.info("HUSI projection",
                 pitcher=f.pitcher_name,
                 exp_ip=exp_ip,
                 ip_tier=ip_tier_label(exp_ip),
                 base_hits=round(base_hits, 2),
                 mgs_hits_mult=round(mgs_hits_mult, 3),
                 mgs_label=mgs_label,
                 projected_hits=round(projected_hits, 2))

    return {
        "husi": round(husi, 2),
        "husi_base": round(husi_base, 2),
        "husi_interaction": round(interaction, 2),
        "husi_volatility": round(volatility, 2),
        "grade": grade,
        "base_hits": round(base_hits, 2),
        "projected_hits": round(projected_hits, 2),
        "mgs_hits_mult": round(mgs_hits_mult, 4),
        "mgs_label": mgs_label,
        "park_hits_multiplier": round(f.park_hits_multiplier or 1.0, 3),
        "park_extreme": f.park_extreme,
        # Block scores for DB storage
        "owc_score": round(owc, 2),
        "pcs_score": round(pcs, 2),
        "ens_score": round(ens, 2),
        "ops_score": round(ops, 2),
        "uhs_score": round(uhs, 2),
        "dsc_score": round(dsc, 2),
    }
