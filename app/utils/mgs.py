"""
mgs.py — Mid-Game Surge (MGS) coefficient.

The MGS addresses a structural blind spot in flat-rate projections: pitcher
performance degrades NON-LINEARLY as batters see the pitcher more times
(Times Through Order, TTO) and pitch count climbs.

MLB Statcast data on pitcher performance by TTO:

    TTO1 (innings 1-3):  OPS against ≈ .680  — batters seeing new stuff
    TTO2 (innings 4-5):  OPS against ≈ .762  — batters timing delivery
    TTO3 (innings 6+):   OPS against ≈ .842  — batters fully dialed in

Translated to per-inning hit-rate multipliers vs. a flat seasonal average:

    TTO1: 0.82×   pitcher is still surprising batters
    TTO2: 1.12×   batter recognition grows — more loud contact
    TTO3: 1.38×   the "fourth inning surge" the user identified

Strikeout TTO effect runs in reverse (first time through = most Ks):

    TTO1: 1.18×   batters chasing unfamiliar stuff
    TTO2: 0.96×   batters lay off borderline pitches
    TTO3: 0.78×   batters are patient, fewer swinging Ks

Pitch count fatigue further amplifies TTO2 and TTO3 effects:

    PC ≤ 65:  × 1.00   fresh arm
    PC 66-80: × 1.08   tiring — velocity and command start slipping
    PC 81-95: × 1.18   laboring — clear command erosion
    PC 96+:   × 1.32   running on fumes — high blowup risk

TTO3 "Death Trap" (Merlin v2.0):
    If Inning ≥ 6 AND Baserunners in last 2 innings > 2,
    the hits multiplier escalates from 1.38× to 1.85×.
    This captures the non-linear collapse that Statcast shows when
    a pitcher re-faces a lineup with baserunner traffic still on.

Usage
─────
Pre-game (no live state):
    hits_mult, ks_mult, label = compute_mgs(exp_ip)

Live (inning + pitch count known):
    hits_mult, ks_mult, label = compute_mgs(exp_ip, current_inning=6, current_pitch_count=91)

Simulation (pre-game with stochastic baserunners):
    hits_mult, ks_mult, label = compute_mgs(exp_ip, baserunners_l2=3)

Returns a multiplier > 1.0 when the surge effect is working AGAINST the pitcher
(more hits expected / fewer Ks expected). Returns < 1.0 when pitcher is early/fresh.
"""
from typing import Optional
from app.utils.logging import get_logger

log = get_logger("mgs")

# ── TTO hit-rate multipliers (relative to flat seasonal average)
TTO_HIT_MULT = {
    1: 0.82,   # innings 1-3: pitcher advantage
    2: 1.12,   # innings 4-5: batter recognition kicks in
    3: 1.38,   # innings 6+:  full surge — bats are hot
}

# ── Death Trap multiplier (Merlin v2.0)
# Fires when pitcher is in TTO3 territory AND there are heavy baserunners.
# Research basis: pitchers with WHIP > 1.5 in 6th inning face dramatically
# worse outcomes when they've been leaving runners on base all game.
TTO3_DEATH_TRAP_MULT = 1.85    # replaces 1.38 when baserunner condition fires
TTO3_DEATH_TRAP_THRESHOLD = 2  # baserunners in last 2 innings to trigger death trap

# ── TTO strikeout-rate multipliers (inverse relationship to hits)
TTO_K_MULT = {
    1: 1.18,   # first time through: most Ks
    2: 0.96,   # second time: Ks decline
    3: 0.78,   # third time: fewest Ks — batters are patient
}

# ── Pitch count fatigue — stepped tiers
# Each tier represents a meaningful threshold where arm fatigue becomes measurable.
# These hit harder than a smooth curve, which is intentional — fatigue in baseball
# doesn't accumulate gradually, it compounds. A pitcher at 88 pitches is NOT twice
# as tired as one at 44 pitches; they are in a different physiological state.
#
#  ≤ 65 pitches: 1.00× — fresh arm, full command and velocity
#  ≤ 80 pitches: 1.08× — tiring — first signs of command erosion
#  ≤ 95 pitches: 1.18× — laboring — the "Bassitt/Wacha cliff" zone
#   96+ pitches: 1.32× — running on fumes — high blowup risk
PC_FATIGUE_TIERS = [
    (65,  1.00),
    (80,  1.08),
    (95,  1.18),
    (9999, 1.32),
]
PC_FATIGUE_MAX = 1.40         # hard cap — in case pitch counts are absurd (rain delay, etc.)

# Innings-per-TTO window boundaries
TTO1_INNINGS = 3.0   # first 3 innings
TTO2_INNINGS = 2.0   # innings 4-5
# TTO3 = everything beyond 5.0 innings


def _pc_fatigue(pitch_count: int) -> float:
    """
    Return the pitch count fatigue amplifier for a given count.
    Stepped tiers — each threshold represents a distinct fatigue state.
    """
    for threshold, factor in PC_FATIGUE_TIERS:
        if pitch_count <= threshold:
            return factor
    return PC_FATIGUE_MAX


def _tto_from_inning(inning: int) -> int:
    """Map a current inning number to a TTO tier (1, 2, or 3)."""
    if inning <= 3:
        return 1
    if inning <= 5:
        return 2
    return 3


def _mgs_label(hits_mult: float, in_surge: bool) -> str:
    if in_surge:
        return "SURGE"
    if hits_mult >= 1.05:
        return "ELEVATED"
    if hits_mult <= 0.90:
        return "SUPPRESSED"
    return "NORMAL"


def compute_mgs(
    exp_ip: float,
    current_inning: int = 0,
    current_pitch_count: int = 0,
    pff_hits_tto1_mult: float = 0.82,
    pff_ks_tto1_mult: float = 1.18,
    pff_tto_late_boost: float = 0.0,
    pff_label: str = "NEUTRAL",
    baserunners_l2: Optional[float] = None,
    silent: bool = False,
) -> tuple[float, float, str]:
    """
    Compute the Mid-Game Surge multipliers for hits and strikeouts.

    Args:
        exp_ip:              Expected innings pitched (from ip_window.py).
        current_inning:      Current game inning (0 = pre-game mode).
        current_pitch_count: Pitcher's current pitch count (0 = pre-game mode).
        pff_hits_tto1_mult:  PFF-adjusted TTO1 hit multiplier (default 0.82 = neutral).
        pff_ks_tto1_mult:    PFF-adjusted TTO1 K multiplier (default 1.18 = neutral).
        pff_tto_late_boost:  Additional % applied to TTO2/TTO3 hits for HOT pitchers.
                             Positive = steeper TTO curve (HOT pitcher gets shelled harder later).
                             Negative = flatter curve (COLD pitcher already getting hit).
        pff_label:           Human-readable PFF tier (for logging).
        baserunners_l2:      Baserunners in the last 2 innings. When > 2 and pitcher is in
                             TTO3 territory, activates the Death Trap (1.38 → 1.85×).
                             In simulation mode this is a stochastic Poisson sample per iteration.
        silent:              If True, suppress all logging (used by SimulationEngine for speed).

    Returns:
        (hits_mult, ks_mult, label)
    """
    if exp_ip <= 0:
        return 1.0, 1.0, "NORMAL"

    live_mode = current_inning > 0

    # Apply PFF to TTO1 base values — everything builds from there
    h_tto1 = pff_hits_tto1_mult
    k_tto1 = pff_ks_tto1_mult

    # TTO2/TTO3 base values — amplified by PFF late boost for HOT pitchers
    # "Throwing lava then getting shelled": if PFF > 0, the TTO2/TTO3 surge
    # is STEEPER because batters are more motivated to crack a dominant pitcher.
    h_tto2 = TTO_HIT_MULT[2] * (1.0 + pff_tto_late_boost)
    h_tto3 = TTO_HIT_MULT[3] * (1.0 + pff_tto_late_boost)
    # Ks in TTO2/TTO3 drop MORE for a HOT pitcher (batters eventually crack him)
    k_tto2 = TTO_K_MULT[2] / (1.0 + pff_tto_late_boost) if pff_tto_late_boost > 0 else TTO_K_MULT[2]
    k_tto3 = TTO_K_MULT[3] / (1.0 + pff_tto_late_boost) if pff_tto_late_boost > 0 else TTO_K_MULT[3]

    # ── TTO3 "Death Trap" (Merlin v2.0)
    # When a pitcher enters TTO3 territory with heavy baserunner traffic in the last 2 innings,
    # the hit surge jumps from 1.38× to 1.85× — a non-linear collapse that's under-modeled
    # by flat TTO rates.
    death_trap_active = (
        baserunners_l2 is not None
        and baserunners_l2 > TTO3_DEATH_TRAP_THRESHOLD
    )
    if death_trap_active:
        h_tto3 = TTO3_DEATH_TRAP_MULT * (1.0 + pff_tto_late_boost)

    if live_mode:
        # ── LIVE MODE: pitcher is in-game — compute exactly for current inning
        tto = _tto_from_inning(current_inning)
        pc_amp = _pc_fatigue(current_pitch_count)

        if tto == 1:
            hits_mult = h_tto1
            ks_mult = k_tto1
        elif tto == 2:
            hits_mult = h_tto2 * pc_amp
            ks_mult = k_tto2 / pc_amp
        else:  # TTO3
            hits_mult = h_tto3 * pc_amp
            ks_mult = k_tto3 / pc_amp

        hits_mult = max(0.50, min(hits_mult, 2.20))
        ks_mult   = max(0.35, min(ks_mult, 1.60))

        in_surge = tto == 3 or (tto == 2 and current_pitch_count >= 80)
        label = _mgs_label(hits_mult, in_surge)
        if death_trap_active and tto == 3:
            label = "DEATH_TRAP"

        if not silent:
            log.info("MGS live",
                     inning=current_inning, pitch_count=current_pitch_count,
                     tto=tto, pc_amp=round(pc_amp, 3),
                     pff=pff_label,
                     baserunners_l2=baserunners_l2,
                     death_trap=death_trap_active and tto == 3,
                     hits_mult=round(hits_mult, 3),
                     ks_mult=round(ks_mult, 3),
                     label=label)

    else:
        # ── PRE-GAME MODE: distribute expected IP across TTO tiers
        tto1_ip = min(exp_ip, TTO1_INNINGS)
        tto2_ip = max(0.0, min(exp_ip - TTO1_INNINGS, TTO2_INNINGS))
        tto3_ip = max(0.0, exp_ip - TTO1_INNINGS - TTO2_INNINGS)

        PITCHES_PER_INNING = 16.0
        est_pc_tto2 = int((TTO1_INNINGS + tto2_ip / 2) * PITCHES_PER_INNING)
        est_pc_tto3 = int((TTO1_INNINGS + TTO2_INNINGS + tto3_ip / 2) * PITCHES_PER_INNING)

        pcf2 = _pc_fatigue(est_pc_tto2)
        pcf3 = _pc_fatigue(est_pc_tto3)

        hits_total = (
            tto1_ip * h_tto1 +
            tto2_ip * h_tto2 * pcf2 +
            tto3_ip * h_tto3 * pcf3  # h_tto3 already elevated if death trap active
        )
        ks_total = (
            tto1_ip * k_tto1 +
            tto2_ip * k_tto2 / pcf2 +
            tto3_ip * k_tto3 / pcf3
        )

        hits_mult = hits_total / exp_ip
        ks_mult   = ks_total / exp_ip

        hits_mult = max(0.60, min(hits_mult, 2.00))  # raised cap for death trap scenario
        ks_mult   = max(0.45, min(ks_mult, 1.50))

        in_surge = tto3_ip > 0.5
        label = _mgs_label(hits_mult, in_surge)
        if death_trap_active and tto3_ip > 0.5:
            label = "DEATH_TRAP"

        if not silent:
            log.info("MGS pre-game",
                     exp_ip=exp_ip, pff=pff_label,
                     tto1_ip=round(tto1_ip, 2), tto2_ip=round(tto2_ip, 2), tto3_ip=round(tto3_ip, 2),
                     h_tto1=round(h_tto1, 3), h_tto2=round(h_tto2, 3), h_tto3=round(h_tto3, 3),
                     pff_late_boost=pff_tto_late_boost,
                     baserunners_l2=baserunners_l2,
                     death_trap=death_trap_active,
                     hits_mult=round(hits_mult, 3),
                     ks_mult=round(ks_mult, 3),
                     label=label)

    return round(hits_mult, 4), round(ks_mult, 4), label
