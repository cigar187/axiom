"""
risk_scorer.py — Pitcher risk profile engine (internal API service).

Called automatically inside the daily pipeline after HUSI/KUSI features
are built. Requires NO external API calls — all inputs come from the
already-computed PitcherFeatureSet.

Risk is surfaced via /v1/risk/today so customers see it without
any manual commands or scripts.

Risk Flags
──────────
  ERA_DISASTER         Season ERA ≥ 6.00
  ERA_STRUGGLING       Season ERA 5.00–5.99
  BOOM_BUST            Recent IP variance — early-exit pattern (Walker Buehler rule)
  EXTREME_PARK         Pitching at a park score < 40 (Coors, Chase, GABP, etc.)
  HITTER_PARK          Pitching at a park score 40–48 (Fenway, Yankee Stadium, etc.)
  HIGH_H9              Season H/9 ≥ 9.5
  TFI_ACTIVE           Travel & Fatigue penalty triggered today
  COLD_START           PFF label contains COLD or STRUGGLING
  LOW_IP_TREND         Recent starts trending shorter than season average
  COMBO_RISK           3 or more flags active simultaneously
  FRAGILITY_ELEVATED   FI score 15–34 — short outing in most recent start
  FRAGILITY_HIGH       FI score 35–59 — early-exit pattern, IP capped at 3.5
  FRAGILITY_EXTREME    FI score ≥ 60  — yanked early, IP capped at 3.0 (Trevor Rogers rule)
  TBAPI_ELEVATED       1.7–1.99 baserunners/inning in recent starts
  TBAPI_HIGH           2.0–2.49 baserunners/inning — 4+ baserunners before 6th out
  TBAPI_EXTREME        ≥ 2.5 baserunners/inning — chronic early-inning traffic
"""
from app.core.features import PitcherFeatureSet
from app.utils.logging import get_logger

log = get_logger("risk_scorer")

# ── Thresholds (mirror feature_builder + pff constants)
ERA_DISASTER        = 6.00
ERA_STRUGGLING      = 5.00
H9_HIGH             = 9.5
EXTREME_PARK_SCORE  = 40.0
HITTER_PARK_SCORE   = 48.0

# ── Flag weights for the composite risk score
FLAG_WEIGHTS = {
    "ERA_DISASTER":       12,
    "ERA_STRUGGLING":      7,
    "BOOM_BUST":           8,
    "EXTREME_PARK":        8,
    "HIGH_H9":             5,
    "HITTER_PARK":         3,
    "TFI_ACTIVE":          4,
    "COLD_START":          5,
    "LOW_IP_TREND":        4,
    "COMBO_RISK":          6,
    # Fragility modifiers
    "FRAGILITY_ELEVATED":  5,
    "FRAGILITY_HIGH":      9,
    "FRAGILITY_EXTREME":  14,
    "TBAPI_ELEVATED":      4,
    "TBAPI_HIGH":          8,
    "TBAPI_EXTREME":      11,
}

# ── Risk tier labels for API display
def risk_tier(score: int) -> str:
    if score >= 20:
        return "HIGH"
    if score >= 8:
        return "MODERATE"
    return "LOW"


def compute_risk_profile(f: PitcherFeatureSet) -> dict:
    """
    Evaluate all risk flags from an already-built PitcherFeatureSet.

    Returns:
        {
            "risk_score":    int       — composite risk number
            "risk_tier":     str       — HIGH / MODERATE / LOW
            "risk_flags":    list[str] — active flag names
            "combo_risk":    bool      — True when 3+ flags active
            "risk_notes":    list[str] — human-readable explanations
        }
    """
    flags = []
    notes = []

    # ── ERA Tier (HV10 mirror)
    era = f.season_era_raw
    if era is not None:
        if era >= ERA_DISASTER:
            flags.append("ERA_DISASTER")
            notes.append(f"Season ERA {era:.2f} — disaster tier, consistently giving up runs all season")
        elif era >= ERA_STRUGGLING:
            flags.append("ERA_STRUGGLING")
            notes.append(f"Season ERA {era:.2f} — struggling tier, below-average performance level")

    # ── H/9 season rate
    h9 = f.season_hits_per_9
    if h9 is not None and h9 >= H9_HIGH:
        flags.append("HIGH_H9")
        notes.append(f"Season H/9 {h9:.1f} — already surrendering hits at an above-average rate")

    # ── PFF Boom-Bust (already computed in pff.py — carried through pff_label)
    pff_lbl = (f.pff_label or "").upper()
    if "BOOM-BUST" in pff_lbl:
        flags.append("BOOM_BUST")
        notes.append(f"Boom-Bust flag active ({f.pff_label}) — IP variance detected in recent starts")

    # ── Cold/Struggling start form
    if any(tier in pff_lbl for tier in ["COLD", "STRUGGLING"]) and "BOOM-BUST" not in pff_lbl:
        flags.append("COLD_START")
        notes.append(f"PFF: {f.pff_label} — pitcher coming in cold based on recent start quality")

    # ── Park factor
    park_score = f.ens_park
    if park_score is not None:
        if park_score < EXTREME_PARK_SCORE:
            flags.append("EXTREME_PARK")
            mult_pct = round((f.park_hits_multiplier - 1.0) * 100) if f.park_hits_multiplier else 0
            notes.append(
                f"Extreme hitter park (score {park_score:.0f}) — "
                f"+{mult_pct}% hits multiplier applied"
            )
        elif park_score < HITTER_PARK_SCORE:
            flags.append("HITTER_PARK")
            notes.append(f"Hitter-friendly park (score {park_score:.0f}) — above-average hit environment")

    # ── Travel & Fatigue Index
    if f.tfi_penalty_pct > 0:
        flags.append("TFI_ACTIVE")
        reason = []
        if f.tfi_getaway_day:
            reason.append(f"{f.tfi_rest_hours:.0f}h rest (getaway day)")
        if f.tfi_cross_timezone:
            reason.append(f"{f.tfi_tz_shift} timezone shift")
        notes.append(f"Travel & Fatigue penalty active ({', '.join(reason) or f.tfi_label}) — −7% HUSI")

    # ── Low IP trend: pff_hits_tto1_mult being high (> 1.10) suggests recent struggles
    # Also check: if avg_ip exists and pff_score is very negative, that signals short outings
    if f.pff_score <= -0.20 and f.pff_starts_used >= 2:
        if "COLD_START" not in flags:
            flags.append("LOW_IP_TREND")
            notes.append(
                f"PFF score {f.pff_score:.2f} (STRUGGLING) — recent starts significantly below season baseline"
            )

    # ── Fragility Index
    fi_tier = getattr(f, "fi_tier", "NONE")
    if fi_tier == "EXTREME":
        flags.append("FRAGILITY_EXTREME")
        cap = getattr(f, "fi_ip_cap", None)
        notes.append(
            f"Fragility Index EXTREME (score {f.fi_score:.0f}) — "
            f"recent early exits, IP capped at {cap or 'N/A'} — "
            + (", ".join(f.fi_notes) if f.fi_notes else "see recent starts")
        )
    elif fi_tier == "HIGH":
        flags.append("FRAGILITY_HIGH")
        notes.append(
            f"Fragility Index HIGH (score {f.fi_score:.0f}) — "
            + (", ".join(f.fi_notes) if f.fi_notes else "see recent starts")
        )
    elif fi_tier == "ELEVATED":
        flags.append("FRAGILITY_ELEVATED")
        notes.append(
            f"Fragility Index ELEVATED (score {f.fi_score:.0f}) — "
            + (", ".join(f.fi_notes) if f.fi_notes else "see recent starts")
        )

    # ── TBAPI
    tbapi_tier = getattr(f, "tbapi_tier", "NORMAL")
    tbapi_val  = round(getattr(f, "tbapi", 0.0), 2)
    if tbapi_tier == "EXTREME":
        flags.append("TBAPI_EXTREME")
        notes.append(
            f"TBAPI {tbapi_val} baserunners/inning (EXTREME) — "
            f"pitcher averaging 4+ baserunners in recent starts before 6th out"
        )
    elif tbapi_tier == "HIGH":
        flags.append("TBAPI_HIGH")
        notes.append(
            f"TBAPI {tbapi_val} baserunners/inning (HIGH) — elevated early-inning traffic pattern"
        )
    elif tbapi_tier == "ELEVATED":
        flags.append("TBAPI_ELEVATED")
        notes.append(
            f"TBAPI {tbapi_val} baserunners/inning (ELEVATED) — above league average baserunner rate"
        )

    # ── Combo Risk (3+ simultaneous flags)
    combo_risk = len(flags) >= 3
    if combo_risk:
        flags.append("COMBO_RISK")

    risk_score = sum(FLAG_WEIGHTS.get(fl, 0) for fl in flags)

    result = {
        "risk_score": risk_score,
        "risk_tier":  risk_tier(risk_score),
        "risk_flags": [fl for fl in flags if fl != "COMBO_RISK"],
        "combo_risk": combo_risk,
        "risk_notes": notes,
    }

    log.info(
        "Risk profile computed",
        pitcher=f.pitcher_name,
        risk_score=risk_score,
        risk_tier=result["risk_tier"],
        flags=result["risk_flags"],
        combo=combo_risk,
    )
    return result
