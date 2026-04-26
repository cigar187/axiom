"""
fragility.py — Fragility Index (FI) and TBAPI modifiers.

Two post-formula adjustments that sit on top of HUSI/KUSI output.
They never modify block scores, weights, or interaction rules.
They apply multipliers AFTER the formula finishes — same layer as the
GB Suppressor, Park Factor Override, and VAA penalty in husi.py.

────────────────────────────────────────────────────────────────────
MODIFIER A: Fragility Index (FI)
────────────────────────────────────────────────────────────────────
Detects pitchers with a recent pattern of early exits. A pitcher who
was yanked at 1.2 IP carries structural fragility that season-average
H/9 and HUSI scores cannot see. The Fragility Index catches it.

Inputs: IP and ERA from each of the last 3 starts (most recent first).

Scoring:
  Most recent start IP < 2.0         → +40 pts  (yanked — shelling)
  Most recent start IP < 3.0         → +25 pts  (early exit)
  Most recent start IP < 4.0         → +10 pts  (short outing)
  2+ of last 3 starts with IP < 4.0  → +20 pts  (pattern of short starts)
  ERA in most recent start ≥ 13.5    → +25 pts  (completely shelled)
  ERA in most recent start ≥  9.0    → +12 pts  (struggled badly)

Tiers and effects:
  EXTREME  (score ≥ 60): ip_cap = 3.0, hits_mult = 1.20
  HIGH     (score ≥ 35): ip_cap = 3.5, hits_mult = 1.12
  ELEVATED (score ≥ 15): ip_cap = 4.0, hits_mult = 1.06
  NONE     (score < 15): no change

────────────────────────────────────────────────────────────────────
MODIFIER B: TBAPI — Total Baserunners Allowed Per Inning
────────────────────────────────────────────────────────────────────
Measures how many baserunners a pitcher allows per inning across recent
starts. League average: ~1.31 per inning. "4+ hits/walks before the
6th out" means 4 baserunners in 2 innings = 2.0 per inning.

When walk data (bb9_this_start) is present, TBAPI = (H + BB) / IP.
When walk data is absent, falls back to H / IP (hits-only proxy).

Weighted across last 3 starts — same recency weights as PFF:
  Most recent: 50%, second: 30%, third: 20%

Tiers and hits multiplier:
  EXTREME  (TBAPI ≥ 2.5): hits_mult = 1.15
  HIGH     (TBAPI ≥ 2.0): hits_mult = 1.08
  ELEVATED (TBAPI ≥ 1.7): hits_mult = 1.04
  NORMAL   (TBAPI < 1.7): hits_mult = 1.00
"""
from app.utils.logging import get_logger

log = get_logger("fragility")

# ── Recency weights (match PFF convention)
_WEIGHTS = [0.50, 0.30, 0.20]

# ── Fragility Index score thresholds
FI_EXTREME  = 60
FI_HIGH     = 35
FI_ELEVATED = 15

# ── TBAPI thresholds (baserunners per inning)
TBAPI_EXTREME   = 2.5
TBAPI_HIGH      = 2.0
TBAPI_ELEVATED  = 1.7
TBAPI_LEAGUE_AVG = 1.31   # reference — used in logging only


def compute_fragility(recent_starts: list[dict]) -> dict:
    """
    Compute the Fragility Index and TBAPI modifiers from a pitcher's last 3 starts.

    Args:
        recent_starts: List of up to 3 start dicts, MOST RECENT FIRST.
                       Each dict should have:
                         "ip"              float  (innings pitched this start)
                         "era_this_start"  float  (ERA for this start)
                         "h9_this_start"   float  (hits per 9 for this start)
                       Optional:
                         "bb9_this_start"  float  (walks per 9 — enables full TBAPI)

    Returns:
        {
            "fi_score":        float       — raw fragility score (0-100)
            "fi_tier":         str         — NONE / ELEVATED / HIGH / EXTREME
            "fi_ip_cap":       float|None  — effective IP ceiling (None = no cap)
            "fi_hits_mult":    float       — hits multiplier from FI (1.0 = no change)
            "fi_notes":        list[str]   — human-readable explanations
            "tbapi":           float       — weighted baserunners per inning
            "tbapi_tier":      str         — NORMAL / ELEVATED / HIGH / EXTREME
            "tbapi_hits_mult": float       — hits multiplier from TBAPI (1.0 = no change)
            "tbapi_uses_bb":   bool        — True when walk data was available
        }
    """
    fi     = _compute_fi(recent_starts)
    tbapi  = _compute_tbapi(recent_starts)

    log.info(
        "Fragility modifiers computed",
        fi_tier=fi["fi_tier"],
        fi_score=fi["fi_score"],
        fi_ip_cap=fi["fi_ip_cap"],
        fi_hits_mult=fi["fi_hits_mult"],
        tbapi=round(tbapi["tbapi"], 3),
        tbapi_tier=tbapi["tbapi_tier"],
        tbapi_hits_mult=tbapi["tbapi_hits_mult"],
        tbapi_uses_bb=tbapi["tbapi_uses_bb"],
    )

    return {**fi, **tbapi}


# ─────────────────────────────────────────────────────────────
# Fragility Index
# ─────────────────────────────────────────────────────────────

def _compute_fi(recent_starts: list[dict]) -> dict:
    """Score the pitcher's recent IP pattern for early-exit fragility."""
    valid = [s for s in recent_starts if s.get("ip", 0) >= 0.1]

    if not valid:
        return _no_fragility()

    starts = valid[:3]
    ip_values       = [s["ip"] for s in starts]
    most_recent_ip  = ip_values[0]
    most_recent_era = starts[0].get("era_this_start")

    score = 0.0
    notes = []

    # ── Signal 1: how short was the most recent outing?
    if most_recent_ip < 2.0:
        score += 40
        notes.append(
            f"Most recent start: {most_recent_ip:.1f} IP — yanked early after shelling"
        )
    elif most_recent_ip < 3.0:
        score += 25
        notes.append(f"Most recent start: {most_recent_ip:.1f} IP — early exit")
    elif most_recent_ip < 4.0:
        score += 10
        notes.append(f"Most recent start: {most_recent_ip:.1f} IP — short outing")

    # ── Signal 2: is this a pattern across the last 3 starts?
    short_count = sum(1 for ip in ip_values if ip < 4.0)
    if short_count >= 2:
        score += 20
        notes.append(
            f"{short_count} of last {len(ip_values)} starts below 4.0 IP — recurring early exits"
        )

    # ── Signal 3: how badly was the pitcher shelled in the most recent start?
    if most_recent_era is not None:
        if most_recent_era >= 13.5:
            score += 25
            notes.append(
                f"ERA {most_recent_era:.1f} in most recent start — completely shelled"
            )
        elif most_recent_era >= 9.0:
            score += 12
            notes.append(
                f"ERA {most_recent_era:.1f} in most recent start — struggled badly"
            )

    score = min(score, 100.0)

    if score >= FI_EXTREME:
        tier      = "EXTREME"
        ip_cap    = 3.0
        hits_mult = 1.20
    elif score >= FI_HIGH:
        tier      = "HIGH"
        ip_cap    = 3.5
        hits_mult = 1.12
    elif score >= FI_ELEVATED:
        tier      = "ELEVATED"
        ip_cap    = 4.0
        hits_mult = 1.06
    else:
        tier      = "NONE"
        ip_cap    = None
        hits_mult = 1.0

    if tier != "NONE":
        log.warning(
            "Fragility Index triggered",
            fi_tier=tier,
            fi_score=score,
            fi_ip_cap=ip_cap,
            fi_hits_mult=hits_mult,
            notes=notes,
        )

    return {
        "fi_score":     round(score, 1),
        "fi_tier":      tier,
        "fi_ip_cap":    ip_cap,
        "fi_hits_mult": hits_mult,
        "fi_notes":     notes,
    }


# ─────────────────────────────────────────────────────────────
# TBAPI — Total Baserunners Allowed Per Inning
# ─────────────────────────────────────────────────────────────

def _compute_tbapi(recent_starts: list[dict]) -> dict:
    """Compute the weighted baserunner-per-inning rate across recent starts."""
    valid = [s for s in recent_starts if s.get("ip", 0) >= 0.1]

    if not valid:
        return _no_tbapi()

    starts   = valid[:3]
    uses_bb  = any("bb9_this_start" in s for s in starts)

    weights  = _WEIGHTS[:len(starts)]
    total_w  = sum(weights)
    weights  = [w / total_w for w in weights]

    per_start_rates = []
    for s in starts:
        ip  = s["ip"]
        h9  = s.get("h9_this_start", 8.8)
        hits = h9 * ip / 9.0

        if uses_bb and "bb9_this_start" in s:
            bb9   = s["bb9_this_start"]
            walks = bb9 * ip / 9.0
            baserunners = hits + walks
        else:
            baserunners = hits

        per_start_rates.append(baserunners / ip)

    tbapi = sum(w * r for w, r in zip(weights, per_start_rates))

    if tbapi >= TBAPI_EXTREME:
        tier      = "EXTREME"
        hits_mult = 1.15
    elif tbapi >= TBAPI_HIGH:
        tier      = "HIGH"
        hits_mult = 1.08
    elif tbapi >= TBAPI_ELEVATED:
        tier      = "ELEVATED"
        hits_mult = 1.04
    else:
        tier      = "NORMAL"
        hits_mult = 1.0

    if tier != "NORMAL":
        log.warning(
            "TBAPI elevated",
            tbapi=round(tbapi, 3),
            tbapi_tier=tier,
            tbapi_hits_mult=hits_mult,
            uses_bb=uses_bb,
            league_avg=TBAPI_LEAGUE_AVG,
        )

    return {
        "tbapi":           round(tbapi, 4),
        "tbapi_tier":      tier,
        "tbapi_hits_mult": hits_mult,
        "tbapi_uses_bb":   uses_bb,
    }


# ─────────────────────────────────────────────────────────────
# Neutral fallbacks — returned when no recent start data exists
# ─────────────────────────────────────────────────────────────

def _no_fragility() -> dict:
    return {
        "fi_score":     0.0,
        "fi_tier":      "NONE",
        "fi_ip_cap":    None,
        "fi_hits_mult": 1.0,
        "fi_notes":     [],
    }


def _no_tbapi() -> dict:
    return {
        "tbapi":           0.0,
        "tbapi_tier":      "NORMAL",
        "tbapi_hits_mult": 1.0,
        "tbapi_uses_bb":   False,
    }
