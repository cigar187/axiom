"""
pff.py — Pitcher Form Factor (PFF).

Accounts for how HOT or COLD a pitcher is entering today's game based on
his last 3 starts. This modifies the starting point of the MGS curve.

The Problem It Solves
─────────────────────
The MGS formula already handles TTO2/TTO3 degradation (innings 4-6 surge).
But it assumes every pitcher starts at the same baseline. Reality:

  HOT pitcher  → Crushing it. Lava. TTO1 is almost untouchable.
                 Batters are desperate by TTO2. When they finally crack him
                 in TTO3, the blowup is WORSE than average because they've
                 been working at his stuff for 6 innings.

  COLD pitcher → Struggling from pitch one. The first inning can look like
                 the 6th inning for an average pitcher. His TTO2/TTO3
                 is already damaged — the "late-game surge" is less severe
                 because he was already getting hit.

  STREAKY      → Last start was a 7-inning gem, the one before was a
                 first-inning blowup. The formula weights recency (last
                 start matters 2× more than start 3 games ago).

PFF Score
─────────
Computed from the pitcher's last 3 starts (game log):
  - Per-start quality = f(ERA, H/9, K/9 for that start)
  - Weighted: most recent 50%, middle 30%, oldest 20%
  - Compared to his own season average quality
  - Output: PFF delta in [-0.30, +0.30]

PFF Tiers and Their Effect on the MGS Curve
────────────────────────────────────────────
  PFF ≥ +0.20   ON FIRE     TTO1 hits × 0.65 | TTO1 Ks × 1.40 | TTO2/3 steeper +15%
  +0.10→+0.20   HOT         TTO1 hits × 0.72 | TTO1 Ks × 1.28 | TTO2/3 steeper +8%
  -0.10→+0.10   NEUTRAL     TTO1 hits × 0.82 | TTO1 Ks × 1.18 | baseline (no change)
  -0.20→-0.10   COLD        TTO1 hits × 0.98 | TTO1 Ks × 0.95 | TTO2/3 slightly flatter
  ≤ -0.20       STRUGGLING  TTO1 hits × 1.15 | TTO1 Ks × 0.78 | no further steepening

Boom-or-Bust Variance Flag
──────────────────────────
Some pitchers are not just COLD — they are HIGH VARIANCE. Their form average
looks neutral (decent recent starts mixed with blowups) but they carry a high
risk of an early-inning implosion.

Examples: pitchers returning from injury, early-season inconsistency, pitchers
who alternate dominant starts with 2-inning disasters.

Detection: IP standard deviation across last 3 starts > 1.8 innings
         AND at least one start with IP < 4.0 in recent history.

When triggered:
  - h_tto1 multiplier increased by +0.18 (more hits expected early)
  - Label gains "/BOOM-BUST" suffix
  - Logs a warning for analyst review

The "Throwing Lava Then Getting Shelled" Model
───────────────────────────────────────────────
When PFF > 0, TTO2/TTO3 multipliers are amplified because:
  - Batters in TTO2 have had more time studying an EXCEPTIONAL pitcher
  - The swing-adjustment from "confused" to "locked in" is more dramatic
  - This is documented in Statcast: aces face the biggest TTO penalty,
    not average pitchers, because they fool batters MORE in TTO1

When PFF < 0, TTO2/TTO3 multipliers are NOT amplified because:
  - Batters are already hitting the pitcher well in TTO1
  - They don't need TTO2 to "figure him out" — they already have
"""
import statistics
from app.utils.logging import get_logger

log = get_logger("pff")

# ── Recency weights for last 3 starts
WEIGHT_LAST = 0.50      # most recent start
WEIGHT_2ND  = 0.30      # second most recent
WEIGHT_3RD  = 0.20      # third most recent

# ── Hard clamp on PFF output
PFF_FLOOR   = -0.30
PFF_CEILING = +0.30

# ── League averages used for start quality normalization
LEAGUE_AVG_ERA   = 4.20
LEAGUE_AVG_H9    = 8.80
LEAGUE_AVG_K9    = 8.60

# ── Boom-or-Bust variance detection thresholds
# If a pitcher's IP swings wildly across starts, they carry an early-implosion risk
# even when their average form looks neutral. Think: Walker Buehler 2026.
BOOM_BUST_IP_STDEV      = 1.6   # IP standard deviation across recent starts
BOOM_BUST_SHORT_OUTING  = 4.0   # starts below this IP count as "early exits"
BOOM_BUST_PANIC_EXIT    = 3.0   # starts below this IP are automatic flags (yanked early)
BOOM_BUST_HIT_PENALTY   = 0.18  # added to h_tto1 when variance flag triggers

# ── TTO1 multiplier adjustments per PFF tier
# Format: (pff_threshold, hits_tto1_mult, ks_tto1_mult, tto_late_boost)
# tto_late_boost: additional multiplier applied to TTO2/TTO3 hit rates
PFF_TIERS = [
    # (min_pff,  label,        h_tto1,  k_tto1,  tto_late_boost)
    (+0.20,  "ON FIRE",    0.65,    1.40,    0.15),
    (+0.10,  "HOT",        0.72,    1.28,    0.08),
    (-0.10,  "NEUTRAL",    0.82,    1.18,    0.00),
    (-0.20,  "COLD",       0.98,    0.95,   -0.04),
    (-9.99,  "STRUGGLING", 1.15,    0.78,    0.00),
]


def get_pff_tier(pff: float) -> tuple[str, float, float, float]:
    """
    Return (label, hits_tto1_mult, ks_tto1_mult, tto_late_boost) for a PFF score.
    """
    for min_pff, label, h_mult, k_mult, boost in PFF_TIERS:
        if pff >= min_pff:
            return label, h_mult, k_mult, boost
    # Fallback — shouldn't reach here with clamped input
    return "STRUGGLING", 1.15, 0.78, 0.00


def compute_pff(recent_starts: list[dict]) -> dict:
    """
    Compute the Pitcher Form Factor from a pitcher's last 3 starts.

    Args:
        recent_starts: List of up to 3 start dicts, MOST RECENT FIRST.
                       Each dict must have:
                         "era_this_start"  float  (earned_runs / ip * 9)
                         "h9_this_start"   float  (hits / ip * 9)
                         "k9_this_start"   float  (ks / ip * 9)
                         "ip"              float  (innings pitched)
                       Optional:
                         "season_era"      float  (pitcher's season ERA for comparison)
                         "season_h9"       float
                         "season_k9"       float

    Returns:
        {
            "pff":               float   # [-0.30, +0.30]
            "label":             str     # ON FIRE / HOT / NEUTRAL / COLD / STRUGGLING
            "hits_tto1_mult":    float   # TTO1 hit rate multiplier
            "ks_tto1_mult":      float   # TTO1 K rate multiplier
            "tto_late_boost":    float   # additional % applied to TTO2/TTO3 hits
            "starts_used":       int     # how many starts contributed
            "weighted_quality":  float   # 0-100
        }
    """
    # Filter to starts where at least one batter was faced (IP >= 0.1).
    # 0.0 IP = pitcher withdrew before throwing a pitch (injury/scratch) — not a performance signal.
    # 0.1+ IP = pitcher faced batters — even a 1.2 IP shelling is valid performance data
    # and MUST be included. The old 1.0 IP floor was silently discarding the most
    # informative early-exit signals (e.g., Rogers 1.2 IP, Buehler 0.2 IP disasters).
    valid = [s for s in recent_starts if s.get("ip", 0) >= 0.1]

    if not valid:
        log.info("PFF: no valid recent starts — defaulting to NEUTRAL")
        return _neutral_result()

    weights = [WEIGHT_LAST, WEIGHT_2ND, WEIGHT_3RD][:len(valid)]
    # Normalize weights in case fewer than 3 starts
    total_w = sum(weights)
    weights = [w / total_w for w in weights]

    # Score each start 0-100
    start_scores = []
    for start in valid[:3]:
        score = _start_quality(start)
        start_scores.append(score)

    # Weighted quality score across last 3 starts
    weighted_q = sum(w * s for w, s in zip(weights, start_scores))

    # Baseline: what is a "neutral" performance for this pitcher?
    # Use season stats if available, else league averages
    baseline_q = _season_baseline_quality(valid[0])

    # PFF = how far above/below baseline this pitcher has been recently
    raw_pff = (weighted_q - baseline_q) / 100.0
    pff = max(PFF_FLOOR, min(PFF_CEILING, raw_pff))

    label, h_tto1, k_tto1, tto_boost = get_pff_tier(pff)

    # ── Boom-or-Bust Variance Check
    # If IP swings wildly across recent starts AND there's at least one early exit,
    # the pitcher carries an early-inning implosion risk that the average form score misses.
    # Example: 2.2 IP → 6.0 IP → 5.0 IP (stdev ≈ 1.7, has panic exit < 3 IP) = flag it.
    # Two triggers:
    #   (a) stdev ≥ 1.6 AND at least one start with IP < 4.0
    #   (b) ANY start with IP < 3.0 (panic exit = automatic flag regardless of stdev)
    boom_bust = False
    ip_values = [s.get("ip", 0.0) for s in valid[:3]]
    if len(ip_values) >= 2:
        ip_stdev = statistics.stdev(ip_values)
        has_short_outing = any(ip < BOOM_BUST_SHORT_OUTING for ip in ip_values)
        has_panic_exit   = any(ip < BOOM_BUST_PANIC_EXIT for ip in ip_values)
        triggered = (ip_stdev >= BOOM_BUST_IP_STDEV and has_short_outing) or has_panic_exit
        if triggered:
            boom_bust = True
            h_tto1 = min(h_tto1 + BOOM_BUST_HIT_PENALTY, 1.45)
            reason = "panic exit < 3IP" if has_panic_exit else f"IP stdev {ip_stdev:.2f} + short outing"
            label = f"{label}/BOOM-BUST"
            log.warning("PFF BOOM-BUST variance flag triggered",
                        pitcher_starts_used=len(valid),
                        ip_values=ip_values,
                        ip_stdev=round(ip_stdev, 2) if len(ip_values) >= 2 else "n/a",
                        reason=reason,
                        h_tto1_adjusted=round(h_tto1, 3))

    # ── Starter Profile: VELOCITY vs DECEPTION vs DEFAULT
    # Detected from season K/9 and H/9. Adjusts the TTO penalty shape.
    starter_profile, profile_notes = _classify_starter_profile(valid[0])

    if starter_profile == "VELOCITY":
        # High-K pitchers need 1 inning to "find the zone."
        # Apply a first-15-pitch front-loaded penalty:
        # TTO1 hit mult increases slightly (they're hittable early),
        # but their TTO2/TTO3 steepness is REDUCED because once locked in,
        # they're harder to adjust to than an average pitcher.
        h_tto1 = min(h_tto1 * 1.12, 1.30)   # hittable before they find zone
        k_tto1 = max(k_tto1 * 0.92, 0.70)    # K rate slightly lower before dialed in
        tto_boost = max(tto_boost - 0.05, -0.10)  # less late-game surge (stuff carries longer)
        label = f"{label}/VEL"

    elif starter_profile == "DECEPTION":
        # Movement/sequencing pitchers peak in TTO1 — batters are confused.
        # Their "cliff" comes at TTO2, not TTO3. Already baked into the fact
        # that TTO2 multiplier will hurt them more if their deception fades.
        h_tto1 = max(h_tto1 * 0.90, 0.58)   # extra early suppression — batters totally lost
        k_tto1 = min(k_tto1 * 1.10, 1.55)    # more Ks when deception is fresh
        tto_boost = min(tto_boost + 0.08, 0.25)  # steeper TTO2 cliff (deception wears off fast)
        label = f"{label}/DEC"

    log.info("PFF computed",
             pff=round(pff, 3),
             label=label,
             starter_profile=starter_profile,
             profile_notes=profile_notes,
             boom_bust=boom_bust,
             weighted_quality=round(weighted_q, 1),
             baseline_quality=round(baseline_q, 1),
             starts_used=len(valid),
             hits_tto1_mult=round(h_tto1, 3),
             ks_tto1_mult=round(k_tto1, 3),
             tto_late_boost=round(tto_boost, 3))

    return {
        "pff":              round(pff, 4),
        "label":            label,
        "hits_tto1_mult":   round(h_tto1, 4),
        "ks_tto1_mult":     round(k_tto1, 4),
        "tto_late_boost":   round(tto_boost, 4),
        "starts_used":      len(valid),
        "weighted_quality": round(weighted_q, 1),
        "baseline_quality": round(baseline_q, 1),
        "starter_profile":  starter_profile,   # VELOCITY / DECEPTION / DEFAULT
        "boom_bust":        boom_bust,          # True when IP variance is dangerously high
    }


def _start_quality(start: dict) -> float:
    """
    Score a single start 0-100.

    Higher = pitcher performed better than league average.
    Uses ERA, H/9, and K/9 for that start, compared to league averages.

    Scoring:
      40% weight on ERA suppression
      40% weight on hit suppression
      20% weight on strikeout generation
    """
    era  = start.get("era_this_start", LEAGUE_AVG_ERA)
    h9   = start.get("h9_this_start",  LEAGUE_AVG_H9)
    k9   = start.get("k9_this_start",  LEAGUE_AVG_K9)

    # ERA score: lower ERA = higher score
    # 0.00 ERA = 100, league avg ERA (4.20) = 50, 9.00+ ERA = 0
    era_score = max(0.0, min(100.0, 100.0 - (era / 9.0) * 100.0))

    # H/9 score: lower H/9 = higher score
    # 0 H/9 = 100, league avg (8.80) = ~50, 18+ H/9 = 0
    h9_score = max(0.0, min(100.0, 100.0 - (h9 / 18.0) * 100.0))

    # K/9 score: higher K/9 = higher score
    # 15+ K/9 = 100, league avg (8.60) = ~57, 0 K/9 = 0
    k9_score = max(0.0, min(100.0, (k9 / 15.0) * 100.0))

    quality = 0.40 * era_score + 0.40 * h9_score + 0.20 * k9_score

    log.debug("Start quality",
              era=era, h9=round(h9, 2), k9=round(k9, 2),
              era_score=round(era_score, 1), h9_score=round(h9_score, 1),
              k9_score=round(k9_score, 1), quality=round(quality, 1))

    return round(quality, 2)


def _season_baseline_quality(sample_start: dict) -> float:
    """
    Compute the pitcher's expected quality from season averages.
    Falls back to league averages if season stats are missing.
    """
    s_era = sample_start.get("season_era", LEAGUE_AVG_ERA)
    s_h9  = sample_start.get("season_h9",  LEAGUE_AVG_H9)
    s_k9  = sample_start.get("season_k9",  LEAGUE_AVG_K9)

    era_score = max(0.0, min(100.0, 100.0 - (s_era / 9.0) * 100.0))
    h9_score  = max(0.0, min(100.0, 100.0 - (s_h9 / 18.0) * 100.0))
    k9_score  = max(0.0, min(100.0, (s_k9 / 15.0) * 100.0))

    return round(0.40 * era_score + 0.40 * h9_score + 0.20 * k9_score, 2)


def _classify_starter_profile(sample_start: dict) -> tuple[str, str]:
    """
    Classify the pitcher as VELOCITY, DECEPTION, or DEFAULT based on season stats.

    VELOCITY:  High K/9 (≥ 9.5) — relies on overpowering stuff.
               Needs ~1 inning to warm up. First-15-pitch penalty applies.
               Cold starts are most common here (high-K guys who "lose it" early).

    DECEPTION: Low K/9 (< 8.0) AND low H/9 (< 8.5) — contact manager.
               Peaks in TTO1 (movement surprises batters). Cliff at TTO2.
               These are the pitchers whose deception wears off fastest.

    DEFAULT:   Everyone else. Standard PFF/MGS curve.

    Returns (profile_type, explanation).
    """
    k9 = sample_start.get("season_k9") or sample_start.get("k9_this_start", 8.0)
    h9 = sample_start.get("season_h9") or sample_start.get("h9_this_start", 9.0)

    if k9 is not None and k9 >= 9.5:
        return "VELOCITY", f"K/9={round(k9,1)} ≥ 9.5 — high-K power pitcher, needs warm-up inning"

    if k9 is not None and h9 is not None and k9 < 8.0 and h9 < 8.5:
        return "DECEPTION", f"K/9={round(k9,1)} + H/9={round(h9,1)} — contact suppressor, deception-based"

    return "DEFAULT", "Standard profile — no specific start-state modifier"


def _neutral_result() -> dict:
    return {
        "pff":              0.0,
        "label":            "NEUTRAL",
        "hits_tto1_mult":   0.82,
        "ks_tto1_mult":     1.18,
        "tto_late_boost":   0.0,
        "starts_used":      0,
        "weighted_quality": 50.0,
        "baseline_quality": 50.0,
        "starter_profile":  "DEFAULT",
        "boom_bust":        False,
    }
