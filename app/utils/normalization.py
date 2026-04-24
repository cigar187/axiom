"""
Reusable normalization layer for the Axiom scoring engine.

Every raw feature is converted to a 0-100 score where:
  100 = strongest support for the UNDER
  0   = weakest support for the UNDER

Two methods are supported:
  - "zscore"     → score = clamp(50 + 15 * z, 0, 100)
  - "percentile" → score = clamp(percentile_rank * 100, 0, 100)

Each feature can be declared with:
  - method:     "zscore" | "percentile"
  - direction:  "normal" (higher raw = higher score) | "reverse" (higher raw = lower score)
  - fallback:   value to return when raw data is missing (usually 50 = neutral)
  - min_sample: minimum number of data points required to trust the score

Usage example:
    spec = FeatureSpec(source_field="babip_against", method="zscore", direction="reverse", fallback=50)
    score = normalize(raw_value=0.310, population=[0.280, 0.295, ...], spec=spec)
"""

import math
import statistics
from dataclasses import dataclass, field
from typing import Literal, Optional


# ─────────────────────────────────────────────────────────────
# FeatureSpec — describes how one feature should be normalized
# ─────────────────────────────────────────────────────────────

@dataclass
class FeatureSpec:
    source_field: str
    method: Literal["zscore", "percentile"] = "zscore"
    direction: Literal["normal", "reverse"] = "normal"
    fallback: float = 50.0          # returned when raw value is None or sample too small
    min_sample: int = 10            # minimum population size to compute a meaningful score


# ─────────────────────────────────────────────────────────────
# Core helpers
# ─────────────────────────────────────────────────────────────

def clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    """Force a value to stay within [low, high]."""
    return max(low, min(high, value))


def zscore_to_score(z: float) -> float:
    """Convert a z-score to a 0-100 score. z=0 → 50, z=+3.3 → 100, z=-3.3 → 0."""
    return clamp(50.0 + 15.0 * z)


def percentile_rank(value: float, population: list[float]) -> float:
    """
    Returns the percentile of `value` within `population` as a 0-1 float.
    Uses the standard 'less than or equal' definition.
    """
    if not population:
        return 0.5
    below_or_equal = sum(1 for v in population if v <= value)
    return below_or_equal / len(population)


# ─────────────────────────────────────────────────────────────
# Main normalize function
# ─────────────────────────────────────────────────────────────

def normalize(
    raw_value: Optional[float],
    population: Optional[list[float]],
    spec: FeatureSpec,
) -> float:
    """
    Convert a raw feature value to a 0-100 score according to the FeatureSpec.

    Returns spec.fallback when:
      - raw_value is None
      - population is None or smaller than spec.min_sample
    """
    if raw_value is None:
        return spec.fallback

    pop = population or []

    if len(pop) < spec.min_sample:
        # Not enough comparison data — return neutral fallback
        return spec.fallback

    if spec.method == "zscore":
        try:
            mean = statistics.mean(pop)
            stdev = statistics.stdev(pop)
        except statistics.StatisticsError:
            return spec.fallback

        if stdev == 0:
            return spec.fallback

        z = (raw_value - mean) / stdev
        score = zscore_to_score(z)

    elif spec.method == "percentile":
        pct = percentile_rank(raw_value, pop)
        score = clamp(pct * 100.0)

    else:
        return spec.fallback

    # Reverse direction: higher raw value should score LOWER
    if spec.direction == "reverse":
        score = 100.0 - score

    return round(score, 2)


# ─────────────────────────────────────────────────────────────
# Convenience: normalize directly from a z-score (no population needed)
# Useful when the caller has already computed z externally.
# ─────────────────────────────────────────────────────────────

def score_from_z(z: float, direction: Literal["normal", "reverse"] = "normal") -> float:
    score = zscore_to_score(z)
    if direction == "reverse":
        score = 100.0 - score
    return round(score, 2)


# ─────────────────────────────────────────────────────────────
# Implied probability helper
# ─────────────────────────────────────────────────────────────

def american_odds_to_implied_prob(american_odds: float) -> float:
    """
    Convert American (moneyline) odds to implied probability (0-1).
    e.g. -110 → 0.524,  +120 → 0.455
    """
    if american_odds < 0:
        return (-american_odds) / (-american_odds + 100.0)
    else:
        return 100.0 / (american_odds + 100.0)
