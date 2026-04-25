"""
Expected IP Window — realistic innings-pitched projection per pitcher tier.

Modern starters are managed aggressively. The 2026 MLB average starter exits
around 5.1 IP. A hard ceiling of 5.5 prevents the formula from over-projecting
Ks for pitchers who will not see the 7th inning in most starts.

Hard windows:
  Minimum returnable: 3.5 IP   (prevents absurd projections for call-ups)
  Maximum returnable: 5.5 IP   (hard ceiling — modern bullpen management reality)

Tier fallbacks (used only when avg_ip_per_start is not available):
  Rookie     0–1 years  →  4.00 IP
  Developing 2–4 years  →  4.75 IP
  Veteran    5–9 years  →  5.25 IP
  Ace        10+ years  →  5.50 IP

When actual avg_ip_per_start IS available, it is clamped to [3.5, 5.5]
and used directly — real data always beats the tier estimate.
"""
from typing import Optional

# Hard floor/ceiling enforced on all projections
IP_FLOOR   = 3.5
IP_CEILING = 5.5

# Tier midpoints (fallback only)
_TIER_TABLE = [
    (1,  4.00),   # 0–1 years service
    (4,  4.75),   # 2–4 years
    (9,  5.25),   # 5–9 years
    (99, 5.50),   # 10+ years (ace tier)
]


def expected_ip(
    avg_ip_per_start: Optional[float],
    mlb_service_years: Optional[int],
) -> float:
    """
    Return the expected innings-pitched midpoint for one pitcher's start.

    Priority:
      1. Pitcher's actual season avg IP/start (clamped to hard window).
      2. Service-year tier midpoint (if no season data yet).
      3. League default (5.25) when neither is available.

    Parameters
    ----------
    avg_ip_per_start : float | None
        This season's IP ÷ GS. None when pitcher has no starts recorded.
    mlb_service_years : int | None
        Full seasons in the majors. None when bio data is unavailable.

    Returns
    -------
    float
        Expected IP in the range [3.5, 5.5].
    """
    if avg_ip_per_start is not None and avg_ip_per_start > 0:
        return max(IP_FLOOR, min(IP_CEILING, avg_ip_per_start))

    if mlb_service_years is not None:
        for threshold, midpoint in _TIER_TABLE:
            if mlb_service_years <= threshold:
                return midpoint

    # No data at all — use developing-pitcher default
    return 5.25


def ip_tier_label(exp_ip: float) -> str:
    """Human-readable label for the IP window (used in logging and the API response)."""
    if exp_ip <= 4.0:
        return "rookie/call-up"
    if exp_ip <= 4.75:
        return "developing"
    if exp_ip <= 5.25:
        return "veteran"
    return "ace"
