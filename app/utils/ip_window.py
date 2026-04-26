"""
Expected IP Window — realistic innings-pitched projection per pitcher tier.

Modern starters are managed aggressively. The 2026 MLB average starter exits
around 5.1–5.3 IP. A hard ceiling of 4.8 strips the phantom half-inning that
sportsbooks bake into their lines and forces the formula to project honestly.

Hard windows:
  Minimum returnable: 3.5 IP   (prevents absurd projections for call-ups)
  Maximum returnable: 4.8 IP   (hard ceiling — eliminates sportsbook line noise)

Tier fallbacks (used only when avg_ip_per_start is not available):
  Rookie     0–1 years  →  4.00 IP
  Developing 2–4 years  →  4.50 IP
  Veteran    5–9 years  →  4.75 IP
  Ace        10+ years  →  4.80 IP

When actual avg_ip_per_start IS available, it is clamped to [3.5, 4.8]
and used directly — real data always beats the tier estimate.
"""
from typing import Optional

# Hard floor/ceiling enforced on all projections
IP_FLOOR   = 3.5
IP_CEILING = 4.8

# Tier midpoints (fallback only — used when no season data is available)
# All values must stay at or below IP_CEILING so they are not silently wrong.
_TIER_TABLE = [
    (1,  4.00),   # 0–1 years service
    (4,  4.50),   # 2–4 years
    (9,  4.75),   # 5–9 years
    (99, 4.80),   # 10+ years (ace tier — matches new ceiling)
]


def expected_ip(
    avg_ip_per_start: Optional[float],
    mlb_service_years: Optional[int],
    fragility_ip_cap: Optional[float] = None,
) -> float:
    """
    Return the expected innings-pitched midpoint for one pitcher's start.

    Priority:
      1. Pitcher's actual season avg IP/start (clamped to hard window).
      2. Service-year tier midpoint (if no season data yet).
      3. League default (4.50) when neither is available.

    If a fragility_ip_cap is supplied (from the Fragility Index modifier),
    it is applied AFTER the main ceiling — it can only push the result lower,
    never higher. This keeps it a pure modifier, not a formula change.

    Parameters
    ----------
    avg_ip_per_start : float | None
        This season's IP ÷ GS. None when pitcher has no starts recorded.
    mlb_service_years : int | None
        Full seasons in the majors. None when bio data is unavailable.
    fragility_ip_cap : float | None
        Optional lower cap from the Fragility Index. When supplied, the
        result is further clamped to this value.

    Returns
    -------
    float
        Expected IP in the range [3.5, 4.8] (further reduced if fragility fires).
    """
    if avg_ip_per_start is not None and avg_ip_per_start > 0:
        result = max(IP_FLOOR, min(IP_CEILING, avg_ip_per_start))
    elif mlb_service_years is not None:
        result = IP_FLOOR  # safe fallback before table lookup
        for threshold, midpoint in _TIER_TABLE:
            if mlb_service_years <= threshold:
                result = midpoint
                break
    else:
        # No data at all — use developing-pitcher default
        result = 4.50

    # Fragility modifier: only reduces, never inflates
    if fragility_ip_cap is not None:
        result = min(result, fragility_ip_cap)

    return result


def ip_tier_label(exp_ip: float) -> str:
    """Human-readable label for the IP window (used in logging and the API response)."""
    if exp_ip <= 4.0:
        return "rookie/call-up"
    if exp_ip <= 4.50:
        return "developing"
    if exp_ip <= 4.75:
        return "veteran"
    return "ace"
