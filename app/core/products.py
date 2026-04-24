"""
app/core/products.py — Axiom B2B Product SKU Catalog.

Every data feature in the Axiom platform is a standalone licensable product
with a unique SKU number. B2B customers license the products they need and
receive only those fields in the API response.

Numbering convention:
  #27-#36  Individual data products (à la carte)
  #50-#52  Bundles (pre-packaged combinations)

This file is the single source of truth for all product definitions.
The /v1/products endpoint exposes this catalog publicly.
"""
from typing import Optional

# ─────────────────────────────────────────────────────────────
# Individual product definitions
# ─────────────────────────────────────────────────────────────

PRODUCT_CATALOG: dict[int, dict] = {
    27: {
        "sku": 27,
        "name": "HUSI",
        "full_name": "Hits Under Score Index",
        "type": "index",
        "description": (
            "Proprietary 0-100 index scoring the probability that a starting pitcher "
            "allows FEWER hits than the sportsbook prop line. Combines opponent contact "
            "profile, park factor, umpire tendencies, temperature, and bullpen fatigue."
        ),
        "fields": ["husi", "husi_grade", "projected_hits", "hits_line", "hits_edge"],
        "update_frequency": "daily",
        "available_as_standalone": True,
    },
    28: {
        "sku": 28,
        "name": "KUSI",
        "full_name": "Strikeouts Under Score Index",
        "type": "index",
        "description": (
            "Proprietary 0-100 index scoring the probability that a starting pitcher "
            "records FEWER strikeouts than the sportsbook prop line. Accounts for "
            "opponent swing tendencies, pitch mix, umpire K-zone, and TTO fatigue."
        ),
        "fields": ["kusi", "kusi_grade", "projected_ks", "k_line", "k_edge"],
        "update_frequency": "daily",
        "available_as_standalone": True,
    },
    29: {
        "sku": 29,
        "name": "ML-ENGINE",
        "full_name": "Machine Learning Engine 2",
        "type": "ml",
        "description": (
            "Second-opinion predictions from a Gradient Boosting ML model trained on "
            "every completed game since Opening Day. Surfaces ML-HUSI and ML-KUSI scores "
            "alongside the formula engine for comparison. Includes a divergence signal "
            "(ALIGNED / SLIGHT_DIFF / DIVERGENT / CONFLICT) that flags when the two "
            "engines disagree — a powerful high-confidence filter."
        ),
        "fields": [
            "ml_husi", "ml_kusi", "ml_husi_grade", "ml_kusi_grade",
            "ml_proj_hits", "ml_proj_ks", "husi_signal", "kusi_signal",
        ],
        "update_frequency": "daily",
        "available_as_standalone": True,
    },
    30: {
        "sku": 30,
        "name": "ENS",
        "full_name": "Environmental Score",
        "type": "feed",
        "description": (
            "Game-day environmental conditions scored for their impact on pitcher "
            "performance. Includes park factor (0-100), temperature adjustment, and "
            "air density score derived from NWS atmospheric data. High air density = "
            "suppressed ball flight = pitcher-friendly."
        ),
        "fields": ["ens_park", "ens_temp", "ens_air"],
        "update_frequency": "daily",
        "available_as_standalone": True,
    },
    31: {
        "sku": 31,
        "name": "UMP",
        "full_name": "Umpire Tightness Rating",
        "type": "feed",
        "description": (
            "Proprietary umpire profile built from Statcast pitch-by-pitch data. "
            "Rates each home plate umpire on their K-zone tightness, called strike "
            "percentage above/below average, and impact on hits-per-9 for starters. "
            "Sourced entirely from public MLB Statcast data — no third-party scraping."
        ),
        "fields": ["umpire_name", "umpire_k_score", "umpire_h_score"],
        "update_frequency": "per_umpire_assignment",
        "available_as_standalone": True,
    },
    32: {
        "sku": 32,
        "name": "BFS",
        "full_name": "Bullpen Fatigue Score",
        "type": "feed",
        "description": (
            "Bullpen fatigue coefficient (β_bp) for both the starting pitcher's own "
            "team and the opposing team. Calculated from weighted pitch counts of "
            "leverage arms over the prior 48 hours. A fatigued opponent bullpen "
            "raises HUSI (starter stays in longer = more exposure). A fresh own "
            "bullpen lowers KUSI risk (starter pulled earlier)."
        ),
        "fields": ["bullpen_fatigue_own", "bullpen_fatigue_opp", "bullpen_label_own"],
        "update_frequency": "daily",
        "available_as_standalone": True,
    },
    33: {
        "sku": 33,
        "name": "PFF",
        "full_name": "Pitcher Form Factor",
        "type": "feed",
        "description": (
            "Hot/cold start profile based on the pitcher's last 3 starts relative to "
            "his season baseline. Modifies the TTO1 multipliers in the MGS curve. "
            "Includes starter archetype classification: VELOCITY (needs warm-up inning) "
            "vs DECEPTION (peaks in TTO1, steeper TTO2 cliff)."
        ),
        "fields": ["pff_score", "pff_label", "pff_starter_profile"],
        "update_frequency": "daily",
        "available_as_standalone": True,
    },
    34: {
        "sku": 34,
        "name": "MGS",
        "full_name": "Mid-Game Surge",
        "type": "feed",
        "description": (
            "Times-Through-Order (TTO) fatigue curve combined with pitch count "
            "stepped tiers. Models the 4th-6th inning 'hot bats' surge where "
            "batters have seen the pitcher 2-3 times and the pitcher's arm is "
            "accumulating fatigue. Critical for live in-game scoring updates."
        ),
        "fields": ["mgs_tto", "mgs_inning", "mgs_pitch_count"],
        "update_frequency": "live",
        "available_as_standalone": True,
    },
    35: {
        "sku": 35,
        "name": "PROPS",
        "full_name": "Live Sportsbook Prop Lines",
        "type": "feed",
        "description": (
            "Current pitcher prop lines from major sportsbooks via The Rundown API. "
            "Covers hits allowed (Market 47) and strikeouts (Market 19) from "
            "DraftKings, FanDuel, BetMGM, Caesars, and more. Includes over/under "
            "odds and implied probability for the under."
        ),
        "fields": [
            "hits_line", "hits_under_odds", "hits_implied_under_prob",
            "k_line", "k_under_odds", "k_implied_under_prob",
        ],
        "update_frequency": "live",
        "available_as_standalone": True,
    },
    36: {
        "sku": 36,
        "name": "PITCHER-PROFILE",
        "full_name": "Pitcher Season Profile",
        "type": "feed",
        "description": (
            "Pitcher season statistics and contextual metadata. Includes season "
            "H/9, K/9, ERA, MLB service years, handedness, and the dynamic expected "
            "IP window (3.5-7.5 IP based on service tier). Foundation data used "
            "by all scoring engines."
        ),
        "fields": [
            "pitcher", "pitcher_id", "team", "team_name", "handedness",
            "projected_hits", "projected_ks",
        ],
        "update_frequency": "daily",
        "available_as_standalone": True,
    },
    37: {
        "sku": 37,
        "name": "CATCHER-FRAMING",
        "full_name": "Catcher Framing Module",
        "type": "feed",
        "description": (
            "Proprietary catcher framing adjustment built from Baseball Savant's "
            "Called Strike Rate leaderboard. If the defending catcher's strike rate "
            "exceeds 50%, a +4% multiplier is applied to KUSI (#28) — elite framers "
            "'steal' borderline strikes, creating called Ks the standard formula "
            "would not count. Poor framers (<48%) trigger a -2% KUSI penalty."
        ),
        "fields": [
            "catcher_id", "catcher_name", "catcher_strike_rate",
            "catcher_framing_label", "catcher_kusi_adj",
        ],
        "update_frequency": "daily",
        "available_as_standalone": True,
    },
    14: {
        "sku": 14,
        "name": "TFI",
        "full_name": "Travel & Fatigue Index",
        "type": "feed",
        "description": (
            "Measures the rest deficit and timezone disruption absorbed by the "
            "pitching team before today's start. Two trigger conditions: "
            "(1) Getaway Day — fewer than 16 hours between yesterday's game end "
            "and today's first pitch. (2) Cross-Timezone — team crossed 2+ time "
            "zones overnight. Either condition applies a -7% Reaction Penalty to "
            "HUSI (#27), reducing confidence in the pitcher's hits-under signal."
        ),
        "fields": [
            "tfi_rest_hours", "tfi_tz_shift",
            "tfi_getaway_day", "tfi_cross_timezone",
            "tfi_penalty_pct", "tfi_label",
        ],
        "update_frequency": "daily",
        "available_as_standalone": True,
    },
    38: {
        "sku": 38,
        "name": "VAA-EXT",
        "full_name": "VAA & Extension Perceived Velocity",
        "type": "feed",
        "description": (
            "Live-game pitch physics from the MLB Stats API live feed. "
            "Vertical Approach Angle (VAA): how steeply the ball descends at the plate. "
            "A flat VAA (< -4.5°) means the ball stays in the hitting zone longer — "
            "batters track it easier — increasing contact probability by 10% in HUSI. "
            "Extension: how far in front of the rubber the pitcher releases. "
            "Elite extension (>6.8 ft) shortens the hitter's reaction time, adding "
            "+1.5 mph perceived velocity and boosting the KUSI strikeout signal."
        ),
        "fields": [
            "vaa_degrees", "extension_ft",
            "vaa_flat", "extension_elite",
            "vaa_contact_penalty", "extension_velo_boost",
        ],
        "update_frequency": "live",
        "available_as_standalone": True,
    },
}

# ─────────────────────────────────────────────────────────────
# Bundle definitions
# ─────────────────────────────────────────────────────────────

BUNDLES: dict[int, dict] = {
    50: {
        "sku": 50,
        "name": "STARTER PACK",
        "description": "Core HUSI + KUSI indices. Best for shops that want the signal without the raw data feeds.",
        "includes": [27, 28],
        "discount_note": "Priced below the cost of SKUs 27+28 purchased separately.",
    },
    51: {
        "sku": 51,
        "name": "INTELLIGENCE PACK",
        "description": "Full indices plus ML engine, environment, and umpire data. The professional tier.",
        "includes": [27, 28, 29, 30, 31],
        "discount_note": "Five products, bundle pricing.",
    },
    52: {
        "sku": 52,
        "name": "FULL ACCESS",
        "description": "Every Axiom data product. All current and future SKUs included.",
        "includes": list(PRODUCT_CATALOG.keys()),
        "discount_note": "Enterprise pricing. Includes priority support and custom delivery.",
    },
}


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def get_product(sku: int) -> Optional[dict]:
    """Return the product definition for a given SKU, or None if not found."""
    return PRODUCT_CATALOG.get(sku) or BUNDLES.get(sku)


def product_tags_for_response() -> dict[str, int]:
    """
    Returns a mapping of API response field names to their SKU number.
    Included in every /v1/pitchers/today response so B2B customers
    know exactly which licensed product each data field belongs to.
    """
    return {
        # SKU #27 — HUSI
        "husi": 27,
        "husi_grade": 27,
        "projected_hits": 27,
        "hits_edge": 27,

        # SKU #28 — KUSI
        "kusi": 28,
        "kusi_grade": 28,
        "projected_ks": 28,
        "k_edge": 28,

        # SKU #29 — ML Engine 2
        "ml_husi": 29,
        "ml_kusi": 29,
        "ml_husi_grade": 29,
        "ml_kusi_grade": 29,
        "ml_proj_hits": 29,
        "ml_proj_ks": 29,
        "husi_signal": 29,
        "kusi_signal": 29,

        # SKU #30 — Environmental Score
        "ens_park": 30,
        "ens_temp": 30,
        "ens_air": 30,

        # SKU #32 — Bullpen Fatigue Score
        "bullpen_fatigue_own": 32,
        "bullpen_fatigue_opp": 32,

        # SKU #33 — Pitcher Form Factor
        "pff_score": 33,
        "pff_label": 33,

        # SKU #34 — Mid-Game Surge
        "mgs_tto": 34,

        # SKU #35 — Props
        "hits_line": 35,
        "hits_under_odds": 35,
        "k_line": 35,
        "k_under_odds": 35,

        # SKU #36 — Pitcher Profile (base data)
        "pitcher": 36,
        "team": 36,
        "team_name": 36,
        "handedness": 36,

        # SKU #37 — Catcher Framing
        "catcher_name": 37,
        "catcher_strike_rate": 37,
        "catcher_framing_label": 37,
        "catcher_kusi_adj": 37,

        # SKU #14 — Travel & Fatigue Index
        "tfi_rest_hours": 14,
        "tfi_tz_shift": 14,
        "tfi_getaway_day": 14,
        "tfi_cross_timezone": 14,
        "tfi_penalty_pct": 14,
        "tfi_label": 14,

        # SKU #38 — VAA & Extension
        "vaa_degrees": 38,
        "extension_ft": 38,
        "vaa_flat": 38,
        "extension_elite": 38,
        "vaa_contact_penalty": 38,
        "extension_velo_boost": 38,
    }
