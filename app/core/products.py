"""
app/core/products.py — Axiom B2B Product SKU Catalog.

Every data feature in the Axiom platform is a standalone licensable product
with a unique SKU number. B2B customers license the products they need and
receive only those fields in the API response.

Numbering convention:
  #14       Travel & Fatigue Index (MLB)
  #27-#38   Individual MLB data products (à la carte)
  #50-#52   MLB bundles (pre-packaged combinations)
  #60-#66   Individual NFL data products (à la carte)
  #70-#72   NFL bundles (pre-packaged combinations)
  #80-#84   Individual NHL data products (à la carte)
  #90-#92   NHL bundles (pre-packaged combinations)

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
        "name": "HSSI",
        "full_name": "Hits Under Score Index",
        "type": "index",
        "description": (
            "Proprietary 0-100 index scoring the probability that a starting pitcher "
            "allows FEWER hits than the sportsbook prop line. Combines opponent contact "
            "profile, park factor, umpire tendencies, temperature, and bullpen fatigue."
        ),
        "fields": ["hssi", "hssi_grade", "husi", "husi_grade", "projected_hits", "hits_line", "hits_edge"],
        "update_frequency": "daily",
        "available_as_standalone": True,
    },
    28: {
        "sku": 28,
        "name": "KSSI",
        "full_name": "Strikeouts Under Score Index",
        "type": "index",
        "description": (
            "Proprietary 0-100 index scoring the probability that a starting pitcher "
            "records FEWER strikeouts than the sportsbook prop line. Accounts for "
            "opponent swing tendencies, pitch mix, umpire K-zone, and TTO fatigue."
        ),
        "fields": ["kssi", "kssi_grade", "kusi", "kusi_grade", "projected_ks", "k_line", "k_edge"],
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

    # ─────────────────────────────────────────────────────────
    # NFL individual products — SKUs #60–#66
    # ─────────────────────────────────────────────────────────

    60: {
        "sku": 60,
        "name": "QPYI",
        "full_name": "QB Passing Yards Index",
        "sport": "NFL",
        "type": "index",
        "description": (
            "Proprietary 0-100 index scoring the expected passing yard output for "
            "a starting quarterback in a given week. Combines opponent secondary "
            "weakness (OSW), QB skill rating (QSR), game script profile (GSP), "
            "supporting cast (SCB), physical durability (PDR), environmental "
            "conditions (ENS), defensive scheme rating (DSR), and referee crew "
            "tendencies (RCT). Graded A+ through D. Projects passing yards with "
            "PDR rest and park/turf multipliers."
        ),
        "fields": [
            "qpyi_score", "qpyi_grade", "projected_yards",
            "prop_passing_yards_line", "passing_yards_edge",
        ],
        "update_frequency": "weekly",
        "available_as_standalone": True,
    },
    61: {
        "sku": 61,
        "name": "QTDI",
        "full_name": "QB Touchdown Index",
        "sport": "NFL",
        "type": "index",
        "description": (
            "Proprietary 0-100 index scoring the expected touchdown output for a "
            "starting quarterback in a given week. Combines opponent red zone "
            "defense (ORD), QB touchdown rate (QTR), TD-specific game script "
            "(GSP-TD), red zone supporting cast (SCB-TD), and shared blocks PDR, "
            "DSR, ENS, and RCT. Graded A+ through D. Projects touchdowns with "
            "PDR rest multiplier."
        ),
        "fields": [
            "qtdi_score", "qtdi_grade", "projected_tds",
            "prop_td_line", "td_edge",
        ],
        "update_frequency": "weekly",
        "available_as_standalone": True,
    },
    62: {
        "sku": 62,
        "name": "NFL-PDR",
        "full_name": "Physical Durability Rating",
        "sport": "NFL",
        "type": "feed",
        "description": (
            "NFL-unique block measuring a quarterback's physical readiness for the "
            "upcoming game. Tracks sack rate, pressure absorbed, pocket mobility, "
            "hits taken per game, rest window (Thursday night = severe penalty, "
            "bye week = full boost), prior-week snap count, practice participation "
            "trend, official injury designation, age-and-wear curve (John DeWitt "
            "PhD methodology), and rolling game-to-game degradation signal "
            "(Andrew Patton PhD). No equivalent block exists in the MLB formula."
        ),
        "fields": [
            "pdr_score", "pdr_rest", "pdr_inj", "pdr_prac",
            "pdr_sack", "pdr_press", "pdr_hits", "pdr_age",
        ],
        "update_frequency": "weekly",
        "available_as_standalone": True,
    },
    63: {
        "sku": 63,
        "name": "NFL-ENS",
        "full_name": "Environmental Score — NFL",
        "sport": "NFL",
        "type": "feed",
        "description": (
            "Game-day environmental conditions scored for their impact on NFL QB "
            "passing production. Covers dome indicator (100=fully enclosed), wind "
            "speed (>15 mph meaningfully reduces yards and accuracy), temperature "
            "(grip, ball travel, route crispness), precipitation probability, "
            "surface type (artificial turf vs natural grass), altitude, and road "
            "crowd noise impact. Sourced from the National Weather Service API "
            "and pre-mapped stadium database (all 32 NFL venues)."
        ),
        "fields": [
            "ens_score", "ens_dome", "ens_wind", "ens_temp",
            "ens_precip", "ens_turf", "ens_alt", "ens_crowd",
        ],
        "update_frequency": "weekly",
        "available_as_standalone": True,
    },
    64: {
        "sku": 64,
        "name": "NFL-DSR",
        "full_name": "Defensive Scheme Rating",
        "sport": "NFL",
        "type": "feed",
        "description": (
            "New block capturing the most predictive gap in the original formula: "
            "the specific matchup between a QB's tendencies and the opposing "
            "defensive coordinator's scheme. Josh Allen vs Tampa-2 is a "
            "fundamentally different problem than Josh Allen vs single-high man. "
            "Includes QB historical EPA + CPOE against zone coverage, man "
            "coverage, and blitz packages; DC base scheme identity; and head-to-"
            "head QB performance history against this specific DC regardless of "
            "team. Sources: nflfastR (Ben Baldwin), Next Gen Stats (Mike Lopez), "
            "Ted Nguyen (The 33rd Team), Cynthia Frelund (NFL Network)."
        ),
        "fields": [
            "dsr_score", "dsr_zone_eff", "dsr_man_eff",
            "dsr_blitz_eff", "dsr_dc_scheme", "dsr_matchup_hist",
        ],
        "update_frequency": "weekly",
        "available_as_standalone": True,
    },
    65: {
        "sku": 65,
        "name": "NFL-PROPS",
        "full_name": "NFL QB Live Prop Lines",
        "sport": "NFL",
        "type": "feed",
        "description": (
            "Current passing yards and touchdown prop lines for all starting "
            "quarterbacks via The Rundown API. Covers both markets from "
            "DraftKings, FanDuel, BetMGM, Caesars, and more. Includes over/under "
            "odds and implied probability for each side. Props are matched to "
            "starters by fuzzy name matching so ESPN and Rundown name format "
            "differences are handled automatically."
        ),
        "fields": [
            "prop_passing_yards_line", "prop_passing_yards_over",
            "prop_passing_yards_under", "prop_passing_yards_imp_prob",
            "prop_td_line", "prop_td_over", "prop_td_under", "prop_td_imp_prob",
        ],
        "update_frequency": "weekly",
        "available_as_standalone": True,
    },
    66: {
        "sku": 66,
        "name": "QB-PROFILE",
        "full_name": "QB Season Profile",
        "sport": "NFL",
        "type": "feed",
        "description": (
            "QB contextual metadata for the current week. Includes player name, "
            "team, opponent, home/away designation, official injury designation "
            "(Questionable / Doubtful / Out), rest days since last game, season "
            "passing stats (blended yards/game and TDs/game with Bayesian "
            "shrinkage toward league averages), and games started this season "
            "for sample-size context. Foundation data used by all NFL scoring "
            "engines."
        ),
        "fields": [
            "qb_name", "team", "opponent", "is_home",
            "injury_designation", "week", "season_year",
            "projected_yards", "projected_tds",
        ],
        "update_frequency": "weekly",
        "available_as_standalone": True,
    },

    # ─────────────────────────────────────────────────────────
    # NHL individual products — SKUs #80–#84
    # ─────────────────────────────────────────────────────────

    80: {
        "sku": 80,
        "name": "GSAI",
        "full_name": "Goalie Shots-Against Index",
        "sport": "NHL",
        "type": "index",
        "description": (
            "Proprietary 0-100 index scoring the shot volume a goalie is expected "
            "to face in tonight's playoff game. Combines six weighted blocks: "
            "Goalie Save Suppression (29%), Opponent Shooting Quality (24%), "
            "Tactical/Operational deployment (18%), Game Environment including rest "
            "and series game number (16%), Referee Flow Score (8%), and Team "
            "Structure & Coverage (5%). Adjusted by interaction boosts (±8.0 cap) "
            "and volatility penalties including GV1 backup flag, GV6 Game 7, and "
            "GV7 missing data. Graded A+ through D. Projects shots faced (15–50 cap)."
        ),
        "fields": [
            "gsai_score", "grade", "projected_value",
            "gss_score", "osq_score", "top_score",
            "gen_score", "rfs_score", "tsc_score",
        ],
        "update_frequency": "daily",
        "available_as_standalone": True,
    },
    81: {
        "sku": 81,
        "name": "PPSI",
        "full_name": "Player Points Scoring Index",
        "sport": "NHL",
        "type": "index",
        "description": (
            "Proprietary 0-100 index scoring a skater's expected production in "
            "tonight's playoff game. Combines six weighted blocks: Opponent Scoring "
            "Resistance (28%), Player Matchup Rating (22%), Player Efficiency Rating "
            "(18%), Points Operational deployment (14%), Referee PP Score (10%), and "
            "Top-Line Deployment (8%). Adjusted by interaction boosts (±7.0 cap) and "
            "volatility penalties including PV1 linemate injury, PV2 B2B, PV3 hot "
            "goalie, PV4 slump, and PV5 Game 7. Graded A+ through D. Projects all "
            "four betting markets: points (0–5), SOG (0–12), goals (0–3), assists (0–4)."
        ),
        "fields": [
            "ppsi_score", "grade",
            "projected_value",       # market-specific (points | goals | assists | shots_on_goal)
            "osr_score", "pmr_score", "per_score",
            "pop_score", "rps_score", "tld_score",
        ],
        "update_frequency": "daily",
        "available_as_standalone": True,
    },
    82: {
        "sku": 82,
        "name": "NHL-ML",
        "full_name": "NHL ML Engine",
        "sport": "NHL",
        "type": "ml",
        "description": (
            "Per-player Gradient Boosting Regressor trained on each skater's own "
            "2025-26 regular season game log from the NHL public API. Generates "
            "independent projections for all four betting markets: points, goals, "
            "assists, and shots on goal. Applies a mandatory 12% playoff discount "
            "to all outputs — regular season training data systematically "
            "over-projects playoff production. Requires minimum 20 training samples; "
            "returns INSUFFICIENT below that threshold. Includes ALIGNED / LEAN / "
            "SPLIT signal comparing formula vs ML for each market."
        ),
        "fields": [
            "ml_projection", "ml_signal",
            "playoff_discount_applied",
        ],
        "update_frequency": "daily",
        "available_as_standalone": True,
    },
    83: {
        "sku": 83,
        "name": "NHL-PROPS",
        "full_name": "NHL Live Player Prop Lines",
        "sport": "NHL",
        "type": "feed",
        "description": (
            "Current NHL player prop lines from major sportsbooks via The Rundown "
            "API. Covers five markets per player: points, goals, assists, shots on "
            "goal (skaters), and shots faced (goalies). Matched to scored players "
            "by fuzzy name matching at 82% threshold. Includes over/under odds. "
            "Edge calculated as projected_value minus prop_line — positive edge "
            "signals formula projects over the line."
        ),
        "fields": ["prop_line", "edge"],
        "update_frequency": "daily",
        "available_as_standalone": True,
    },
    84: {
        "sku": 84,
        "name": "PLAYER-PROFILE",
        "full_name": "NHL Player Season Profile",
        "sport": "NHL",
        "type": "feed",
        "description": (
            "Player contextual metadata for tonight's game. Includes player name, "
            "team, opponent, position, line number (1-4), power play unit (PP1/PP2), "
            "injury designation from the official NHL injury report, and whether the "
            "player's team is on a back-to-back. For goalies, includes confirmed "
            "starter status. Foundation data used by all NHL scoring engines."
        ),
        "fields": [
            "player_name", "team", "opponent", "position",
            "signal_tag", "playoff_discount_applied",
        ],
        "update_frequency": "daily",
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
        "description": "Core HSSI + KSSI indices. Best for shops that want the signal without the raw data feeds.",
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

    # ── NFL bundles ────────────────────────────────────────────

    70: {
        "sku": 70,
        "name": "NFL-STARTER",
        "sport": "NFL",
        "description": "Core QPYI + QTDI indices. Both QB scoring engines, grades, projections, and prop edges.",
        "includes": [60, 61],
        "discount_note": "Priced below the cost of SKUs 60+61 purchased separately.",
    },
    71: {
        "sku": 71,
        "name": "NFL-INTEL",
        "sport": "NFL",
        "description": "Full QB indices plus physical durability, environmental, and defensive scheme data. The professional NFL tier.",
        "includes": [60, 61, 62, 63, 64],
        "discount_note": "Five NFL products, bundle pricing.",
    },
    72: {
        "sku": 72,
        "name": "NFL-FULL",
        "sport": "NFL",
        "description": "Every Axiom NFL data product. All current and future NFL SKUs included.",
        "includes": [60, 61, 62, 63, 64, 65, 66],
        "discount_note": "Enterprise tier. Includes priority support, custom delivery, and all future NFL SKUs at no additional cost.",
    },

    # ── NHL bundles ────────────────────────────────────────────

    90: {
        "sku": 90,
        "name": "NHL-STARTER",
        "sport": "NHL",
        "description": (
            "Core GSAI + PPSI indices. Both NHL scoring engines, grades, projections, "
            "and all six block scores for goalies and skaters."
        ),
        "includes": [80, 81],
        "discount_note": "Priced below the cost of SKUs 80+81 purchased separately.",
    },
    91: {
        "sku": 91,
        "name": "NHL-INTEL",
        "sport": "NHL",
        "description": (
            "Full NHL indices plus ML engine and live prop lines. "
            "The professional NHL playoff tier — formula score, ML second opinion, "
            "ALIGNED/LEAN/SPLIT divergence signal, and live edges on all five markets."
        ),
        "includes": [80, 81, 82, 83],
        "discount_note": "Four NHL products, bundle pricing.",
    },
    92: {
        "sku": 92,
        "name": "NHL-FULL",
        "sport": "NHL",
        "description": (
            "Every Axiom NHL data product. All current and future NHL SKUs included. "
            "Enterprise tier with priority support and custom delivery."
        ),
        "includes": [80, 81, 82, 83, 84],
        "discount_note": "Enterprise tier. Includes priority support, custom delivery, and all future NHL SKUs at no additional cost.",
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
        # SKU #27 — HSSI (husi/husi_grade kept as backward-compat aliases)
        "hssi": 27,
        "hssi_grade": 27,
        "husi": 27,
        "husi_grade": 27,
        "projected_hits": 27,
        "hits_edge": 27,

        # SKU #28 — KSSI (kusi/kusi_grade kept as backward-compat aliases)
        "kssi": 28,
        "kssi_grade": 28,
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
