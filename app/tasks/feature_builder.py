"""
Feature builder — maps raw data from all providers into PitcherFeatureSet objects.

This is the ONLY place where provider data is translated into scoring engine inputs.
The scoring engine (husi.py / kusi.py) never touches raw API data directly.

Normalization of all season stats to 0-100 scores happens here using the
normalization utility (zscore method across all pitchers fetched today).
"""
import statistics
from datetime import date
from typing import Optional

from app.core.features import PitcherFeatureSet
from app.utils.normalization import score_from_z, clamp
from app.utils.ip_window import expected_ip
from app.utils.logging import get_logger

log = get_logger("feature_builder")

# Park factor scores (0-100) for all 30 MLB venues.
# 100 = most pitcher-friendly (suppresses hits), 50 = neutral, 0 = extreme hitter park.
#
# Calibrated from 3-year (2022-2024) Statcast Hits Park Factor index.
# Standard baseball scale: 100 = neutral, 120 = 20% more hits (like Coors).
# Conversion: our_score = 50 - (bf_index - 100) * 1.5
#
# Biggest corrections vs. prior version:
#   Chase Field (ARI): 55 → 38  (hot + 1082ft altitude, heavily hitter-friendly)
#   Globe Life Field (TEX): 50 → 42  (Texas heat, hitter-friendly when roof open)
#   Oracle Park (SF): 72 → 64  (spacious but not as extreme as we had it)
#   Petco Park (SD): 70 → 62  (pitcher-friendly but adjusted down)
#   PNC Park (PIT): 65 → 58  (adjusted down, not as extreme)
#   Kauffman Stadium (KC): 60 → 54  (more neutral than we had)
PARK_FACTOR_SCORES: dict[str, float] = {
    # ── EXTREME HITTER PARKS (score < 35 → park_extreme flag triggers HV9)
    "coors field":              20,   # COL — altitude 5280ft, worst pitcher park in baseball
    "great american ball park": 34,   # CIN — small, hitter-friendly dimensions
    "chase field":              38,   # ARI — 1082ft altitude + desert heat, very hitter-friendly

    # ── HITTER-FRIENDLY (35-47)
    "citizens bank park":       38,   # PHI — short porch in right, hitter-friendly
    "fenway park":              41,   # BOS — short left field wall, above-average hits
    "yankee stadium":           44,   # NYY — short porch in right
    "oriole park at camden yards": 44, # BAL — open since 1992, slightly hitter-friendly
    "camden yards":             44,   # BAL — alternate name
    "globe life field":         42,   # TEX — Texas heat; dome but hitter-friendly when open
    "wrigley field":            48,   # CHC — wind-dependent, near-neutral over full season
    "rogers centre":            46,   # TOR — dome; dimensions slightly hitter-friendly

    # ── NEUTRAL (48-53)
    "truist park":              52,   # ATL — neutral
    "guaranteed rate field":    51,   # CWS — slightly above neutral
    "rate field":               51,   # CWS — alternate short name
    "minute maid park":         50,   # HOU — dome/retractable, neutral
    "loandepot park":           50,   # MIA — dome, neutral
    "nationals park":           52,   # WSH — slightly pitcher-friendly

    # ── PITCHER-FRIENDLY (54-65)
    "dodger stadium":           54,   # LAD — spacious, mild climate
    "target field":             55,   # MIN — cold weather suppresses scoring
    "angel stadium":            53,   # LAA — spacious foul territory
    "progressive field":        55,   # CLE — cold + spacious
    "comerica park":            57,   # DET — deep power alleys
    "american family field":    54,   # MIL — dome, pitcher-friendly dimensions
    "busch stadium":            54,   # STL — neutral-to-pitcher-friendly
    "kauffman stadium":         54,   # KC — very spacious outfield
    "tropicana field":          56,   # TB — dome, artificial turf, pitcher-friendly
    "citi field":               57,   # NYM — spacious, cold spring/fall
    "pnc park":                 58,   # PIT — deep dimensions, pitcher-friendly

    # ── STRONG PITCHER PARKS (62-70)
    "t-mobile park":            60,   # SEA — marine layer, one of better pitcher parks
    "petco park":               62,   # SD — spacious, marine layer, pitcher-friendly
    "oracle park":              64,   # SF — marine layer + dimensions, strong pitcher park
    "sutter health park":       54,   # OAK/SAC — neutral placeholder (A's in Sacramento 2025+)
    "oakland coliseum":         56,   # OAK — legacy, spacious and pitcher-friendly
}

DOME_VENUES = {
    "tropicana field", "minute maid park", "american family field",
    "globe life field", "rogers centre", "loanDepot park",
}


def build_features(
    pitcher_data: dict,
    props: dict,
    umpire_profiles: dict,
    all_pitchers_data: dict,
    game_info: dict,
    target_date: date,
    team_hitting_stats: dict = None,
    lineup_data: dict = None,
    hook_data: dict = None,
    bullpen_own: dict = None,
    bullpen_opp: dict = None,
    catcher_framing: dict = None,
    travel_fatigue: dict = None,
    vaa_data: dict = None,
    swing_profiles: dict = None,
    oaa_data: dict = None,
    arsenal_data: dict = None,
) -> PitcherFeatureSet:
    """
    Build a PitcherFeatureSet for one pitcher.

        Args:
        pitcher_data:      Single pitcher dict from MLBStatsAdapter
        props:             All pitcher props from RundownAdapter { name_lower: {strikeouts: ..., hits_allowed: ...} }
        umpire_profiles:   Umpire data keyed by umpire_id
        all_pitchers_data: All pitchers dict (used for population-level z-score normalization)
        game_info:         Game dict from MLBStatsAdapter
        target_date:       The date being processed
        catcher_framing:   Dict from catcher_service.get_framing_data() for the defending catcher
        travel_fatigue:    Dict from travel_fatigue.compute_travel_fatigue_index() for pitching team
        vaa_data:          Dict with vaa_degrees and extension_ft from live game feed
    """
    pid = pitcher_data["pitcher_id"]
    name = pitcher_data["pitcher_name"]
    game_id = pitcher_data["game_id"]

    log.info("Building features", pitcher=name, pitcher_id=pid)

    f = PitcherFeatureSet(
        pitcher_id=pid,
        pitcher_name=name,
        game_id=game_id,
        team=pitcher_data.get("team", ""),
        team_id_numeric=str(pitcher_data.get("team_id", "") or ""),
        opponent=pitcher_data.get("opponent", ""),
        handedness=pitcher_data.get("handedness"),
        lineup_confirmed=pitcher_data.get("confirmed", False),
        bullpen_data_available=bool(pitcher_data.get("bullpen_logs")),
    )

    # ── Raw season stats
    f.season_hits_per_9 = pitcher_data.get("season_hits_per_9")
    f.season_k_per_9 = pitcher_data.get("season_k_per_9")

    # ── Season ERA — stored for logging/reference only, no longer drives a penalty
    era = pitcher_data.get("season_era") or pitcher_data.get("era")
    if era is not None:
        f.season_era_raw = float(era)

    # ── Hard Hit Rate Tier (replaces ERA tier as HV10 — Tango/SABR validated)
    # ERA is defense-dependent and lagging; hard-hit rate is the pitcher's true contact quality allowed.
    # Source: pitcher_data["season_hard_hit_pct"] from MLB Stats / Baseball Savant
    hard_hit = pitcher_data.get("season_hard_hit_pct")
    if hard_hit is not None:
        hard_hit = float(hard_hit)
        f.season_hard_hit_pct = hard_hit
        if hard_hit > 40.0:
            f.hard_hit_tier = "DISASTER"    # pitcher is getting crushed — ball is jumping off the bat
        elif hard_hit > 35.0:
            f.hard_hit_tier = "STRUGGLING"  # above-average hard contact allowed
        elif hard_hit < 28.0:
            f.hard_hit_tier = "ELITE"       # pitcher is suppressing hard contact — a genuine hit suppressor
        else:
            f.hard_hit_tier = "NORMAL"
    # else: defaults to NORMAL — no penalty, no boost, neutral

    # ── Raw GB% — stored for direct multiplier in husi.py (not normalized)
    gb_pct = pitcher_data.get("season_gb_pct")
    if gb_pct is not None:
        f.season_gb_pct = float(gb_pct)

    # ── IP window — drives realistic per-start projection baseline
    f.avg_ip_per_start = pitcher_data.get("avg_ip_per_start")   # actual season avg IP/GS
    f.mlb_service_years = pitcher_data.get("mlb_service_years") # tier fallback when no starts

    # ── Prop lines (match by lowercase name)
    name_key = name.strip().lower()
    pitcher_props = props.get(name_key, {})
    if pitcher_props.get("strikeouts"):
        sp = pitcher_props["strikeouts"]
        f.k_line = sp.get("line")
        f.k_over_odds = sp.get("over_odds")
        f.k_under_odds = sp.get("under_odds")
    if pitcher_props.get("hits_allowed"):
        hp = pitcher_props["hits_allowed"]
        f.hits_line = hp.get("line")
        f.hits_over_odds = hp.get("over_odds")
        f.hits_under_odds = hp.get("under_odds")

    # ── Build population lists for cross-pitcher z-score normalization
    all_h9 = [p["season_hits_per_9"] for p in all_pitchers_data.values() if p.get("season_hits_per_9")]
    all_k9 = [p["season_k_per_9"] for p in all_pitchers_data.values() if p.get("season_k_per_9")]
    all_bb9 = [p.get("season_bb_per_9") for p in all_pitchers_data.values() if p.get("season_bb_per_9")]
    # Use season_go_ao (GO/AO ratio from MLB Stats API) for z-score normalization.
    # This is ALWAYS on the same scale (0.5-2.5) regardless of whether Statcast data
    # is available — avoids mixing GO/AO ratios with Statcast GB percentages in the
    # population list. season_gb_pct is used separately for the husi.py direct suppressor.
    all_go_ao = [p.get("season_go_ao") for p in all_pitchers_data.values() if p.get("season_go_ao")]
    # Hard-hit rate population — used to differentiate pcs_bara from pcs_hha (no double-counting)
    all_hhr = [p.get("season_hard_hit_pct") for p in all_pitchers_data.values() if p.get("season_hard_hit_pct")]
    # WHIP proxy (H/9 + BB/9) — combined baserunner rate for ops_traffic
    all_whip_proxy = [
        p["season_hits_per_9"] + p["season_bb_per_9"]
        for p in all_pitchers_data.values()
        if p.get("season_hits_per_9") and p.get("season_bb_per_9")
    ]
    # K/BB ratio — stuff × control combined; a distinct signal from K/9 or BB/9 alone
    # Used for per_ppa (pitching efficiency). High ratio = works ahead, efficient per AB.
    all_k_bb = [
        p["season_k_per_9"] / p["season_bb_per_9"]
        for p in all_pitchers_data.values()
        if p.get("season_k_per_9") and p.get("season_bb_per_9") and p["season_bb_per_9"] > 0
    ]

    # ── PCS — Pitcher Contact Suppression
    # Ground-ball GO/AO ratio: higher = more grounders = better for under → normal direction
    if pitcher_data.get("season_go_ao") and len(all_go_ao) >= 3:
        f.pcs_gb = _zscore_score(pitcher_data["season_go_ao"], all_go_ao, direction="normal")
    # Hits/9: lower = better for under. Drives pcs_hha (hits allowed per 9).
    if pitcher_data.get("season_hits_per_9") and len(all_h9) >= 3:
        f.pcs_hha = _zscore_score(pitcher_data["season_hits_per_9"], all_h9, direction="reverse")
    # Hard-hit rate %: drives pcs_bara (barrel/hard-contact rate against).
    # Uses Statcast hard_hit_pct — a DIFFERENT signal than hits/9.
    # Eliminates multicollinearity: pcs_bara was previously set equal to pcs_hha (both using hits/9),
    # giving hits/9 a combined 0.30 weight in PCS when it should only carry 0.14.
    hhr = pitcher_data.get("season_hard_hit_pct")
    if hhr is not None and len(all_hhr) >= 3:
        # High hard-hit rate = pitcher allows more hard contact = bad for hits under → reverse
        f.pcs_bara = _zscore_score(float(hhr), all_hhr, direction="reverse")
    elif pitcher_data.get("season_hits_per_9") and len(all_h9) >= 3:
        # Fallback only when Statcast HHR unavailable — avoids None but marks as imperfect
        f.pcs_bara = _zscore_score(pitcher_data["season_hits_per_9"], all_h9, direction="reverse")
    # Walk rate: lower = better → reverse direction
    if pitcher_data.get("season_bb_per_9") and len(all_bb9) >= 3:
        f.pcs_cmd = _zscore_score(pitcher_data["season_bb_per_9"], all_bb9, direction="reverse")

    # ── OCR — Opponent Contact Rate (from the OPPOSING TEAM's hitting stats)
    # This is the most important correction: OCR measures how contact-prone the opposing lineup
    # is — NOT how many Ks the pitcher gets. A contact-heavy lineup hitting INTO a pitcher with
    # a high K/9 is STILL a contact-heavy lineup and supports the K under.
    team_stats = team_hitting_stats or {}
    all_team_stats = list(team_stats.values())
    all_k_rates = [t["k_rate"] for t in all_team_stats if t.get("k_rate")]
    all_contact_rates = [t["contact_rate"] for t in all_team_stats if t.get("contact_rate")]
    all_bb_rates = [t["bb_rate"] for t in all_team_stats if t.get("bb_rate")]

    # Determine opponent team ID from game info
    opponent_team_id = None
    home_team_id = game_info.get("home_team_id", "")
    away_team_id = game_info.get("away_team_id", "")
    pitcher_team_id = pitcher_data.get("team_id", "")
    if pitcher_team_id == home_team_id:
        opponent_team_id = away_team_id
    elif pitcher_team_id == away_team_id:
        opponent_team_id = home_team_id

    opp_stats = team_stats.get(str(opponent_team_id), {}) if opponent_team_id else {}

    if opp_stats and len(all_k_rates) >= 20:
        opp_k_rate = opp_stats.get("k_rate", 25.0)
        opp_contact_rate = opp_stats.get("contact_rate", 75.0)
        opp_bb_rate = opp_stats.get("bb_rate", 8.0)

        # High opponent K rate = lineup strikes out a lot = EASY to K = good for pitcher
        f.ocr_k = _zscore_score(opp_k_rate, all_k_rates, direction="normal")
        # High contact rate = lineup puts ball in play = HARD to K = bad for pitcher
        f.ocr_con = _zscore_score(opp_contact_rate, all_contact_rates, direction="reverse")
        # Higher BB rate = more patient = harder to K = bad for pitcher
        f.ocr_disc = _zscore_score(opp_bb_rate, all_bb_rates, direction="reverse")

        log.info("OCR from opponent hitting stats",
                 pitcher=pitcher_data.get("pitcher_name"),
                 opponent_team_id=opponent_team_id,
                 opp_k_rate=opp_k_rate,
                 ocr_k=f.ocr_k,
                 ocr_con=f.ocr_con)
    else:
        # Fallback neutral if team stats unavailable
        f.ocr_k = 50.0
        f.ocr_con = 50.0
        f.ocr_disc = 50.0

    # ── OWC — Opponent Weaknesses vs Contact (from opposing team's season hitting stats)
    # Replaces static 50.0 defaults with real data from the same team_hitting_stats dict
    # already fetched for OCR. No additional API calls needed.
    #
    # Convention: HIGHER OWC score = lineup is WEAKER at contact = BETTER for pitcher
    #   (supports the hits-allowed under prop)
    #
    # Mapping:
    #   owc_babip ← team AVG     (high AVG lineup = dangerous = lower score)  [reverse]
    #   owc_hh    ← team SLG     (high SLG = hard contact tendency = lower)   [reverse]
    #   owc_bar   ← team SLG     (same data; SLG best proxy for barrel rate)  [reverse]
    #   owc_ld    ← contact_rate (high contact rate = more balls in play)      [reverse]
    #   owc_xba   ← team OBP     (high OBP = patient, dangerous = lower)      [reverse]
    #   owc_bot3  ← team k_rate  (high K rate = weak bottom of lineup = higher)[normal]
    #   owc_topheavy: computed later from individual batter K rates (lineup data)
    if opp_stats and len(all_team_stats) >= 20:
        all_avg = [t["avg"] for t in all_team_stats if t.get("avg")]
        all_slg = [t["slg"] for t in all_team_stats if t.get("slg")]
        all_obp = [t["obp"] for t in all_team_stats if t.get("obp")]

        opp_avg = opp_stats.get("avg", 0.250)
        opp_slg = opp_stats.get("slg", 0.400)
        opp_obp = opp_stats.get("obp", 0.320)
        opp_k   = opp_stats.get("k_rate", 20.0)
        opp_con = opp_stats.get("contact_rate", 75.0)

        if len(all_avg) >= 20:
            f.owc_babip = _zscore_score(opp_avg, all_avg, direction="reverse")
        if len(all_slg) >= 20:
            # owc_hh: opponent SLG — measures overall hard-contact / extra-base tendency.
            f.owc_hh = _zscore_score(opp_slg, all_slg, direction="reverse")
        if len(all_avg) >= 20 and len(all_slg) >= 20:
            # owc_bar: Isolated Power (ISO = SLG - AVG) — measures pure extra-base power
            # independent of batting average. This is a DIFFERENT signal from SLG:
            # ISO strips out singles and measures only the batter's ability to drive
            # the ball for extra bases (doubles, triples, HRs) — i.e., barrel tendency.
            # Previously owc_bar was set equal to owc_hh (same SLG value), giving SLG
            # a combined 0.38 weight (0.20 + 0.18) in OWC — a clear multicollinearity bug.
            all_iso = [
                t["slg"] - t["avg"]
                for t in all_team_stats
                if t.get("slg") is not None and t.get("avg") is not None
            ]
            opp_iso = opp_slg - opp_avg
            if len(all_iso) >= 20:
                f.owc_bar = _zscore_score(opp_iso, all_iso, direction="reverse")
        if len(all_contact_rates) >= 20:
            f.owc_ld = _zscore_score(opp_con, all_contact_rates, direction="reverse")
        if len(all_obp) >= 20:
            f.owc_xba = _zscore_score(opp_obp, all_obp, direction="reverse")
        if len(all_k_rates) >= 20:
            f.owc_bot3 = _zscore_score(opp_k, all_k_rates, direction="normal")

        log.info("OWC from team hitting stats",
                 pitcher=pitcher_data.get("pitcher_name"),
                 opponent_team_id=opponent_team_id,
                 opp_avg=opp_avg, owc_babip=round(f.owc_babip or 50.0, 1),
                 opp_slg=opp_slg, owc_hh=round(f.owc_hh or 50.0, 1),
                 opp_obp=opp_obp, owc_xba=round(f.owc_xba or 50.0, 1),
                 opp_k=opp_k, owc_bot3=round(f.owc_bot3 or 50.0, 1))

    # per_velo from K/9 (fallback when SwStr% not available)
    if pitcher_data.get("season_k_per_9") and len(all_k9) >= 3:
        f.per_velo = _zscore_score(pitcher_data["season_k_per_9"], all_k9, direction="normal")

    # per_putw: Swinging Strike Rate (SwStr%) — Eno Sarris / Stuff+ validated as more predictive than K/9.
    # SwStr% directly measures how often the pitcher generates whiffs, independent of ball/strike calls.
    # Falls back to K/9 z-score normalization if SwStr% is not available.
    swstr = pitcher_data.get("season_swstr_pct")
    if swstr is not None and swstr > 0:
        f.season_swstr_pct = float(swstr)
        # Collect population-level SwStr% for z-score normalization
        all_swstr = [
            p.get("season_swstr_pct") for p in all_pitchers_data.values()
            if p.get("season_swstr_pct")
        ]
        if len(all_swstr) >= 3:
            f.per_putw = _zscore_score(swstr, all_swstr, direction="normal")
            log.info("SwStr% wired to per_putw",
                     pitcher=name, swstr_pct=swstr, per_putw=f.per_putw)
        else:
            # Not enough pitchers with SwStr% today — fall back to K/9
            if pitcher_data.get("season_k_per_9") and len(all_k9) >= 3:
                f.per_putw = _zscore_score(pitcher_data["season_k_per_9"], all_k9, direction="normal")
    elif pitcher_data.get("season_k_per_9") and len(all_k9) >= 3:
        # No SwStr% available — K/9 fallback
        f.per_putw = _zscore_score(pitcher_data["season_k_per_9"], all_k9, direction="normal")

    # ── PER — walk rate as BB score
    if pitcher_data.get("season_bb_per_9") and len(all_bb9) >= 3:
        # Higher BB/9 = less efficient = bad for under → reverse
        f.per_bb = _zscore_score(pitcher_data["season_bb_per_9"], all_bb9, direction="reverse")

    # ── ENS — Environmental Score
    park_name = (game_info.get("park") or "").lower().strip()
    park_score = PARK_FACTOR_SCORES.get(park_name, 50.0)
    f.ens_park = park_score

    # ── Park Factor Direct Override (SKU #27 calibration)
    # The ENS block only captures ~5.6% of the HUSI signal from park factors,
    # which is far too weak for extreme venues like Coors Field (+18% hits IRL).
    # This direct multiplier is applied to projected_hits in husi.py.
    # Formula: park_hits_multiplier = 1.0 + ((50 - park_score) / 50) * 0.30
    # Range: 0.82 (Oracle/Petco) to 1.18 (Coors)
    raw_park_mult = 1.0 + ((50.0 - park_score) / 50.0) * 0.30
    f.park_hits_multiplier = round(max(0.80, min(1.25, raw_park_mult)), 3)
    f.park_extreme = park_score < 40  # triggers HV9 volatility penalty (Coors, GABP, Chase, Citizens Bank, Globe Life)
    log.info("Park factor override computed",
             pitcher=pitcher_data.get("pitcher_name"),
             park_name=park_name,
             park_score=park_score,
             park_hits_multiplier=f.park_hits_multiplier,
             park_extreme=f.park_extreme)

    # Wind direction score: "in from center/left/right" helps pitcher; "out" hurts
    wind = (game_info.get("wind_direction") or "").lower()
    if "in" in wind:
        f.ens_windin = 70.0
    elif "out" in wind:
        f.ens_windin = 30.0
    elif park_name in DOME_VENUES or game_info.get("is_dome"):
        f.ens_windin = 65.0  # no wind factor in dome = slight advantage
    else:
        f.ens_windin = 50.0

    # Temperature: cold air = ball doesn't carry = better for pitcher
    temp = game_info.get("temperature_f")
    if temp is not None:
        if temp < 50:
            f.ens_temp = 72.0   # cold, ball dies
        elif temp < 65:
            f.ens_temp = 60.0
        elif temp < 80:
            f.ens_temp = 50.0
        else:
            f.ens_temp = 35.0   # hot, ball carries
    else:
        f.ens_temp = 50.0

    # Dome/roof score
    if park_name in DOME_VENUES or game_info.get("is_dome"):
        f.ens_roof = 70.0
    else:
        f.ens_roof = 50.0

    # Air density score from NWS (thin air = ball carries = more hits = lower score for pitcher)
    # Comes from weather.py calculation: altitude + temp + humidity combined
    air_density = game_info.get("air_density_score")
    if air_density is not None:
        f.ens_air = float(air_density)
    else:
        # Fallback: estimate from altitude of known parks
        if "coors" in park_name or "denver" in park_name:
            f.ens_air = 85.0   # Coors Field — extremely thin air
        elif game_info.get("is_dome"):
            f.ens_air = 50.0   # Dome — controlled environment
        else:
            f.ens_air = 50.0   # Unknown — neutral

    # ── Umpire features
    ump_game_id = game_id
    ump_profile = umpire_profiles.get(ump_game_id)
    if ump_profile:
        f.umpire_confirmed = ump_profile.get("confirmed", False)
        f.uhs_cstr  = ump_profile.get("called_strike_rate", 50.0)
        f.uhs_zone  = ump_profile.get("zone_accuracy", 50.0)
        f.uhs_early = ump_profile.get("early_count_strikes", 50.0)
        f.uhs_weak  = ump_profile.get("weak_contact_tendency", 50.0)
        f.uks_tight = ump_profile.get("zone_tightness", 50.0)
        f.uks_cstrl = 50.0   # neutral — avoids double-counting CSR (already in uks_tight)
        f.uks_2exp  = ump_profile.get("two_strike_expansion", 50.0)
        f.uks_count = 50.0   # neutral — avoids double-counting zone_accuracy (already in uks_2exp)
    else:
        # All umpire features default to neutral until scraper is live (HV6/KV2 penalty will apply)
        f.umpire_confirmed = False
        f.uhs_cstr = f.uhs_zone = f.uhs_early = f.uhs_weak = 50.0
        f.uks_tight = f.uks_cstrl = f.uks_2exp = f.uks_count = 50.0

    # ── Bullpen
    bullpen_logs = pitcher_data.get("bullpen_logs", [])
    if bullpen_logs:
        f.bullpen_data_available = True
        # Calculate average bullpen innings from last 3 game logs
        bp_innings = [
            float(log_entry.get("stat", {}).get("inningsPitched", 0) or 0)
            for log_entry in bullpen_logs
        ]
        avg_bp_innings = statistics.mean(bp_innings) if bp_innings else 0
        # More bullpen innings = more depleted = worse score
        f.ops_bpen = clamp(70.0 - avg_bp_innings * 5)
        f.kop_bpen = f.ops_bpen
    else:
        f.ops_bpen = 50.0
        f.kop_bpen = 50.0

    # ── Operational defaults (features not yet sourced — neutral until data source connected)
    # ── Manager hook tendency (real data from team game logs)
    hd = hook_data or {}
    hook_score = hd.get("hook_score", 50.0)
    avg_ip = hd.get("avg_starter_ip", 5.5)
    games_sampled = hd.get("games_sampled", 0)

    if games_sampled >= 5:
        # Real data available — use the calculated hook score
        f.ops_hook = hook_score
        f.kop_hook = hook_score
        log.info("Manager hook from real data",
                 pitcher=pitcher_data.get("pitcher_name"),
                 hook_score=hook_score,
                 avg_ip=avg_ip,
                 games_sampled=games_sampled)
    else:
        # Not enough games yet (early season) — neutral
        f.ops_hook = 50.0
        f.kop_hook = 50.0

    # ── OPS / KOP computed from existing data (no new API calls)
    # expected_ip already imported — gives realistic innings projection for this pitcher.
    _exp_ip = expected_ip(f.avg_ip_per_start, f.mlb_service_years)

    # ops_pcap / kop_pcap: pitch count capacity.
    # A pitcher who averages 6.5 IP throws ~100 pitches deep; one who averages 4.5 IP exits at 65.
    # Range: 35 (short-arm/piggyback) to 95 (workhorse ace). Midpoint 65 ≈ 5.5 IP average.
    _pcap = clamp(round(35.0 + (_exp_ip / 7.0) * 60.0, 2))
    f.ops_pcap = _pcap
    f.kop_pcap = _pcap

    # ops_traffic: high-traffic inning avoidance.
    # WHIP proxy (H/9 + BB/9) = baserunner rate. Lower = fewer runners = better for pitcher.
    # z-score reverse: a low WHIP proxy pitcher → fewer jams → higher ops_traffic score.
    _h9  = pitcher_data.get("season_hits_per_9") or 9.0
    _bb9 = pitcher_data.get("season_bb_per_9") or 3.0
    _whip_proxy = _h9 + _bb9
    if len(all_whip_proxy) >= 3:
        f.ops_traffic = _zscore_score(_whip_proxy, all_whip_proxy, direction="reverse")
    else:
        f.ops_traffic = 50.0

    # ops_tto / kop_tto: TTO (times-through-order) awareness.
    # Pitchers who regularly go 6+ IP face TTO3 and must manage it.
    # Tier: deeper starters have more TTO3 experience and survival → slight advantage.
    if _exp_ip >= 6.5:
        _tto_score = 65.0
    elif _exp_ip >= 5.5:
        _tto_score = 55.0
    elif _exp_ip >= 5.0:
        _tto_score = 48.0
    else:
        _tto_score = 40.0  # short starter, rarely reaches TTO3
    f.ops_tto = _tto_score
    f.kop_tto = _tto_score

    f.ops_inj   = 60.0   # no injury report source connected — slight positive default
    f.ops_trend = 50.0   # overwritten below after PFF block (pff_score not yet computed)
    f.ops_fat   = 55.0   # overwritten by TFI block when travel data is available

    f.kop_pat   = 50.0   # patience vs this pitcher — would overlap with ocr_disc; keep neutral
    f.kop_inj   = 60.0
    f.kop_fat   = 55.0   # overwritten by TFI block when travel data is available

    # ── DSC — Defense Score from Baseball Savant OAA (Outs Above Average)
    # OAA measures actual fielding value: positive = above-average defense.
    # League average is ~0 OAA. Elite teams run +30 to +50; poor teams -30 to -50.
    # Higher defense = pitcher gets more outs on balls in play = better for hits under.
    f.dsc_catch  = 50.0  # overwritten below after catcher_framing block
    f.dsc_align  = 50.0  # no clean free source for shift data — leave neutral

    oaa = oaa_data or {}
    # OAA is keyed by team_id — use the team DEFENDING (fielding) behind this pitcher
    defending_team_id = str(pitcher_data.get("team_id", "") or "")
    team_oaa = oaa.get(defending_team_id, {})

    if team_oaa and len(oaa) >= 20:
        # Build league population lists for z-score normalization
        all_oaa_total = [v["oaa_total"] for v in oaa.values() if v.get("oaa_total") is not None]
        all_oaa_inf   = [v["oaa_inf"]   for v in oaa.values() if v.get("oaa_inf")   is not None]
        all_oaa_of    = [v["oaa_of"]    for v in oaa.values() if v.get("oaa_of")    is not None]

        if team_oaa.get("oaa_total") is not None and len(all_oaa_total) >= 20:
            # Higher OAA = better defense = good for pitcher → normal direction
            f.dsc_def = _zscore_score(team_oaa["oaa_total"], all_oaa_total, direction="normal")
        else:
            f.dsc_def = 50.0

        if team_oaa.get("oaa_inf") is not None and len(all_oaa_inf) >= 20:
            f.dsc_infdef = _zscore_score(team_oaa["oaa_inf"], all_oaa_inf, direction="normal")
        else:
            f.dsc_infdef = 50.0

        if team_oaa.get("oaa_of") is not None and len(all_oaa_of) >= 20:
            f.dsc_ofdef = _zscore_score(team_oaa["oaa_of"], all_oaa_of, direction="normal")
        else:
            f.dsc_ofdef = 50.0

        log.info("DSC from OAA",
                 pitcher=pitcher_data.get("pitcher_name"),
                 team_id=defending_team_id,
                 oaa_total=team_oaa.get("oaa_total"),
                 dsc_def=round(f.dsc_def, 1),
                 dsc_infdef=round(f.dsc_infdef, 1),
                 dsc_ofdef=round(f.dsc_ofdef, 1))
    else:
        f.dsc_def    = 50.0
        f.dsc_infdef = 50.0
        f.dsc_ofdef  = 50.0

    # OWC fallbacks — only applied if real team stats were unavailable above
    if f.owc_babip    is None: f.owc_babip    = 50.0
    if f.owc_hh       is None: f.owc_hh       = 50.0
    if f.owc_bar      is None: f.owc_bar      = 50.0
    if f.owc_ld       is None: f.owc_ld       = 50.0
    if f.owc_xba      is None: f.owc_xba      = 50.0
    if f.owc_bot3     is None: f.owc_bot3     = 50.0
    if f.owc_topheavy is None: f.owc_topheavy = 50.0

    # ── PMR — Pitch Mix Rating
    # ── pmr_p1 / pmr_p2 / pmr_put: from Baseball Savant pitch arsenal
    # Whiff rate on primary/secondary/putaway pitch. Higher = more swing-and-miss
    # on the pitches the pitcher actually throws → good for K pitcher → normal direction.
    pid_str = str(pitcher_data.get("pitcher_id", "") or "")
    ar = (arsenal_data or {}).get(pid_str, {})
    if ar:
        all_p1_whiff = [
            v["p1_whiff"] for v in (arsenal_data or {}).values()
            if v.get("p1_whiff") is not None
        ]
        all_p2_whiff = [
            v["p2_whiff"] for v in (arsenal_data or {}).values()
            if v.get("p2_whiff") is not None
        ]
        all_put_whiff = [
            v["put_whiff"] for v in (arsenal_data or {}).values()
            if v.get("put_whiff") is not None
        ]

        if ar.get("p1_whiff") is not None and len(all_p1_whiff) >= 3:
            f.pmr_p1 = _zscore_score(ar["p1_whiff"], all_p1_whiff, direction="normal")
        else:
            f.pmr_p1 = 50.0

        if ar.get("p2_whiff") is not None and len(all_p2_whiff) >= 3:
            f.pmr_p2 = _zscore_score(ar["p2_whiff"], all_p2_whiff, direction="normal")
        else:
            f.pmr_p2 = 50.0

        if ar.get("put_whiff") is not None and len(all_put_whiff) >= 3:
            f.pmr_put = _zscore_score(ar["put_whiff"], all_put_whiff, direction="normal")
        else:
            f.pmr_put = 50.0

        log.info("PMR arsenal wired",
                 pitcher=name,
                 p1=ar.get("p1_name"), p1_whiff=ar.get("p1_whiff"),
                 put=ar.get("put_name"), put_whiff=ar.get("put_whiff"),
                 pmr_p1=round(f.pmr_p1, 1), pmr_put=round(f.pmr_put, 1))
    else:
        f.pmr_p1  = 50.0
        f.pmr_p2  = 50.0
        f.pmr_put = 50.0

    # ── pmr_run: opponent running game disrupts K counts.
    # High stolen base rate = more stolen base attempts = more disrupted at-bats,
    # broken counts, and focus shifts for the pitcher → bad for K total → reverse.
    opp_sb_per_game = opp_stats.get("sb_per_game") if opp_stats else None
    if opp_sb_per_game is not None and len(all_team_stats) >= 20:
        all_sb = [t.get("sb_per_game", 0.0) for t in all_team_stats if t.get("sb_per_game") is not None]
        if len(all_sb) >= 20:
            f.pmr_run = _zscore_score(opp_sb_per_game, all_sb, direction="reverse")
        else:
            f.pmr_run = 50.0
    else:
        f.pmr_run = 50.0

    # ── pmr_top6: top-6 K rate of opposing lineup.
    # High K rate among top 6 batters = lineup strikes out a lot = pitcher can
    # accumulate Ks through the order → good for K pitcher → normal direction.
    # Wired from the same batter K rate data already computed for TLR above.
    # opp_batters is already in scope from the TLR block.
    if opp_batters:
        k_rates_pmr = [b["k_rate"] for b in opp_batters if b.get("ab", 0) > 20]
        if len(k_rates_pmr) >= 4:
            top6_k_pmr = statistics.mean(k_rates_pmr[:min(6, len(k_rates_pmr))])
            # League avg K rate 22%, std 7% → normalize: higher = more K-able = better for pitcher
            z_top6 = (top6_k_pmr - 22.0) / 7.0
            f.pmr_top6 = round(clamp(50.0 + 15.0 * z_top6), 2)
        else:
            f.pmr_top6 = 50.0
    else:
        f.pmr_top6 = 50.0

    # ── pmr_plat: platoon advantage for this pitcher vs. opposing lineup.
    # A pitcher benefits when facing opposite-handed batters (breaking balls break away).
    #   RHP → wants to face LHB (platoon advantage)
    #   LHP → wants to face RHB (platoon advantage)
    # Switch hitters count as neutral (they match the pitcher's optimal side).
    pitcher_hand = f.handedness  # "R" or "L" or None
    if pitcher_hand and opp_batters:
        batters_with_side = [b for b in opp_batters if b.get("bat_side")]
        if batters_with_side:
            total_sided = len(batters_with_side)
            # Opposite-handed batter = pitcher platoon advantage
            if pitcher_hand == "R":
                opp_hand_count = sum(1 for b in batters_with_side if b["bat_side"] == "L")
            else:  # LHP
                opp_hand_count = sum(1 for b in batters_with_side if b["bat_side"] == "R")
            # Switch hitters split evenly — count as 0.5 each
            switch_count = sum(1 for b in batters_with_side if b["bat_side"] == "S")
            effective_opp = opp_hand_count + switch_count * 0.5
            plat_ratio = effective_opp / total_sided  # 0.0 = all same-hand, 1.0 = all opposite
            # Scale: 0% opp-hand → 35 (disadvantage), 50% → 50 (neutral), 100% → 70 (big advantage)
            f.pmr_plat = round(clamp(35.0 + plat_ratio * 35.0), 2)
            log.info("PMR platoon wired",
                     pitcher=name, hand=pitcher_hand,
                     opp_hand=opp_hand_count, switch=switch_count,
                     total=total_sided, pmr_plat=f.pmr_plat)
        else:
            f.pmr_plat = 50.0
    else:
        f.pmr_plat = 50.0

    # PER remaining defaults (computed where a non-overlapping signal exists)
    # per_ppa: pitching efficiency (pitches per at-bat).
    # K/BB ratio captures this cleanly: high Ks + low BBs = works ahead = fewer pitches per AB.
    # This is a distinct signal from K/9 alone (per_velo) and BB/9 alone (per_bb).
    _k9  = pitcher_data.get("season_k_per_9") or 0.0
    _bb9_per = pitcher_data.get("season_bb_per_9") or 0.0
    if _k9 > 0 and _bb9_per > 0 and len(all_k_bb) >= 3:
        _k_bb_ratio = _k9 / _bb9_per
        # High K/BB = works efficiently ahead in counts = good per_ppa → normal direction
        f.per_ppa = _zscore_score(_k_bb_ratio, all_k_bb, direction="normal")
    else:
        f.per_ppa = 50.0

    # per_deep: ability to pitch deep into games — directly from avg IP per start.
    # The cleanest mapping: 7 IP average → 90, 5 IP average → 40.
    f.per_deep = clamp(round(40.0 + (_exp_ip / 7.0) * 50.0, 2))

    f.per_fps  = 50.0   # first-pitch strike rate — no clean non-overlapping proxy available
    f.per_cmdd = 50.0   # command/location — would overlap with per_bb and pcs_cmd if wired to BB/9

    # OCR sub-features not yet sourced from live data — set only if not already populated above
    if f.ocr_zcon is None: f.ocr_zcon = 50.0
    if f.ocr_2s   is None: f.ocr_2s   = 50.0
    if f.ocr_foul is None: f.ocr_foul = 50.0
    if f.ocr_dec  is None: f.ocr_dec  = 50.0

    # ── TLR — Top-Lineup Resistance (from real individual batter K rates)
    # Determine which side of the lineup is the opponent
    ld = lineup_data or {}
    home_team_id = game_info.get("home_team_id", "")
    away_team_id = game_info.get("away_team_id", "")
    pitcher_team_id = pitcher_data.get("team_id", "")
    if pitcher_team_id == home_team_id:
        opp_batters = ld.get("away", [])
    else:
        opp_batters = ld.get("home", [])

    if opp_batters:
        k_rates = [b["k_rate"] for b in opp_batters if b.get("ab", 0) > 20]
        if len(k_rates) >= 4:
            # All-batter K rate population for normalization
            all_batter_k = k_rates  # use the lineup itself; in production use league-wide

            # TLR_TOP4K: avg K rate of the top 4 batters in the order
            # LOWER K rate = more contact = HARDER to strike them out = GOOD for K under
            # direction=reverse: low K rate lineup → high TLR score → supports K under
            top4_k = statistics.mean(k_rates[:4])
            top6_k = statistics.mean(k_rates[:min(6, len(k_rates))])
            top2_k = statistics.mean(k_rates[:2])

            # League average K rate is ~22% for individual batters
            league_batter_avg_k = 22.0
            league_batter_std_k = 7.0  # typical stdev across lineup positions

            def batter_k_score(k_rate: float) -> float:
                """Low K rate = contact hitter = harder to strike out = high TLR score."""
                z = (k_rate - league_batter_avg_k) / league_batter_std_k
                return clamp(100 - (50 + 15 * z))  # reverse: low K rate → high score

            f.tlr_top4k = round(batter_k_score(top4_k), 2)
            f.tlr_top6c = round(batter_k_score(top6_k), 2)
            f.tlr_top2  = round(batter_k_score(top2_k), 2)

            # VET score: proxy by avg at-bats (more AB = more veteran = harder to strike out)
            avg_ab = statistics.mean([b.get("ab", 0) for b in opp_batters[:6]])
            f.tlr_vet = clamp(round((avg_ab / 600) * 100, 2))  # 600 AB = 100, 0 AB = 0

            # ── Lineup Fluidity Score
            # Measures how aggressively a manager will pinch-hit in TTO3.
            # Large K-rate spread between bottom and top of order = manager has weak
            # slots worth replacing with a dangerous bench bat in late innings.
            # The simulation uses this to model stochastic lineup changes.
            full_k = k_rates  # all batters (up to 9) with enough ABs
            if len(full_k) >= 6:
                top3_k_flu    = statistics.mean(full_k[:3])
                bottom3_k_flu = statistics.mean(full_k[-3:])
                # spread: positive = bottom orders strike out more (top-heavy = fluidity target)
                flu_spread = bottom3_k_flu - top3_k_flu
                # Normalize: +10% spread → very fluid (score ≈ 100); 0% spread → neutral (50)
                league_flu_std = 8.0
                flu_z = flu_spread / league_flu_std
                f.lineup_fluidity_score = round(clamp(50.0 + 20.0 * flu_z), 2)
            else:
                f.lineup_fluidity_score = 50.0  # not enough batters — neutral

            # ── OWC_TOPHEAVY from individual batter K rates
            # Measures whether the lineup is top-heavy: dangerous top batters, weak bottom.
            # Large spread (bottom-3 K rate >> top-3 K rate) = pitcher gets easy outs at 7-8-9.
            if len(k_rates) >= 6:
                top3_k    = statistics.mean(k_rates[:3])
                bottom3_k = statistics.mean(k_rates[-3:])
                # topheavy_spread: positive = bottom strikeouts more than top = top-heavy lineup
                topheavy_spread = bottom3_k - top3_k
                # Normalize: 0 spread = neutral (50). +10% spread = very top-heavy (high score).
                league_spread_std = 6.0  # typical spread between top and bottom of lineup
                z_th = topheavy_spread / league_spread_std
                f.owc_topheavy = round(clamp(50.0 + 15.0 * z_th), 2)
                log.info("OWC topheavy from lineup K spread",
                         pitcher=pitcher_data.get("pitcher_name"),
                         top3_k=round(top3_k, 1),
                         bottom3_k=round(bottom3_k, 1),
                         topheavy_spread=round(topheavy_spread, 1),
                         owc_topheavy=f.owc_topheavy)

            log.info("TLR from real batter K rates",
                     pitcher=pitcher_data.get("pitcher_name"),
                     top4_k=round(top4_k, 1),
                     top2_k=round(top2_k, 1),
                     tlr_top4k=f.tlr_top4k,
                     tlr_top6c=f.tlr_top6c,
                     lineup_size=len(k_rates))
        else:
            f.tlr_top4k = f.tlr_top6c = f.tlr_vet = f.tlr_top2 = 50.0
    else:
        f.tlr_top4k = f.tlr_top6c = f.tlr_vet = f.tlr_top2 = 50.0

    # Fly-ball suppression
    f.fly_ball_suppression = 100.0 - (f.pcs_gb or 50.0)  # inverse of GB score

    # Pitcher median Ks — use the same IP window as the scoring engine so the K5
    # interaction rule (line >= pitcher_median + 1.0) is consistent.
    if f.season_k_per_9:
        exp_ip = expected_ip(f.avg_ip_per_start, f.mlb_service_years)
        f.pitcher_median_ks = f.season_k_per_9 * (exp_ip / 9.0)
    else:
        f.pitcher_median_ks = None

    # ── Bullpen Fatigue Coefficient (β_bp)
    # Own team fatigue affects KUSI (tired bullpen = less K protection for starter)
    # Opponent fatigue affects HUSI (tired opponent bullpen = more hit opportunities)
    if bullpen_own:
        f.bullpen_fatigue_own = bullpen_own.get("bfs", 0.0)
        f.bullpen_red_alert_own = bullpen_own.get("red_alert", False)
        f.bullpen_label_own = bullpen_own.get("label", "NO DATA")
        f.bullpen_data_available = bullpen_own.get("arms_sampled", 0) > 0
    if bullpen_opp:
        f.bullpen_fatigue_opp = bullpen_opp.get("bfs", 0.0)
        f.bullpen_red_alert_opp = bullpen_opp.get("red_alert", False)
        f.bullpen_label_opp = bullpen_opp.get("label", "NO DATA")

    # ── Mid-Game Surge (MGS) live state
    # If the pipeline is running mid-game (live scoring mode), game_info carries
    # the current inning and this pitcher's live pitch count. Pre-game both are 0,
    # which tells the MGS engine to use the expected-IP distribution curve instead.
    raw_inning = game_info.get("current_inning", 0)
    raw_pc     = game_info.get("pitcher_pitch_count", 0)
    f.mgs_inning      = int(raw_inning) if raw_inning else 0
    f.mgs_pitch_count = int(raw_pc)     if raw_pc     else 0
    # Derive TTO tier for reference (stored on the feature set for logging)
    if f.mgs_inning >= 6:
        f.mgs_tto = 3
    elif f.mgs_inning >= 4:
        f.mgs_tto = 2
    elif f.mgs_inning > 0:
        f.mgs_tto = 1
    else:
        f.mgs_tto = 0   # pre-game

    if f.mgs_inning > 0:
        log.info("MGS live state populated",
                 pitcher=name,
                 inning=f.mgs_inning,
                 pitch_count=f.mgs_pitch_count,
                 tto=f.mgs_tto)

    # ── Pitcher Form Factor (PFF)
    # Populated from pitcher_data["recent_form"] which is a list of last-3-start
    # dicts fetched by MLBStatsAdapter.fetch_pitcher_recent_form() in the pipeline.
    # Defaults to NEUTRAL (pff=0.0) if no recent starts are available.
    from app.utils.pff import compute_pff
    recent_form = pitcher_data.get("recent_form", [])
    pff_result = compute_pff(recent_form)
    f.pff_score           = pff_result["pff"]
    f.pff_label           = pff_result["label"]
    f.pff_hits_tto1_mult  = pff_result["hits_tto1_mult"]
    f.pff_ks_tto1_mult    = pff_result["ks_tto1_mult"]
    f.pff_tto_late_boost  = pff_result["tto_late_boost"]
    f.pff_starts_used     = pff_result["starts_used"]

    if recent_form:
        log.info("PFF applied",
                 pitcher=name,
                 pff=f.pff_score,
                 label=f.pff_label,
                 starts_used=f.pff_starts_used,
                 hits_tto1=f.pff_hits_tto1_mult,
                 ks_tto1=f.pff_ks_tto1_mult)

    # ── ops_trend: now that PFF is computed, wire recent form into the trend score.
    # PFF is in [-0.30, +0.30]; +0.30 (ON FIRE) → ops_trend 80; -0.30 (STRUGGLING) → 20.
    # This replaces the static 50.0 placeholder set earlier.
    f.ops_trend = clamp(round(50.0 + f.pff_score * 100.0, 2))

    # ── SKU #37 — Catcher Framing
    # The defending catcher's framing ability modifies KUSI. An elite framer
    # "steals" borderline strikes, giving the pitcher extra Ks on called strikes.
    cf = catcher_framing or {}
    if cf:
        from app.services.catcher_service import compute_framing_score
        f.catcher_id             = cf.get("catcher_id")
        f.catcher_name           = cf.get("catcher_name")
        f.catcher_strike_rate    = cf.get("strike_rate", 50.0)
        f.catcher_kusi_adj       = cf.get("kusi_adjustment", 0.0)
        f.catcher_framing_label  = cf.get("framing_label", "NEUTRAL")
        # Also feed the DSC block's catch sub-score
        f.catcher_framing_score  = compute_framing_score(f.catcher_id)
        f.dsc_catch              = f.catcher_framing_score
        log.info("Catcher framing applied",
                 pitcher=name,
                 catcher=f.catcher_name,
                 strike_rate=f.catcher_strike_rate,
                 kusi_adj=f.catcher_kusi_adj,
                 dsc_catch=f.dsc_catch)

    # ── SKU #14 — Travel & Fatigue Index
    # A tired pitching team (getaway day or cross-timezone travel) gives up more
    # hits early. Penalty reduces HUSI when triggered.
    tf = travel_fatigue or {}
    if tf:
        f.tfi_rest_hours       = tf.get("rest_hours", 24.0)
        f.tfi_tz_shift         = tf.get("tz_shift", 0)           # absolute delta for display
        f.tfi_signed_tz_shift  = tf.get("signed_tz_shift", 0)    # signed: + east, - west
        f.tfi_getaway_day      = tf.get("getaway_day", False)
        f.tfi_cross_timezone   = tf.get("cross_timezone", False)
        f.tfi_penalty_pct      = tf.get("penalty_pct", 0.0)      # already directional (Merlin)
        f.tfi_label            = tf.get("tfi_label", "NO DATA")

        # Also update ops_fat — days rest / fatigue score feeds the OPS block
        rest_hours = f.tfi_rest_hours
        if rest_hours < 16:
            f.ops_fat = 25.0   # severely fatigued
            f.kop_fat = 25.0
        elif rest_hours < 20:
            f.ops_fat = 40.0   # somewhat fatigued
            f.kop_fat = 40.0
        elif rest_hours >= 48:
            f.ops_fat = 70.0   # well-rested
            f.kop_fat = 70.0
        # (else keep the default 55.0)

        if f.tfi_penalty_pct > 0:
            log.info("TFI penalty wired",
                     pitcher=name,
                     label=f.tfi_label,
                     rest_h=f.tfi_rest_hours,
                     tz_delta=f.tfi_tz_shift,
                     penalty=f.tfi_penalty_pct)

    # ── SKU #38 — VAA & Extension Perceived Velocity
    # VAA > -4.5° = flat trajectory (less steep, closer to 0) → hitter tracks easier → +10% contact.
    # VAA < -4.5° = steep descent (like a curveball drop) → harder to track.
    # Extension > 6.8 ft → pitcher releases closer to home plate → +1.5 mph perceived velocity.
    #
    # Merlin v2.0 VAA Elevation override: if flat (VAA > -4.5) AND pitch thrown high
    # in zone (pitch_location_high_pct > 60%), the flat+high combination produces pop-ups
    # rather than hard contact — REVERSE to suppression boost. Applied in husi.py.
    vd = vaa_data or {}
    if vd:
        vaa  = vd.get("vaa_degrees")
        ext  = vd.get("extension_ft")
        f.vaa_degrees    = vaa
        f.extension_ft   = ext
        # pitch_location_high_pct: % of pitches in upper zone (from live Statcast feed)
        # If not available from the live feed, defaults to None → elevation override skips.
        f.pitch_location_high_pct = vd.get("pitch_location_high_pct")

        # VAA flat: greater than -4.5° (less negative) = flatter trajectory = easier to track
        if vaa is not None and vaa > -4.5:
            f.vaa_flat           = True
            f.vaa_contact_penalty = 0.10
        else:
            f.vaa_flat           = False
            f.vaa_contact_penalty = 0.0

        # Extension boost: > 6.8 ft → +1.5 mph perceived velocity
        # Implemented as a per_velo score boost (5-point lift on the 0-100 scale)
        if ext is not None and ext > 6.8:
            f.extension_elite    = True
            f.extension_velo_boost = 5.0
            f.per_velo = clamp((f.per_velo or 50.0) + 5.0)  # boost the PER block velocity score
        else:
            f.extension_elite    = False
            f.extension_velo_boost = 0.0

        if vaa is not None:
            log.info("VAA/Extension applied",
                     pitcher=name,
                     vaa=vaa,
                     ext=ext,
                     flat=f.vaa_flat,
                     elite_ext=f.extension_elite)

    # ── SKU #39 — Swing Plane Collision Score
    # Physics: the pitcher's ball descends at |VAA|° below horizontal.
    # Each batter's "ideal" attack angle to square that pitch is:
    #   ideal_aa = |vaa| + 5.0°
    # (the +5° offset accounts for the geometry of a bat rising to meet a descending ball;
    #  empirically validated by Eno Sarris / Statcast bat-tracking research)
    # When a batter's actual attack angle diverges from ideal_aa, the collision is
    # inefficient — producing soft contact, pop-ups, or foul tips instead of barrels.
    #
    # Drives:
    #   f.pcs_soft            → HUSI PCS block (soft contact proxy, weight 0.16)
    #   f.swing_plane_collision_score → raw value used by KUSI K9 interaction rule
    sp = swing_profiles or {}
    if sp and f.vaa_degrees is not None and opp_batters:
        ideal_aa = abs(f.vaa_degrees) + 5.0
        mismatches = []
        for batter in opp_batters:
            batter_id = str(batter.get("batter_id") or "")
            profile = sp.get(batter_id, {})
            aa = profile.get("attack_angle")
            if aa is not None:
                mismatches.append(abs(aa - ideal_aa))

        if mismatches:
            avg_mismatch = statistics.mean(mismatches)
            # 0° mismatch = neutral (50). Each degree of avg mismatch = +4 score points.
            # +5° avg mismatch → score ≈ 70 (elevated advantage)
            # +10° avg mismatch → score ≈ 90 (strong advantage)
            collision_score = round(clamp(50.0 + avg_mismatch * 4.0), 2)
            f.swing_plane_collision_score = collision_score
            f.pcs_soft = collision_score  # soft contact proxy — drives HUSI PCS block
            log.info("SKU #39 Swing Plane Collision",
                     pitcher=name,
                     vaa=f.vaa_degrees,
                     ideal_aa=round(ideal_aa, 1),
                     avg_mismatch_deg=round(avg_mismatch, 2),
                     collision_score=collision_score,
                     batters_matched=len(mismatches),
                     batters_total=len(opp_batters))
        else:
            log.debug("Swing Plane Collision: no batter profiles matched lineup",
                      pitcher=name, opp_batters=len(opp_batters))
    elif not sp:
        log.debug("Swing Plane Collision: profiles not fetched yet — pcs_soft remains None",
                  pitcher=name)
    elif f.vaa_degrees is None:
        log.debug("Swing Plane Collision: VAA unavailable — skipping", pitcher=name)

    log.info("Features built", pitcher=name, k_line=f.k_line, hits_line=f.hits_line,
             k9=f.season_k_per_9, h9=f.season_hits_per_9, ens_park=f.ens_park,
             bullpen_own_bfs=f.bullpen_fatigue_own, bullpen_own_label=f.bullpen_label_own,
             bullpen_opp_bfs=f.bullpen_fatigue_opp, bullpen_opp_label=f.bullpen_label_opp)

    return f


def _zscore_score(value: float, population: list[float], direction: str = "normal") -> float:
    """Compute z-score and convert to 0-100 score."""
    if not population or len(population) < 2:
        return 50.0
    try:
        mean = statistics.mean(population)
        stdev = statistics.stdev(population)
        if stdev == 0:
            return 50.0
        z = (value - mean) / stdev
        score = clamp(50.0 + 15.0 * z)
        if direction == "reverse":
            score = 100.0 - score
        return round(score, 2)
    except statistics.StatisticsError:
        return 50.0
