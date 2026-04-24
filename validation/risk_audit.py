"""
risk_audit.py — Cross every today's starter against every formula variable.

Pulls probable starters from the MLB Stats API and scores each pitcher
against all risk flags our formula knows about. Ranks them from highest
risk (most likely to give up hits) to safest.

This is the Buehler Lesson in practice: never let one factor mask another.

Risk Flags Checked
──────────────────
  ERA_DISASTER    Season ERA ≥ 6.00 — pitcher is struggling all season
  ERA_STRUGGLING  Season ERA 5.00-5.99 — below-average performance level
  BOOM_BUST       IP variance across recent starts — wild early-exit history
  EXTREME_PARK    Pitching at Coors, Chase, GABP, Citizens Bank, Globe Life
  HITTER_PARK     Pitching at any park with score < 48 (mildly hitter-friendly)
  LOW_IP_TREND    Recent starts trending shorter (possible injury/fatigue signal)
  HIGH_H9         Season H/9 above 9.5 — already giving up a lot of hits
  WINDIN_RISK     Wind blowing out at venue today
  COMBO_RISK      3+ flags simultaneously — highest danger level

Usage:
  python validation/risk_audit.py
  python validation/risk_audit.py --date 2026-04-22
  python validation/risk_audit.py --export       # saves risk_audit.csv
"""
import argparse
import asyncio
import json
import os
import statistics
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import httpx

# ── Project path
sys.path.insert(0, str(Path(__file__).parent.parent))

# ── Park factor table (mirrors feature_builder.py)
PARK_SCORES: dict[str, float] = {
    "coors field": 20, "great american ball park": 34, "chase field": 38,
    "citizens bank park": 38, "fenway park": 41, "yankee stadium": 44,
    "oriole park at camden yards": 44, "camden yards": 44,
    "globe life field": 42, "wrigley field": 48, "rogers centre": 46,
    "truist park": 52, "guaranteed rate field": 51, "rate field": 51,
    "minute maid park": 50, "loandepot park": 50, "nationals park": 52,
    "dodger stadium": 54, "target field": 55, "angel stadium": 53,
    "progressive field": 55, "comerica park": 57,
    "american family field": 54, "busch stadium": 54,
    "kauffman stadium": 54, "tropicana field": 56, "citi field": 57,
    "pnc park": 58, "t-mobile park": 60, "petco park": 62,
    "oracle park": 64, "sutter health park": 54, "oakland coliseum": 56,
}

MLB_BASE = "https://statsapi.mlb.com/api/v1"
BOOM_BUST_IP_STDEV   = 1.6
BOOM_BUST_PANIC_EXIT = 3.0
ERA_STRUGGLING       = 5.00
ERA_DISASTER         = 6.00
H9_HIGH_THRESHOLD    = 9.5
EXTREME_PARK_SCORE   = 40
HITTER_PARK_SCORE    = 48

# ─────────────────────────────────────────────────────────────
# MLB Stats API helpers
# ─────────────────────────────────────────────────────────────

async def fetch_probable_starters(client: httpx.AsyncClient, target_date: str) -> list[dict]:
    """Return list of probable starters with game context."""
    url = f"{MLB_BASE}/schedule"
    params = {
        "sportId": 1,
        "date": target_date,
        "hydrate": "probablePitcher(note),linescore,venue",
    }
    r = await client.get(url, params=params, timeout=20)
    r.raise_for_status()
    data = r.json()

    starters = []
    for day in data.get("dates", []):
        for game in day.get("games", []):
            venue_name = game.get("venue", {}).get("name", "Unknown")
            game_pk = game.get("gamePk")
            game_time = game.get("gameDate", "")

            for side in ("home", "away"):
                team_data = game.get("teams", {}).get(side, {})
                pp = team_data.get("probablePitcher")
                if not pp:
                    continue
                starters.append({
                    "pitcher_id":   str(pp.get("id")),
                    "pitcher_name": pp.get("fullName", "Unknown"),
                    "team":         team_data.get("team", {}).get("abbreviation", "???"),
                    "side":         side,
                    "venue":        venue_name,
                    "game_pk":      game_pk,
                    "game_time":    game_time,
                    "opponent":     game.get("teams", {}).get(
                        "away" if side == "home" else "home", {}
                    ).get("team", {}).get("abbreviation", "???"),
                })
    return starters


async def fetch_pitcher_season_stats(client: httpx.AsyncClient, pitcher_id: str) -> dict:
    """Fetch ERA, H/9, K/9, and game log for one pitcher."""
    url = f"{MLB_BASE}/people/{pitcher_id}/stats"
    params = {"stats": "season,gameLog", "group": "pitching", "season": datetime.now().year}
    try:
        r = await client.get(url, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
    except Exception:
        return {}

    season_stats = {}
    game_log = []

    for stat_block in data.get("stats", []):
        stat_type = stat_block.get("type", {}).get("displayName", "")
        splits = stat_block.get("splits", [])

        if stat_type == "season" and splits:
            s = splits[0].get("stat", {})
            era = s.get("era", "-.--")
            try:
                era_f = float(era)
            except (ValueError, TypeError):
                era_f = None

            ip_total = s.get("inningsPitched")
            try:
                ip_f = float(ip_total) if ip_total else None
            except (ValueError, TypeError):
                ip_f = None

            h = s.get("hits", 0)
            k = s.get("strikeOuts", 0)
            gs = s.get("gamesStarted", 0)

            season_stats = {
                "era":         era_f,
                "h9":          round(h / ip_f * 9, 2) if ip_f and ip_f > 0 else None,
                "k9":          round(k / ip_f * 9, 2) if ip_f and ip_f > 0 else None,
                "gs":          gs,
                "avg_ip":      round(ip_f / gs, 2) if gs and gs > 0 and ip_f else None,
            }

        elif stat_type == "gameLog":
            for split in splits[-5:]:  # last 5 starts
                s = split.get("stat", {})
                ip_str = s.get("inningsPitched", "0")
                try:
                    ip_f = float(ip_str)
                except (ValueError, TypeError):
                    ip_f = 0.0
                game_log.append({
                    "date":   split.get("date", ""),
                    "ip":     ip_f,
                    "h":      s.get("hits", 0),
                    "er":     s.get("earnedRuns", 0),
                    "k":      s.get("strikeOuts", 0),
                })

    return {**season_stats, "game_log": game_log}


async def fetch_game_wind(client: httpx.AsyncClient, venue_name: str) -> str:
    """Quick NWS wind check for a venue. Returns 'OUT', 'IN', or 'CALM'."""
    # Venue → approximate lat/lon
    VENUE_COORDS = {
        "coors field": (39.7559, -104.9942),
        "great american ball park": (39.0975, -84.5075),
        "chase field": (33.4453, -112.0667),
        "citizens bank park": (39.9061, -75.1665),
        "fenway park": (42.3467, -71.0972),
        "wrigley field": (41.9484, -87.6553),
        "yankee stadium": (40.8296, -73.9262),
        "globe life field": (32.7473, -97.0823),
    }
    vkey = venue_name.lower()
    coords = VENUE_COORDS.get(vkey)
    if not coords:
        return "UNKNOWN"
    try:
        lat, lon = coords
        r = await client.get(f"https://api.weather.gov/points/{lat},{lon}",
                             headers={"User-Agent": "AxiomRiskAudit/1.0"}, timeout=8)
        r.raise_for_status()
        forecast_url = r.json().get("properties", {}).get("forecastHourly", "")
        if not forecast_url:
            return "UNKNOWN"
        r2 = await client.get(forecast_url, headers={"User-Agent": "AxiomRiskAudit/1.0"}, timeout=8)
        r2.raise_for_status()
        periods = r2.json().get("properties", {}).get("periods", [])
        if periods:
            wind_dir = (periods[0].get("windDirection") or "").upper()
            wind_spd_str = periods[0].get("windSpeed", "0 mph")
            try:
                wind_spd = float(wind_spd_str.lower().replace("mph","").strip().split()[0])
            except Exception:
                wind_spd = 0.0
            if wind_spd < 5:
                return "CALM"
            # Very rough: S/SW/SE = out at most parks; N/NW/NE = in
            if any(d in wind_dir for d in ["S", "SW", "SE"]):
                return "OUT"
            else:
                return "IN"
    except Exception:
        return "UNKNOWN"


# ─────────────────────────────────────────────────────────────
# Risk scorer
# ─────────────────────────────────────────────────────────────

def score_risk(pitcher: dict, stats: dict) -> dict:
    """
    Evaluate all risk flags for one pitcher. Returns a risk profile dict.
    """
    flags = []
    notes = []

    era    = stats.get("era")
    h9     = stats.get("h9")
    gs     = stats.get("gs", 0)
    avg_ip = stats.get("avg_ip")
    log    = stats.get("game_log", [])
    venue  = pitcher.get("venue", "").lower()

    # ── ERA Tier (HV10)
    if era is not None:
        if era >= ERA_DISASTER:
            flags.append("ERA_DISASTER")
            notes.append(f"Season ERA {era:.2f} (disaster tier ≥6.00)")
        elif era >= ERA_STRUGGLING:
            flags.append("ERA_STRUGGLING")
            notes.append(f"Season ERA {era:.2f} (struggling tier ≥5.00)")

    # ── H/9 High (already giving up lots of hits)
    if h9 is not None and h9 >= H9_HIGH_THRESHOLD:
        flags.append("HIGH_H9")
        notes.append(f"Season H/9 {h9:.1f} (above {H9_HIGH_THRESHOLD} threshold)")

    # ── Boom-Bust IP variance
    ip_values = [s["ip"] for s in log if s.get("ip", 0) >= 0.3][-3:]
    if len(ip_values) >= 2:
        ip_stdev = statistics.stdev(ip_values)
        has_short = any(ip < 4.0 for ip in ip_values)
        has_panic = any(ip < BOOM_BUST_PANIC_EXIT for ip in ip_values)
        if (ip_stdev >= BOOM_BUST_IP_STDEV and has_short) or has_panic:
            reason = "panic exit (<3 IP)" if has_panic else f"IP stdev {ip_stdev:.1f}"
            flags.append("BOOM_BUST")
            notes.append(f"IP variance flag: {[round(x,1) for x in ip_values]} — {reason}")

    # ── Low IP Trend (last 2 starts shorter than season avg)
    if avg_ip and len(ip_values) >= 2:
        recent_avg = sum(ip_values[-2:]) / 2
        if recent_avg < avg_ip * 0.75:
            flags.append("LOW_IP_TREND")
            notes.append(f"Recent IP avg {recent_avg:.1f} < 75% of season avg {avg_ip:.1f}")

    # ── Park Factor
    park_score = PARK_SCORES.get(venue, 50.0)
    park_mult  = round(1.0 + ((50.0 - park_score) / 50.0) * 0.30, 3)
    if park_score < EXTREME_PARK_SCORE:
        flags.append("EXTREME_PARK")
        notes.append(f"Extreme hitter park: {pitcher['venue']} (score {park_score}, +{(park_mult-1)*100:.0f}% hits)")
    elif park_score < HITTER_PARK_SCORE:
        flags.append("HITTER_PARK")
        notes.append(f"Hitter-friendly park: {pitcher['venue']} (score {park_score})")

    # ── Combo Risk (3+ flags = highest danger)
    if len(flags) >= 3:
        flags.append("COMBO_RISK")

    # ── Numeric risk score for ranking (higher = more dangerous)
    weights = {
        "ERA_DISASTER":   12,
        "ERA_STRUGGLING":  7,
        "BOOM_BUST":       8,
        "EXTREME_PARK":    8,
        "HIGH_H9":         5,
        "HITTER_PARK":     3,
        "LOW_IP_TREND":    4,
        "COMBO_RISK":      6,
    }
    risk_score = sum(weights.get(f, 0) for f in flags)

    return {
        "pitcher":     pitcher["pitcher_name"],
        "team":        pitcher["team"],
        "opponent":    pitcher["opponent"],
        "venue":       pitcher["venue"],
        "era":         era,
        "h9":          h9,
        "gs":          gs,
        "recent_ips":  ip_values,
        "park_score":  park_score,
        "park_mult":   park_mult,
        "flags":       [f for f in flags if f != "COMBO_RISK"],
        "combo_risk":  "COMBO_RISK" in flags,
        "risk_score":  risk_score,
        "notes":       notes,
    }


# ─────────────────────────────────────────────────────────────
# Printer
# ─────────────────────────────────────────────────────────────

def _bar(score: int, max_score: int = 35) -> str:
    filled = min(int(score / max_score * 20), 20)
    return "█" * filled + "░" * (20 - filled)


RISK_COLORS = {
    "ERA_DISASTER":   "🔴",
    "ERA_STRUGGLING": "🟠",
    "BOOM_BUST":      "⚡",
    "EXTREME_PARK":   "🏟",
    "HITTER_PARK":    "📍",
    "HIGH_H9":        "📈",
    "LOW_IP_TREND":   "📉",
}


def print_report(profiles: list[dict], target_date: str):
    profiles_sorted = sorted(profiles, key=lambda x: x["risk_score"], reverse=True)

    print()
    print("=" * 72)
    print(f"  AXIOM PITCHER RISK AUDIT  —  {target_date}")
    print(f"  {len(profiles)} probable starters analyzed")
    print("=" * 72)

    # ── HIGH RISK section
    high = [p for p in profiles_sorted if p["risk_score"] >= 15]
    mid  = [p for p in profiles_sorted if 6 <= p["risk_score"] < 15]
    safe = [p for p in profiles_sorted if p["risk_score"] < 6]

    for tier_label, tier_list in [("🚨 HIGH RISK", high), ("⚠️  MODERATE RISK", mid), ("✅  LOW RISK", safe)]:
        if not tier_list:
            continue
        print(f"\n{tier_label} ({len(tier_list)} pitchers)\n{'─'*72}")
        for p in tier_list:
            era_str  = f"{p['era']:.2f}"  if p['era'] is not None else "N/A"
            h9_str   = f"{p['h9']:.1f}"   if p['h9']  is not None else "N/A"
            ips_str  = str([round(x,1) for x in p['recent_ips']]) if p['recent_ips'] else "N/A"
            pk_mult  = f"×{p['park_mult']:.2f}" if p['park_mult'] != 1.0 else "×1.00"
            flag_icons = " ".join(RISK_COLORS.get(f, "❓") + f for f in p["flags"])

            print(f"\n  {p['pitcher']} ({p['team']}) vs {p['opponent']}")
            print(f"  Venue: {p['venue']}  |  Risk Score: {p['risk_score']}  {_bar(p['risk_score'])}")
            print(f"  ERA: {era_str}  |  H/9: {h9_str}  |  Recent IPs: {ips_str}  |  Park: {pk_mult}")
            if p["flags"]:
                print(f"  Flags: {flag_icons}")
            for note in p["notes"]:
                print(f"    → {note}")
            if p["combo_risk"]:
                print(f"  ⛔  COMBO RISK — multiple danger factors stacking. Strong OVER lean.")

    # ── Summary table
    print(f"\n{'='*72}")
    print("  RANKED RISK TABLE")
    print(f"  {'Pitcher':<28} {'ERA':>6} {'H/9':>6} {'Park':>6} {'Score':>6}  Flags")
    print(f"  {'─'*28} {'─'*6} {'─'*6} {'─'*6} {'─'*6}  {'─'*20}")
    for p in profiles_sorted:
        era_str = f"{p['era']:.2f}" if p['era'] is not None else " N/A"
        h9_str  = f"{p['h9']:.1f}"  if p['h9']  is not None else " N/A"
        pk_str  = f"{p['park_score']:.0f}"
        flags   = ",".join(p["flags"])
        marker  = "⛔" if p["combo_risk"] else ("🔴" if p["risk_score"] >= 15 else ("🟠" if p["risk_score"] >= 6 else "✅"))
        print(f"  {p['pitcher']:<28} {era_str:>6} {h9_str:>6} {pk_str:>6} {p['risk_score']:>6}  {marker} {flags}")

    print()


def export_csv(profiles: list[dict], target_date: str):
    import csv
    fname = f"validation/risk_audit_{target_date}.csv"
    fields = ["pitcher","team","opponent","venue","era","h9","gs","recent_ips","park_score",
              "park_mult","risk_score","combo_risk","flags","notes"]
    with open(fname, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for p in sorted(profiles, key=lambda x: x["risk_score"], reverse=True):
            row = {k: p.get(k) for k in fields}
            row["flags"] = "|".join(p.get("flags", []))
            row["notes"] = " | ".join(p.get("notes", []))
            row["recent_ips"] = str(p.get("recent_ips", []))
            w.writerow(row)
    print(f"  CSV saved → {fname}")


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

async def run(target_date: str, do_export: bool):
    print(f"\nAxiom Risk Audit — fetching starters for {target_date}...")

    async with httpx.AsyncClient(timeout=20) as client:
        starters = await fetch_probable_starters(client, target_date)
        if not starters:
            print(f"No probable starters found for {target_date}.")
            return

        print(f"Found {len(starters)} probable starters. Pulling season stats...")

        # Fetch stats concurrently
        tasks = [fetch_pitcher_season_stats(client, s["pitcher_id"]) for s in starters]
        all_stats = await asyncio.gather(*tasks)

        profiles = []
        for starter, stats in zip(starters, all_stats):
            profile = score_risk(starter, stats)
            profiles.append(profile)

        print_report(profiles, target_date)

        if do_export:
            export_csv(profiles, target_date)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Axiom Pitcher Risk Audit")
    parser.add_argument("--date", default=date.today().isoformat(),
                        help="Date to audit (YYYY-MM-DD, default: today)")
    parser.add_argument("--export", action="store_true",
                        help="Export results to CSV")
    args = parser.parse_args()

    asyncio.run(run(args.date, args.export))
