"""
validation/hidden_var_audit.py — Backtest the three hidden variable modules.

Answers the one question that matters before you go B2B:
"Do the hidden variables actually improve accuracy — or are they noise?"

How it works
────────────
For every RESOLVED game in results_log.jsonl the script:

  1. Checks if the pitching team was on a GETAWAY DAY (rest < 16h) or crossing
     time zones (SKU #14 — TFI). Uses the free MLB Stats API.

  2. Checks if the defending catcher was an ELITE or POOR framer (SKU #37).
     Cross-references the game's boxscore against data/framing_cache.json.

  3. Checks if any VAA data is stored in the log for the game (SKU #38).
     If not yet available, marks as UNKNOWN.

It then splits the WIN/LOSS scorecard into two buckets:
  ACTIVE   — the hidden variable was triggered
  INACTIVE — it was not triggered (baseline)

If accuracy is meaningfully higher in the INACTIVE bucket, the hidden
variable's penalty/boost is overcorrecting and needs tuning.
If accuracy is HIGHER in the ACTIVE bucket, the hidden variable is working.

Usage
─────
    python validation/hidden_var_audit.py
    python validation/hidden_var_audit.py --days 14
    python validation/hidden_var_audit.py --var tfi     # just TFI analysis
    python validation/hidden_var_audit.py --var framing # just catcher framing
    python validation/hidden_var_audit.py --var vaa     # just VAA
    python validation/hidden_var_audit.py --export      # writes audit.csv

Requires:
    - Internet access (free MLB Stats API — no key needed)
    - validation/results_log.jsonl with at least some resolved entries
    - data/framing_cache.json (already in repo)
"""
import argparse
import csv
import json
import math
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

import httpx

# ─────────────────────────────────────────────────────────────
# Paths & constants
# ─────────────────────────────────────────────────────────────
LOG_FILE = Path(__file__).parent / "results_log.jsonl"
FRAMING_CACHE = Path(__file__).parent.parent / "data" / "framing_cache.json"
MLB_BASE = "https://statsapi.mlb.com/api/v1"

# TFI thresholds — must match travel_fatigue.py
TFI_REST_THRESHOLD = 16.0      # hours
TFI_TZ_THRESHOLD   = 2         # hour timezone delta

# Framing thresholds — must match catcher_service.py
FRAMING_ELITE_THRESHOLD = 50.0  # strike_rate > this = elite framer
FRAMING_POOR_THRESHOLD  = 48.0  # strike_rate < this = poor framer

AVG_GAME_HOURS = 3.1  # estimated game duration used to compute rest hours


# ─────────────────────────────────────────────────────────────
# Load static data
# ─────────────────────────────────────────────────────────────

def _load_framing_cache() -> dict:
    if FRAMING_CACHE.exists():
        with open(FRAMING_CACHE) as f:
            data = json.load(f)
        return data.get("catchers", {})
    return {}


def _load_log(days: int | None) -> list[dict]:
    if not LOG_FILE.exists():
        return []
    cutoff = None
    if days:
        cutoff = str(date.today() - timedelta(days=days))
    records = []
    with LOG_FILE.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not r.get("resolved"):
                continue
            if cutoff and r.get("game_date", "") < cutoff:
                continue
            records.append(r)
    return records


# ─────────────────────────────────────────────────────────────
# MLB Stats API helpers
# ─────────────────────────────────────────────────────────────

def _mlb_get(path: str, params: dict = None) -> dict:
    try:
        resp = httpx.get(f"{MLB_BASE}{path}", params=params or {}, timeout=20.0)
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        return {}


def _find_pitcher_team_id(pitcher_id: str, game_date: str) -> str | None:
    """Look up which team a pitcher was on for a specific date."""
    data = _mlb_get("/schedule", params={
        "sportId": 1,
        "date": game_date,
        "hydrate": "probablePitcher",
    })
    for date_block in data.get("dates", []):
        for game in date_block.get("games", []):
            for side in ("home", "away"):
                pp = game.get("teams", {}).get(side, {}).get("probablePitcher", {})
                if str(pp.get("id", "")) == str(pitcher_id):
                    return str(game.get("teams", {}).get(side, {}).get("team", {}).get("id", ""))
    return None


def _fetch_team_previous_game(team_id: str, before_date: str) -> dict | None:
    """Fetch the team's most recent completed game before a given date."""
    start = (datetime.strptime(before_date, "%Y-%m-%d") - timedelta(days=2)).strftime("%Y-%m-%d")
    yesterday = (datetime.strptime(before_date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    data = _mlb_get("/schedule", params={
        "teamId": team_id,
        "sportId": 1,
        "startDate": start,
        "endDate": yesterday,
        "hydrate": "venue",
        "gameType": "R",
    })
    games = []
    for date_block in data.get("dates", []):
        for game in date_block.get("games", []):
            if "Final" in game.get("status", {}).get("abstractGameState", ""):
                games.append({
                    "game_date": date_block["date"],
                    "game_time_utc": game.get("gameDate"),
                    "venue_name": game.get("venue", {}).get("name", "").lower(),
                })
    games.sort(key=lambda g: g["game_date"], reverse=True)
    return games[0] if games else None


def _fetch_today_game_time(team_id: str, game_date: str) -> tuple[str | None, str]:
    """Return (game_time_utc, venue_name) for a team on a given date."""
    data = _mlb_get("/schedule", params={
        "teamId": team_id,
        "sportId": 1,
        "startDate": game_date,
        "endDate": game_date,
        "hydrate": "venue",
    })
    for date_block in data.get("dates", []):
        for game in date_block.get("games", []):
            return (
                game.get("gameDate"),
                game.get("venue", {}).get("name", "").lower(),
            )
    return None, ""


def _venue_tz_offset(venue_name: str) -> float:
    """Approximate UTC offset for a venue name. Returns 0 if unknown."""
    OFFSETS = {
        "yankee stadium": -4, "fenway park": -4, "camden yards": -4,
        "tropicana field": -4, "rogers centre": -4, "citi field": -4,
        "nationals park": -4, "citizens bank park": -4, "truist park": -4,
        "loandepot park": -4, "guaranteed rate field": -5, "comerica park": -4,
        "kauffman stadium": -5, "target field": -5, "minute maid park": -5,
        "globe life field": -5, "wrigley field": -5, "great american ball park": -4,
        "busch stadium": -5, "american family field": -5, "pnc park": -4,
        "progressive field": -4, "angel stadium": -7, "t-mobile park": -7,
        "oracle park": -7, "dodger stadium": -7, "petco park": -7,
        "chase field": -7, "coors field": -6,
    }
    for k, v in OFFSETS.items():
        if k in venue_name:
            return float(v)
    return 0.0


def _compute_rest_hours(yesterday_start_utc: str | None, today_start_utc: str | None) -> float | None:
    if not yesterday_start_utc or not today_start_utc:
        return None
    try:
        fmt = "%Y-%m-%dT%H:%M:%S"
        y_dt = datetime.strptime(yesterday_start_utc[:19], fmt)
        t_dt = datetime.strptime(today_start_utc[:19], fmt)
        y_end = y_dt + timedelta(hours=AVG_GAME_HOURS)
        secs = (t_dt - y_end).total_seconds()
        return round(secs / 3600.0, 1)
    except Exception:
        return None


def _fetch_game_catcher(pitcher_id: str, game_date: str, framing_cache: dict) -> dict:
    """
    Find the defending catcher for a pitcher's game and look up their framing data.
    The defending catcher is the OPPONENT's catcher.
    """
    # First find the game
    data = _mlb_get("/schedule", params={"sportId": 1, "date": game_date, "hydrate": "lineups"})
    for date_block in data.get("dates", []):
        for game in date_block.get("games", []):
            for side in ("home", "away"):
                pp = game.get("teams", {}).get(side, {}).get("probablePitcher", {})
                if str(pp.get("id", "")) == str(pitcher_id):
                    # Found the pitcher's side — opposing catcher is on the other side
                    opp_side = "away" if side == "home" else "home"
                    batting_order = game.get("teams", {}).get(opp_side, {}).get("battingOrder", [])
                    # Fall back to boxscore if lineups not hydrated
                    if not batting_order:
                        game_pk = str(game.get("gamePk", ""))
                        if game_pk:
                            return _fetch_catcher_from_boxscore(game_pk, opp_side, framing_cache)
                    # Find the catcher in the batting order
                    for player_id in batting_order:
                        cid = str(player_id)
                        if cid in framing_cache:
                            entry = framing_cache[cid]
                            return {"catcher_id": cid, "name": entry["name"],
                                    "strike_rate": entry["strike_rate"],
                                    "tier": entry.get("tier", "AVG"),
                                    "source": "lineup"}
    return {"catcher_id": None, "name": "UNKNOWN", "strike_rate": 50.0,
            "tier": "AVG", "source": "not_found"}


def _fetch_catcher_from_boxscore(game_pk: str, opp_side: str, framing_cache: dict) -> dict:
    data = _mlb_get(f"/game/{game_pk}/boxscore")
    players = data.get("teams", {}).get(opp_side, {}).get("players", {})
    for key, pdata in players.items():
        pos = pdata.get("position", {})
        if pos.get("abbreviation") == "C":
            cid = str(pdata.get("person", {}).get("id", ""))
            name = pdata.get("person", {}).get("fullName", "Unknown")
            entry = framing_cache.get(cid, {})
            return {
                "catcher_id": cid,
                "name": name,
                "strike_rate": entry.get("strike_rate", 50.0),
                "tier": entry.get("tier", "NOT_IN_CACHE"),
                "source": "boxscore",
            }
    return {"catcher_id": None, "name": "UNKNOWN", "strike_rate": 50.0,
            "tier": "AVG", "source": "not_found"}


# ─────────────────────────────────────────────────────────────
# Analysis
# ─────────────────────────────────────────────────────────────

def _classify_tfi(record: dict) -> str:
    """Classify one record's TFI status using stored log fields or MLB API lookup."""
    # If the snapshot log already has TFI fields, use them
    if record.get("tfi_label") and record["tfi_label"] not in ("NO DATA", None):
        return "ACTIVE" if record.get("tfi_penalty_pct", 0) > 0 else "INACTIVE"
    # Otherwise fall back to computing it from the MLB API
    pid = record.get("pitcher_id")
    game_date = record.get("game_date")
    if not pid or not game_date:
        return "UNKNOWN"

    team_id = _find_pitcher_team_id(pid, game_date)
    if not team_id:
        return "UNKNOWN"

    yesterday_game = _fetch_team_previous_game(team_id, game_date)
    today_start, today_venue = _fetch_today_game_time(team_id, game_date)

    if not yesterday_game:
        return "INACTIVE"

    rest = _compute_rest_hours(yesterday_game.get("game_time_utc"), today_start)
    getaway = rest is not None and rest < TFI_REST_THRESHOLD

    tz_y = _venue_tz_offset(yesterday_game.get("venue_name", ""))
    tz_t = _venue_tz_offset(today_venue)
    tz_shift = abs(int(tz_y - tz_t))
    cross_tz = tz_shift >= TFI_TZ_THRESHOLD

    active = getaway or cross_tz
    label = "INACTIVE"
    if active:
        parts = []
        if getaway:
            parts.append(f"GETAWAY({rest:.0f}h)")
        if cross_tz:
            parts.append(f"CROSS_TZ(Δ{tz_shift}h)")
        label = "ACTIVE: " + "+".join(parts)
    return label


def _classify_framing(record: dict, framing_cache: dict) -> str:
    """Classify the catcher framing tier for this game."""
    if record.get("catcher_framing_label"):
        lbl = record["catcher_framing_label"]
        if "ELITE" in lbl:  return "ELITE (>50%)"
        if "POOR"  in lbl:  return "POOR (<48%)"
        return "AVG"
    pid = record.get("pitcher_id")
    game_date = record.get("game_date")
    if not pid or not game_date:
        return "UNKNOWN"
    catcher = _fetch_game_catcher(pid, game_date, framing_cache)
    rate = catcher.get("strike_rate", 50.0)
    if rate > FRAMING_ELITE_THRESHOLD:
        return f"ELITE: {catcher['name']} ({rate}%)"
    elif rate < FRAMING_POOR_THRESHOLD:
        return f"POOR: {catcher['name']} ({rate}%)"
    else:
        return f"AVG: {catcher['name']} ({rate}%)"


def _classify_vaa(record: dict) -> str:
    if record.get("vaa_flat") is not None:
        return "FLAT (penalty active)" if record["vaa_flat"] else "NORMAL"
    if record.get("vaa_degrees") is not None:
        v = record["vaa_degrees"]
        return "FLAT" if v < -4.5 else f"NORMAL ({v}°)"
    return "UNKNOWN (pre-game)"


# ─────────────────────────────────────────────────────────────
# Report printing
# ─────────────────────────────────────────────────────────────

def _accuracy_split(records: list, label_fn, market: str) -> dict:
    """
    Split records into buckets by label_fn output and compute accuracy for each.
    Returns { label: {wins, losses, pushes, pct, avg_err} }
    """
    result_key = "hits_result" if market == "hits" else "ks_result"
    proj_key   = "projected_hits" if market == "hits" else "projected_ks"
    actual_key = "actual_hits" if market == "hits" else "actual_ks"

    buckets: dict[str, dict] = defaultdict(lambda: {"wins": 0, "losses": 0, "pushes": 0, "errors": []})

    for r in records:
        label = label_fn(r)
        # Normalize to ACTIVE / INACTIVE / UNKNOWN
        if label.startswith("ACTIVE") or label.startswith("ELITE") or label == "FLAT (penalty active)":
            bucket_key = "ACTIVE (penalty/boost triggered)"
        elif label.startswith("INACTIVE") or label.startswith("AVG") or label == "NORMAL":
            bucket_key = "INACTIVE (no adjustment)"
        else:
            bucket_key = f"UNKNOWN ({label})"

        result = r.get(result_key)
        if result == "WIN":    buckets[bucket_key]["wins"]   += 1
        elif result == "LOSS": buckets[bucket_key]["losses"] += 1
        elif result == "PUSH": buckets[bucket_key]["pushes"] += 1

        proj   = r.get(proj_key)
        actual = r.get(actual_key)
        if proj is not None and actual is not None:
            buckets[bucket_key]["errors"].append(abs(proj - actual))

    out = {}
    for k, v in buckets.items():
        total = v["wins"] + v["losses"]
        mae = round(sum(v["errors"]) / len(v["errors"]), 2) if v["errors"] else None
        out[k] = {
            "wins": v["wins"],
            "losses": v["losses"],
            "pushes": v["pushes"],
            "total": total,
            "pct": f"{round(100*v['wins']/total)}%" if total > 0 else "N/A",
            "mae": mae,
        }
    return out


def _print_split(title: str, split: dict):
    print(f"\n  {title}")
    print(f"  {'─'*64}")
    print(f"  {'Group':<40} {'W':>4} {'L':>4} {'P':>4} {'Win%':>6} {'MAE':>6}")
    print(f"  {'─'*40} {'─'*4} {'─'*4} {'─'*4} {'─'*6} {'─'*6}")
    for label, data in sorted(split.items()):
        mae_str = str(data["mae"]) if data["mae"] is not None else "—"
        print(f"  {label:<40} {data['wins']:>4} {data['losses']:>4} {data['pushes']:>4} "
              f"{data['pct']:>6} {mae_str:>6}")


def _print_insight(title: str, split: dict, market: str):
    """Print a plain-English takeaway about the split."""
    active   = split.get("ACTIVE (penalty/boost triggered)")
    inactive = split.get("INACTIVE (no adjustment)")

    if not active or not inactive:
        return

    act_pct  = active["wins"]  / (active["wins"]  + active["losses"])  if (active["wins"]  + active["losses"])  else None
    inact_pct = inactive["wins"] / (inactive["wins"] + inactive["losses"]) if (inactive["wins"] + inactive["losses"]) else None

    if act_pct is None or inact_pct is None:
        return

    delta = round((act_pct - inact_pct) * 100, 1)
    market_label = "hits UNDER" if market == "hits" else "strikeout UNDER"

    print(f"\n  INSIGHT — {title}")
    if abs(delta) < 3:
        print(f"  No meaningful difference detected ({delta:+.1f}pp). Need more data.")
    elif delta > 0:
        print(f"  ✓ The formula correctly identifies these {market_label} edges.")
        print(f"    ACTIVE games win {delta:+.1f}pp more often than baseline.")
    else:
        print(f"  ⚠ Formula may be overcorrecting.")
        print(f"    ACTIVE games win {delta:+.1f}pp LESS often than baseline — review weights.")


def print_report(records: list, var_filter: str | None, export: bool):
    if not records:
        print("\n  No resolved records found in results_log.jsonl.")
        print("  Run track_results.py first to resolve predictions.\n")
        return

    framing_cache = _load_framing_cache()

    dates = sorted(set(r.get("game_date", "") for r in records))
    total = len(records)

    print(f"\n{'═'*72}")
    print(f"  AXIOM HIDDEN VARIABLE AUDIT")
    print(f"  {total} resolved predictions  |  {len(dates)} day(s)")
    print(f"  Date range: {dates[0]} → {dates[-1]}")
    print(f"  Comparing accuracy ACTIVE (adjustment triggered) vs INACTIVE (baseline)")
    print(f"{'═'*72}")

    export_rows = []

    # ── TFI Analysis (SKU #14)
    if var_filter in (None, "tfi"):
        print(f"\n{'─'*72}")
        print(f"  SKU #14 — TRAVEL & FATIGUE INDEX (TFI)")
        print(f"  Penalty: -7% HUSI when rest < 16h OR timezone shift >= 2h")
        print(f"  Fetching TFI data from MLB schedule API (may take ~30 seconds)...")

        tfi_labels = {}
        for i, r in enumerate(records):
            pid = r.get("pitcher_id", "")
            key = f"{pid}_{r.get('game_date', '')}"
            if key not in tfi_labels:
                label = _classify_tfi(r)
                tfi_labels[key] = label
                sys.stdout.write(f"\r  Processed {i+1}/{total} pitchers...")
                sys.stdout.flush()
            r["_tfi_label"] = tfi_labels[key]
        print()

        for market in ("hits", "ks"):
            split = _accuracy_split(records, lambda r: r.get("_tfi_label", "UNKNOWN"), market)
            _print_split(f"TFI impact on {'HITS' if market=='hits' else 'STRIKEOUTS'} under accuracy", split)
            _print_insight("TFI", split, market)
            if export:
                for lbl, data in split.items():
                    export_rows.append({
                        "module": "TFI (#14)", "market": market,
                        "group": lbl, **data
                    })

    # ── Catcher Framing Analysis (SKU #37)
    if var_filter in (None, "framing"):
        print(f"\n{'─'*72}")
        print(f"  SKU #37 — CATCHER FRAMING")
        print(f"  Boost: +4% KUSI when catcher strike_rate > 50% (elite framer)")
        print(f"  Penalty: -2% KUSI when strike_rate < 48% (poor framer)")
        print(f"  Fetching catcher data from MLB boxscores...")

        framing_labels = {}
        for i, r in enumerate(records):
            pid = r.get("pitcher_id", "")
            key = f"{pid}_{r.get('game_date', '')}"
            if key not in framing_labels:
                label = _classify_framing(r, framing_cache)
                framing_labels[key] = label
                sys.stdout.write(f"\r  Processed {i+1}/{total} pitchers...")
                sys.stdout.flush()
            r["_framing_label"] = framing_labels[key]
        print()

        for market in ("hits", "ks"):
            split = _accuracy_split(records, lambda r: r.get("_framing_label", "UNKNOWN"), market)
            _print_split(f"FRAMING impact on {'HITS' if market=='hits' else 'STRIKEOUTS'} under accuracy", split)
            _print_insight("CATCHER FRAMING", split, market)
            if export:
                for lbl, data in split.items():
                    export_rows.append({
                        "module": "CATCHER FRAMING (#37)", "market": market,
                        "group": lbl, **data
                    })

    # ── VAA Analysis (SKU #38) — uses stored log data only
    if var_filter in (None, "vaa"):
        print(f"\n{'─'*72}")
        print(f"  SKU #38 — VAA & EXTENSION PERCEIVED VELOCITY")
        print(f"  Penalty: +10% projected hits when VAA < -4.5° (flat approach angle)")
        print(f"  Boost: +1.5mph perceived velocity when extension > 6.8ft")
        print(f"  (VAA data only available for live-scored games — pre-game = UNKNOWN)")

        has_vaa = sum(1 for r in records if r.get("vaa_degrees") is not None)
        print(f"  VAA data available: {has_vaa}/{total} records")

        for market in ("hits", "ks"):
            split = _accuracy_split(records, _classify_vaa, market)
            _print_split(f"VAA impact on {'HITS' if market=='hits' else 'STRIKEOUTS'} under accuracy", split)
            if has_vaa > 0:
                _print_insight("VAA", split, market)
            if export:
                for lbl, data in split.items():
                    export_rows.append({
                        "module": "VAA (#38)", "market": market,
                        "group": lbl, **data
                    })

    # ── Overall miss analysis: which hidden variable was active during losses
    print(f"\n{'─'*72}")
    print(f"  MISS ANALYSIS — Hidden variables present during model losses")
    print(f"  (shows which variables were triggered on losing days)")
    losses = [r for r in records
              if r.get("hits_result") == "LOSS" or r.get("ks_result") == "LOSS"]
    if losses:
        tfi_on = sum(1 for r in losses if r.get("_tfi_label", "").startswith("ACTIVE"))
        fr_on  = sum(1 for r in losses if r.get("_framing_label", "").startswith("ELITE"))
        vaa_on = sum(1 for r in losses if r.get("vaa_flat") is True)
        print(f"\n  Of {len(losses)} total losses:")
        print(f"  TFI (#14) was ACTIVE : {tfi_on} ({round(100*tfi_on/len(losses))}%)")
        print(f"  FRAMING elite (#37)  : {fr_on}  ({round(100*fr_on/len(losses))}%)")
        print(f"  VAA flat (#38)       : {vaa_on} ({round(100*vaa_on/len(losses))}%)")
    else:
        print("  No losses in the selected date range. Keep going.")

    print(f"\n{'═'*72}")

    if export and export_rows:
        out_path = Path(__file__).parent / "audit.csv"
        with open(out_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["module", "market", "group",
                                                    "wins", "losses", "pushes", "total", "pct", "mae"])
            writer.writeheader()
            writer.writerows(export_rows)
        print(f"  Exported audit results → {out_path}")

    print()


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Audit the impact of hidden variables (TFI, Framing, VAA) on model accuracy."
    )
    parser.add_argument("--days", type=int, default=None,
                        help="Only include the last N days (default: all time)")
    parser.add_argument("--var", choices=["tfi", "framing", "vaa"], default=None,
                        help="Analyze only one specific hidden variable")
    parser.add_argument("--export", action="store_true",
                        help="Export results to validation/audit.csv")
    args = parser.parse_args()

    records = _load_log(args.days)
    if not records:
        print("\nNo resolved records found.")
        print("Run track_results.py to resolve predictions after games finish.")
        print("Then run this script again.\n")
        return

    print_report(records, var_filter=args.var, export=args.export)


if __name__ == "__main__":
    main()
