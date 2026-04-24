"""
track_results.py — Compare Axiom predictions against actual MLB boxscore results.

Run this AFTER games are final for the day. It:
  1. Pulls today's predictions from the Axiom API (via proxy)
  2. Fetches actual pitcher stats from the free MLB Stats API (no auth needed)
  3. Prints a scorecard showing WIN / LOSS / PUSH for each HUSI and KUSI call
  4. Appends resolved results to validation/results_log.jsonl

Usage:
    python validation/track_results.py                     # checks today
    python validation/track_results.py --date 2026-04-21  # checks a specific date

Requires:
    - The proxy running in Tab 1 (for Axiom API calls)
    - Internet access (for MLB Stats API calls — free, no key needed)
"""
import argparse
import json
from datetime import date, datetime, timedelta
from pathlib import Path

import httpx

AXIOM_BASE = "http://localhost:8081"
MLB_BASE = "https://statsapi.mlb.com/api/v1"
LOG_FILE = Path(__file__).parent / "results_log.jsonl"

GRADE_EMOJI = {"WIN": "✅", "LOSS": "❌", "PUSH": "➖", "NO LINE": "⚪"}


# ─────────────────────────────────────────────────────────────
# Data fetching
# ─────────────────────────────────────────────────────────────

def fetch_predictions(target_date: str) -> list:
    """Pull scored pitchers from Axiom API."""
    try:
        resp = httpx.get(
            f"{AXIOM_BASE}/v1/pitchers/today",
            params={"target_date": target_date},
            timeout=30.0,
        )
        resp.raise_for_status()
        return resp.json().get("pitchers", [])
    except httpx.ConnectError:
        print("\nWARNING: Cannot reach Axiom API — is the proxy running?")
        print("  gcloud run services proxy axiom-engine --project=axiom-gtmvelo --region=us-central1 --port=8081\n")
        return []


def fetch_schedule(target_date: str) -> list:
    """Fetch the day's game schedule with game PKs."""
    resp = httpx.get(
        f"{MLB_BASE}/schedule",
        params={"sportId": 1, "date": target_date},
        timeout=20.0,
    )
    resp.raise_for_status()
    games = []
    for date_block in resp.json().get("dates", []):
        for g in date_block.get("games", []):
            games.append({
                "game_pk": str(g.get("gamePk")),
                "status": g.get("status", {}).get("detailedState", ""),
                "home_id": str(g.get("teams", {}).get("home", {}).get("team", {}).get("id", "")),
                "away_id": str(g.get("teams", {}).get("away", {}).get("team", {}).get("id", "")),
            })
    return games


def fetch_boxscore_pitchers(game_pk: str) -> dict:
    """
    Returns a dict of pitcher_id -> stats for a completed game.
    Stats include: hits allowed, strikeouts, innings pitched.
    Only includes pitchers who started (first listed pitcher for each team).
    """
    try:
        resp = httpx.get(
            f"{MLB_BASE}/game/{game_pk}/boxscore",
            timeout=20.0,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"  Warning: Could not fetch boxscore for game {game_pk}: {e}")
        return {}

    result = {}
    for side in ("home", "away"):
        pitchers = data.get("teams", {}).get(side, {}).get("pitchers", [])
        pitcher_info = data.get("teams", {}).get(side, {}).get("players", {})

        if not pitchers:
            continue

        # First pitcher in the list is the starter
        starter_id = str(pitchers[0])
        player_key = f"ID{starter_id}"
        player = pitcher_info.get(player_key, {})
        stats = player.get("stats", {}).get("pitching", {})

        if not stats:
            continue

        def safe_float(v):
            try:
                return float(v)
            except (TypeError, ValueError):
                return None

        # Innings pitched comes as "6.1" meaning 6 and 1/3 innings
        ip_raw = stats.get("inningsPitched", "0")
        try:
            ip_whole, ip_frac = str(ip_raw).split(".") if "." in str(ip_raw) else (str(ip_raw), "0")
            ip = int(ip_whole) + int(ip_frac) / 3.0
        except Exception:
            ip = safe_float(ip_raw) or 0.0

        result[starter_id] = {
            "hits_allowed": safe_float(stats.get("hits")),
            "strikeouts": safe_float(stats.get("strikeOuts")),
            "innings_pitched": round(ip, 2),
            "earned_runs": safe_float(stats.get("earnedRuns")),
            "walks": safe_float(stats.get("baseOnBalls")),
        }

    return result


# ─────────────────────────────────────────────────────────────
# Resolution logic
# ─────────────────────────────────────────────────────────────

def resolve(projection: float | None, line: float | None, actual: float | None) -> str:
    """Determine WIN / LOSS / PUSH for an UNDER bet."""
    if line is None:
        return "NO LINE"
    if actual is None:
        return "PENDING"
    if actual < line:
        return "WIN"    # under hit
    elif actual > line:
        return "LOSS"   # went over
    else:
        return "PUSH"   # exactly at the line


# ─────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────

def update_log(target_date: str, resolved_pitchers: list):
    """
    Append or update results_log.jsonl with resolved results.
    Rewrites any existing unresolved entries for the same date+pitcher.
    """
    existing = []
    if LOG_FILE.exists():
        with LOG_FILE.open() as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        existing.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass

    # Build lookup for resolved data
    resolved_map = {r["pitcher_id"]: r for r in resolved_pitchers}

    updated = []
    already_updated = set()

    for entry in existing:
        if entry.get("game_date") == target_date and entry.get("pitcher_id") in resolved_map:
            pid = entry["pitcher_id"]
            r = resolved_map[pid]
            entry.update({
                "actual_hits": r.get("actual_hits"),
                "actual_ks": r.get("actual_ks"),
                "actual_ip": r.get("actual_ip"),
                "hits_result": r.get("hits_result"),
                "ks_result": r.get("ks_result"),
                "resolved": True,
                "resolved_at": datetime.utcnow().isoformat(),
            })
            already_updated.add(pid)
        updated.append(entry)

    # Append any pitchers not already in the log
    for r in resolved_pitchers:
        if r["pitcher_id"] not in already_updated:
            updated.append(r)

    with LOG_FILE.open("w") as f:
        for entry in updated:
            f.write(json.dumps(entry) + "\n")


# ─────────────────────────────────────────────────────────────
# Display
# ─────────────────────────────────────────────────────────────

def _fmt(val, decimals=1):
    if val is None:
        return "—"
    return f"{round(val, decimals)}"


def print_scorecard(resolved: list, target_date: str):
    hits_wins = hits_losses = hits_pushes = hits_no_line = 0
    ks_wins = ks_losses = ks_pushes = ks_no_line = 0

    has_lines = any(r.get("hits_line") or r.get("k_line") for r in resolved)
    has_actuals = any(r.get("actual_hits") is not None for r in resolved)

    # ── Projection accuracy table (always shown when we have actuals)
    if has_actuals:
        print(f"\n{'═'*90}")
        print(f"  AXIOM PROJECTION ACCURACY — {target_date}")
        print(f"  (comparing model projections vs actual boxscore results)")
        print(f"{'═'*90}")
        print(f"  {'Pitcher':<22} {'IP':>5}  "
              f"{'HUSI':>5} {'Proj-H':>6} {'Act-H':>5} {'H-Err':>6}  "
              f"{'KUSI':>5} {'Proj-K':>6} {'Act-K':>5} {'K-Err':>6}")
        print(f"  {'─'*22} {'─'*5}  "
              f"{'─'*5} {'─'*6} {'─'*5} {'─'*6}  "
              f"{'─'*5} {'─'*6} {'─'*5} {'─'*6}")

        h_errors, k_errors = [], []
        for r in sorted(resolved, key=lambda x: x.get("husi") or 0, reverse=True):
            ph = r.get("projected_hits")
            ah = r.get("actual_hits")
            pk = r.get("projected_ks")
            ak = r.get("actual_ks")
            h_err = round(ph - ah, 1) if ph is not None and ah is not None else None
            k_err = round(pk - ak, 1) if pk is not None and ak is not None else None
            if h_err is not None: h_errors.append(abs(h_err))
            if k_err is not None: k_errors.append(abs(k_err))

            # Show direction arrow: + means model projected more than actual (conservative = good for UNDER)
            h_err_str = f"{'+' if h_err and h_err > 0 else ''}{h_err}" if h_err is not None else "—"
            k_err_str = f"{'+' if k_err and k_err > 0 else ''}{k_err}" if k_err is not None else "—"

            print(
                f"  {r.get('pitcher','?'):<22} {_fmt(r.get('actual_ip')):>5}  "
                f"{_fmt(r.get('husi')):>5} {_fmt(r.get('projected_hits')):>6} {_fmt(r.get('actual_hits')):>5} {h_err_str:>6}  "
                f"{_fmt(r.get('kusi')):>5} {_fmt(r.get('projected_ks')):>6} {_fmt(r.get('actual_ks')):>5} {k_err_str:>6}"
            )

        print(f"{'─'*90}")
        if h_errors:
            print(f"  Avg hits projection error  : {round(sum(h_errors)/len(h_errors), 2)} hits")
        if k_errors:
            print(f"  Avg Ks   projection error  : {round(sum(k_errors)/len(k_errors), 2)} Ks")
        print(f"  (H-Err / K-Err: + = model over-projected, - = model under-projected)")

    # ── WIN/LOSS table (only shown when we have prop lines)
    if has_lines:
        print(f"\n{'═'*82}")
        print(f"  AXIOM WIN/LOSS SCORECARD — {target_date}")
        print(f"{'═'*82}")
        print(f"  {'Pitcher':<22} {'IP':>4}  "
              f"{'HUSI':>5} {'H-Line':>6} {'Actual':>6} {'H-Result':<10}  "
              f"{'KUSI':>5} {'K-Line':>6} {'Actual':>6} {'K-Result':<10}")
        print(f"  {'─'*22} {'─'*4}  "
              f"{'─'*5} {'─'*6} {'─'*6} {'─'*10}  "
              f"{'─'*5} {'─'*6} {'─'*6} {'─'*10}")

        for r in sorted(resolved, key=lambda x: x.get("husi") or 0, reverse=True):
            h_res = r.get("hits_result", "PENDING")
            k_res = r.get("ks_result", "PENDING")
            h_icon = GRADE_EMOJI.get(h_res, "⏳")
            k_icon = GRADE_EMOJI.get(k_res, "⏳")

            if h_res == "WIN":      hits_wins += 1
            elif h_res == "LOSS":   hits_losses += 1
            elif h_res == "PUSH":   hits_pushes += 1
            elif h_res == "NO LINE": hits_no_line += 1

            if k_res == "WIN":      ks_wins += 1
            elif k_res == "LOSS":   ks_losses += 1
            elif k_res == "PUSH":   ks_pushes += 1
            elif k_res == "NO LINE": ks_no_line += 1

            print(
                f"  {r.get('pitcher','?'):<22} {_fmt(r.get('actual_ip')):>4}  "
                f"{_fmt(r.get('husi')):>5} {_fmt(r.get('hits_line')):>6} {_fmt(r.get('actual_hits')):>6} "
                f"{h_icon} {h_res:<9}  "
                f"{_fmt(r.get('kusi')):>5} {_fmt(r.get('k_line')):>6} {_fmt(r.get('actual_ks')):>6} "
                f"{k_icon} {k_res:<9}"
            )

        print(f"{'─'*82}")

        def pct(w, l):
            total = w + l
            return f"{round(100*w/total)}%" if total > 0 else "N/A"

        print(f"\n  HITS (UNDER) : {hits_wins}W  {hits_losses}L  {hits_pushes}P  {hits_no_line} no-line  →  {pct(hits_wins, hits_losses)}")
        print(f"  KS   (UNDER) : {ks_wins}W  {ks_losses}L  {ks_pushes}P  {ks_no_line} no-line  →  {pct(ks_wins, ks_losses)}")
        combined_w = hits_wins + ks_wins
        combined_l = hits_losses + ks_losses
        print(f"  COMBINED     : {combined_w}W  {combined_l}L  →  {pct(combined_w, combined_l)}")

    elif not has_actuals:
        print("\n  No actual results found — games may not be final yet.")

    if not has_lines and has_actuals:
        print(f"\n  NOTE: No prop lines were stored for this date.")
        print(f"  Run score_today.py BEFORE games start to capture lines.")
        print(f"  Projection accuracy above is still valid for model tuning.\n")

    print(f"{'═'*82}\n")


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Check Axiom predictions vs actual MLB results.")
    parser.add_argument("--date", default=str(date.today() - timedelta(days=1)),
                        help="Date to check (YYYY-MM-DD, defaults to yesterday)")
    parser.add_argument("--no-log", action="store_true", help="Do not update results_log.jsonl")
    args = parser.parse_args()
    target_date = args.date

    print(f"\nFetching Axiom predictions for {target_date}...")
    predictions = fetch_predictions(target_date)

    if not predictions:
        print("No predictions found in Axiom API for this date.")
        print("Run score_today.py first to score the pitchers.\n")
        return

    print(f"Found {len(predictions)} pitchers. Fetching MLB schedule...")
    games = fetch_schedule(target_date)
    print(f"Found {len(games)} games. Fetching boxscores...")

    # Fetch all boxscores
    all_actual: dict[str, dict] = {}
    final_games = [g for g in games if "Final" in g.get("status", "") or "Game Over" in g.get("status", "")]
    print(f"{len(final_games)} games are final.")

    for g in games:
        stats = fetch_boxscore_pitchers(g["game_pk"])
        all_actual.update(stats)

    # Resolve each prediction
    resolved = []
    for p in predictions:
        pid = p.get("pitcher_id", "")
        actual = all_actual.get(pid, {})

        actual_hits = actual.get("hits_allowed")
        actual_ks = actual.get("strikeouts")
        actual_ip = actual.get("innings_pitched")

        hits_result = resolve(p.get("projected_hits"), p.get("hits_line"), actual_hits)
        ks_result = resolve(p.get("projected_ks"), p.get("k_line"), actual_ks)

        resolved.append({
            "pitcher_id": pid,
            "pitcher": p.get("pitcher"),
            "team": p.get("team"),
            "game_date": target_date,
            "husi": p.get("husi"),
            "kusi": p.get("kusi"),
            "husi_grade": p.get("husi_grade"),
            "kusi_grade": p.get("kusi_grade"),
            "hits_line": p.get("hits_line"),
            "projected_hits": p.get("projected_hits"),
            "k_line": p.get("k_line"),
            "projected_ks": p.get("projected_ks"),
            "actual_hits": actual_hits,
            "actual_ks": actual_ks,
            "actual_ip": actual_ip,
            "hits_result": hits_result,
            "ks_result": ks_result,
            "resolved": actual_hits is not None or actual_ks is not None,
            "resolved_at": datetime.utcnow().isoformat() if actual_hits is not None else None,
        })

    print_scorecard(resolved, target_date)

    if not args.no_log:
        update_log(target_date, resolved)
        print(f"Results saved → {LOG_FILE}\n")


if __name__ == "__main__":
    main()
