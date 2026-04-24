"""
score_today.py — Run the Axiom pipeline and save a snapshot of today's predictions.

Usage:
    python validation/score_today.py                     # scores today
    python validation/score_today.py --date 2026-04-21  # scores a specific date
    python validation/score_today.py --dry-run           # scores but does NOT save to DB

Output:
    Prints a summary table to the terminal.
    Appends each pitcher's prediction to validation/results_log.jsonl
    (one JSON object per line — easy to read back later for accuracy checking).

Requires:
    - The proxy running in Tab 1:
      gcloud run services proxy axiom-engine --project=axiom-gtmvelo --region=us-central1 --port=8081
"""
import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path

import httpx

AXIOM_BASE = "http://localhost:8081"
TOKEN = "4lhXHZsyl9_N3NaJlYou8byR_tTo_F-7sdK9q2GZ2QI"
LOG_FILE = Path(__file__).parent / "results_log.jsonl"


def run_pipeline(target_date: str, dry_run: bool) -> dict:
    resp = httpx.post(
        f"{AXIOM_BASE}/v1/tasks/run-daily",
        headers={"AXIOM-INTERNAL-TOKEN": TOKEN, "Content-Type": "application/json"},
        json={"target_date": target_date, "dry_run": dry_run},
        timeout=120.0,
    )
    resp.raise_for_status()
    return resp.json()


def fetch_predictions(target_date: str) -> list:
    resp = httpx.get(
        f"{AXIOM_BASE}/v1/pitchers/today",
        params={"target_date": target_date},
        timeout=30.0,
    )
    resp.raise_for_status()
    return resp.json().get("pitchers", [])


def save_snapshot(pitchers: list, target_date: str) -> int:
    """Append each pitcher as one line to results_log.jsonl. Returns count saved."""
    saved = 0
    with LOG_FILE.open("a") as f:
        for p in pitchers:
            record = {
                "snapshot_at": datetime.utcnow().isoformat(),
                "game_date": target_date,
                "pitcher_id": p.get("pitcher_id"),
                "pitcher": p.get("pitcher"),
                "team": p.get("team"),
                "husi": p.get("husi"),
                "kusi": p.get("kusi"),
                "husi_grade": p.get("husi_grade"),
                "kusi_grade": p.get("kusi_grade"),
                "hits_line": p.get("hits_line"),
                "projected_hits": p.get("projected_hits"),
                "k_line": p.get("k_line"),
                "projected_ks": p.get("projected_ks"),
                "hits_edge": p.get("hits_edge"),
                "k_edge": p.get("k_edge"),
                "confidence": p.get("confidence"),
                "data_quality_flag": p.get("data_quality_flag"),
                # Hidden variables (SKU #14, #37, #38) — populated after deploy
                "catcher_name": p.get("catcher_name"),
                "catcher_strike_rate": p.get("catcher_strike_rate"),
                "catcher_framing_label": p.get("catcher_framing_label"),
                "catcher_kusi_adj": p.get("catcher_kusi_adj"),
                "tfi_rest_hours": p.get("tfi_rest_hours"),
                "tfi_tz_shift": p.get("tfi_tz_shift"),
                "tfi_getaway_day": p.get("tfi_getaway_day"),
                "tfi_cross_timezone": p.get("tfi_cross_timezone"),
                "tfi_penalty_pct": p.get("tfi_penalty_pct"),
                "tfi_label": p.get("tfi_label"),
                "vaa_degrees": p.get("vaa_degrees"),
                "extension_ft": p.get("extension_ft"),
                "vaa_flat": p.get("vaa_flat"),
                "extension_elite": p.get("extension_elite"),
                # actual results filled in later by track_results.py
                "actual_hits": None,
                "actual_ks": None,
                "actual_ip": None,
                "hits_result": None,
                "ks_result": None,
                "resolved": False,
            }
            f.write(json.dumps(record) + "\n")
            saved += 1
    return saved


def print_table(pitchers: list, target_date: str):
    print(f"\n{'─'*100}")
    print(f"  AXIOM PREDICTIONS — {target_date}   (F1=Formula  ML=Engine2)")
    print(f"{'─'*100}")
    print(f"  {'Pitcher':<22} {'Team':<5} {'F1-HUSI':>7} {'ML-HUSI':>7} {'Signal':>8} {'F1-KUSI':>7} {'ML-KUSI':>7} {'Proj-H':>7} {'Proj-K':>7}")
    print(f"  {'─'*22} {'─'*5} {'─'*7} {'─'*7} {'─'*8} {'─'*7} {'─'*7} {'─'*7} {'─'*7}")
    tfi_flagged = []
    framing_flagged = []
    for p in pitchers:
        ml_husi = p.get('ml_husi')
        ml_kusi = p.get('ml_kusi')
        signal = p.get('husi_signal') or '—'
        if signal == 'ALIGNED':
            signal = '✓ ALIGN'
        elif signal == 'CONFLICT':
            signal = '⚠ SPLIT'
        elif signal == 'DIVERGENT':
            signal = '~ DIV'
        print(
            f"  {p.get('pitcher','?'):<22} "
            f"{str(p.get('team','?')):<5} "
            f"{p.get('husi') or '—':>7} "
            f"{round(ml_husi,1) if ml_husi else '—':>7} "
            f"{signal:>8} "
            f"{p.get('kusi') or '—':>7} "
            f"{round(ml_kusi,1) if ml_kusi else '—':>7} "
            f"{round(p.get('projected_hits') or 0, 1) or '—':>7} "
            f"{round(p.get('projected_ks') or 0, 1) or '—':>7}"
        )
        # Collect hidden variable flags for summary section below the table
        if p.get("tfi_penalty_pct") and p["tfi_penalty_pct"] > 0:
            tfi_flagged.append(f"  {p.get('pitcher','?'):<22} → {p.get('tfi_label','?')}")
        if p.get("catcher_kusi_adj") and abs(p["catcher_kusi_adj"]) > 0:
            adj_str = f"+{p['catcher_kusi_adj']*100:.0f}%" if p["catcher_kusi_adj"] > 0 else f"{p['catcher_kusi_adj']*100:.0f}%"
            framing_flagged.append(f"  {p.get('pitcher','?'):<22} → {p.get('catcher_name','?')} ({adj_str} KUSI)")
    print(f"{'─'*100}\n")
    # Show hidden variable flags if any were active today
    if tfi_flagged:
        print(f"  SKU #14 TFI PENALTIES ACTIVE:")
        for line in tfi_flagged:
            print(line)
        print()
    if framing_flagged:
        print(f"  SKU #37 CATCHER FRAMING ADJUSTMENTS:")
        for line in framing_flagged:
            print(line)
        print()


def main():
    parser = argparse.ArgumentParser(description="Score today's pitchers via Axiom API.")
    parser.add_argument("--date", default=str(date.today()), help="Date to score (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true", help="Score but do NOT save to database")
    parser.add_argument("--no-log", action="store_true", help="Do not append to results_log.jsonl")
    args = parser.parse_args()

    target_date = args.date

    print(f"\nRunning pipeline for {target_date}...")
    try:
        result = run_pipeline(target_date, dry_run=args.dry_run)
    except httpx.ConnectError:
        print("\nERROR: Cannot connect to Axiom API.")
        print("Make sure the proxy is running in Tab 1:")
        print("  gcloud run services proxy axiom-engine --project=axiom-gtmvelo --region=us-central1 --port=8081\n")
        sys.exit(1)

    print(f"Pipeline status : {result.get('status')}")
    print(f"Pitchers scored : {result.get('pitchers_scored', 0)}")
    print(f"Message         : {result.get('message')}")

    if result.get("status") not in ("success",):
        print("Nothing to display.\n")
        sys.exit(0)

    print(f"\nFetching predictions from API...")
    pitchers = fetch_predictions(target_date)

    if not pitchers:
        print("No predictions returned.\n")
        sys.exit(0)

    print_table(pitchers, target_date)

    if not args.no_log and not args.dry_run:
        saved = save_snapshot(pitchers, target_date)
        print(f"Saved {saved} pitcher snapshots → {LOG_FILE}")
    elif args.dry_run:
        print("Dry run — results_log.jsonl NOT updated.")

    print()


if __name__ == "__main__":
    main()
