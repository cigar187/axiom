"""
accuracy_report.py — Multi-day accuracy dashboard from results_log.jsonl.

Run anytime to see how the model has performed across all saved days.

Usage:
    python validation/accuracy_report.py
    python validation/accuracy_report.py --days 7    # last 7 days only
    python validation/accuracy_report.py --market ks # strikeouts only
"""
import argparse
import json
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

LOG_FILE = Path(__file__).parent / "results_log.jsonl"

GRADE_ORDER = ["A+", "A", "B+", "B", "C", "D", "F"]


def load_log(days: int | None, market_filter: str | None) -> list:
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


def pct(w, l):
    total = w + l
    return f"{round(100*w/total)}%" if total > 0 else "N/A"


def win_loss(records: list, market: str):
    """Return (wins, losses, pushes, no_line) for a market ('hits' or 'ks')."""
    w = l = p = n = 0
    result_key = "hits_result" if market == "hits" else "ks_result"
    for r in records:
        res = r.get(result_key, "")
        if res == "WIN":    w += 1
        elif res == "LOSS": l += 1
        elif res == "PUSH": p += 1
        elif res == "NO LINE": n += 1
    return w, l, p, n


def print_report(records: list):
    if not records:
        print("\nNo resolved records found in results_log.jsonl.")
        print("Run track_results.py first to resolve predictions.\n")
        return

    dates = sorted(set(r.get("game_date", "") for r in records))
    print(f"\n{'═'*70}")
    print(f"  AXIOM ACCURACY REPORT")
    print(f"  {len(dates)} day(s) of data  |  {len(records)} resolved predictions")
    print(f"  Date range: {dates[0]} → {dates[-1]}")
    print(f"{'═'*70}")

    # Overall
    hw, hl, hp, hn = win_loss(records, "hits")
    kw, kl, kp, kn = win_loss(records, "ks")
    cw, cl = hw + kw, hl + kl

    print(f"\n  OVERALL RESULTS")
    print(f"  {'─'*40}")
    print(f"  Hits  (UNDER) : {hw}W  {hl}L  {hp}P  {hn} no-line  →  {pct(hw, hl)}")
    print(f"  KS    (UNDER) : {kw}W  {kl}L  {kp}P  {kn} no-line  →  {pct(kw, kl)}")
    print(f"  Combined      : {cw}W  {cl}L  →  {pct(cw, cl)}")

    # By day
    print(f"\n  BY DAY")
    print(f"  {'─'*60}")
    print(f"  {'Date':<12} {'Pitchers':>8}  {'H: W-L':>8}  {'H%':>5}  {'K: W-L':>8}  {'K%':>5}")
    print(f"  {'─'*12} {'─'*8}  {'─'*8}  {'─'*5}  {'─'*8}  {'─'*5}")
    for d in dates:
        day_recs = [r for r in records if r.get("game_date") == d]
        dhw, dhl, _, _ = win_loss(day_recs, "hits")
        dkw, dkl, _, _ = win_loss(day_recs, "ks")
        print(
            f"  {d:<12} {len(day_recs):>8}  "
            f"{dhw}W-{dhl}L{'':<3}  {pct(dhw,dhl):>5}  "
            f"{dkw}W-{dkl}L{'':<3}  {pct(dkw,dkl):>5}"
        )

    # By HUSI grade
    print(f"\n  HITS ACCURACY BY HUSI GRADE")
    print(f"  {'─'*40}")
    by_hgrade = defaultdict(list)
    for r in records:
        by_hgrade[r.get("husi_grade", "?")].append(r)
    for grade in GRADE_ORDER + ["?"]:
        grecs = by_hgrade.get(grade)
        if not grecs:
            continue
        gw, gl, _, _ = win_loss(grecs, "hits")
        print(f"  Grade {grade:<3}  {len(grecs):>3} pitchers  {gw}W-{gl}L  →  {pct(gw, gl)}")

    # By KUSI grade
    print(f"\n  KS ACCURACY BY KUSI GRADE")
    print(f"  {'─'*40}")
    by_kgrade = defaultdict(list)
    for r in records:
        by_kgrade[r.get("kusi_grade", "?")].append(r)
    for grade in GRADE_ORDER + ["?"]:
        grecs = by_kgrade.get(grade)
        if not grecs:
            continue
        gw, gl, _, _ = win_loss(grecs, "ks")
        print(f"  Grade {grade:<3}  {len(grecs):>3} pitchers  {gw}W-{gl}L  →  {pct(gw, gl)}")

    # Bottom performers (model was most wrong)
    losses = [r for r in records if r.get("hits_result") == "LOSS" or r.get("ks_result") == "LOSS"]
    if losses:
        print(f"\n  WHERE THE MODEL MISSED ({len(losses)} total losses)")
        print(f"  {'─'*60}")
        for r in sorted(losses, key=lambda x: x.get("game_date", ""), reverse=True)[:10]:
            h_miss = r.get("hits_result") == "LOSS"
            k_miss = r.get("ks_result") == "LOSS"
            market = "HITS" if h_miss else "KS"
            line = r.get("hits_line") if h_miss else r.get("k_line")
            actual = r.get("actual_hits") if h_miss else r.get("actual_ks")
            proj = r.get("projected_hits") if h_miss else r.get("projected_ks")
            print(f"  {r.get('game_date')}  {r.get('pitcher','?'):<22} {market}  "
                  f"line={line}  proj={proj}  actual={actual}")

    print(f"\n{'═'*70}\n")


def main():
    parser = argparse.ArgumentParser(description="Axiom multi-day accuracy dashboard.")
    parser.add_argument("--days", type=int, default=None, help="Only include last N days")
    parser.add_argument("--market", choices=["hits", "ks"], default=None, help="Filter by market")
    args = parser.parse_args()

    records = load_log(args.days, args.market)
    print_report(records)


if __name__ == "__main__":
    main()
