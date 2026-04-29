"""
Quick script — pulls today's pitcher props directly from The Rundown API
and shows every pitcher who has a line posted, sorted by game time.

Usage:
    python3 validation/props_now.py
    python3 validation/props_now.py --date 2026-04-22
"""
import asyncio
import argparse
from datetime import date

import httpx

RUNDOWN_KEY = "25bf2c3f04a17935d5f26aeb03992fd579ed84b43b30ce102e680996aa8ce028"
RUNDOWN_BASE = "https://therundown.io/api/v2"
HEADERS = {
    "x-therundown-key": RUNDOWN_KEY,
}
MLB_SPORT_ID = 3
MARKET_STRIKEOUTS = 19
MARKET_HITS_ALLOWED = 47


async def fetch_props(date_str: str) -> dict:
    """Pull all pitcher props from The Rundown for a given date."""
    props: dict[str, dict] = {}

    async with httpx.AsyncClient() as client:
        for market_id, market_name in [
            (MARKET_STRIKEOUTS,  "strikeouts"),
            (MARKET_HITS_ALLOWED, "hits_allowed"),
        ]:
            try:
                resp = await client.get(
                    f"{RUNDOWN_BASE}/sports/{MLB_SPORT_ID}/events/{date_str}/props",
                    headers=HEADERS,
                    params={
                        "market_id": market_id,
                        "affiliate_ids": "3,6,19,21,22,23,24",
                        "main_line": "true",
                    },
                    timeout=20.0,
                )
                resp.raise_for_status()
                raw = resp.json()
            except Exception as e:
                print(f"  Warning: Could not fetch {market_name}: {e}")
                continue

            events = raw.get("prop_markets", raw.get("events", raw.get("data", [])))
            for event in events:
                for participant in event.get("participants", []):
                    name = participant.get("name", "").strip()
                    if not name:
                        continue
                    key = name.lower()
                    if key not in props:
                        props[key] = {"name": name, "k_line": None, "hits_line": None,
                                      "k_odds": None, "hits_odds": None, "sportsbook": None}

                    for aff in participant.get("affiliate_props", participant.get("props", [])):
                        aff_name = aff.get("affiliate", {}).get("affiliate_name", "") or aff.get("sportsbook", "")
                        for line in aff.get("lines", []):
                            val = line.get("total")
                            under_odds = line.get("under", {}).get("decimal") or line.get("under_odds")
                            if val is None:
                                continue
                            if market_name == "strikeouts" and props[key]["k_line"] is None:
                                props[key]["k_line"] = float(val)
                                props[key]["k_odds"] = under_odds
                                props[key]["sportsbook"] = aff_name
                            elif market_name == "hits_allowed" and props[key]["hits_line"] is None:
                                props[key]["hits_line"] = float(val)
                                props[key]["hits_odds"] = under_odds

    return props


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=str(date.today()))
    args = parser.parse_args()
    target_date = args.date

    print(f"\nFetching props from The Rundown for {target_date}...")
    props = await fetch_props(target_date)

    if not props:
        print("No props found. Lines may not be posted yet.\n")
        return

    print(f"\n{'═'*65}")
    print(f"  THE RUNDOWN — PITCHER PROPS FOR {target_date}")
    print(f"  {len(props)} pitchers with lines posted")
    print(f"{'═'*65}")
    print(f"  {'Pitcher':<26} {'K Line':>7} {'K Odds':>8} {'H Line':>7} {'Book':<12}")
    print(f"  {'─'*26} {'─'*7} {'─'*8} {'─'*7} {'─'*12}")

    for key in sorted(props.keys()):
        p = props[key]
        k = f"{p['k_line']:.1f}" if p['k_line'] else "—"
        h = f"{p['hits_line']:.1f}" if p['hits_line'] else "—"
        ko = f"{p['k_odds']}" if p['k_odds'] else "—"
        book = p.get("sportsbook") or "—"
        print(f"  {p['name']:<26} {k:>7} {ko:>8} {h:>7} {book:<12}")

    print(f"{'═'*65}\n")
    print("NOTE: Run score_today.py to get HUSI/KUSI scores for these pitchers.")
    print("      Lines above are live — bet window is open NOW.\n")


if __name__ == "__main__":
    asyncio.run(main())
