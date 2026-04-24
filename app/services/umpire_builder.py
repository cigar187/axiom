"""
umpire_builder.py — Proprietary Umpire Tightness Rating engine.

Strategy (per the Axiom architecture spec):
  Instead of buying umpire stats, we build our own by scraping raw pitch-by-pitch
  data from the MLB Statcast public feed (Baseball Savant). We attribute every
  called strike and called ball to the home plate umpire for that game and build
  a per-umpire called_strike_rate and zone_accuracy score.

  The result is stored in our own umpire_profiles table. Nobody can buy or copy
  this — it's built from primary source data run through our own math.

Key metrics we compute:
  1. called_strike_rate  — % of pitches called as strikes (higher = pitcher-friendly)
  2. zone_accuracy       — % of in-zone pitches correctly called strikes
                           AND out-of-zone pitches correctly called balls
  3. favor_direction     — "pitcher" | "batter" | "neutral"
  4. sample_games        — number of games in sample (quality signal)

Data source:
  MLB Stats API schedule + boxscore to identify umpires.
  Baseball Savant Statcast CSV endpoint for pitch-by-pitch data.
  URL: https://baseballsavant.mlb.com/statcast_search/csv

Usage:
  python -m app.services.umpire_builder --season 2026 --days 30
  (run once to backfill, then nightly to stay current)
"""
import asyncio
import csv
import io
import sys
from datetime import date, timedelta
from typing import Optional

import httpx

# Guard: only import SQLAlchemy when running as a script
try:
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.dialects.postgresql import insert as pg_insert
    from sqlalchemy.orm import sessionmaker
    HAS_DB = True
except ImportError:
    HAS_DB = False

from app.utils.logging import get_logger

log = get_logger("umpire_builder")

# Statcast CSV endpoint — free, public, no auth needed
SAVANT_CSV_URL = "https://baseballsavant.mlb.com/statcast_search/csv"

# MLB Stats API
MLB_BASE = "https://statsapi.mlb.com/api/v1"

# Zone accuracy: pitch zones 1-9 are "in zone", 11-14 are "out of zone" (shadow/chase)
# Baseball Savant uses a 13-zone system; zones 1-9 = strike zone
IN_ZONE = {1, 2, 3, 4, 5, 6, 7, 8, 9}
OUT_OF_ZONE = {11, 12, 13, 14}

# Minimum pitches to be included in profiles
MIN_PITCHES = 100


# ─────────────────────────────────────────────────────────────
# Statcast fetcher
# ─────────────────────────────────────────────────────────────

async def fetch_statcast_pitches(
    client: httpx.AsyncClient,
    start_date: str,
    end_date: str,
) -> list[dict]:
    """
    Pull raw pitch-by-pitch data from Baseball Savant for a date range.
    Returns a list of pitch dicts with relevant columns only.
    """
    params = {
        "all": "true",
        "hfPT": "",          # all pitch types
        "hfAB": "",
        "hfBBT": "",
        "hfPR": "called_strike||ball||blocked_ball||",  # called pitches only
        "hfZ": "",
        "stadium": "",
        "hfBBL": "",
        "hfNewZones": "",
        "hfGT": "R||",       # regular season only
        "hfC": "",
        "hfSea": "",
        "hfSit": "",
        "player_type": "pitcher",
        "hfOuts": "",
        "opponent": "",
        "pitcher_throws": "",
        "batter_stands": "",
        "hfSA": "",
        "game_date_gt": start_date,
        "game_date_lt": end_date,
        "hfInfield": "",
        "team": "",
        "position": "",
        "hfRO": "",
        "home_road": "",
        "hfFlag": "",
        "hfPull": "",
        "metric_1": "",
        "hfInn": "",
        "min_pitches": "0",
        "min_results": "0",
        "group_by": "name",
        "sort_col": "pitches",
        "player_event_sort": "api_p_release_speed",
        "sort_order": "desc",
        "min_pas": "0",
        "type": "details",
    }

    log.info("Fetching Statcast pitches", start=start_date, end=end_date)
    try:
        resp = await client.get(
            SAVANT_CSV_URL,
            params=params,
            timeout=60.0,
            headers={"User-Agent": "AxiomDataEngine/1.0"},
        )
        resp.raise_for_status()
        raw_csv = resp.text
    except Exception as exc:
        log.error("Statcast fetch failed", error=str(exc))
        return []

    pitches = []
    reader = csv.DictReader(io.StringIO(raw_csv))
    for row in reader:
        # Only keep called strikes and called balls
        desc = row.get("description", "")
        if desc not in ("called_strike", "ball", "blocked_ball"):
            continue
        pitches.append({
            "game_pk": row.get("game_pk", ""),
            "game_date": row.get("game_date", ""),
            "pitcher": row.get("pitcher", ""),
            "description": desc,
            "zone": _safe_int(row.get("zone")),
            "home_team": row.get("home_team", ""),
            "away_team": row.get("away_team", ""),
        })

    log.info("Statcast pitches fetched", total=len(pitches))
    return pitches


# ─────────────────────────────────────────────────────────────
# Umpire assignment fetcher
# ─────────────────────────────────────────────────────────────

async def fetch_umpire_assignments(
    client: httpx.AsyncClient,
    start_date: str,
    end_date: str,
) -> dict[str, dict]:
    """
    Returns {game_pk: {"umpire_id": str, "umpire_name": str}}
    by scanning the MLB schedule for home plate umpire officials.
    """
    assignments = {}
    try:
        resp = await client.get(
            f"{MLB_BASE}/schedule",
            params={
                "sportId": 1,
                "startDate": start_date,
                "endDate": end_date,
                "hydrate": "officials",
                "gameType": "R",
            },
            timeout=30.0,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        log.error("Umpire assignment fetch failed", error=str(exc))
        return {}

    for date_block in data.get("dates", []):
        for g in date_block.get("games", []):
            gid = str(g.get("gamePk", ""))
            for official in g.get("officials", []):
                if official.get("officialType") == "Home Plate":
                    person = official.get("official", {})
                    assignments[gid] = {
                        "umpire_id": str(person.get("id", "")),
                        "umpire_name": person.get("fullName", ""),
                    }
                    break

    log.info("Umpire assignments fetched", games=len(assignments))
    return assignments


# ─────────────────────────────────────────────────────────────
# Profile builder
# ─────────────────────────────────────────────────────────────

def build_umpire_profiles(
    pitches: list[dict],
    umpire_assignments: dict[str, dict],
) -> dict[str, dict]:
    """
    Combine pitch-by-pitch data with umpire assignments to build profiles.

    Returns:
    {
        umpire_id: {
            "umpire_name": str,
            "called_strike_rate": float,   # 0-100 normalized
            "zone_accuracy": float,        # 0-100 normalized
            "favor_direction": str,        # "pitcher" | "batter" | "neutral"
            "sample_pitches": int,
            "sample_games": int,
        }
    }
    """
    # Per-umpire accumulators
    ump_stats: dict[str, dict] = {}

    for pitch in pitches:
        gid = str(pitch.get("game_pk", ""))
        assignment = umpire_assignments.get(gid)
        if not assignment:
            continue

        uid = assignment["umpire_id"]
        uname = assignment["umpire_name"]

        if uid not in ump_stats:
            ump_stats[uid] = {
                "umpire_name": uname,
                "total_called": 0,
                "called_strikes": 0,
                "in_zone_pitches": 0,
                "in_zone_called_strikes": 0,    # should be strikes → correct calls
                "out_zone_pitches": 0,
                "out_zone_called_balls": 0,      # should be balls → correct calls
                "game_pks": set(),
            }

        s = ump_stats[uid]
        s["game_pks"].add(gid)
        s["total_called"] += 1
        desc = pitch.get("description", "")
        zone = pitch.get("zone")

        is_called_strike = desc == "called_strike"
        if is_called_strike:
            s["called_strikes"] += 1

        if zone in IN_ZONE:
            s["in_zone_pitches"] += 1
            if is_called_strike:
                s["in_zone_called_strikes"] += 1   # correct: in zone, called strike
        elif zone in OUT_OF_ZONE:
            s["out_zone_pitches"] += 1
            if not is_called_strike:
                s["out_zone_called_balls"] += 1     # correct: out of zone, called ball

    # Build final profiles
    profiles = {}
    for uid, s in ump_stats.items():
        if s["total_called"] < MIN_PITCHES:
            continue

        # Called strike rate (raw %)
        csr_raw = s["called_strikes"] / s["total_called"] if s["total_called"] > 0 else 0.30

        # Zone accuracy: % of in-zone pitches correctly called + % of out-zone correctly called
        in_acc = (
            s["in_zone_called_strikes"] / s["in_zone_pitches"]
            if s["in_zone_pitches"] > 0 else 0.85
        )
        out_acc = (
            s["out_zone_called_balls"] / s["out_zone_pitches"]
            if s["out_zone_pitches"] > 0 else 0.85
        )
        zone_acc_raw = (in_acc + out_acc) / 2.0

        # Normalize to 0-100
        # League average CSR ≈ 0.30 (30% of all pitches are called strikes)
        # We scale 0.24-0.36 → 0-100, centered at 50
        csr_norm = _normalize(csr_raw, low=0.24, high=0.36)

        # Zone accuracy: 0.75-0.95 → 0-100
        zone_norm = _normalize(zone_acc_raw, low=0.75, high=0.95)

        # Favor direction
        if csr_norm >= 60:
            favor = "pitcher"
        elif csr_norm <= 40:
            favor = "batter"
        else:
            favor = "neutral"

        profiles[uid] = {
            "umpire_name": s["umpire_name"],
            "called_strike_rate": round(csr_norm, 2),
            "zone_accuracy": round(zone_norm, 2),
            "favor_direction": favor,
            "sample_pitches": s["total_called"],
            "sample_games": len(s["game_pks"]),
        }

        log.info("Umpire profile built",
                 umpire=s["umpire_name"],
                 csr_raw=round(csr_raw, 3),
                 csr_norm=round(csr_norm, 1),
                 zone_acc=round(zone_acc_raw, 3),
                 favor=favor,
                 games=len(s["game_pks"]))

    return profiles


# ─────────────────────────────────────────────────────────────
# Database writer
# ─────────────────────────────────────────────────────────────

async def save_umpire_profiles(profiles: dict, db_url: str):
    """Upsert all computed umpire profiles into the umpire_profiles table."""
    if not HAS_DB:
        log.error("SQLAlchemy not available — cannot save profiles")
        return

    from app.models.models import UmpireProfile
    from datetime import datetime, timezone

    engine = create_async_engine(db_url, echo=False)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with async_session() as session:
        for uid, p in profiles.items():
            stmt = pg_insert(UmpireProfile).values(
                umpire_id=uid,
                umpire_name=p["umpire_name"],
                called_strike_rate=p["called_strike_rate"],
                zone_accuracy=p["zone_accuracy"],
                favor_direction=p["favor_direction"],
                sample_games=p["sample_games"],
                last_updated=datetime.now(timezone.utc),
            ).on_conflict_do_update(
                index_elements=["umpire_id"],
                set_={
                    "called_strike_rate": p["called_strike_rate"],
                    "zone_accuracy": p["zone_accuracy"],
                    "favor_direction": p["favor_direction"],
                    "sample_games": p["sample_games"],
                    "last_updated": datetime.now(timezone.utc),
                },
            )
            await session.execute(stmt)
        await session.commit()

    await engine.dispose()
    log.info("Umpire profiles saved to database", count=len(profiles))


# ─────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────

async def run(days_back: int = 30, db_url: Optional[str] = None):
    """
    Full pipeline: fetch Statcast data → assign umpires → build profiles → save.

    Args:
        days_back: How many days of historical data to process.
        db_url:    Cloud SQL async DB URL. If None, prints profiles to stdout only.
    """
    end = date.today()
    start = end - timedelta(days=days_back)
    start_str = start.strftime("%Y-%m-%d")
    end_str = end.strftime("%Y-%m-%d")

    log.info("Umpire builder starting", start=start_str, end=end_str)

    async with httpx.AsyncClient() as client:
        pitches, assignments = await asyncio.gather(
            fetch_statcast_pitches(client, start_str, end_str),
            fetch_umpire_assignments(client, start_str, end_str),
        )

    profiles = build_umpire_profiles(pitches, assignments)
    log.info("Profiles built", count=len(profiles))

    if db_url:
        await save_umpire_profiles(profiles, db_url)
    else:
        # Print summary to terminal for inspection
        print(f"\n{'═'*70}")
        print(f"  AXIOM UMPIRE PROFILES ({len(profiles)} umpires, last {days_back} days)")
        print(f"{'═'*70}")
        print(f"  {'Umpire':<28} {'CSR':>5} {'Zone':>5} {'Favor':<10} {'Games':>6}")
        print(f"  {'─'*28} {'─'*5} {'─'*5} {'─'*10} {'─'*6}")
        for uid, p in sorted(profiles.items(), key=lambda x: x[1]["called_strike_rate"], reverse=True):
            print(f"  {p['umpire_name']:<28} {p['called_strike_rate']:>5.1f} "
                  f"{p['zone_accuracy']:>5.1f} {p['favor_direction']:<10} {p['sample_games']:>6}")
        print(f"{'═'*70}\n")

    return profiles


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def _safe_int(val) -> Optional[int]:
    try:
        return int(val) if val is not None and val != "" else None
    except (ValueError, TypeError):
        return None


def _normalize(val: float, low: float, high: float) -> float:
    """Scale val from [low, high] to [0, 100]. Clamps to [0, 100]."""
    if high == low:
        return 50.0
    normalized = (val - low) / (high - low) * 100
    return max(0.0, min(100.0, normalized))


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Build Axiom umpire profiles from Statcast data.")
    parser.add_argument("--days", type=int, default=30, help="Days of history to process (default 30)")
    parser.add_argument("--db-url", type=str, default=None, help="Database URL (omit to print only)")
    args = parser.parse_args()

    asyncio.run(run(days_back=args.days, db_url=args.db_url))
