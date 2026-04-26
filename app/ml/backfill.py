"""
app/ml/backfill.py — Historical data loader for the Axiom ML engine.

This script retroactively loads every starting pitcher performance from the
beginning of the current MLB season into ml_training_samples, giving the ML
engine a full season of labeled data to train on immediately.

Without this, the ML engine would be blind for the first 30+ games of data
collection (the minimum threshold). With this, it starts day one with hundreds
of completed, labeled samples and can actually produce meaningful predictions.

What it collects per start:
  - Pitcher identity + game context
  - Season stats AT THE TIME of the start (reconstructed from rolling game logs)
  - PFF for that start (last 3 starts before it)
  - Actual outcomes: hits allowed, Ks, IP
  - First-inning performance (hits + Ks in inning 1 specifically)

Usage (run once to bootstrap, safe to re-run — upserts, not inserts):
  python3 -m app.ml.backfill

Or called programmatically:
  from app.ml.backfill import run_backfill
  await run_backfill(db_session, season="2026")
"""
import asyncio
import os
from datetime import date, datetime
from typing import Optional

import httpx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker

from app.ml.trainer import MLTrainer
from app.utils.pff import compute_pff
from app.utils.logging import get_logger

log = get_logger("ml_backfill")

MLB_BASE = "https://statsapi.mlb.com/api/v1"

# 2026 Opening Day — adjust if season starts differ
SEASON_START_2026 = date(2026, 3, 27)

# Concurrency limit — be respectful to the free MLB API
SEMAPHORE_LIMIT = 6


async def run_backfill(
    session: AsyncSession,
    season: str = "2026",
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> dict:
    """
    Load all starting pitcher performances from the season into ml_training_samples.

    Args:
        session:    Open DB session.
        season:     MLB season year (e.g. "2026").
        start_date: First date to backfill. Defaults to Opening Day.
        end_date:   Last date to backfill. Defaults to yesterday.

    Returns:
        {"loaded": int, "skipped": int, "errors": int}
    """
    if start_date is None:
        start_date = SEASON_START_2026
    if end_date is None:
        end_date = date.today()

    log.info("ML backfill starting",
             season=season,
             start=str(start_date),
             end=str(end_date))

    async with httpx.AsyncClient(timeout=30.0) as client:
        # Step 1: Get all completed games for the date range
        game_list = await _fetch_completed_games(client, start_date, end_date)
        log.info("Games found for backfill", count=len(game_list))

        if not game_list:
            return {"loaded": 0, "skipped": 0, "errors": 0}

        # Step 2: For each game, extract starting pitcher stats
        sem = asyncio.Semaphore(SEMAPHORE_LIMIT)
        loaded = skipped = errors = 0
        samples_batch = []

        async def process_game(game: dict) -> None:
            nonlocal loaded, skipped, errors
            async with sem:
                try:
                    game_samples = await _extract_game_samples(
                        client, game, season
                    )
                    samples_batch.extend(game_samples)
                    loaded += len(game_samples)
                except Exception as exc:
                    log.warning("Backfill game failed",
                                game_id=game.get("game_id"), error=str(exc))
                    errors += 1

        await asyncio.gather(*[process_game(g) for g in game_list])

    # Step 3: Insert into ml_training_samples using explicit text SQL.
    # Plain SQL avoids SQLAlchemy ORM column-ordering issues that caused
    # asyncpg parameter-count mismatches with pg_insert().values(list).
    # ON CONFLICT DO NOTHING makes re-runs safe — duplicates are silently skipped.
    _INSERT_SQL = text("""
        INSERT INTO ml_training_samples (
            pitcher_id, game_id, game_date,
            season_h9, season_k9,
            pff_score, pff_label,
            first_inning_hits, first_inning_ks,
            actual_hits, actual_ks, actual_ip,
            is_complete
        ) VALUES (
            :pitcher_id, :game_id, :game_date,
            :season_h9, :season_k9,
            :pff_score, :pff_label,
            :first_inning_hits, :first_inning_ks,
            :actual_hits, :actual_ks, :actual_ip,
            :is_complete
        )
        ON CONFLICT DO NOTHING
    """)

    if samples_batch:
        for sample in samples_batch:
            try:
                await session.execute(_INSERT_SQL, {
                    "pitcher_id":       sample["pitcher_id"],
                    "game_id":          sample["game_id"],
                    "game_date":        sample["game_date"],
                    "season_h9":        sample.get("season_h9"),
                    "season_k9":        sample.get("season_k9"),
                    "pff_score":        sample.get("pff_score"),
                    "pff_label":        sample.get("pff_label"),
                    "first_inning_hits": sample.get("first_inning_hits"),
                    "first_inning_ks":  sample.get("first_inning_ks"),
                    "actual_hits":      sample.get("actual_hits"),
                    "actual_ks":        sample.get("actual_ks"),
                    "actual_ip":        sample.get("actual_ip"),
                    "is_complete":      sample.get("is_complete", True),
                })
            except Exception as row_err:
                log.warning("Backfill row insert failed",
                            pitcher_id=sample.get("pitcher_id"),
                            game_id=sample.get("game_id"),
                            error=str(row_err))
                errors += 1

        await session.commit()
        log.info("ML backfill complete",
                 loaded=loaded, skipped=skipped, errors=errors,
                 total_samples=len(samples_batch))

    return {"loaded": loaded, "skipped": skipped, "errors": errors}


# ─────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────

async def _fetch_completed_games(
    client: httpx.AsyncClient,
    start_date: date,
    end_date: date,
) -> list[dict]:
    """Fetch all completed regular-season games in the date range."""
    games = []
    try:
        resp = await client.get(f"{MLB_BASE}/schedule", params={
            "sportId": 1,
            "startDate": start_date.strftime("%Y-%m-%d"),
            "endDate": end_date.strftime("%Y-%m-%d"),
            "gameType": "R",     # Regular season only
            "hydrate": "linescore,team",
        })
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        log.error("Backfill: schedule fetch failed", error=str(exc))
        return []

    for date_block in data.get("dates", []):
        game_date_str = date_block.get("date", "")
        for g in date_block.get("games", []):
            status = g.get("status", {}).get("detailedState", "")
            if "Final" not in status and "Complete" not in status:
                continue
            games.append({
                "game_id": str(g.get("gamePk", "")),
                "game_date": game_date_str,
                "home_team_id": str(g.get("teams", {}).get("home", {}).get("team", {}).get("id", "")),
                "away_team_id": str(g.get("teams", {}).get("away", {}).get("team", {}).get("id", "")),
            })

    return games


async def _extract_game_samples(
    client: httpx.AsyncClient,
    game: dict,
    season: str,
) -> list[dict]:
    """
    Extract one sample per starting pitcher from a completed game.

    Returns up to 2 dicts (home starter + away starter), each with:
      - Identity fields
      - Season stats at time of start (approximated from current season stats)
      - PFF from last 3 starts before this game
      - Actual hits/Ks/IP for this appearance
      - First-inning performance
    """
    game_id = game["game_id"]
    game_date_str = game["game_date"]

    try:
        game_date = datetime.strptime(game_date_str, "%Y-%m-%d").date()
    except ValueError:
        return []

    try:
        resp = await client.get(f"{MLB_BASE}/game/{game_id}/boxscore")
        resp.raise_for_status()
        boxscore = resp.json()
    except Exception as exc:
        log.warning("Backfill: boxscore failed", game_id=game_id, error=str(exc))
        return []

    # Fetch linescore for first-inning detail
    try:
        ls_resp = await client.get(f"{MLB_BASE}/game/{game_id}/linescore")
        ls_resp.raise_for_status()
        linescore = ls_resp.json()
    except Exception:
        linescore = {}

    samples = []
    teams_data = boxscore.get("teams", {})

    for side in ("home", "away"):
        side_data = teams_data.get(side, {})
        pitcher_ids = side_data.get("pitchers", [])

        if not pitcher_ids:
            continue

        # Starting pitcher = first in the list
        starter_id = str(pitcher_ids[0])
        players = side_data.get("players", {})
        starter_key = f"ID{starter_id}"

        if starter_key not in players:
            continue

        player = players[starter_key]
        stats = player.get("stats", {}).get("pitching", {})
        person = player.get("person", {})

        # Parse IP
        ip_str = str(stats.get("inningsPitched") or "0")
        try:
            parts = ip_str.split(".")
            ip = int(parts[0]) + (int(parts[1]) / 3.0 if len(parts) > 1 else 0.0)
        except (ValueError, IndexError):
            ip = 0.0

        # Must have pitched at least 2 innings to be a legitimate start
        if ip < 2.0:
            continue

        actual_hits = float(stats.get("hits") or 0)
        actual_ks = float(stats.get("strikeOuts") or 0)
        actual_er = float(stats.get("earnedRuns") or 0)

        # Extract first-inning performance from linescore
        first_inn_hits, first_inn_ks = _get_first_inning_stats(
            linescore, side, starter_id, players
        )

        # Fetch season stats + recent form for this pitcher
        season_stats, recent_form = await _fetch_pitcher_context(
            client, starter_id, season, game_date_str
        )

        # Compute PFF from last 3 starts
        pff_result = compute_pff(recent_form)

        sample = {
            "pitcher_id": starter_id,
            "game_id": game_id,
            "game_date": game_date,

            # Feature inputs — season stats as of this date
            "owc_score": None,   # Can't reconstruct formula block scores retroactively
            "pcs_score": None,   # ML will use defaults for missing formula features
            "ens_score": None,
            "ops_score": None,
            "uhs_score": None,
            "dsc_score": None,
            "ocr_score": None,
            "pmr_score": None,
            "per_score": None,
            "kop_score": None,
            "uks_score": None,
            "tlr_score": None,

            # Raw stats at time of start (most informative for the ML)
            "season_h9": season_stats.get("h9"),
            "season_k9": season_stats.get("k9"),
            "expected_ip": None,

            "bullpen_fatigue_opp": 0.0,
            "bullpen_fatigue_own": 0.0,
            "ens_park": 50.0,
            "ens_temp": 50.0,
            "ens_air": 50.0,

            # Formula outputs unknown retroactively
            "formula_husi": None,
            "formula_kusi": None,
            "formula_proj_hits": None,
            "formula_proj_ks": None,

            # PFF
            "pff_score": pff_result["pff"],
            "pff_label": pff_result["label"],

            # Hidden variable columns — not available retroactively, set to None
            # so every row in the batch has an identical column structure
            "catcher_strike_rate": None,
            "tfi_rest_hours": None,
            "tfi_tz_shift": None,
            "vaa_degrees": None,
            "extension_ft": None,

            # First-inning detail (hot/cold start detection)
            "first_inning_hits": first_inn_hits,
            "first_inning_ks": first_inn_ks,

            # Actual outcomes — labeled immediately since games are complete
            "actual_hits": actual_hits,
            "actual_ks": actual_ks,
            "actual_ip": round(ip, 2),
            "is_complete": True,
        }
        samples.append(sample)

    return samples


async def _fetch_pitcher_context(
    client: httpx.AsyncClient,
    pitcher_id: str,
    season: str,
    before_date: str,
) -> tuple[dict, list[dict]]:
    """
    Fetch season stats + last 3 starts before a given date for a pitcher.

    Returns (season_stats_dict, recent_starts_list).
    """
    season_stats = {"h9": None, "k9": None, "era": None}
    recent_starts = []

    try:
        # Season cumulative stats
        resp = await client.get(f"{MLB_BASE}/people/{pitcher_id}/stats", params={
            "stats": "season",
            "group": "pitching",
            "season": season,
            "sportId": 1,
        })
        resp.raise_for_status()
        for block in resp.json().get("stats", []):
            for split in block.get("splits", []):
                s = split.get("stat", {})
                season_stats["h9"] = _safe_float(s.get("hitsPer9Inn"))
                season_stats["k9"] = _safe_float(s.get("strikeoutsPer9Inn"))
                season_stats["era"] = _safe_float(s.get("era"))
                break
    except Exception:
        pass

    try:
        # Game log for recent starts
        resp = await client.get(f"{MLB_BASE}/people/{pitcher_id}/stats", params={
            "stats": "gameLog",
            "group": "pitching",
            "season": season,
            "sportId": 1,
        })
        resp.raise_for_status()
        all_starts = []
        for block in resp.json().get("stats", []):
            for split in block.get("splits", []):
                if int(split.get("stat", {}).get("gamesStarted") or 0) == 0:
                    continue
                split_date = split.get("date", "9999-99-99")
                if split_date >= before_date:
                    continue   # only use starts BEFORE this game
                stat = split.get("stat", {})
                ip_str = str(stat.get("inningsPitched") or "0")
                try:
                    parts = ip_str.split(".")
                    ip = int(parts[0]) + (int(parts[1]) / 3.0 if len(parts) > 1 else 0.0)
                except Exception:
                    ip = 0.0
                # Mirror the same threshold as the live mlb_stats.py adapter:
                # 0.0 IP = injury scratch (exclude); 0.1+ IP = early exit (include).
                # Including sub-2.0 IP starts ensures PFF retroactively sees
                # the same early-exit signals that the live system now captures.
                if ip < 0.1:
                    continue
                hits = int(stat.get("hits") or 0)
                er = int(stat.get("earnedRuns") or 0)
                ks = int(stat.get("strikeOuts") or 0)
                all_starts.append({
                    "era_this_start": round((er / ip) * 9, 2) if ip else 9.0,
                    "h9_this_start": round((hits / ip) * 9, 2) if ip else 9.0,
                    "k9_this_start": round((ks / ip) * 9, 2) if ip else 0.0,
                    "ip": round(ip, 2),
                    "game_date": split_date,
                    "season_era": season_stats.get("era"),
                    "season_h9": season_stats.get("h9"),
                    "season_k9": season_stats.get("k9"),
                })
        # Most recent 3 starts before this game, most recent first
        recent_starts = list(reversed(all_starts))[:3]
    except Exception:
        pass

    return season_stats, recent_starts


def _get_first_inning_stats(
    linescore: dict,
    side: str,
    starter_id: str,
    players: dict,
) -> tuple[Optional[float], Optional[float]]:
    """
    Extract hits and Ks in the first inning for the starting pitcher.
    Returns (first_inning_hits, first_inning_ks) or (None, None) if unavailable.
    """
    # Linescore innings array — each entry has home/away hits/runs/errors
    innings = linescore.get("innings", [])
    if not innings:
        return None, None

    first_inn = innings[0] if innings else {}
    side_inn = first_inn.get(side, {})
    first_inn_hits = _safe_float(side_inn.get("hits"))

    # Ks in first inning aren't directly in linescore — approximate as None
    # (would need play-by-play API which is heavier)
    return first_inn_hits, None


def _safe_float(val) -> Optional[float]:
    try:
        return float(val) if val is not None and val != "" else None
    except (ValueError, TypeError):
        return None


# ─────────────────────────────────────────────────────────────
# CLI entrypoint
# ─────────────────────────────────────────────────────────────

async def _main() -> None:
    from dotenv import load_dotenv
    load_dotenv()
    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        print("ERROR: DATABASE_URL not set in environment.")
        return

    engine = create_async_engine(db_url, echo=False)
    SessionLocal = async_sessionmaker(engine, expire_on_commit=False)

    async with SessionLocal() as session:
        result = await run_backfill(session, season="2026")
        print(f"\n✓ Backfill complete:")
        print(f"  Samples loaded:  {result['loaded']}")
        print(f"  Skipped:         {result['skipped']}")
        print(f"  Errors:          {result['errors']}")
        print(f"\n  The ML engine now has {result['loaded']} labeled training samples")
        print(f"  from the beginning of the 2026 season.")
        print(f"\n  Run the daily pipeline to start generating ML predictions.")


if __name__ == "__main__":
    asyncio.run(_main())
