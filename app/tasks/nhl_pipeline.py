"""
nhl_pipeline.py — NHL scoring pipeline orchestrator.

Execution order:
  1. Fetch game contexts from the NHL API (nhl_schedule.py)
  2. Build all feature sets and score all players (nhl_feature_builder.py)
  3. Fetch player prop lines from The Rundown API (nhl_props.py)
  4. Calculate edges (projected − line) and assign signal tags
  5. Save all results to the database (unless dry_run=True)
  6. Write a row to pipeline_run_log (sport="NHL")
  7. Return a summary dict

Edge convention (matches NFL): projected_value − prop_line.
  Positive edge → formula projects over the line.
  Negative edge → formula projects under the line.

Signal tags are formula-derived NHL-specific labels (not edge-based):
  Goalies:  ELITE/STOP | VOLATILE/BACKUP | FATIGUE/B2B
  Skaters:  HOT/MATCHUP | COLD/DEF | FATIGUE/B2B

ML signal (ALIGNED | LEAN | SPLIT) compares formula vs ML projection
per market for skaters. Goalies have no ML component.

Safety rule: if the pipeline returns 0 scored players, nothing is written
to the database.

Run locally without writing to the DB:
  python -m app.tasks.nhl_pipeline
"""
import asyncio
import time
import zoneinfo
from datetime import date, datetime
from typing import Optional

_EASTERN = zoneinfo.ZoneInfo("America/New_York")


def _today_eastern() -> date:
    """Today's date in Eastern time (NHL schedule uses ET, not UTC)."""
    return datetime.now(tz=_EASTERN).date()

from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.core.nhl.features import NHLGoalieFeatureSet, NHLSkaterFeatureSet
from app.models.base import AsyncSessionLocal
from app.models.models import (
    NHLGame,
    NHLGameRoster,
    NHLGoalieFeaturesDaily,
    NHLSkaterFeaturesDaily,
    NHLModelOutputDaily,
    PipelineRunLog,
)
from app.services.nhl_schedule import build_game_contexts
from app.services.nhl_props import (
    get_nhl_player_props,
    build_props_lookup,
    lookup_prop,
)
from app.tasks.nhl_feature_builder import build_all_feature_sets, compute_signal
from app.services.rundown import RundownAdapter
from app.utils.logging import get_logger

log = get_logger("nhl_pipeline")


# ─────────────────────────────────────────────────────────────
# Season helper
# ─────────────────────────────────────────────────────────────

def _nhl_season_year(game_date: date) -> int:
    """
    Return the NHL season start year for a given date.
    The NHL season runs October through June.
    Dates Aug–Dec belong to the current year's season start.
    Dates Jan–Jul belong to the prior year's season start.
    """
    return game_date.year if game_date.month >= 8 else game_date.year - 1


# ─────────────────────────────────────────────────────────────
# Signal tag helpers
# ─────────────────────────────────────────────────────────────

def _signal_tag_goalie(fs: NHLGoalieFeatureSet, score: dict) -> Optional[str]:
    """
    Assign a formula-based signal tag to a goalie result.
    Priority order — first match wins:
      ELITE/STOP      — GSAI ≥ 65: elite shot suppression environment
      VOLATILE/BACKUP — GV1 triggered: unconfirmed or backup starter
      FATIGUE/B2B     — GV3 triggered: goalie's team on back-to-back
    """
    if score["gsai"] >= 65:
        return "ELITE/STOP"
    if not fs.is_confirmed_starter:
        return "VOLATILE/BACKUP"
    # GV3: gen_b2b_penalty norm score < 35 → team is on B2B
    b2b_val = getattr(fs, "gen_b2b_penalty", None)
    if b2b_val is not None and b2b_val < 35:
        return "FATIGUE/B2B"
    return None


def _signal_tag_skater(fs: NHLSkaterFeatureSet, score: dict) -> Optional[str]:
    """
    Assign a formula-based signal tag to a skater result.
    Priority order — first match wins:
      HOT/MATCHUP — PPSI ≥ 63: favorable scoring environment
      COLD/DEF    — PPSI < 44: stingy defensive matchup
      FATIGUE/B2B — PV2 triggered: player's team on back-to-back
    """
    if score["ppsi"] >= 63:
        return "HOT/MATCHUP"
    if score["ppsi"] < 44:
        return "COLD/DEF"
    if fs.ctx:
        on_b2b = fs.ctx.home_b2b if fs.is_home else fs.ctx.away_b2b
        if on_b2b:
            return "FATIGUE/B2B"
    return None


def _ml_signal(formula_val: float, ml_val: Optional[float]) -> Optional[str]:
    """Compute ML signal for a single market. Returns None if ML is unavailable."""
    if ml_val is None:
        return None
    return compute_signal(formula_val, ml_val)


# ─────────────────────────────────────────────────────────────
# Primary pipeline function
# ─────────────────────────────────────────────────────────────

async def run_nhl_pipeline(game_date: str = None, dry_run: bool = False) -> dict:
    """
    Run the full NHL scoring pipeline for a given date.

    Args:
        game_date: Date string "YYYY-MM-DD". Defaults to today.
        dry_run:   If True, compute all scores but do NOT save anything to the
                   database. Use this for local testing or pre-flight validation.

    Returns:
        Summary dict with keys: sport, status, games_processed, goalies_scored,
        skaters_scored, dry_run, elapsed_seconds, errors.
    """
    target_date = date.fromisoformat(game_date) if game_date else _today_eastern()
    date_str    = str(target_date)
    start_time  = time.monotonic()
    errors: list[str] = []

    log.info("NHL pipeline starting", date=date_str, dry_run=dry_run)

    try:
        # ─────────────────────────────────────────────────────
        # Step 1 — Fetch game contexts
        # ─────────────────────────────────────────────────────
        log.info("NHL pipeline step 1: fetching game contexts")
        game_contexts = build_game_contexts(game_date=date_str)

        if not game_contexts:
            log.warning("NHL pipeline: no games found for %s — exiting early", date_str)
            elapsed = round(time.monotonic() - start_time, 2)
            await _write_run_log(
                status="no_data",
                target_date=target_date,
                games_processed=0,
                players_scored=0,
                elapsed=elapsed,
                dry_run=dry_run,
                error_message=f"No NHL games found for {date_str}.",
            )
            return {
                "sport":           "NHL",
                "status":          "no_data",
                "games_processed": 0,
                "goalies_scored":  0,
                "skaters_scored":  0,
                "dry_run":         dry_run,
                "elapsed_seconds": elapsed,
                "errors":          errors,
            }

        log.info("NHL pipeline step 1 complete", games_found=len(game_contexts))

        # ─────────────────────────────────────────────────────
        # Step 1b — Fetch game lines (totals + moneylines)
        # ─────────────────────────────────────────────────────
        game_lines_data: dict = {}
        try:
            rundown = RundownAdapter()
            game_lines_data = await rundown.fetch_game_lines(target_date, sport_id=7)
            log.info("NHL game lines fetched", games_with_lines=len(game_lines_data))
        except Exception as exc:
            log.warning("NHL game lines fetch failed, continuing without",
                        error=str(exc))

        # ─────────────────────────────────────────────────────
        # Step 2 — Build all feature sets and score all players
        # ─────────────────────────────────────────────────────
        log.info("NHL pipeline step 2: building feature sets and scoring players")
        all_results = build_all_feature_sets(game_contexts, is_playoff=True, game_lines_data=game_lines_data)

        goalies = [
            (p, fs, s) for p, fs, s in all_results
            if isinstance(fs, NHLGoalieFeatureSet)
        ]
        skaters = [
            (p, fs, s) for p, fs, s in all_results
            if isinstance(fs, NHLSkaterFeatureSet)
        ]

        log.info("NHL pipeline step 2 complete",
                 goalies_scored=len(goalies), skaters_scored=len(skaters))

        if not all_results:
            log.warning("NHL pipeline: 0 players scored — nothing written to database")
            elapsed = round(time.monotonic() - start_time, 2)
            await _write_run_log(
                status="no_results",
                target_date=target_date,
                games_processed=len(game_contexts),
                players_scored=0,
                elapsed=elapsed,
                dry_run=dry_run,
                error_message="Scoring engine returned 0 players.",
            )
            return {
                "sport":           "NHL",
                "status":          "no_results",
                "games_processed": len(game_contexts),
                "goalies_scored":  0,
                "skaters_scored":  0,
                "dry_run":         dry_run,
                "elapsed_seconds": elapsed,
                "errors":          errors,
            }

        # ─────────────────────────────────────────────────────
        # Step 3 — Fetch prop lines
        # ─────────────────────────────────────────────────────
        log.info("NHL pipeline step 3: fetching NHL player props")
        props_lookup: dict = {}
        try:
            raw_props    = get_nhl_player_props(date_str)
            props_lookup = build_props_lookup(raw_props)
            log.info("NHL pipeline step 3 complete",
                     unique_players_with_props=len(props_lookup))
        except Exception as prop_exc:
            msg = f"Props fetch failed: {prop_exc}"
            log.warning("NHL pipeline: props unavailable — scoring continues without lines",
                        error=str(prop_exc))
            errors.append(msg)

        # ─────────────────────────────────────────────────────
        # Step 4 — Calculate edges and assign signal tags
        # ─────────────────────────────────────────────────────
        log.info("NHL pipeline step 4: calculating edges and signal tags")

        enriched: list[dict] = []

        for player_dict, fs, score in all_results:
            is_goalie = isinstance(fs, NHLGoalieFeatureSet)

            if is_goalie:
                signal_tag = _signal_tag_goalie(fs, score)
                ml         = None
                markets = [
                    {
                        "market":        "shots_faced",
                        "projected":     score["projected_shots"],
                        "gsai_score":    score["gsai"],
                        "ppsi_score":    None,
                        "ml_projection": None,
                        "ml_signal":     None,
                    },
                ]
            else:
                signal_tag = _signal_tag_skater(fs, score)
                ml         = score.get("ml") or {}
                ml_pts     = ml.get("ml_proj_points")
                ml_sog     = ml.get("ml_proj_shots")
                ml_goals   = ml.get("ml_proj_goals")
                ml_assists = ml.get("ml_proj_assists")
                markets = [
                    {
                        "market":        "points",
                        "projected":     score["projected_points"],
                        "gsai_score":    None,
                        "ppsi_score":    score["ppsi"],
                        "ml_projection": ml_pts,
                        "ml_signal":     _ml_signal(score["projected_points"], ml_pts),
                    },
                    {
                        "market":        "goals",
                        "projected":     score["projected_goals"],
                        "gsai_score":    None,
                        "ppsi_score":    score["ppsi"],
                        "ml_projection": ml_goals,
                        "ml_signal":     _ml_signal(score["projected_goals"], ml_goals),
                    },
                    {
                        "market":        "assists",
                        "projected":     score["projected_assists"],
                        "gsai_score":    None,
                        "ppsi_score":    score["ppsi"],
                        "ml_projection": ml_assists,
                        "ml_signal":     _ml_signal(score["projected_assists"], ml_assists),
                    },
                    {
                        "market":        "shots_on_goal",
                        "projected":     score["projected_sog"],
                        "gsai_score":    None,
                        "ppsi_score":    score["ppsi"],
                        "ml_projection": ml_sog,
                        "ml_signal":     _ml_signal(score["projected_sog"], ml_sog),
                    },
                ]

            for m in markets:
                prop      = lookup_prop(fs.player_name, m["market"], props_lookup)
                prop_line = prop["line"] if prop else None
                m["prop_line"] = prop_line
                m["edge"] = (
                    round(m["projected"] - prop_line, 2)
                    if prop_line is not None else None
                )

            enriched.append({
                "player_dict": player_dict,
                "feature_set": fs,
                "score":       score,
                "signal_tag":  signal_tag,
                "is_goalie":   is_goalie,
                "markets":     markets,
            })

        log.info("NHL pipeline step 4 complete", players_enriched=len(enriched))

        # ─────────────────────────────────────────────────────
        # Step 4b — Cap to game-night dress sizes per team
        # Top 12 forwards + top 6 defensemen + top 2 goalies,
        # ranked by projected_value.  Players outside those caps
        # are dropped here and never written to the database.
        # ─────────────────────────────────────────────────────
        _FWD_POS = {"C", "L", "R"}
        from collections import defaultdict as _dd
        _tg: dict = _dd(list)
        _tf: dict = _dd(list)
        _td: dict = _dd(list)
        for _row in enriched:
            _team = _row["feature_set"].team
            _pos  = (_row["player_dict"].get("position") or "").upper()
            if _row["is_goalie"]:
                _tg[_team].append(_row)
            elif _pos in _FWD_POS:
                _tf[_team].append(_row)
            else:
                _td[_team].append(_row)
        _capped: list = []
        for _team in set(list(_tg) + list(_tf) + list(_td)):
            _capped += sorted(_tg[_team],
                              key=lambda r: r["score"].get("gsai", 0),
                              reverse=True)[:2]
            _capped += sorted(_tf[_team],
                              key=lambda r: r["score"].get("projected_points", 0),
                              reverse=True)[:12]
            _capped += sorted(_td[_team],
                              key=lambda r: r["score"].get("projected_points", 0),
                              reverse=True)[:6]
        log.info("NHL pipeline step 4b: capped to dress sizes",
                 before=len(enriched), after=len(_capped))
        enriched = _capped

        # ─────────────────────────────────────────────────────
        # Step 5 — Save to database
        # ─────────────────────────────────────────────────────
        if dry_run:
            log.info("NHL pipeline step 5: DRY RUN — no data written",
                     players=len(enriched))
        else:
            log.info("NHL pipeline step 5: saving results to database")
            async with AsyncSessionLocal() as db:
                try:
                    await _persist_nhl_results(
                        db=db,
                        target_date=target_date,
                        game_contexts=game_contexts,
                        enriched=enriched,
                    )
                except Exception as db_exc:
                    await db.rollback()
                    msg = f"Database write failed: {db_exc}"
                    log.error("NHL pipeline: database write error", error=str(db_exc))
                    errors.append(msg)
                    raise

        # ─────────────────────────────────────────────────────
        # Step 6 — Log the pipeline run
        # ─────────────────────────────────────────────────────
        elapsed = round(time.monotonic() - start_time, 2)
        log.info("NHL pipeline complete",
                 games_processed=len(game_contexts),
                 goalies_scored=len(goalies),
                 skaters_scored=len(skaters),
                 elapsed_seconds=elapsed,
                 dry_run=dry_run)

        await _write_run_log(
            status="success",
            target_date=target_date,
            games_processed=len(game_contexts),
            players_scored=len(goalies) + len(skaters),
            elapsed=elapsed,
            dry_run=dry_run,
            error_message="; ".join(errors) if errors else None,
        )

        return {
            "sport":           "NHL",
            "status":          "success",
            "games_processed": len(game_contexts),
            "goalies_scored":  len(goalies),
            "skaters_scored":  len(skaters),
            "dry_run":         dry_run,
            "elapsed_seconds": elapsed,
            "errors":          errors,
        }

    except Exception as exc:
        elapsed = round(time.monotonic() - start_time, 2)
        log.error("NHL pipeline: unhandled error",
                  error=str(exc), elapsed_seconds=elapsed)
        await _write_run_log(
            status="error",
            target_date=target_date,
            games_processed=0,
            players_scored=0,
            elapsed=elapsed,
            dry_run=dry_run,
            error_message=str(exc),
        )
        raise


# ─────────────────────────────────────────────────────────────
# Database persistence
# ─────────────────────────────────────────────────────────────

async def _persist_nhl_results(
    db,
    target_date:   date,
    game_contexts: list,
    enriched:      list[dict],
) -> None:
    """
    Upsert all NHL game, roster, feature, and output rows.
    Uses PostgreSQL ON CONFLICT DO UPDATE so re-running the pipeline
    never creates duplicate rows.
    """
    season_year = _nhl_season_year(target_date)

    # ── Upsert nhl_games ─────────────────────────────────────
    for ctx in game_contexts:
        stmt = pg_insert(NHLGame).values(
            game_id            = ctx.game_id,
            game_date          = target_date,
            season_year        = season_year,
            series_game_number = ctx.series_game_number,
            home_team          = ctx.home_team,
            away_team          = ctx.away_team,
            home_series_wins   = ctx.home_series_wins,
            away_series_wins   = ctx.away_series_wins,
            venue              = ctx.venue,
            is_playoff         = True,
        ).on_conflict_do_update(
            index_elements=["game_id"],
            set_={
                "series_game_number": ctx.series_game_number,
                "home_series_wins":   ctx.home_series_wins,
                "away_series_wins":   ctx.away_series_wins,
                "venue":              ctx.venue,
            },
        )
        await db.execute(stmt)

    log.info("NHL pipeline: nhl_games upserted", count=len(game_contexts))

    # ── Upsert nhl_game_rosters, features, and outputs ───────
    goalie_feat_count  = 0
    skater_feat_count  = 0
    output_row_count   = 0

    for row in enriched:
        fs         = row["feature_set"]
        score      = row["score"]
        is_goalie  = row["is_goalie"]
        signal_tag = row["signal_tag"]
        markets    = row["markets"]
        player_dict = row["player_dict"]

        game_id     = fs.ctx.game_id if fs.ctx else None
        player_id   = player_dict.get("playerId")
        player_name = fs.player_name
        team        = fs.team
        opponent    = fs.opponent
        position    = player_dict.get("position", "G" if is_goalie else "F")
        is_home     = fs.is_home
        line_number = player_dict.get("line")
        pp_unit     = player_dict.get("pp_unit")

        if not game_id or not player_id:
            log.warning("NHL pipeline: skipping player with missing game_id or player_id",
                        player=player_name)
            continue

        # ── Roster row ────────────────────────────────────────
        roster_stmt = pg_insert(NHLGameRoster).values(
            game_id            = game_id,
            player_id          = player_id,
            player_name        = player_name,
            team               = team,
            opponent           = opponent,
            position           = position,
            is_home            = is_home,
            line_number        = line_number,
            pp_unit            = pp_unit,
            injury_designation = None,
        ).on_conflict_do_update(
            constraint="uq_nhl_roster_game_player",
            set_={
                "player_name":        player_name,
                "team":               team,
                "opponent":           opponent,
                "position":           position,
                "is_home":            is_home,
                "line_number":        line_number,
                "pp_unit":            pp_unit,
                "injury_designation": None,
            },
        )
        await db.execute(roster_stmt)

        # ── Goalie feature row ────────────────────────────────
        if is_goalie:
            blocks = score.get("blocks", {})
            feat_stmt = pg_insert(NHLGoalieFeaturesDaily).values(
                game_id        = game_id,
                player_id      = player_id,
                player_name    = player_name,
                team           = team,
                gsai_score     = score["gsai"],
                gss_score      = blocks.get("GSS"),
                osq_score      = blocks.get("OSQ"),
                top_score      = blocks.get("TOP"),
                gen_score      = blocks.get("GEN"),
                rfs_score      = blocks.get("RFS"),
                tsc_score      = blocks.get("TSC"),
                projected_shots = score["projected_shots"],
            ).on_conflict_do_update(
                constraint="uq_nhl_goalie_feat_game_player",
                set_={
                    "gsai_score":      score["gsai"],
                    "gss_score":       blocks.get("GSS"),
                    "osq_score":       blocks.get("OSQ"),
                    "top_score":       blocks.get("TOP"),
                    "gen_score":       blocks.get("GEN"),
                    "rfs_score":       blocks.get("RFS"),
                    "tsc_score":       blocks.get("TSC"),
                    "projected_shots": score["projected_shots"],
                },
            )
            await db.execute(feat_stmt)
            goalie_feat_count += 1

        # ── Skater feature row ────────────────────────────────
        else:
            blocks = score.get("blocks", {})
            feat_stmt = pg_insert(NHLSkaterFeaturesDaily).values(
                game_id           = game_id,
                player_id         = player_id,
                player_name       = player_name,
                team              = team,
                ppsi_score        = score["ppsi"],
                osr_score         = blocks.get("OSR"),
                pmr_score         = blocks.get("PMR"),
                per_score         = blocks.get("PER"),
                pop_score         = blocks.get("POP"),
                rps_score         = blocks.get("RPS"),
                tld_score         = blocks.get("TLD"),
                projected_pts     = score["projected_points"],
                projected_sog     = score["projected_sog"],
                projected_goals   = score["projected_goals"],
                projected_assists = score["projected_assists"],
            ).on_conflict_do_update(
                constraint="uq_nhl_skater_feat_game_player",
                set_={
                    "ppsi_score":        score["ppsi"],
                    "osr_score":         blocks.get("OSR"),
                    "pmr_score":         blocks.get("PMR"),
                    "per_score":         blocks.get("PER"),
                    "pop_score":         blocks.get("POP"),
                    "rps_score":         blocks.get("RPS"),
                    "tld_score":         blocks.get("TLD"),
                    "projected_pts":     score["projected_points"],
                    "projected_sog":     score["projected_sog"],
                    "projected_goals":   score["projected_goals"],
                    "projected_assists": score["projected_assists"],
                },
            )
            await db.execute(feat_stmt)
            skater_feat_count += 1

        # ── Model output rows — one per market ────────────────
        ml_is_playoff = bool(
            score.get("ml", {}).get("playoff_discount_applied", True)
            if score.get("ml") else True
        )

        for m in markets:
            out_stmt = pg_insert(NHLModelOutputDaily).values(
                game_id                  = game_id,
                player_id                = player_id,
                player_name              = player_name,
                team                     = team,
                opponent                 = opponent,
                position                 = position,
                market                   = m["market"],
                gsai_score               = m["gsai_score"],
                ppsi_score               = m["ppsi_score"],
                grade                    = score.get("grade"),
                projected_value          = m["projected"],
                prop_line                = m["prop_line"],
                edge                     = m["edge"],
                signal_tag               = signal_tag,
                ml_projection            = m["ml_projection"],
                ml_signal                = m["ml_signal"],
                playoff_discount_applied = ml_is_playoff,
            ).on_conflict_do_update(
                constraint="uq_nhl_output_game_player_market",
                set_={
                    "grade":                    score.get("grade"),
                    "projected_value":          m["projected"],
                    "prop_line":                m["prop_line"],
                    "edge":                     m["edge"],
                    "signal_tag":               signal_tag,
                    "ml_projection":            m["ml_projection"],
                    "ml_signal":                m["ml_signal"],
                    "playoff_discount_applied": ml_is_playoff,
                    "gsai_score":               m["gsai_score"],
                    "ppsi_score":               m["ppsi_score"],
                },
            )
            await db.execute(out_stmt)
            output_row_count += 1

    await db.commit()
    log.info(
        "NHL pipeline: database commit complete",
        goalie_features=goalie_feat_count,
        skater_features=skater_feat_count,
        output_rows=output_row_count,
    )


# ─────────────────────────────────────────────────────────────
# Run-log helper
# ─────────────────────────────────────────────────────────────

async def _write_run_log(
    status:        str,
    target_date:   date,
    games_processed: int,
    players_scored: int,
    elapsed:       float,
    dry_run:       bool,
    error_message: Optional[str] = None,
) -> None:
    """
    Write a row to pipeline_run_log (sport="NHL").
    Uses pitchers_scored to store total NHL players scored (goalies + skaters).
    Silently swallows any write failure so the pipeline summary always returns.
    """
    try:
        async with AsyncSessionLocal() as db:
            db.add(PipelineRunLog(
                sport           = "NHL",
                target_date     = target_date,
                status          = status,
                pitchers_scored = players_scored,
                games_processed = games_processed,
                elapsed_seconds = elapsed,
                error_message   = error_message,
                dry_run         = dry_run,
            ))
            await db.commit()
    except Exception as log_exc:
        log.warning("NHL pipeline: run log write failed", error=str(log_exc))


# ─────────────────────────────────────────────────────────────
# Standalone runner — dry run, no database writes
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    result = asyncio.run(run_nhl_pipeline(dry_run=True))
    print("\n─── NHL Pipeline Summary ───")
    for k, v in result.items():
        print(f"  {k}: {v}")
