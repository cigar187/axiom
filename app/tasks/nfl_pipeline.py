"""
NFL pipeline runner — orchestrates all NFL data fetchers, then runs the scoring engines.

Execution order:
  1. Fetch games + QB starters from ESPN (nfl_schedule.py)
  2. Fetch QB prop lines from The Rundown API (nfl_props.py)
  3. Fetch weather for each game's home stadium (nfl_weather.py)
  4. Build QBFeatureSet for each starter (nfl_feature_builder.py)
  5. Run compute_qpyi + compute_qtdi for each QB
  6. Save all results to the database (unless dry_run=True)
  7. Write a row to pipeline_run_log — same table as MLB
  8. Return a summary dict

Safety rule: if the pipeline returns 0 scored QBs, nothing is written to the database.

Run locally without writing to the DB:
  python -m app.tasks.nfl_pipeline
"""
import asyncio
import time
from datetime import date, datetime, timezone
from typing import Optional

from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.core.nfl.qpyi import compute_qpyi
from app.core.nfl.qtdi import compute_qtdi
from app.models.base import AsyncSessionLocal
from app.models.models import (
    NFLGame,
    NFLQBStarter,
    NFLQBFeaturesDaily,
    NFLModelOutputDaily,
    PipelineRunLog,
)
from app.services.nfl_schedule import get_all_starters_this_week
from app.services.nfl_props import get_nfl_qb_props, match_props_to_starters
from app.services.nfl_weather import get_weather_for_all_games
from app.tasks.nfl_feature_builder import build_all_feature_sets
from app.utils.logging import get_logger

log = get_logger("nfl_pipeline")


# ─────────────────────────────────────────────────────────────
# Season / week helpers
# ─────────────────────────────────────────────────────────────

def _nfl_season_year(today: date) -> int:
    """
    Return the NFL season year for a given date.
    The NFL season runs September through February.
    Any date in Aug–Dec belongs to the current calendar year's season.
    Any date in Jan–Jul belongs to the prior calendar year's season.
    """
    return today.year if today.month >= 8 else today.year - 1


def _nfl_week_estimate(game_date: date, season_year: int) -> int:
    """
    Approximate the NFL week number for a given game date.
    Week 1 begins on the Thursday of the first full weekend in September.
    Approximated as Sep 5 of the season year — accurate within ±1 week.
    In production, week is parsed directly from the ESPN scoreboard response.
    """
    try:
        season_start = date(season_year, 9, 5)
        delta_days = (game_date - season_start).days
        week = max(1, min(22, delta_days // 7 + 1))
        return week
    except Exception:
        return 1


def _signal_tag(edge: Optional[float]) -> Optional[str]:
    """Return a human-readable signal tag based on projection edge vs prop line."""
    if edge is None:
        return None
    if edge >= 15:
        return "STRONG_OVER"
    if edge >= 6:
        return "LEAN_OVER"
    if edge <= -15:
        return "STRONG_UNDER"
    if edge <= -6:
        return "LEAN_UNDER"
    return "NEUTRAL"


# ─────────────────────────────────────────────────────────────
# Primary pipeline function
# ─────────────────────────────────────────────────────────────

async def run_nfl_pipeline(dry_run: bool = False) -> dict:
    """
    Run the full NFL QB scoring pipeline for the current week.

    Args:
        dry_run: If True, compute all scores but do NOT save anything to the database.
                 Use this for local testing or pre-flight validation.

    Returns:
        Summary dict containing: sport, games_processed, qbs_scored, dry_run, errors.
    """
    today      = date.today()
    start_time = time.monotonic()
    errors: list[str] = []

    log.info("NFL pipeline starting", date=str(today), dry_run=dry_run)

    try:
        # ─────────────────────────────────────────────────────
        # Step 1 — Fetch games and QB starters
        # ─────────────────────────────────────────────────────
        log.info("NFL pipeline step 1: fetching games and starters")
        games, starters = get_all_starters_this_week()

        if not games:
            log.warning("NFL pipeline: no games found for this week — exiting early")
            elapsed = round(time.monotonic() - start_time, 2)
            await _write_run_log(
                status="no_data",
                games_processed=0,
                qbs_scored=0,
                elapsed=elapsed,
                dry_run=dry_run,
                error_message="No NFL games found for this week.",
            )
            return {
                "sport":            "NFL",
                "status":           "no_data",
                "games_processed":  0,
                "qbs_scored":       0,
                "dry_run":          dry_run,
                "elapsed_seconds":  elapsed,
                "errors":           errors,
            }

        log.info("NFL pipeline step 1 complete",
                 games_found=len(games), starters_found=len(starters))

        # ─────────────────────────────────────────────────────
        # Step 2 — Fetch prop lines
        # ─────────────────────────────────────────────────────
        log.info("NFL pipeline step 2: fetching QB props")
        date_str = today.strftime("%Y-%m-%d")
        props = get_nfl_qb_props(date_str)
        starters = match_props_to_starters(props, starters)
        log.info("NFL pipeline step 2 complete", props_found=len(props))

        # ─────────────────────────────────────────────────────
        # Step 3 — Fetch weather
        # ─────────────────────────────────────────────────────
        log.info("NFL pipeline step 3: fetching game-day weather")
        weather_by_game = get_weather_for_all_games(games)
        log.info("NFL pipeline step 3 complete",
                 weather_fetches=len(weather_by_game))

        # ─────────────────────────────────────────────────────
        # Step 4 — Build feature sets
        # ─────────────────────────────────────────────────────
        log.info("NFL pipeline step 4: building QB feature sets")
        feature_tuples = build_all_feature_sets(starters, weather_by_game, props)
        log.info("NFL pipeline step 4 complete",
                 feature_sets_built=len(feature_tuples))

        # ─────────────────────────────────────────────────────
        # Step 5 — Score each QB
        # ─────────────────────────────────────────────────────
        log.info("NFL pipeline step 5: scoring QBs")
        scored_qbs: list[dict] = []

        for starter, feature_set in feature_tuples:
            qb_name = starter.get("qb_name", "Unknown")
            try:
                qpyi_result = compute_qpyi(feature_set)
                qtdi_result = compute_qtdi(feature_set)

                # Edge: projected value minus prop line (positive = over lean)
                yards_edge = None
                yards_line = feature_set.pass_yards_line
                if qpyi_result.get("projected_yards") is not None and yards_line is not None:
                    yards_edge = round(qpyi_result["projected_yards"] - yards_line, 2)

                td_edge = None
                td_line = feature_set.td_line
                if qtdi_result.get("projected_tds") is not None and td_line is not None:
                    td_edge = round(qtdi_result["projected_tds"] - td_line, 2)

                scored_qbs.append({
                    "starter":      starter,
                    "feature_set":  feature_set,
                    "qpyi":         qpyi_result,
                    "qtdi":         qtdi_result,
                    "yards_edge":   yards_edge,
                    "td_edge":      td_edge,
                })

                log.info(
                    "NFL pipeline: QB scored",
                    qb=qb_name,
                    team=starter.get("team"),
                    qpyi=qpyi_result["qpyi"],
                    qpyi_grade=qpyi_result["grade"],
                    projected_yards=qpyi_result.get("projected_yards"),
                    yards_edge=yards_edge,
                    qtdi=qtdi_result["qtdi"],
                    qtdi_grade=qtdi_result["grade"],
                    projected_tds=qtdi_result.get("projected_tds"),
                    td_edge=td_edge,
                )

            except Exception as exc:
                msg = f"Scoring failed for {qb_name}: {exc}"
                log.error("NFL pipeline: QB scoring error", qb=qb_name, error=str(exc))
                errors.append(msg)

        if not scored_qbs:
            log.warning("NFL pipeline: 0 QBs scored — nothing written to database")
            elapsed = round(time.monotonic() - start_time, 2)
            await _write_run_log(
                status="no_results",
                games_processed=len(games),
                qbs_scored=0,
                elapsed=elapsed,
                dry_run=dry_run,
                error_message="Scoring engine returned 0 QBs.",
            )
            return {
                "sport":           "NFL",
                "status":          "no_results",
                "games_processed": len(games),
                "qbs_scored":      0,
                "dry_run":         dry_run,
                "elapsed_seconds": elapsed,
                "errors":          errors,
            }

        log.info("NFL pipeline step 5 complete", qbs_scored=len(scored_qbs))

        # ─────────────────────────────────────────────────────
        # Step 6 — Save to database
        # ─────────────────────────────────────────────────────
        if dry_run:
            log.info("NFL pipeline step 6: DRY RUN — no data written",
                     qbs_scored=len(scored_qbs))
        else:
            log.info("NFL pipeline step 6: saving results to database")
            async with AsyncSessionLocal() as db:
                try:
                    await _persist_nfl_results(
                        db=db,
                        today=today,
                        games=games,
                        starters=starters,
                        scored_qbs=scored_qbs,
                    )
                    log.info("NFL pipeline step 6 complete",
                             rows_written=len(scored_qbs))
                except Exception as db_exc:
                    await db.rollback()
                    msg = f"Database write failed: {db_exc}"
                    log.error("NFL pipeline: database write error", error=str(db_exc))
                    errors.append(msg)
                    raise

        # ─────────────────────────────────────────────────────
        # Step 7 — Log the pipeline run
        # ─────────────────────────────────────────────────────
        elapsed = round(time.monotonic() - start_time, 2)
        log.info("NFL pipeline complete",
                 games_processed=len(games),
                 qbs_scored=len(scored_qbs),
                 elapsed_seconds=elapsed,
                 dry_run=dry_run)

        await _write_run_log(
            status="success",
            games_processed=len(games),
            qbs_scored=len(scored_qbs),
            elapsed=elapsed,
            dry_run=dry_run,
            error_message="; ".join(errors) if errors else None,
        )

        return {
            "sport":           "NFL",
            "status":          "success",
            "games_processed": len(games),
            "qbs_scored":      len(scored_qbs),
            "dry_run":         dry_run,
            "elapsed_seconds": elapsed,
            "errors":          errors,
        }

    except Exception as exc:
        elapsed = round(time.monotonic() - start_time, 2)
        log.error("NFL pipeline: unhandled error",
                  error=str(exc), elapsed_seconds=elapsed)
        await _write_run_log(
            status="error",
            games_processed=0,
            qbs_scored=0,
            elapsed=elapsed,
            dry_run=dry_run,
            error_message=str(exc),
        )
        raise


# ─────────────────────────────────────────────────────────────
# Database persistence
# ─────────────────────────────────────────────────────────────

async def _persist_nfl_results(
    db,
    today:       date,
    games:       list[dict],
    starters:    list[dict],
    scored_qbs:  list[dict],
) -> None:
    """
    Upsert all NFL game, starter, feature, and output rows.
    Uses PostgreSQL ON CONFLICT DO UPDATE so re-running the pipeline
    never creates duplicate rows.
    """
    season_year = _nfl_season_year(today)

    # ── Upsert nfl_games ─────────────────────────────────────
    for g in games:
        game_date = g["game_date"]
        week      = _nfl_week_estimate(game_date, season_year)

        stmt = pg_insert(NFLGame).values(
            game_id=g["game_id"],
            game_date=game_date,
            season_year=season_year,
            week=week,
            home_team=g["home_team"],
            away_team=g["away_team"],
            stadium=g.get("stadium", ""),
            surface=g.get("surface", "unknown"),
            is_dome=g.get("is_dome", False),
        ).on_conflict_do_update(
            index_elements=["game_id"],
            set_={
                "stadium": g.get("stadium", ""),
                "surface": g.get("surface", "unknown"),
                "is_dome": g.get("is_dome", False),
            },
        )
        await db.execute(stmt)

    log.info("NFL pipeline: nfl_games upserted", count=len(games))

    # ── Upsert nfl_qb_starters ───────────────────────────────
    for s in starters:
        stmt = pg_insert(NFLQBStarter).values(
            game_id=s["game_id"],
            qb_name=s["qb_name"],
            team=s["team"],
            opponent=s["opponent"],
            is_home=s.get("is_home", True),
            injury_designation=s.get("injury_designation"),
        ).on_conflict_do_update(
            constraint="uq_nfl_starter_game_qb",
            set_={
                "injury_designation": s.get("injury_designation"),
                "is_home":            s.get("is_home", True),
            },
        )
        await db.execute(stmt)

    log.info("NFL pipeline: nfl_qb_starters upserted", count=len(starters))

    # ── Upsert nfl_qb_features_daily + nfl_model_outputs_daily ──
    for result in scored_qbs:
        starter     = result["starter"]
        feature_set = result["feature_set"]
        qpyi_r      = result["qpyi"]
        qtdi_r      = result["qtdi"]
        yards_edge  = result["yards_edge"]
        td_edge     = result["td_edge"]

        game_id     = starter["game_id"]
        qb_name     = starter["qb_name"]
        team        = starter["team"]
        opponent    = starter["opponent"]

        # Determine game-level metadata
        game_match  = next((g for g in games if g["game_id"] == game_id), {})
        game_date   = game_match.get("game_date", today)
        week        = _nfl_week_estimate(game_date, season_year)

        # ── Feature row — all 12 block scores
        feat_stmt = pg_insert(NFLQBFeaturesDaily).values(
            game_id=game_id,
            qb_name=qb_name,
            team=team,
            week=week,
            season_year=season_year,
            # QPYI block scores
            osw_score=qpyi_r.get("osw"),
            qsr_score=qpyi_r.get("qsr"),
            gsp_score=qpyi_r.get("gsp"),
            scb_score=qpyi_r.get("scb"),
            pdr_score=qpyi_r.get("pdr"),
            ens_score=qpyi_r.get("ens"),
            dsr_score=qpyi_r.get("dsr"),
            rct_score=qpyi_r.get("rct"),
            # QTDI-specific block scores
            ord_score=qtdi_r.get("ord"),
            qtr_score=qtdi_r.get("qtr"),
            gsp_td_score=qtdi_r.get("gsp_td"),
            scb_td_score=qtdi_r.get("scb_td"),
        ).on_conflict_do_update(
            constraint="uq_nfl_features_game_qb",
            set_={
                "osw_score":    qpyi_r.get("osw"),
                "qsr_score":    qpyi_r.get("qsr"),
                "gsp_score":    qpyi_r.get("gsp"),
                "scb_score":    qpyi_r.get("scb"),
                "pdr_score":    qpyi_r.get("pdr"),
                "ens_score":    qpyi_r.get("ens"),
                "dsr_score":    qpyi_r.get("dsr"),
                "rct_score":    qpyi_r.get("rct"),
                "ord_score":    qtdi_r.get("ord"),
                "qtr_score":    qtdi_r.get("qtr"),
                "gsp_td_score": qtdi_r.get("gsp_td"),
                "scb_td_score": qtdi_r.get("scb_td"),
            },
        )
        await db.execute(feat_stmt)

        # ── Output rows — one per market (passing_yards, touchdowns)
        for market, score, grade, proj_value, prop_line, edge in [
            (
                "passing_yards",
                qpyi_r["qpyi"],
                qpyi_r["grade"],
                qpyi_r.get("projected_yards"),
                feature_set.pass_yards_line,
                yards_edge,
            ),
            (
                "touchdowns",
                qtdi_r["qtdi"],
                qtdi_r["grade"],
                qtdi_r.get("projected_tds"),
                feature_set.td_line,
                td_edge,
            ),
        ]:
            out_stmt = pg_insert(NFLModelOutputDaily).values(
                game_id=game_id,
                qb_name=qb_name,
                team=team,
                opponent=opponent,
                week=week,
                season_year=season_year,
                market=market,
                qpyi_score=qpyi_r["qpyi"] if market == "passing_yards" else None,
                qtdi_score=qtdi_r["qtdi"] if market == "touchdowns"    else None,
                grade=grade,
                projected_value=proj_value,
                prop_line=prop_line,
                edge=edge,
                signal_tag=_signal_tag(edge),
            ).on_conflict_do_update(
                constraint="uq_nfl_output_game_qb_market",
                set_={
                    "qpyi_score":      qpyi_r["qpyi"] if market == "passing_yards" else None,
                    "qtdi_score":      qtdi_r["qtdi"] if market == "touchdowns"    else None,
                    "grade":           grade,
                    "projected_value": proj_value,
                    "prop_line":       prop_line,
                    "edge":            edge,
                    "signal_tag":      _signal_tag(edge),
                },
            )
            await db.execute(out_stmt)

    await db.commit()
    log.info("NFL pipeline: database commit complete",
             features_written=len(scored_qbs),
             outputs_written=len(scored_qbs) * 2)


# ─────────────────────────────────────────────────────────────
# Run-log helper
# ─────────────────────────────────────────────────────────────

async def _write_run_log(
    status:        str,
    games_processed: int,
    qbs_scored:    int,
    elapsed:       float,
    dry_run:       bool,
    error_message: Optional[str] = None,
) -> None:
    """
    Write a row to pipeline_run_log — same table as MLB.
    Uses pitchers_scored column to store QB count (same semantics, different sport).
    Silently swallows any write failure so the pipeline summary always returns.
    """
    try:
        async with AsyncSessionLocal() as db:
            db.add(PipelineRunLog(
                target_date=date.today(),
                status=status,
                pitchers_scored=qbs_scored,
                games_processed=games_processed,
                elapsed_seconds=elapsed,
                error_message=error_message,
                dry_run=dry_run,
            ))
            await db.commit()
    except Exception as log_exc:
        log.warning("NFL pipeline: run log write failed", error=str(log_exc))


# ─────────────────────────────────────────────────────────────
# Standalone runner — dry run, no database writes
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    result = asyncio.run(run_nfl_pipeline(dry_run=True))
    print("\n─── NFL Pipeline Summary ───")
    for k, v in result.items():
        print(f"  {k}: {v}")
