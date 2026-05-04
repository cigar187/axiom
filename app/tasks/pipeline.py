"""
Daily pipeline runner — orchestrates all data fetchers, then runs the scoring engine.

Execution order:
  1. Fetch games + probable pitchers from MLB Stats API
  2. Fetch prop lines from The Rundown API
  3. Fetch umpire profiles from scraper (returns neutral stubs for now)
  4. Build PitcherFeatureSet for each pitcher (feature_builder.py)
  5. Run compute_hssi + compute_kssi for each pitcher
  6. Save all results to the database (unless dry_run=True)
  7. Return summary

Safety rule: if the pipeline returns 0 pitchers, nothing is written to the database.
"""
import difflib
import time
from datetime import date, datetime
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.core.features import PitcherFeatureSet
from app.core.hssi import compute_hssi
from app.core.kssi import compute_kssi
from app.core.simulation import SimulationEngine
from app.models.models import (
    Game, ProbablePitcher, SportsbookProp,
    PitcherFeaturesDaily, ModelOutputDaily, UmpireProfile, AxiomGameLineup,
    PipelineRunLog,
)
from app.services.mlb_stats import MLBStatsAdapter
from app.services.rundown import RundownAdapter
from app.services.umpire import UmpireScraperAdapter
from app.services.weather import fetch_game_weather
from app.tasks.feature_builder import build_features
from app.utils.bullpen import compute_bfs
from app.utils.normalization import american_odds_to_implied_prob
from app.utils.logging import get_logger

log = get_logger("pipeline")


async def run_daily_pipeline(
    db: AsyncSession,
    target_date: Optional[date] = None,
    dry_run: bool = False,
) -> dict:
    """
    Run the full Axiom daily scoring pipeline.

    Args:
        db:          Active database session
        target_date: Date to score. Defaults to today.
        dry_run:     If True, compute scores but do NOT save to database.

    Returns:
        Summary dict with status, pitcher count, and timing info.
    """
    if target_date is None:
        target_date = date.today()

    start_time = time.monotonic()
    log.info("Pipeline starting", date=str(target_date), dry_run=dry_run)

    # ─────────────────────────────────────────────────────────
    # Step 1: Fetch MLB data
    # ─────────────────────────────────────────────────────────
    mlb = MLBStatsAdapter()
    try:
        mlb_data = await mlb.fetch(target_date)
    except Exception as exc:
        log.error("MLB Stats API fetch failed", error=str(exc))
        try:
            db.add(PipelineRunLog(
                target_date=target_date, status="error", pitchers_scored=0,
                games_processed=0, elapsed_seconds=round(time.monotonic() - start_time, 2),
                error_message=f"MLB Stats API failed: {exc}", dry_run=dry_run,
            ))
            await db.commit()
        except Exception:
            pass
        return {
            "status": "error",
            "message": f"MLB Stats API failed: {exc}",
            "pitchers_scored": 0,
            "target_date": target_date,
            "dry_run": dry_run,
            "elapsed_seconds": time.monotonic() - start_time,
        }

    games_data = mlb_data.get("games", [])
    pitchers_data = mlb_data.get("pitchers", {})
    umpires_from_schedule = mlb_data.get("umpires", {})  # { game_id: {id, name} }

    if not pitchers_data:
        log.warning("No probable pitchers found", date=str(target_date))
        try:
            db.add(PipelineRunLog(
                target_date=target_date, status="no_data", pitchers_scored=0,
                games_processed=len(games_data), elapsed_seconds=round(time.monotonic() - start_time, 2),
                error_message="No probable pitchers found for this date.", dry_run=dry_run,
            ))
            await db.commit()
        except Exception:
            pass
        return {
            "status": "no_data",
            "message": "No probable pitchers found for this date.",
            "pitchers_scored": 0,
            "target_date": target_date,
            "dry_run": dry_run,
            "elapsed_seconds": time.monotonic() - start_time,
        }

    log.info("MLB data fetched", games=len(games_data), pitchers=len(pitchers_data))

    season = str(target_date.year)

    # Fetch all 30 team hitting stats for OCR normalization (opponent K rate, contact rate)
    try:
        team_hitting_stats = await mlb.fetch_all_team_hitting_stats(season)
    except Exception as exc:
        log.warning("Team hitting stats fetch failed, defaulting to neutral", error=str(exc))
        team_hitting_stats = {}
    log.info("Team hitting stats fetched", teams=len(team_hitting_stats))

    # Fetch confirmed lineups + individual batter K rates for all games (feeds TLR block)
    lineup_data: dict[str, dict] = {}
    for g in games_data:
        gid = g["game_id"]
        try:
            lineup_data[gid] = await mlb.fetch_lineup_batter_stats(gid, season)
        except Exception as exc:
            log.warning("Lineup batter stats failed", game_id=gid, error=str(exc))
            lineup_data[gid] = {"home": [], "away": [], "lineup_confirmed": False}
    log.info("Lineup batter stats fetched", games=len(lineup_data))

    # Fetch manager hook tendency for all pitching teams (feeds OPS_HOOK / KOP_HOOK)
    hook_data: dict[str, dict] = {}
    pitcher_team_ids = {p["team_id"] for p in pitchers_data.values() if p.get("team_id")}
    for tid in pitcher_team_ids:
        try:
            hook_data[tid] = await mlb.fetch_manager_hook_tendency(tid, season)
        except Exception as exc:
            log.warning("Manager hook fetch failed", team_id=tid, error=str(exc))
            hook_data[tid] = {"hook_score": 50.0, "games_sampled": 0}
    log.info("Manager hook data fetched", teams=len(hook_data))

    # ── Pitcher Form Factor (PFF): last 3 starts per pitcher
    # Drives TTO1 multipliers in the MGS formula — HOT pitchers suppress early,
    # COLD pitchers struggle from inning 1.
    import asyncio as _asyncio
    pff_sem = _asyncio.Semaphore(8)

    async def _fetch_pff(pid: str) -> None:
        async with pff_sem:
            try:
                form = await mlb.fetch_pitcher_recent_form(pid, season)
                pitchers_data[pid]["recent_form"] = form
            except Exception as exc:
                log.warning("PFF fetch failed", pitcher_id=pid, error=str(exc))
                pitchers_data[pid]["recent_form"] = []

    await _asyncio.gather(*[_fetch_pff(pid) for pid in pitchers_data])
    log.info("Pitcher form factors fetched", pitchers=len(pitchers_data))

    # ── Bullpen fatigue data for all teams involved today
    # Fetches 48-hour rolling window of RP pitch counts for both home and away teams.
    bullpen_fatigue: dict[str, dict] = {}   # {team_id: {yesterday, two_days_ago, closer_id}}
    all_team_ids = set()
    for g in games_data:
        if g.get("home_team_id"): all_team_ids.add(g["home_team_id"])
        if g.get("away_team_id"): all_team_ids.add(g["away_team_id"])

    for tid in all_team_ids:
        try:
            raw = await mlb.fetch_bullpen_fatigue_data(tid, target_date)
            bfs_result = compute_bfs(
                yesterday_pitches=raw["yesterday"],
                two_days_ago_pitches=raw["two_days_ago"],
                closer_id=raw.get("closer_id"),
            )
            bullpen_fatigue[tid] = bfs_result
            log.info("Bullpen fatigue loaded",
                     team_id=tid,
                     bfs=bfs_result["bfs"],
                     label=bfs_result["label"],
                     arms=bfs_result["arms_sampled"])
        except Exception as exc:
            log.warning("Bullpen fatigue fetch failed", team_id=tid, error=str(exc))
            bullpen_fatigue[tid] = {"bfs": 0.0, "red_alert": False, "arms_sampled": 0,
                                    "top_arms": [], "label": "NO DATA"}
    log.info("Bullpen fatigue data fetched", teams=len(bullpen_fatigue))

    # ─────────────────────────────────────────────────────────
    # Step 2: Fetch prop lines
    # ─────────────────────────────────────────────────────────
    rundown = RundownAdapter()
    try:
        rundown_data = await rundown.fetch(target_date)
        props = rundown_data.get("props", {})
    except Exception as exc:
        log.warning("Rundown API failed, continuing without props", error=str(exc))
        props = {}

    log.info("Props fetched", pitchers_with_props=len(props))

    try:
        game_lines_data = await rundown.fetch_game_lines(target_date)
    except Exception as exc:
        log.warning("Rundown game lines fetch failed, continuing without game lines",
                    error=str(exc))
        game_lines_data = {}

    log.info("Game lines fetched", games_with_lines=len(game_lines_data))

    # ─────────────────────────────────────────────────────────
    # Step 3a: Load umpire profiles from our database
    # Profiles are built by umpire_builder.py from Statcast data.
    # Falls back to neutral if table is empty or umpire not found.
    # ─────────────────────────────────────────────────────────
    umpire_profiles: dict[str, dict] = {}
    umpire_scraper = UmpireScraperAdapter()

    # Load all known umpire profiles from DB in one query
    all_ump_rows = (await db.execute(select(UmpireProfile))).scalars().all()
    db_umpire_map: dict[str, UmpireProfile] = {row.umpire_id: row for row in all_ump_rows}
    log.info("Umpire profiles loaded from DB", count=len(db_umpire_map))

    for game_id, ump_info in umpires_from_schedule.items():
        uid = str(ump_info.get("id", ""))
        uname = ump_info.get("name", "")

        if uid and uid in db_umpire_map:
            row = db_umpire_map[uid]
            umpire_profiles[game_id] = {
                "umpire_id": uid,
                "umpire_name": uname,
                # UHS inputs — real data
                "called_strike_rate": row.called_strike_rate or 50.0,
                "zone_accuracy": row.zone_accuracy or 50.0,
                "early_count_strikes": row.called_strike_rate or 50.0,
                "weak_contact_tendency": 50.0,
                # UKS inputs — use best available; neutral where no distinct data exists
                # uks_tight (0.34 weight) = called_strike_rate (best signal for zone size)
                # uks_cstrl (0.26 weight) = neutral — avoids double-counting CSR in UKS
                # uks_2exp (0.22 weight) = zone_accuracy (zone expansion proxy)
                # uks_count (0.18 weight) = neutral — avoids double-counting zone_accuracy
                "zone_tightness": row.called_strike_rate or 50.0,
                "two_strike_expansion": row.zone_accuracy or 50.0,
                "favor_direction": row.favor_direction or "neutral",
                "confirmed": True,
            }
            log.info("Umpire profile matched from DB",
                     game_id=game_id, umpire=uname,
                     csr=row.called_strike_rate, favor=row.favor_direction)
        else:
            # Not in our DB yet — use neutral stub and flag as unconfirmed
            umpire_profiles[game_id] = umpire_scraper.get_neutral_profile(uid, uname)
            if uname:
                log.warning("Umpire not in DB, using neutral", umpire=uname, uid=uid)

    # ─────────────────────────────────────────────────────────
    # Step 3b: Fetch NWS weather for each game's home stadium
    # Free government data — no API key, no licensing fees.
    # ─────────────────────────────────────────────────────────
    import httpx as _httpx
    weather_by_game: dict[str, dict] = {}
    async with _httpx.AsyncClient() as wx_client:
        for g in games_data:
            gid = g["game_id"]
            home_team_abbrev = g.get("home_team_abbrev") or _team_id_to_abbrev(g.get("home_team_id", ""))
            try:
                wx = await fetch_game_weather(wx_client, home_team_abbrev)
                weather_by_game[gid] = wx
                log.info("Weather fetched",
                         game_id=gid, team=home_team_abbrev,
                         temp_f=wx.get("temperature_f"),
                         wind_mph=wx.get("wind_speed_mph"),
                         air_density=wx.get("air_density_score"),
                         source=wx.get("source"))
            except Exception as exc:
                log.warning("Weather fetch failed, using neutral", game_id=gid, error=str(exc))
                weather_by_game[gid] = {}
    log.info("Weather data fetched", games=len(weather_by_game))

    # ─────────────────────────────────────────────────────────
    # Step 3c: Fetch Catcher Framing data (SKU #37)
    # Look up the primary catcher for each game and calculate the KUSI framing boost.
    # ─────────────────────────────────────────────────────────
    from app.services.catcher_service import fetch_game_catchers, get_framing_data
    catcher_by_game: dict[str, dict] = {}   # game_id → {home_catcher_id, away_catcher_id, ...}
    catcher_framing_by_game: dict[str, dict] = {}  # game_id → {home: framing_dict, away: framing_dict}

    async with _httpx.AsyncClient() as cts_client:
        for g in games_data:
            gid = g["game_id"]
            try:
                catchers = await fetch_game_catchers(cts_client, gid)
                catcher_by_game[gid] = catchers
                catcher_framing_by_game[gid] = {
                    "home": get_framing_data(catchers.get("home_catcher_id")),
                    "away": get_framing_data(catchers.get("away_catcher_id")),
                }
                log.info("Catcher framing loaded",
                         game_id=gid,
                         home=catchers.get("home_catcher_name"),
                         away=catchers.get("away_catcher_name"))
            except Exception as exc:
                log.warning("Catcher framing failed, neutral", game_id=gid, error=str(exc))
                catcher_framing_by_game[gid] = {"home": {}, "away": {}}
    log.info("Catcher framing fetched", games=len(catcher_framing_by_game))

    # ─────────────────────────────────────────────────────────
    # Step 3d: Travel & Fatigue Index (SKU #14)
    # Check each pitching team's schedule for getaway day / cross-timezone travel.
    # ─────────────────────────────────────────────────────────
    from app.utils.travel_fatigue import fetch_team_schedule, compute_travel_fatigue_index
    tfi_by_team: dict[str, dict] = {}   # team_id → TFI result dict

    async with _httpx.AsyncClient() as tfi_client:
        for tid in pitcher_team_ids:
            try:
                past_games = await fetch_team_schedule(tfi_client, tid, look_back_days=2,
                                                       target_date=target_date)
                yesterday_game = past_games[0] if past_games else None

                # Find today's game start time for this team
                today_game_time = None
                today_venue_name = ""
                for g in games_data:
                    if str(g.get("home_team_id")) == str(tid) or str(g.get("away_team_id")) == str(tid):
                        today_game_time = g.get("game_time_utc") or g.get("scheduled_start")
                        today_venue_name = (g.get("park") or "").lower()
                        break

                tfi = compute_travel_fatigue_index(
                    team_id=tid,
                    yesterday_game=yesterday_game,
                    today_game_time_utc=today_game_time,
                    today_venue_name=today_venue_name,
                )
                tfi_by_team[tid] = tfi
                if tfi["penalty_active"]:
                    log.info("TFI penalty active",
                             team_id=tid,
                             label=tfi["tfi_label"],
                             rest_h=tfi["rest_hours"],
                             tz_shift=tfi["tz_shift"])
            except Exception as exc:
                log.warning("TFI fetch failed, neutral", team_id=tid, error=str(exc))
                tfi_by_team[tid] = {"rest_hours": 24.0, "tz_shift": 0, "signed_tz_shift": 0,
                                    "penalty_pct": 0.0, "tfi_label": "NO DATA", "penalty_active": False}
    log.info("Travel & Fatigue Index fetched", teams=len(tfi_by_team))

    # ─────────────────────────────────────────────────────────
    # Step 3e: VAA & Extension data (SKU #38)
    # For live games: pull from the live game feed.
    # For pre-game: vaa_data is empty — formula runs without VAA adjustment.
    # ─────────────────────────────────────────────────────────
    vaa_by_pitcher: dict[str, dict] = {}   # pitcher_id → {vaa_degrees, extension_ft}

    async with _httpx.AsyncClient() as vaa_client:
        for pid, pd in pitchers_data.items():
            gid = pd.get("game_id", "")
            try:
                vaa_result = await mlb.fetch_pitcher_vaa_data(vaa_client, pid, gid)
                if vaa_result:
                    vaa_by_pitcher[pid] = vaa_result
            except Exception as exc:
                log.debug("VAA fetch failed, no adjustment", pitcher_id=pid, error=str(exc))

    if vaa_by_pitcher:
        log.info("VAA data fetched", pitchers_with_vaa=len(vaa_by_pitcher))

    # ─────────────────────────────────────────────────────────
    # Step 3f: Statcast pitcher data — SwStr%, HardHit%, actual GB%
    # Baseball Savant is a free public endpoint (no API key).
    # Fetches season-to-date stats and merges into pitchers_data BEFORE
    # feature building so the feature builder sees the real values.
    #
    # Merges:
    #   season_swstr_pct   → per_putw in KUSI (better than K/9)
    #   season_hard_hit_pct → hard_hit_tier in HUSI HV10 penalty
    #   season_gb_pct      → overrides GO/AO ratio with real GB% for HUSI suppressor
    # ─────────────────────────────────────────────────────────
    from app.services.statcast import (
        fetch_and_cache_statcast_stats,
        merge_statcast_into_pitchers,
        update_axiom_pitcher_stats,
        fetch_batter_swing_profiles,
        fetch_team_oaa,
        fetch_team_batting_discipline,
        fetch_pitch_arsenal,
        fetch_team_sprint_speed,
    )

    statcast_data: dict[str, dict] = {}
    try:
        async with _httpx.AsyncClient() as sc_client:
            # Cache-first: writes to our vault, reads from vault if Savant is down
            statcast_data = await fetch_and_cache_statcast_stats(sc_client, db, season)
        sc_matched = merge_statcast_into_pitchers(pitchers_data, statcast_data)
        log.info("Statcast data merged",
                 pitchers_matched=sc_matched,
                 total=len(pitchers_data),
                 source=next((v.get("source") for v in statcast_data.values()), "unknown"))
    except Exception as exc:
        log.warning("Statcast step failed (non-fatal) — stats fall back to MLB API values",
                    error=str(exc))

    # ── SKU #39 — Batter Swing Profiles (bat-tracking leaderboard)
    swing_profiles: dict[str, dict] = {}
    try:
        async with _httpx.AsyncClient() as sp_client:
            swing_profiles = await fetch_batter_swing_profiles(sp_client, int(season))
        log.info("Batter swing profiles fetched", batters=len(swing_profiles))
    except Exception as exc:
        log.warning("Batter swing profiles fetch failed (non-fatal) — collision score will be None",
                    error=str(exc))

    # ── DSC — Team OAA (Outs Above Average) for dsc_def / dsc_infdef / dsc_ofdef
    oaa_data: dict[str, dict] = {}
    try:
        async with _httpx.AsyncClient() as oaa_client:
            oaa_data = await fetch_team_oaa(oaa_client, int(season))
        log.info("Team OAA fetched", teams=len(oaa_data))
    except Exception as exc:
        log.warning("Team OAA fetch failed (non-fatal) — DSC defense stays neutral",
                    error=str(exc))

    # ── OCR — Team Batting Discipline (zone contact, chase rate, foul rate, two-strike K)
    batting_disc_data: dict[str, dict] = {}
    try:
        async with _httpx.AsyncClient() as disc_client:
            batting_disc_data = await fetch_team_batting_discipline(disc_client, int(season))
        log.info("Team batting discipline fetched", teams=len(batting_disc_data))
    except Exception as exc:
        log.warning("Team batting discipline fetch failed (non-fatal) — ocr_zcon/2s/foul/dec stay neutral",
                    error=str(exc))

    # ── OPS/KOP — Team IL Roster (injured batter count per opponent team)
    il_data: dict[str, int] = {}
    try:
        il_data = await mlb.fetch_team_injured_batters(season)
    except Exception as exc:
        log.warning("Team IL roster fetch failed (non-fatal) — ops_inj/kop_inj stay at default",
                    error=str(exc))

    # ── PMR — Pitch Arsenal (whiff rates per pitch type for pmr_p1/p2/put)
    arsenal_data: dict[str, dict] = {}
    try:
        async with _httpx.AsyncClient() as ar_client:
            arsenal_data = await fetch_pitch_arsenal(ar_client, int(season))
        log.info("Pitch arsenal fetched", pitchers=len(arsenal_data))
    except Exception as exc:
        log.warning("Pitch arsenal fetch failed (non-fatal) — PMR p1/p2/put stay neutral",
                    error=str(exc))

    # ── DSC — Team Sprint Speed (dsc_align proxy)
    sprint_speed_data: dict[str, dict] = {}
    try:
        async with _httpx.AsyncClient() as ss_client:
            sprint_speed_data = await fetch_team_sprint_speed(ss_client, int(season))
        log.info("Team sprint speed fetched", teams=len(sprint_speed_data))
    except Exception as exc:
        log.warning("Team sprint speed fetch failed (non-fatal) — dsc_align stays neutral",
                    error=str(exc))

    # ── Always write to Axiom's proprietary data vault, regardless of Statcast success.
    # MLB stats are always available (fetched in Step 1). Statcast fields are NULL
    # when Savant was unavailable — but the MLB layer is still persisted so the vault
    # grows every single pipeline run no matter what external APIs do.
    try:
        await update_axiom_pitcher_stats(db, pitchers_data, statcast_data, season)
    except Exception as vault_exc:
        log.warning("Axiom pitcher stats vault update failed (non-fatal)",
                    error=str(vault_exc))
        await db.rollback()

    # ─────────────────────────────────────────────────────────
    # Step 4 + 5: Build features and score each pitcher
    # ─────────────────────────────────────────────────────────
    game_lookup = {g["game_id"]: g for g in games_data}
    scored_pitchers = []

    # Merge Rundown game lines into game_lookup by fuzzy home team name match.
    # game_lines_data is keyed by Rundown event_id (not MLB game_id), so we
    # match on home team name since that is stable across both APIs.
    for _event_id, gl in game_lines_data.items():
        rundown_home = (gl.get("home_team_name") or "").lower()
        for g in game_lookup.values():
            mlb_home = (g.get("home_team") or "").lower()
            if rundown_home and mlb_home and (
                difflib.SequenceMatcher(None, rundown_home, mlb_home).ratio() >= 0.82
            ):
                g["game_total"]     = gl.get("game_total")
                g["home_moneyline"] = gl.get("home_moneyline")
                g["away_moneyline"] = gl.get("away_moneyline")
                log.debug("Game lines merged",
                          game_id=g["game_id"],
                          total=gl.get("game_total"),
                          home_ml=gl.get("home_moneyline"),
                          away_ml=gl.get("away_moneyline"))
                break

    for pid, pitcher_data in pitchers_data.items():
        game_id = pitcher_data.get("game_id", "")
        game_info = game_lookup.get(game_id, {})

        try:
            # Identify own team and opponent team IDs for bullpen lookup
            own_team_id = pitcher_data.get("team_id", "")
            opp_team_id = (
                game_info.get("away_team_id")
                if pitcher_data.get("side") == "home"
                else game_info.get("home_team_id")
            )

            # Determine which catcher is defending (opponent's catcher)
            # A pitcher faces the opposing team's catcher — that catcher's framing
            # is what actually affects whether borderline pitches are called strikes.
            pitcher_side = pitcher_data.get("side", "home")
            opp_side = "away" if pitcher_side == "home" else "home"
            game_framing = catcher_framing_by_game.get(game_id, {})
            defending_catcher_framing = game_framing.get(opp_side, {})

            features = build_features(
                pitcher_data=pitcher_data,
                props=props,
                umpire_profiles=umpire_profiles,
                all_pitchers_data=pitchers_data,
                game_info={**game_info, **weather_by_game.get(game_id, {})},
                target_date=target_date,
                team_hitting_stats=team_hitting_stats,
                lineup_data=lineup_data.get(game_id, {}),
                hook_data=hook_data.get(own_team_id, {}),
                bullpen_own=bullpen_fatigue.get(own_team_id, {}),
                bullpen_opp=bullpen_fatigue.get(opp_team_id, {}),
                catcher_framing=defending_catcher_framing,
                travel_fatigue=tfi_by_team.get(str(own_team_id), {}),
                vaa_data=vaa_by_pitcher.get(pid, {}),
                swing_profiles=swing_profiles,
                oaa_data=oaa_data,
                arsenal_data=arsenal_data,
                batting_disc_data=batting_disc_data,
                sprint_speed_data=sprint_speed_data,
                il_data=il_data,
            )
            # Stamp the numeric team_id so the simulation can find the manager profile
            features.team_id_numeric = str(own_team_id)

            hssi_result = compute_hssi(features)
            kssi_result = compute_kssi(features)

            # ── Risk Profile (automatic — no manual commands needed)
            from app.services.risk_scorer import compute_risk_profile
            risk_profile = compute_risk_profile(features)

            scored_pitchers.append({
                "features": features,
                "hssi": hssi_result,
                "husi": hssi_result,
                "kssi": kssi_result,
                "kusi": kssi_result,
                "risk": risk_profile,
                "game_info": game_info,
                "pitcher_data": pitcher_data,
            })

            log.info(
                "Pitcher scored",
                pitcher=features.pitcher_name,
                hssi=hssi_result["hssi"],
                kssi=kssi_result["kssi"],
            )

        except Exception as exc:
            log.error("Scoring failed for pitcher",
                      pitcher_id=pid, name=pitcher_data.get("pitcher_name"), error=str(exc))

    if not scored_pitchers:
        log.warning("Pipeline produced 0 scored pitchers — nothing written to database")
        return {
            "status": "no_results",
            "message": "Scoring engine returned 0 pitchers. Nothing saved.",
            "pitchers_scored": 0,
            "target_date": target_date,
            "dry_run": dry_run,
            "elapsed_seconds": time.monotonic() - start_time,
        }

    # ─────────────────────────────────────────────────────────
    # Step 5b: Merlin Probabilistic Simulation (N=2000)
    # Runs AFTER all pitchers are scored so each scorer gets a fresh
    # feature set with fully populated prop lines, park factors, etc.
    # Each pitcher gets 2,000 Monte Carlo iterations with Gaussian jitter
    # on PCS_CMD, OCR_DISC, and ENS_TEMP.
    # ─────────────────────────────────────────────────────────
    sim_engine = SimulationEngine()
    for result in scored_pitchers:
        features: PitcherFeatureSet = result["features"]
        try:
            sim_result = sim_engine.run(
                features=features,
                hits_line=features.hits_line,
                k_line=features.k_line,
            )
            result["sim"] = sim_result
        except Exception as exc:
            log.warning("Simulation failed for pitcher (non-fatal)",
                        pitcher=features.pitcher_name, error=str(exc))
            result["sim"] = None
    log.info("Merlin simulation complete", pitchers=len(scored_pitchers))

    # ─────────────────────────────────────────────────────────
    # Step 6: Save to database
    # ─────────────────────────────────────────────────────────
    if not dry_run:
        await _persist_results(db, target_date, games_data, pitchers_data, props, scored_pitchers, lineup_data, season, swing_profiles)
        log.info("Pipeline results saved to database", count=len(scored_pitchers))
    else:
        log.info("Dry run — results NOT saved", count=len(scored_pitchers))

    elapsed = round(time.monotonic() - start_time, 2)
    log.info("Pipeline complete", pitchers=len(scored_pitchers), elapsed_seconds=elapsed, dry_run=dry_run)

    if not dry_run:
        try:
            db.add(PipelineRunLog(
                target_date=target_date, status="success",
                pitchers_scored=len(scored_pitchers), games_processed=len(games_data),
                elapsed_seconds=elapsed, dry_run=False,
            ))
            await db.commit()
        except Exception as log_exc:
            log.warning("Pipeline run log write failed", error=str(log_exc))

    return {
        "status": "success",
        "message": f"Scored {len(scored_pitchers)} pitchers.",
        "pitchers_scored": len(scored_pitchers),
        "target_date": target_date,
        "dry_run": dry_run,
        "elapsed_seconds": elapsed,
    }


# ─────────────────────────────────────────────────────────────
# Database persistence
# ─────────────────────────────────────────────────────────────

async def _persist_results(
    db: AsyncSession,
    target_date: date,
    games_data: list,
    pitchers_data: dict,
    props: dict,
    scored_pitchers: list,
    lineup_data: dict | None = None,
    season: str = "",
    swing_profiles: dict | None = None,
) -> None:
    """Upsert all scored data into the database."""

    # Upsert games
    for g in games_data:
        stmt = pg_insert(Game).values(
            game_id=g["game_id"],
            game_date=target_date,
            home_team=g.get("home_team", ""),
            away_team=g.get("away_team", ""),
            park=g.get("park"),
            temperature_f=g.get("temperature_f"),
            wind_speed_mph=g.get("wind_speed_mph"),
            wind_direction=g.get("wind_direction"),
            is_dome=g.get("is_dome", False),
            weather_condition=g.get("weather_condition"),
            status=g.get("status", "scheduled"),
            game_total=g.get("game_total"),
            home_moneyline=g.get("home_moneyline"),
            away_moneyline=g.get("away_moneyline"),
        ).on_conflict_do_update(
            index_elements=["game_id"],
            set_={
                "status": g.get("status", "scheduled"),
                "temperature_f": g.get("temperature_f"),
                "wind_direction": g.get("wind_direction"),
                "game_total":     g.get("game_total"),
                "home_moneyline": g.get("home_moneyline"),
                "away_moneyline": g.get("away_moneyline"),
            },
        )
        await db.execute(stmt)

    # Upsert probable pitchers
    for pid, pd in pitchers_data.items():
        stmt = pg_insert(ProbablePitcher).values(
            pitcher_id=pid,
            game_id=pd["game_id"],
            team_id=pd.get("team_id", ""),
            pitcher_name=pd.get("pitcher_name", ""),
            handedness=pd.get("handedness"),
            confirmed_flag=pd.get("confirmed", False),
        ).on_conflict_do_update(
            constraint="uq_pitcher_game",
            set_={
                "confirmed_flag": pd.get("confirmed", False),
                "handedness": pd.get("handedness"),
            },
        )
        await db.execute(stmt)

    # Upsert prop lines
    for result in scored_pitchers:
        features: PitcherFeatureSet = result["features"]
        for market_type, line_val, over_odds, under_odds, sportsbook in [
            ("strikeouts", features.k_line, features.k_over_odds, features.k_under_odds, None),
            ("hits_allowed", features.hits_line, features.hits_over_odds, features.hits_under_odds, None),
        ]:
            if line_val is not None:
                prop_name_key = features.pitcher_name.strip().lower()
                prop_data = props.get(prop_name_key, {})
                mk_key = "strikeouts" if market_type == "strikeouts" else "hits_allowed"
                sbok = prop_data.get(mk_key, {}).get("sportsbook", "unknown")

                await db.execute(
                    pg_insert(SportsbookProp).values(
                        game_id=features.game_id,
                        pitcher_id=features.pitcher_id,
                        sportsbook=sbok,
                        market_type=market_type,
                        line=line_val,
                        over_odds=over_odds,
                        under_odds=under_odds,
                    ).on_conflict_do_nothing()
                )

    # Upsert feature scores
    for result in scored_pitchers:
        features: PitcherFeatureSet = result["features"]
        hssi_r = result["hssi"]
        husi_r = hssi_r
        kssi_r = result["kssi"]
        kusi_r = kssi_r
        risk_r = result.get("risk", {})

        stmt = pg_insert(PitcherFeaturesDaily).values(
            pitcher_id=features.pitcher_id,
            game_id=features.game_id,
            game_date=target_date,
            # HUSI blocks
            owc_score=husi_r.get("owc_score"),
            pcs_score=husi_r.get("pcs_score"),
            ens_score=husi_r.get("ens_score"),
            ops_score=husi_r.get("ops_score"),
            uhs_score=husi_r.get("uhs_score"),
            dsc_score=husi_r.get("dsc_score"),
            # HUSI sub-features
            owc_babip=features.owc_babip, owc_hh=features.owc_hh, owc_bar=features.owc_bar,
            owc_ld=features.owc_ld, owc_xba=features.owc_xba, owc_bot3=features.owc_bot3,
            owc_topheavy=features.owc_topheavy,
            pcs_gb=features.pcs_gb, pcs_soft=features.pcs_soft, pcs_bara=features.pcs_bara,
            pcs_hha=features.pcs_hha, pcs_xbaa=features.pcs_xbaa, pcs_xwobaa=features.pcs_xwobaa,
            pcs_cmd=features.pcs_cmd, pcs_reg=features.pcs_reg,
            ens_park=features.ens_park, ens_windin=features.ens_windin, ens_temp=features.ens_temp,
            ens_air=features.ens_air, ens_roof=features.ens_roof, ens_of=features.ens_of,
            ens_inf=features.ens_inf,
            ops_pcap=features.ops_pcap, ops_hook=features.ops_hook, ops_traffic=features.ops_traffic,
            ops_tto=features.ops_tto, ops_bpen=features.ops_bpen, ops_inj=features.ops_inj,
            ops_trend=features.ops_trend, ops_fat=features.ops_fat,
            uhs_cstr=features.uhs_cstr, uhs_zone=features.uhs_zone, uhs_early=features.uhs_early,
            uhs_weak=features.uhs_weak,
            dsc_def=features.dsc_def, dsc_infdef=features.dsc_infdef, dsc_ofdef=features.dsc_ofdef,
            dsc_catch=features.dsc_catch, dsc_align=features.dsc_align,
            # KUSI blocks
            ocr_score=kusi_r.get("ocr_score"), pmr_score=kusi_r.get("pmr_score"),
            per_score=kusi_r.get("per_score"), kop_score=kusi_r.get("kop_score"),
            uks_score=kusi_r.get("uks_score"), tlr_score=kusi_r.get("tlr_score"),
            # KUSI sub-features
            ocr_k=features.ocr_k, ocr_con=features.ocr_con, ocr_zcon=features.ocr_zcon,
            ocr_disc=features.ocr_disc, ocr_2s=features.ocr_2s, ocr_foul=features.ocr_foul,
            ocr_dec=features.ocr_dec,
            pmr_p1=features.pmr_p1, pmr_p2=features.pmr_p2, pmr_put=features.pmr_put,
            pmr_run=features.pmr_run, pmr_top6=features.pmr_top6, pmr_plat=features.pmr_plat,
            per_ppa=features.per_ppa, per_bb=features.per_bb, per_fps=features.per_fps,
            per_deep=features.per_deep, per_putw=features.per_putw, per_cmdd=features.per_cmdd,
            per_velo=features.per_velo,
            kop_pcap=features.kop_pcap, kop_hook=features.kop_hook, kop_tto=features.kop_tto,
            kop_bpen=features.kop_bpen, kop_pat=features.kop_pat, kop_inj=features.kop_inj,
            kop_fat=features.kop_fat,
            uks_tight=features.uks_tight, uks_cstrl=features.uks_cstrl,
            uks_2exp=features.uks_2exp, uks_count=features.uks_count,
            tlr_top4k=features.tlr_top4k, tlr_top6c=features.tlr_top6c,
            tlr_vet=features.tlr_vet, tlr_top2=features.tlr_top2,
            # Quality flags
            lineup_confirmed=features.lineup_confirmed,
            umpire_confirmed=features.umpire_confirmed,
            bullpen_data_available=features.bullpen_data_available,
            data_quality_flag=_quality_flag(features),
        ).on_conflict_do_update(
            constraint="uq_feat_pitcher_game",
            set_={
                "owc_score": husi_r.get("owc_score"),
                "pcs_score": husi_r.get("pcs_score"),
                "ens_score": husi_r.get("ens_score"),
                "ocr_score": kusi_r.get("ocr_score"),
                "data_quality_flag": _quality_flag(features),
            },
        )
        await db.execute(stmt)

    # Upsert model outputs
    for result in scored_pitchers:
        features: PitcherFeatureSet = result["features"]
        hssi_r = result.get("hssi") or {}
        husi_r = hssi_r
        kssi_r = result.get("kssi") or {}
        kusi_r = kssi_r
        sim_r = result.get("sim")  # SimulationResult or None
        name_key = features.pitcher_name.strip().lower()
        pitcher_props = props.get(name_key, {})

        for market_type, index_score, index_key, proj_key, line_val, under_odds, sbok in [
            (
                "hits_allowed",
                hssi_r.get("hssi"),
                "hssi",
                "projected_hits",
                features.hits_line,
                features.hits_under_odds,
                pitcher_props.get("hits_allowed", {}).get("sportsbook"),
            ),
            (
                "strikeouts",
                kssi_r.get("kssi"),
                "kssi",
                "projected_ks",
                features.k_line,
                features.k_under_odds,
                pitcher_props.get("strikeouts", {}).get("sportsbook"),
            ),
        ]:
            projection = hssi_r.get("projected_hits") if market_type == "hits_allowed" else kssi_r.get("projected_ks")
            stat_edge = None
            if projection is not None and line_val is not None:
                stat_edge = round(line_val - projection, 2)  # positive = under edge

            implied_prob = None
            if under_odds is not None:
                implied_prob = round(american_odds_to_implied_prob(under_odds), 4)

            grade = hssi_r["grade"] if market_type == "hits_allowed" else kssi_r["grade"]
            confidence = _confidence_label(index_score)

            notes_parts = []
            if not features.lineup_confirmed:
                notes_parts.append("lineup unconfirmed")
            if not features.umpire_confirmed:
                notes_parts.append("umpire stub")
            if stat_edge and stat_edge > 0:
                notes_parts.append(f"under edge +{stat_edge}")
            elif stat_edge and stat_edge < 0:
                notes_parts.append(f"over lean {stat_edge}")

            stmt = pg_insert(ModelOutputDaily).values(
                pitcher_id=features.pitcher_id,
                game_id=features.game_id,
                game_date=target_date,
                market_type=market_type,
                hssi=hssi_r["hssi"],
                kssi=kssi_r["kssi"],
                hssi_base=hssi_r["hssi_base"],
                kssi_base=kssi_r["kssi_base"],
                hssi_interaction=hssi_r["hssi_interaction"],
                kssi_interaction=kssi_r["kssi_interaction"],
                hssi_volatility=hssi_r["hssi_volatility"],
                kssi_volatility=kssi_r["kssi_volatility"],
                base_hits=husi_r.get("base_hits"),
                base_ks=kusi_r.get("base_ks"),
                projected_hits=husi_r.get("projected_hits"),
                projected_ks=kusi_r.get("projected_ks"),
                sportsbook=sbok,
                line=line_val,
                under_odds=under_odds,
                implied_under_prob=implied_prob,
                stat_edge=stat_edge,
                grade=grade,
                confidence=confidence,
                notes=", ".join(notes_parts) or None,
                data_quality_flag=_quality_flag(features),
                # ── Risk profile (auto-computed, surfaced via /v1/risk/today)
                pff_score=features.pff_score,
                pff_label=features.pff_label,
                risk_score=risk_r.get("risk_score", 0),
                risk_tier=risk_r.get("risk_tier", "LOW"),
                risk_flags="|".join(risk_r.get("risk_flags", [])) or None,
                combo_risk=risk_r.get("combo_risk", False),
                season_era_tier=features.hard_hit_tier,
                park_extreme=features.park_extreme,
                park_hits_multiplier=features.park_hits_multiplier,
                # ── Merlin Simulation outputs (N=2000)
                sim_median_hits=sim_r.median_hits if sim_r else None,
                sim_median_ks=sim_r.median_ks if sim_r else None,
                sim_over_pct_hits=sim_r.over_pct_hits if sim_r else None,
                sim_under_pct_hits=sim_r.under_pct_hits if sim_r else None,
                sim_p5_hits=sim_r.p5_hits if sim_r else None,
                sim_p95_hits=sim_r.p95_hits if sim_r else None,
                sim_over_pct_ks=sim_r.over_pct_ks if sim_r else None,
                sim_under_pct_ks=sim_r.under_pct_ks if sim_r else None,
                sim_p5_ks=sim_r.p5_ks if sim_r else None,
                sim_p95_ks=sim_r.p95_ks if sim_r else None,
                sim_confidence_hits=sim_r.sim_confidence_hits if sim_r else None,
                sim_confidence_ks=sim_r.sim_confidence_ks if sim_r else None,
                sim_kill_streak_prob=sim_r.kill_streak_probability if sim_r else None,
            ).on_conflict_do_update(
                constraint="uq_output_pitcher_game_market",
                set_={
                    "hssi": hssi_r["hssi"],
                    "kssi": kssi_r["kssi"],
                    "projected_hits": hssi_r.get("projected_hits"),
                    "projected_ks": kssi_r.get("projected_ks"),
                    "stat_edge": stat_edge,
                    "grade": grade,
                    "confidence": confidence,
                    "notes": ", ".join(notes_parts) or None,
                    "data_quality_flag": _quality_flag(features),
                    "pff_score": features.pff_score,
                    "pff_label": features.pff_label,
                    "risk_score": risk_r.get("risk_score", 0),
                    "risk_tier": risk_r.get("risk_tier", "LOW"),
                    "risk_flags": "|".join(risk_r.get("risk_flags", [])) or None,
                    "combo_risk": risk_r.get("combo_risk", False),
                    "season_era_tier": features.hard_hit_tier,
                    "park_extreme": features.park_extreme,
                    "park_hits_multiplier": features.park_hits_multiplier,
                    # ── Simulation columns update on conflict
                    "sim_median_hits": sim_r.median_hits if sim_r else None,
                    "sim_median_ks": sim_r.median_ks if sim_r else None,
                    "sim_over_pct_hits": sim_r.over_pct_hits if sim_r else None,
                    "sim_under_pct_hits": sim_r.under_pct_hits if sim_r else None,
                    "sim_p5_hits": sim_r.p5_hits if sim_r else None,
                    "sim_p95_hits": sim_r.p95_hits if sim_r else None,
                    "sim_over_pct_ks": sim_r.over_pct_ks if sim_r else None,
                    "sim_under_pct_ks": sim_r.under_pct_ks if sim_r else None,
                    "sim_p5_ks": sim_r.p5_ks if sim_r else None,
                    "sim_p95_ks": sim_r.p95_ks if sim_r else None,
                    "sim_confidence_hits": sim_r.sim_confidence_hits if sim_r else None,
                    "sim_confidence_ks": sim_r.sim_confidence_ks if sim_r else None,
                    "sim_kill_streak_prob": sim_r.kill_streak_probability if sim_r else None,
                },
            )
            await db.execute(stmt)

    await db.commit()
    log.info("Database commit complete")

    # ── ML Engine: collect samples + label completed games + train + predict
    # Runs in its OWN session so that any ML failure cannot contaminate
    # the main formula session (which already committed above).
    # _persist_results is only called when not dry_run, so always run ML here.
    try:
        from app.ml.trainer import MLTrainer
        from app.utils.ip_window import expected_ip as _expected_ip
        from app.models.base import AsyncSessionLocal
        ml_trainer = MLTrainer()

        # Build the sample dicts the ML trainer needs (merge features + outputs)
        ml_samples = []
        formula_outputs_by_pitcher: dict[str, dict] = {}
        for result in scored_pitchers:
            f: PitcherFeatureSet = result["features"]
            hr = result["hssi"]
            kr = result["kssi"]
            # fragility_ip_cap MUST be included here so the ML training
            # sample reflects the same IP that HUSI/KUSI actually used.
            # Without it, the ML learns the wrong hits<->IP mapping for
            # fragile pitchers and will over-project their stats.
            exp_ip = _expected_ip(
                f.avg_ip_per_start,
                f.mlb_service_years,
                fragility_ip_cap=getattr(f, "fi_ip_cap", None),
            )
            sample = {
                "pitcher_id": f.pitcher_id,
                "game_id": f.game_id,
                "owc_score": hr.get("owc_score"), "pcs_score": hr.get("pcs_score"),
                "ens_score": hr.get("ens_score"), "ops_score": hr.get("ops_score"),
                "uhs_score": hr.get("uhs_score"), "dsc_score": hr.get("dsc_score"),
                "ocr_score": kr.get("ocr_score"), "pmr_score": kr.get("pmr_score"),
                "per_score": kr.get("per_score"), "kop_score": kr.get("kop_score"),
                "uks_score": kr.get("uks_score"), "tlr_score": kr.get("tlr_score"),
                "season_hits_per_9": f.season_hits_per_9,
                "season_k_per_9": f.season_k_per_9,
                "expected_ip": exp_ip,
                "bullpen_fatigue_opp": f.bullpen_fatigue_opp,
                "bullpen_fatigue_own": f.bullpen_fatigue_own,
                "ens_park": f.ens_park,
                "ens_temp": f.ens_temp,
                "ens_air": f.ens_air,
                "hssi": hr["hssi"], "husi": hr["hssi"],
                "kssi": kr["kssi"], "kusi": kr["kssi"],
                "projected_hits": hr.get("projected_hits"),
                "projected_ks": kr.get("projected_ks"),
                # Hidden variables for ML residual drift analysis
                "catcher_strike_rate": f.catcher_strike_rate,
                "tfi_rest_hours": f.tfi_rest_hours,
                "tfi_tz_shift": f.tfi_tz_shift,
                "vaa_degrees": f.vaa_degrees,
                "extension_ft": f.extension_ft,
            }
            ml_samples.append(sample)
            formula_outputs_by_pitcher[f.pitcher_id] = {
                "hssi": hr["hssi"], "husi": hr["hssi"],
                "kssi": kr["kssi"], "kusi": kr["kssi"],
                "hssi_grade": hr["grade"], "husi_grade": hr["grade"],
                "kssi_grade": kr["grade"], "kusi_grade": kr["grade"],
                "projected_hits": hr.get("projected_hits"),
                "projected_ks": kr.get("projected_ks"),
            }

        # ── Step 1: Collect today's samples (isolated — failure won't block training)
        async with AsyncSessionLocal() as ml_session:
            try:
                await ml_trainer.collect_today(ml_session, target_date, ml_samples)
                await ml_session.commit()
            except Exception as e:
                log.warning("ML collect_today failed (non-fatal)", error=str(e))
                await ml_session.rollback()

        # ── Step 2: Label any completed games from prior days
        async with AsyncSessionLocal() as ml_session:
            try:
                await ml_trainer.label_completed_games(ml_session)
                await ml_session.commit()
            except Exception as e:
                log.warning("ML label_completed_games failed (non-fatal)", error=str(e))
                await ml_session.rollback()

        # ── Step 2b: Flag pitchers with recent underperformance trends
        flags_written = await _flag_underperforming_pitchers(target_date)
        log.info("Pitcher warning flags written", count=flags_written)

        # ── Step 3: Train + Predict (always runs regardless of steps 1/2)
        ml_result = {"trained": False, "ml_outputs": []}
        async with AsyncSessionLocal() as ml_session:
            try:
                ml_result = await ml_trainer.train_and_predict(
                    ml_session, target_date, ml_samples, formula_outputs_by_pitcher
                )
                await ml_session.commit()
                log.info("ML engine cycle complete",
                         trained=ml_result.get("trained"),
                         n_samples=ml_result.get("n_samples"),
                         mae_hits=ml_result.get("mae_hits"),
                         mae_ks=ml_result.get("mae_ks"),
                         ml_outputs=len(ml_result.get("ml_outputs", [])))

                # ── Entropy Filter: measure agreement between Engine 1 and ML Engine
                # Runs in its own session so a failure here cannot roll back ML outputs.
                if ml_result.get("ml_outputs"):
                    from sqlalchemy import text as _text
                    async with AsyncSessionLocal() as entropy_session:
                        try:
                            for o in ml_result["ml_outputs"]:
                                pid = o["pitcher_id"]
                                formula = formula_outputs_by_pitcher.get(pid, {})
                                e1_hits = formula.get("projected_hits")
                                ml_hits = o.get("ml_proj_hits")
                                e1_ks   = formula.get("projected_ks")
                                ml_ks   = o.get("ml_proj_ks")

                                hits_entropy = round(abs(e1_hits - ml_hits), 3) if e1_hits is not None and ml_hits is not None else None
                                ks_entropy   = round(abs(e1_ks   - ml_ks),   3) if e1_ks   is not None and ml_ks   is not None else None

                                if hits_entropy is not None and ks_entropy is not None:
                                    if hits_entropy >= 1.4 or ks_entropy >= 1.4:
                                        entropy_label = "HIGH_ENTROPY"
                                    elif hits_entropy >= 0.8 or ks_entropy >= 0.8:
                                        entropy_label = "DIVERGING"
                                    else:
                                        entropy_label = "ALIGNED"
                                else:
                                    entropy_label = None

                                await entropy_session.execute(
                                    _text("""
                                        UPDATE model_outputs_daily
                                        SET hits_entropy  = :he,
                                            entropy_label = :el
                                        WHERE pitcher_id  = :pid
                                          AND game_date   = :gd
                                          AND market_type = 'hits_allowed'
                                    """),
                                    {"he": hits_entropy,
                                     "el": entropy_label, "pid": pid, "gd": target_date},
                                )
                                await entropy_session.execute(
                                    _text("""
                                        UPDATE model_outputs_daily
                                        SET ks_entropy    = :ke,
                                            entropy_label = :el
                                        WHERE pitcher_id  = :pid
                                          AND game_date   = :gd
                                          AND market_type = 'strikeouts'
                                    """),
                                    {"ke": ks_entropy,
                                     "el": entropy_label, "pid": pid, "gd": target_date},
                                )
                            await entropy_session.commit()
                            log.info("Entropy filter written",
                                     pitchers=len(ml_result["ml_outputs"]))
                        except Exception as entropy_exc:
                            log.warning("Entropy filter write failed (non-fatal)",
                                        error=str(entropy_exc))
                            await entropy_session.rollback()

            except Exception as e:
                log.warning("ML train_and_predict failed (non-fatal)", error=str(e))
                await ml_session.rollback()

    except Exception as ml_exc:
        log.warning("ML engine cycle failed (non-fatal)", error=str(ml_exc))

    # ── Axiom Lineup Vault — store every batter's stats for every game we score
    # This gives Axiom ownership of historical lineup data and powers the
    # lineup fluidity analysis used by the simulation engine (TTO3 pinch-hitter model).
    if lineup_data and season:
        try:
            batters_written = 0
            for game_id, game_lineup in lineup_data.items():
                lineup_confirmed = game_lineup.get("lineup_confirmed", False)
                # Determine team_ids for this game from pitchers_data
                game_team_ids: dict[str, str] = {}
                for _pid, pd in pitchers_data.items():
                    if pd.get("game_id") == game_id:
                        side = pd.get("side", "")
                        tid  = pd.get("team_id", "")
                        if side and tid:
                            game_team_ids[side] = tid

                for side in ("home", "away"):
                    batters = game_lineup.get(side, [])
                    team_id = game_team_ids.get(side, "")
                    for b in batters:
                        batter_id = b.get("batter_id", "")
                        if not batter_id:
                            continue
                        # Normalize danger: inverse of K rate (low K = dangerous contact hitter)
                        # League avg K rate ~20%; 0% K rate → 100 danger, 40% → 0 danger
                        k = b.get("k_rate", 20.0)
                        slot_danger = round(max(0.0, min(100.0, (40.0 - k) / 40.0 * 100.0)), 2)
                        # SKU #39 — swing profile from bat-tracking leaderboard
                        sp_batter = (swing_profiles or {}).get(batter_id, {})
                        stmt = pg_insert(AxiomGameLineup).values(
                            game_id=game_id,
                            team_id=team_id,
                            game_date=target_date,
                            season=season,
                            side=side,
                            lineup_confirmed=lineup_confirmed,
                            batter_id=batter_id,
                            batter_name=b.get("name"),
                            batting_order=b.get("batting_order"),
                            k_rate=b.get("k_rate"),
                            k_per_pa=b.get("k_per_pa"),
                            bb_rate=b.get("bb_rate"),
                            avg=b.get("avg"),
                            obp=b.get("obp"),
                            slg=b.get("slg"),
                            at_bats=b.get("ab"),
                            lineup_slot_danger=slot_danger,
                            avg_attack_angle=sp_batter.get("attack_angle"),
                            swing_tilt=sp_batter.get("swing_tilt"),
                        ).on_conflict_do_update(
                            constraint="uq_game_team_batter",
                            set_={
                                "batting_order": b.get("batting_order"),
                                "k_rate": b.get("k_rate"),
                                "avg": b.get("avg"),
                                "obp": b.get("obp"),
                                "slg": b.get("slg"),
                                "lineup_slot_danger": slot_danger,
                                "avg_attack_angle": sp_batter.get("attack_angle"),
                                "swing_tilt": sp_batter.get("swing_tilt"),
                                "updated_at": func.now(),
                            },
                        )
                        await db.execute(stmt)
                        batters_written += 1
            await db.commit()
            log.info("Axiom lineup vault updated", batters=batters_written, games=len(lineup_data))
        except Exception as lineup_exc:
            log.warning("Axiom lineup vault write failed (non-fatal)", error=str(lineup_exc))


def _quality_flag(f: PitcherFeatureSet) -> str:
    missing = []
    if not f.lineup_confirmed:
        missing.append("lineup")
    if not f.umpire_confirmed:
        missing.append("umpire")
    if not f.bullpen_data_available:
        missing.append("bullpen")
    if not missing:
        return "complete"
    return "partial:" + "+".join(missing)


def _confidence_label(score: float) -> str:
    if score is None:
        return "WEAK"
    if score >= 76:
        return "HIGH"
    elif score >= 65:
        return "MEDIUM"
    elif score >= 55:
        return "LOW"
    else:
        return "WEAK"


# MLB team ID → abbreviation map (used to look up NWS weather by stadium)
_TEAM_ID_TO_ABBREV: dict[str, str] = {
    "109": "ARI", "144": "ATL", "110": "BAL", "111": "BOS",
    "112": "CHC", "145": "CWS", "113": "CIN", "114": "CLE",
    "115": "COL", "116": "DET", "117": "HOU", "118": "KC",
    "108": "LAA", "119": "LAD", "146": "MIA", "158": "MIL",
    "142": "MIN", "121": "NYM", "147": "NYY", "133": "OAK",
    "143": "PHI", "134": "PIT", "135": "SD",  "137": "SF",
    "136": "SEA", "138": "STL", "139": "TB",  "140": "TEX",
    "141": "TOR", "120": "WSH",
}


def _team_id_to_abbrev(team_id: str) -> str:
    return _TEAM_ID_TO_ABBREV.get(str(team_id), "")


# ─────────────────────────────────────────────────────────────────────────────
# Pitcher warning flags — post-labeling underperformance detection
# ─────────────────────────────────────────────────────────────────────────────

async def _flag_underperforming_pitchers(target_date: "date") -> int:
    """
    For each pitcher scored today, check their last 3 completed starts.
    If they missed their sim median in 2 or more of those starts, write
    a warning flag to pitcher_warning_flags.

    flag_type values:
      UNRELIABLE_KS    — actual_ks < sim_median_ks in 2+ of last 3
      UNRELIABLE_HITS  — actual_hits > sim_median_hits in 2+ of last 3
      UNRELIABLE_BOTH  — both conditions true simultaneously

    Runs in its own session — failure is non-fatal.
    Returns count of flags written.
    """
    from sqlalchemy import text as _text
    from app.models.base import AsyncSessionLocal

    flags_written = 0

    async with AsyncSessionLocal() as session:
        try:
            # ── 1. Get distinct pitchers scored today + their names
            today_rows = await session.execute(
                _text("""
                    SELECT DISTINCT mod.pitcher_id, pp.pitcher_name
                    FROM model_outputs_daily mod
                    JOIN probable_pitchers pp
                      ON pp.pitcher_id = mod.pitcher_id
                    WHERE mod.game_date = :today
                """),
                {"today": target_date},
            )
            pitchers = today_rows.fetchall()

            for row in pitchers:
                pitcher_id   = row.pitcher_id
                pitcher_name = row.pitcher_name

                # ── 2. Fetch last 3 completed starts with both actuals and sim medians
                history = await session.execute(
                    _text("""
                        SELECT
                            mts.game_date,
                            mts.actual_ks,
                            mts.actual_hits,
                            mod.sim_median_ks,
                            mod.sim_median_hits
                        FROM ml_training_samples mts
                        JOIN model_outputs_daily mod
                          ON mod.pitcher_id = mts.pitcher_id
                         AND mod.game_date  = mts.game_date
                        WHERE mts.pitcher_id  = :pid
                          AND mts.is_complete = true
                          AND mts.game_date   < :today
                          AND mts.actual_ks   IS NOT NULL
                          AND mts.actual_hits IS NOT NULL
                          AND mod.sim_median_ks   IS NOT NULL
                          AND mod.sim_median_hits IS NOT NULL
                        ORDER BY mts.game_date DESC
                        LIMIT 3
                    """),
                    {"pid": pitcher_id, "today": target_date},
                )
                starts = history.fetchall()

                if len(starts) < 2:
                    continue

                # ── 3. Score each condition
                ks_misses   = sum(1 for s in starts if s.actual_ks   < s.sim_median_ks)
                hits_misses = sum(1 for s in starts if s.actual_hits > s.sim_median_hits)

                unreliable_ks   = ks_misses   >= 2
                unreliable_hits = hits_misses >= 2

                if not unreliable_ks and not unreliable_hits:
                    continue

                if unreliable_ks and unreliable_hits:
                    flag_type = "UNRELIABLE_BOTH"
                elif unreliable_ks:
                    flag_type = "UNRELIABLE_KS"
                else:
                    flag_type = "UNRELIABLE_HITS"

                # ── 4. Skip if this pitcher already has a flag today
                existing = await session.execute(
                    _text("""
                        SELECT 1 FROM pitcher_warning_flags
                        WHERE pitcher_id = :pid
                          AND game_date  = :today
                        LIMIT 1
                    """),
                    {"pid": pitcher_id, "today": target_date},
                )
                if existing.fetchone():
                    continue

                # ── 5. Write the flag
                last = starts[0]
                await session.execute(
                    _text("""
                        INSERT INTO pitcher_warning_flags
                            (pitcher_id, pitcher_name, game_date,
                             actual_ks, floor_ks,
                             actual_hits, floor_hits,
                             flag_type,
                             ks_misses, hits_misses)
                        VALUES
                            (:pid, :name, :today,
                             :actual_ks, :floor_ks,
                             :actual_hits, :floor_hits,
                             :flag_type,
                             :ks_misses, :hits_misses)
                    """),
                    {
                        "pid":         pitcher_id,
                        "name":        pitcher_name,
                        "today":       target_date,
                        "actual_ks":   last.actual_ks,
                        "floor_ks":    last.sim_median_ks,
                        "actual_hits": last.actual_hits,
                        "floor_hits":  last.sim_median_hits,
                        "flag_type":   flag_type,
                        "ks_misses":   ks_misses,
                        "hits_misses": hits_misses,
                    },
                )
                flags_written += 1
                log.info(
                    "Pitcher warning flag written",
                    pitcher=pitcher_name,
                    flag=flag_type,
                    ks_misses=ks_misses,
                    hits_misses=hits_misses,
                )

            await session.commit()

        except Exception as exc:
            log.warning("flag_underperforming_pitchers failed (non-fatal)", error=str(exc))
            await session.rollback()

    return flags_written
