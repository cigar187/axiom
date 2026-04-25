"""
MLB Stats API adapter — real working implementation.
Base URL: https://statsapi.mlb.com/api/v1

Fetches:
  - Today's schedule and games
  - Probable starting pitchers for each game
  - Season stats for each pitcher (ERA, K/9, hits/9, GB%, etc.)
  - Bullpen usage logs (last 3 games)
  - Lineup data (confirmed or not)
  - Umpire assignments (from the schedule endpoint)

All HTTP calls use httpx with automatic retries via tenacity.
"""
import asyncio
import statistics
from datetime import date
from typing import Any, Optional

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from app.services.base import BaseProvider
from app.utils.normalization import clamp
from app.utils.logging import get_logger

log = get_logger("mlb_stats")

MLB_BASE = "https://statsapi.mlb.com/api/v1"

# Stat group IDs used in the stats endpoint
PITCHING_STATS = "pitching"
SEASON_STAT_TYPE = "season"


class MLBStatsAdapter(BaseProvider):
    """
    Fetches everything available from the free MLB Stats API.
    Returns a structured dict keyed by pitcher_id with all collected data.
    """

    @property
    def name(self) -> str:
        return "MLB Stats API"

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=8))
    async def _get(self, client: httpx.AsyncClient, path: str, params: dict = None) -> dict:
        url = f"{MLB_BASE}{path}"
        log.debug("MLB API request", url=url, params=params)
        resp = await client.get(url, params=params or {}, timeout=20.0)
        resp.raise_for_status()
        return resp.json()

    async def fetch(self, target_date: date) -> dict:
        """
        Main entry point. Returns:
        {
          "games": [...],        # list of game dicts
          "pitchers": {
            "pitcher_id": {
              "name": ...,
              "team": ...,
              "opponent": ...,
              "game_id": ...,
              "handedness": ...,
              "confirmed": bool,
              "season_hits_per_9": float,
              "season_k_per_9": float,
              "season_era": float,
              "season_go_ao": float,       # raw GO/AO ratio — preserved for z-score normalization
              "season_gb_pct": float,      # starts as GO/AO ratio, overridden by Statcast real GB%
              "season_bb_per_9": float,
              "season_k_pct": float,
              "season_swstr_pct": float,   # swinging strike rate (from Statcast)
              "season_hard_hit_pct": float, # hard-hit rate (from Statcast)
              "avg_ip_per_start": float,
              "bullpen_logs": [...],
            }
          },
          "umpires": { "game_id": {"id": ..., "name": ...} }
        }
        """
        date_str = target_date.strftime("%Y-%m-%d")
        log.info("MLB Stats API fetch starting", date=date_str)

        async with httpx.AsyncClient() as client:
            schedule = await self._fetch_schedule(client, date_str)
            games = self._parse_games(schedule)
            umpires = self._parse_umpires(schedule)

            pitchers: dict[str, dict] = {}

            for game in games:
                game_id = game["game_id"]
                home_team = game["home_team"]
                away_team = game["away_team"]

                # Fetch probable pitchers for this game
                await self._enrich_probable_pitchers(
                    client, game_id, home_team, away_team, pitchers
                )

            # Fetch season stats for all pitchers in parallel
            if pitchers:
                await self._bulk_fetch_season_stats(client, pitchers, date_str)

            # Fetch last-3-game bullpen logs per team
            team_ids = {p["team_id"] for p in pitchers.values() if p.get("team_id")}
            bullpen_data = await self._fetch_bullpen_logs(client, team_ids, date_str)

            for pid, p_data in pitchers.items():
                t_id = p_data.get("team_id", "")
                p_data["bullpen_logs"] = bullpen_data.get(t_id, [])

        log.info("MLB Stats API fetch complete",
                 date=date_str, games=len(games), pitchers=len(pitchers))
        return {
            "games": games,
            "pitchers": pitchers,
            "umpires": umpires,
        }

    # ─────────────────────────────────────────────────────────
    # Schedule
    # ─────────────────────────────────────────────────────────

    async def _fetch_schedule(self, client: httpx.AsyncClient, date_str: str) -> dict:
        return await self._get(client, "/schedule", params={
            "sportId": 1,
            "date": date_str,
            "hydrate": "probablePitcher,linescore,officials,weather,team",
        })

    def _parse_games(self, schedule: dict) -> list[dict]:
        games = []
        for date_block in schedule.get("dates", []):
            for g in date_block.get("games", []):
                gid = str(g.get("gamePk", ""))
                teams = g.get("teams", {})
                weather = g.get("weather", {})
                venue = g.get("venue", {})

                games.append({
                    "game_id": gid,
                    "home_team": teams.get("home", {}).get("team", {}).get("name", ""),
                    "home_team_id": str(teams.get("home", {}).get("team", {}).get("id", "")),
                    "away_team": teams.get("away", {}).get("team", {}).get("name", ""),
                    "away_team_id": str(teams.get("away", {}).get("team", {}).get("id", "")),
                    "park": venue.get("name", ""),
                    "venue_id": str(venue.get("id", "")),
                    "status": g.get("status", {}).get("detailedState", "scheduled"),
                    "temperature_f": self._safe_float(weather.get("temp")),
                    "wind_speed_mph": self._safe_float(
                        (weather.get("wind", "") or "").split(" mph")[0].split()[-1]
                        if "mph" in (weather.get("wind", "") or "") else None
                    ),
                    "wind_direction": self._parse_wind_direction(weather.get("wind", "")),
                    "weather_condition": weather.get("condition", ""),
                    # Dome detection: check venue name heuristics
                    "is_dome": self._is_dome(venue.get("name", "")),
                })
        return games

    def _parse_umpires(self, schedule: dict) -> dict[str, dict]:
        umpires = {}
        for date_block in schedule.get("dates", []):
            for g in date_block.get("games", []):
                gid = str(g.get("gamePk", ""))
                officials = g.get("officials", [])
                for official in officials:
                    if official.get("officialType") == "Home Plate":
                        person = official.get("official", {})
                        umpires[gid] = {
                            "id": str(person.get("id", "")),
                            "name": person.get("fullName", ""),
                        }
                        break
        return umpires

    # ─────────────────────────────────────────────────────────
    # Probable pitchers
    # ─────────────────────────────────────────────────────────

    async def _enrich_probable_pitchers(
        self,
        client: httpx.AsyncClient,
        game_id: str,
        home_team: str,
        away_team: str,
        pitchers: dict,
    ) -> None:
        found_any = False
        try:
            data = await self._get(client, f"/game/{game_id}/boxscore")
            teams_data = data.get("teams", {})

            for side in ("home", "away"):
                side_data = teams_data.get(side, {})
                team_info = side_data.get("team", {})
                team_name = home_team if side == "home" else away_team
                team_id = str(team_info.get("id", ""))
                opponent = away_team if side == "home" else home_team

                # Starter is the first pitcher listed in the boxscore
                pitchers_list = side_data.get("pitchers", [])
                if not pitchers_list:
                    continue  # game not started yet — will fall back below

                pid = str(pitchers_list[0])
                player_data = side_data.get("players", {}).get(f"ID{pid}", {})
                person = player_data.get("person", {})

                pitchers[pid] = {
                    "pitcher_id": pid,
                    "pitcher_name": person.get("fullName", f"Player {pid}"),
                    "team": team_name,
                    "team_id": team_id,
                    "side": side,           # "home" or "away" — critical for bullpen/catcher lookup
                    "opponent": opponent,
                    "game_id": game_id,
                    "handedness": person.get("pitchHand", {}).get("code"),
                    "confirmed": True,
                    "season_hits_per_9": None,
                    "season_k_per_9": None,
                    "season_era": None,
                    "season_go_ao": None,           # GO/AO ratio — never overwritten, used for z-score
                    "season_gb_pct": None,          # starts as GO/AO ratio, Statcast overrides with real GB%
                    "season_bb_per_9": None,
                    "season_k_pct": None,
                    "season_swstr_pct": None,
                    "season_hard_hit_pct": None,
                    "avg_ip_per_start": None,       # populated by _bulk_fetch_season_stats
                    "mlb_service_years": None,      # tier fallback when avg_ip unavailable
                    "bullpen_logs": [],
                }
                found_any = True

        except Exception as exc:
            log.warning("Boxscore fetch failed for game", game_id=game_id, error=str(exc))

        # Pre-game: boxscore has no pitchers yet — use schedule probable pitchers instead.
        # This is the normal case when the pipeline runs before first pitch.
        if not found_any:
            log.info("No pitchers in boxscore — trying schedule probables", game_id=game_id)
            await self._enrich_from_schedule_probables(client, game_id, home_team, away_team, pitchers)

    async def _enrich_from_schedule_probables(
        self,
        client: httpx.AsyncClient,
        game_id: str,
        home_team: str,
        away_team: str,
        pitchers: dict,
    ) -> None:
        """Secondary fallback using the schedule probable pitcher fields."""
        try:
            data = await self._get(client, "/schedule", params={
                "sportId": 1,
                "gamePk": game_id,
                "hydrate": "probablePitcher,team",
            })
            for date_block in data.get("dates", []):
                for g in date_block.get("games", []):
                    teams = g.get("teams", {})
                    for side in ("home", "away"):
                        side_data = teams.get(side, {})
                        probable = side_data.get("probablePitcher")
                        if not probable:
                            continue
                        pid = str(probable.get("id", ""))
                        team_name = home_team if side == "home" else away_team
                        team_id = str(side_data.get("team", {}).get("id", ""))
                        opponent = away_team if side == "home" else home_team
                        pitchers[pid] = {
                            "pitcher_id": pid,
                            "pitcher_name": probable.get("fullName", f"Player {pid}"),
                            "team": team_name,
                            "team_id": team_id,
                            "side": side,       # "home" or "away" — critical for bullpen/catcher lookup
                            "opponent": opponent,
                            "game_id": game_id,
                            "handedness": None,
                            "confirmed": False,
                            "season_hits_per_9": None,
                            "season_k_per_9": None,
                            "season_era": None,
                            "season_go_ao": None,
                            "season_gb_pct": None,
                            "season_bb_per_9": None,
                            "season_k_pct": None,
                            "season_swstr_pct": None,
                            "season_hard_hit_pct": None,
                            "avg_ip_per_start": None,
                            "mlb_service_years": None,
                            "bullpen_logs": [],
                        }
        except Exception as exc:
            log.error("Schedule probable fallback failed", game_id=game_id, error=str(exc))

    # ─────────────────────────────────────────────────────────
    # Season stats
    # ─────────────────────────────────────────────────────────

    async def _bulk_fetch_season_stats(
        self,
        client: httpx.AsyncClient,
        pitchers: dict,
        date_str: str,
    ) -> None:
        """Fetch season stats for all pitchers in parallel (max 10 concurrent)."""
        sem = asyncio.Semaphore(10)

        async def fetch_one(pid: str) -> None:
            async with sem:
                try:
                    data = await self._get(client, f"/people/{pid}/stats", params={
                        "stats": SEASON_STAT_TYPE,
                        "group": PITCHING_STATS,
                        "season": date_str[:4],
                        "sportId": 1,
                    })
                    stats_list = data.get("stats", [])
                    if stats_list:
                        splits = stats_list[0].get("splits", [])
                        if splits:
                            stat = splits[0].get("stat", {})
                            pitchers[pid]["season_era"] = self._safe_float(stat.get("era"))
                            pitchers[pid]["season_k_per_9"] = self._safe_float(stat.get("strikeoutsPer9Inn"))
                            pitchers[pid]["season_hits_per_9"] = self._safe_float(stat.get("hitsPer9Inn"))
                            pitchers[pid]["season_bb_per_9"] = self._safe_float(stat.get("walksPer9Inn"))
                            pitchers[pid]["season_k_pct"] = self._safe_float(stat.get("strikeoutPercentage"))
                            go_ao = self._safe_float(stat.get("groundOutsToAirouts"))
                            # season_go_ao: always the raw GO/AO ratio (0.5-2.5) — NEVER overwritten
                            # Used for pcs_gb z-score normalization (stable scale, all pitchers have it)
                            pitchers[pid]["season_go_ao"] = go_ao
                            # season_gb_pct: starts as GO/AO ratio, gets OVERRIDDEN by Statcast real GB%
                            # Used for HUSI GB suppressor (auto-detects % vs ratio scale)
                            pitchers[pid]["season_gb_pct"] = go_ao

                            # ── IP window: compute avg IP per start this season
                            raw_ip = self._safe_float(stat.get("inningsPitched"))  # total IP as float
                            gs = self._safe_float(stat.get("gamesStarted"))
                            if raw_ip is not None and gs and gs > 0:
                                pitchers[pid]["avg_ip_per_start"] = round(raw_ip / gs, 2)
                            else:
                                pitchers[pid]["avg_ip_per_start"] = None

                            log.debug("Season stats fetched", pitcher_id=pid,
                                      k9=pitchers[pid]["season_k_per_9"],
                                      h9=pitchers[pid]["season_hits_per_9"],
                                      avg_ip=pitchers[pid].get("avg_ip_per_start"))
                except Exception as exc:
                    log.warning("Season stats fetch failed", pitcher_id=pid, error=str(exc))

        await asyncio.gather(*[fetch_one(pid) for pid in pitchers])

    # ─────────────────────────────────────────────────────────
    # Confirmed lineup + individual batter K rates
    # Used to score TLR (Top-Lineup Resistance) features.
    # ─────────────────────────────────────────────────────────

    async def fetch_lineup_batter_stats(
        self,
        game_id: str,
        season: str,
    ) -> dict:
        """
        Fetch the confirmed batting lineup for a game and each batter's
        season strikeout rate, walk rate, and contact profile.

        Returns:
        {
          "home": [ { "batter_id", "name", "batting_order", "k_rate", "bb_rate", "avg" }, ... ],
          "away": [ ... ],
          "lineup_confirmed": bool
        }
        """
        async with httpx.AsyncClient() as client:
            try:
                data = await self._get(client, f"/game/{game_id}/boxscore")
            except Exception as exc:
                log.warning("Lineup fetch failed", game_id=game_id, error=str(exc))
                return {"home": [], "away": [], "lineup_confirmed": False}

            result = {"home": [], "away": [], "lineup_confirmed": False}
            any_lineup = False

            for side in ("home", "away"):
                side_data = data.get("teams", {}).get(side, {})
                batters = side_data.get("batters", [])
                players = side_data.get("players", {})

                if not batters:
                    continue
                any_lineup = True

                # Fetch season stats for all batters in parallel
                sem = asyncio.Semaphore(10)
                batter_records = []

                async def fetch_batter(bid: int, order_index: int) -> None:
                    async with sem:
                        pdata = players.get(f"ID{bid}", {})
                        name = pdata.get("person", {}).get("fullName", f"Player {bid}")
                        batting_order = order_index + 1
                        try:
                            stats_data = await self._get(
                                client,
                                f"/people/{bid}/stats",
                                params={
                                    "stats": SEASON_STAT_TYPE,
                                    "group": "hitting",
                                    "season": season,
                                    "sportId": 1,
                                },
                            )
                            stat = {}
                            for block in stats_data.get("stats", []):
                                splits = block.get("splits", [])
                                if splits:
                                    stat = splits[0].get("stat", {})
                                    break

                            ab = int(stat.get("atBats") or 1) or 1
                            pa = ab + int(stat.get("baseOnBalls") or 0)
                            k = int(stat.get("strikeOuts") or 0)
                            bb = int(stat.get("baseOnBalls") or 0)

                            bat_side = (
                                pdata.get("person", {})
                                     .get("batSide", {})
                                     .get("code")
                            )
                            batter_records.append({
                                "batter_id": str(bid),
                                "name": name,
                                "batting_order": batting_order,
                                "bat_side": bat_side,   # "R", "L", "S" (switch) or None
                                "k_rate": round(k / ab * 100, 2) if ab else 20.0,
                                "k_per_pa": round(k / pa * 100, 2) if pa else 20.0,
                                "bb_rate": round(bb / pa * 100, 2) if pa else 8.0,
                                "avg": self._safe_float(stat.get("avg")) or 0.250,
                                "obp": self._safe_float(stat.get("obp")) or 0.320,
                                "slg": self._safe_float(stat.get("slg")) or 0.400,
                                "ab": ab,
                            })
                        except Exception as exc2:
                            log.warning("Batter stat fetch failed",
                                        batter_id=bid, name=name, error=str(exc2))
                            batter_records.append({
                                "batter_id": str(bid), "name": name,
                                "batting_order": batting_order,
                                "bat_side": None,
                                "k_rate": 20.0, "k_per_pa": 20.0,
                                "bb_rate": 8.0, "avg": 0.250,
                                "obp": 0.320, "slg": 0.400, "ab": 0,
                            })

                await asyncio.gather(*[fetch_batter(bid, i) for i, bid in enumerate(batters[:9])])
                batter_records.sort(key=lambda x: x["batting_order"])
                result[side] = batter_records

            result["lineup_confirmed"] = any_lineup
            return result

    # ─────────────────────────────────────────────────────────
    # Opponent team hitting stats
    # Used to score OCR (Opponent Contact Rate) features correctly.
    # The K rate comes from the OPPOSING LINEUP, not the pitcher.
    # ─────────────────────────────────────────────────────────

    async def fetch_all_team_hitting_stats(self, season: str) -> dict[str, dict]:
        """
        Fetch season hitting stats for all 30 MLB teams.
        Returns { team_id: { k_rate, contact_rate, avg, obp, ... } }

        This is called once per pipeline run and shared across all pitchers.
        Each pitcher's opponent_team_id is used to look up the opposing lineup profile.
        """
        log.info("Fetching all team hitting stats", season=season)

        async with httpx.AsyncClient() as client:
            # Get all team IDs
            teams_data = await self._get(client, "/teams", params={
                "sportId": 1,
                "season": season,
            })
            team_ids = [
                str(t["id"]) for t in teams_data.get("teams", [])
                if t.get("sport", {}).get("id") == 1
            ]

            team_stats: dict[str, dict] = {}
            sem = asyncio.Semaphore(15)

            async def fetch_one_team(tid: str) -> None:
                async with sem:
                    try:
                        data = await self._get(client, f"/teams/{tid}/stats", params={
                            "stats": SEASON_STAT_TYPE,
                            "group": "hitting",
                            "season": season,
                            "sportId": 1,
                        })
                        for block in data.get("stats", []):
                            for split in block.get("splits", []):
                                s = split.get("stat", {})
                                ab = int(s.get("atBats") or 1) or 1
                                k = int(s.get("strikeOuts") or 0)
                                hits = int(s.get("hits") or 0)
                                bb = int(s.get("baseOnBalls") or 0)
                                sb = int(s.get("stolenBases") or 0)
                                gp = int(s.get("gamesPlayed") or 1) or 1
                                pa = ab + bb
                                team_stats[tid] = {
                                    "team_id": tid,
                                    "k_rate": round(k / ab * 100, 2) if ab else 20.0,
                                    "k_per_pa": round(k / pa * 100, 2) if pa else 20.0,
                                    "contact_rate": round((ab - k) / ab * 100, 2) if ab else 80.0,
                                    "avg": self._safe_float(s.get("avg")) or 0.250,
                                    "obp": self._safe_float(s.get("obp")) or 0.320,
                                    "slg": self._safe_float(s.get("slg")) or 0.400,
                                    "bb_rate": round(bb / pa * 100, 2) if pa else 8.0,
                                    "hits_per_pa": round(hits / pa, 4) if pa else 0.25,
                                    "sb_per_game": round(sb / gp, 3),  # stolen base rate per game
                                    "at_bats": ab,
                                }
                    except Exception as exc:
                        log.warning("Team hitting stats failed", team_id=tid, error=str(exc))

            await asyncio.gather(*[fetch_one_team(tid) for tid in team_ids])

        log.info("Team hitting stats fetched", teams=len(team_stats))
        return team_stats

    # ─────────────────────────────────────────────────────────
    # Manager hook tendency
    # Calculated from the team's own starter game logs.
    # No external database needed — all from MLB Stats API.
    # ─────────────────────────────────────────────────────────

    async def fetch_manager_hook_tendency(
        self,
        team_id: str,
        season: str,
    ) -> dict:
        """
        Calculate how quickly a manager pulls starters by analyzing
        the team's pitching game logs for the current season.

        Returns:
        {
          "team_id": str,
          "avg_starter_ip": float,       # average innings started pitchers go
          "avg_starter_pc": float,       # average pitch count at removal
          "pct_games_7plus": float,      # % of games starters went 7+ innings
          "hook_score": float,           # normalized 0-100 (100 = leaves starter in long)
          "games_sampled": int,
        }
        """
        async with httpx.AsyncClient() as client:
            try:
                data = await self._get(client, f"/teams/{team_id}/stats", params={
                    "stats": "gameLog",
                    "group": PITCHING_STATS,
                    "season": season,
                    "sportId": 1,
                })
            except Exception as exc:
                log.warning("Manager hook fetch failed", team_id=team_id, error=str(exc))
                return {"team_id": team_id, "hook_score": 50.0, "games_sampled": 0}

        starter_ips = []
        starter_pcs = []
        games_7plus = 0

        for block in data.get("stats", []):
            for split in block.get("splits", []):
                stat = split.get("stat", {})
                # Only count games where pitcher started (gs >= 1)
                gs = int(stat.get("gamesStarted") or 0)
                if gs == 0:
                    continue
                ip_str = str(stat.get("inningsPitched") or "0")
                try:
                    # IP format is X.Y where .1 = 1 out, .2 = 2 outs
                    parts = ip_str.split(".")
                    full_innings = int(parts[0])
                    partial = int(parts[1]) / 3 if len(parts) > 1 else 0
                    ip = full_innings + partial
                except (ValueError, IndexError):
                    continue

                pc = int(stat.get("numberOfPitches") or 0)
                if ip > 0:
                    starter_ips.append(ip)
                    if pc > 0:
                        starter_pcs.append(pc)
                    if ip >= 7.0:
                        games_7plus += 1

        if not starter_ips:
            return {"team_id": team_id, "hook_score": 50.0, "games_sampled": 0}

        avg_ip = statistics.mean(starter_ips)
        avg_pc = statistics.mean(starter_pcs) if starter_pcs else 90.0
        pct_7plus = games_7plus / len(starter_ips)

        # Hook score: higher = leaves starter in longer = more K opportunities
        # Scale: avg IP 5.0 = score 40, avg IP 6.0 = score 55, avg IP 7.0 = score 80
        ip_score = clamp((avg_ip - 4.0) / 3.5 * 100)
        pc_score = clamp((avg_pc - 75.0) / 40.0 * 100)
        pct7_score = clamp(pct_7plus * 150)  # 67%+ goes to 100
        hook_score = round(0.5 * ip_score + 0.3 * pct7_score + 0.2 * pc_score, 2)

        log.info("Manager hook calculated",
                 team_id=team_id,
                 avg_ip=round(avg_ip, 2),
                 avg_pc=round(avg_pc, 1),
                 pct_7plus=round(pct_7plus, 3),
                 hook_score=hook_score,
                 games_sampled=len(starter_ips))

        return {
            "team_id": team_id,
            "avg_starter_ip": round(avg_ip, 2),
            "avg_starter_pc": round(avg_pc, 1),
            "pct_games_7plus": round(pct_7plus, 3),
            "hook_score": hook_score,
            "games_sampled": len(starter_ips),
        }

    # ─────────────────────────────────────────────────────────
    # Bullpen logs
    # ─────────────────────────────────────────────────────────

    async def _fetch_bullpen_logs(
        self,
        client: httpx.AsyncClient,
        team_ids: set[str],
        date_str: str,
    ) -> dict[str, list]:
        """
        For each team, fetch the last 3 game logs to calculate bullpen usage.
        Returns { team_id: [game_log, ...] }
        """
        bullpen_data: dict[str, list] = {}
        season = date_str[:4]

        async def fetch_team(tid: str) -> None:
            try:
                data = await self._get(client, f"/teams/{tid}/stats", params={
                    "stats": "gameLog",
                    "group": PITCHING_STATS,
                    "season": season,
                    "sportId": 1,
                })
                splits = []
                for stat_block in data.get("stats", []):
                    splits.extend(stat_block.get("splits", []))
                # Most recent 3 games
                bullpen_data[tid] = splits[-3:] if len(splits) >= 3 else splits
            except Exception as exc:
                log.warning("Bullpen log fetch failed", team_id=tid, error=str(exc))
                bullpen_data[tid] = []

        await asyncio.gather(*[fetch_team(tid) for tid in team_ids])
        return bullpen_data

    # ─────────────────────────────────────────────────────────
    # Bullpen Fatigue Data
    # ─────────────────────────────────────────────────────────

    async def fetch_bullpen_fatigue_data(
        self,
        team_id: str,
        game_date: date,
    ) -> dict:
        """
        Fetch relief pitcher pitch counts for the 48-hour window before game_date.

        Returns:
        {
            "yesterday": {pitcher_id: pitch_count, ...},
            "two_days_ago": {pitcher_id: pitch_count, ...},
            "closer_id": str | None,   # ID of identified primary closer
        }

        Method:
          1. Fetch schedule for yesterday and day before to get game PKs.
          2. For each game, pull the boxscore.
          3. Filter to relief pitchers only (skip the starter = first pitcher listed).
          4. Sum pitch counts per pitcher per day.
          5. Identify the closer as the RP with the most games-finished appearances.
        """
        from datetime import timedelta
        yesterday = game_date - timedelta(days=1)
        two_days_ago = game_date - timedelta(days=2)

        async with httpx.AsyncClient() as client:
            yesterday_data = await self._fetch_rp_pitch_counts(
                client, team_id, yesterday
            )
            two_days_ago_data = await self._fetch_rp_pitch_counts(
                client, team_id, two_days_ago
            )

        # Closer = RP who appeared in the most games finished across both days
        # (simplest proxy without needing role data from the API)
        closer_id = self._identify_closer(yesterday_data, two_days_ago_data)

        log.info("Bullpen fatigue data fetched",
                 team_id=team_id,
                 date=str(game_date),
                 yesterday_arms=len(yesterday_data),
                 two_days_ago_arms=len(two_days_ago_data),
                 closer_id=closer_id)

        return {
            "yesterday": yesterday_data,
            "two_days_ago": two_days_ago_data,
            "closer_id": closer_id,
        }

    async def _fetch_rp_pitch_counts(
        self,
        client: httpx.AsyncClient,
        team_id: str,
        game_date: date,
    ) -> dict[str, int]:
        """
        Returns {pitcher_id: pitch_count} for all relief pitchers
        used by team_id on game_date.
        """
        date_str = game_date.strftime("%Y-%m-%d")
        try:
            schedule = await self._get(
                client,
                "/schedule",
                params={"sportId": 1, "teamId": team_id, "date": date_str, "gameType": "R"},
            )
        except Exception as exc:
            log.warning("Bullpen: schedule fetch failed", team_id=team_id, date=date_str, error=str(exc))
            return {}

        game_pk = None
        for date_block in schedule.get("dates", []):
            for g in date_block.get("games", []):
                status = g.get("status", {}).get("detailedState", "")
                if "Final" in status or "Game Over" in status or "Completed" in status:
                    game_pk = str(g.get("gamePk", ""))
                    break

        if not game_pk:
            return {}

        try:
            boxscore = await self._get(client, f"/game/{game_pk}/boxscore")
        except Exception as exc:
            log.warning("Bullpen: boxscore fetch failed", game_pk=game_pk, error=str(exc))
            return {}

        # Determine which side our team is on
        teams_data = boxscore.get("teams", {})
        side = None
        for s in ("home", "away"):
            team_info = teams_data.get(s, {}).get("team", {})
            if str(team_info.get("id", "")) == str(team_id):
                side = s
                break

        if not side:
            return {}

        side_data = teams_data.get(side, {})
        pitcher_ids = side_data.get("pitchers", [])
        players = side_data.get("players", {})

        if not pitcher_ids:
            return {}

        # Skip the starter (first pitcher in the list)
        relief_pitchers = pitcher_ids[1:]
        rp_pitches: dict[str, int] = {}

        for pid in relief_pitchers:
            player_key = f"ID{pid}"
            player = players.get(player_key, {})
            pitches = player.get("stats", {}).get("pitching", {}).get("pitchesThrown")
            if pitches and int(pitches) > 0:
                rp_pitches[str(pid)] = int(pitches)

        return rp_pitches

    @staticmethod
    def _identify_closer(
        yesterday: dict[str, int],
        two_days_ago: dict[str, int],
    ) -> Optional[str]:
        """
        Identify the primary closer as the RP who threw the most pitches
        across both days combined. This is a simple but effective proxy
        for 'highest leverage arm' without needing role/appearance data.
        """
        totals: dict[str, int] = {}
        for pid, count in yesterday.items():
            totals[pid] = totals.get(pid, 0) + count
        for pid, count in two_days_ago.items():
            totals[pid] = totals.get(pid, 0) + count
        if not totals:
            return None
        return max(totals, key=lambda p: totals[p])

    # ─────────────────────────────────────────────────────────
    # Pitcher recent form (for PFF — Pitcher Form Factor)
    # ─────────────────────────────────────────────────────────

    async def fetch_pitcher_recent_form(
        self,
        pitcher_id: str,
        season: str,
        n_starts: int = 3,
    ) -> list[dict]:
        """
        Fetch the pitcher's last N starts from their game log.

        Returns a list of start dicts (most recent FIRST), each with:
          {
            "era_this_start":  float   (earned_runs / ip × 9)
            "h9_this_start":   float   (hits / ip × 9)
            "k9_this_start":   float   (ks / ip × 9)
            "ip":              float
            "hits":            int
            "earned_runs":     int
            "ks":              int
            "game_date":       str     (YYYY-MM-DD)
            "season_era":      float   (pitcher's full season ERA for baseline)
            "season_h9":       float
            "season_k9":       float
          }

        Only game-started appearances (gs >= 1 and ip >= 2.0) are returned.
        Empty list if no starts found.
        """
        async with httpx.AsyncClient() as client:
            try:
                # Fetch the game log (per-appearance stats for this season)
                data = await self._get(client, f"/people/{pitcher_id}/stats", params={
                    "stats": "gameLog",
                    "group": PITCHING_STATS,
                    "season": season,
                    "sportId": 1,
                })

                # Also grab season totals for the baseline comparison
                season_data = await self._get(client, f"/people/{pitcher_id}/stats", params={
                    "stats": SEASON_STAT_TYPE,
                    "group": PITCHING_STATS,
                    "season": season,
                    "sportId": 1,
                })
            except Exception as exc:
                log.warning("Pitcher form fetch failed", pitcher_id=pitcher_id, error=str(exc))
                return []

        # Parse season totals for baseline
        season_era = season_h9 = season_k9 = None
        for block in season_data.get("stats", []):
            for split in block.get("splits", []):
                stat = split.get("stat", {})
                season_era = self._safe_float(stat.get("era"))
                season_h9 = self._safe_float(stat.get("hitsPer9Inn"))
                season_k9 = self._safe_float(stat.get("strikeoutsPer9Inn"))
                break

        # Parse game log — collect starts only
        starts = []
        for block in data.get("stats", []):
            for split in block.get("splits", []):
                stat = split.get("stat", {})

                # Only count games where pitcher started
                gs = int(stat.get("gamesStarted") or 0)
                if gs == 0:
                    continue

                # Parse IP (MLB format: 5.2 = 5 innings + 2 outs = 5.67 IP)
                ip_str = str(stat.get("inningsPitched") or "0")
                try:
                    parts = ip_str.split(".")
                    ip = int(parts[0]) + (int(parts[1]) / 3.0 if len(parts) > 1 else 0.0)
                except (ValueError, IndexError):
                    ip = 0.0

                # Skip abbreviated outings (injury exit, etc.)
                if ip < 2.0:
                    continue

                hits = int(stat.get("hits") or 0)
                earned = int(stat.get("earnedRuns") or 0)
                ks = int(stat.get("strikeOuts") or 0)

                # Per-9 rates for this start
                era_this = round((earned / ip) * 9.0, 2) if ip > 0 else 9.0
                h9_this = round((hits / ip) * 9.0, 2) if ip > 0 else 9.0
                k9_this = round((ks / ip) * 9.0, 2) if ip > 0 else 0.0

                starts.append({
                    "era_this_start": era_this,
                    "h9_this_start": h9_this,
                    "k9_this_start": k9_this,
                    "ip": round(ip, 2),
                    "hits": hits,
                    "earned_runs": earned,
                    "ks": ks,
                    "game_date": split.get("date", ""),
                    # Season baseline attached to each start for PFF comparison
                    "season_era": season_era,
                    "season_h9": season_h9,
                    "season_k9": season_k9,
                })

        # Return most recent N starts first (game log is oldest-first by default)
        starts_recent_first = list(reversed(starts))[:n_starts]

        log.info("Pitcher form fetched",
                 pitcher_id=pitcher_id,
                 starts_found=len(starts),
                 using=len(starts_recent_first),
                 season_era=season_era)

        return starts_recent_first

    # ─────────────────────────────────────────────────────────
    # Live game state (for MGS live scoring mode)
    # ─────────────────────────────────────────────────────────

    async def fetch_live_game_state(self, game_id: str) -> dict:
        """
        Fetch the current live state of a game in progress.

        Used by the MGS (Mid-Game Surge) formula to score pitchers
        in real time based on current inning, pitch count, and TTO.

        Returns:
        {
            "game_id": str,
            "status": str,           # "In Progress" | "Final" | "Scheduled" etc.
            "current_inning": int,   # 0 if not started
            "inning_half": str,      # "top" | "bottom"
            "home_pitcher": {
                "pitcher_id": str,
                "name": str,
                "pitch_count": int,
                "batters_faced": int,
            } | None,
            "away_pitcher": {
                "pitcher_id": str,
                "name": str,
                "pitch_count": int,
                "batters_faced": int,
            } | None,
        }
        """
        async with httpx.AsyncClient() as client:
            try:
                # The live feed endpoint returns the full current game state
                data = await self._get(client, f"/game/{game_id}/linescore")
                boxscore = await self._get(client, f"/game/{game_id}/boxscore")
            except Exception as exc:
                log.warning("Live game state fetch failed", game_id=game_id, error=str(exc))
                return {"game_id": game_id, "status": "Unknown", "current_inning": 0,
                        "home_pitcher": None, "away_pitcher": None}

        current_inning = int(data.get("currentInning") or 0)
        inning_half = data.get("inningHalf", "top").lower()
        innings = data.get("innings", [])
        status = data.get("defense", {})  # placeholder — status is in schedule endpoint

        # Parse current pitchers from boxscore
        teams_data = boxscore.get("teams", {})
        pitchers_out = {}

        for side in ("home", "away"):
            side_data = teams_data.get(side, {})
            pitcher_ids = side_data.get("pitchers", [])
            players = side_data.get("players", {})

            if not pitcher_ids:
                pitchers_out[side] = None
                continue

            # Current pitcher = last in the list (most recently entered)
            current_pid = str(pitcher_ids[-1])
            player_key = f"ID{current_pid}"
            player = players.get(player_key, {})
            person = player.get("person", {})
            stats = player.get("stats", {}).get("pitching", {})

            pitchers_out[side] = {
                "pitcher_id": current_pid,
                "name": person.get("fullName", f"Player {current_pid}"),
                "pitch_count": int(stats.get("pitchesThrown") or 0),
                "batters_faced": int(stats.get("battersFaced") or 0),
            }

        log.info("Live game state fetched",
                 game_id=game_id,
                 inning=current_inning,
                 half=inning_half,
                 home_pitcher=pitchers_out.get("home", {}) and pitchers_out["home"].get("name"),
                 away_pitcher=pitchers_out.get("away", {}) and pitchers_out["away"].get("name"))

        return {
            "game_id": game_id,
            "current_inning": current_inning,
            "inning_half": inning_half,
            "home_pitcher": pitchers_out.get("home"),
            "away_pitcher": pitchers_out.get("away"),
        }

    # ─────────────────────────────────────────────────────────
    # Helpers
    # ─────────────────────────────────────────────────────────

    async def fetch_pitcher_first_inning_splits(
        self,
        pitcher_id: str,
        season: str,
    ) -> dict:
        """
        Fetch a pitcher's first-inning performance stats for the season.
        Used by the PFF system to detect cold-start patterns.
        """
        async with httpx.AsyncClient() as client:
            try:
                data = await self._get(client, f"/people/{pitcher_id}/stats", params={
                    "stats": "statSplits",
                    "group": "pitching",
                    "season": season,
                    "sportId": 1,
                    "sitCodes": "t1",
                })

                for block in data.get("stats", []):
                    for split in block.get("splits", []):
                        inning_label = (
                            split.get("inning") or
                            split.get("split", {}).get("description", "")
                        )
                        if "1st" not in str(inning_label) and "t1" not in str(inning_label).lower():
                            continue

                        s = split.get("stat", {})
                        ip_str = str(s.get("inningsPitched") or "0")
                        try:
                            parts = ip_str.split(".")
                            ip = int(parts[0]) + (int(parts[1]) / 3.0 if len(parts) > 1 else 0.0)
                        except Exception:
                            ip = 0.0

                        if ip < 0.1:
                            continue

                        hits = float(s.get("hits") or 0)
                        er = float(s.get("earnedRuns") or 0)
                        ks = float(s.get("strikeOuts") or 0)
                        games = float(s.get("gamesPlayed") or s.get("gamesPitched") or ip)

                        return {
                            "inn1_era": round((er / ip) * 9, 2) if ip else 0.0,
                            "inn1_h9": round((hits / ip) * 9, 2) if ip else 0.0,
                            "inn1_k9": round((ks / ip) * 9, 2) if ip else 0.0,
                            "inn1_ip": round(ip, 2),
                            "cold_start_rate": round(er / games, 3) if games else 0.0,
                        }

            except Exception as exc:
                log.warning("First-inning splits fetch failed", pitcher_id=pitcher_id, error=str(exc))

        return {"inn1_era": None, "inn1_h9": None, "inn1_k9": None, "inn1_ip": None, "cold_start_rate": None}


    async def fetch_pitcher_vaa_data(
        self,
        client: httpx.AsyncClient,
        pitcher_id: str,
        game_id: str,
    ) -> dict:
        """
        Fetch Vertical Approach Angle (VAA) and release extension for a pitcher.

        For live games: extracts pitch data from the live game feed.
        For pre-game / completed games: returns empty dict (no VAA adjustment applied).

        VAA = atan2((plate_z - release_z), -(60.5 - extension)) × (180/π)
        A flat VAA (> -4.5°, i.e. less steep) → easier to track → +10% contact probability.
        Extension > 6.8 ft → +1.5 mph perceived velocity → boosts per_velo.

        Returns:
            {
                "vaa_degrees": float,     # negative angle (e.g. -4.2° for good FB)
                "extension_ft": float,    # release extension in feet (e.g. 6.5)
                "pitch_count": int,       # number of pitches sampled
                "game_status": str,       # "live" | "pre_game"
            }
            or {} if game is not in progress (no live adjustment needed).
        """
        if not game_id:
            return {}

        try:
            # First check if the game is actually live via linescore
            linescore = await self._get(client, f"/game/{game_id}/linescore")
            current_inning = int(linescore.get("currentInning") or 0)
            if current_inning == 0:
                return {}  # pre-game: no VAA data available yet

            # Game is live — pull pitch-by-pitch data from the live feed
            feed = await self._get(client, f"/game/{game_id}/feed/live")
            all_plays = (
                feed.get("liveData", {})
                    .get("plays", {})
                    .get("allPlays", [])
            )

            vaa_list = []
            ext_list = []
            str_pid = str(pitcher_id)

            for play in all_plays:
                # Confirm this pitcher threw the pitch
                matchup = play.get("matchup", {})
                pitcher = matchup.get("pitcher", {})
                if str(pitcher.get("id", "")) != str_pid:
                    continue

                for event in play.get("playEvents", []):
                    if event.get("type") != "pitch":
                        continue

                    pd_ = event.get("pitchData", {})
                    coords = pd_.get("coordinates", {})

                    ext = pd_.get("extension")
                    pz  = coords.get("pZ")
                    z0  = coords.get("z0")   # release height

                    # Calculate VAA if we have enough data
                    if ext is not None and pz is not None and z0 is not None:
                        try:
                            ext_f  = float(ext)
                            pz_f   = float(pz)
                            z0_f   = float(z0)
                            # Horizontal distance from release to plate
                            dist = 60.5 - ext_f
                            if dist > 0:
                                import math
                                vaa = math.degrees(math.atan2(pz_f - z0_f, -dist))
                                vaa_list.append(vaa)
                                ext_list.append(ext_f)
                        except (ValueError, TypeError):
                            pass

            if not vaa_list:
                return {}

            avg_vaa = sum(vaa_list) / len(vaa_list)
            avg_ext = sum(ext_list) / len(ext_list)

            log.info("VAA data fetched from live feed",
                     pitcher_id=pitcher_id,
                     game_id=game_id,
                     pitches_sampled=len(vaa_list),
                     avg_vaa=round(avg_vaa, 2),
                     avg_extension=round(avg_ext, 2))

            return {
                "vaa_degrees": round(avg_vaa, 2),
                "extension_ft": round(avg_ext, 2),
                "pitch_count": len(vaa_list),
                "game_status": "live",
            }

        except Exception as exc:
            log.debug("VAA fetch failed", pitcher_id=pitcher_id, game_id=game_id, error=str(exc))
            return {}

    @staticmethod
    def _safe_float(val: Any) -> Optional[float]:
        try:
            return float(val) if val is not None and val != "" else None
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _parse_wind_direction(wind_str: str) -> Optional[str]:
        """Extract just the directional label from a wind string like '7 mph, In From Left'."""
        if not wind_str:
            return None
        if "mph," in wind_str:
            return wind_str.split("mph,", 1)[1].strip()
        if "mph" in wind_str:
            return wind_str.split("mph", 1)[1].strip().lstrip(",").strip()
        return wind_str.strip() or None

    @staticmethod
    def _is_dome(venue_name: str) -> bool:
        dome_keywords = ["dome", "field house", "rogers centre", "tropicana", "minute maid"]
        return any(kw in venue_name.lower() for kw in dome_keywords)
