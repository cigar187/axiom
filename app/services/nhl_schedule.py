"""
nhl_schedule.py — NHL schedule and game context fetcher.

Data source: NHL public API (no API key required)
  Schedule: https://api-web.nhle.com/v1/schedule/{date}
  Landing:  https://api-web.nhle.com/v1/gamecenter/{game_id}/landing
  Boxscore: https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore

Fetches:
  - All NHL games scheduled for a given date
  - Series context for playoff games (game number, wins per team)
  - Back-to-back and rest-day flags (derived from prior-day schedule checks)
  - Venue name per game
  - Forward / defense / goalie rosters from the boxscore

Usage:
  Called by the NHL pipeline before scoring to build NHLGameContext objects.
  Can also be run standalone: python -m app.services.nhl_schedule

Error handling:
  - All HTTP calls timeout after 10 seconds
  - On any failure: logs at WARNING/ERROR level and returns empty list / empty dict
  - Never raises — caller always receives a list (possibly empty)
"""
import json
import urllib.error
import urllib.request
from datetime import date, timedelta, datetime, timezone
from typing import Optional
import zoneinfo

from app.core.nhl.features import NHLGameContext
from app.utils.logging import get_logger

log = get_logger("nhl_schedule")

NHL_API = "https://api-web.nhle.com/v1"
TIMEOUT = 10  # seconds


# ─────────────────────────────────────────────────────────────
# Internal HTTP helper
# ─────────────────────────────────────────────────────────────

def _fetch(url: str, timeout: int = TIMEOUT) -> Optional[dict | list]:
    """
    Fetch a URL from the NHL public API and return the parsed JSON.
    Returns None on any error — callers must handle None gracefully.
    """
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "AxiomNHL/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        log.warning("nhl_schedule: HTTP error", url=url, status=exc.code)
        return None
    except urllib.error.URLError as exc:
        log.warning("nhl_schedule: URL error", url=url, error=str(exc.reason))
        return None
    except TimeoutError:
        log.warning("nhl_schedule: request timed out", url=url)
        return None
    except Exception as exc:
        log.warning("nhl_schedule: unexpected fetch error", url=url, error=str(exc))
        return None


# ─────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────

def _today_str() -> str:
    """Return today's date in Eastern time (NHL schedule uses ET, not UTC)."""
    eastern = zoneinfo.ZoneInfo("America/New_York")
    return datetime.now(tz=eastern).strftime("%Y-%m-%d")


def _teams_that_played(game_date_str: str) -> set[str]:
    """
    Return the set of team abbreviations that played on the given date.
    Used to detect back-to-back situations for today's games.
    """
    data = _fetch(f"{NHL_API}/schedule/{game_date_str}")
    if not data:
        return set()
    teams: set[str] = set()
    for gw in data.get("gameWeek", []):
        if gw.get("date") == game_date_str:
            for g in gw.get("games", []):
                home = g.get("homeTeam", {}).get("abbrev", "")
                away = g.get("awayTeam", {}).get("abbrev", "")
                if home:
                    teams.add(home)
                if away:
                    teams.add(away)
    return teams


def _rest_days(team_abbrev: str, game_date_str: str) -> tuple[bool, int]:
    """
    Determine how many rest days a team has before game_date_str and whether
    they are on a back-to-back.

    Checks up to 4 prior days. Returns (is_b2b, rest_days).
      rest_days = 1  → played yesterday (B2B)
      rest_days = 2  → played two days ago
      rest_days = 3  → played three days ago
      rest_days = 4  → played four or more days ago (e.g. after a series win/loss)
    """
    game_date = date.fromisoformat(game_date_str)
    for days_back in range(1, 5):
        prior = str(game_date - timedelta(days=days_back))
        if team_abbrev in _teams_that_played(prior):
            return (days_back == 1), days_back
    return False, 4   # no game found in last 4 days → well rested


# ─────────────────────────────────────────────────────────────
# Public functions — schedule and game data
# ─────────────────────────────────────────────────────────────

def get_today_schedule(game_date: str = None) -> list[dict]:
    """
    Fetch all NHL games scheduled for the given date.

    Args:
        game_date: ISO date string "YYYY-MM-DD". Defaults to today if None.

    Returns a list of cleaned game dicts, each containing:
      game_id           — NHL game ID (int)
      game_date         — date string "YYYY-MM-DD"
      home_team         — team abbreviation (e.g. "TBL")
      away_team         — team abbreviation (e.g. "MTL")
      venue             — arena name
      game_type         — int (1=preseason, 2=regular, 3=playoff)
      series_status     — raw seriesStatus dict (None for regular season)

    Returns empty list if the API call fails.
    """
    target = game_date or _today_str()
    data = _fetch(f"{NHL_API}/schedule/{target}")
    if not data:
        log.error("nhl_schedule: schedule fetch returned nothing", date=target)
        return []

    games = []
    for gw in data.get("gameWeek", []):
        if gw.get("date") != target:
            continue
        for g in gw.get("games", []):
            game_id   = g.get("id")
            home_abbr = g.get("homeTeam", {}).get("abbrev", "")
            away_abbr = g.get("awayTeam", {}).get("abbrev", "")
            venue     = g.get("venue", {}).get("default", "Unknown Arena")
            game_type = g.get("gameType", 2)

            if not game_id or not home_abbr or not away_abbr:
                log.warning("nhl_schedule: skipping incomplete game record",
                            game_id=game_id, date=target)
                continue

            games.append({
                "game_id":       game_id,
                "game_date":     target,
                "home_team":     home_abbr,
                "away_team":     away_abbr,
                "venue":         venue,
                "game_type":     game_type,
                "series_status": g.get("seriesStatus"),   # None for regular season
            })

    log.info("nhl_schedule: games found", date=target, count=len(games))
    return games


def get_game_landing(game_id: int) -> dict:
    """
    Fetch the full landing page for a specific game.
    Returns empty dict if the API call fails.
    """
    data = _fetch(f"{NHL_API}/gamecenter/{game_id}/landing")
    return data if isinstance(data, dict) else {}


def get_game_boxscore(game_id: int) -> dict:
    """
    Fetch the boxscore for a specific game.
    Returns empty dict if the API call fails.
    """
    data = _fetch(f"{NHL_API}/gamecenter/{game_id}/boxscore")
    return data if isinstance(data, dict) else {}


def get_roster_from_boxscore(
    game_id: int,
    team_abbrev: str,
) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Pull the skater and goalie roster for one team from the game boxscore.

    Player data lives under playerByGameStats.homeTeam / awayTeam, NOT directly
    under homeTeam / awayTeam (which only carries aggregate scores).

    Returns a tuple of (forwards, defense, goalies) — each a list of player dicts.
    Each dict includes: playerId, name.default, position, toi, goals, assists, etc.
    Goalies have position="G" and include saves, shotsAgainst, starter fields.

    Returns ([], [], []) if the team is not found or the API call fails.
    """
    bs = get_game_boxscore(game_id)
    pgbs = bs.get("playerByGameStats", {})
    for side in ("homeTeam", "awayTeam"):
        # Identity check against the top-level team object (has abbrev)
        if bs.get(side, {}).get("abbrev", "") == team_abbrev:
            team_data = pgbs.get(side, {})
            return (
                team_data.get("forwards", []),
                team_data.get("defense", []),
                team_data.get("goalies", []),
            )
    log.warning("nhl_schedule: team not found in boxscore",
                game_id=game_id, team=team_abbrev)
    return [], [], []


# ─────────────────────────────────────────────────────────────
# Pre-game roster fallback (used when boxscore is empty / game FUT)
# ─────────────────────────────────────────────────────────────

def get_roster_from_pregame(
    game_id: int,
) -> tuple[
    tuple[list[dict], list[dict], list[dict]],   # home: (fwds, defs, gols)
    tuple[list[dict], list[dict], list[dict]],   # away: (fwds, defs, gols)
]:
    """
    Fetch the game-specific eligible roster from the NHL play-by-play endpoint.

    Used as a fallback when get_roster_from_boxscore() returns nothing because
    the game has not started yet (gameState = FUT).

    The play-by-play endpoint exposes rosterSpots which lists every player
    registered for this specific game by the team — the actual game roster,
    NOT the full season roster.  This is the most accurate pre-game source
    available in the NHL public API.

    Player dicts are shaped identically to the boxscore endpoint so the
    feature builder can consume them without modification:
      {
        "playerId": int,
        "name":     {"default": "First Last"},
        "position": str,   # C, L, R, D, or G
        "toi":      "0:00",
        "goals": 0, "assists": 0, "points": 0, "shots": 0,
      }

    Returns ((home_fwds, home_defs, home_gols), (away_fwds, away_defs, away_gols)).
    On any failure, returns two empty tuples so the caller can handle gracefully.
    """
    empty = ([], [], [])
    data = _fetch(f"{NHL_API}/gamecenter/{game_id}/play-by-play")
    if not data:
        log.warning("nhl_schedule: play-by-play fetch returned nothing",
                    game_id=game_id)
        return empty, empty

    home_id   = data.get("homeTeam", {}).get("id")
    away_id   = data.get("awayTeam", {}).get("id")
    home_abbr = data.get("homeTeam", {}).get("abbrev", "HOME")
    away_abbr = data.get("awayTeam", {}).get("abbrev", "AWAY")
    spots     = data.get("rosterSpots", [])

    if not spots:
        log.warning("nhl_schedule: rosterSpots empty in play-by-play",
                    game_id=game_id)
        return empty, empty

    def _to_player_dict(raw: dict) -> dict:
        first = raw.get("firstName", {}).get("default", "")
        last  = raw.get("lastName",  {}).get("default", "")
        return {
            "playerId": raw.get("playerId", 0),
            "name":     {"default": f"{first} {last}".strip()},
            "position": raw.get("positionCode", "C"),
            "toi":      "0:00",
            "goals":    0,
            "assists":  0,
            "points":   0,
            "shots":    0,
        }

    def _split(team_spots: list[dict]) -> tuple[list[dict], list[dict], list[dict]]:
        fwds = [_to_player_dict(s) for s in team_spots
                if s.get("positionCode") in ("C", "L", "R")]
        defs = [_to_player_dict(s) for s in team_spots
                if s.get("positionCode") == "D"]
        gols = [_to_player_dict(s) for s in team_spots
                if s.get("positionCode") == "G"]
        return fwds, defs, gols

    home_spots = [s for s in spots if s.get("teamId") == home_id]
    away_spots = [s for s in spots if s.get("teamId") == away_id]

    home_roster = _split(home_spots)
    away_roster = _split(away_spots)

    log.info(
        "nhl_schedule: pre-game roster fetched from play-by-play rosterSpots",
        game_id=game_id,
        home=f"{home_abbr} {len(home_spots)} players",
        away=f"{away_abbr} {len(away_spots)} players",
    )
    return home_roster, away_roster


# ─────────────────────────────────────────────────────────────
# Build game contexts — the primary entry point for the pipeline
# ─────────────────────────────────────────────────────────────

def build_game_contexts(game_date: str = None) -> list[NHLGameContext]:
    """
    Fetch today's NHL schedule and build a fully populated NHLGameContext
    object for every game.

    Args:
        game_date: ISO date string "YYYY-MM-DD". Defaults to today if None.

    Each NHLGameContext includes:
      - game_id, game_date, home_team, away_team, venue
      - series_game_number, home_series_wins, away_series_wins (playoffs only)
      - home_b2b, away_b2b, home_rest_days, away_rest_days

    Returns empty list if no games are found or the schedule fetch fails.
    Logs at WARNING and continues if a single game context fails to build.
    """
    target = game_date or _today_str()
    games  = get_today_schedule(target)

    if not games:
        log.warning("nhl_schedule: no games found, cannot build contexts",
                    date=target)
        return []

    contexts: list[NHLGameContext] = []

    for g in games:
        game_id   = g["game_id"]
        home_abbr = g["home_team"]
        away_abbr = g["away_team"]

        try:
            # ── Series context (playoffs only) ────────────────────────────
            series_game_number = 0
            home_series_wins   = 0
            away_series_wins   = 0

            ss = g.get("series_status")
            if ss:
                series_game_number = int(ss.get("gameNumberOfSeries", 0) or 0)
                top_abbrev  = ss.get("topSeedTeamAbbrev", "")
                top_wins    = int(ss.get("topSeedWins", 0) or 0)
                bot_wins    = int(ss.get("bottomSeedWins", 0) or 0)

                if home_abbr == top_abbrev:
                    home_series_wins = top_wins
                    away_series_wins = bot_wins
                else:
                    home_series_wins = bot_wins
                    away_series_wins = top_wins

            # ── Back-to-back and rest days ────────────────────────────────
            home_b2b, home_rest = _rest_days(home_abbr, target)
            away_b2b, away_rest = _rest_days(away_abbr, target)

            ctx = NHLGameContext(
                game_id=str(game_id),
                game_date=target,
                home_team=home_abbr,
                away_team=away_abbr,
                venue=g["venue"],
                series_game_number=series_game_number,
                home_series_wins=home_series_wins,
                away_series_wins=away_series_wins,
                home_b2b=home_b2b,
                away_b2b=away_b2b,
                home_rest_days=home_rest,
                away_rest_days=away_rest,
            )

            contexts.append(ctx)

            log.info(
                "nhl_schedule: game context built",
                game_id=game_id,
                matchup=f"{away_abbr}@{home_abbr}",
                series_game=series_game_number,
                home_series_wins=home_series_wins,
                away_series_wins=away_series_wins,
                home_b2b=home_b2b,
                away_b2b=away_b2b,
                home_rest=home_rest,
                away_rest=away_rest,
            )

        except Exception as exc:
            log.warning(
                "nhl_schedule: failed to build context for game — skipping",
                game_id=game_id,
                matchup=f"{away_abbr}@{home_abbr}",
                error=str(exc),
            )
            continue

    log.info("nhl_schedule: contexts built",
             date=target, total=len(contexts))
    return contexts


# ─────────────────────────────────────────────────────────────
# Standalone runner — live feed test
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    contexts = build_game_contexts()
    if not contexts:
        print("No NHL games found today.")
    else:
        print(f"\n{len(contexts)} NHL game(s) today:\n")
        for ctx in contexts:
            b2b_str = ""
            if ctx.home_b2b:
                b2b_str += f" [{ctx.home_team} B2B]"
            if ctx.away_b2b:
                b2b_str += f" [{ctx.away_team} B2B]"
            series_str = ""
            if ctx.series_game_number:
                series_str = (
                    f"  Game {ctx.series_game_number} of series  "
                    f"({ctx.home_team} {ctx.home_series_wins}–{ctx.away_series_wins} "
                    f"{ctx.away_team})"
                )
            print(
                f"  {ctx.away_team} @ {ctx.home_team}  |  "
                f"{ctx.venue}  |  "
                f"rest: {ctx.home_team}={ctx.home_rest_days}d  "
                f"{ctx.away_team}={ctx.away_rest_days}d"
                f"{b2b_str}"
            )
            if series_str:
                print(f"  {series_str}")
            print()
