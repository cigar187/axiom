"""
nfl_schedule.py — ESPN NFL schedule and QB starter fetcher.

Data source: ESPN public NFL API (no API key required)
  Scoreboard: https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard
  Summary:    https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary?event={game_id}

Fetches:
  - All NFL games scheduled for the current week
  - Starting QB for each team in each game (when available)
  - Venue name, surface type, and dome flag per game

Usage:
  Called by the NFL pipeline before scoring to populate nfl_games and nfl_qb_starters tables.
  Can also be run standalone: python -m app.services.nfl_schedule

Error handling:
  - All HTTP calls timeout after 10 seconds
  - On any failure: logs at ERROR/WARNING level and returns empty list
  - Never raises — caller always receives a list (possibly empty)
"""
from datetime import date, datetime, timezone
from typing import Optional

import httpx

from app.utils.logging import get_logger

log = get_logger("nfl_schedule")

ESPN_BASE      = "https://site.api.espn.com/apis/site/v2/sports/football/nfl"
SCOREBOARD_URL = f"{ESPN_BASE}/scoreboard"
SUMMARY_URL    = f"{ESPN_BASE}/summary"
TIMEOUT        = 10.0  # seconds


# ─────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────

def _parse_surface(competition: dict) -> str:
    """Extract playing surface from an ESPN competition object."""
    venue = competition.get("venue", {})
    surface = venue.get("grass")
    if surface is True:
        return "grass"
    if surface is False:
        return "artificial"
    # Fall back to the surfaceType field if present
    return competition.get("venue", {}).get("surfaceType", "unknown")


def _parse_is_dome(competition: dict) -> bool:
    """Return True if the venue is indoor/retractable."""
    return bool(competition.get("venue", {}).get("indoor", False))


def _parse_game_date(event: dict) -> Optional[date]:
    """Parse the ISO-8601 date string from an ESPN event into a Python date."""
    raw = event.get("date", "")
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc).date()
    except ValueError:
        return None


def _find_starting_qbs(roster_teams: list[dict], game_id: str,
                        home_abbr: str, away_abbr: str) -> list[dict]:
    """
    Walk the roster payload from the ESPN summary endpoint and return
    up to 2 starter dicts — one per team — where position is QB.

    ESPN marks the projected/confirmed starter with starter=True.
    Falls back to the first active QB on the depth chart if starter
    flag is absent (pre-game depth chart mode).
    """
    starters = []

    for team_block in roster_teams:
        team_abbr = team_block.get("team", {}).get("abbreviation", "")
        is_home   = team_abbr.upper() == home_abbr.upper()
        opponent  = away_abbr if is_home else home_abbr

        athletes = team_block.get("roster", [])

        # Pass 1: explicit starter flag
        qb = next(
            (a for a in athletes
             if a.get("athlete", {}).get("position", {}).get("abbreviation", "") == "QB"
             and a.get("starter", False)),
            None
        )

        # Pass 2: first active QB on depth chart
        if qb is None:
            qb = next(
                (a for a in athletes
                 if a.get("athlete", {}).get("position", {}).get("abbreviation", "") == "QB"
                 and a.get("active", True)),
                None
            )

        if qb is None:
            log.warning("nfl_schedule: no QB found in roster",
                        game_id=game_id, team=team_abbr)
            continue

        athlete    = qb.get("athlete", {})
        qb_name    = athlete.get("displayName", "Unknown")

        # Injury designation — lives inside athlete.status.type.description
        inj_status = (
            athlete.get("status", {})
                   .get("type", {})
                   .get("description")
        )
        # Only keep meaningful designations; None means healthy/no report
        meaningful = {"Questionable", "Doubtful", "Out", "Injured Reserve", "PUP"}
        injury_designation = inj_status if inj_status in meaningful else None

        starters.append({
            "game_id":            game_id,
            "qb_name":            qb_name,
            "team":               team_abbr,
            "opponent":           opponent,
            "is_home":            is_home,
            "injury_designation": injury_designation,
        })

    return starters


# ─────────────────────────────────────────────────────────────
# Public functions
# ─────────────────────────────────────────────────────────────

def get_nfl_games_this_week() -> list[dict]:
    """
    Fetch all NFL games scheduled for the current week from the ESPN scoreboard.

    Returns a list of game dicts, each containing:
      game_id   — ESPN event id (string)
      game_date — Python date object
      home_team — team abbreviation (e.g. "KC")
      away_team — team abbreviation (e.g. "CHI")
      stadium   — venue full name
      surface   — "grass" | "artificial" | "unknown"
      is_dome   — bool

    Returns empty list if the API call fails.
    """
    try:
        resp = httpx.get(SCOREBOARD_URL, timeout=TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
    except httpx.TimeoutException:
        log.error("nfl_schedule: scoreboard request timed out", url=SCOREBOARD_URL)
        return []
    except httpx.HTTPStatusError as exc:
        log.error("nfl_schedule: scoreboard HTTP error",
                  status=exc.response.status_code, url=SCOREBOARD_URL)
        return []
    except Exception as exc:
        log.error("nfl_schedule: scoreboard unexpected error", error=str(exc))
        return []

    games = []
    for event in data.get("events", []):
        game_id    = str(event.get("id", ""))
        game_date  = _parse_game_date(event)

        competitions = event.get("competitions", [])
        if not competitions:
            continue
        competition = competitions[0]

        home_team = ""
        away_team = ""
        for competitor in competition.get("competitors", []):
            abbr = competitor.get("team", {}).get("abbreviation", "")
            if competitor.get("homeAway") == "home":
                home_team = abbr
            else:
                away_team = abbr

        venue   = competition.get("venue", {})
        stadium = venue.get("fullName", "Unknown Stadium")
        surface = _parse_surface(competition)
        is_dome = _parse_is_dome(competition)

        if not game_id or not game_date or not home_team or not away_team:
            log.warning("nfl_schedule: skipping incomplete game record",
                        game_id=game_id, game_date=str(game_date))
            continue

        games.append({
            "game_id":   game_id,
            "game_date": game_date,
            "home_team": home_team,
            "away_team": away_team,
            "stadium":   stadium,
            "surface":   surface,
            "is_dome":   is_dome,
        })

    log.info("nfl_schedule: games found this week", count=len(games))
    return games


def get_qb_starters(game_id: str, home_team: str, away_team: str) -> list[dict]:
    """
    Fetch the starting QB for each team in a specific game.

    Calls the ESPN game summary endpoint and parses the roster/lineup section.
    Returns up to 2 dicts (one per team), each containing:
      game_id            — string
      qb_name            — full display name
      team               — team abbreviation
      opponent           — opposing team abbreviation
      is_home            — bool
      injury_designation — string if Questionable/Doubtful/Out/IR, else None

    Returns empty list if starters cannot be determined.
    """
    try:
        resp = httpx.get(SUMMARY_URL, params={"event": game_id}, timeout=TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
    except httpx.TimeoutException:
        log.warning("nfl_schedule: summary request timed out",
                    game_id=game_id, url=SUMMARY_URL)
        return []
    except httpx.HTTPStatusError as exc:
        log.warning("nfl_schedule: summary HTTP error",
                    game_id=game_id, status=exc.response.status_code)
        return []
    except Exception as exc:
        log.warning("nfl_schedule: summary unexpected error",
                    game_id=game_id, error=str(exc))
        return []

    roster_teams = data.get("rosters", [])
    if not roster_teams:
        log.warning("nfl_schedule: no roster data in summary",
                    game_id=game_id)
        return []

    starters = _find_starting_qbs(roster_teams, game_id, home_team, away_team)

    if not starters:
        log.warning("nfl_schedule: no QB starters found",
                    game_id=game_id, home_team=home_team, away_team=away_team)

    return starters


def get_all_starters_this_week() -> tuple[list[dict], list[dict]]:
    """
    Fetch all NFL games and their starting QBs for the current week.

    Calls get_nfl_games_this_week() first, then calls get_qb_starters()
    for each game. Returns a tuple of (games, starters) — both lists
    are ready to be written directly to the nfl_games and nfl_qb_starters tables.

    Returns ([], []) if no games are found.
    """
    games = get_nfl_games_this_week()
    if not games:
        log.warning("nfl_schedule: no games found — skipping QB starter fetch")
        return [], []

    all_starters: list[dict] = []
    for game in games:
        starters = get_qb_starters(
            game_id=game["game_id"],
            home_team=game["home_team"],
            away_team=game["away_team"],
        )
        all_starters.extend(starters)

    log.info("nfl_schedule: week fetch complete",
             total_games=len(games),
             total_starters=len(all_starters))

    return games, all_starters


# ─────────────────────────────────────────────────────────────
# Standalone runner
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    games, starters = get_all_starters_this_week()
    print(f"\n{len(games)} games found:")
    for g in games:
        print(f"  {g['away_team']} @ {g['home_team']}  —  {g['game_date']}  "
              f"({'dome' if g['is_dome'] else 'outdoor'}, {g['surface']})")
    print(f"\n{len(starters)} QB starters found:")
    for s in starters:
        inj = f" [{s['injury_designation']}]" if s["injury_designation"] else ""
        print(f"  {s['qb_name']} ({s['team']} vs {s['opponent']}){inj}")
