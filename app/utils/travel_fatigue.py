"""
app/utils/travel_fatigue.py — SKU #14 Travel & Fatigue Index (TFI).

Measures how much travel stress a pitching team has absorbed before
today's game. Two conditions independently trigger a -7% Reaction Penalty
applied to HUSI (#27):

  1. GETAWAY DAY:  Rest hours between the END of yesterday's game and the
                   START of today's game is < 16 hours.
                   A pitcher who finished a 3-hour game at 10pm and takes
                   a red-eye to pitch at 1pm has had ~11 hours "rest" —
                   that is deeply fatiguing and historically correlates with
                   more hits allowed early in starts.

  2. TIMEZONE SHIFT:  The team's home venue is in a different time zone than
                      yesterday's venue, AND the delta is >= 2 hours.
                      East Coast → West Coast (3 hours) is the classic case.
                      The human body needs ~1 day per time zone to adjust.

Data source: MLB Stats API standard schedule endpoint.
  GET /v1/schedule?teamId={teamId}&sportId=1
                  &startDate={yesterday}&endDate={yesterday}
                  &hydrate=venue,game(content(summary))

All HTTP calls use httpx (consistent with the rest of the stack).
"""
import math
from datetime import date, datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from app.utils.logging import get_logger

log = get_logger("travel_fatigue")

MLB_BASE = "https://statsapi.mlb.com/api/v1"

# Penalty applied to HUSI when travel/fatigue conditions are triggered
TFI_HUSI_PENALTY_PCT = 0.07   # 7% reduction in HUSI score
GETAWAY_REST_THRESHOLD = 16.0  # hours of rest below which penalty triggers
TZ_SHIFT_THRESHOLD = 2         # timezone hour delta at or above which penalty triggers

# MLB venue → IANA timezone name.
# Used to calculate timezone delta between yesterday's venue and today's.
VENUE_TIMEZONES: dict[str, str] = {
    # AL East
    "yankee stadium":           "America/New_York",
    "fenway park":              "America/New_York",
    "camden yards":             "America/New_York",
    "tropicana field":          "America/New_York",
    "rogers centre":            "America/Toronto",
    # AL Central
    "guaranteed rate field":    "America/Chicago",
    "comerica park":            "America/Detroit",
    "progressive field":        "America/New_York",
    "kauffman stadium":         "America/Chicago",
    "target field":             "America/Chicago",
    # AL West
    "minute maid park":         "America/Chicago",
    "angel stadium":            "America/Los_Angeles",
    "t-mobile park":            "America/Los_Angeles",
    "oakland coliseum":         "America/Los_Angeles",
    "sutter health park":       "America/Los_Angeles",  # OAK temp
    "globe life field":         "America/Chicago",
    # NL East
    "citi field":               "America/New_York",
    "nationals park":           "America/New_York",
    "citizens bank park":       "America/New_York",
    "truist park":              "America/New_York",
    "loandepot park":           "America/New_York",
    # NL Central
    "wrigley field":            "America/Chicago",
    "great american ball park": "America/New_York",
    "busch stadium":            "America/Chicago",
    "american family field":    "America/Chicago",
    "pnc park":                 "America/New_York",
    # NL West
    "chase field":              "America/Phoenix",
    "dodger stadium":           "America/Los_Angeles",
    "petco park":               "America/Los_Angeles",
    "oracle park":              "America/Los_Angeles",
    "coors field":              "America/Denver",
}

# Average game duration in hours used when exact end time is unavailable
AVG_GAME_HOURS = 3.1


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=8))
async def _get(client: httpx.AsyncClient, path: str, params: dict = None) -> dict:
    url = f"{MLB_BASE}{path}"
    resp = await client.get(url, params=params or {}, timeout=20.0)
    resp.raise_for_status()
    return resp.json()


async def fetch_team_schedule(
    client: httpx.AsyncClient,
    team_id: str,
    look_back_days: int = 2,
    target_date: Optional[date] = None,
) -> list[dict]:
    """
    Fetch the team's most recent completed games (up to look_back_days before target_date).
    Returns a list of game summary dicts sorted by game date descending.
    """
    today = target_date or date.today()
    start = today - timedelta(days=look_back_days)

    try:
        data = await _get(
            client,
            "/schedule",
            params={
                "teamId": team_id,
                "sportId": 1,
                "startDate": start.strftime("%Y-%m-%d"),
                "endDate": (today - timedelta(days=1)).strftime("%Y-%m-%d"),
                "hydrate": "venue,game(content(summary))",
                "gameType": "R",
            },
        )

        games = []
        for date_entry in data.get("dates", []):
            for game in date_entry.get("games", []):
                status = game.get("status", {}).get("abstractGameState", "")
                if status == "Final":
                    games.append({
                        "game_date": date_entry["date"],
                        "game_pk": game.get("gamePk"),
                        "status": status,
                        "game_time_utc": game.get("gameDate"),  # ISO 8601 UTC
                        "venue_name": game.get("venue", {}).get("name", "").lower().strip(),
                        "venue_id": game.get("venue", {}).get("id"),
                        "home_team_id": game.get("teams", {}).get("home", {}).get("team", {}).get("id"),
                        "away_team_id": game.get("teams", {}).get("away", {}).get("team", {}).get("id"),
                    })

        games.sort(key=lambda g: g["game_date"], reverse=True)
        return games

    except Exception as exc:
        log.warning("Schedule fetch failed", team_id=team_id, error=str(exc))
        return []


def _venue_tz_offset_hours(venue_name: str) -> float:
    """
    Return the UTC offset in hours for a venue name (lowercase).
    Uses a simplified fixed-offset approach — we only care about the delta
    between two venues, so DST edge cases cancel out.
    Returns 0 if unknown.
    """
    tz_name = VENUE_TIMEZONES.get(venue_name.lower().strip())
    if not tz_name:
        # Try partial match
        for k, v in VENUE_TIMEZONES.items():
            if k in venue_name or venue_name in k:
                tz_name = v
                break

    if not tz_name:
        return 0.0

    try:
        tz = ZoneInfo(tz_name)
        # Use a fixed reference datetime; we only care about offsets relative to each other
        ref = datetime(2026, 4, 15, 12, 0, 0, tzinfo=tz)
        return ref.utcoffset().total_seconds() / 3600.0
    except Exception:
        return 0.0


def compute_travel_fatigue_index(
    team_id: str,
    yesterday_game: Optional[dict],
    today_game_time_utc: Optional[str],
    today_venue_name: str,
) -> dict:
    """
    Compute the Travel & Fatigue Index for a team on a given game day.

    Args:
        team_id:             MLB team ID
        yesterday_game:      Game dict from fetch_team_schedule (most recent completed game)
        today_game_time_utc: ISO 8601 UTC string for today's first pitch (e.g. "2026-04-22T23:10:00Z")
        today_venue_name:    Name of today's home venue (lowercase)

    Returns:
        {
            "team_id": str,
            "rest_hours": float,
            "tz_shift": int,              # absolute timezone delta in hours
            "getaway_day": bool,           # rest_hours < 16
            "cross_timezone": bool,        # tz_shift >= 2
            "penalty_active": bool,        # True if either condition triggered
            "penalty_pct": float,          # 0.07 if active, else 0.0
            "tfi_label": str,              # human-readable label
        }
    """
    result = {
        "team_id": str(team_id),
        "rest_hours": 24.0,          # assume full rest as safe default
        "tz_shift": 0,
        "getaway_day": False,
        "cross_timezone": False,
        "penalty_active": False,
        "penalty_pct": 0.0,
        "tfi_label": "NO DATA",
    }

    if not yesterday_game:
        log.debug("TFI: no yesterday game found", team_id=team_id)
        return result

    # ── Calculate rest hours
    yesterday_start_utc = yesterday_game.get("game_time_utc")
    if yesterday_start_utc and today_game_time_utc:
        try:
            # Parse ISO 8601 timestamps
            fmt_options = ["%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S%z"]
            yesterday_dt = None
            today_dt = None
            for fmt in fmt_options:
                try:
                    yesterday_dt = datetime.strptime(yesterday_start_utc[:19], "%Y-%m-%dT%H:%M:%S")
                    today_dt    = datetime.strptime(today_game_time_utc[:19],  "%Y-%m-%dT%H:%M:%S")
                    break
                except ValueError:
                    continue

            if yesterday_dt and today_dt:
                # Estimated game end = yesterday start + avg game duration
                yesterday_end = yesterday_dt + timedelta(hours=AVG_GAME_HOURS)
                rest_secs = (today_dt - yesterday_end).total_seconds()
                result["rest_hours"] = round(rest_secs / 3600.0, 2)

        except Exception as exc:
            log.debug("TFI: time parsing failed", error=str(exc))

    # ── Calculate timezone shift
    yesterday_venue = yesterday_game.get("venue_name", "").lower()
    today_venue = today_venue_name.lower()

    if yesterday_venue and today_venue and yesterday_venue != today_venue:
        tz_yesterday = _venue_tz_offset_hours(yesterday_venue)
        tz_today     = _venue_tz_offset_hours(today_venue)
        shift = abs(int(tz_yesterday - tz_today))
        result["tz_shift"] = shift
    else:
        result["tz_shift"] = 0

    # ── Evaluate conditions
    getaway = result["rest_hours"] < GETAWAY_REST_THRESHOLD
    cross_tz = result["tz_shift"] >= TZ_SHIFT_THRESHOLD
    penalty_active = getaway or cross_tz

    result["getaway_day"]     = getaway
    result["cross_timezone"]  = cross_tz
    result["penalty_active"]  = penalty_active
    result["penalty_pct"]     = TFI_HUSI_PENALTY_PCT if penalty_active else 0.0

    # Build human-readable label
    if not penalty_active:
        result["tfi_label"] = "RESTED"
    elif getaway and cross_tz:
        result["tfi_label"] = f"GETAWAY+CROSS_TZ ({result['rest_hours']:.1f}h rest, Δ{result['tz_shift']}hr TZ)"
    elif getaway:
        result["tfi_label"] = f"GETAWAY DAY ({result['rest_hours']:.1f}h rest)"
    else:
        result["tfi_label"] = f"CROSS_TZ (Δ{result['tz_shift']}hr timezone shift)"

    log.info("TFI computed",
             team_id=team_id,
             rest_hours=result["rest_hours"],
             tz_shift=result["tz_shift"],
             getaway=getaway,
             cross_tz=cross_tz,
             penalty=penalty_active,
             label=result["tfi_label"])

    return result


def apply_tfi_to_husi(husi: float, tfi_penalty_pct: float) -> float:
    """
    Apply the Travel & Fatigue Index penalty to a HUSI score.

    A fatigued/traveling pitching team is expected to pitch worse → more hits
    allowed → HUSI (hits UNDER probability) decreases.

    Formula: HUSI_tfi = HUSI × (1 - penalty_pct)
    """
    if tfi_penalty_pct <= 0.0:
        return husi
    adjusted = husi * (1.0 - tfi_penalty_pct)
    return round(max(0.0, adjusted), 2)
