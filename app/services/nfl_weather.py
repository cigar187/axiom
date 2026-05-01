"""
nfl_weather.py — National Weather Service (NWS) environmental data fetcher for NFL stadiums.

Data source: api.weather.gov (US government, free, no API key, no licensing fees)
Same API and same two-step call pattern as app/services/weather.py.

Strategy:
  - All 32 NFL stadiums are pre-mapped to lat/lng coordinates.
  - For dome stadiums: skip the API entirely and return fixed neutral values.
  - For outdoor stadiums: ping the NWS hourly forecast endpoint for the game date.
  - Returns wind, temperature, precipitation chance, and condition for use in the
    ENS block of the QPYI and QTDI formulas.

Usage:
  Called by the NFL pipeline before scoring to populate weather context per game.
  Can also be run standalone: python -m app.services.nfl_weather
"""
from datetime import date, datetime, timezone
from typing import Optional

import httpx

from app.utils.logging import get_logger

log = get_logger("nfl_weather")

NWS_BASE = "https://api.weather.gov"
TIMEOUT  = 10.0  # seconds


# ─────────────────────────────────────────────────────────────
# All 32 NFL stadiums — lat/lng, dome flag, surface type
# ─────────────────────────────────────────────────────────────

NFL_STADIUMS: dict[str, dict] = {
    # Dome / retractable-roof stadiums — weather is irrelevant when closed
    "ARI": {"name": "State Farm Stadium",          "city": "Glendale",       "lat": 33.5276, "lon": -112.2626, "is_dome": True,  "surface": "artificial"},
    "ATL": {"name": "Mercedes-Benz Stadium",        "city": "Atlanta",        "lat": 33.7554, "lon": -84.4008,  "is_dome": True,  "surface": "artificial"},
    "DAL": {"name": "AT&T Stadium",                 "city": "Arlington",      "lat": 32.7480, "lon": -97.0928,  "is_dome": True,  "surface": "artificial"},
    "DET": {"name": "Ford Field",                   "city": "Detroit",        "lat": 42.3400, "lon": -83.0456,  "is_dome": True,  "surface": "artificial"},
    "HOU": {"name": "NRG Stadium",                  "city": "Houston",        "lat": 29.6847, "lon": -95.4107,  "is_dome": True,  "surface": "artificial"},
    "IND": {"name": "Lucas Oil Stadium",            "city": "Indianapolis",   "lat": 39.7601, "lon": -86.1639,  "is_dome": True,  "surface": "artificial"},
    "LV":  {"name": "Allegiant Stadium",            "city": "Las Vegas",      "lat": 36.0909, "lon": -115.1833, "is_dome": True,  "surface": "artificial"},
    "MIN": {"name": "U.S. Bank Stadium",            "city": "Minneapolis",    "lat": 44.9740, "lon": -93.2578,  "is_dome": True,  "surface": "artificial"},
    "NO":  {"name": "Caesars Superdome",            "city": "New Orleans",    "lat": 29.9511, "lon": -90.0812,  "is_dome": True,  "surface": "artificial"},
    # Outdoor stadiums — weather matters
    "BAL": {"name": "M&T Bank Stadium",             "city": "Baltimore",      "lat": 39.2780, "lon": -76.6227,  "is_dome": False, "surface": "natural"},
    "BUF": {"name": "Highmark Stadium",             "city": "Orchard Park",   "lat": 42.7738, "lon": -78.7870,  "is_dome": False, "surface": "artificial"},
    "CAR": {"name": "Bank of America Stadium",      "city": "Charlotte",      "lat": 35.2258, "lon": -80.8528,  "is_dome": False, "surface": "natural"},
    "CHI": {"name": "Soldier Field",                "city": "Chicago",        "lat": 41.8623, "lon": -87.6167,  "is_dome": False, "surface": "natural"},
    "CIN": {"name": "Paycor Stadium",               "city": "Cincinnati",     "lat": 39.0955, "lon": -84.5160,  "is_dome": False, "surface": "natural"},
    "CLE": {"name": "Huntington Bank Field",        "city": "Cleveland",      "lat": 41.5061, "lon": -81.6995,  "is_dome": False, "surface": "natural"},
    "DEN": {"name": "Empower Field at Mile High",   "city": "Denver",         "lat": 39.7439, "lon": -105.0201, "is_dome": False, "surface": "natural"},
    "GB":  {"name": "Lambeau Field",                "city": "Green Bay",      "lat": 44.5013, "lon": -88.0622,  "is_dome": False, "surface": "natural"},
    "JAX": {"name": "EverBank Stadium",             "city": "Jacksonville",   "lat": 30.3239, "lon": -81.6373,  "is_dome": False, "surface": "natural"},
    "KC":  {"name": "GEHA Field at Arrowhead",      "city": "Kansas City",    "lat": 39.0489, "lon": -94.4839,  "is_dome": False, "surface": "natural"},
    "LAC": {"name": "SoFi Stadium",                 "city": "Inglewood",      "lat": 33.9535, "lon": -118.3392, "is_dome": False, "surface": "natural"},
    "LAR": {"name": "SoFi Stadium",                 "city": "Inglewood",      "lat": 33.9535, "lon": -118.3392, "is_dome": False, "surface": "natural"},
    "MIA": {"name": "Hard Rock Stadium",            "city": "Miami Gardens",  "lat": 25.9580, "lon": -80.2389,  "is_dome": False, "surface": "natural"},
    "NE":  {"name": "Gillette Stadium",             "city": "Foxborough",     "lat": 42.0909, "lon": -71.2643,  "is_dome": False, "surface": "natural"},
    "NYG": {"name": "MetLife Stadium",              "city": "East Rutherford","lat": 40.8135, "lon": -74.0745,  "is_dome": False, "surface": "artificial"},
    "NYJ": {"name": "MetLife Stadium",              "city": "East Rutherford","lat": 40.8135, "lon": -74.0745,  "is_dome": False, "surface": "artificial"},
    "PHI": {"name": "Lincoln Financial Field",      "city": "Philadelphia",   "lat": 39.9008, "lon": -75.1675,  "is_dome": False, "surface": "natural"},
    "PIT": {"name": "Acrisure Stadium",             "city": "Pittsburgh",     "lat": 40.4468, "lon": -80.0158,  "is_dome": False, "surface": "natural"},
    "SF":  {"name": "Levi's Stadium",               "city": "Santa Clara",    "lat": 37.4032, "lon": -121.9698, "is_dome": False, "surface": "natural"},
    "SEA": {"name": "Lumen Field",                  "city": "Seattle",        "lat": 47.5952, "lon": -122.3316, "is_dome": False, "surface": "artificial"},
    "TB":  {"name": "Raymond James Stadium",        "city": "Tampa",          "lat": 27.9759, "lon": -82.5033,  "is_dome": False, "surface": "natural"},
    "TEN": {"name": "Nissan Stadium",               "city": "Nashville",      "lat": 36.1665, "lon": -86.7713,  "is_dome": False, "surface": "natural"},
    "WAS": {"name": "Northwest Stadium",            "city": "Landover",       "lat": 38.9078, "lon": -76.8645,  "is_dome": False, "surface": "natural"},
}


# ─────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────

def _parse_wind_speed(wind_str: str) -> float:
    """Parse NWS wind speed string like '12 mph' or '5 to 15 mph' into a float."""
    try:
        parts   = wind_str.lower().replace("mph", "").strip().split()
        numbers = [float(p) for p in parts if p.replace(".", "").isdigit()]
        return sum(numbers) / len(numbers) if numbers else 0.0
    except Exception:
        return 0.0


def _neutral_weather(is_dome: bool = False, surface: str = "unknown") -> dict:
    """Return a safe neutral weather dict used as a fallback on any API failure."""
    return {
        "wind_mph":      5.0,
        "temp_f":        72.0,
        "precip_chance": 0.0,
        "condition":     "Unknown",
        "is_dome":       is_dome,
        "surface":       surface,
        "source":        "neutral_fallback",
    }


def _dome_weather(surface: str) -> dict:
    """Return fixed neutral values for dome/retractable-roof stadiums."""
    return {
        "wind_mph":      0.0,
        "temp_f":        72.0,
        "precip_chance": 0.0,
        "condition":     "Dome",
        "is_dome":       True,
        "surface":       surface,
        "source":        "fixed_dome",
    }


def _find_period_for_date(periods: list[dict], target_date: date) -> Optional[dict]:
    """
    Find the first NWS hourly forecast period that falls on target_date
    during typical NFL game hours (noon to 9 pm local).
    Falls back to any period on that date, then to the first available period.
    """
    game_hour_range = range(12, 22)  # noon to 10 pm inclusive

    afternoon: list[dict] = []
    any_on_date: list[dict] = []

    for period in periods:
        start_raw = period.get("startTime", "")
        if not start_raw:
            continue
        try:
            dt = datetime.fromisoformat(start_raw).astimezone(timezone.utc)
        except ValueError:
            continue
        # Compare in UTC; most NFL games fall cleanly on the right date UTC
        if dt.date() == target_date:
            any_on_date.append(period)
            if dt.hour in game_hour_range:
                afternoon.append(period)

    if afternoon:
        return afternoon[0]
    if any_on_date:
        return any_on_date[0]
    # Fallback: first available period in the feed (handles same-day requests)
    return periods[0] if periods else None


# ─────────────────────────────────────────────────────────────
# Public functions
# ─────────────────────────────────────────────────────────────

def get_weather_for_game(home_team: str, game_date: str) -> dict:
    """
    Fetch weather conditions for an NFL game based on the home team's stadium.

    Args:
      home_team: Team abbreviation matching NFL_STADIUMS (e.g. "KC", "BUF").
      game_date: Date string in "YYYY-MM-DD" format.

    For dome stadiums, skips the API entirely and returns neutral indoor values.
    For outdoor stadiums, calls the NWS two-step endpoint (points → hourly forecast)
    and returns conditions for the game date.

    Returns a dict containing:
      wind_mph      — float
      temp_f        — float
      precip_chance — float 0–100
      condition     — string (e.g. "Partly Cloudy", "Rain", "Snow", "Dome")
      is_dome       — bool
      surface       — "natural" or "artificial"
      source        — "NWS", "fixed_dome", or "neutral_fallback"
    """
    abbr    = home_team.upper()
    stadium = NFL_STADIUMS.get(abbr)

    if not stadium:
        log.warning("nfl_weather: unknown home team, using neutral weather",
                    team=home_team)
        return _neutral_weather()

    if stadium["is_dome"]:
        log.info("nfl_weather: dome stadium — skipping API",
                 team=abbr, stadium=stadium["name"])
        return _dome_weather(stadium["surface"])

    lat = stadium["lat"]
    lon = stadium["lon"]
    try:
        target_date = date.fromisoformat(game_date)
    except ValueError:
        log.warning("nfl_weather: invalid game_date format",
                    game_date=game_date, team=abbr)
        return _neutral_weather(surface=stadium["surface"])

    try:
        # Step 1 — Get NWS grid point for this stadium's coordinates
        points_resp = httpx.get(
            f"{NWS_BASE}/points/{lat},{lon}",
            headers={"User-Agent": "AxiomDataEngine/1.0 (contact@axiom.com)"},
            timeout=TIMEOUT,
        )
        points_resp.raise_for_status()
        forecast_hourly_url = points_resp.json()["properties"]["forecastHourly"]

        # Step 2 — Get hourly forecast from the grid point URL
        forecast_resp = httpx.get(
            forecast_hourly_url,
            headers={"User-Agent": "AxiomDataEngine/1.0"},
            timeout=TIMEOUT,
        )
        forecast_resp.raise_for_status()
        periods = forecast_resp.json()["properties"]["periods"]

    except httpx.TimeoutException:
        log.warning("nfl_weather: NWS request timed out, using neutral",
                    team=abbr, stadium=stadium["name"])
        return _neutral_weather(surface=stadium["surface"])
    except httpx.HTTPStatusError as exc:
        log.warning("nfl_weather: NWS HTTP error, using neutral",
                    team=abbr, status=exc.response.status_code)
        return _neutral_weather(surface=stadium["surface"])
    except Exception as exc:
        log.warning("nfl_weather: NWS unexpected error, using neutral",
                    team=abbr, error=str(exc))
        return _neutral_weather(surface=stadium["surface"])

    period = _find_period_for_date(periods, target_date)
    if not period:
        log.warning("nfl_weather: no forecast period found for date",
                    team=abbr, game_date=game_date)
        return _neutral_weather(surface=stadium["surface"])

    temp_f        = float(period.get("temperature", 72))
    wind_mph      = _parse_wind_speed(period.get("windSpeed", "0 mph"))
    condition     = period.get("shortForecast", "Clear")
    precip_raw    = period.get("probabilityOfPrecipitation", {})
    precip_chance = float(precip_raw.get("value") or 0)

    result = {
        "wind_mph":      wind_mph,
        "temp_f":        temp_f,
        "precip_chance": precip_chance,
        "condition":     condition,
        "is_dome":       False,
        "surface":       stadium["surface"],
        "source":        "NWS",
    }

    log.info("nfl_weather: weather fetched",
             team=abbr,
             stadium=stadium["name"],
             game_date=game_date,
             temp_f=temp_f,
             wind_mph=wind_mph,
             precip_chance=precip_chance,
             condition=condition)

    return result


def get_weather_for_all_games(games: list[dict]) -> dict[str, dict]:
    """
    Fetch weather for every game in the weekly games list.

    Args:
      games: List of game dicts from nfl_schedule.get_all_starters_this_week()[0].
             Each dict must contain: game_id, home_team, game_date.

    Returns a dict keyed by game_id mapping to its weather dict.
    If any individual game fails, logs a WARNING and continues — one bad
    fetch never stops the rest.
    """
    weather_by_game: dict[str, dict] = {}

    for game in games:
        game_id   = game.get("game_id", "unknown")
        home_team = game.get("home_team", "")
        game_date = str(game.get("game_date", ""))

        try:
            weather = get_weather_for_game(home_team, game_date)
            weather_by_game[game_id] = weather
        except Exception as exc:
            log.warning("nfl_weather: failed to fetch weather for game",
                        game_id=game_id, home_team=home_team, error=str(exc))
            stadium = NFL_STADIUMS.get(home_team.upper(), {})
            weather_by_game[game_id] = _neutral_weather(
                is_dome=stadium.get("is_dome", False),
                surface=stadium.get("surface", "unknown"),
            )

    log.info("nfl_weather: week weather fetch complete",
             total_games=len(games),
             fetched=len(weather_by_game))

    return weather_by_game


# ─────────────────────────────────────────────────────────────
# Standalone runner
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    from datetime import date as _date
    today = str(_date.today())
    print(f"\nWeather check for all outdoor stadiums — {today}\n")
    for abbr, stadium in NFL_STADIUMS.items():
        if not stadium["is_dome"]:
            w = get_weather_for_game(abbr, today)
            print(f"  {abbr:4s} {stadium['name']:<35s} "
                  f"{w['temp_f']:5.1f}°F  wind {w['wind_mph']:4.1f} mph  "
                  f"precip {w['precip_chance']:4.0f}%  {w['condition']}")
