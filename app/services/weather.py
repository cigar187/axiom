"""
weather.py — National Weather Service (NWS) environmental data fetcher.

Data source: api.weather.gov (US government, free, no API key, no licensing fees)

Strategy:
  - All 30 MLB stadiums are pre-mapped to lat/lng coordinates.
  - Two hours before first pitch, we ping the NWS hourly forecast endpoint.
  - We extract temperature, wind speed, wind direction, humidity, and
    barometric pressure (for air density calculation).
  - All data is stored in the games table and used to feed the ENS block
    of the HUSI formula.

Air Density Note:
  Air density directly affects how far a baseball travels.
  Thin air (high altitude, hot/dry) = more carry = more hits/HRs.
  Dense air (cold, humid, sea level) = suppressed ball flight = fewer hits.
  Formula: ρ = (P × M) / (R × T) — simplified to a 0-100 score.

Usage:
  Called automatically by the pipeline 2hrs before first pitch.
  Can also be called standalone: python -m app.services.weather
"""
from datetime import date, datetime
from typing import Optional

import httpx

from app.utils.logging import get_logger

log = get_logger("weather")

NWS_BASE = "https://api.weather.gov"

# ─────────────────────────────────────────────────────────────
# All 30 MLB stadiums — lat/lng + altitude (feet above sea level)
# ─────────────────────────────────────────────────────────────
STADIUMS: dict[str, dict] = {
    # Team abbrev → stadium data
    "ARI": {"name": "Chase Field",              "lat": 33.4453, "lon": -112.0667, "alt_ft": 1082, "dome": True},
    "ATL": {"name": "Truist Park",              "lat": 33.8908, "lon": -84.4678,  "alt_ft": 1050, "dome": False},
    "BAL": {"name": "Oriole Park at Camden",    "lat": 39.2839, "lon": -76.6218,  "alt_ft": 20,   "dome": False},
    "BOS": {"name": "Fenway Park",              "lat": 42.3467, "lon": -71.0972,  "alt_ft": 20,   "dome": False},
    "CHC": {"name": "Wrigley Field",            "lat": 41.9484, "lon": -87.6553,  "alt_ft": 595,  "dome": False},
    "CWS": {"name": "Guaranteed Rate Field",    "lat": 41.8300, "lon": -87.6339,  "alt_ft": 595,  "dome": False},
    "CIN": {"name": "Great American Ball Park", "lat": 39.0975, "lon": -84.5075,  "alt_ft": 490,  "dome": False},
    "CLE": {"name": "Progressive Field",        "lat": 41.4962, "lon": -81.6852,  "alt_ft": 653,  "dome": False},
    "COL": {"name": "Coors Field",              "lat": 39.7559, "lon": -104.9942, "alt_ft": 5200, "dome": False},
    "DET": {"name": "Comerica Park",            "lat": 42.3390, "lon": -83.0485,  "alt_ft": 585,  "dome": False},
    "HOU": {"name": "Minute Maid Park",         "lat": 29.7573, "lon": -95.3555,  "alt_ft": 43,   "dome": True},
    "KC":  {"name": "Kauffman Stadium",         "lat": 39.0517, "lon": -94.4803,  "alt_ft": 750,  "dome": False},
    "LAA": {"name": "Angel Stadium",            "lat": 33.8003, "lon": -117.8827, "alt_ft": 150,  "dome": False},
    "LAD": {"name": "Dodger Stadium",           "lat": 34.0739, "lon": -118.2400, "alt_ft": 510,  "dome": False},
    "MIA": {"name": "loanDepot Park",           "lat": 25.7781, "lon": -80.2196,  "alt_ft": 10,   "dome": True},
    "MIL": {"name": "American Family Field",    "lat": 43.0280, "lon": -87.9712,  "alt_ft": 635,  "dome": True},
    "MIN": {"name": "Target Field",             "lat": 44.9817, "lon": -93.2781,  "alt_ft": 841,  "dome": False},
    "NYM": {"name": "Citi Field",               "lat": 40.7571, "lon": -73.8458,  "alt_ft": 20,   "dome": False},
    "NYY": {"name": "Yankee Stadium",           "lat": 40.8296, "lon": -73.9262,  "alt_ft": 55,   "dome": False},
    "OAK": {"name": "Oakland Coliseum",         "lat": 37.7516, "lon": -122.2005, "alt_ft": 25,   "dome": False},
    "PHI": {"name": "Citizens Bank Park",       "lat": 39.9061, "lon": -75.1665,  "alt_ft": 20,   "dome": False},
    "PIT": {"name": "PNC Park",                 "lat": 40.4468, "lon": -80.0057,  "alt_ft": 730,  "dome": False},
    "SD":  {"name": "Petco Park",               "lat": 32.7073, "lon": -117.1566, "alt_ft": 20,   "dome": False},
    "SF":  {"name": "Oracle Park",              "lat": 37.7786, "lon": -122.3893, "alt_ft": 10,   "dome": False},
    "SEA": {"name": "T-Mobile Park",            "lat": 47.5914, "lon": -122.3325, "alt_ft": 15,   "dome": True},
    "STL": {"name": "Busch Stadium",            "lat": 38.6226, "lon": -90.1928,  "alt_ft": 465,  "dome": False},
    "TB":  {"name": "Tropicana Field",          "lat": 27.7683, "lon": -82.6534,  "alt_ft": 15,   "dome": True},
    "TEX": {"name": "Globe Life Field",         "lat": 32.7473, "lon": -97.0823,  "alt_ft": 551,  "dome": True},
    "TOR": {"name": "Rogers Centre",            "lat": 43.6414, "lon": -79.3894,  "alt_ft": 250,  "dome": True},
    "WSH": {"name": "Nationals Park",           "lat": 38.8730, "lon": -77.0074,  "alt_ft": 25,   "dome": False},
}

# ─────────────────────────────────────────────────────────────
# NWS API fetcher
# ─────────────────────────────────────────────────────────────

async def fetch_game_weather(
    client: httpx.AsyncClient,
    team_abbrev: str,
    game_datetime: Optional[datetime] = None,
) -> dict:
    """
    Fetch current/forecast weather for an MLB stadium.

    Returns a normalized weather dict ready to populate the games table:
    {
        "temperature_f": float,
        "wind_speed_mph": float,
        "wind_direction": str,       # "N", "SW", "Out to Left", etc.
        "humidity_pct": float,
        "pressure_mb": float,
        "air_density_score": float,  # 0-100 (100 = very thin air, bad for pitchers)
        "condition": str,
        "is_dome": bool,
        "source": "NWS",
    }
    """
    stadium = STADIUMS.get(team_abbrev.upper())
    if not stadium:
        log.warning("Stadium not found", team=team_abbrev)
        return _neutral_weather(team_abbrev)

    # Domes get a fixed neutral environment — weather is irrelevant
    if stadium["dome"]:
        log.info("Dome stadium — using fixed environment", team=team_abbrev, stadium=stadium["name"])
        return {
            "temperature_f": 72.0,
            "wind_speed_mph": 0.0,
            "wind_direction": "None",
            "humidity_pct": 50.0,
            "pressure_mb": 1013.25,
            "air_density_score": 50.0,
            "condition": "Dome",
            "is_dome": True,
            "source": "fixed",
        }

    lat = stadium["lat"]
    lon = stadium["lon"]
    alt_ft = stadium["alt_ft"]

    try:
        # Step 1: Get NWS grid point for this location
        points_resp = await client.get(
            f"{NWS_BASE}/points/{lat},{lon}",
            headers={"User-Agent": "AxiomDataEngine/1.0 (contact@axiom.com)"},
            timeout=15.0,
        )
        points_resp.raise_for_status()
        points_data = points_resp.json()

        forecast_hourly_url = points_data["properties"]["forecastHourly"]

        # Step 2: Get hourly forecast
        forecast_resp = await client.get(
            forecast_hourly_url,
            headers={"User-Agent": "AxiomDataEngine/1.0"},
            timeout=15.0,
        )
        forecast_resp.raise_for_status()
        forecast_data = forecast_resp.json()

        # Step 3: Find the period closest to game time (default: next available)
        periods = forecast_data["properties"]["periods"]
        period = periods[0] if periods else None

        if not period:
            return _neutral_weather(team_abbrev)

        temp_f = period.get("temperature", 72)
        wind_str = period.get("windSpeed", "0 mph")
        wind_mph = _parse_wind_speed(wind_str)
        wind_dir = period.get("windDirection", "Calm")
        condition = period.get("shortForecast", "Clear")

        # Humidity from dewpoint if available
        humidity_pct = 50.0
        dewpoint = period.get("dewpoint", {})
        if dewpoint.get("value") is not None:
            humidity_pct = _estimate_humidity(temp_f, dewpoint["value"])

        # Air density score (0-100, 100 = thinnest air = most carry = most hits)
        pressure_mb = 1013.25 - (alt_ft * 0.0295)  # approx pressure at altitude
        air_density_score = _compute_air_density_score(temp_f, humidity_pct, pressure_mb)

        result = {
            "temperature_f": float(temp_f),
            "wind_speed_mph": float(wind_mph),
            "wind_direction": wind_dir,
            "humidity_pct": float(humidity_pct),
            "pressure_mb": round(pressure_mb, 1),
            "air_density_score": round(air_density_score, 1),
            "condition": condition,
            "is_dome": False,
            "source": "NWS",
        }

        log.info("Weather fetched",
                 team=team_abbrev,
                 stadium=stadium["name"],
                 temp_f=temp_f,
                 wind_mph=wind_mph,
                 wind_dir=wind_dir,
                 air_density=round(air_density_score, 1),
                 condition=condition)

        return result

    except Exception as exc:
        log.warning("NWS fetch failed, using neutral", team=team_abbrev, error=str(exc))
        return _neutral_weather(team_abbrev)


# ─────────────────────────────────────────────────────────────
# Air density scoring
# ─────────────────────────────────────────────────────────────

def _compute_air_density_score(temp_f: float, humidity_pct: float, pressure_mb: float) -> float:
    """
    Compute a 0-100 air density score.
    100 = very thin air (hot + high altitude + dry) = ball carries far = more hits
    0   = very dense air (cold + sea level + humid) = ball dies = fewer hits
    50  = league average conditions

    Simplified model — full formula: ρ = (P × Md) / (R × T)
    We use relative scoring vs. a standard atmosphere.
    """
    # Standard atmosphere baseline
    std_temp_f = 70.0
    std_pressure_mb = 1013.25
    std_humidity = 50.0

    # Temperature effect: every 10°F hotter = ~1% less dense = slightly thinner
    temp_factor = (temp_f - std_temp_f) / 10.0 * 0.01

    # Pressure/altitude effect: every 100mb lower = ~10% less dense = much thinner
    pressure_factor = (std_pressure_mb - pressure_mb) / 100.0 * 0.10

    # Humidity effect: humid air is slightly less dense than dry air
    humidity_factor = (humidity_pct - std_humidity) / 100.0 * 0.005

    total_factor = temp_factor + pressure_factor + humidity_factor

    # Convert to 0-100 score (centered at 50)
    # A ±0.20 factor range maps to 0-100
    score = 50.0 + (total_factor / 0.20) * 50.0
    return max(0.0, min(100.0, score))


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def _parse_wind_speed(wind_str: str) -> float:
    """Parse NWS wind speed string like '12 mph' or '5 to 15 mph'."""
    try:
        parts = wind_str.lower().replace("mph", "").strip().split()
        numbers = [float(p) for p in parts if p.replace(".", "").isdigit()]
        return sum(numbers) / len(numbers) if numbers else 0.0
    except Exception:
        return 0.0


def _estimate_humidity(temp_f: float, dewpoint_c: float) -> float:
    """Estimate relative humidity from temperature and dewpoint."""
    try:
        temp_c = (temp_f - 32) * 5 / 9
        rh = 100 - 5 * (temp_c - dewpoint_c)
        return max(0.0, min(100.0, rh))
    except Exception:
        return 50.0


def _neutral_weather(team_abbrev: str) -> dict:
    return {
        "temperature_f": 72.0,
        "wind_speed_mph": 5.0,
        "wind_direction": "Calm",
        "humidity_pct": 50.0,
        "pressure_mb": 1013.25,
        "air_density_score": 50.0,
        "condition": "Unknown",
        "is_dome": False,
        "source": "neutral_fallback",
    }


def get_stadium(team_abbrev: str) -> Optional[dict]:
    """Public accessor for stadium metadata."""
    return STADIUMS.get(team_abbrev.upper())
