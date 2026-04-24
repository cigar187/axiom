"""
app/utils/teams.py — MLB team identity map.

Maps team abbreviations, MLB team IDs, and full names so every
Axiom API response shows the complete team name alongside the
short code. B2B customers and their end users shouldn't have to
look up what "CWS" means.

Usage:
    from app.utils.teams import get_team_name, TEAM_ABBREV_TO_NAME

    get_team_name("LAD")          # "Los Angeles Dodgers"
    get_team_name("119")          # "Los Angeles Dodgers"  (by MLB ID)
    get_team_name("lad")          # "Los Angeles Dodgers"  (case-insensitive)
"""

# Abbreviation → full name (all 30 current MLB teams)
TEAM_ABBREV_TO_NAME: dict[str, str] = {
    "ARI": "Arizona Diamondbacks",
    "ATL": "Atlanta Braves",
    "BAL": "Baltimore Orioles",
    "BOS": "Boston Red Sox",
    "CHC": "Chicago Cubs",
    "CWS": "Chicago White Sox",
    "CIN": "Cincinnati Reds",
    "CLE": "Cleveland Guardians",
    "COL": "Colorado Rockies",
    "DET": "Detroit Tigers",
    "HOU": "Houston Astros",
    "KC":  "Kansas City Royals",
    "LAA": "Los Angeles Angels",
    "LAD": "Los Angeles Dodgers",
    "MIA": "Miami Marlins",
    "MIL": "Milwaukee Brewers",
    "MIN": "Minnesota Twins",
    "NYM": "New York Mets",
    "NYY": "New York Yankees",
    "OAK": "Oakland Athletics",
    "PHI": "Philadelphia Phillies",
    "PIT": "Pittsburgh Pirates",
    "SD":  "San Diego Padres",
    "SF":  "San Francisco Giants",
    "SEA": "Seattle Mariners",
    "STL": "St. Louis Cardinals",
    "TB":  "Tampa Bay Rays",
    "TEX": "Texas Rangers",
    "TOR": "Toronto Blue Jays",
    "WSH": "Washington Nationals",
}

# MLB Stats API team ID → abbreviation
MLB_ID_TO_ABBREV: dict[str, str] = {
    "109": "ARI",
    "144": "ATL",
    "110": "BAL",
    "111": "BOS",
    "112": "CHC",
    "145": "CWS",
    "113": "CIN",
    "114": "CLE",
    "115": "COL",
    "116": "DET",
    "117": "HOU",
    "118": "KC",
    "108": "LAA",
    "119": "LAD",
    "146": "MIA",
    "158": "MIL",
    "142": "MIN",
    "121": "NYM",
    "147": "NYY",
    "133": "OAK",
    "143": "PHI",
    "134": "PIT",
    "135": "SD",
    "137": "SF",
    "136": "SEA",
    "138": "STL",
    "139": "TB",
    "140": "TEX",
    "141": "TOR",
    "120": "WSH",
}


def get_team_name(team_code: str) -> str:
    """
    Return the full team name for any team identifier.
    Accepts abbreviations ("LAD"), MLB IDs ("119"), or lowercase ("lad").
    Returns the input unchanged if not found (safe fallback).
    """
    if not team_code:
        return team_code or ""

    upper = team_code.strip().upper()

    # Direct abbreviation lookup
    if upper in TEAM_ABBREV_TO_NAME:
        return TEAM_ABBREV_TO_NAME[upper]

    # MLB numeric ID lookup
    if team_code.strip().isdigit():
        abbrev = MLB_ID_TO_ABBREV.get(team_code.strip())
        if abbrev:
            return TEAM_ABBREV_TO_NAME.get(abbrev, abbrev)

    # Fallback — return what was passed in
    return team_code


def get_team_abbrev(team_id: str) -> str:
    """Return the abbreviation for an MLB team ID. Returns empty string if unknown."""
    return MLB_ID_TO_ABBREV.get(str(team_id), "")
