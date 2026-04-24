"""
app/utils/manager_profiles.py — Managerial "Leash" profiles (Merlin v2.0).

Controls how the SimulationEngine models a manager's willingness to leave a
starter in the game. Two styles:

  Analytics  — data-driven hook. Hard exit probability at 95 pitches or TTO3.
               The starter's IP ceiling is firmly capped regardless of performance.

  Old_School — gut-feel hook. The manager lets starters work deeper, but there is
               a 20% chance in any simulation run of the pitcher exceeding his
               expected IP by 1 inning — at the cost of rising hit probability
               as the arm wears after 100 pitches.

Lookup key: MLB team_id (string). Falls back to "Analytics" when unknown,
reflecting the league-wide trend toward shorter starts.

Source: 2026 managerial tendencies based on publicly available hook-rate
and pitch-count data from Baseball Reference / Baseball Savant.
"""
from typing import Literal

ManagerStyle = Literal["Analytics", "Old_School"]

# team_id (string) → managerial style
# Updated for the 2026 season. Old_School managers identified by:
#   - allowing starters to exceed 95 pitches at a rate 20%+ above league avg
#   - willingness to deploy starters in the 7th inning with 90+ pitches
_MANAGER_STYLES: dict[str, ManagerStyle] = {
    # ── AL East
    "147": "Analytics",    # New York Yankees — Aaron Boone
    "111": "Analytics",    # Boston Red Sox — Alex Cora
    "110": "Analytics",    # Baltimore Orioles — Brandon Hyde
    "139": "Analytics",    # Tampa Bay Rays — Kevin Cash
    "141": "Analytics",    # Toronto Blue Jays — John Schneider

    # ── AL Central
    "145": "Old_School",   # Chicago White Sox — Will Venable
    "116": "Analytics",    # Detroit Tigers — A.J. Hinch
    "114": "Analytics",    # Cleveland Guardians — Stephen Vogt
    "118": "Analytics",    # Kansas City Royals — Matt Quatraro
    "142": "Analytics",    # Minnesota Twins — Rocco Baldelli

    # ── AL West
    "117": "Analytics",    # Houston Astros — Joe Espada
    "108": "Old_School",   # Los Angeles Angels — Ron Washington
    "133": "Analytics",    # Oakland Athletics — Mark Kotsay
    "136": "Analytics",    # Seattle Mariners — Dan Wilson
    "140": "Old_School",   # Texas Rangers — Bruce Bochy

    # ── NL East
    "144": "Old_School",   # Atlanta Braves — Brian Snitker
    "146": "Analytics",    # Miami Marlins — Skip Schumaker
    "121": "Analytics",    # New York Mets — Carlos Mendoza
    "143": "Analytics",    # Philadelphia Phillies — Rob Thomson
    "120": "Analytics",    # Washington Nationals — Dave Martinez

    # ── NL Central
    "112": "Analytics",    # Chicago Cubs — Craig Counsell
    "113": "Analytics",    # Cincinnati Reds — David Bell
    "158": "Old_School",   # Milwaukee Brewers — Pat Murphy
    "134": "Old_School",   # Pittsburgh Pirates — Derek Shelton
    "138": "Analytics",    # St. Louis Cardinals — Oliver Marmol

    # ── NL West
    "109": "Analytics",    # Arizona Diamondbacks — Torey Lovullo
    "115": "Old_School",   # Colorado Rockies — Bud Black
    "119": "Analytics",    # Los Angeles Dodgers — Dave Roberts
    "135": "Analytics",    # San Diego Padres — Mike Shildt
    "137": "Analytics",    # San Francisco Giants — Bob Melvin
}

_DEFAULT_STYLE: ManagerStyle = "Analytics"


def get_manager_style(team_id: str) -> ManagerStyle:
    """
    Return the manager style for a given MLB team_id.

    Args:
        team_id: MLB numeric team ID as a string (e.g. "147" for Yankees).

    Returns:
        "Analytics" or "Old_School". Defaults to "Analytics" when team not found,
        reflecting the league-wide shift toward data-driven hooks.
    """
    return _MANAGER_STYLES.get(str(team_id), _DEFAULT_STYLE)


def is_analytics_manager(team_id: str) -> bool:
    return get_manager_style(team_id) == "Analytics"


def is_old_school_manager(team_id: str) -> bool:
    return get_manager_style(team_id) == "Old_School"
