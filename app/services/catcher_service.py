"""
app/services/catcher_service.py — SKU #37 Catcher Framing Module.

Primary data source: MLB Stats API schedule endpoint with hydrate=lineups.
  https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={date}&hydrate=lineups

Cross-references the detected catcher ID against data/framing_cache.json
which is built from Baseball Savant's Called Strike Rate leaderboard.

Formula injection rule:
  If catcher's strike_rate > 50.0%, apply a +4% multiplier to KUSI (#28).
  The framing catcher is "stealing" strikes — borderline pitches are being
  called as strikes, giving the pitcher free Ks that the formula wouldn't
  otherwise credit.

  If strike_rate < 48.0% (poor framer), apply -2% to KUSI because
  the catcher is GIVING strikes back — balls that should be called strikes
  are being called balls, reducing strikeout opportunities.
"""
import json
import math
from pathlib import Path
from typing import Optional

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from app.utils.logging import get_logger

log = get_logger("catcher_service")

MLB_BASE = "https://statsapi.mlb.com/api/v1"

# Load the framing cache at module import time — it's a small JSON file.
_CACHE_PATH = Path(__file__).parent.parent.parent / "data" / "framing_cache.json"

try:
    with open(_CACHE_PATH) as f:
        _FRAMING_DATA = json.load(f)
    _CATCHERS = _FRAMING_DATA.get("catchers", {})
    _THRESHOLD = _FRAMING_DATA["_meta"]["threshold"]
    log.info("Framing cache loaded", entries=len(_CATCHERS))
except Exception as e:
    log.warning("Could not load framing_cache.json, defaulting to neutral", error=str(e))
    _CATCHERS = {}
    _THRESHOLD = 50.0

# KUSI adjustment constants
FRAMING_BOOST_PCT = 0.04    # +4% when strike_rate > 50.0% (stealing strikes)
FRAMING_PENALTY_PCT = -0.02  # -2% when strike_rate < 48.0% (giving strikes back)
POOR_FRAMER_THRESHOLD = 48.0


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=8))
async def _get(client: httpx.AsyncClient, path: str, params: dict = None) -> dict:
    url = f"{MLB_BASE}{path}"
    resp = await client.get(url, params=params or {}, timeout=20.0)
    resp.raise_for_status()
    return resp.json()


async def fetch_game_catchers(
    client: httpx.AsyncClient,
    game_id: str,
) -> dict[str, Optional[str]]:
    """
    Fetch the primary catcher for both teams in a given game.

    Returns:
        {
          "home_catcher_id": "672521",   # MLB player ID or None
          "away_catcher_id": "663728",
          "home_catcher_name": "Patrick Bailey",
          "away_catcher_name": "Cal Raleigh",
        }
    """
    try:
        data = await _get(client, f"/game/{game_id}/boxscore")
        teams = data.get("teams", {})
        result = {}
        for side in ("home", "away"):
            team_data = teams.get(side, {})
            # Players is a dict keyed by "ID<playerId>"
            players = team_data.get("players", {})
            catcher_id = None
            catcher_name = None
            for player_key, player_info in players.items():
                pos = player_info.get("position", {})
                if pos.get("abbreviation") == "C" or pos.get("code") == "2":
                    pid = str(player_info.get("person", {}).get("id", ""))
                    if pid:
                        catcher_id = pid
                        catcher_name = player_info.get("person", {}).get("fullName")
                        # Prefer the batter who is listed in the batting order
                        if player_info.get("battingOrder"):
                            break
            result[f"{side}_catcher_id"] = catcher_id
            result[f"{side}_catcher_name"] = catcher_name

        log.debug("Catchers fetched",
                  game_id=game_id,
                  home=result.get("home_catcher_name"),
                  away=result.get("away_catcher_name"))
        return result

    except Exception as exc:
        log.warning("Catcher fetch failed, using neutral", game_id=game_id, error=str(exc))
        return {
            "home_catcher_id": None, "home_catcher_name": None,
            "away_catcher_id": None, "away_catcher_name": None,
        }


def get_framing_data(catcher_id: Optional[str]) -> dict:
    """
    Look up a catcher in the framing cache and return their framing data.

    Returns a dict with:
        strike_rate: float (raw %, e.g. 51.8)
        tier: str (ELITE / ABOVE_AVG / AVG / BELOW_AVG / POOR)
        kusi_adjustment: float (signed multiplier, e.g. +0.04 or -0.02)
        framing_label: str (human-readable for API / logging)
        in_cache: bool
    """
    if not catcher_id:
        return _neutral_framing("NO_CATCHER")

    cid = str(catcher_id)
    if cid not in _CATCHERS:
        return _neutral_framing("NOT_IN_CACHE")

    entry = _CATCHERS[cid]
    rate = entry["strike_rate"]
    tier = entry.get("tier", "AVG")
    name = entry.get("name", cid)

    if rate > _THRESHOLD:
        adj = FRAMING_BOOST_PCT
        label = f"{name} (ELITE FRAMER: {rate:.1f}%)"
    elif rate < POOR_FRAMER_THRESHOLD:
        adj = FRAMING_PENALTY_PCT
        label = f"{name} (POOR FRAMER: {rate:.1f}%)"
    else:
        adj = 0.0
        label = f"{name} (AVG FRAMER: {rate:.1f}%)"

    log.info("Framing data found",
             catcher=name, rate=rate, tier=tier, kusi_adj=adj)

    return {
        "catcher_id": cid,
        "catcher_name": name,
        "strike_rate": rate,
        "tier": tier,
        "kusi_adjustment": adj,
        "framing_label": label,
        "in_cache": True,
    }


def compute_framing_score(catcher_id: Optional[str]) -> float:
    """
    Returns a 0-100 framing score for use in the DSC block (dsc_catch field).
    Keeps backward compatibility with the existing DSC scoring block.

    League average → 50.  Elite framers → 75-85. Poor framers → 25-40.
    """
    if not catcher_id:
        return 50.0

    cid = str(catcher_id)
    if cid not in _CATCHERS:
        return 50.0

    rate = _CATCHERS[cid]["strike_rate"]
    # Linear map: 48% → 25, 50% → 50, 52.5% → 85 (capped 0-100)
    score = 50.0 + (rate - 50.0) * 14.0
    return max(0.0, min(100.0, round(score, 2)))


def _neutral_framing(reason: str) -> dict:
    return {
        "catcher_id": None,
        "catcher_name": None,
        "strike_rate": 50.0,
        "tier": "AVG",
        "kusi_adjustment": 0.0,
        "framing_label": f"NEUTRAL ({reason})",
        "in_cache": False,
    }
