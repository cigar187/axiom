"""
nhl_props.py — The Rundown API adapter for NHL player props.

Fetches prop lines for NHL skaters and goalies across five markets:
  - player_points      (skater)
  - player_goals       (skater)
  - player_assists     (skater)
  - player_shots_on_goal (skater)
  - player_shots_faced (goalie)

Sportsbooks requested: affiliate_ids=3,6,19,21,22,23,24
Main line only: main_line=true

API key: loaded from environment (RUNDOWN_API_KEY)
Base URL: loaded from environment / config (RUNDOWN_BASE_URL)

NOTE: Verify NHL market IDs against the live Rundown API before first NHL prop run:
  GET {RUNDOWN_BASE_URL}/sports/{NHL_SPORT_ID}/markets
  Update all MARKET_* constants below if the IDs differ from these placeholder values.
"""
import difflib
from typing import Optional

import httpx

from app.config import settings
from app.utils.logging import get_logger

log = get_logger("nhl_props")

# ─────────────────────────────────────────────────────────────
# Rundown sport and market IDs — NHL
# NOTE: These market IDs are placeholder estimates.
#       Run GET {RUNDOWN_BASE_URL}/sports/{NHL_SPORT_ID}/markets
#       against the live Rundown API when NHL lines are active and update below.
# ─────────────────────────────────────────────────────────────
NHL_SPORT_ID         = 7    # NHL on The Rundown — verify before first run
MARKET_POINTS        = 180  # player_points      — verify at /sports/7/markets
MARKET_GOALS         = 181  # player_goals        — verify at /sports/7/markets
MARKET_ASSISTS       = 182  # player_assists      — verify at /sports/7/markets
MARKET_SHOTS_ON_GOAL = 183  # player_shots_on_goal — verify at /sports/7/markets
MARKET_SHOTS_FACED   = 184  # player_shots_faced (goalie) — verify at /sports/7/markets

AFFILIATE_IDS = "3,6,19,21,22,23,24"

TIMEOUT = 10.0  # seconds

# Minimum fuzzy-match ratio to accept a name match (matches nfl_props.py)
_FUZZY_THRESHOLD = 0.82


# ─────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────

def _headers() -> dict:
    return {"x-therundown-key": settings.RUNDOWN_API_KEY}


def _fetch_market(game_date: str, market_id: int) -> list[dict]:
    """
    Call one Rundown prop endpoint and return the raw event list.
    Returns empty list on any HTTP or parse failure.
    """
    url = f"{settings.RUNDOWN_BASE_URL}/sports/{NHL_SPORT_ID}/events/{game_date}/props"
    params = {
        "market_id":     market_id,
        "affiliate_ids": AFFILIATE_IDS,
        "main_line":     "true",
        "include":       "scores",
    }
    try:
        resp = httpx.get(url, headers=_headers(), params=params, timeout=TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
    except httpx.TimeoutException:
        log.error("nhl_props: request timed out", market_id=market_id, url=url)
        return []
    except httpx.HTTPStatusError as exc:
        log.error("nhl_props: HTTP error",
                  market_id=market_id, status=exc.response.status_code)
        return []
    except Exception as exc:
        log.error("nhl_props: unexpected error", market_id=market_id, error=str(exc))
        return []

    events = data.get("prop_markets", data.get("events", data.get("data", [])))
    return events if isinstance(events, list) else []


def _parse_events(events: list[dict], market_label: str) -> list[dict]:
    """
    Walk a raw Rundown event list and extract one prop dict per player.
    Takes the first affiliate with a valid line (priority: affiliate_ids order).

    Returns list of dicts with keys:
      player_name, team, market, line, over_odds, under_odds
    """
    results: list[dict] = []

    for event in events:
        for participant in event.get("participants", []):
            name_raw = participant.get("name", "").strip()
            if not name_raw:
                continue

            team = (
                participant.get("team", {}).get("abbreviation", "")
                or participant.get("team_abbr", "")
            )

            affiliate_props = participant.get(
                "affiliate_props", participant.get("props", [])
            )
            if not affiliate_props:
                continue

            chosen: Optional[dict] = None
            for aff in affiliate_props:
                for line in aff.get("lines", []):
                    total = line.get("total")
                    if total is None:
                        continue
                    over_raw  = line.get("over",  {}).get("decimal") or line.get("over_odds")
                    under_raw = line.get("under", {}).get("decimal") or line.get("under_odds")
                    chosen = {
                        "player_name": name_raw,
                        "team":        team,
                        "market":      market_label,
                        "line":        float(total),
                        "over_odds":   int(over_raw)  if over_raw  is not None else None,
                        "under_odds":  int(under_raw) if under_raw is not None else None,
                    }
                    break  # first valid line in this affiliate
                if chosen:
                    break  # first affiliate with a valid line wins

            if chosen:
                log.debug("nhl_props: prop parsed",
                          player=name_raw, market=market_label, line=chosen["line"])
                results.append(chosen)

    return results


def _normalize_name(name: str) -> str:
    """Lowercase and strip punctuation for fuzzy comparison."""
    return name.lower().replace(".", "").replace("-", " ").strip()


def _fuzzy_match(target: str, candidates: list[str]) -> Optional[str]:
    """
    Return the best fuzzy match from candidates for target, or None
    if no candidate clears the threshold.
    """
    norm_target     = _normalize_name(target)
    norm_candidates = [_normalize_name(c) for c in candidates]
    matches = difflib.get_close_matches(
        norm_target, norm_candidates, n=1, cutoff=_FUZZY_THRESHOLD
    )
    if not matches:
        return None
    idx = norm_candidates.index(matches[0])
    return candidates[idx]


# ─────────────────────────────────────────────────────────────
# Public functions
# ─────────────────────────────────────────────────────────────

def get_nhl_player_props(game_date: str) -> list[dict]:
    """
    Fetch all NHL player prop lines for a given date from The Rundown.

    Args:
      game_date: Date string in "YYYY-MM-DD" format.

    Returns a list of prop dicts, each containing:
      player_name  — player name as returned by The Rundown
      team         — team abbreviation
      market       — "points" | "goals" | "assists" | "shots_on_goal" | "shots_faced"
      line         — over/under number as float
      over_odds    — American odds integer or None
      under_odds   — American odds integer or None

    Returns empty list if all market fetches fail.
    """
    log.info("nhl_props: fetching player props", date=game_date)

    all_props: list[dict] = []

    for market_id, market_label in [
        (MARKET_POINTS,        "points"),
        (MARKET_GOALS,         "goals"),
        (MARKET_ASSISTS,       "assists"),
        (MARKET_SHOTS_ON_GOAL, "shots_on_goal"),
        (MARKET_SHOTS_FACED,   "shots_faced"),
    ]:
        events = _fetch_market(game_date, market_id)
        parsed = _parse_events(events, market_label)
        log.info("nhl_props: market fetched",
                 market=market_label, market_id=market_id, props_found=len(parsed))
        all_props.extend(parsed)

    log.info("nhl_props: total props found", date=game_date, total=len(all_props))
    return all_props


def build_props_lookup(props: list[dict]) -> dict[str, dict[str, dict]]:
    """
    Index props for fast lookup by normalized player name, then market.

    Returns:
      { normalized_name: { market: prop_dict } }
    """
    lookup: dict[str, dict[str, dict]] = {}
    for prop in props:
        key = _normalize_name(prop["player_name"])
        if key not in lookup:
            lookup[key] = {}
        lookup[key][prop["market"]] = prop
    return lookup


def lookup_prop(
    player_name: str,
    market: str,
    props_lookup: dict[str, dict[str, dict]],
) -> Optional[dict]:
    """
    Fuzzy-match player_name against the props lookup and return the prop dict
    for the requested market, or None if no match is found.
    """
    candidates = list(props_lookup.keys())
    if not candidates:
        return None
    best = _fuzzy_match(player_name, candidates)
    if best is None:
        return None
    return props_lookup[best].get(market)
