"""
nfl_props.py — The Rundown API adapter for NFL QB player props.

Fetches passing yards and touchdown prop lines for NFL quarterbacks.
  - Market MARKET_PASSING_YARDS = player_passing_yards over/under
  - Market MARKET_PASSING_TDS   = player_passing_tds over/under

Sportsbooks requested: affiliate_ids=3,6,19,21,22,23,24
Main line only: main_line=true

API key: loaded from environment (RUNDOWN_API_KEY)
Base URL: loaded from environment / config (RUNDOWN_BASE_URL)

NOTE: Verify NFL market IDs against the live Rundown API before first run:
  GET {RUNDOWN_BASE_URL}/sports/{NFL_SPORT_ID}/markets
  Update MARKET_PASSING_YARDS and MARKET_PASSING_TDS if the IDs differ.
"""
import difflib
from typing import Optional

import httpx

from app.config import settings
from app.utils.logging import get_logger

log = get_logger("nfl_props")

# ─────────────────────────────────────────────────────────────
# Rundown sport and market IDs — NFL
# ─────────────────────────────────────────────────────────────
NFL_SPORT_ID       = 2     # NFL on The Rundown (MLB = 3 for reference)
MARKET_PASSING_YARDS = 143  # player_passing_yards — verify at /sports/2/markets
MARKET_PASSING_TDS   = 144  # player_passing_tds   — verify at /sports/2/markets

AFFILIATE_IDS = "3,6,19,21,22,23,24"

AFFILIATE_NAMES = {
    3:  "DraftKings",
    6:  "FanDuel",
    19: "BetMGM",
    21: "Caesars",
    22: "PointsBet",
    23: "Unibet",
    24: "Barstool",
}

TIMEOUT = 10.0  # seconds, matching all other NFL services

# Minimum fuzzy match ratio (0.0–1.0) to accept a name match
_FUZZY_THRESHOLD = 0.82


# ─────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────

def _headers() -> dict:
    """Return the Rundown API auth header. Mirrors RundownAdapter._headers()."""
    return {"x-therundown-key": settings.RUNDOWN_API_KEY}


def _fetch_market(game_date: str, market_id: int) -> list[dict]:
    """
    Call one Rundown market endpoint and return the raw event list.
    Returns empty list on any HTTP or parse failure.
    """
    url    = f"{settings.RUNDOWN_BASE_URL}/sports/{NFL_SPORT_ID}/events/{game_date}/props"
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
        log.error("nfl_props: request timed out", market_id=market_id, url=url)
        return []
    except httpx.HTTPStatusError as exc:
        log.error("nfl_props: HTTP error",
                  market_id=market_id, status=exc.response.status_code)
        return []
    except Exception as exc:
        log.error("nfl_props: unexpected error", market_id=market_id, error=str(exc))
        return []

    # Rundown uses prop_markets or events depending on the endpoint version
    events = data.get("prop_markets", data.get("events", data.get("data", [])))
    return events if isinstance(events, list) else []


def _implied_prob(american_odds: Optional[int]) -> Optional[float]:
    """
    Convert American odds integer to implied probability (0.0–1.0).
    Returns None if odds are missing or zero.
    """
    if american_odds is None or american_odds == 0:
        return None
    if american_odds < 0:
        return abs(american_odds) / (abs(american_odds) + 100)
    return 100 / (american_odds + 100)


def _parse_events(events: list[dict], market_label: str) -> list[dict]:
    """
    Walk a raw Rundown event list and extract one prop dict per player.
    Takes the first affiliate in affiliate_ids priority order with a valid line.

    Returns a list of dicts with keys:
      qb_name, team, market, line, over_odds, under_odds, implied_prob_under
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
                        "qb_name":          name_raw,
                        "team":             team,
                        "market":           market_label,
                        "line":             float(total),
                        "over_odds":        int(over_raw)  if over_raw  is not None else None,
                        "under_odds":       int(under_raw) if under_raw is not None else None,
                        "implied_prob_under": _implied_prob(
                            int(under_raw) if under_raw is not None else None
                        ),
                    }
                    break  # first valid line in this affiliate
                if chosen:
                    break  # first affiliate with a valid line wins

            if chosen:
                log.debug("nfl_props: prop parsed",
                          qb=name_raw, market=market_label, line=chosen["line"])
                results.append(chosen)

    return results


def _normalize_name(name: str) -> str:
    """Lowercase and strip punctuation for fuzzy comparison."""
    return name.lower().replace(".", "").replace("-", " ").strip()


def _fuzzy_match(target: str, candidates: list[str]) -> Optional[str]:
    """
    Return the best fuzzy match from candidates for target, or None
    if no match clears the threshold.
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

def get_nfl_qb_props(game_date: str) -> list[dict]:
    """
    Fetch passing yards and touchdown prop lines for all NFL QBs on a given date.

    Args:
      game_date: Date string in "YYYY-MM-DD" format.

    Returns a list of prop dicts, each containing:
      qb_name           — player name string (as returned by The Rundown)
      team              — team abbreviation
      market            — "passing_yards" or "touchdowns"
      line              — over/under number as float
      over_odds         — American odds integer
      under_odds        — American odds integer
      implied_prob_under — float 0.0–1.0 (probability of under hitting)

    Returns empty list if the API call fails for both markets.
    """
    log.info("nfl_props: fetching QB props", date=game_date)

    all_props: list[dict] = []

    for market_id, market_label in [
        (MARKET_PASSING_YARDS, "passing_yards"),
        (MARKET_PASSING_TDS,   "touchdowns"),
    ]:
        events = _fetch_market(game_date, market_id)
        parsed = _parse_events(events, market_label)
        log.info("nfl_props: market fetched",
                 market=market_label, market_id=market_id, props_found=len(parsed))
        all_props.extend(parsed)

    log.info("nfl_props: total props found", date=game_date, total=len(all_props))
    return all_props


def match_props_to_starters(
    props: list[dict],
    starters: list[dict],
) -> list[dict]:
    """
    Attach prop lines to starter dicts by fuzzy name match.

    Args:
      props:    Output of get_nfl_qb_props() — one dict per market per QB.
      starters: Output of nfl_schedule.get_all_starters_this_week()[1] — one dict per QB.

    For each starter, finds matching props from both markets (passing_yards and
    touchdowns) and attaches them. Name matching is fuzzy (threshold 0.82) to
    handle format differences between ESPN and The Rundown.

    Each enriched starter dict gains these fields:
      prop_passing_yards_line     — float or None
      prop_passing_yards_over     — int or None
      prop_passing_yards_under    — int or None
      prop_passing_yards_imp_prob — float or None
      prop_td_line                — float or None
      prop_td_over                — int or None
      prop_td_under               — int or None
      prop_td_imp_prob            — float or None

    Starters with no prop match are kept in the list with all prop fields set to None.
    Returns the enriched starters list (same order, same length as input).
    """
    # Index props by (market, normalized_name) for fast lookup
    yards_props: dict[str, dict] = {}
    td_props:    dict[str, dict] = {}

    for p in props:
        key = _normalize_name(p["qb_name"])
        if p["market"] == "passing_yards":
            yards_props[key] = p
        elif p["market"] == "touchdowns":
            td_props[key] = p

    yards_names = list(yards_props.keys())
    td_names    = list(td_props.keys())

    enriched: list[dict] = []

    for starter in starters:
        row = dict(starter)  # copy — do not mutate the input

        # ── Passing yards prop ──
        yards_match = _fuzzy_match(row["qb_name"], yards_names)
        if yards_match:
            yp = yards_props[_normalize_name(yards_match)]
            row["prop_passing_yards_line"]     = yp["line"]
            row["prop_passing_yards_over"]     = yp["over_odds"]
            row["prop_passing_yards_under"]    = yp["under_odds"]
            row["prop_passing_yards_imp_prob"] = yp["implied_prob_under"]
        else:
            row["prop_passing_yards_line"]     = None
            row["prop_passing_yards_over"]     = None
            row["prop_passing_yards_under"]    = None
            row["prop_passing_yards_imp_prob"] = None

        # ── Touchdown prop ──
        td_match = _fuzzy_match(row["qb_name"], td_names)
        if td_match:
            tp = td_props[_normalize_name(td_match)]
            row["prop_td_line"]     = tp["line"]
            row["prop_td_over"]     = tp["over_odds"]
            row["prop_td_under"]    = tp["under_odds"]
            row["prop_td_imp_prob"] = tp["implied_prob_under"]
        else:
            row["prop_td_line"]     = None
            row["prop_td_over"]     = None
            row["prop_td_under"]    = None
            row["prop_td_imp_prob"] = None

        # Warn on any missing props so the pipeline log makes it obvious
        missing = []
        if row["prop_passing_yards_line"] is None:
            missing.append("passing_yards")
        if row["prop_td_line"] is None:
            missing.append("touchdowns")

        if missing:
            log.warning("nfl_props: no prop match for starter",
                        qb=row["qb_name"], team=row["team"], missing_markets=missing)

        enriched.append(row)

    log.info("nfl_props: props matched to starters",
             total_starters=len(enriched),
             matched_yards=sum(1 for r in enriched if r["prop_passing_yards_line"] is not None),
             matched_tds=sum(1 for r in enriched if r["prop_td_line"] is not None))

    return enriched
