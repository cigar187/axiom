"""
The Rundown API adapter — real working implementation.

Fetches pitcher prop lines for:
  - Market 19 = Strikeouts
  - Market 47 = Hits Allowed

Sportsbooks requested: affiliate_ids=3,6,19,21,22,23,24
Main line only: main_line=true

API key: loaded from environment (RUNDOWN_API_KEY)
"""
from datetime import date
from typing import Any, Optional

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from app.config import settings
from app.services.base import BaseProvider
from app.utils.logging import get_logger

log = get_logger("rundown")

# Market IDs
MARKET_STRIKEOUTS = 19
MARKET_HITS_ALLOWED = 47

# Sportsbook affiliate IDs to pull from
AFFILIATE_IDS = "3,6,19,21,22,23,24"

# Human-readable names for affiliate IDs (for reference/logging)
AFFILIATE_NAMES = {
    3: "DraftKings",
    6: "FanDuel",
    19: "BetMGM",
    21: "Caesars",
    22: "PointsBet",
    23: "Unibet",
    24: "Barstool",
}

# Sport ID for MLB on The Rundown
MLB_SPORT_ID = 3


class RundownAdapter(BaseProvider):
    """
    Fetches pitcher prop lines from The Rundown API.
    Returns a dict keyed by pitcher name (lowercase) containing their prop lines.
    """

    @property
    def name(self) -> str:
        return "The Rundown API"

    def _headers(self) -> dict:
        return {
            "x-therundown-key": settings.RUNDOWN_API_KEY,
        }

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=8))
    async def _get(self, client: httpx.AsyncClient, path: str, params: dict = None) -> dict:
        url = f"{settings.RUNDOWN_BASE_URL}{path}"
        log.debug("Rundown API request", url=url, params=params)
        resp = await client.get(url, headers=self._headers(), params=params or {}, timeout=20.0)
        resp.raise_for_status()
        return resp.json()

    async def fetch(self, target_date: date) -> dict:
        """
        Fetch all pitcher props for the given date from The Rundown.

        Returns:
        {
          "props": {
            "<pitcher_name_lower>": {
              "strikeouts": {
                "line": float,
                "over_odds": float,
                "under_odds": float,
                "sportsbook": str,
              },
              "hits_allowed": {
                "line": float,
                "over_odds": float,
                "under_odds": float,
                "sportsbook": str,
              }
            }
          }
        }
        """
        date_str = target_date.strftime("%Y-%m-%d")
        log.info("Rundown API fetch starting", date=date_str)

        props: dict[str, dict] = {}

        async with httpx.AsyncClient() as client:
            for market_id, market_name in [
                (MARKET_STRIKEOUTS, "strikeouts"),
                (MARKET_HITS_ALLOWED, "hits_allowed"),
            ]:
                try:
                    raw = await self._fetch_market(client, date_str, market_id)
                    self._parse_props(raw, market_name, props)
                    log.info("Rundown market fetched",
                             market=market_name, market_id=market_id,
                             entries=len(props))
                except Exception as exc:
                    log.error("Rundown market fetch failed",
                              market=market_name, market_id=market_id, error=str(exc))

        log.info("Rundown API fetch complete", date=date_str, pitchers_with_props=len(props))
        return {"props": props}

    async def _fetch_market(
        self,
        client: httpx.AsyncClient,
        date_str: str,
        market_id: int,
    ) -> dict:
        """Fetch all prop events for a single market on a given date."""
        return await self._get(
            client,
            f"/sports/{MLB_SPORT_ID}/events/{date_str}/props",
            params={
                "market_id": market_id,
                "affiliate_ids": AFFILIATE_IDS,
                "main_line": "true",
                "include": "scores",
            },
        )

    def _parse_props(
        self,
        raw: dict,
        market_name: str,
        props: dict,
    ) -> None:
        """
        Parse raw Rundown response into the normalized props dict.
        We take the first available sportsbook line per pitcher (priority order
        follows the affiliate_ids list).
        """
        events = raw.get("prop_markets", raw.get("events", []))
        if not events:
            # Try alternate response shape
            events = raw.get("data", [])

        for event in events:
            participants = event.get("participants", [])
            for participant in participants:
                name_raw = participant.get("name", "")
                if not name_raw:
                    continue
                name_key = name_raw.strip().lower()

                affiliate_props = participant.get("affiliate_props", participant.get("props", []))
                if not affiliate_props:
                    continue

                for aff in affiliate_props:
                    aff_name = (
                        aff.get("affiliate", {}).get("affiliate_name", "")
                        or aff.get("sportsbook", "unknown")
                    )
                    for line in aff.get("lines", []):
                        val = line.get("total")
                        if val is None:
                            continue
                        if name_key not in props:
                            props[name_key] = {}
                        props[name_key][market_name] = {
                            "line": float(val),
                            "over_odds": line.get("over", {}).get("decimal") or line.get("over_odds"),
                            "under_odds": line.get("under", {}).get("decimal") or line.get("under_odds"),
                            "sportsbook": aff_name,
                        }
                        log.debug("Prop parsed",
                                  pitcher=name_key, market=market_name,
                                  line=float(val),
                                  sportsbook=aff_name)
                        break  # first valid line in this affiliate
                    if name_key in props and market_name in props[name_key]:
                        break  # first affiliate with a valid line wins

    @staticmethod
    def _pick_best_line(lines: list[dict]) -> Optional[dict]:
        """
        Return the first non-null line from the list.
        Lines come back ordered by affiliate priority.
        """
        for line in lines:
            if line.get("line") is not None:
                return line
        return None

    async def fetch_game_lines(self, target_date: date, sport_id: int = MLB_SPORT_ID) -> dict:
        """
        Fetch pre-game total and moneyline odds for each game on target_date.

        Calls GET /sports/{sport_id}/events/{date} and parses:
          - market_id=3 (totals)    → game_total (the main-line over/under value)
          - market_id=1 (moneyline) → home_moneyline and away_moneyline (American odds)

        Returns:
        {
          "<rundown_event_id>": {
            "game_total":       float | None,   # e.g. 8.5
            "home_moneyline":   int   | None,   # e.g. -140
            "away_moneyline":   int   | None,   # e.g. +120
            "home_team_name":   str,
            "away_team_name":   str,
          }
        }
        Returns empty dict on any failure — non-blocking.
        """
        date_str = target_date.strftime("%Y-%m-%d")
        log.info("Rundown game lines fetch starting", date=date_str)

        try:
            async with httpx.AsyncClient() as client:
                raw = await self._get(
                    client,
                    f"/sports/{sport_id}/events/{date_str}",
                    params={"include": "scores,all_periods,lines"},
                )
        except Exception as exc:
            log.error("Rundown game lines fetch failed", date=date_str, error=str(exc))
            return {}

        game_lines: dict[str, dict] = {}

        for event in raw.get("events", []):
            event_id = event.get("event_id", "")
            if not event_id:
                continue

            # Identify home and away team names from the teams list
            home_team_name = ""
            away_team_name = ""
            for team in event.get("teams", []):
                full_name = f"{team.get('name', '')} {team.get('mascot', '')}".strip()
                if team.get("is_home"):
                    home_team_name = full_name
                elif team.get("is_away"):
                    away_team_name = full_name

            game_total: Optional[float] = None
            home_moneyline: Optional[int] = None
            away_moneyline: Optional[int] = None

            for market in event.get("markets", []):
                market_id = market.get("market_id")

                # ── Game total (market_id=3, pre-game period_id=0)
                if market_id == 3 and market.get("period_id") == 0:
                    for participant in market.get("participants", []):
                        if participant.get("name", "").lower() != "over":
                            continue
                        for line in participant.get("lines", []):
                            for _book_id, price in line.get("prices", {}).items():
                                if price.get("is_main_line"):
                                    try:
                                        game_total = float(line.get("value", 0))
                                    except (TypeError, ValueError):
                                        pass
                                    break
                            if game_total is not None:
                                break
                        if game_total is not None:
                            break

                # ── Moneyline (market_id=1, pre-game period_id=0)
                elif market_id == 1 and market.get("period_id") == 0:
                    participants = market.get("participants", [])
                    for participant in participants:
                        p_name = participant.get("name", "")
                        for line in participant.get("lines", []):
                            for _book_id, price in line.get("prices", {}).items():
                                if price.get("is_main_line"):
                                    try:
                                        ml_val = int(price.get("price", 0))
                                    except (TypeError, ValueError):
                                        ml_val = None
                                    if ml_val is not None:
                                        if home_team_name and p_name == home_team_name:
                                            home_moneyline = ml_val
                                        elif away_team_name and p_name == away_team_name:
                                            away_moneyline = ml_val
                                    break

                    # Positional fallback — if name matching found nothing,
                    # treat participant[0] as away and participant[1] as home.
                    # The Rundown returns away team first in the participants list.
                    if home_moneyline is None and away_moneyline is None and len(participants) >= 2:
                        for idx, participant in enumerate(participants[:2]):
                            for line in participant.get("lines", []):
                                for _book_id, price in line.get("prices", {}).items():
                                    if price.get("is_main_line"):
                                        try:
                                            ml_val = int(price.get("price", 0))
                                        except (TypeError, ValueError):
                                            ml_val = None
                                        if ml_val is not None:
                                            if idx == 0:
                                                away_moneyline = ml_val
                                            else:
                                                home_moneyline = ml_val
                                        break

            game_lines[event_id] = {
                "game_total":     game_total,
                "home_moneyline": home_moneyline,
                "away_moneyline": away_moneyline,
                "home_team_name": home_team_name,
                "away_team_name": away_team_name,
            }
            log.debug("Game lines parsed",
                      event_id=event_id,
                      home=home_team_name,
                      away=away_team_name,
                      total=game_total,
                      home_ml=home_moneyline,
                      away_ml=away_moneyline)

        log.info("Rundown game lines fetch complete",
                 date=date_str, games_parsed=len(game_lines))
        return game_lines
