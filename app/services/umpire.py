"""
Umpire scraper — real implementation using umpirescorecard.com.

Scrapes once per day per umpire. Stores results in the umpire_profiles table.
Falls back to neutral (50) for any umpire not found.

Data pulled per umpire:
  - called_strike_rate: how often they ring up borderline pitches
  - zone_accuracy: how closely their zone matches the true strike zone
  - favor_direction: "pitcher" | "hitter" | "neutral"
  - two_strike_expansion: do they expand the zone on 0-2, 1-2 counts?

All values are normalized to 0-100 for use in UHS and UKS blocks.
  100 = most pitcher-friendly (lots of called strikes, tight zone)
  0   = most hitter-friendly (small zone, won't call borderline strikes)
"""
import re
from datetime import date, datetime
from typing import Optional

import httpx
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential

from app.services.base import BaseProvider
from app.utils.normalization import clamp
from app.utils.logging import get_logger

log = get_logger("umpire_scraper")

SCORECARD_BASE = "https://umpirescorecard.com"
NEUTRAL_SCORE = 50.0

# League-wide baseline values for normalization
# These are typical MLB averages — used for z-score conversion
LEAGUE_CSR_MEAN = 0.935   # called strike rate on borderline pitches
LEAGUE_CSR_STD  = 0.025
LEAGUE_ACC_MEAN = 0.90    # zone accuracy
LEAGUE_ACC_STD  = 0.035


class UmpireScraperAdapter(BaseProvider):
    """
    Scrapes umpirescorecard.com for home plate umpire tendency profiles.
    Called once at the start of the daily pipeline after the MLB Stats API
    provides umpire assignments.
    """

    @property
    def name(self) -> str:
        return "Umpire Scraper (umpirescorecard.com)"

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=4))
    async def _get_html(self, client: httpx.AsyncClient, url: str) -> str:
        log.debug("Umpire scraper request", url=url)
        resp = await client.get(
            url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                )
            },
            timeout=15.0,
            follow_redirects=True,
        )
        resp.raise_for_status()
        return resp.text

    async def fetch(self, target_date: date) -> dict:
        """
        Fetch umpire profiles for all home plate umpires working on target_date.
        The MLB Stats API provides umpire names — we look each one up here.

        Returns:
        {
          "<umpire_id>": {
            "umpire_id": str,
            "umpire_name": str,
            "called_strike_rate": float,      # 0-100
            "zone_accuracy": float,           # 0-100
            "early_count_strikes": float,     # 0-100
            "weak_contact_tendency": float,   # 0-100
            "two_strike_expansion": float,    # 0-100
            "zone_tightness": float,          # 0-100
            "favor_direction": str,
            "sample_games": int,
            "confirmed": bool,
          }
        }
        """
        # This method is called with umpire assignments from the MLB schedule.
        # The pipeline will call fetch_umpire_by_name() for each assigned umpire.
        log.info("Umpire scraper: use fetch_umpire_by_name() per umpire", date=str(target_date))
        return {}

    async def fetch_umpire_by_name(self, umpire_name: str, umpire_id: str = "") -> dict:
        """
        Scrape the umpire's profile from umpirescorecard.com by name.

        The site URL format is:
          https://umpirescorecard.com/umpires/<first>-<last>
        e.g.: https://umpirescorecard.com/umpires/angel-hernandez
        """
        name_slug = self._name_to_slug(umpire_name)
        url = f"{SCORECARD_BASE}/umpires/{name_slug}"

        async with httpx.AsyncClient() as client:
            try:
                html = await self._get_html(client, url)
                profile = self._parse_umpire_page(html, umpire_id, umpire_name)
                if profile:
                    log.info("Umpire profile scraped",
                             name=umpire_name, slug=name_slug,
                             csr=profile.get("called_strike_rate"),
                             accuracy=profile.get("zone_accuracy"))
                    return profile
                else:
                    log.warning("Umpire profile parse failed — using neutral",
                                name=umpire_name, url=url)
                    return self.get_neutral_profile(umpire_id, umpire_name)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 404:
                    log.warning("Umpire not found on scorecard", name=umpire_name, url=url)
                else:
                    log.warning("Umpire scrape HTTP error",
                                name=umpire_name, status=exc.response.status_code)
                return self.get_neutral_profile(umpire_id, umpire_name)
            except Exception as exc:
                log.warning("Umpire scrape failed", name=umpire_name, error=str(exc))
                return self.get_neutral_profile(umpire_id, umpire_name)

    def _parse_umpire_page(self, html: str, umpire_id: str, umpire_name: str) -> Optional[dict]:
        """
        Parse the umpire's profile page HTML.

        umpirescorecard.com shows stats in a stat card layout.
        We look for:
          - "CSR" or "Called Strike Rate" — percentage of borderline pitches called strikes
          - "Accuracy" or "Overall Accuracy" — zone accuracy score
          - "Favor" — pitcher or hitter favored
          - Game count for sample size
        """
        soup = BeautifulSoup(html, "lxml")

        # Extract all text stat blocks — the site renders key metrics in labeled divs
        raw_csr = self._extract_stat(soup, ["csr", "called strike rate", "called strikes"])
        raw_accuracy = self._extract_stat(soup, ["accuracy", "overall accuracy", "zone accuracy"])
        raw_favor = self._extract_text(soup, ["favor", "favors"])
        raw_games = self._extract_stat(soup, ["games", "sample", "g "])

        if raw_csr is None and raw_accuracy is None:
            return None  # Couldn't find key stats — page format may have changed

        # ── CSR normalization (called strike rate)
        # Raw CSR from scorecard is typically a percentage like 94.2 or a decimal 0.942
        if raw_csr is not None:
            if raw_csr > 1.0:
                raw_csr = raw_csr / 100.0  # convert percentage to decimal
            # z-score vs league average
            z_csr = (raw_csr - LEAGUE_CSR_MEAN) / LEAGUE_CSR_STD
            csr_score = round(clamp(50 + 15 * z_csr), 2)
        else:
            csr_score = NEUTRAL_SCORE

        # ── Zone accuracy normalization
        if raw_accuracy is not None:
            if raw_accuracy > 1.0:
                raw_accuracy = raw_accuracy / 100.0
            z_acc = (raw_accuracy - LEAGUE_ACC_MEAN) / LEAGUE_ACC_STD
            accuracy_score = round(clamp(50 + 15 * z_acc), 2)
        else:
            accuracy_score = NEUTRAL_SCORE

        # ── Favor direction
        favor = "neutral"
        if raw_favor:
            fl = raw_favor.lower()
            if "pitcher" in fl:
                favor = "pitcher"
            elif "hitter" in fl or "batter" in fl:
                favor = "hitter"

        # Favor adjustment to zone tightness
        if favor == "pitcher":
            zone_tightness = round(clamp(csr_score + 8), 2)
            two_strike_expansion = round(clamp(csr_score + 5), 2)
        elif favor == "hitter":
            zone_tightness = round(clamp(csr_score - 8), 2)
            two_strike_expansion = round(clamp(csr_score - 5), 2)
        else:
            zone_tightness = csr_score
            two_strike_expansion = csr_score

        sample_games = int(raw_games) if raw_games and raw_games > 0 else 0

        return {
            "umpire_id": umpire_id,
            "umpire_name": umpire_name,
            "called_strike_rate": csr_score,
            "zone_accuracy": accuracy_score,
            "early_count_strikes": csr_score,          # CSR is best proxy for early-count tendency
            "weak_contact_tendency": accuracy_score,    # accurate zone = pitcher gets weak contact
            "two_strike_expansion": two_strike_expansion,
            "zone_tightness": zone_tightness,
            "favor_direction": favor,
            "sample_games": sample_games,
            "confirmed": True,
        }

    # ─────────────────────────────────────────────────────────
    # HTML parsing helpers
    # ─────────────────────────────────────────────────────────

    def _extract_stat(self, soup: BeautifulSoup, labels: list[str]) -> Optional[float]:
        """
        Search all text nodes for a label match and extract the nearby numeric value.
        Handles both 'label: value' and adjacent-element layouts.
        """
        text = soup.get_text(separator=" ").lower()
        for label in labels:
            # Look for patterns like "CSR: 94.2%" or "Called Strike Rate 93.8"
            patterns = [
                rf"{re.escape(label)}[:\s]+([0-9]+\.?[0-9]*)\s*%?",
                rf"([0-9]+\.?[0-9]*)\s*%?\s+{re.escape(label)}",
            ]
            for pat in patterns:
                match = re.search(pat, text)
                if match:
                    try:
                        return float(match.group(1))
                    except ValueError:
                        pass
        return None

    def _extract_text(self, soup: BeautifulSoup, labels: list[str]) -> Optional[str]:
        """Extract a nearby text value for a given label."""
        text = soup.get_text(separator=" ").lower()
        for label in labels:
            pattern = rf"{re.escape(label)}[:\s]+([a-z]+)"
            match = re.search(pattern, text)
            if match:
                return match.group(1)
        return None

    @staticmethod
    def _name_to_slug(name: str) -> str:
        """Convert 'Angel Hernandez' → 'angel-hernandez'."""
        return name.strip().lower().replace(" ", "-").replace(".", "")

    # ─────────────────────────────────────────────────────────
    # Neutral fallback
    # ─────────────────────────────────────────────────────────

    def get_neutral_profile(self, umpire_id: str = "", umpire_name: str = "") -> dict:
        """
        Returns a neutral (50) profile for any umpire not found on the scorecard.
        The scoring engine applies HV6 (-0.8) and KV2 (-1.0) penalties when confirmed=False.
        """
        return {
            "umpire_id": umpire_id,
            "umpire_name": umpire_name,
            "called_strike_rate": NEUTRAL_SCORE,
            "zone_accuracy": NEUTRAL_SCORE,
            "early_count_strikes": NEUTRAL_SCORE,
            "weak_contact_tendency": NEUTRAL_SCORE,
            "two_strike_expansion": NEUTRAL_SCORE,
            "zone_tightness": NEUTRAL_SCORE,
            "favor_direction": "neutral",
            "sample_games": 0,
            "confirmed": False,
        }
