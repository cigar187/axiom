"""
app/services/statcast.py — Statcast pitching stats via Baseball Savant.

DATA SOVEREIGNTY ARCHITECTURE
──────────────────────────────
Baseball Savant is a free public endpoint today. But Axiom cannot be built
on a foundation that relies entirely on third-party services staying free,
staying online, and staying accessible. The solution is a vault-first pattern:

  1. Try to fetch fresh data from Baseball Savant
  2. Write everything we get into our own database (statcast_pitcher_cache)
  3. If Baseball Savant is unavailable, read from our cache
  4. Every day we run, we accumulate more data that we OWN

This means:
  - Day 1: we're mirroring Baseball Savant into our vault
  - Year 1: we own a full season of Statcast stats for every pitcher we scored
  - Year 3: we can start computing things Baseball Savant doesn't even publish
             (e.g., how a pitcher's SwStr% trends in his last 5 starts, weighted
              by our own park factor adjustments — that's purely Axiom IP)
  - Year 5+: our vault IS the data source; external APIs are optional supplements

Stats collected (pitchers):
  • Swinging Strike Rate (Whiff %): best predictor of future strikeouts.
    More reliable than K/9 — it's stuff quality, not outcome luck. (Eno Sarris)

  • Hard Hit Rate: % of batted balls at 95+ mph exit velocity.
    Forward-looking hit suppression indicator. ERA is lagging; HHR is leading.
    (Tom Tango, SABR DIPS research)

  • Ground Ball %: actual batted-ball grounders, not the GO/AO ratio.
    Directly suppresses extra-base hits — fewer fly balls = fewer XBH. (Tango)

Stats collected (batters — SKU #39 Swing Plane Collision):
  • Attack Angle: vertical angle of the bat at contact (degrees, ideal 5-20°).
    A batter with a flat swing (low angle) struggles vs. high, steep fastballs.
    A batter with a steep uppercut (high angle) rolls over on sinkers.
    Mismatch vs. pitcher's VAA → weak contact, fewer barrels. (Eno Sarris / TrackMan)

  • Swing Path Tilt: angular orientation of the entire swing plane (degrees).
    Higher = steeper/more upward-tilted swing. Stored for longitudinal analysis.

These feed into HUSI and KUSI as inputs. HUSI and KUSI remain the engine.
External data is the fuel. Axiom's formulas are the proprietary combustion.
"""
import csv
import io
from datetime import datetime, timezone
from typing import Optional

import httpx
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.models import StatcastPitcherCache, AxiomPitcherStats
from app.utils.logging import get_logger

log = get_logger("statcast")

SAVANT_BASE = "https://baseballsavant.mlb.com"

LEADERBOARD_SELECTIONS = ",".join([
    "player_name",
    "pitcher",
    "p_formatted_ip",
    "hard_hit_percent",
    "groundballs_percent",
    "whiff_percent",
    "xba",
    "xwoba",
])

# Pitchers with < 5 IP have too small a sample to be reliable
MIN_IP_FOR_STATS = 5.0

# Cache is considered fresh if fetched within 24 hours
CACHE_FRESHNESS_HOURS = 24


# ─────────────────────────────────────────────────────────────
# Primary entry point — always use this from the pipeline
# ─────────────────────────────────────────────────────────────

async def fetch_and_cache_statcast_stats(
    client: httpx.AsyncClient,
    db: AsyncSession,
    season: int | str,
) -> dict[str, dict]:
    """
    Cache-first Statcast data fetch.

    Steps:
      1. Try to fetch fresh stats from Baseball Savant
      2. On success: save everything to statcast_pitcher_cache + axiom_pitcher_stats
      3. On failure: load from our own cache (vault fallback)

    Returns:
        Dict keyed by pitcher_id (str):
        {
            "swstr_pct":    float | None
            "hard_hit_pct": float | None
            "gb_pct":       float | None
            "ip":           float | None
            "name":         str
            "source":       "live" | "cache"
        }
    """
    year = int(season)
    season_str = str(year)

    # ── Attempt live fetch
    live_data: dict[str, dict] = {}
    fetch_succeeded = False

    try:
        live_data = await _fetch_from_savant(client, year)
        fetch_succeeded = bool(live_data)
    except Exception as exc:
        log.warning("Baseball Savant fetch failed — falling back to Axiom vault",
                    error=str(exc))

    if fetch_succeeded and live_data:
        # ── Save to vault (non-blocking — failure here does not crash the pipeline)
        try:
            await _save_to_cache(db, live_data, season_str)
        except Exception as exc:
            log.warning("Statcast vault save failed (non-fatal)", error=str(exc))

        for entry in live_data.values():
            entry["source"] = "live"

        log.info("Statcast: live data used", pitchers=len(live_data), season=year)
        return live_data

    # ── Vault fallback: load from our own cache
    log.info("Statcast: using Axiom vault (live source unavailable)", season=year)
    cached = await _load_from_cache(db, season_str)
    for entry in cached.values():
        entry["source"] = "cache"
    log.info("Statcast: vault loaded", pitchers=len(cached), season=year)
    return cached


def merge_statcast_into_pitchers(
    pitchers_data: dict[str, dict],
    statcast_data: dict[str, dict],
) -> int:
    """
    Merge Statcast stats into pitchers_data (in-place).

    Overwrites:
      season_swstr_pct   ← statcast["swstr_pct"]
      season_hard_hit_pct ← statcast["hard_hit_pct"]
      season_gb_pct      ← statcast["gb_pct"]  (real %, overrides GO/AO ratio)

    Returns:
        Number of pitchers that received Statcast data.
    """
    matched = 0
    for pid, pdata in pitchers_data.items():
        sc = statcast_data.get(str(pid))
        if not sc:
            continue

        if sc.get("swstr_pct") is not None:
            pdata["season_swstr_pct"] = sc["swstr_pct"]
        if sc.get("hard_hit_pct") is not None:
            pdata["season_hard_hit_pct"] = sc["hard_hit_pct"]
        if sc.get("gb_pct") is not None:
            pdata["season_gb_pct"] = sc["gb_pct"]
        if sc.get("xba") is not None:
            pdata["season_xba"] = sc["xba"]
        if sc.get("xwoba") is not None:
            pdata["season_xwoba"] = sc["xwoba"]

        matched += 1
        log.debug(
            "Statcast merged",
            pitcher=pdata.get("pitcher_name", pid),
            source=sc.get("source", "unknown"),
            swstr_pct=sc.get("swstr_pct"),
            hard_hit_pct=sc.get("hard_hit_pct"),
            gb_pct=sc.get("gb_pct"),
            xba=sc.get("xba"),
            xwoba=sc.get("xwoba"),
        )

    log.info("Statcast merge complete",
             matched=matched,
             total_pitchers=len(pitchers_data))
    return matched


async def update_axiom_pitcher_stats(
    db: AsyncSession,
    pitchers_data: dict[str, dict],
    statcast_data: dict[str, dict],
    season: str,
) -> None:
    """
    Upsert Axiom's proprietary pitcher stats table.

    This is the data vault layer — every stat we've collected for every pitcher
    we've ever scored gets written here. This is what Axiom owns.

    Called from the pipeline after both MLB Stats API and Statcast data are ready.
    """
    now = datetime.now(timezone.utc)

    for pid, pdata in pitchers_data.items():
        sc = statcast_data.get(str(pid), {})

        stmt = pg_insert(AxiomPitcherStats).values(
            pitcher_id=str(pid),
            season=season,
            player_name=pdata.get("pitcher_name"),
            team_id=str(pdata.get("team_id", "") or ""),
            # MLB Stats layer
            season_era=_safe_float(pdata.get("season_era")),
            season_k_per_9=_safe_float(pdata.get("season_k_per_9")),
            season_h_per_9=_safe_float(pdata.get("season_hits_per_9")),
            season_bb_per_9=_safe_float(pdata.get("season_bb_per_9")),
            season_k_pct=_safe_float(pdata.get("season_k_pct")),
            season_go_ao=_safe_float(pdata.get("season_go_ao")),   # always the raw GO/AO ratio
            avg_ip_per_start=_safe_float(pdata.get("avg_ip_per_start")),
            mlb_service_years=_safe_float(pdata.get("mlb_service_years")),
            # Statcast layer
            season_swstr_pct=_safe_float(sc.get("swstr_pct") or pdata.get("season_swstr_pct")),
            season_hard_hit_pct=_safe_float(sc.get("hard_hit_pct") or pdata.get("season_hard_hit_pct")),
            season_gb_pct=_safe_float(sc.get("gb_pct") or pdata.get("season_gb_pct")),
            # Timestamps
            mlb_stats_last_updated=now,
            statcast_last_updated=now if sc else None,
        ).on_conflict_do_update(
            constraint="uq_axiom_pitcher_season",
            set_={
                "player_name": pdata.get("pitcher_name"),
                "team_id": str(pdata.get("team_id", "") or ""),
                "season_era": _safe_float(pdata.get("season_era")),
                "season_k_per_9": _safe_float(pdata.get("season_k_per_9")),
                "season_h_per_9": _safe_float(pdata.get("season_hits_per_9")),
                "season_bb_per_9": _safe_float(pdata.get("season_bb_per_9")),
                "season_k_pct": _safe_float(pdata.get("season_k_pct")),
                "season_go_ao": _safe_float(pdata.get("season_go_ao")),   # always the raw GO/AO ratio
                "avg_ip_per_start": _safe_float(pdata.get("avg_ip_per_start")),
                "mlb_service_years": _safe_float(pdata.get("mlb_service_years")),
                "season_swstr_pct": _safe_float(sc.get("swstr_pct") or pdata.get("season_swstr_pct")),
                "season_hard_hit_pct": _safe_float(sc.get("hard_hit_pct") or pdata.get("season_hard_hit_pct")),
                "season_gb_pct": _safe_float(sc.get("gb_pct") or pdata.get("season_gb_pct")),
                "mlb_stats_last_updated": now,
                "statcast_last_updated": now if sc else None,
                "updated_at": now,
            },
        )
        await db.execute(stmt)

    log.info("Axiom pitcher stats vault updated", pitchers=len(pitchers_data), season=season)


# ─────────────────────────────────────────────────────────────
# SKU #39 — Batter Swing Profiles (bat-tracking leaderboard)
# ─────────────────────────────────────────────────────────────

async def fetch_batter_swing_profiles(
    client: httpx.AsyncClient,
    year: int,
) -> dict[str, dict]:
    """
    Fetch batter attack angle and swing path tilt from Baseball Savant's
    bat-tracking leaderboard (swing-path-attack-angle endpoint).

    CSV columns used:
      id             — MLB player ID
      name           — "Last, First" display name
      attack_angle   — vertical angle of bat at contact (degrees, ideal 5-20°)
      swing_tilt     — angular tilt of the swing plane (higher = steeper)

    The Swing Plane Collision Score uses these to compute:
      ideal_aa = |pitcher_vaa| + 5.0   (empirical offset: batters need ~5° upward
                                         tilt to square a ball descending at |vaa|°)
      mismatch = |batter_attack_angle - ideal_aa|
      collision_score = clamp(50 + mean(mismatch) * 4.0, 0, 100)

    Minimum 50 competitive swings required for reliable measurement.

    Returns:
        Dict keyed by batter_id (str MLB player ID):
        {
            "attack_angle": float | None,
            "swing_tilt":   float | None,
            "name":         str
        }
    """
    url = (
        f"{SAVANT_BASE}/leaderboard/bat-tracking/swing-path-attack-angle"
        f"?gameType=Regular&minGroupSwings=1&minSwings=50"
        f"&seasonEnd={year}&seasonStart={year}"
        f"&sortColumn=name&sortDirection=asc&type=batter&csv=true"
    )

    log.info("Batter swing profiles fetch starting", year=year)
    resp = await client.get(url, timeout=25.0, follow_redirects=True)
    resp.raise_for_status()

    result: dict[str, dict] = {}
    reader = csv.DictReader(io.StringIO(resp.text))

    for row in reader:
        batter_id = (row.get("id") or "").strip()
        if not batter_id:
            continue
        result[batter_id] = {
            "attack_angle": _safe_float(row.get("attack_angle")),
            "swing_tilt":   _safe_float(row.get("swing_tilt")),
            "name":         (row.get("name") or "").strip(),
        }

    log.info("Batter swing profiles parsed", year=year, batters=len(result))
    return result


# ─────────────────────────────────────────────────────────────
# DSC — Team Outs Above Average (Baseball Savant OAA leaderboard)
# ─────────────────────────────────────────────────────────────

async def fetch_team_oaa(
    client: httpx.AsyncClient,
    year: int,
) -> dict[str, dict]:
    """
    Fetch team-level Outs Above Average (OAA) from Baseball Savant.

    OAA measures how many outs a team's defense converts beyond expectation,
    based on the difficulty of each fielding opportunity. Positive = elite defense.

    Used to populate DSC block:
      dsc_def    ← overall team OAA
      dsc_infdef ← infield OAA
      dsc_ofdef  ← outfield OAA

    Returns:
        Dict keyed by MLB team_id (str):
        {
            "oaa_total":   float | None,   # overall OAA (+ = above average)
            "oaa_inf":     float | None,   # infield OAA
            "oaa_of":      float | None,   # outfield OAA
            "team_name":   str
        }
    """
    url = (
        f"{SAVANT_BASE}/leaderboard/outs_above_average"
        f"?type=Fielding&startYear={year}&endYear={year}"
        f"&split=no&team=yes&csv=true"
    )

    log.info("Team OAA fetch starting", year=year)
    try:
        resp = await client.get(url, timeout=25.0, follow_redirects=True)
        resp.raise_for_status()
    except Exception as exc:
        log.warning("Team OAA fetch failed", year=year, error=str(exc))
        return {}

    result: dict[str, dict] = {}
    reader = csv.DictReader(io.StringIO(resp.text))

    for row in reader:
        # Baseball Savant team OAA CSV uses numeric team_id
        team_id = (row.get("team_id") or row.get("teamId") or "").strip()
        if not team_id:
            continue
        result[team_id] = {
            "oaa_total": _safe_float(
                row.get("outs_above_average") or row.get("oaa")
            ),
            "oaa_inf": _safe_float(
                row.get("outs_above_average_inf") or row.get("oaa_inf")
            ),
            "oaa_of": _safe_float(
                row.get("outs_above_average_of") or row.get("oaa_of")
            ),
            "team_name": (row.get("team_name") or row.get("name") or "").strip(),
        }

    log.info("Team OAA parsed", year=year, teams=len(result))
    return result


# ─────────────────────────────────────────────────────────────
# OCR — Team Batting Discipline (Baseball Savant custom leaderboard)
# ─────────────────────────────────────────────────────────────

async def fetch_team_batting_discipline(
    client: httpx.AsyncClient,
    year: int,
) -> dict[str, dict]:
    """
    Fetch team-level batting discipline stats from Baseball Savant.

    Pulls four metrics that directly feed the OCR block sub-features
    which are currently hardcoded to 50 neutral:

      iz_contact_percent   → ocr_zcon  (zone contact rate — high = lineup makes contact in zone)
      oz_swing_percent     → ocr_disc  (chase rate — high = undisciplined = chases = good for K under)
      foul_percent         → ocr_foul  (foul ball rate — high = extends at-bats = bad for K under)
      two_strike_k_percent → ocr_2s    (K rate on two-strike counts — high = weak in two-strike = good for K under)

    Free public endpoint — no API key required.

    Returns:
        Dict keyed by MLB team_id (str):
        {
            "zone_contact_pct":  float | None,   # iz_contact_percent
            "chase_rate":        float | None,   # oz_swing_percent
            "foul_rate":         float | None,   # foul_percent
            "two_strike_k_pct":  float | None,   # two_strike_k_percent
            "team_name":         str
        }
    """
    url = (
        f"{SAVANT_BASE}/leaderboard/custom"
        f"?year={year}&type=batter&filter="
        f"&sort=team_name&sortDir=asc&min=1&team=yes"
        f"&selections=iz_contact_percent,oz_swing_percent,foul_percent,two_strike_k_percent"
        f"&csv=true"
    )

    log.info("Team batting discipline fetch starting", year=year)
    try:
        resp = await client.get(url, timeout=25.0, follow_redirects=True)
        resp.raise_for_status()
    except Exception as exc:
        log.warning("Team batting discipline fetch failed", year=year, error=str(exc))
        return {}

    result: dict[str, dict] = {}
    reader = csv.DictReader(io.StringIO(resp.text))

    for row in reader:
        team_id = (row.get("team_id") or row.get("teamId") or "").strip()
        if not team_id:
            continue
        result[team_id] = {
            "zone_contact_pct": _safe_float(row.get("iz_contact_percent")),
            "chase_rate":       _safe_float(row.get("oz_swing_percent")),
            "foul_rate":        _safe_float(row.get("foul_percent")),
            "two_strike_k_pct": _safe_float(row.get("two_strike_k_percent")),
            "team_name":        (row.get("team_name") or row.get("name") or "").strip(),
        }

    log.info("Team batting discipline parsed", year=year, teams=len(result))
    return result


# ─────────────────────────────────────────────────────────────
# PMR — Pitch Arsenal (Baseball Savant pitch-type stats)
# ─────────────────────────────────────────────────────────────

async def fetch_pitch_arsenal(
    client: httpx.AsyncClient,
    year: int,
) -> dict[str, dict]:
    """
    Fetch per-pitcher pitch arsenal stats from Baseball Savant.

    Each pitcher has multiple rows — one per pitch type they throw.
    We aggregate into: primary pitch (highest usage), secondary pitch,
    and putaway pitch (highest whiff rate among pitches thrown ≥5% of the time).

    Used to populate PMR block:
      pmr_p1  ← whiff% of primary pitch (by usage)
      pmr_p2  ← whiff% of secondary pitch (by usage)
      pmr_put ← whiff% of true putaway pitch (highest whiff%, ≥5% usage threshold)

    Returns:
        Dict keyed by pitcher_id (str):
        {
            "p1_whiff":  float | None,   # primary pitch whiff rate (%)
            "p2_whiff":  float | None,   # secondary pitch whiff rate (%)
            "put_whiff": float | None,   # putaway pitch whiff rate (%)
            "p1_name":   str,            # e.g. "4-Seam Fastball"
            "put_name":  str,            # e.g. "Slider"
        }
    """
    url = (
        f"{SAVANT_BASE}/leaderboard/pitch-arsenal-stats"
        f"?type=pitcher&pitchType=&year={year}&team=&min=10&csv=true"
    )

    log.info("Pitch arsenal fetch starting", year=year)
    try:
        resp = await client.get(url, timeout=25.0, follow_redirects=True)
        resp.raise_for_status()
    except Exception as exc:
        log.warning("Pitch arsenal fetch failed", year=year, error=str(exc))
        return {}

    # First pass: collect all pitch rows per pitcher
    raw: dict[str, list[dict]] = {}
    reader = csv.DictReader(io.StringIO(resp.text))

    for row in reader:
        pid = (row.get("pitcher") or row.get("player_id") or "").strip()
        if not pid:
            continue
        usage = _safe_float(row.get("percent") or row.get("pitch_percent")) or 0.0
        whiff = _safe_float(row.get("whiff_percent") or row.get("whiff_pct")) or 0.0
        pitch_name = (row.get("pitch_name") or row.get("pitch_type_name") or "").strip()

        raw.setdefault(pid, []).append({
            "pitch_name": pitch_name,
            "usage":      usage,
            "whiff":      whiff,
        })

    # Second pass: aggregate per pitcher
    result: dict[str, dict] = {}
    for pid, pitches in raw.items():
        if not pitches:
            continue
        # Sort by usage descending
        by_usage = sorted(pitches, key=lambda p: p["usage"], reverse=True)
        # Putaway pitch = highest whiff among pitches thrown ≥5% of the time
        eligible = [p for p in pitches if p["usage"] >= 5.0]
        putaway = max(eligible, key=lambda p: p["whiff"]) if eligible else None

        result[pid] = {
            "p1_whiff":  by_usage[0]["whiff"] if len(by_usage) >= 1 else None,
            "p2_whiff":  by_usage[1]["whiff"] if len(by_usage) >= 2 else None,
            "put_whiff": putaway["whiff"] if putaway else None,
            "p1_name":   by_usage[0]["pitch_name"] if by_usage else "",
            "put_name":  putaway["pitch_name"] if putaway else "",
        }

    log.info("Pitch arsenal parsed", year=year, pitchers=len(result))
    return result


# ─────────────────────────────────────────────────────────────
# DSC — Team Sprint Speed (dsc_align proxy)
# ─────────────────────────────────────────────────────────────

async def fetch_team_sprint_speed(
    client: httpx.AsyncClient,
    year: int,
) -> dict[str, dict]:
    """
    Fetch team average sprint speed from Baseball Savant.

    Sprint speed (ft/sec) measures how fast defenders actually move.
    This is a DISTINCT signal from OAA (OAA measures fielding outcomes;
    sprint speed measures the raw athletic capability to cover ground).

    A fast defense can play more aggressive, optimal positioning — they
    can align correctly AND still reach balls that slower defenders could not.
    Tom Tango research: sprint speed correlates with range independent of
    positioning decisions made by the coaching staff.

    Used for: dsc_align (DSC block, 10% weight × 5% HUSI = 0.5% total)

    Returns:
        Dict keyed by team_id (str):
        {
            "sprint_speed":  float | None,   # team avg ft/sec (elite ≈ 28+, slow ≈ 26-)
            "team_name":     str
        }
    """
    url = (
        f"{SAVANT_BASE}/leaderboard/sprint_speed"
        f"?type=team&year={year}&position=&team=&csv=true"
    )

    log.info("Team sprint speed fetch starting", year=year)
    try:
        resp = await client.get(url, timeout=25.0, follow_redirects=True)
        resp.raise_for_status()
    except Exception as exc:
        log.warning("Team sprint speed fetch failed", year=year, error=str(exc))
        return {}

    result: dict[str, dict] = {}
    reader = csv.DictReader(io.StringIO(resp.text))

    for row in reader:
        team_id = (row.get("team_id") or row.get("teamId") or "").strip()
        if not team_id:
            continue
        result[team_id] = {
            "sprint_speed": _safe_float(
                row.get("sprint_speed") or row.get("r_sprint_speed_top50percent")
            ),
            "team_name": (row.get("team_name") or row.get("name") or "").strip(),
        }

    log.info("Team sprint speed parsed", year=year, teams=len(result))
    return result


# ─────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────

async def _fetch_from_savant(
    client: httpx.AsyncClient,
    year: int,
) -> dict[str, dict]:
    """Fetch the Baseball Savant pitcher leaderboard CSV."""
    url = (
        f"{SAVANT_BASE}/leaderboard/custom"
        f"?year={year}&type=pitcher&filter="
        f"&sort=player_name&sortDir=asc&min=1"
        f"&selections={LEADERBOARD_SELECTIONS}&csv=true"
    )

    log.info("Statcast fetch starting", url=url)
    resp = await client.get(url, timeout=25.0, follow_redirects=True)
    resp.raise_for_status()

    result: dict[str, dict] = {}
    reader = csv.DictReader(io.StringIO(resp.text))
    skipped = 0

    for row in reader:
        pitcher_id = (row.get("pitcher") or row.get("player_id") or "").strip()
        if not pitcher_id:
            continue

        ip = _safe_float(row.get("p_formatted_ip"))
        if ip is not None and ip < MIN_IP_FOR_STATS:
            skipped += 1
            continue

        result[pitcher_id] = {
            "swstr_pct":    _safe_float(row.get("whiff_percent")),
            "hard_hit_pct": _safe_float(row.get("hard_hit_percent")),
            "gb_pct":       _safe_float(row.get("groundballs_percent")),
            "xba":          _safe_float(row.get("xba")),
            "xwoba":        _safe_float(row.get("xwoba")),
            "ip":           ip,
            "name":         (row.get("player_name") or "").strip(),
        }

    log.info("Statcast CSV parsed",
             year=year, pitchers=len(result), skipped_small_sample=skipped)
    return result


async def _save_to_cache(
    db: AsyncSession,
    statcast_data: dict[str, dict],
    season: str,
) -> None:
    """Upsert all fetched Statcast stats into statcast_pitcher_cache."""
    now = datetime.now(timezone.utc)

    for pitcher_id, data in statcast_data.items():
        stmt = pg_insert(StatcastPitcherCache).values(
            pitcher_id=pitcher_id,
            season=season,
            player_name=data.get("name"),
            swstr_pct=data.get("swstr_pct"),
            hard_hit_pct=data.get("hard_hit_pct"),
            gb_pct=data.get("gb_pct"),
            innings_pitched=data.get("ip"),
            fetched_at=now,
            data_source="baseball_savant",
        ).on_conflict_do_update(
            constraint="uq_statcast_pitcher_season",
            set_={
                "player_name": data.get("name"),
                "swstr_pct": data.get("swstr_pct"),
                "hard_hit_pct": data.get("hard_hit_pct"),
                "gb_pct": data.get("gb_pct"),
                "innings_pitched": data.get("ip"),
                "fetched_at": now,
                "updated_at": now,
            },
        )
        await db.execute(stmt)

    # Do NOT commit here — the main pipeline session commits atomically in _persist_results.
    # Committing early would bypass dry_run semantics and split the transaction.
    log.info("Statcast vault staged (commit deferred to pipeline)", pitchers=len(statcast_data), season=season)


async def _load_from_cache(
    db: AsyncSession,
    season: str,
) -> dict[str, dict]:
    """Load Statcast stats from our own cache (vault fallback)."""
    rows = (
        await db.execute(
            select(StatcastPitcherCache).where(StatcastPitcherCache.season == season)
        )
    ).scalars().all()

    result = {}
    for row in rows:
        result[row.pitcher_id] = {
            "swstr_pct":    row.swstr_pct,
            "hard_hit_pct": row.hard_hit_pct,
            "gb_pct":       row.gb_pct,
            "ip":           row.innings_pitched,
            "name":         row.player_name or "",
        }

    log.info("Statcast cache loaded", pitchers=len(result), season=season)
    return result


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    if isinstance(val, float):
        return val
    s = str(val).strip()
    if s in ("", "null", "None", "-", "N/A"):
        return None
    try:
        return float(s)
    except (ValueError, TypeError):
        return None
