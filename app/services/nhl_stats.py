"""
nhl_stats.py — NHL player and team statistics fetcher.

Data sources: NHL public API (no API key required)
  Player landing: https://api-web.nhle.com/v1/player/{id}/landing
  Game log:       https://api-web.nhle.com/v1/player/{id}/game-log/{season}/{type}
  Team stats:     https://api-web.nhle.com/v1/club-stats/{abbrev}/now
  EDGE tracking:  https://api-web.nhle.com/v1/edge/skater-skating-speed-detail/...
                  https://api-web.nhle.com/v1/edge/skater-shot-speed-detail/...
                  https://api-web.nhle.com/v1/edge/skater-zone-time/...

Fetches:
  - Player season stats, playoff stats, and game logs (skaters and goalies)
  - Team aggregate stats
  - NHL EDGE API percentile data: skating speed, shot speed, zone time
  - Trains per-player ML models and returns projections

Usage:
  Called by the NHL feature builder after the schedule context is built.
  Can also be run standalone: python -m app.services.nhl_stats

Error handling:
  - All HTTP calls timeout via the shared _fetch() helper in nhl_schedule
  - On any failure: logs at WARNING and returns empty dict / None
  - Never raises — callers always receive a safe default
"""
import time
from typing import Optional

from app.core.nhl.ml_engine import NHLPlayerMLEngine, parse_game_log
from app.services.nhl_schedule import _fetch
from app.utils.logging import get_logger

log = get_logger("nhl_stats")

NHL_API  = "https://api-web.nhle.com/v1"
EDGE_API = "https://api-web.nhle.com/v1/edge"

# Current regular season — used for game log and EDGE data pulls
CURRENT_SEASON = "20252026"
REGULAR_SEASON = "2"   # gameTypeId 2 = regular season
PLAYOFF_TYPE   = "3"   # gameTypeId 3 = playoffs


# ─────────────────────────────────────────────────────────────
# Team stats
# ─────────────────────────────────────────────────────────────

def get_team_stats(team_abbrev: str) -> dict:
    """
    Fetch current season team stats for the given team abbreviation.
    Returns empty dict if the API call fails.
    """
    data = _fetch(f"{NHL_API}/club-stats/{team_abbrev}/now")
    return data if isinstance(data, dict) else {}


# ─────────────────────────────────────────────────────────────
# Player landing pages
# ─────────────────────────────────────────────────────────────

def get_goalie_stats(player_id: int) -> dict:
    """
    Fetch the full landing page for a goalie. Contains season totals,
    playoff totals, career stats, and basic bio.
    Returns empty dict if the API call fails.
    """
    data = _fetch(f"{NHL_API}/player/{player_id}/landing")
    return data if isinstance(data, dict) else {}


def get_player_stats(player_id: int) -> dict:
    """
    Fetch the full landing page for a skater. Contains season totals,
    playoff totals, career stats, and basic bio.
    Returns empty dict if the API call fails.
    """
    data = _fetch(f"{NHL_API}/player/{player_id}/landing")
    return data if isinstance(data, dict) else {}


# ─────────────────────────────────────────────────────────────
# Game log
# ─────────────────────────────────────────────────────────────

def get_player_game_log(
    player_id: int,
    season: str = CURRENT_SEASON,
    game_type: int = 2,
) -> list[dict]:
    """
    Fetch a player's full game log for the given season and game type.

    Args:
        player_id:  NHL player ID
        season:     Season string, e.g. "20252026"
        game_type:  2 = regular season, 3 = playoffs

    Returns a list of raw game log dicts in chronological order (oldest first).
    The NHL API returns newest-first — this function reverses the order.
    Returns empty list if the API call fails.
    """
    data = _fetch(f"{NHL_API}/player/{player_id}/game-log/{season}/{game_type}")
    if not data:
        return []
    return list(reversed(data.get("gameLog", [])))


# ─────────────────────────────────────────────────────────────
# NHL EDGE tracking data
# ─────────────────────────────────────────────────────────────

def get_skater_edge_data(player_id: int) -> dict:
    """
    Fetch three EDGE endpoints for a skater and return a flat dict of
    key percentile scores already on a 0–100 scale.

    The EDGE API returns percentile as a 0.0–1.0 float; this function
    multiplies by 100 so values plug directly into the feature engine.

    Keys returned:
        max_speed_pct     — max skating speed percentile vs full league
        burst22_pct       — bursts above 22 mph percentile (explosiveness)
        avg_shot_spd_pct  — average shot speed percentile (shot power)
        top_shot_spd_pct  — top single shot speed percentile
        oz_time_pct       — even-strength OZ time percentile
        oz_raw_pct        — raw % of ES time spent in offensive zone (0–100)

    Falls back to 50.0 (neutral) for any value that fails to load.
    Includes 120ms sleeps between sub-calls to be respectful of the API.
    """
    spd_url  = f"{EDGE_API}/skater-skating-speed-detail/{player_id}/{CURRENT_SEASON}/{REGULAR_SEASON}"
    shot_url = f"{EDGE_API}/skater-shot-speed-detail/{player_id}/{CURRENT_SEASON}/{REGULAR_SEASON}"
    zone_url = f"{EDGE_API}/skater-zone-time/{player_id}/{CURRENT_SEASON}/{REGULAR_SEASON}"

    spd_data  = _fetch(spd_url)  or {}
    time.sleep(0.12)
    shot_data = _fetch(shot_url) or {}
    time.sleep(0.12)
    zone_data = _fetch(zone_url) or {}

    spd  = spd_data.get("skatingSpeedDetails",  {})
    shot = shot_data.get("shotSpeedDetails",     {})

    max_spd_pct  = spd.get("maxSkatingSpeed", {}).get("percentile", 0.5) * 100.0
    burst22_pct  = spd.get("burstsOver22",    {}).get("percentile", 0.5) * 100.0
    avg_shot_pct = shot.get("avgShotSpeed",   {}).get("percentile", 0.5) * 100.0
    top_shot_pct = shot.get("topShotSpeed",   {}).get("percentile", 0.5) * 100.0

    oz_time_pct = 50.0
    oz_raw      = 40.0
    for z in zone_data.get("zoneTimeDetails", []):
        if z.get("strengthCode") == "es":
            oz_time_pct = z.get("offensiveZonePercentile", 0.5) * 100.0
            oz_raw      = z.get("offensiveZonePctg",       0.40) * 100.0
            break

    return {
        "max_speed_pct":    max_spd_pct,
        "burst22_pct":      burst22_pct,
        "avg_shot_spd_pct": avg_shot_pct,
        "top_shot_spd_pct": top_shot_pct,
        "oz_time_pct":      oz_time_pct,
        "oz_raw_pct":       oz_raw,
    }


# ─────────────────────────────────────────────────────────────
# Stat extraction helpers
# ─────────────────────────────────────────────────────────────

def extract_player_season_stats(landing: dict, season_id: str = CURRENT_SEASON) -> dict:
    """
    Pull current regular-season stats from a player landing page.

    Tries seasonTotals first (most reliable), falls back to
    featuredStats.regularSeason.subSeason if not found.
    Returns empty dict if no matching season data is found.
    """
    out = {}
    best_gp = 0
    for row in landing.get("seasonTotals", []):
        if str(row.get("season", "")) == season_id and row.get("gameTypeId") == 2:
            if isinstance(row, dict) and row.get("gamesPlayed", 0) > best_gp:
                out = dict(row)
                best_gp = out.get("gamesPlayed", 0)

    if out:
        return out

    try:
        sub = (
            landing.get("featuredStats", {})
                   .get("regularSeason", {})
                   .get("subSeason", {})
        )
        if isinstance(sub, dict) and "gamesPlayed" in sub:
            return dict(sub)
    except Exception:
        pass

    return out


def extract_goalie_season_stats(landing: dict, season_id: str = CURRENT_SEASON) -> dict:
    """
    Pull current regular-season goalie stats from a landing page.
    Mirrors extract_player_season_stats — same fallback logic.
    """
    out = {}
    best_gp = 0
    for row in landing.get("seasonTotals", []):
        if str(row.get("season", "")) == season_id and row.get("gameTypeId") == 2:
            if isinstance(row, dict) and row.get("gamesPlayed", 0) > best_gp:
                out = dict(row)
                best_gp = out.get("gamesPlayed", 0)

    if out:
        return out

    try:
        sub = (
            landing.get("featuredStats", {})
                   .get("regularSeason", {})
                   .get("subSeason", {})
        )
        if isinstance(sub, dict) and "gamesPlayed" in sub:
            return dict(sub)
    except Exception:
        pass

    return out


def extract_playoff_stats(landing: dict) -> dict:
    """
    Extract the most recent playoff season stats from a player landing page.
    Returns the playoff row with the most games played.
    Returns empty dict if no playoff data is found.
    """
    out = {}
    best_gp = 0
    for row in landing.get("seasonTotals", []):
        if row.get("gameTypeId") == 3:
            if isinstance(row, dict) and row.get("gamesPlayed", 0) >= best_gp:
                out = dict(row)
                best_gp = out.get("gamesPlayed", 0)
    return out


# ─────────────────────────────────────────────────────────────
# ML training and prediction
# ─────────────────────────────────────────────────────────────

def train_player_ml(
    player_id: int,
    player_name: str,
    is_home: bool,
    is_playoff: bool = True,
) -> Optional[dict]:
    """
    Fetch this player's full regular season game log, train a per-player
    GradientBoosting model on all four betting markets, and return ML
    projections for tonight.

    Args:
        player_id:   NHL player ID
        player_name: Display name (used for logging)
        is_home:     Whether the player's team is home tonight
        is_playoff:  When True, applies the 12% playoff discount to all
                     ML projections before returning (default True — the
                     pipeline runs during playoffs)

    Returns None if the game log fetch fails entirely.
    Returns {"ml_active": False, "n_samples": N} if fewer than 20 training
    samples are available after skipping the first 10 games.
    Returns full projection dict on success.
    """
    raw_log = get_player_game_log(player_id)
    if not raw_log:
        log.warning("nhl_stats: no game log found", player=player_name,
                    player_id=player_id)
        return None

    game_log = parse_game_log(raw_log)
    engine   = NHLPlayerMLEngine(player_name)
    trained  = engine.train(game_log)

    if not trained:
        log.info("nhl_stats: insufficient ML data", player=player_name,
                 n_samples=len(game_log))
        return {"ml_active": False, "n_samples": len(game_log)}

    return engine.predict(game_log, is_home=is_home, is_playoff=is_playoff)


# ─────────────────────────────────────────────────────────────
# Bulk enrichment — one call per game roster
# ─────────────────────────────────────────────────────────────

def get_all_player_stats_for_game(
    roster: list[dict],
    is_playoff: bool = True,
) -> list[dict]:
    """
    Enrich a flat roster list with stats fetched from the NHL API.

    Args:
        roster:     Flat list of player dicts as returned by the boxscore
                    (combine forwards + defense + goalies from
                    get_roster_from_boxscore() before passing in).
                    Each dict must contain at least: playerId, position.
        is_playoff: Passed through to train_player_ml for the ML discount.

    Each player in the returned list gains three new keys:
        "stats"  — landing page dict from get_player_stats() or
                   get_goalie_stats(), or None on fetch failure
        "edge"   — EDGE tracking dict from get_skater_edge_data(),
                   or None for goalies / on fetch failure
        "is_goalie" — True if position == "G"

    If any individual player fetch fails the player is kept in the list
    with stats=None and edge=None — they are never silently dropped.
    """
    enriched = []
    n_ok = 0

    for player in roster:
        player_id  = player.get("playerId")
        position   = player.get("position", "")
        name_obj   = player.get("name", {})
        name_str   = name_obj.get("default", f"player_{player_id}") if isinstance(name_obj, dict) else str(name_obj)
        is_goalie  = (position == "G")

        row = dict(player)
        row["is_goalie"] = is_goalie
        row["stats"]     = None
        row["edge"]      = None

        if not player_id:
            log.warning("nhl_stats: player has no playerId — skipping enrichment",
                        name=name_str)
            enriched.append(row)
            continue

        try:
            if is_goalie:
                row["stats"] = get_goalie_stats(player_id)
            else:
                row["stats"] = get_player_stats(player_id)
                row["edge"]  = get_skater_edge_data(player_id)
            n_ok += 1
        except Exception as exc:
            log.warning("nhl_stats: failed to enrich player — keeping with stats=None",
                        player=name_str, player_id=player_id, error=str(exc))

        enriched.append(row)

    log.info("nhl_stats: roster enrichment complete",
             total=len(enriched), enriched_ok=n_ok)
    return enriched


# ─────────────────────────────────────────────────────────────
# Standalone runner — live chain test
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    from app.services.nhl_schedule import build_game_contexts, get_roster_from_boxscore

    print("\nFetching today's game contexts...")
    contexts = build_game_contexts()

    if not contexts:
        print("No games today — cannot run stats test.")
    else:
        ctx = contexts[0]
        game_id = int(ctx.game_id)
        home    = ctx.home_team
        print(f"Using game: {ctx.away_team} @ {home}  (game_id={game_id})\n")

        fwds, defs, gols = get_roster_from_boxscore(game_id, home)
        roster = fwds + defs + gols

        if not roster:
            print(f"No boxscore roster available yet for {home} — game may not have started.")
            print("Testing single player stat fetch instead (Nikita Kucherov, id=8476453)...")
            landing = get_player_stats(8476453)
            season  = extract_player_season_stats(landing)
            edge    = get_skater_edge_data(8476453)
            name    = (landing.get("firstName", {}).get("default", "?") + " " +
                       landing.get("lastName",  {}).get("default", "?"))
            print(f"\n  {name}")
            print(f"  GP={season.get('gamesPlayed')}  "
                  f"G={season.get('goals')}  A={season.get('assists')}  "
                  f"PTS={season.get('points')}  "
                  f"SV%=N/A (skater)")
            print(f"  EDGE burst22={edge['burst22_pct']:.1f}  "
                  f"oz_time={edge['oz_time_pct']:.1f}  "
                  f"avg_shot={edge['avg_shot_spd_pct']:.1f}")
        else:
            print(f"Enriching {len(roster)} {home} players from live API...\n")
            enriched = get_all_player_stats_for_game(roster[:3])  # first 3 to keep it fast

            for p in enriched:
                name      = p.get("name", {}).get("default", "?")
                pos       = p.get("position", "?")
                is_goalie = p.get("is_goalie", False)
                stats     = p.get("stats") or {}
                edge      = p.get("edge")

                season = (extract_goalie_season_stats(stats) if is_goalie
                          else extract_player_season_stats(stats))

                if is_goalie:
                    print(f"  G  {name:<24}  "
                          f"GP={season.get('gamesPlayed','?')}  "
                          f"SV%={season.get('savePctg','?')}")
                else:
                    print(f"  {pos:<2} {name:<24}  "
                          f"GP={season.get('gamesPlayed','?')}  "
                          f"G={season.get('goals','?')}  "
                          f"A={season.get('assists','?')}  "
                          f"PTS={season.get('points','?')}")
                    if edge:
                        print(f"     EDGE: burst22={edge['burst22_pct']:.1f}  "
                              f"oz_time={edge['oz_time_pct']:.1f}  "
                              f"avg_shot={edge['avg_shot_spd_pct']:.1f}")
                print()

    print("nhl_stats chain test complete.")
