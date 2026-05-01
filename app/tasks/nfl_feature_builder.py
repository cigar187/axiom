"""
NFL Feature Builder — maps raw data from all providers into QBFeatureSet objects.

This is the ONLY place where provider data is translated into scoring engine inputs.
The scoring engines (qpyi.py / qtdi.py) never touch raw API data directly.

Data sources wired in this sprint:
  - Weather (ENS block): wind, temperature, precipitation, dome, surface — fully wired
  - Starter dict (PDR block): rest days, injury designation, practice status — partially wired
  - Props: mapped to output/comparison fields only — NOT formula inputs

All remaining inputs default to 50.0 (neutral) until their data source is connected
in a future sprint. This matches the same pattern as feature_builder.py, where new
data sources are added incrementally without breaking the formula.
"""
from app.core.nfl.features import QBFeatureSet
from app.utils.normalization import clamp
from app.utils.logging import get_logger

log = get_logger("nfl_feature_builder")


# ─────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────

def _map_days_rest(days_rest: int) -> float:
    """
    Convert raw rest days into a 0–100 PDR rest score.

    Thresholds based on NFL game spacing impact on QB performance:
      3 days → 20.0  (Thursday night, severe fatigue penalty)
      6 days → 50.0  (short week)
      7 days → 65.0  (normal Sunday-to-Sunday)
      10+ days → 85.0  (bye week or international game extra rest)

    Values between anchor points are linearly interpolated.
    """
    if days_rest <= 3:
        return 20.0
    if days_rest <= 6:
        # 3→20 to 6→50: linear, +10 per day
        return 20.0 + (days_rest - 3) * 10.0
    if days_rest <= 7:
        # 6→50 to 7→65: linear, +15 per day
        return 50.0 + (days_rest - 6) * 15.0
    if days_rest < 10:
        # 7→65 to 10→85: linear, ~6.67 per day
        return 65.0 + (days_rest - 7) * (20.0 / 3.0)
    return 85.0


def _map_injury_designation(designation: str | None) -> float:
    """
    Convert official NFL injury designation to a 0–100 PDR injury score.

    Out          →  0.0  (confirmed inactive — should not appear, included for safety)
    Doubtful     → 15.0  (very likely to miss — questionable availability)
    Questionable → 45.0  (coin flip; limited practice participation typical)
    None / Clear → 85.0  (no reported injury — healthy and practicing fully)
    """
    mapping = {
        "Out":          0.0,
        "Doubtful":    15.0,
        "Questionable": 45.0,
    }
    return mapping.get(designation, 85.0)


# ─────────────────────────────────────────────────────────────
# Public functions
# ─────────────────────────────────────────────────────────────

def build_qb_feature_set(
    starter: dict,
    weather: dict,
    props:   dict,
) -> QBFeatureSet:
    """
    Build a QBFeatureSet for one QB from all available provider data.

    This is the single translation layer between raw provider dicts and the
    scoring engines. The engines (qpyi.py / qtdi.py) only read QBFeatureSet fields.

    Args:
      starter: Enriched starter dict from nfl_schedule / nfl_props pipeline.
               Keys include: qb_name, team, opponent, is_home, injury_designation,
               game_id, and optionally days_rest, prop_passing_yards_line, prop_td_line, etc.
      weather: Weather dict from nfl_weather.get_weather_for_game().
               Keys: wind_mph, temp_f, precip_chance, is_dome, surface.
      props:   Props dict for this specific QB. Keys: passing_yards_line,
               passing_yards_over, passing_yards_under, td_line, td_over, td_under.
               Props are mapped to output/comparison fields only — never to 0–100 inputs.

    All formula inputs not yet wired to a live data source default to 50.0 (neutral).
    These will be populated in future sprints when the full data pipeline is complete.
    """
    qb_name = starter.get("qb_name", "Unknown")
    team    = starter.get("team", "")

    log.info("nfl_feature_builder: building feature set", qb=qb_name, team=team)

    f = QBFeatureSet(
        player_name = qb_name,
        team        = team,
        opponent    = starter.get("opponent", ""),
        game_id     = starter.get("game_id", ""),
        is_home     = starter.get("is_home", True),
    )

    # ── Prop lines — output/comparison fields, NOT formula inputs
    # These are the betting lines the formula result will be compared against.
    # They do not influence any 0–100 block score.
    f.pass_yards_line       = props.get("passing_yards_line")
    f.pass_yards_over_odds  = props.get("passing_yards_over")
    f.pass_yards_under_odds = props.get("passing_yards_under")
    f.td_line               = props.get("td_line")
    f.td_over_odds          = props.get("td_over")
    f.td_under_odds         = props.get("td_under")

    # ── ENS — Environmental block
    # All 5 weather-derived inputs are fully wired from nfl_weather.py output.
    # ens_alt and ens_crowd remain at None (default 50) — pending stadium altitude
    # and road-crowd noise data sources.

    wind_mph      = float(weather.get("wind_mph", 5.0))
    temp_f        = float(weather.get("temp_f", 72.0))
    precip_chance = float(weather.get("precip_chance", 0.0))
    is_dome       = bool(weather.get("is_dome", False))
    surface       = weather.get("surface", "unknown")

    # Wind: 0 mph = 100 (ideal passing), 25+ mph = 0 (severe penalty)
    f.ens_wind = clamp(100.0 - wind_mph * 4.0)

    # Temperature: 72°F = 100 (ideal), cold or very hot penalizes
    f.ens_temp = clamp(100.0 - abs(72.0 - temp_f) * 1.5)

    # Precipitation: 0% chance = 100 (dry), 100% chance = 0 (wet ball penalty)
    f.ens_precip = clamp(100.0 - precip_chance)

    # Dome: full passing-environment benefit if enclosed
    f.ens_dome = 100.0 if is_dome else 0.0

    # Surface: artificial turf favors routes, speed, and yards after catch
    f.ens_turf = 80.0 if surface == "artificial" else 20.0

    log.info("nfl_feature_builder: ENS wired",
             qb=qb_name,
             ens_wind=round(f.ens_wind, 1),
             ens_temp=round(f.ens_temp, 1),
             ens_precip=round(f.ens_precip, 1),
             ens_dome=f.ens_dome,
             ens_turf=f.ens_turf)

    # ── PDR — Physical Durability Rating (partial — 3 of 10 inputs wired)
    # pdr_rest and pdr_inj are available from schedule + injury report data.
    # pdr_prac defaults to 65.0 (assumed limited practice) until practice report
    # data source is connected.
    # All remaining PDR inputs (pdr_sack, pdr_press, pdr_mob, pdr_hits,
    # pdr_snaps_prior, pdr_age, pdr_trend) remain at None → default to 50.0 neutral.

    days_rest = starter.get("days_rest")
    if days_rest is not None:
        f.pdr_rest = _map_days_rest(int(days_rest))
    else:
        f.pdr_rest = 65.0  # normal full week rest — conservative default

    f.pdr_inj  = _map_injury_designation(starter.get("injury_designation"))
    f.pdr_prac = 65.0  # assumed limited practice — updated when practice report source is added

    log.info("nfl_feature_builder: PDR wired",
             qb=qb_name,
             pdr_rest=f.pdr_rest,
             pdr_inj=f.pdr_inj,
             injury_designation=starter.get("injury_designation"))

    # ── All remaining block inputs (OSW, QSR, GSP, SCB, DSR, RCT, ORD, QTR)
    # These remain at None — the scoring engines treat None as 50.0 (neutral).
    # Each input will be individually wired in a future sprint when its
    # data source (PFF, nflfastR, Next Gen Stats, DVOA, etc.) is connected.
    # This is the same incremental approach used in feature_builder.py.

    log.info("nfl_feature_builder: feature set complete",
             qb=qb_name,
             team=team,
             pass_yards_line=f.pass_yards_line,
             td_line=f.td_line)

    return f


def build_all_feature_sets(
    starters:       list[dict],
    weather_by_game: dict[str, dict],
    props:          list[dict],
) -> list[tuple[dict, QBFeatureSet]]:
    """
    Build a QBFeatureSet for every QB starter in the weekly slate.

    Args:
      starters:        List of enriched starter dicts from nfl_schedule / nfl_props pipeline.
                       Each dict should already have prop fields attached
                       (output of nfl_props.match_props_to_starters()).
      weather_by_game: Dict keyed by game_id from nfl_weather.get_weather_for_all_games().
      props:           Raw props list from nfl_props.get_nfl_qb_props() — used as fallback
                       lookup if prop fields are not already attached to the starter dict.

    Returns a list of (starter_dict, QBFeatureSet) tuples — the starter metadata stays
    attached to each feature set so downstream steps (pipeline, DB writer) have full context.
    """
    # Build a props lookup keyed by normalized QB name for fast access
    yards_props: dict[str, dict] = {}
    td_props:    dict[str, dict] = {}
    for p in props:
        key = p.get("qb_name", "").strip().lower()
        if p.get("market") == "passing_yards":
            yards_props[key] = p
        elif p.get("market") == "touchdowns":
            td_props[key] = p

    results: list[tuple[dict, QBFeatureSet]] = []

    for starter in starters:
        qb_name  = starter.get("qb_name", "Unknown")
        game_id  = starter.get("game_id", "")
        name_key = qb_name.strip().lower()

        log.debug("nfl_feature_builder: processing starter", qb=qb_name,
                  team=starter.get("team"), game_id=game_id)

        # Weather for this game — fall back to empty dict (neutral values) if missing
        weather = weather_by_game.get(game_id, {})

        # Build the props dict for this QB.
        # Prefer pre-matched fields already on the starter (from match_props_to_starters).
        # Fall back to the raw props lookup if those fields are absent.
        yp = yards_props.get(name_key, {})
        tp = td_props.get(name_key, {})

        props_for_qb = {
            "passing_yards_line":  starter.get("prop_passing_yards_line") or yp.get("line"),
            "passing_yards_over":  starter.get("prop_passing_yards_over") or yp.get("over_odds"),
            "passing_yards_under": starter.get("prop_passing_yards_under") or yp.get("under_odds"),
            "td_line":             starter.get("prop_td_line")  or tp.get("line"),
            "td_over":             starter.get("prop_td_over")  or tp.get("over_odds"),
            "td_under":            starter.get("prop_td_under") or tp.get("under_odds"),
        }

        feature_set = build_qb_feature_set(
            starter = starter,
            weather = weather,
            props   = props_for_qb,
        )

        results.append((starter, feature_set))

    log.info("nfl_feature_builder: all feature sets built",
             total=len(results))

    return results
