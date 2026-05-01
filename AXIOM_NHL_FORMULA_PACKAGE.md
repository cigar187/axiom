# AXIOM NHL Formula Package
## For external refinement — drop this entire file into Claude

---

## What this is

Two scoring engines built to predict NHL betting markets (Points, Goals, Assists, Shots on Goal).
Modeled after the existing Axiom baseball engines (HUSI / KUSI).

**Engine 1 — Formula (deterministic)**
- **GSAI** — Goalie Shots-Against Index (mirrors HUSI architecture)
- **PPSI** — Player Points Scoring Index (mirrors KUSI architecture)
- Both produce a 0–100 index score. 50 = neutral. >50 = favors the Under. <50 = favors the Over.

**Engine 2 — ML (GradientBoosting, standalone)**
- Trains per-player on their full 2025-26 regular season game log fetched from the NHL public API
- Predicts the same four markets, compared against Engine 1 with an ALIGNED / LEAN / SPLIT signal

**Bettable markets output:**
- PROJ-P (Points), PROJ-G (Goals), PROJ-A (Assists), PROJ-SOG (Shots on Goal)

**Data sources:**
- NHL public API: `https://api-web.nhle.com/v1`
- NHL EDGE tracking API: `https://api-web.nhle.com/v1/edge` (speed bursts, shot velocity, zone time — used internally in formula, not shown to bettors)

---

## File 1 — `app/core/nhl/features.py`
### Data contracts (input structs for both engines)

```python
"""
NHL Feature Sets — the data contracts between the live data fetcher and
the GSAI / PPSI scoring engines.

Convention:
  - All block sub-scores are normalized to 0-100
  - 50 = neutral, >50 favors UNDER, <50 favors OVER
  - None fields fall back to 50 inside the engines
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class NHLGameContext:
    game_id: str = ""
    game_date: str = ""
    home_team: str = ""
    away_team: str = ""
    venue: str = ""
    series_game_number: int = 0        # 1–7 for playoffs
    home_series_wins: int = 0
    away_series_wins: int = 0
    home_b2b: bool = False
    away_b2b: bool = False
    home_rest_days: int = 2
    away_rest_days: int = 2
    referee_crew_pp_per_game: Optional[float] = None


@dataclass
class NHLGoalieFeatureSet:
    player_id: int = 0
    player_name: str = ""
    team: str = ""
    is_home: bool = False
    is_confirmed_starter: bool = True
    avg_shots_faced_per_game: float = 29.0
    ctx: Optional[NHLGameContext] = None

    # OSQ: Opponent Shooting Quality (27% weight)
    osq_shots_pg: Optional[float] = None
    osq_shooting_pct: Optional[float] = None
    osq_pp_pct: Optional[float] = None
    osq_high_danger_rate: Optional[float] = None
    osq_series_momentum: Optional[float] = None
    osq_xgf_per_60: Optional[float] = None

    # GSS: Goalie Save Suppression (26% weight)
    gss_sv_pct: Optional[float] = None
    gss_gsax: Optional[float] = None
    gss_hd_sv_pct: Optional[float] = None
    gss_playoff_sv_pct: Optional[float] = None
    gss_rebound_control: Optional[float] = None
    gss_consistency: Optional[float] = None

    # GEN: Game Environment (16% weight)
    gen_is_home: Optional[float] = None
    gen_rest_days: Optional[float] = None
    gen_b2b_penalty: Optional[float] = None
    gen_series_game: Optional[float] = None
    gen_opponent_b2b: Optional[float] = None

    # TOP: Tactical / Operational (18% weight)
    top_starter_prob: Optional[float] = None
    top_pk_pct: Optional[float] = None
    top_coach_defensive: Optional[float] = None
    top_injury_status: Optional[float] = None
    top_opponent_pp_rate: Optional[float] = None

    # RFS: Referee Flow Score (8% weight)
    rfs_crew_pp_rate: Optional[float] = None
    rfs_home_bias: Optional[float] = None

    # TSC: Team Structure & Coverage (5% weight)
    tsc_blocks_pg: Optional[float] = None
    tsc_cf_pct: Optional[float] = None
    tsc_dzone_exit_pct: Optional[float] = None


@dataclass
class NHLSkaterFeatureSet:
    player_id: int = 0
    player_name: str = ""
    team: str = ""
    position: str = ""
    is_home: bool = False
    line_number: int = 1
    pp_unit: int = 1
    avg_points_per_game: float = 0.5
    avg_shots_per_game: float = 2.5
    avg_shooting_pct: float = 0.105
    ctx: Optional[NHLGameContext] = None

    # OSR: Opponent Scoring Resistance (28% weight)
    osr_goals_against_pg: Optional[float] = None
    osr_sv_pct_against: Optional[float] = None
    osr_shots_against_pg: Optional[float] = None
    osr_pk_pct_against: Optional[float] = None
    osr_hd_chances_against: Optional[float] = None
    osr_xga_per_60: Optional[float] = None

    # PMR: Player Matchup Rating (22% weight)
    pmr_shooting_pct: Optional[float] = None
    pmr_opp_goalie_sv_pct: Optional[float] = None
    pmr_zone_start_pct: Optional[float] = None   # fed by EDGE burst speed percentile
    pmr_opp_goalie_gsax: Optional[float] = None
    pmr_shot_location: Optional[float] = None    # fed by EDGE OZ time percentile

    # PER: Player Efficiency Rating (18% weight)
    per_shots_pg: Optional[float] = None
    per_points_pg: Optional[float] = None
    per_primary_pts_pg: Optional[float] = None
    per_ixg_per_60: Optional[float] = None
    per_shooting_talent: Optional[float] = None  # blended: API sh% + EDGE shot speed %ile

    # POP: Points Operational (14% weight)
    pop_toi_pg: Optional[float] = None
    pop_pp_toi_pg: Optional[float] = None
    pop_linemate_quality: Optional[float] = None  # fed by EDGE max speed percentile
    pop_injury_linemates: Optional[float] = None

    # RPS: Referee PP Score (10% weight)
    rps_crew_pp_rate: Optional[float] = None
    rps_player_draw_rate: Optional[float] = None

    # TLD: Top-Line Deployment (8% weight)
    tld_toi_percentile: Optional[float] = None
    tld_line_position: Optional[float] = None
    tld_pp1_status: Optional[float] = None
```

---

## File 2 — `app/core/nhl/gsaui.py`
### GSAI — Goalie Shots-Against Index

```python
"""
GSAI — Goalie Shots-Against Index

Formula:
  GSAI_base = 0.27*OSQ + 0.26*GSS + 0.16*GEN + 0.18*TOP + 0.08*RFS + 0.05*TSC

  OSQ: 0.20*shots_pg + 0.18*shooting_pct + 0.18*pp_pct + 0.16*high_danger_rate
       + 0.14*series_momentum + 0.14*xgf_per_60

  GSS: 0.26*sv_pct + 0.22*gsax + 0.20*hd_sv_pct + 0.16*playoff_sv_pct
       + 0.10*rebound_control + 0.06*consistency

  GEN: 0.30*is_home + 0.28*rest_days + 0.24*b2b_penalty + 0.12*series_game
       + 0.06*opponent_b2b

  TOP: 0.30*starter_prob + 0.22*pk_pct + 0.20*coach_defensive
       + 0.16*injury_status + 0.12*opponent_pp_rate

  RFS: 0.60*crew_pp_rate + 0.40*home_bias

  TSC: 0.40*blocks_pg + 0.35*cf_pct + 0.25*dzone_exit_pct

Interaction boosts (capped +8.0):
  G1: GSS > 70 and OSQ > 65           → +2.0
  G2: GEN > 70 and TOP > 65           → +1.5
  G3: TSC > 65 and OSQ > 60           → +1.0
  G4: RFS < 40 and opp PP rate > 65   → -1.5
  G5: Both teams B2B                  → +1.0

Volatility penalties (capped -8.0):
  GV1: not confirmed starter           → -3.0
  GV2: opponent hot streak             → -1.5
  GV3: goalie on B2B                   → -2.0
  GV4: high-danger rate vs goalie      → -1.5
  GV5: high-PP crew                    → -1.0

Final: GSAI = clamp(base + interaction - volatility, 0, 100)
Projected shots = base_shots * (1 - 0.18 * ((GSAI - 50) / 50))
"""

from app.core.nhl.features import NHLGoalieFeatureSet

NEUTRAL = 50.0

def _f(val, fallback=NEUTRAL):
    return val if val is not None else fallback

def _clamp(v):
    return max(0.0, min(100.0, v))

def score_osq(f):
    return (0.20 * _f(f.osq_shots_pg) + 0.18 * _f(f.osq_shooting_pct) +
            0.18 * _f(f.osq_pp_pct) + 0.16 * _f(f.osq_high_danger_rate) +
            0.14 * _f(f.osq_series_momentum) + 0.14 * _f(f.osq_xgf_per_60))

def score_gss(f):
    return (0.26 * _f(f.gss_sv_pct) + 0.22 * _f(f.gss_gsax) +
            0.20 * _f(f.gss_hd_sv_pct) + 0.16 * _f(f.gss_playoff_sv_pct) +
            0.10 * _f(f.gss_rebound_control) + 0.06 * _f(f.gss_consistency))

def score_gen(f):
    return (0.30 * _f(f.gen_is_home) + 0.28 * _f(f.gen_rest_days) +
            0.24 * _f(f.gen_b2b_penalty) + 0.12 * _f(f.gen_series_game) +
            0.06 * _f(f.gen_opponent_b2b))

def score_top(f):
    return (0.30 * _f(f.top_starter_prob) + 0.22 * _f(f.top_pk_pct) +
            0.20 * _f(f.top_coach_defensive) + 0.16 * _f(f.top_injury_status) +
            0.12 * _f(f.top_opponent_pp_rate))

def score_rfs(f):
    return 0.60 * _f(f.rfs_crew_pp_rate) + 0.40 * _f(f.rfs_home_bias)

def score_tsc(f):
    return (0.40 * _f(f.tsc_blocks_pg) + 0.35 * _f(f.tsc_cf_pct) +
            0.25 * _f(f.tsc_dzone_exit_pct))

def compute_gsai_interaction(osq, gss, gen, top, rfs, tsc,
                               b2b_both=False, opp_pp_rate_raw=NEUTRAL):
    boost = 0.0
    if gss > 70 and osq > 65:          boost += 2.0
    if gen > 70 and top > 65:          boost += 1.5
    if tsc > 65 and osq > 60:          boost += 1.0
    if rfs < 40 and opp_pp_rate_raw > 65: boost -= 1.5
    if b2b_both:                       boost += 1.0
    return min(boost, 8.0)

def compute_gsai_volatility(f, gss, gen, rfs):
    penalty = 0.0
    if not f.is_confirmed_starter:               penalty += 3.0
    if _f(f.osq_series_momentum) < 35:           penalty += 1.5
    if _f(f.gen_b2b_penalty) < 35:              penalty += 2.0
    if _f(f.osq_high_danger_rate) < 35:          penalty += 1.5
    if rfs < 40:                                  penalty += 1.0
    return min(penalty, 8.0)

def gsai_grade(s):
    if s >= 72: return "A"
    if s >= 62: return "B"
    if s >= 50: return "C"
    if s >= 38: return "D"
    return "F"

def compute_gsai(f, b2b_both=False):
    osq = score_osq(f); gss = score_gss(f); gen = score_gen(f)
    top = score_top(f); rfs = score_rfs(f); tsc = score_tsc(f)
    base = 0.27*osq + 0.26*gss + 0.16*gen + 0.18*top + 0.08*rfs + 0.05*tsc
    interaction = compute_gsai_interaction(osq, gss, gen, top, rfs, tsc,
                                             b2b_both, _f(f.top_opponent_pp_rate))
    volatility  = compute_gsai_volatility(f, gss, gen, rfs)
    gsaui = _clamp(base + interaction - volatility)
    proj  = round(f.avg_shots_faced_per_game * (1.0 - 0.18 * ((gsaui - 50) / 50)), 1)
    return {
        "gsai": round(gsaui, 1), "grade": gsai_grade(gsaui),
        "projected_shots": proj, "base_shots": round(f.avg_shots_faced_per_game, 1),
        "blocks": {"OSQ": round(osq,1), "GSS": round(gss,1), "GEN": round(gen,1),
                   "TOP": round(top,1), "RFS": round(rfs,1), "TSC": round(tsc,1)},
        "interaction": round(interaction, 2), "volatility": round(volatility, 2),
    }
```

---

## File 3 — `app/core/nhl/ppui.py`
### PPSI — Player Points Scoring Index

```python
"""
PPSI — Player Points Scoring Index

Formula:
  PPSI_base = 0.28*OSR + 0.22*PMR + 0.18*PER + 0.14*POP + 0.10*RPS + 0.08*TLD

  OSR: 0.22*goals_against_pg + 0.20*sv_pct_against + 0.18*shots_against_pg
       + 0.16*pk_pct_against + 0.14*hd_chances_against + 0.10*xga_per_60

  PMR: 0.28*shooting_pct + 0.26*opp_goalie_sv_pct + 0.22*zone_start_pct
       + 0.14*opp_goalie_gsax + 0.10*shot_location

  PER: 0.24*shots_pg + 0.22*points_pg + 0.20*primary_pts_pg
       + 0.20*ixg_per_60 + 0.14*shooting_talent

  POP: 0.30*toi_pg + 0.30*pp_toi_pg + 0.22*linemate_quality + 0.18*injury_linemates

  RPS: 0.55*crew_pp_rate + 0.45*player_draw_rate

  TLD: 0.40*toi_percentile + 0.35*line_position + 0.25*pp1_status

Interaction boosts (capped +7.0):
  P1: OSR > 65 and PER > 65          → +2.0
  P2: PP1 player and RPS > 65        → +1.5
  P3: PMR > 70 and PER > 65          → +1.5
  P4: OSR < 35 and PMR < 40          → -1.5

Volatility penalties (capped -7.0):
  PV1: key linemate injured           → -2.5
  PV2: player on B2B                  → -1.5
  PV3: opponent hot goalie            → -2.0
  PV4: player in scoring slump        → -1.0

Final: PPSI = clamp(base + interaction - volatility, 0, 100)
Projected points = base_pts * (1 - 0.22 * ((PPSI - 50) / 50))
Projected SOG    = base_sog * (1 - 0.12 * ((PPSI - 50) / 50))
Projected goals  = proj_sog * shooting_pct
Projected assists = max(0, proj_points - proj_goals)
"""

from app.core.nhl.features import NHLSkaterFeatureSet

NEUTRAL = 50.0

def _f(val, fallback=NEUTRAL):
    return val if val is not None else fallback

def _clamp(v):
    return max(0.0, min(100.0, v))

def score_osr(f):
    return (0.22 * _f(f.osr_goals_against_pg) + 0.20 * _f(f.osr_sv_pct_against) +
            0.18 * _f(f.osr_shots_against_pg) + 0.16 * _f(f.osr_pk_pct_against) +
            0.14 * _f(f.osr_hd_chances_against) + 0.10 * _f(f.osr_xga_per_60))

def score_pmr(f):
    return (0.28 * _f(f.pmr_shooting_pct) + 0.26 * _f(f.pmr_opp_goalie_sv_pct) +
            0.22 * _f(f.pmr_zone_start_pct) + 0.14 * _f(f.pmr_opp_goalie_gsax) +
            0.10 * _f(f.pmr_shot_location))

def score_per(f):
    return (0.24 * _f(f.per_shots_pg) + 0.22 * _f(f.per_points_pg) +
            0.20 * _f(f.per_primary_pts_pg) + 0.20 * _f(f.per_ixg_per_60) +
            0.14 * _f(f.per_shooting_talent))

def score_pop(f):
    return (0.30 * _f(f.pop_toi_pg) + 0.30 * _f(f.pop_pp_toi_pg) +
            0.22 * _f(f.pop_linemate_quality) + 0.18 * _f(f.pop_injury_linemates))

def score_rps(f):
    return 0.55 * _f(f.rps_crew_pp_rate) + 0.45 * _f(f.rps_player_draw_rate)

def score_tld(f):
    return (0.40 * _f(f.tld_toi_percentile) + 0.35 * _f(f.tld_line_position) +
            0.25 * _f(f.tld_pp1_status))

def compute_ppsi_interaction(osr, pmr, per, pop, rps, tld, pp1_status=NEUTRAL):
    boost = 0.0
    if osr > 65 and per > 65:              boost += 2.0
    if pp1_status >= 80 and rps > 65:      boost += 1.5
    if pmr > 70 and per > 65:             boost += 1.5
    if osr < 35 and pmr < 40:             boost -= 1.5
    return min(boost, 7.0)

def compute_ppsi_volatility(f, pmr):
    penalty = 0.0
    if _f(f.pop_injury_linemates) < 35:   penalty += 2.5
    if f.ctx:
        on_b2b = f.ctx.away_b2b if not f.is_home else f.ctx.home_b2b
        if on_b2b:                         penalty += 1.5
    if _f(f.pmr_opp_goalie_gsax) < 30:   penalty += 2.0
    if _f(f.per_points_pg) < 38:         penalty += 1.0
    return min(penalty, 7.0)

def ppsi_grade(s):
    if s >= 72: return "A"
    if s >= 62: return "B"
    if s >= 50: return "C"
    if s >= 38: return "D"
    return "F"

def compute_ppsi(f):
    osr = score_osr(f); pmr = score_pmr(f); per = score_per(f)
    pop = score_pop(f); rps = score_rps(f); tld = score_tld(f)
    base = 0.28*osr + 0.22*pmr + 0.18*per + 0.14*pop + 0.10*rps + 0.08*tld
    interaction = compute_ppsi_interaction(osr, pmr, per, pop, rps, tld, _f(f.tld_pp1_status))
    volatility  = compute_ppsi_volatility(f, pmr)
    ppui = _clamp(base + interaction - volatility)
    proj_pts = round(f.avg_points_per_game * (1.0 - 0.22 * ((ppui - 50) / 50)), 2)
    proj_sog = round(f.avg_shots_per_game  * (1.0 - 0.12 * ((ppui - 50) / 50)), 1)
    proj_g   = round(proj_sog * f.avg_shooting_pct, 2)
    proj_a   = round(max(0.0, proj_pts - proj_g), 2)
    return {
        "ppsi": round(ppui, 1), "grade": ppsi_grade(ppui),
        "projected_points": proj_pts, "projected_goals": proj_g,
        "projected_assists": proj_a, "projected_shots": proj_sog,
        "base_points": round(f.avg_points_per_game, 2),
        "blocks": {"OSR": round(osr,1), "PMR": round(pmr,1), "PER": round(per,1),
                   "POP": round(pop,1), "RPS": round(rps,1), "TLD": round(tld,1)},
        "interaction": round(interaction, 2), "volatility": round(volatility, 2),
    }
```

---

## File 4 — `app/core/nhl/ml_engine.py`
### ML Engine — GradientBoosting per player

```python
"""
NHL Player ML Engine — standalone / testing only.

Per-player GradientBoostingRegressor trained on the player's own
2025-26 regular season game log (fetched live from NHL API).

Four targets: points, goals, assists, shots (all bettable markets).
Min 20 games to activate. Uses rolling-window feature engineering.
"""

import numpy as np
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline

MIN_SAMPLES = 20
TARGETS = ["points", "goals", "assists", "shots"]

FEATURE_NAMES = [
    "is_home", "game_number",
    "rolling_pts_5g", "rolling_pts_10g",
    "rolling_goals_5g", "rolling_goals_10g",
    "rolling_assists_5g", "rolling_assists_10g",
    "rolling_sog_5g", "rolling_sog_10g",
    "rolling_toi_5g", "rolling_toi_10g",
    "rolling_sh_pct_10g",
]

def _rolling(values, n):
    if not values: return 0.0
    return float(np.mean(values[-n:]))

def _parse_toi(toi_str):
    try:
        m, s = str(toi_str).split(":")
        return int(m) + int(s) / 60.0
    except: return 0.0

def parse_game_log(raw_log):
    """Convert raw NHL API game log (newest-first) to clean chronological list."""
    cleaned = []
    for i, g in enumerate(raw_log):
        cleaned.append({
            "goals":   int(g.get("goals",   0) or 0),
            "assists": int(g.get("assists",  0) or 0),
            "points":  int(g.get("points",   0) or 0),
            "shots":   int(g.get("shots",    0) or 0),
            "toi_min": _parse_toi(g.get("toi", "0:00")),
            "is_home": 1.0 if g.get("homeRoadFlag", "R") == "H" else 0.0,
            "game_number": float(i + 1),
        })
    return cleaned

def build_feature_vector(history, game_number, is_home):
    pts_h  = [g["points"]  for g in history]
    gls_h  = [g["goals"]   for g in history]
    ast_h  = [g["assists"] for g in history]
    sog_h  = [g["shots"]   for g in history]
    toi_h  = [g["toi_min"] for g in history]
    sh_h   = [g["goals"]/g["shots"] if g["shots"] > 0 else 0.0 for g in history]
    return [
        is_home, game_number,
        _rolling(pts_h,5), _rolling(pts_h,10),
        _rolling(gls_h,5), _rolling(gls_h,10),
        _rolling(ast_h,5), _rolling(ast_h,10),
        _rolling(sog_h,5), _rolling(sog_h,10),
        _rolling(toi_h,5), _rolling(toi_h,10),
        _rolling(sh_h,10),
    ]

def build_training_matrix(game_log):
    X, y = [], {t: [] for t in TARGETS}
    for i in range(10, len(game_log)):
        history = game_log[:i]
        tonight = game_log[i]
        X.append(build_feature_vector(history, float(i+1), tonight["is_home"]))
        for t in TARGETS:
            y[t].append(float(tonight[t]))
    return np.array(X), {t: np.array(v) for t, v in y.items()}

def _make_pipeline():
    return Pipeline([
        ("scaler", StandardScaler()),
        ("model", GradientBoostingRegressor(
            n_estimators=150, max_depth=3, learning_rate=0.08,
            subsample=0.85, min_samples_leaf=3, random_state=42,
        )),
    ])

class NHLPlayerMLEngine:
    def __init__(self, player_name):
        self.player_name = player_name
        self.models = {}
        self.is_trained = False
        self.n_samples = 0

    def train(self, game_log):
        X, y = build_training_matrix(game_log)
        if len(X) < MIN_SAMPLES:
            return False
        self.n_samples = len(X)
        for target in TARGETS:
            pipe = _make_pipeline()
            pipe.fit(X, y[target])
            self.models[target] = pipe
        self.is_trained = True
        return True

    def predict(self, game_log, is_home):
        if not self.is_trained: return None
        fv = build_feature_vector(game_log, float(len(game_log)+1), 1.0 if is_home else 0.0)
        x = np.array(fv).reshape(1, -1)
        result = {f"ml_proj_{t}": max(0.0, float(self.models[t].predict(x)[0])) for t in TARGETS}
        result["n_samples"] = self.n_samples
        result["ml_active"] = True
        return result

def compute_signal(formula_pts, ml_pts):
    if formula_pts <= 0 and ml_pts <= 0: return "ALIGNED"
    base = max(formula_pts, ml_pts, 0.01)
    diff = abs(formula_pts - ml_pts) / base
    if diff < 0.10: return "ALIGNED"
    if diff < 0.25: return "LEAN"
    return "SPLIT"
```

---

## Block weights summary (for refinement reference)

### GSAI (Goalie)
| Block | Weight | What it measures |
|-------|--------|-----------------|
| OSQ | 27% | Opponent shooting quality — how dangerous is the team firing at this goalie |
| GSS | 26% | Goalie's own save skill — sv%, GSAx, high-danger sv% |
| TOP | 18% | Tactical deployment — PK%, confirmed starter, opponent PP rate |
| GEN | 16% | Game environment — home ice, rest, B2B, series game number |
| RFS | 8%  | Referee crew — low-PP crew = fewer power-play shots |
| TSC | 5%  | Team defensive structure — shot blocks, Corsi%, zone exits |

### PPSI (Skater)
| Block | Weight | What it measures |
|-------|--------|-----------------|
| OSR | 28% | Opponent scoring resistance — how leaky is the defense/goalie they're playing against |
| PMR | 22% | Player matchup — shooting%, zone starts (EDGE burst speed), OZ time (EDGE), opposing goalie GSAx |
| PER | 18% | Player efficiency — shots/game, pts/game, primary pts, EDGE shot velocity blend |
| POP | 14% | Operational deployment — TOI, PP time, linemate quality (EDGE speed), health |
| RPS | 10% | Referee / PP — crew PP rate, player foul-drawing tendency |
| TLD | 8%  | Top-line deployment — TOI rank, line position, PP1 status |

---

## Key design decisions to consider refining

1. **Projection sensitivity** — GSAI uses ±18% swing, PPSI uses ±22% and ±12% for SOG. Are these calibrated correctly for hockey variance?

2. **Block weights** — OSQ at 27% for goalies and OSR at 28% for skaters both put opponent quality first. Does this match what hockey analytics experts believe?

3. **Interaction thresholds** — G1 fires at GSS>70 AND OSQ>65. Are these thresholds too tight or too loose?

4. **ML feature set** — Currently 13 features, all player-level rolling averages. Missing: opponent quality per game, power play time per game (from API), recent playoff vs regular season split.

5. **Signal thresholds** — ALIGNED = within 10%, LEAN = 10-25%, SPLIT = >25%. Should these be tighter given playoff hockey variance?

6. **EDGE data usage** — Speed burst percentile feeds `pmr_zone_start_pct`, OZ time percentile feeds `pmr_shot_location`, max speed feeds `pop_linemate_quality`. Is this the right mapping?
```
