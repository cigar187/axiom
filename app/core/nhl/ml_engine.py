"""
app/core/nhl/ml_engine.py — NHL Player ML Engine (standalone / testing)

One GradientBoostingRegressor per betting market, trained per player on their
own 2025-26 regular season game log fetched live from the NHL public API.

Four targets (all bettable markets):
  points  — goals + assists
  goals   — goals scored
  assists — primary + secondary assists
  shots   — shots on goal (SOG)

Why per-player training?
  Every player has their own scoring profile — a power-play specialist
  spikes differently than an energy checker. Training one model per player
  lets the regressor learn individual tendencies (home/road splits, fatigue
  curves, hot/cold streaks) from their own 50-82 game sample.

Why GradientBoostingRegressor?
  Same rationale as the baseball engine: handles non-linear relationships,
  performs well on small datasets (50-82 samples), gives feature_importances_,
  and runs fast on a single CPU with no GPU required.

Minimum samples: 20 completed games. Below that we flag ML as INSUFFICIENT
and fall back to showing formula-only.

Output per player:
  ml_proj_points, ml_proj_goals, ml_proj_assists, ml_proj_shots
  n_samples (games trained on), signal vs formula (ALIGNED / LEAN / SPLIT)

Standalone: this engine reads ONLY from the NHL public API and sklearn.
It does NOT touch the production database or the baseball pipeline.
"""

import logging

import numpy as np
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline

logger = logging.getLogger(__name__)

# ── Minimum games needed before ML activates for a player
MIN_SAMPLES = 20

# ── Betting market targets
TARGETS = ["points", "goals", "assists", "shots"]

# ── Feature names (matches build_feature_vector order)
FEATURE_NAMES = [
    "is_home",           # 1 = home, 0 = road
    "game_number",       # sequential game in season (1-82) — captures fatigue arc
    "rolling_pts_5g",    # avg points over last 5 games before this game
    "rolling_pts_10g",   # avg points over last 10 games before this game
    "rolling_goals_5g",
    "rolling_goals_10g",
    "rolling_assists_5g",
    "rolling_assists_10g",
    "rolling_sog_5g",
    "rolling_sog_10g",
    "rolling_toi_5g",    # avg time-on-ice (minutes) last 5 games
    "rolling_toi_10g",
    "rolling_sh_pct_10g",  # shooting % rolling 10g (goals / shots)
]

N_FEATURES = len(FEATURE_NAMES)


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _rolling(values: list[float], n: int) -> float:
    """Mean of last n values; uses all available if fewer than n."""
    if not values:
        return 0.0
    return float(np.mean(values[-n:]))


def _parse_toi(toi_str: str) -> float:
    """Convert 'MM:SS' string to decimal minutes."""
    try:
        m, s = str(toi_str).split(":")
        return int(m) + int(s) / 60.0
    except Exception:
        return 0.0


def parse_game_log(raw_log: list[dict]) -> list[dict]:
    """
    Convert raw NHL API game log entries into clean dicts ready for
    feature engineering.

    Each entry:
      {
        "goals": int, "assists": int, "points": int,
        "shots": int, "toi_min": float, "is_home": bool,
        "game_number": int (1-indexed position in the season)
      }
    """
    cleaned = []
    for i, g in enumerate(raw_log):
        cleaned.append({
            "goals":       int(g.get("goals",   0) or 0),
            "assists":     int(g.get("assists",  0) or 0),
            "points":      int(g.get("points",   0) or 0),
            "shots":       int(g.get("shots",    0) or 0),
            "toi_min":     _parse_toi(g.get("toi", "0:00")),
            "is_home":     1.0 if g.get("homeRoadFlag", "R") == "H" else 0.0,
            "game_number": float(i + 1),
        })
    return cleaned


def build_feature_vector(history: list[dict], game_number: float, is_home: float) -> list[float]:
    """
    Build a single feature vector from the games BEFORE the game being predicted.

    history   — list of cleaned game dicts leading up to (not including) tonight
    game_number — sequential position (e.g. 83 for the first playoff game)
    is_home   — 1.0 if home tonight, 0.0 if road
    """
    pts_h    = [g["points"]  for g in history]
    goals_h  = [g["goals"]   for g in history]
    ast_h    = [g["assists"]  for g in history]
    sog_h    = [g["shots"]   for g in history]
    toi_h    = [g["toi_min"] for g in history]
    sh_pct_h = [
        g["goals"] / g["shots"] if g["shots"] > 0 else 0.0
        for g in history
    ]

    return [
        is_home,
        game_number,
        _rolling(pts_h, 5),
        _rolling(pts_h, 10),
        _rolling(goals_h, 5),
        _rolling(goals_h, 10),
        _rolling(ast_h, 5),
        _rolling(ast_h, 10),
        _rolling(sog_h, 5),
        _rolling(sog_h, 10),
        _rolling(toi_h, 5),
        _rolling(toi_h, 10),
        _rolling(sh_pct_h, 10),
    ]


def build_training_matrix(game_log: list[dict]):
    """
    Convert a full season game log into a training matrix.

    We use a rolling-window approach: for each game N, the features are
    derived from games 1..N-1, and the target is game N's actual stats.
    We skip the first 10 games (not enough history for rolling features).
    """
    X, y = [], {t: [] for t in TARGETS}

    for i in range(10, len(game_log)):
        history = game_log[:i]
        tonight = game_log[i]

        fv = build_feature_vector(
            history=history,
            game_number=float(i + 1),
            is_home=tonight["is_home"],
        )
        X.append(fv)
        for t in TARGETS:
            y[t].append(float(tonight[t]))

    return np.array(X), {t: np.array(v) for t, v in y.items()}


# ──────────────────────────────────────────────────────────────────────────────
# ML Engine — one instance per player
# ──────────────────────────────────────────────────────────────────────────────

def _make_pipeline() -> Pipeline:
    return Pipeline([
        ("scaler", StandardScaler()),
        ("model", GradientBoostingRegressor(
            n_estimators=150,
            max_depth=3,
            learning_rate=0.08,
            subsample=0.85,
            min_samples_leaf=3,
            random_state=42,
        )),
    ])


class NHLPlayerMLEngine:
    """
    Per-player ML engine. Train on that player's regular season game log,
    then predict their stats for tonight's game.
    """

    def __init__(self, player_name: str):
        self.player_name = player_name
        self.models: dict[str, Pipeline] = {}
        self.is_trained = False
        self.n_samples  = 0
        self.insufficient = False

    def train(self, game_log: list[dict]) -> bool:
        """
        Train all four target models on this player's game log.
        Returns True if training succeeded, False if insufficient data.
        """
        X, y = build_training_matrix(game_log)

        if len(X) < MIN_SAMPLES:
            self.insufficient = True
            return False

        self.n_samples = len(X)

        for target in TARGETS:
            pipe = _make_pipeline()
            pipe.fit(X, y[target])
            self.models[target] = pipe

        self.is_trained = True
        return True

    def predict(
        self,
        game_log: list[dict],
        is_home: bool,
        is_playoff: bool = False,
    ) -> dict | None:
        """
        Predict tonight's stats using end-of-season rolling form.
        game_log should be the full regular season log (chronological).
        is_home: whether this player's team is home tonight.
        is_playoff: when True, applies a 12% discount to all projections.
          Regular season training data systematically over-projects playoff
          output — playoff hockey is slower, more defensive, and lower scoring.
        """
        if not self.is_trained:
            return None

        fv = build_feature_vector(
            history=game_log,
            game_number=float(len(game_log) + 1),
            is_home=1.0 if is_home else 0.0,
        )
        x = np.array(fv).reshape(1, -1)

        pts_pred     = max(0.0, float(self.models["points"].predict(x)[0]))
        sog_pred     = max(0.0, float(self.models["shots"].predict(x)[0]))
        goals_pred   = max(0.0, float(self.models["goals"].predict(x)[0]))
        assists_pred = max(0.0, float(self.models["assists"].predict(x)[0]))

        if is_playoff:
            discount = 0.88  # 12% playoff discount
            pts_pred     *= discount
            sog_pred     *= discount
            goals_pred   *= discount
            assists_pred *= discount
            logger.info(
                "Playoff discount applied — 12%% reduction to all ML projections for %s",
                self.player_name,
            )

        pts_pred     = round(max(0.0, min(5.0,  pts_pred)),  2)
        sog_pred     = round(max(0.0, min(12.0, sog_pred)),  1)
        goals_pred   = round(max(0.0, min(3.0,  goals_pred)), 2)
        assists_pred = round(max(0.0, min(4.0,  assists_pred)), 2)

        return {
            "ml_proj_points":  pts_pred,
            "ml_proj_shots":   sog_pred,
            "ml_proj_goals":   goals_pred,
            "ml_proj_assists": assists_pred,
            "n_samples":       self.n_samples,
            "ml_active":       True,
            "playoff_discount_applied": is_playoff,
        }


# ──────────────────────────────────────────────────────────────────────────────
# Signal computation — agreement between formula and ML
# ──────────────────────────────────────────────────────────────────────────────

def compute_signal(formula_pts: float, ml_pts: float) -> str:
    """
    Compare formula and ML point projections and return a signal string.

    ALIGNED   — both within 10% of each other
    LEAN      — 10-25% difference (mild divergence)
    SPLIT     — >25% difference (strong divergence, pay attention)
    """
    if formula_pts <= 0 and ml_pts <= 0:
        return "ALIGNED"
    base = max(formula_pts, ml_pts, 0.01)
    diff = abs(formula_pts - ml_pts) / base
    if diff < 0.10:
        return "ALIGNED"
    if diff < 0.25:
        return "LEAN"
    return "SPLIT"
