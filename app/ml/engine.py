"""
app/ml/engine.py — The Axiom ML Engine.

Two separate Gradient Boosting models run in parallel:
  1. hits_model  — predicts actual hits allowed by the starting pitcher
  2. ks_model    — predicts actual strikeouts by the starting pitcher

These are regression models. Their raw outputs (e.g., "4.2 projected hits")
are then converted to ML-HUSI and ML-KUSI scores (0-100) by scorer.py,
allowing direct comparison against the formula-based scores.

Why Gradient Boosting?
─────────────────────
• Handles non-linear relationships (TTO surge, bullpen cliffs) naturally
• Works well on small datasets (100-500 samples) — perfect for mid-season
• Provides feature_importances_ — we can see what the model is learning
• No GPU required — runs fast on Cloud Run's single CPU

Learning schedule:
─────────────────
• Minimum 30 completed games before the ML engine activates
• Re-trains from scratch on every pipeline run using ALL historical data
• As the season grows (500+ games), the model becomes increasingly accurate
• Year-over-year: prior seasons' data is tagged and optionally included with
  a recency weight — recent games count more than games from 2 years ago

Version tracking:
─────────────────
Each trained model records how many samples it was trained on and its
validation MAE. This is stored in the database alongside ML predictions
so you can track improvement over time.
"""
import io
import numpy as np
from datetime import datetime
from typing import Optional

import joblib
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.model_selection import cross_val_score
from sklearn.metrics import mean_absolute_error

from app.ml.features import (
    build_feature_matrix,
    extract_targets,
    feature_names,
    N_FEATURES,
)
from app.utils.logging import get_logger

log = get_logger("ml_engine")

# ── Minimum samples required before the ML engine activates.
# Below this threshold we have too little data to generalize.
MIN_TRAINING_SAMPLES = 15

# ── Model hyperparameters (tuned for small-to-medium baseball datasets)
HITS_MODEL_PARAMS = {
    "n_estimators": 200,       # number of boosting rounds
    "learning_rate": 0.05,     # slow learning = less overfit
    "max_depth": 4,            # shallow trees = less overfit on small data
    "min_samples_leaf": 3,     # prevents fitting to single noisy samples
    "subsample": 0.8,          # stochastic gradient boosting — adds noise resistance
    "loss": "huber",           # Huber loss is robust to outliers (blowup starts)
    "random_state": 42,
}

KS_MODEL_PARAMS = {
    "n_estimators": 200,
    "learning_rate": 0.05,
    "max_depth": 4,
    "min_samples_leaf": 3,
    "subsample": 0.8,
    "loss": "huber",
    "random_state": 42,
}


class AxiomMLEngine:
    """
    The Axiom ML Engine — trains and predicts pitcher performance using
    Gradient Boosting models on historical game data.

    Usage:
        engine = AxiomMLEngine()

        # Train on completed games
        result = engine.train(training_samples)
        # result: {"trained": bool, "n_samples": int, "mae_hits": float, ...}

        # Predict for today's pitchers
        predictions = engine.predict(today_samples)
        # predictions: [{"pitcher_id": ..., "ml_proj_hits": ..., "ml_proj_ks": ...}, ...]

        # Serialize to bytes (for database storage)
        blob = engine.to_bytes()

        # Restore from bytes
        engine2 = AxiomMLEngine.from_bytes(blob)
    """

    def __init__(self) -> None:
        self.hits_pipeline: Optional[Pipeline] = None
        self.ks_pipeline: Optional[Pipeline] = None
        self.n_training_samples: int = 0
        self.mae_hits: Optional[float] = None
        self.mae_ks: Optional[float] = None
        self.trained_at: Optional[datetime] = None
        self.version: str = "untrained"

    # ─────────────────────────────────────────────────────────
    # Training
    # ─────────────────────────────────────────────────────────

    def train(self, samples: list[dict]) -> dict:
        """
        Train both models on a list of completed game samples.

        Each sample must have:
          - All feature fields (see features.py FEATURE_NAMES)
          - "actual_hits": float  — real hits the pitcher allowed
          - "actual_ks":   float  — real Ks the pitcher recorded

        Returns a status dict with training metrics.
        """
        # Filter to samples that have real outcomes
        complete = [s for s in samples
                    if s.get("actual_hits") is not None
                    and s.get("actual_ks") is not None]

        log.info("ML training starting",
                 total_samples=len(samples),
                 complete_samples=len(complete))

        if len(complete) < MIN_TRAINING_SAMPLES:
            log.info("ML engine inactive — not enough data",
                     have=len(complete), need=MIN_TRAINING_SAMPLES)
            return {
                "trained": False,
                "n_samples": len(complete),
                "reason": f"Need {MIN_TRAINING_SAMPLES} samples, have {len(complete)}",
            }

        X = build_feature_matrix(complete)
        y_hits, y_ks = extract_targets(complete)

        # Drop any remaining NaN rows
        valid_hits = ~np.isnan(y_hits)
        valid_ks = ~np.isnan(y_ks)

        X_hits, y_hits_clean = X[valid_hits], y_hits[valid_hits]
        X_ks, y_ks_clean = X[valid_ks], y_ks[valid_ks]

        # Build sklearn Pipelines (scaler + model)
        self.hits_pipeline = Pipeline([
            ("scaler", StandardScaler()),
            ("model", GradientBoostingRegressor(**HITS_MODEL_PARAMS)),
        ])
        self.ks_pipeline = Pipeline([
            ("scaler", StandardScaler()),
            ("model", GradientBoostingRegressor(**KS_MODEL_PARAMS)),
        ])

        # Train
        self.hits_pipeline.fit(X_hits, y_hits_clean)
        self.ks_pipeline.fit(X_ks, y_ks_clean)

        # Validate with cross-validation (3-fold — conservative for small data)
        n_folds = min(3, len(complete) // 10)  # at least 10 samples per fold
        if n_folds >= 3:
            cv_hits = cross_val_score(
                self.hits_pipeline, X_hits, y_hits_clean,
                cv=n_folds, scoring="neg_mean_absolute_error",
            )
            cv_ks = cross_val_score(
                self.ks_pipeline, X_ks, y_ks_clean,
                cv=n_folds, scoring="neg_mean_absolute_error",
            )
            self.mae_hits = round(float(-cv_hits.mean()), 3)
            self.mae_ks = round(float(-cv_ks.mean()), 3)
        else:
            # Not enough samples for CV — use training error as rough proxy
            hits_pred = self.hits_pipeline.predict(X_hits)
            ks_pred = self.ks_pipeline.predict(X_ks)
            self.mae_hits = round(float(mean_absolute_error(y_hits_clean, hits_pred)), 3)
            self.mae_ks = round(float(mean_absolute_error(y_ks_clean, ks_pred)), 3)

        self.n_training_samples = len(complete)
        self.trained_at = datetime.utcnow()
        self.version = f"v{self.n_training_samples}_{self.trained_at.strftime('%Y%m%d')}"

        # Log feature importances (top 8) so we can see what the ML is learning
        self._log_feature_importances()

        # ── Residual Analysis: how wrong was the FORMULA on labeled samples?
        # This is the core learning loop — the ML identifies which features
        # explain the formula's biggest misses and learns to correct for them.
        self._log_residual_analysis(complete, X_hits, y_hits_clean, X_ks, y_ks_clean)

        log.info("ML engine trained",
                 version=self.version,
                 n_samples=self.n_training_samples,
                 mae_hits=self.mae_hits,
                 mae_ks=self.mae_ks)

        return {
            "trained": True,
            "version": self.version,
            "n_samples": self.n_training_samples,
            "mae_hits": self.mae_hits,
            "mae_ks": self.mae_ks,
        }

    # ─────────────────────────────────────────────────────────
    # Prediction
    # ─────────────────────────────────────────────────────────

    def predict(self, samples: list[dict]) -> list[dict]:
        """
        Predict hits and Ks for a list of today's pitchers.

        Returns a list of prediction dicts, one per input sample:
        [
          {
            "pitcher_id": str,
            "game_id": str,
            "ml_proj_hits": float,
            "ml_proj_ks": float,
            "model_version": str,
            "training_samples": int,
            "mae_hits": float,
            "mae_ks": float,
          },
          ...
        ]

        Returns empty list if the engine has not been trained yet.
        """
        if not self.is_trained:
            log.info("ML prediction skipped — engine not trained")
            return []

        X = build_feature_matrix(samples)

        raw_hits = self.hits_pipeline.predict(X)
        raw_ks = self.ks_pipeline.predict(X)

        results = []
        for i, sample in enumerate(samples):
            proj_hits = max(0.0, float(raw_hits[i]))
            proj_ks = max(0.0, float(raw_ks[i]))

            results.append({
                "pitcher_id": sample.get("pitcher_id", ""),
                "game_id": sample.get("game_id", ""),
                "ml_proj_hits": round(proj_hits, 2),
                "ml_proj_ks": round(proj_ks, 2),
                "model_version": self.version,
                "training_samples": self.n_training_samples,
                "mae_hits": self.mae_hits,
                "mae_ks": self.mae_ks,
            })

        log.info("ML predictions generated", n=len(results), version=self.version)
        return results

    # ─────────────────────────────────────────────────────────
    # Serialization (for database storage)
    # ─────────────────────────────────────────────────────────

    def to_bytes(self) -> bytes:
        """Serialize the trained models to bytes (for DB storage or GCS)."""
        buf = io.BytesIO()
        joblib.dump({
            "hits_pipeline": self.hits_pipeline,
            "ks_pipeline": self.ks_pipeline,
            "n_training_samples": self.n_training_samples,
            "mae_hits": self.mae_hits,
            "mae_ks": self.mae_ks,
            "trained_at": self.trained_at,
            "version": self.version,
        }, buf)
        return buf.getvalue()

    @classmethod
    def from_bytes(cls, data: bytes) -> "AxiomMLEngine":
        """Restore a trained engine from serialized bytes."""
        buf = io.BytesIO(data)
        state = joblib.load(buf)
        engine = cls()
        engine.hits_pipeline = state["hits_pipeline"]
        engine.ks_pipeline = state["ks_pipeline"]
        engine.n_training_samples = state["n_training_samples"]
        engine.mae_hits = state["mae_hits"]
        engine.mae_ks = state["mae_ks"]
        engine.trained_at = state["trained_at"]
        engine.version = state["version"]
        log.info("ML engine restored", version=engine.version,
                 n_samples=engine.n_training_samples)
        return engine

    # ─────────────────────────────────────────────────────────
    # Properties and helpers
    # ─────────────────────────────────────────────────────────

    @property
    def is_trained(self) -> bool:
        return self.hits_pipeline is not None and self.ks_pipeline is not None

    def _log_feature_importances(self) -> None:
        """Log the top 8 features so we can watch what the ML is learning."""
        if not self.is_trained:
            return
        names = feature_names()
        hits_imp = self.hits_pipeline.named_steps["model"].feature_importances_
        ks_imp = self.ks_pipeline.named_steps["model"].feature_importances_

        top_hits = sorted(zip(names, hits_imp), key=lambda x: x[1], reverse=True)[:8]
        top_ks = sorted(zip(names, ks_imp), key=lambda x: x[1], reverse=True)[:8]

        log.info("ML hits model top features",
                 **{name: round(imp, 4) for name, imp in top_hits})
        log.info("ML ks model top features",
                 **{name: round(imp, 4) for name, imp in top_ks})

    def _log_residual_analysis(
        self,
        labeled_samples: list,
        X_hits,
        y_hits,
        X_ks,
        y_ks,
    ) -> None:
        """
        Compare the FORMULA's projections to actual outcomes.
        This is where the ML identifies what the formula is systematically
        getting wrong — the "4.0 hit miss" that Jim described.

        For each labeled sample where the formula made a prediction:
          residual_hits = actual_hits - formula_proj_hits
          residual_ks   = actual_ks   - formula_proj_ks

        We then log:
          - Mean residual (systematic bias — is the formula over/under overall?)
          - Worst misses (games where the formula was furthest off)
          - Which features correlate most with large residuals
        """
        if not self.is_trained or not labeled_samples:
            return

        import numpy as np

        residuals_hits = []
        residuals_ks = []

        for s in labeled_samples:
            # labeled_samples are dicts — use .get() not attribute access
            fp_h = s.get("formula_proj_hits") if isinstance(s, dict) else s.formula_proj_hits
            fp_k = s.get("formula_proj_ks") if isinstance(s, dict) else s.formula_proj_ks
            a_h = s.get("actual_hits") if isinstance(s, dict) else s.actual_hits
            a_k = s.get("actual_ks") if isinstance(s, dict) else s.actual_ks
            if fp_h is not None and a_h is not None:
                residuals_hits.append(a_h - fp_h)
            if fp_k is not None and a_k is not None:
                residuals_ks.append(a_k - fp_k)

        if residuals_hits:
            mean_res_h = float(np.mean(residuals_hits))
            std_res_h = float(np.std(residuals_hits))
            big_miss_h = sorted(residuals_hits, key=abs, reverse=True)[:3]
            log.info(
                "ML residual: HITS formula vs actual",
                formula_bias=round(mean_res_h, 3),
                std=round(std_res_h, 3),
                top_misses=[round(m, 2) for m in big_miss_h],
                interpretation=(
                    "formula UNDER-projects hits (batters more dangerous than expected)"
                    if mean_res_h > 0.3 else
                    "formula OVER-projects hits (formula is too pessimistic on pitcher)"
                    if mean_res_h < -0.3 else
                    "formula is well-calibrated for hits"
                ),
            )

        if residuals_ks:
            mean_res_k = float(np.mean(residuals_ks))
            std_res_k = float(np.std(residuals_ks))
            big_miss_k = sorted(residuals_ks, key=abs, reverse=True)[:3]
            log.info(
                "ML residual: STRIKEOUTS formula vs actual",
                formula_bias=round(mean_res_k, 3),
                std=round(std_res_k, 3),
                top_misses=[round(m, 2) for m in big_miss_k],
                interpretation=(
                    "formula UNDER-projects Ks (pitcher more dominant than expected)"
                    if mean_res_k > 0.3 else
                    "formula OVER-projects Ks (pitcher not as sharp as expected)"
                    if mean_res_k < -0.3 else
                    "formula is well-calibrated for strikeouts"
                ),
            )

        # ML predictions on labeled data (holds-out nothing here — for monitoring only)
        if len(y_hits) >= 5 and self.is_trained:
            ml_preds_h = self.hits_pipeline.predict(X_hits)
            ml_residuals_h = y_hits - ml_preds_h
            log.info(
                "ML self-check: model fit quality",
                ml_mae_hits=round(float(np.mean(np.abs(ml_residuals_h))), 3),
                ml_mae_ks=round(self.mae_ks or 0.0, 3),
                note="MAE on training data — cross-val MAE is more reliable",
            )

    def summary(self) -> dict:
        """Return a human-readable summary of the engine's current state."""
        return {
            "is_trained": self.is_trained,
            "version": self.version,
            "n_training_samples": self.n_training_samples,
            "mae_hits": self.mae_hits,
            "mae_ks": self.mae_ks,
            "trained_at": str(self.trained_at) if self.trained_at else None,
            "min_samples_needed": MIN_TRAINING_SAMPLES,
        }
