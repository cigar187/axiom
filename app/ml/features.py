"""
app/ml/features.py — Feature vector builder for the ML engine.

This module converts a database row (from pitcher_features_daily joined with
ml_training_samples) into a clean numpy array that the ML models can train on.

The feature vector is completely separate from the formula engine — the ML
engine learns which raw features actually predict real-world outcomes, without
being constrained by the hand-tuned formula weights.

Feature groups used by the ML engine:
  1. Formula block scores (the 12 aggregated 0-100 scores from HUSI/KUSI formula)
  2. Key raw stats (H/9, K/9, expected IP, bullpen fatigue)
  3. Environmental context (park score, temp, air density)
  4. Operational context (hook score, bullpen label encoded)

Using the block scores — not the 70+ individual sub-scores — keeps the feature
space manageable and avoids overfitting on sparse early-season data.
"""
import numpy as np
from typing import Optional

# ── Column names in the exact order they appear in the feature vector.
# This order MUST stay fixed — changing it breaks any saved model.
# Add new features only at the END to preserve backward compatibility.
FEATURE_NAMES: list[str] = [
    # ── HUSI block scores (formula outputs, 0-100)
    "owc_score",       # Opponent Weaknesses vs Contact
    "pcs_score",       # Pitcher Contact Suppression
    "ens_score",       # Environmental Score
    "ops_score",       # Operational Score
    "uhs_score",       # Umpire Hits Score
    "dsc_score",       # Defense Score

    # ── KUSI block scores (formula outputs, 0-100)
    "ocr_score",       # Opponent Contact Rate
    "pmr_score",       # Pitch Mix Rating
    "per_score",       # Pitcher Efficiency Rating
    "kop_score",       # K-Operational Profile
    "uks_score",       # Umpire K Score
    "tlr_score",       # Top-Lineup Resistance

    # ── Raw stats (already-normalized by the formula; used as ML context)
    "season_h9",       # H/9 this season (raw, not normalized)
    "season_k9",       # K/9 this season (raw)
    "expected_ip",     # Dynamic expected innings pitched window

    # ── Bullpen fatigue coefficients
    "bullpen_fatigue_opp",   # opponent's β_bp (feeds HUSI)
    "bullpen_fatigue_own",   # own team's β_bp (feeds KUSI)

    # ── Environmental raw context
    "ens_park",        # park factor score (0-100)
    "ens_temp",        # temperature score (0-100)
    "ens_air",         # air density score (0-100)

    # ── Formula final scores (teach the ML what the formula thought)
    # These are INPUT features — the ML learns when to trust or override them.
    "formula_husi",
    "formula_kusi",
    "formula_proj_hits",
    "formula_proj_ks",

    # ── Hidden variables (appended at end for backward compatibility)
    # SKU #37 — Catcher Framing
    "catcher_strike_rate",      # raw called-strike rate (%) — > 50 = elite framer
    # SKU #14 — Travel & Fatigue Index
    "tfi_rest_hours",           # hours of rest before today's game
    "tfi_tz_shift",             # absolute timezone delta from yesterday's venue
    # SKU #38 — VAA & Extension
    "vaa_degrees",              # average vertical approach angle (negative degrees; flat = < -4.5)
    "extension_ft",             # average release extension (feet; elite = > 6.8)
]

N_FEATURES = len(FEATURE_NAMES)

# ── Neutral fallback values for missing data.
# These match the "50 = neutral" convention used throughout the formula engine.
FEATURE_DEFAULTS: dict[str, float] = {
    "owc_score": 50.0,
    "pcs_score": 50.0,
    "ens_score": 50.0,
    "ops_score": 50.0,
    "uhs_score": 50.0,
    "dsc_score": 50.0,
    "ocr_score": 50.0,
    "pmr_score": 50.0,
    "per_score": 50.0,
    "kop_score": 50.0,
    "uks_score": 50.0,
    "tlr_score": 50.0,
    "season_h9": 9.0,
    "season_k9": 8.0,
    "expected_ip": 4.8,
    "bullpen_fatigue_opp": 0.0,
    "bullpen_fatigue_own": 0.0,
    "ens_park": 50.0,
    "ens_temp": 50.0,
    "ens_air": 50.0,
    "formula_husi": 50.0,
    "formula_kusi": 50.0,
    "formula_proj_hits": 5.0,
    "formula_proj_ks": 4.5,
    # Hidden variable defaults (neutral — no bias when data is missing)
    "catcher_strike_rate": 50.0,   # league average framing
    "tfi_rest_hours": 24.0,        # assume full rest
    "tfi_tz_shift": 0,             # no timezone change
    "vaa_degrees": -4.2,           # approximate league-average fastball VAA
    "extension_ft": 6.4,           # approximate league-average extension
}


def build_feature_vector(sample: dict) -> np.ndarray:
    """
    Convert a training sample dict (from the database) into a 1-D numpy array.

    Args:
        sample: Dict with keys matching FEATURE_NAMES (plus outcome fields).
                Missing keys fall back to FEATURE_DEFAULTS.

    Returns:
        numpy array of shape (N_FEATURES,) with dtype float32.
    """
    vec = []
    for name in FEATURE_NAMES:
        val = sample.get(name)
        if val is None or (isinstance(val, float) and np.isnan(val)):
            val = FEATURE_DEFAULTS.get(name, 50.0)
        vec.append(float(val))
    return np.array(vec, dtype=np.float32)


def build_feature_matrix(samples: list[dict]) -> np.ndarray:
    """
    Convert a list of sample dicts into a 2-D feature matrix.

    Returns:
        numpy array of shape (n_samples, N_FEATURES).
    """
    if not samples:
        return np.empty((0, N_FEATURES), dtype=np.float32)
    return np.vstack([build_feature_vector(s) for s in samples])


def extract_targets(samples: list[dict]) -> tuple[np.ndarray, np.ndarray]:
    """
    Extract the target arrays (actual hits and actual Ks) from training samples.

    Returns:
        (y_hits, y_ks) — 1-D float32 arrays of length n_samples.
        Samples with None/NaN targets are masked out in the caller.
    """
    y_hits = np.array(
        [float(s["actual_hits"]) if s.get("actual_hits") is not None else np.nan
         for s in samples],
        dtype=np.float32,
    )
    y_ks = np.array(
        [float(s["actual_ks"]) if s.get("actual_ks") is not None else np.nan
         for s in samples],
        dtype=np.float32,
    )
    return y_hits, y_ks


def feature_names() -> list[str]:
    """Return the ordered list of feature names (for logging and explainability)."""
    return list(FEATURE_NAMES)
