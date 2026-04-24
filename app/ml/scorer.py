"""
app/ml/scorer.py — Convert ML raw predictions into ML-HUSI and ML-KUSI scores.

The ML engine outputs raw numbers: "this pitcher will allow 4.2 hits."
This module converts those raw numbers into the same 0-100 HUSI/KUSI
scale that the formula engine uses, so you can directly compare:

    Formula HUSI: 61.4  (B+)
    ML HUSI:      58.9  (B)     ← ML is slightly less bullish on the under

    Formula KUSI: 54.2  (C+)
    ML KUSI:      63.1  (A-)    ← ML thinks the K under is stronger than the formula

When both engines agree → HIGH CONFIDENCE signal.
When they diverge significantly → flag for review; one of them is seeing
something the other isn't (e.g., formula is reacting to umpire data,
ML is reacting to a pattern in how THIS pitcher performs in specific parks).

Conversion method:
──────────────────
For HUSI (hits-based):
  - Collect all ML projected hits across today's pitcher slate
  - Normalize: more projected hits → LOWER ML-HUSI (pitcher is riskier for under)
  - Formula: ml_husi = 100 × (1 - (proj_hits - MIN) / (MAX - MIN))
  - Bounded by historical range [0.5 hits, 12.0 hits]

For KUSI (K-based):
  - More projected Ks → LOWER ML-KUSI (pitcher is riskier for K under)
  - Same normalization pattern
  - Bounded by [0.5 Ks, 15.0 Ks]

Grades use the same thresholds as the formula engine for consistency.
"""
from app.utils.logging import get_logger

log = get_logger("ml_scorer")

# ── Historical ranges used for normalization.
# These bracket the realistic range of starter outcomes for a single game.
# Values outside these ranges are clamped before normalization.
HITS_RANGE_MIN = 0.5    # starter allowing <0.5 hits is essentially impossible
HITS_RANGE_MAX = 12.0   # starter allowing >12 hits is extremely rare

KS_RANGE_MIN = 0.5      # starter with <0.5 Ks in modern baseball is nearly impossible
KS_RANGE_MAX = 15.0     # elite strikeout game ceiling

# Grade thresholds — identical to formula engine for apple-to-apple comparison
GRADE_THRESHOLDS = [
    (62.0, "A+"),
    (57.0, "A"),
    (52.0, "B"),
    (47.0, "C"),
    (0.0,  "D"),
]


def _normalize_to_score(value: float, range_min: float, range_max: float,
                         invert: bool = True) -> float:
    """
    Normalize a raw value to a 0-100 score.

    Args:
        value:     The raw prediction (e.g., projected hits).
        range_min: Minimum expected value for this stat.
        range_max: Maximum expected value for this stat.
        invert:    If True, lower value → higher score (correct for hits/Ks
                   since fewer hits = better for the UNDER bet).
    """
    clamped = max(range_min, min(value, range_max))
    ratio = (clamped - range_min) / (range_max - range_min)
    score = (1.0 - ratio) * 100.0 if invert else ratio * 100.0
    return round(max(0.0, min(100.0, score)), 2)


def _grade(score: float) -> str:
    for threshold, grade in GRADE_THRESHOLDS:
        if score >= threshold:
            return grade
    return "D"


def _divergence_label(formula_score: float, ml_score: float) -> str:
    """
    Describe how much the ML score diverges from the formula score.
    Used as a signal for analysts reviewing the predictions.
    """
    delta = abs(ml_score - formula_score)
    if delta < 3.0:
        return "ALIGNED"      # both engines agree — strong signal
    if delta < 8.0:
        return "SLIGHT_DIFF"  # minor divergence — worth noting
    if delta < 15.0:
        return "DIVERGENT"    # meaningful difference — review both
    return "CONFLICT"         # large gap — treat with caution; one engine is wrong


def convert_ml_predictions(
    raw_predictions: list[dict],
    formula_outputs: dict[str, dict],
) -> list[dict]:
    """
    Convert raw ML predictions (hits, Ks) into ML-HUSI/KUSI scores.

    Args:
        raw_predictions:  Output of AxiomMLEngine.predict() — list of
                          {"pitcher_id", "game_id", "ml_proj_hits", "ml_proj_ks", ...}
        formula_outputs:  Dict keyed by pitcher_id with formula engine outputs
                          {"husi": ..., "kusi": ..., "projected_hits": ..., "projected_ks": ..., "grade": ...}

    Returns:
        List of enriched ML output dicts, one per pitcher:
        {
            "pitcher_id": str,
            "game_id": str,
            "ml_proj_hits": float,
            "ml_proj_ks": float,
            "ml_husi": float,          # 0-100
            "ml_kusi": float,          # 0-100
            "ml_husi_grade": str,      # A+/A/B/C/D
            "ml_kusi_grade": str,
            "formula_husi": float,
            "formula_kusi": float,
            "formula_husi_grade": str,
            "formula_kusi_grade": str,
            "husi_delta": float,       # ml_husi - formula_husi
            "kusi_delta": float,
            "husi_divergence": str,    # ALIGNED / SLIGHT_DIFF / DIVERGENT / CONFLICT
            "kusi_divergence": str,
            "consensus_husi_grade": str,   # grade only when both engines agree
            "consensus_kusi_grade": str,
            "model_version": str,
            "training_samples": int,
            "mae_hits": float,
            "mae_ks": float,
        }
    """
    results = []

    for pred in raw_predictions:
        pitcher_id = pred["pitcher_id"]
        game_id = pred["game_id"]
        ml_proj_hits = pred["ml_proj_hits"]
        ml_proj_ks = pred["ml_proj_ks"]

        # Convert raw projections to 0-100 scores
        ml_husi = _normalize_to_score(ml_proj_hits, HITS_RANGE_MIN, HITS_RANGE_MAX, invert=True)
        ml_kusi = _normalize_to_score(ml_proj_ks, KS_RANGE_MIN, KS_RANGE_MAX, invert=True)

        ml_husi_grade = _grade(ml_husi)
        ml_kusi_grade = _grade(ml_kusi)

        # Pull formula outputs for comparison
        fout = formula_outputs.get(pitcher_id, {})
        formula_husi = fout.get("husi", 50.0) or 50.0
        formula_kusi = fout.get("kusi", 50.0) or 50.0
        formula_husi_grade = fout.get("husi_grade", "?")
        formula_kusi_grade = fout.get("kusi_grade", "?")

        husi_delta = round(ml_husi - formula_husi, 2)
        kusi_delta = round(ml_kusi - formula_kusi, 2)

        husi_divergence = _divergence_label(formula_husi, ml_husi)
        kusi_divergence = _divergence_label(formula_kusi, ml_kusi)

        # Consensus grade: only when both engines give the same grade
        consensus_husi_grade = ml_husi_grade if ml_husi_grade == formula_husi_grade else "SPLIT"
        consensus_kusi_grade = ml_kusi_grade if ml_kusi_grade == formula_kusi_grade else "SPLIT"

        log.info("ML score converted",
                 pitcher_id=pitcher_id,
                 ml_proj_hits=ml_proj_hits, ml_husi=ml_husi, ml_husi_grade=ml_husi_grade,
                 ml_proj_ks=ml_proj_ks, ml_kusi=ml_kusi, ml_kusi_grade=ml_kusi_grade,
                 formula_husi=formula_husi, husi_delta=husi_delta, husi_divergence=husi_divergence,
                 formula_kusi=formula_kusi, kusi_delta=kusi_delta, kusi_divergence=kusi_divergence,
                 consensus_husi=consensus_husi_grade, consensus_kusi=consensus_kusi_grade)

        results.append({
            "pitcher_id": pitcher_id,
            "game_id": game_id,
            "ml_proj_hits": ml_proj_hits,
            "ml_proj_ks": ml_proj_ks,
            "ml_husi": ml_husi,
            "ml_kusi": ml_kusi,
            "ml_husi_grade": ml_husi_grade,
            "ml_kusi_grade": ml_kusi_grade,
            "formula_husi": formula_husi,
            "formula_kusi": formula_kusi,
            "formula_husi_grade": formula_husi_grade,
            "formula_kusi_grade": formula_kusi_grade,
            "husi_delta": husi_delta,
            "kusi_delta": kusi_delta,
            "husi_divergence": husi_divergence,
            "kusi_divergence": kusi_divergence,
            "consensus_husi_grade": consensus_husi_grade,
            "consensus_kusi_grade": consensus_kusi_grade,
            "model_version": pred.get("model_version", "unknown"),
            "training_samples": pred.get("training_samples", 0),
            "mae_hits": pred.get("mae_hits"),
            "mae_ks": pred.get("mae_ks"),
        })

    return results
