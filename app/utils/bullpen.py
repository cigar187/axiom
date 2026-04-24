"""
bullpen.py — Bullpen Fatigue Coefficient (β_bp) calculator.

Formula (per the Axiom spec):
    BFS = (avg_weighted_pitches_per_leverage_arm - BASELINE) / 100

Where:
    - weighted_pitches per arm = (pitches_yesterday × 1.5) + (pitches_2days_ago × 1.0)
    - avg is taken across the top 3-4 leverage arms (closer + setup men)
    - BASELINE = 15 pitches (average fresh arm workload)
    - BFS is clamped to [-0.20, 0.50] to prevent score explosions

Integration:
    Final_HUSI = Base_HUSI × (1 + BFS_opponent)   # tired opp bullpen = more hits
    Final_KUSI = Base_KUSI × (1 - BFS_own)         # tired own bullpen = fewer Ks held

Red Alert:
    If the team's primary closer (most appearances) threw on back-to-back days,
    BFS is forced to a minimum of 0.30 regardless of raw pitch count.
"""
from typing import Optional
from app.utils.logging import get_logger

log = get_logger("bullpen")

# Recency weights
WEIGHT_YESTERDAY = 1.5
WEIGHT_TWO_DAYS_AGO = 1.0

# Fresh arm baseline (pitches — arms below this are effectively rested)
BASELINE_PITCHES = 15.0

# Number of top leverage arms to sample
TOP_ARMS = 4

# Hard clamp on BFS output
BFS_FLOOR = -0.20   # very fresh bullpen
BFS_CEILING = 0.50  # extremely gassed bullpen

# Red Alert threshold — forced minimum BFS when closer is back-to-back
RED_ALERT_BFS = 0.30


def compute_bfs(
    yesterday_pitches: dict[str, int],    # {pitcher_id: pitch_count} for yesterday
    two_days_ago_pitches: dict[str, int],  # {pitcher_id: pitch_count} for 2 days ago
    closer_id: Optional[str] = None,       # ID of team's primary closer (most appearances)
) -> dict:
    """
    Compute the Bullpen Fatigue Score (BFS) for one team.

    Args:
        yesterday_pitches:    Relief pitcher pitch counts from yesterday's game.
        two_days_ago_pitches: Relief pitcher pitch counts from 2 days ago.
        closer_id:            Player ID of the team's identified primary closer.

    Returns:
        {
            "bfs": float,              # the raw fatigue score (-0.20 to 0.50)
            "red_alert": bool,         # closer threw back-to-back days
            "arms_sampled": int,       # how many RP arms contributed
            "top_arms": list[dict],    # per-arm breakdown for logging
            "label": str,              # "FRESH" | "NORMAL" | "TIRED" | "RED ALERT"
        }
    """
    # Combine all pitchers seen in either window
    all_pitchers = set(yesterday_pitches.keys()) | set(two_days_ago_pitches.keys())

    if not all_pitchers:
        return _empty_result()

    # Compute weighted pitch total per arm
    arm_data = []
    for pid in all_pitchers:
        p_yest = yesterday_pitches.get(pid, 0)
        p_2day = two_days_ago_pitches.get(pid, 0)
        weighted = (p_yest * WEIGHT_YESTERDAY) + (p_2day * WEIGHT_TWO_DAYS_AGO)
        arm_data.append({
            "pitcher_id": pid,
            "pitches_yesterday": p_yest,
            "pitches_2days_ago": p_2day,
            "weighted": weighted,
        })

    # Sort by weighted usage — highest usage = leverage arms
    arm_data.sort(key=lambda x: x["weighted"], reverse=True)
    top = arm_data[:TOP_ARMS]

    if not top:
        return _empty_result()

    avg_weighted = sum(a["weighted"] for a in top) / len(top)
    raw_bfs = (avg_weighted - BASELINE_PITCHES) / 100.0

    # Red alert: closer appeared on back-to-back days
    red_alert = False
    if closer_id and closer_id in yesterday_pitches and closer_id in two_days_ago_pitches:
        red_alert = True
        log.warning("Bullpen RED ALERT — closer threw back-to-back",
                    closer_id=closer_id,
                    pitches_yest=yesterday_pitches[closer_id],
                    pitches_2day=two_days_ago_pitches[closer_id])

    # Apply red alert floor
    if red_alert:
        raw_bfs = max(raw_bfs, RED_ALERT_BFS)

    # Clamp
    bfs = max(BFS_FLOOR, min(BFS_CEILING, raw_bfs))

    label = _label(bfs, red_alert)

    log.info("Bullpen fatigue computed",
             bfs=round(bfs, 3),
             avg_weighted=round(avg_weighted, 1),
             arms_sampled=len(top),
             red_alert=red_alert,
             label=label)

    return {
        "bfs": round(bfs, 4),
        "red_alert": red_alert,
        "arms_sampled": len(top),
        "top_arms": top,
        "label": label,
    }


def apply_bullpen_to_husi(husi_score: float, bfs_opponent: float) -> float:
    """
    Apply opponent bullpen fatigue to HUSI.
    Tired opponent bullpen → more hit opportunities → HUSI goes UP.

    Final_HUSI = Base_HUSI × (1 + BFS_opponent)
    """
    adjusted = husi_score * (1.0 + bfs_opponent)
    return round(max(0.0, min(100.0, adjusted)), 4)


def apply_bullpen_to_kusi(kusi_score: float, bfs_own: float) -> float:
    """
    Apply own team bullpen fatigue to KUSI.
    Tired own bullpen → harder to protect a lead → starter K opportunity goes DOWN.

    Final_KUSI = Base_KUSI × (1 - BFS_own)
    """
    adjusted = kusi_score * (1.0 - bfs_own)
    return round(max(0.0, min(100.0, adjusted)), 4)


def _label(bfs: float, red_alert: bool) -> str:
    if red_alert:
        return "RED ALERT"
    if bfs <= -0.10:
        return "FRESH"
    if bfs <= 0.10:
        return "NORMAL"
    if bfs <= 0.25:
        return "TIRED"
    return "GASSED"


def _empty_result() -> dict:
    return {
        "bfs": 0.0,
        "red_alert": False,
        "arms_sampled": 0,
        "top_arms": [],
        "label": "NO DATA",
    }
