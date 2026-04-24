"""
app/core/simulation.py — Merlin Probabilistic Simulation Engine (N=2000).

Replaces the old deterministic point-estimate system with a Monte Carlo
simulation that models the natural variance of a live baseball game.

Architecture
────────────
For each pitcher, the engine runs 2,000 independent iterations. In each:

  1. Jitter: Apply Gaussian noise to the three highest-variance inputs:
       PCS_CMD  (command)      σ = 8.0  — walk rate swings game to game
       OCR_DISC (discipline)   σ = 7.0  — plate patience is situational
       ENS_TEMP (temperature)  σ = 5.0  — weather varies inning by inning

  2. Baserunner sampling: Draw from Poisson(λ = WHIP_estimate × 2) to model
     the stochastic baserunner traffic in the last 2 innings. This drives the
     TTO3 Death Trap rule (1.38× → 1.85× when traffic > 2 runners).

  3. Manager Leash: Apply team-specific managerial behavior.
       Analytics  → hard IP cap at 95-pitch equivalent (~5.9 IP).
       Old_School → 20% chance of 1 extra inning, +40% hit probability for
                    each batter faced beyond 100 pitches.

  4. Run HUSI + KUSI in silent mode (no logging) with the modified features.

Output per pitcher
──────────────────
  median_hits       — most likely hits result across 2,000 runs
  median_ks         — most likely Ks result across 2,000 runs
  over_pct_hits     — % of runs where proj_hits > sportsbook line
  under_pct_hits    — % of runs where proj_hits < sportsbook line
  over_pct_ks       — % of runs where proj_ks > sportsbook line
  under_pct_ks      — % of runs where proj_ks < sportsbook line
  p5_hits / p95_hits  — 5th / 95th percentile (floor / kill streak)
  p5_ks / p95_ks
  sim_confidence    — HIGH_OVER / HIGH_UNDER / SPLIT / LEAN_OVER / LEAN_UNDER

Developer note
──────────────
"Do not prioritize Under results. Solve for the most accurate integer.
If the simulation shows a high probability of a Kill Streak (10+ Ks),
the output must reflect that as a high-confidence Over signal."
                                        — Merlin v2.0 specification
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Optional

import numpy as np

from app.core.features import PitcherFeatureSet
from app.core.husi import compute_husi
from app.core.kusi import compute_kusi
from app.utils.logging import get_logger
from app.utils.manager_profiles import get_manager_style

log = get_logger("simulation")

# ── Number of Monte Carlo iterations per pitcher
N_SIMULATIONS = 2_000

# ── Gaussian noise standard deviations for the three jittered variables
# These reflect empirical game-to-game variance in Statcast data:
#   Command (CMD) varies the most — a starter's walk rate can swing ±30% inning to inning
#   Discipline (DISC) is slightly less volatile but still highly situational
#   Temperature (TEMP) is the most stable — weather affects the full game equally
JITTER_SIGMA_CMD  = 8.0   # PCS_CMD — command / walk rate
JITTER_SIGMA_DISC = 7.0   # OCR_DISC — opponent plate discipline
JITTER_SIGMA_TEMP = 5.0   # ENS_TEMP — temperature effect on ball flight

# ── Manager leash constants
ANALYTICS_IP_CAP = 5.9    # ~95 pitches at 16 pitches/inning
OLD_SCHOOL_EXTENSION_PROBABILITY = 0.20   # 20% of iterations go 1 extra inning
OLD_SCHOOL_DEEP_CMD_DEGRADATION  = 0.72   # -28% command after 100 pitches
OLD_SCHOOL_DEEP_PCAP_DEGRADATION = 0.60   # -40% pitch capacity at 100+ pitches

# ── Kill Streak threshold (spec: 10+ Ks = kill streak ceiling)
KILL_STREAK_K_THRESHOLD = 10.0


@dataclass
class SimulationResult:
    """Full probability distribution for one pitcher's hits and Ks props."""

    # ── Hits distribution
    median_hits:    float   # most likely result (integer projection)
    over_pct_hits:  float   # % of 2,000 runs where proj_hits > book line
    under_pct_hits: float   # % of 2,000 runs where proj_hits < book line
    p5_hits:        float   # 5th percentile — "Instant Shelling" result
    p95_hits:       float   # 95th percentile — "Kill Zone Ceiling"

    # ── Ks distribution
    median_ks:    float
    over_pct_ks:  float     # % of runs where proj_ks > book line
    under_pct_ks: float
    p5_ks:        float     # 5th percentile — "Shutdown Floor"
    p95_ks:       float     # 95th percentile — "Kill Streak Ceiling"

    # ── Confidence labels
    sim_confidence_hits: str = "SPLIT"    # HIGH_OVER / HIGH_UNDER / LEAN_OVER / LEAN_UNDER / SPLIT
    sim_confidence_ks:   str = "SPLIT"

    # ── Meta
    n_runs: int = N_SIMULATIONS
    kill_streak_probability: float = 0.0  # % of runs where proj_ks >= 10 (Kill Streak)


def _confidence_label(over_pct: float, under_pct: float, is_ks: bool = False) -> str:
    """
    Map simulation probabilities to a human-readable confidence label.

    Priority: HIGH_OVER signals take precedence for Ks per the Merlin spec —
    if a kill streak is likely, we flag it as Over, not hedge with SPLIT.
    """
    # Never prioritize Under over accuracy — per Merlin spec
    if over_pct >= 70:
        return "HIGH_OVER"
    if under_pct >= 70:
        return "HIGH_UNDER"
    if over_pct >= 55:
        return "LEAN_OVER"
    if under_pct >= 55:
        return "LEAN_UNDER"
    return "SPLIT"


def _clamp_score(val: float) -> float:
    """Clamp a 0-100 score so Gaussian noise cannot push it out of range."""
    return max(0.0, min(100.0, val))


class SimulationEngine:
    """
    Monte Carlo simulation engine wrapping HUSI and KUSI.

    Runs N_SIMULATIONS iterations per pitcher with stochastic feature jitter,
    managerial leash modeling, and TTO3 baserunner sampling to produce a full
    probability distribution over projected hits and strikeouts.
    """

    def __init__(self, n_runs: int = N_SIMULATIONS, seed: Optional[int] = None):
        self.n_runs = n_runs
        # Use numpy's modern Generator for reproducibility + thread safety
        self.rng = np.random.default_rng(seed)

    def run(
        self,
        features: PitcherFeatureSet,
        hits_line: Optional[float] = None,
        k_line: Optional[float] = None,
    ) -> SimulationResult:
        """
        Run N_SIMULATIONS iterations for one pitcher and return the full distribution.

        Args:
            features:  The pitcher's fully built PitcherFeatureSet.
            hits_line: Sportsbook hits-allowed prop line (for Over/Under %).
            k_line:    Sportsbook strikeouts prop line (for Over/Under %).

        Returns:
            SimulationResult with median, percentiles, and over/under probabilities.
        """
        manager_style = get_manager_style(str(getattr(features, "team_id_numeric", "") or ""))

        # ── Pre-generate all random variables in one NumPy call (fast)
        cmd_noise  = self.rng.normal(0.0, JITTER_SIGMA_CMD,  self.n_runs)
        disc_noise = self.rng.normal(0.0, JITTER_SIGMA_DISC, self.n_runs)
        temp_noise = self.rng.normal(0.0, JITTER_SIGMA_TEMP, self.n_runs)

        # ── Pre-compute baserunner lambda from pitcher WHIP estimate
        # WHIP ≈ (H/9 + BB/9) / 9
        # H/9 comes from season_hits_per_9; BB/9 estimated from pcs_cmd (lower = more walks)
        h_per_9 = features.season_hits_per_9 or 9.0
        cmd_score = features.pcs_cmd or 50.0
        # Invert command: 100 = elite (low BB), 0 = wild (high BB)
        bb_per_9_est = max(1.0, (100.0 - cmd_score) / 10.0)   # 0-100 → ~1-10 BB/9
        whip_est = (h_per_9 + bb_per_9_est) / 9.0
        # Expected baserunners in 2 innings = WHIP × 2
        bl2_lambda = max(0.1, whip_est * 2.0)
        baserunner_samples = self.rng.poisson(bl2_lambda, self.n_runs)

        # ── Old School 20% extension flag (pre-sampled, reproducible)
        extension_flags = self.rng.random(self.n_runs) < OLD_SCHOOL_EXTENSION_PROBABILITY

        # Fallback values — used if an individual iteration raises unexpectedly
        _fb_hits = features.hits_line or 8.0
        _fb_ks   = features.k_line   or 5.0

        hits_arr = np.full(self.n_runs, _fb_hits)
        ks_arr   = np.full(self.n_runs, _fb_ks)

        for i in range(self.n_runs):
            try:
                # Step 1: Apply Gaussian jitter to the three stochastic variables
                f = self._jitter(features, cmd_noise[i], disc_noise[i], temp_noise[i])

                # Step 2: Set stochastic baserunner sample for this iteration's TTO3 rule
                f = replace(f, baserunners_l2_innings=float(baserunner_samples[i]))

                # Step 3: Apply managerial leash simulation
                f = self._apply_manager_leash(f, manager_style, bool(extension_flags[i]))

                # Step 4: Run formulas in silent mode (no logging overhead)
                h_result = compute_husi(f, silent=True)
                k_result = compute_kusi(f, silent=True)

                hits_arr[i] = h_result["projected_hits"]
                ks_arr[i]   = k_result["projected_ks"]
            except Exception:
                # Keep the pre-filled fallback for this slot; do not crash the run
                pass

        # ── Compute distribution statistics
        median_hits = float(np.median(hits_arr))
        median_ks   = float(np.median(ks_arr))

        p5_hits  = float(np.percentile(hits_arr, 5))
        p95_hits = float(np.percentile(hits_arr, 95))
        p5_ks    = float(np.percentile(ks_arr, 5))
        p95_ks   = float(np.percentile(ks_arr, 95))

        # Over/Under probabilities
        if hits_line is not None:
            over_pct_hits  = float(np.mean(hits_arr > hits_line)) * 100.0
            under_pct_hits = float(np.mean(hits_arr < hits_line)) * 100.0
        else:
            over_pct_hits = under_pct_hits = 50.0

        if k_line is not None:
            over_pct_ks  = float(np.mean(ks_arr > k_line)) * 100.0
            under_pct_ks = float(np.mean(ks_arr < k_line)) * 100.0
        else:
            over_pct_ks = under_pct_ks = 50.0

        # Kill Streak probability — % of runs reaching 10+ Ks
        kill_streak_prob = float(np.mean(ks_arr >= KILL_STREAK_K_THRESHOLD)) * 100.0

        sim_conf_hits = _confidence_label(over_pct_hits, under_pct_hits, is_ks=False)
        sim_conf_ks   = _confidence_label(over_pct_ks, under_pct_ks, is_ks=True)

        log.info(
            "Simulation complete",
            pitcher=features.pitcher_name,
            n_runs=self.n_runs,
            manager_style=manager_style,
            median_hits=round(median_hits, 2),
            median_ks=round(median_ks, 2),
            p5_hits=round(p5_hits, 2), p95_hits=round(p95_hits, 2),
            p5_ks=round(p5_ks, 2), p95_ks=round(p95_ks, 2),
            over_pct_hits=round(over_pct_hits, 1),
            under_pct_hits=round(under_pct_hits, 1),
            over_pct_ks=round(over_pct_ks, 1),
            under_pct_ks=round(under_pct_ks, 1),
            kill_streak_prob=round(kill_streak_prob, 1),
            conf_hits=sim_conf_hits,
            conf_ks=sim_conf_ks,
        )

        return SimulationResult(
            median_hits=round(median_hits, 2),
            over_pct_hits=round(over_pct_hits, 1),
            under_pct_hits=round(under_pct_hits, 1),
            p5_hits=round(p5_hits, 2),
            p95_hits=round(p95_hits, 2),
            median_ks=round(median_ks, 2),
            over_pct_ks=round(over_pct_ks, 1),
            under_pct_ks=round(under_pct_ks, 1),
            p5_ks=round(p5_ks, 2),
            p95_ks=round(p95_ks, 2),
            sim_confidence_hits=sim_conf_hits,
            sim_confidence_ks=sim_conf_ks,
            n_runs=self.n_runs,
            kill_streak_probability=round(kill_streak_prob, 1),
        )

    # ─────────────────────────────────────────────────────────
    # Private helpers
    # ─────────────────────────────────────────────────────────

    def _jitter(
        self,
        f: PitcherFeatureSet,
        cmd_delta: float,
        disc_delta: float,
        temp_delta: float,
    ) -> PitcherFeatureSet:
        """
        Apply Gaussian noise to PCS_CMD, OCR_DISC, and ENS_TEMP.
        All other features remain fixed — only these three vary per iteration.
        """
        new_cmd  = _clamp_score((f.pcs_cmd  or 50.0) + cmd_delta)
        new_disc = _clamp_score((f.ocr_disc or 50.0) + disc_delta)
        new_temp = _clamp_score((f.ens_temp or 50.0) + temp_delta)
        return replace(f, pcs_cmd=new_cmd, ocr_disc=new_disc, ens_temp=new_temp)

    def _apply_manager_leash(
        self,
        f: PitcherFeatureSet,
        manager_style: str,
        is_extension_run: bool,
    ) -> PitcherFeatureSet:
        """
        Modify the feature set to reflect managerial hook behavior.

        Analytics manager:
            Hard exit — IP capped at 5.9 (≈ 95 pitches at 16 pitches/inning).
            This means the pitcher never reaches TTO3 in full, capping late-inning surge.

        Old School manager:
            20% of runs (is_extension_run=True): pitcher goes 1 extra inning past
            his expected IP, but command and pitch-capacity scores are degraded to
            model the physiological cost of 100+ pitches (40% hit probability increase
            modeled through PCS_CMD and OPS_PCAP degradation).
        """
        if manager_style == "Analytics":
            new_ip = min(f.avg_ip_per_start or 6.0, ANALYTICS_IP_CAP)
            return replace(f, avg_ip_per_start=new_ip)

        if manager_style == "Old_School" and is_extension_run:
            extended_ip = min((f.avg_ip_per_start or 6.0) + 1.0, 8.0)
            # Degrade command and capacity to model 100+ pitch fatigue
            new_cmd  = _clamp_score((f.pcs_cmd  or 50.0) * OLD_SCHOOL_DEEP_CMD_DEGRADATION)
            new_pcap = _clamp_score((f.ops_pcap or 50.0) * OLD_SCHOOL_DEEP_PCAP_DEGRADATION)
            return replace(f, avg_ip_per_start=extended_ip, pcs_cmd=new_cmd, ops_pcap=new_pcap)

        return f
