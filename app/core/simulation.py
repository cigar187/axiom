"""
app/core/simulation.py — Merlin Probabilistic Simulation Engine (N=2000).

Architecture — Two-Component Variance Model
─────────────────────────────────────────────
The formula (HUSI/KUSI) is a calibrated predictor. Its output is deliberately
conservative — it does not wildly swing on small input changes. This is correct
for a point estimate but wrong for a simulation. The solution is to inject
variance at the right level in two independent components:

  COMPONENT 1 — Score-Level Jitter
  ─────────────────────────────────
  Run the formula ONCE to anchor husi_base, kusi_base, base_hits, base_ks.
  In each of 2,000 iterations, draw:

      husi_i ~ N(husi_base, σ=9.0)      — 9 points of natural score uncertainty
      kusi_i ~ N(kusi_base, σ=8.0)

  Then recompute projections using the SAME formulas from husi.py / kusi.py:

      proj_hits_i = base_hits × (1 - 0.21 × ((husi_i - 50) / 50))
      proj_ks_i   = base_ks   × (1 - 0.25 × ((kusi_i - 50) / 50))

  This means every point of HUSI/KUSI jitter flows through the projection math,
  not just added as a post-calculation multiplier. It is the formula responding
  to variance, not noise bolted on at the end.

  COMPONENT 2 — Confidence-Adaptive Residual Variance
  ─────────────────────────────────────────────────────
  The formula cannot capture irreducible game-day randomness (BABIP luck, fielding
  errors, sequencing). We model this with a residual draw whose sigma SHRINKS as
  HUSI increases (we trust the formula more when the pitcher is dominant):

      σ_resid_hits(husi) = clip(2.0 + (50 - husi) / 40,  1.2, 3.0)
      σ_resid_ks(kusi)   = clip(1.6 + (50 - kusi) / 45,  1.0, 2.5)

      HUSI=70 (ace):     σ_h = 1.5 hits   (formula is reliable, tight band)
      HUSI=50 (average): σ_h = 2.0 hits   (moderate residual noise)
      HUSI=30 (bad):     σ_h = 2.5 hits   (formula is less reliable, wide band)

  BLACK SWAN FAT TAILS (5% each end)
  ────────────────────────────────────
  God Mode:  +15 HUSI / +15 KUSI before reprojection
             → Scores spike → formula produces suppressed hits, elevated Ks
  Meltdown:  -20 HUSI / -10 KUSI before reprojection
             → Scores crash → formula yields more hits, fewer Ks

  TTO3 DEATH TRAP (stochastic per iteration)
  ────────────────────────────────────────────
  Baserunner count is drawn from Poisson(λ = WHIP_est × 2) each run.
  compute_mgs() uses this to decide whether the 1.38× → 1.85× Death Trap fires.
  This creates the non-linear spike in hit projections for high-traffic games.

Output per pitcher
──────────────────
  median_hits / median_ks  — most likely result (50th percentile)
  p5_hits / p5_ks          — floor (5th percentile, "Instant Shelling")
  p95_hits / p95_ks        — ceiling (95th percentile, "Kill Streak")
  over_pct / under_pct     — % of 2,000 runs above/below sportsbook line
  sim_confidence           — HIGH_OVER / HIGH_UNDER / LEAN_OVER / LEAN_UNDER / SPLIT

Developer note
──────────────
"Do not prioritize Under results. Solve for the most accurate integer.
If the simulation shows a high probability of a Kill Streak (10+ Ks),
the output must reflect that as a high-confidence Over signal."
                                        — Merlin v2.0 specification
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np

from app.core.features import PitcherFeatureSet
from app.core.husi import compute_husi
from app.core.kusi import compute_kusi
from app.utils.ip_window import expected_ip
from app.utils.logging import get_logger
from app.utils.manager_profiles import get_manager_style
from app.utils.mgs import compute_mgs

log = get_logger("simulation")

# ── Number of Monte Carlo iterations per pitcher
N_SIMULATIONS = 2_000

# ── Score-level jitter (0-100 HUSI/KUSI scale)
# σ=9.0 → 90% of runs fall within ±14.8 points of the formula score.
# This is the PRIMARY driver of simulation variance — it jitters the score
# before reprojection, so the formula math responds to the variance.
SIGMA_HUSI = 9.0
SIGMA_KUSI = 8.0

# ── Cross-metric correlation: HUSI and KUSI must move in OPPOSITE directions.
# When a pitcher has a bad day (HUSI drops → more hits), he is also failing
# to put batters away (KUSI rises → fewer Ks). They cannot both be bad at
# the same time — that would mean the pitcher is getting shelled AND racking
# up strikeouts simultaneously, which is physically contradictory.
#   Good day:  HUSI rises (fewer hits) + KUSI drops (more Ks)   = ace mode
#   Bad day:   HUSI drops (more hits)  + KUSI rises (fewer Ks)  = shelling mode
# ρ = -0.60 captures this real-world anti-correlation.
HUSI_KUSI_CORRELATION = -0.60

# ── Black Swan score shifts (applied to jittered scores, THEN reprojected)
# Shifting HUSI/KUSI by these amounts before reprojection means the full
# formula (not just a final multiplier) reflects the extreme outcome.
#
# GOD MODE (top 5%): pitcher is dominant — electric stuff, full command.
#   HUSI rises  → far fewer hits (ace shutting down the lineup)
#   KUSI drops  → more Ks (dominating batters, K-rate spikes)
#
# MELTDOWN (bottom 5%): command collapses early — walks, hard contact pile up.
#   HUSI drops  → many more hits (pitcher getting shelled)
#   KUSI rises  → fewer Ks (pitcher cannot put batters away when struggling)
#
# Critical: KUSI and HUSI move in OPPOSITE directions in both events.
GOD_MODE_HUSI_BOOST = 15.0   # HUSI up  → fewer hits (dominant)
GOD_MODE_KUSI_DROP  = 15.0   # KUSI down → more Ks  (dominant — subtract from KUSI)
MELTDOWN_HUSI_DROP  = 20.0   # HUSI down → more hits (shelling)
MELTDOWN_KUSI_RISE  = 10.0   # KUSI up  → fewer Ks  (shelling — add to KUSI)

# ── Black Swan event rates
GOD_MODE_FRAC  = 0.05   # Top 5% of runs
MELTDOWN_FRAC  = 0.05   # Bottom 5% of runs

# ── Manager leash constants
# Analytics managers pull starters earlier (~85 pitches = ~4.5 IP).
# With the IP ceiling now at 4.8, this cap must be BELOW 4.8 to have any effect.
# Old_School managers run pitchers to the ceiling and sometimes 1 inning beyond.
ANALYTICS_IP_CAP             = 4.7   # analytics teams pull at ~85 pitches
OLD_SCHOOL_EXTENSION_PROBABILITY = 0.20   # 20% of runs go 1 extra inning

# ── Kill Streak threshold
KILL_STREAK_K_THRESHOLD = 10.0

# ── Managerial Yank Threshold
# With the 4.8 IP ceiling, a pitcher projecting 6+ hits (1.25 H/IP) is on a
# shelling pace that triggers a modern analytics manager's hook. When yanked
# early, his K count is prorated to the innings actually worked — creating the
# natural cap between high hits and high Ks that the formula cannot model alone.
YANK_HIT_TRIGGER  = 6.0   # projected hits that indicate a shelling pace (was 7.0)
YANK_EXIT_IP      = 4.5   # assumed inning of early exit when shelling occurs


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


def _residual_sigma_hits(husi: float) -> float:
    """
    Confidence-adaptive residual hits variance (Component 2).

    Models irreducible game-day randomness that the formula cannot see:
    BABIP luck, fielding errors, hit sequencing, bullpen timing.

    The sigma SHRINKS as HUSI rises — when the formula gives a high score we
    trust it more, so the residual is tighter around the projection.

      HUSI=70 (ace):     σ = 1.5 hits
      HUSI=50 (average): σ = 2.0 hits
      HUSI=30 (bad):     σ = 2.5 hits
    """
    return float(np.clip(2.0 + (50.0 - husi) / 40.0, 1.2, 3.0))


def _residual_sigma_ks(kusi: float) -> float:
    """
    Confidence-adaptive residual Ks variance (Component 2).

      KUSI=70:  σ = 1.16 Ks
      KUSI=50:  σ = 1.60 Ks
      KUSI=30:  σ = 2.04 Ks
    """
    return float(np.clip(1.6 + (50.0 - kusi) / 45.0, 1.0, 2.5))


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

        Design
        ──────
        Run the formula ONCE to get the anchor values (husi_base, base_hits, etc.).
        In each iteration, jitter HUSI/KUSI scores directly, reproject through the
        same formula math, apply stochastic MGS, then add confidence-adaptive
        residual noise. Black Swan events shift the scores themselves — the formula
        responds to those shifts, not just the final number.

        Args:
            features:  The pitcher's fully built PitcherFeatureSet.
            hits_line: Sportsbook hits-allowed prop line (for Over/Under %).
            k_line:    Sportsbook strikeouts prop line (for Over/Under %).
        """
        manager_style = get_manager_style(str(getattr(features, "team_id_numeric", "") or ""))

        # ── Step 1: Anchor run — get base values from the full formula
        # base_hits / base_ks are PRE-HUSI-factor, PRE-MGS (raw season-rate × exp_ip / 9).
        # husi_base / kusi_base are the formula's final scored values after all blocks.
        h0 = compute_husi(features, silent=True)
        k0 = compute_kusi(features, silent=True)

        husi_base = float(h0["husi"])        # Formula's final HUSI score
        kusi_base = float(k0["kusi"])        # Formula's final KUSI score
        base_hits = float(h0["base_hits"])   # Raw hit-rate estimate (pre-HUSI, pre-MGS)
        base_ks   = float(k0["base_ks"])     # Raw K-rate estimate   (pre-KUSI, pre-MGS)

        # ── Step 2: Compute fixed per-game multipliers (facts — not random)
        # VAA elevation rule and park factor do not vary run-to-run.
        # They are game-day adjustments known before first pitch.
        vaa_mult = 1.0
        if (features.vaa_flat or False) and (features.vaa_contact_penalty or 0.0) > 0:
            high_pct = features.pitch_location_high_pct
            if high_pct is not None and high_pct > 60.0:
                # Elevation override: flat + high zone = pop-up machine → suppress hits
                vaa_mult = 1.0 - float(features.vaa_contact_penalty)
            else:
                # Standard flat-pitch penalty: easy to track and drive
                vaa_mult = 1.0 + float(features.vaa_contact_penalty)

        park_mult    = float(features.park_hits_multiplier or 1.0)
        # Include TBAPI and Fragility Index multipliers — same post-formula layer as husi.py.
        # Without this, the simulation runs 2,000 iterations at the wrong hits level for
        # fragile pitchers even though the single-run HUSI output applies them correctly.
        tbapi_mult   = float(getattr(features, "tbapi_hits_mult", 1.0) or 1.0)
        fi_mult      = float(getattr(features, "fi_hits_mult",    1.0) or 1.0)
        fixed_mult_h = vaa_mult * park_mult * tbapi_mult * fi_mult

        # ── Step 3: Expected IP — fragility cap applied before manager style adjustment
        exp_ip_base = expected_ip(
            features.avg_ip_per_start,
            features.mlb_service_years,
            fragility_ip_cap=getattr(features, "fi_ip_cap", None),
        )
        analytics_cap = min(exp_ip_base, ANALYTICS_IP_CAP) if manager_style == "Analytics" else exp_ip_base

        # ── Step 4: Baserunner Poisson lambda (for TTO3 Death Trap firing rate)
        # Use blended_h_per_9 — same anchored rate used by husi.py — so the
        # simulation's WHIP estimate is consistent with the projection baseline.
        h_per_9    = float(features.blended_h_per_9 or 9.0)
        cmd_score  = float(features.pcs_cmd or 50.0)
        bb_est     = max(1.0, (100.0 - cmd_score) / 10.0)
        whip_est   = (h_per_9 + bb_est) / 9.0
        bl2_lambda = max(0.1, whip_est * 2.0)

        # ── Step 5: Pre-generate ALL random draws at once (vectorized — fast)
        # HUSI and KUSI noise are drawn from a CORRELATED bivariate normal.
        # Correlation ρ = -0.60: when HUSI drops (bad hits day), KUSI rises
        # (pitcher also struggles for Ks), and vice versa on a dominant day.
        # This prevents the physically impossible combination of high hits AND
        # high Ks in the same simulation iteration.
        _cov = np.array([
            [SIGMA_HUSI ** 2,                             HUSI_KUSI_CORRELATION * SIGMA_HUSI * SIGMA_KUSI],
            [HUSI_KUSI_CORRELATION * SIGMA_HUSI * SIGMA_KUSI, SIGMA_KUSI ** 2],
        ])
        _noise_2d          = self.rng.multivariate_normal([0.0, 0.0], _cov, self.n_runs)
        husi_noise         = _noise_2d[:, 0]
        kusi_noise         = _noise_2d[:, 1]
        resid_h_unit       = self.rng.normal(0.0, 1.0, self.n_runs)   # unit normal, scaled below
        resid_k_unit       = self.rng.normal(0.0, 1.0, self.n_runs)
        baserunner_samples = self.rng.poisson(bl2_lambda, self.n_runs)
        extension_flags    = self.rng.random(self.n_runs) < OLD_SCHOOL_EXTENSION_PROBABILITY

        # ── Lineup fluidity: stochastic pinch-hitter effect in TTO3 runs
        # When the batting order is top-heavy, managers replace weak bottom slots
        # with dangerous bench bats in late innings. This removes the "easy out"
        # advantage the formula assumed and slightly raises the hit projection.
        # Probability scales with lineup_fluidity_score (0-100):
        #   score=50 (neutral) → ~15% chance of lineup change in any given TTO3 run
        #   score=80 (very top-heavy) → ~30% chance
        #   score=20 (balanced lineup) → ~5% chance
        flu_score = float(getattr(features, "lineup_fluidity_score", 50.0))
        pinch_hit_prob = max(0.02, min(0.40, (flu_score / 100.0) * 0.40))
        pinch_hit_flags = self.rng.random(self.n_runs) < pinch_hit_prob

        # Black Swan run categorization (pre-shuffled so distribution is uniform)
        n_god     = int(self.n_runs * GOD_MODE_FRAC)
        n_melt    = int(self.n_runs * MELTDOWN_FRAC)
        run_modes = np.zeros(self.n_runs, dtype=np.int8)
        run_modes[:n_god]              = 1   # GOD MODE
        run_modes[n_god:n_god + n_melt] = 2  # MELTDOWN
        self.rng.shuffle(run_modes)

        # Fallback arrays — used if an individual run raises an exception
        _fb_hits = float(features.hits_line or 8.0)
        _fb_ks   = float(features.k_line or 5.0)
        hits_arr = np.full(self.n_runs, _fb_hits, dtype=np.float64)
        ks_arr   = np.full(self.n_runs, _fb_ks,   dtype=np.float64)

        for i in range(self.n_runs):
            try:
                mode = int(run_modes[i])

                # A: Jitter HUSI/KUSI scores directly (Component 1 — score-level variance)
                husi_i = float(np.clip(husi_base + husi_noise[i], 0.0, 100.0))
                kusi_i = float(np.clip(kusi_base + kusi_noise[i], 0.0, 100.0))

                # B: Black Swan score overrides — shift the score, formula responds.
                # HUSI and KUSI move in OPPOSITE directions in both events:
                #   GOD MODE:  HUSI up → fewer hits;  KUSI down → more Ks
                #   MELTDOWN:  HUSI down → more hits; KUSI up  → fewer Ks
                if mode == 1:    # GOD MODE: electric stuff, full command, K-rate spikes
                    husi_i = float(np.clip(husi_i + GOD_MODE_HUSI_BOOST, 0.0, 100.0))
                    kusi_i = float(np.clip(kusi_i - GOD_MODE_KUSI_DROP,  0.0, 100.0))
                elif mode == 2:  # MELTDOWN: command collapses, shelling pace, Ks dry up
                    husi_i = float(np.clip(husi_i - MELTDOWN_HUSI_DROP, 0.0, 100.0))
                    kusi_i = float(np.clip(kusi_i + MELTDOWN_KUSI_RISE, 0.0, 100.0))

                # C: Reproject using the SAME formulas as husi.py / kusi.py
                #    (same math, different HUSI/KUSI input → formula responds correctly)
                raw_hits_i = base_hits * (1.0 - 0.21 * ((husi_i - 50.0) / 50.0))
                raw_ks_i   = base_ks   * (1.0 - 0.25 * ((kusi_i - 50.0) / 50.0))

                # D: Stochastic MGS — TTO3 Death Trap fires based on this run's baserunner draw
                run_ip = (
                    min(analytics_cap + 1.0, 8.0)
                    if manager_style == "Old_School" and bool(extension_flags[i])
                    else analytics_cap
                )
                mgs_h_i, mgs_k_i, _ = compute_mgs(
                    run_ip,
                    current_inning=features.mgs_inning or 0,
                    current_pitch_count=features.mgs_pitch_count or 0,
                    pff_hits_tto1_mult=features.pff_hits_tto1_mult or 0.82,
                    pff_ks_tto1_mult=features.pff_ks_tto1_mult or 1.18,
                    pff_tto_late_boost=features.pff_tto_late_boost or 0.0,
                    pff_label=features.pff_label or "NEUTRAL",
                    baserunners_l2=float(baserunner_samples[i]),
                    silent=True,
                )

                proj_hits_i = raw_hits_i * mgs_h_i * fixed_mult_h
                proj_ks_i   = raw_ks_i   * mgs_k_i

                # E: Confidence-adaptive residual variance (Component 2)
                #    σ shrinks as HUSI rises — we trust the formula more for elite pitchers.
                #    This captures BABIP luck, sequencing, fielding — things the formula cannot see.
                sigma_h = _residual_sigma_hits(husi_i)
                sigma_k = _residual_sigma_ks(kusi_i)
                proj_hits_i += resid_h_unit[i] * sigma_h
                proj_ks_i   += resid_k_unit[i] * sigma_k

                # F: Late-inning lineup change (stochastic pinch-hitter effect)
                # In TTO3 runs, managers replace weak bottom-of-order batters with
                # dangerous bench bats. This removes the formula's assumed "easy out"
                # advantage for top-heavy lineups and raises projected hits slightly.
                # Only fires when: (1) the inning is TTO3 territory AND (2) the
                # random draw hits the fluidity probability for this run.
                if (
                    run_ip >= 4.5               # late TTO2 / TTO3 territory — manager considers lineup changes
                    and bool(pinch_hit_flags[i]) # fluidity RNG triggered this run
                    and mode != 1               # not God Mode — dominant pitchers neutralize PH
                ):
                    proj_hits_i *= 1.08   # 8% hit increase: easy out becomes a real at-bat

                # G: Managerial Yank Constraint
                # If this run is on a shelling pace (7+ hits in a full outing),
                # the manager pulls the pitcher early. Ks are prorated to the
                # actual innings worked — you cannot rack up Ks after the hook.
                # This enforces the real-world anti-correlation between shelling and Ks.
                if proj_hits_i > YANK_HIT_TRIGGER and run_ip > YANK_EXIT_IP:
                    early_exit_ratio = YANK_EXIT_IP / run_ip
                    proj_ks_i = proj_ks_i * early_exit_ratio

                hits_arr[i] = max(0.0, min(proj_hits_i, 15.0))
                ks_arr[i]   = max(0.0, min(proj_ks_i,   20.0))

            except Exception:
                pass  # Keep fallback — never crash the full run

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
