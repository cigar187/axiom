"""
test_third_pass.py — Axiom System Validation (All Three Passes)

Tests every fix made across passes 1, 2, and 3.
No database, no API keys, no server needed.
Run with: python3 test_third_pass.py
"""
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

PASS  = "\033[32m✓ PASS\033[0m"
FAIL  = "\033[31m✗ FAIL\033[0m"
TITLE = "\033[1m\033[34m"
RESET = "\033[0m"
WARN  = "\033[33m"

results = []

def check(label: str, condition: bool, detail: str = ""):
    status = PASS if condition else FAIL
    print(f"  {status}  {label}" + (f" — {detail}" if detail else ""))
    results.append((label, condition))

def section(title: str):
    print(f"\n{TITLE}{'─'*60}")
    print(f"  {title}")
    print(f"{'─'*60}{RESET}")


# ─────────────────────────────────────────────────────────────────────
# SECTION 1: ip_window.py — ceiling, tier table, fragility cap
# ─────────────────────────────────────────────────────────────────────
section("PASS 1 ▸ ip_window.py — IP ceiling and fragility cap")
from app.utils.ip_window import expected_ip, IP_CEILING, IP_FLOOR, _TIER_TABLE

check("IP_CEILING is 4.8", IP_CEILING == 4.8, f"got {IP_CEILING}")
check("IP_FLOOR  is 3.5",  IP_FLOOR  == 3.5,  f"got {IP_FLOOR}")

# Ace tier must not exceed the ceiling
ace_tier = next(m for t, m in _TIER_TABLE if t == 99)
check("Ace tier fallback ≤ 4.8", ace_tier <= 4.8, f"got {ace_tier}")

# Season avg above ceiling should be clamped
clamped = expected_ip(6.0, 8)
check("6.0 IP avg clamped to 4.8", clamped == 4.8, f"got {clamped}")

# Normal season avg passes through unchanged
normal = expected_ip(4.5, 5)
check("4.5 IP avg passes through as 4.5", normal == 4.5, f"got {normal}")

# Fragility cap only reduces
fragility_cap_result = expected_ip(4.8, 8, fragility_ip_cap=3.0)
check("Fragility cap 3.0 reduces 4.8 → 3.0", fragility_cap_result == 3.0,
      f"got {fragility_cap_result}")

# Fragility cap never inflates
fragility_no_inflate = expected_ip(3.6, 8, fragility_ip_cap=4.5)
check("Fragility cap 4.5 does NOT inflate 3.6", fragility_no_inflate == 3.6,
      f"got {fragility_no_inflate}")

# No data — should return 4.50 league default
default = expected_ip(None, None)
check("No data returns 4.50 default", default == 4.50, f"got {default}")


# ─────────────────────────────────────────────────────────────────────
# SECTION 2: pff.py — early-exit filter fixed to 0.1 IP
# ─────────────────────────────────────────────────────────────────────
section("PASS 1 ▸ pff.py — early exit now included (>= 0.1 IP)")
from app.utils.pff import compute_pff

# Build recent_form that includes a 1.2 IP shelling (1.67 decimal)
recent_form_with_early_exit = [
    {"ip": 1.67, "era_this_start": 27.0, "h9_this_start": 27.0, "k9_this_start": 0.0,
     "season_era": 4.50, "season_h9": 9.0, "season_k9": 7.5},
    {"ip": 5.0,  "era_this_start": 3.60, "h9_this_start": 8.5, "k9_this_start": 7.5,
     "season_era": 4.50, "season_h9": 9.0, "season_k9": 7.5},
    {"ip": 6.0,  "era_this_start": 2.25, "h9_this_start": 7.0, "k9_this_start": 9.0,
     "season_era": 4.50, "season_h9": 9.0, "season_k9": 7.5},
]
pff = compute_pff(recent_form_with_early_exit)

check("pff uses all 3 starts including 1.67 IP", pff["starts_used"] == 3,
      f"starts_used={pff['starts_used']}")
check("pff label is not NEUTRAL (early exit penalizes form)",
      pff["label"] != "NEUTRAL",
      f"label={pff['label']} pff={pff['pff']:.3f}")

# If we filter the early exit out manually, starts_used should drop to 2
recent_form_no_early_exit = [s for s in recent_form_with_early_exit if s["ip"] >= 2.0]
pff_no_early = compute_pff(recent_form_no_early_exit)
check("Without early exit: 2 starts used", pff_no_early["starts_used"] == 2,
      f"got {pff_no_early['starts_used']}")


# ─────────────────────────────────────────────────────────────────────
# SECTION 3: fragility.py — fires on early-exit recent form
# ─────────────────────────────────────────────────────────────────────
section("PASS 1 ▸ fragility.py — Fragility Index and TBAPI")
from app.utils.fragility import compute_fragility

# Trevor Rogers scenario: 1.67 IP, ERA 27.0
rogers_form = [
    {"ip": 1.67, "era_this_start": 27.0, "h9_this_start": 32.0, "k9_this_start": 0.0},
    {"ip": 5.1,  "era_this_start": 4.50, "h9_this_start": 9.0,  "k9_this_start": 7.2},
    {"ip": 6.0,  "era_this_start": 1.50, "h9_this_start": 6.0,  "k9_this_start": 9.0},
]
frag = compute_fragility(rogers_form)

check("Rogers FI tier is ELEVATED, HIGH, or EXTREME",
      frag["fi_tier"] in ("ELEVATED", "HIGH", "EXTREME"),
      f"fi_tier={frag['fi_tier']} fi_score={frag['fi_score']}")
check("Rogers FI hits_mult > 1.0", frag["fi_hits_mult"] > 1.0,
      f"fi_hits_mult={frag['fi_hits_mult']}")
check("Rogers FI ip_cap is not None", frag["fi_ip_cap"] is not None,
      f"fi_ip_cap={frag['fi_ip_cap']}")
check("Rogers TBAPI is ELEVATED or above",
      frag["tbapi_tier"] in ("ELEVATED", "HIGH", "EXTREME"),
      f"tbapi={frag['tbapi']:.3f} tier={frag['tbapi_tier']}")

# Clean pitcher should have no fragility
clean_form = [
    {"ip": 6.0, "era_this_start": 2.0, "h9_this_start": 7.5, "k9_this_start": 9.0},
    {"ip": 5.2, "era_this_start": 3.6, "h9_this_start": 8.0, "k9_this_start": 8.0},
    {"ip": 7.0, "era_this_start": 1.3, "h9_this_start": 6.0, "k9_this_start": 10.0},
]
frag_clean = compute_fragility(clean_form)

check("Clean pitcher FI tier is NONE", frag_clean["fi_tier"] == "NONE",
      f"fi_tier={frag_clean['fi_tier']} fi_score={frag_clean['fi_score']}")
check("Clean pitcher FI ip_cap is None", frag_clean["fi_ip_cap"] is None,
      f"fi_ip_cap={frag_clean['fi_ip_cap']}")
check("Clean pitcher TBAPI is NORMAL", frag_clean["tbapi_tier"] == "NORMAL",
      f"tbapi={frag_clean['tbapi']:.3f}")

# Empty form should return safe defaults
frag_empty = compute_fragility([])
check("Empty form: NONE tier, no multipliers",
      frag_empty["fi_tier"] == "NONE" and frag_empty["fi_hits_mult"] == 1.0,
      f"tier={frag_empty['fi_tier']} mult={frag_empty['fi_hits_mult']}")


# ─────────────────────────────────────────────────────────────────────
# SECTION 4: ml/features.py — default IP updated
# ─────────────────────────────────────────────────────────────────────
section("PASS 1 ▸ ml/features.py — default expected_ip updated to 4.8")
from app.ml.features import FEATURE_DEFAULTS

check("FEATURE_DEFAULTS expected_ip == 4.8",
      FEATURE_DEFAULTS.get("expected_ip") == 4.8,
      f"got {FEATURE_DEFAULTS.get('expected_ip')}")


# ─────────────────────────────────────────────────────────────────────
# SECTION 5: ml/engine.py — MIN_TRAINING_SAMPLES
# ─────────────────────────────────────────────────────────────────────
section("PASS 1 ▸ ml/engine.py — MIN_TRAINING_SAMPLES raised to 30")
from app.ml.engine import MIN_TRAINING_SAMPLES

check("MIN_TRAINING_SAMPLES == 30", MIN_TRAINING_SAMPLES == 30,
      f"got {MIN_TRAINING_SAMPLES}")


# ─────────────────────────────────────────────────────────────────────
# SECTION 6: simulation.py — constants corrected
# ─────────────────────────────────────────────────────────────────────
section("PASS 2 ▸ simulation.py — constants and multiplier chain")
from app.core.simulation import (
    ANALYTICS_IP_CAP,
    KILL_STREAK_K_THRESHOLD,
    YANK_HIT_TRIGGER,
    YANK_EXIT_IP,
)

check("ANALYTICS_IP_CAP == 4.5 (below 4.8 ceiling)", ANALYTICS_IP_CAP == 4.5,
      f"got {ANALYTICS_IP_CAP}")
check("YANK_EXIT_IP == 4.5", YANK_EXIT_IP == 4.5, f"got {YANK_EXIT_IP}")
check("KILL_STREAK_K_THRESHOLD == 10.0", KILL_STREAK_K_THRESHOLD == 10.0,
      f"got {KILL_STREAK_K_THRESHOLD}")

# Run a quick simulation with fragile pitcher features to check outputs
section("PASS 2 ▸ simulation.py — Monte Carlo with fragile pitcher (N=200)")
from app.core.simulation import SimulationEngine
from app.core.features import PitcherFeatureSet

# Build a minimal PitcherFeatureSet for the fragile case
f_fragile = PitcherFeatureSet(
    pitcher_id="99999",
    pitcher_name="Test Fragile",
    game_id="TEST001",
    avg_ip_per_start=4.8,
    mlb_service_years=3,
    season_hits_per_9=10.5,
    season_k_per_9=7.0,
    fi_ip_cap=3.0,           # EXTREME fragility
    fi_hits_mult=1.20,
    fi_tier="EXTREME",
    fi_score=75.0,
    tbapi_hits_mult=1.08,
    tbapi_tier="HIGH",
)

f_clean = PitcherFeatureSet(
    pitcher_id="88888",
    pitcher_name="Test Clean",
    game_id="TEST002",
    avg_ip_per_start=4.8,
    mlb_service_years=6,
    season_hits_per_9=8.0,
    season_k_per_9=9.5,
)

eng = SimulationEngine()

sim_fragile = eng.run(f_fragile, hits_line=3.5, k_line=4.5)
sim_clean   = eng.run(f_clean,   hits_line=3.5, k_line=4.5)

# The fragile pitcher (IP 3.0) will actually project FEWER total hits than the
# clean pitcher (IP 4.5), because shorter innings = fewer plate appearances overall.
# The fragility multipliers push hits UP but don't fully overcome the IP reduction.
# What we verify: fragile hits are HIGHER than they would be at the same IP without
# fragility — i.e., compare fragile to a clone with no multipliers at 3.0 IP.
f_fragile_no_mult = PitcherFeatureSet(
    pitcher_id="77777",
    pitcher_name="Test Fragile No Mult",
    game_id="TEST003",
    avg_ip_per_start=4.8,
    mlb_service_years=3,
    season_hits_per_9=10.5,
    season_k_per_9=7.0,
    fi_ip_cap=3.0,       # same IP cap...
    fi_hits_mult=1.0,    # ...but NO multipliers
    fi_tier="EXTREME",
)
sim_fragile_no_mult = eng.run(f_fragile_no_mult, hits_line=3.5, k_line=4.5)

check("Fragility multipliers INCREASE median hits vs same pitcher without them",
      sim_fragile.median_hits > sim_fragile_no_mult.median_hits,
      f"with_mult={sim_fragile.median_hits:.2f} vs no_mult={sim_fragile_no_mult.median_hits:.2f}")
check("Fragile sim_median_ks < clean sim_median_ks (IP cap limits Ks)",
      sim_fragile.median_ks < sim_clean.median_ks,
      f"fragile={sim_fragile.median_ks:.2f} vs clean={sim_clean.median_ks:.2f}")
check("Fragile p95_hits > 0", sim_fragile.p95_hits > 0,
      f"p95_hits={sim_fragile.p95_hits:.2f}")
check("Fragile p95_hits under hard cap 15",
      sim_fragile.p95_hits < 15.0,
      f"p95_hits={sim_fragile.p95_hits:.2f}")
check("Clean p95_hits ≤ 4.8 ceiling effect (should be < 10)",
      sim_clean.p95_hits < 10.0,
      f"p95_hits={sim_clean.p95_hits:.2f}")


# ─────────────────────────────────────────────────────────────────────
# SECTION 7-9: Source-code checks (read files directly to avoid
#              import-chain side effects from bs4/BeautifulSoup, etc.)
# ─────────────────────────────────────────────────────────────────────
import pathlib
BASE = pathlib.Path(__file__).parent

section("PASS 3 ▸ mlb_stats.py — early-exit IP filter corrected to 0.1")
mlb_src = (BASE / "app/services/mlb_stats.py").read_text()
# Isolate only the fetch_pitcher_recent_form function text
fn_start = mlb_src.find("def fetch_pitcher_recent_form")
fn_end   = mlb_src.find("\n    async def ", fn_start + 1)
fn_src   = mlb_src[fn_start:fn_end] if fn_end > 0 else mlb_src[fn_start:]

old_filter = "ip < 2.0" in fn_src
new_filter = "ip < 0.1" in fn_src

check("Old 'ip < 2.0' filter removed from fetch_pitcher_recent_form",
      not old_filter,
      "STILL PRESENT — early exits silently dropped" if old_filter else "")
check("New 'ip < 0.1' filter in place in fetch_pitcher_recent_form",
      new_filter,
      "MISSING" if not new_filter else "")

section("PASS 3 ▸ backfill.py — recent_form filter consistent with live system")
bf_src = (BASE / "app/ml/backfill.py").read_text()
fn2_start = bf_src.find("def _fetch_pitcher_context")
fn2_end   = bf_src.find("\ndef ", fn2_start + 1)
fn2_src   = bf_src[fn2_start:fn2_end] if fn2_end > 0 else bf_src[fn2_start:]

old_bf = "ip < 2.0" in fn2_src
new_bf = "ip < 0.1" in fn2_src

check("Old 'ip < 2.0' filter removed from _fetch_pitcher_context",
      not old_bf,
      "STILL PRESENT" if old_bf else "")
check("New 'ip < 0.1' filter in place in _fetch_pitcher_context",
      new_bf,
      "MISSING" if not new_bf else "")

section("PASS 3 ▸ pipeline.py — ML training sample uses fragility_ip_cap")
pipe_src = (BASE / "app/tasks/pipeline.py").read_text()
check("ML sample builder passes fragility_ip_cap to expected_ip",
      "fragility_ip_cap" in pipe_src,
      "MISSING — ML will over-project fragile pitchers" if "fragility_ip_cap" not in pipe_src else "")


# ─────────────────────────────────────────────────────────────────────
# SECTION 10: show_report.py — VOLATILE threshold and FI column
# ─────────────────────────────────────────────────────────────────────
section("PASS 3 ▸ show_report.py — VOLATILE threshold and FI column")
import importlib.util, pathlib

rp = pathlib.Path(__file__).parent / "show_report.py"
report_source = rp.read_text()

# Check for the actual code line (not a comment) using the if-statement text
classify_block = report_source.split("def classify")[1]
volatile_line  = next((l for l in classify_block.splitlines() if "VOLATILE" in l and "return" in l), "")
check("VOLATILE threshold uses 8.5 (not 9.0)",
      "8.5" in volatile_line and "kc" in volatile_line,
      f"volatile line: {volatile_line.strip()}")
check("FI column header present in report",
      "'FI'" in report_source or "FI" in report_source,
      "FI column not found in header")
check("Fragility legend printed at bottom of report",
      "FRAGILITY EXTREME" in report_source,
      "Legend missing")


# ─────────────────────────────────────────────────────────────────────
# SECTION 11: mgs.py — Death Trap does not fire pre-game at 4.8 IP
# ─────────────────────────────────────────────────────────────────────
section("SANITY ▸ mgs.py — Death Trap correctly inactive at 4.8 IP")
from app.utils.mgs import compute_mgs

# At 4.8 IP, tto3_ip = max(0, 4.8 - 5.0) = 0. Death trap should not fire.
hits_m, ks_m, label = compute_mgs(4.8, baserunners_l2=5.0, silent=True)
check("Death Trap does NOT fire at 4.8 IP (no TTO3 innings)",
      label != "DEATH_TRAP",
      f"label={label}")

# At 5.8 IP (old-school extension), TTO3 = 0.8 innings — Death Trap CAN fire
hits_m2, ks_m2, label2 = compute_mgs(5.8, baserunners_l2=5.0, silent=True)
check("Death Trap fires correctly at 5.8 IP with high baserunners",
      label2 == "DEATH_TRAP",
      f"label={label2} (expected DEATH_TRAP)")


# ─────────────────────────────────────────────────────────────────────
# SUMMARY
# ─────────────────────────────────────────────────────────────────────
print(f"\n{'═'*62}")
total   = len(results)
passed  = sum(1 for _, ok in results if ok)
failed  = total - passed

if failed == 0:
    print(f"\033[32m\033[1m  ALL {total} CHECKS PASSED — System is clean.\033[0m")
else:
    print(f"\033[31m\033[1m  {failed} of {total} CHECKS FAILED:\033[0m")
    for label, ok in results:
        if not ok:
            print(f"    \033[31m✗ {label}\033[0m")
print(f"\n{'═'*62}\n")
