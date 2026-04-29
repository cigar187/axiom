"""
Axiom Daily Pitcher Intelligence Report — Full Board
"""
import json
import subprocess

GREEN   = "\033[92m"
YELLOW  = "\033[93m"
RED     = "\033[91m"
MAGENTA = "\033[95m"
CYAN    = "\033[96m"
BOLD    = "\033[1m"
RESET   = "\033[0m"

def husi_color(s):
    try:
        v = float(s)
        if v >= 65: return GREEN
        if v >= 52: return YELLOW
        return RED
    except: return RESET

def kusi_color(s):
    try:
        v = float(s)
        if v >= 52: return GREEN
        if v >= 40: return YELLOW
        return RED
    except: return RESET

def ml_proj_hits(sim_median_h, husi, ml_h):
    """Convert ML-H score into projected hits using same formula math as HUSI."""
    try:
        f1 = 1.0 - 0.21 * ((float(husi)  - 50.0) / 50.0)
        f2 = 1.0 - 0.21 * ((float(ml_h)  - 50.0) / 50.0)
        if f1 <= 0: return float(sim_median_h)
        return round(float(sim_median_h) * f2 / f1, 1)
    except: return None

def ml_proj_ks(sim_median_k, kusi, ml_k):
    """Convert ML-K score into projected Ks using same formula math as KUSI."""
    try:
        f1 = 1.0 - 0.25 * ((float(kusi)  - 50.0) / 50.0)
        f2 = 1.0 - 0.25 * ((float(ml_k)  - 50.0) / 50.0)
        if f1 <= 0: return float(sim_median_k)
        return round(float(sim_median_k) * f2 / f1, 1)
    except: return None

def signal_label(ml_h_proj, formula_hm, ml_k_proj, formula_km):
    """Compare ML projections vs formula projections."""
    try:
        h_diff = abs(float(ml_h_proj) - float(formula_hm))
        k_diff = abs(float(ml_k_proj) - float(formula_km))
        avg    = (h_diff + k_diff) / 2
        if avg >= 2.5: return RED    + BOLD, "CONFLICT "
        if avg >= 1.5: return YELLOW,        "DIVERGENT"
        if avg >= 0.7: return CYAN,          "SLIGHT   "
        return GREEN,                        "ALIGNED  "
    except: return RESET, "---      "

def label_color(label):
    if "KILLER"   in label: return GREEN   + BOLD
    if "VOLATILE" in label: return RED     + BOLD
    if "SHELLED"  in label: return MAGENTA + BOLD
    return RESET

# ── Fetch
result = subprocess.check_output([
    "curl", "-s", "https://axiom-api-965804388585.us-central1.run.app/v1/pitchers/today"
], text=True)

data     = json.loads(result)
pitchers = data if isinstance(data, list) else data.get("pitchers", [])
if not pitchers:
    print("No pitchers found."); exit()

def fv(p, k):
    v = p.get(k)
    return float(v) if v is not None else 0.0

rows = []
for p in pitchers:
    husi = fv(p,"husi_score") or fv(p,"husi")
    kusi = fv(p,"kusi_score") or fv(p,"kusi")
    hm   = fv(p,"sim_median_hits")
    km   = fv(p,"sim_median_ks")
    ml_h = fv(p,"ml_husi")
    ml_k = fv(p,"ml_kusi")

    ml_h_proj = ml_proj_hits(hm, husi, ml_h) if ml_h else None
    ml_k_proj = ml_proj_ks(km,  kusi, ml_k)  if ml_k else None

    # Fragility flag: mark pitchers with FRAGILITY or TBAPI risk in their flags.
    # The API returns risk_flags as a pipe-delimited string e.g. "ERA_STRUGGLING|FRAGILITY_HIGH".
    risk_flags_str = p.get("risk_flags") or ""
    frag_marker = ""
    if "FRAGILITY_EXTREME" in risk_flags_str or "TBAPI_EXTREME" in risk_flags_str:
        frag_marker = "!!"   # double-bang: extreme fragility — high-confidence early exit risk
    elif "FRAGILITY_HIGH" in risk_flags_str or "TBAPI_HIGH" in risk_flags_str:
        frag_marker = "! "   # single-bang: elevated fragility
    elif "FRAGILITY_ELEVATED" in risk_flags_str or "TBAPI_ELEVATED" in risk_flags_str:
        frag_marker = "~ "   # tilde: mild fragility warning

    rows.append({
        "name":      (p.get("pitcher") or "?")[:22],
        "opp":       (p.get("opponent") or p.get("opp") or "?")[:4],
        "husi":      husi,
        "hgrd":      (p.get("husi_grade") or p.get("grade") or "--")[:2],
        "kusi":      kusi,
        "kgrd":      (p.get("kusi_grade") or "--")[:2],
        "hf":        fv(p,"sim_p5_hits"),
        "hm":        hm,
        "hc":        fv(p,"sim_p95_hits"),
        "kf":        fv(p,"sim_p5_ks"),
        "km":        km,
        "kc":        fv(p,"sim_p95_ks"),
        "ml_h_proj": ml_h_proj,
        "ml_k_proj": ml_k_proj,
        "frag":      frag_marker,
    })

# ── Correlation Penalty
h_meds_d  = sorted([r["hm"] for r in rows], reverse=True)
top20_cut = h_meds_d[max(0, int(len(h_meds_d)*0.20)-1)]
for r in rows:
    r["pen"] = r["hm"] >= top20_cut
    if r["pen"]: r["kc"] = round(r["kc"]*0.85, 1)

# ── Classification
h_ceils_s = sorted([r["hc"] for r in rows])
h30       = h_ceils_s[int(len(h_ceils_s)*0.30)]
k_ceils_s = sorted([r["kc"] for r in rows], reverse=True)
k_top10   = k_ceils_s[min(9, len(k_ceils_s)-1)]
k_meds_s  = sorted([r["km"] for r in rows])
k30       = k_meds_s[int(len(k_meds_s)*0.30)]
h_meds_s  = sorted([r["hm"] for r in rows], reverse=True)
h_top30   = h_meds_s[int(len(h_meds_s)*0.30)]

def classify(r):
    # Thresholds recalibrated for the 4.8 IP ceiling.
    # With max IP at 4.8, p95 hits tops ~8.5 and p95 Ks top ~9.5 for most starters.
    # Old thresholds (hc>9.0 and kc>9.0) were essentially unreachable and
    # suppressed the VOLATILE label entirely.
    if r["kc"] > 8.5 and r["hc"] > 7.5: return "VOLATILE"
    if r["kc"] >= k_top10 and r["hc"] <= h30: return "KILLER"
    if r["hm"] >= h_top30 and r["km"] <= k30: return "SHELLED"
    return "NEUTRAL"

for r in rows:
    r["label"] = classify(r)

order = {"KILLER":0,"NEUTRAL":1,"SHELLED":2,"VOLATILE":3}
rows.sort(key=lambda r: (order.get(r["label"],9), -r["km"]))

# ── Header
# FI column: fragility marker — !! = EXTREME, !  = HIGH, ~  = ELEVATED, blank = NONE
W = 132
HDR = (f"{'LABEL':<10} {'PITCHER':<22} {'OPP':<4} {'FI':<3}"
       f"{'HUSI':>5}{'H':>2} {'H-FL':>5} {'H-MD':>5} {'H-CL':>5}  "
       f"{'KUSI':>5}{'K':>2} {'K-FL':>5} {'K-MD':>5} {'K-CL':>5}  "
       f"{'E2-H':>5} {'E2-K':>5} {'SIGNAL':<9}")
print(f"\n{BOLD}{HDR}{RESET}")
print("─" * W)

prev_label = None
for r in rows:
    if prev_label is not None and r["label"] != prev_label:
        print()
    prev_label = r["label"]

    e2h = r["ml_h_proj"]
    e2k = r["ml_k_proj"]
    sc, sl = signal_label(e2h, r["hm"], e2k, r["km"])
    pen    = "~" if r["pen"] else " "
    e2h_s  = f"{e2h:5.1f}" if e2h is not None else "   --"
    e2k_s  = f"{e2k:5.1f}" if e2k is not None else "   --"
    lbl    = f"[{r['label']}]"
    name   = r["name"][:22]

    # Color-code the fragility marker: red for extreme, yellow for elevated
    frag   = r.get("frag", "")
    if frag == "!!":
        frag_s = RED + BOLD + f"{frag:<3}" + RESET
    elif frag == "! ":
        frag_s = YELLOW + f"{frag:<3}" + RESET
    else:
        frag_s = f"{frag:<3}"

    print(
        f"{label_color(r['label'])}{lbl:<10}{RESET} {name:<22} {r['opp']:<4} "
        f"{frag_s}"
        f"{husi_color(r['husi'])}{r['husi']:5.1f}{r['hgrd']:>2} {r['hf']:5.1f} {r['hm']:5.1f} {r['hc']:5.1f}{RESET}  "
        f"{kusi_color(r['kusi'])}{r['kusi']:5.1f}{r['kgrd']:>2} {r['kf']:5.1f} {r['km']:5.1f} {r['kc']:5.1f}{pen}{RESET}  "
        f"{sc}{e2h_s} {e2k_s} {sl}{RESET}"
    )

# ── Summaries
clean    = [r for r in rows if r["label"]=="KILLER"]
volatile = [r for r in rows if r["label"]=="VOLATILE"]
shell    = [r for r in rows if r["label"]=="SHELLED"]

print(f"\n{GREEN}{BOLD}── KILLERS  (K Over plays — low hit risk, manager keeps them in){RESET}")
for r in clean:
    e2h = r["ml_h_proj"]; e2k = r["ml_k_proj"]
    sc,sl = signal_label(e2h, r["hm"], e2k, r["km"])
    print(f"   ✓  {r['name']:<22}  E1: H {r['hm']:4.1f} / K {r['km']:4.1f}  |  E2: H {e2h or '--':>4} / K {e2k or '--':>4}  |  {sc}{sl.strip()}{RESET}")

if volatile:
    print(f"\n{RED}{BOLD}── VOLATILE  (Avoid parlays — Early Exit Risk){RESET}")
    for r in volatile:
        print(f"   ✗  {r['name']:<22}  K-CEIL {r['kc']:4.1f} vs H-CEIL {r['hc']:4.1f}  COIN FLIP")

print(f"\n{MAGENTA}{BOLD}── SHELLED  (Hit props — hits pile up before the hook){RESET}")
for r in shell:
    e2h = r["ml_h_proj"]
    print(f"   ⚡  {r['name']:<22}  E1 H-MED {r['hm']:4.1f} | E2 H-PROJ {e2h or '--':>4} | H-CEIL {r['hc']:4.1f}")

print(f"\n{YELLOW}~ K-CEIL -15% Correlation Penalty (H-MED top 20%){RESET}")
print(f"{CYAN}SIGNAL compares E2 projected stats vs E1 median: ALIGNED / SLIGHT / DIVERGENT / CONFLICT{RESET}")
print(f"{GREEN}GREEN{RESET} HUSI≥65/KUSI≥52  {YELLOW}YELLOW{RESET} HUSI 52-65/KUSI 40-52  {RED}RED{RESET} below threshold")
print(f"{RED}{BOLD}!! = FRAGILITY EXTREME (early-exit risk — IP capped 3.0)  {RESET}"
      f"{YELLOW}!  = FRAGILITY HIGH (IP cap 3.5){RESET}  ~ = FRAGILITY ELEVATED")
print(f"\nTotal: {len(pitchers)} pitchers")
