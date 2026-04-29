# Axiom — Merlin v2.2 Morning Handoff
**Last updated:** April 25, 2026 — all fixes committed & pushed to GitHub.
**Status:** Ready to deploy and run.

---

## Step 1 — Deploy (do this first, one time)

The two crash fixes from last night are in GitHub but not yet live on the server.
Open your terminal inside the `axiom` folder and run:

```
./deploy.sh
```

Takes about 2-3 minutes. When it finishes you will see a Cloud Run URL printed.
That URL is your base address for every command below — it looks like:
`https://axiom-api-XXXXXXXX-uc.a.run.app`

---

## Step 2 — Confirm the server is alive

Replace `YOUR_URL` with the URL from Step 1:

```
curl YOUR_URL/health
```

You should get back: `{"status":"ok"}`

---

## Step 3 — Run the daily pipeline (scores all pitchers)

This fires the engine — it fetches every probable starter for today, runs
HUSI + KUSI + Merlin simulation (N=2000), and saves everything to the database.
Takes 3–8 minutes. The server responds immediately and works in the background.

```
curl -X POST YOUR_URL/v1/tasks/run-daily \
  -H "Content-Type: application/json" \
  -H "AXIOM-INTERNAL-TOKEN: YOUR_TOKEN" \
  -d '{"dry_run": false}'
```

Replace `YOUR_TOKEN` with the value of `AXIOM_INTERNAL_TOKEN` from your `.env` file.

Wait 5 minutes, then move to Step 4.

---

## Step 4 — Pull the Merlin Board (the report you approved)

This is the main report. It shows every starter sorted best to worst,
with floor / median / ceiling for both Hits and Ks props.

```
curl YOUR_URL/v1/reports/merlin-board | python3 -m json.tool
```

### What you are reading — column by column:

| Field | What it means |
|---|---|
| `pitcher` | Starter's full name |
| `team` | His team abbreviation |
| `opponent` | Who he faces today |
| `husi` | Hits Under Score (0-100). Higher = stronger Under signal |
| `husi_grade` | Letter grade: A+ / A / B / C / D |
| `hits_line` | Sportsbook hits allowed prop line |
| `h_floor` | 5th percentile — his best-case suppression (hits floor) |
| `h_median` | 50th percentile — most likely hits total |
| `h_ceil` | 95th percentile — worst case / blowup ceiling |
| `h_under_pct` | % of 2,000 simulations that landed UNDER the line |
| `hits_color` | GREEN = strong Under / YELLOW = neutral / RED = risky |
| `kusi` | Strikeouts Under Score (0-100). Higher = stronger Under |
| `kusi_grade` | Letter grade: A+ / A / B / C / D |
| `k_line` | Sportsbook K prop line (e.g. 7.5) |
| `k_floor` | K floor — worst case (fewest Ks) |
| `k_median` | Most likely K total |
| `k_ceil` | K ceiling — best case (most Ks, e.g. 10.2) |
| `k_under_pct` | % of 2,000 sims that landed UNDER the K line |
| `ks_color` | GREEN = K ceiling ≥ 9.0 / YELLOW = 7-9 / RED = below 7 |
| `kill_streak_prob` | % chance of a 10+ K game |

### Reading a row — example:
```
"pitcher": "Gavin Williams",
"husi": 61.4,  "husi_grade": "A",
"hits_line": 5.5,
"h_floor": 3.0,  "h_median": 4.8,  "h_ceil": 7.1,
"h_under_pct": 0.71,               ← 71% of sims went UNDER
"hits_color": "GREEN",
"kusi": 54.2,  "kusi_grade": "B",
"k_line": 7.5,
"k_floor": 5.0,  "k_median": 7.3,  "k_ceil": 9.8,
"k_under_pct": 0.53,               ← 53% of sims went UNDER
"ks_color": "GREEN"
```
This tells you: **Gavin Williams** is a strong Hits Under play (71% sims Under,
floor of 3 hits) and a lean-Under on Ks (53% sims Under, but ceiling of 9.8
means the Over has real upside — keep an eye on it).

---

## Step 5 — Full pitcher table (all stats, ranked by HUSI)

```
curl YOUR_URL/v1/pitchers/today | python3 -m json.tool
```

Includes everything from the Merlin Board PLUS:
- ML Engine 2 divergence signals (`husi_signal`, `kusi_signal`)
- Catcher framing label (ELITE / AVG / POOR)
- Travel & Fatigue flag (`tfi_label`)
- VAA / extension data
- Risk score and risk tier
- PFF (Pitcher Form Factor) label: ON FIRE / HOT / NEUTRAL / COLD / STRUGGLING

---

## Step 6 — Risk report (who to avoid)

```
curl YOUR_URL/v1/risk/today | python3 -m json.tool
```

Lists every starter by risk score. Flags to watch:
- `COMBO_RISK` = 3+ danger flags stacking — strong Over lean
- `COLD_START` = pitcher's last 3 starts trended down — form is poor
- `EXTREME_PARK` = Coors / Chase / GABP / Citizens Bank — hits park penalty
- `TFI_ACTIVE` = team flew across time zones or played last night — fatigue penalty

Filter to just HIGH risk starters:
```
curl "YOUR_URL/v1/risk/today?tier=HIGH" | python3 -m json.tool
```

---

## Step 7 — Deep dive on one pitcher

Get the pitcher's ID from the board (the `pitcher_id` field), then:

```
curl "YOUR_URL/v1/pitchers/PITCHER_ID/profile" | python3 -m json.tool
```

Shows all 12 block scores (OWC, PCS, ENS, OPS, UHS, DSC, OCR, PMR, PER, KOP, UKS, TLR),
interaction boosts that fired, volatility penalties, and the full prop breakdown.

---

## Step 8 — Download the spreadsheet

```
curl "YOUR_URL/v1/exports/daily.csv" -o axiom_today.csv
```

Opens in Excel / Numbers. Every pitcher, both markets, all scores and projections.

---

## Grading Scale — Quick Reference

| HUSI / KUSI Score | Grade | What it means |
|---|---|---|
| 70-100 | A+ | Elite Under signal — high conviction |
| 60-69 | A | Strong Under signal |
| 57-59 | B+ | Above average, leaning Under |
| 50-56 | B/C | Neutral — no strong edge |
| 40-49 | D | Slight Over lean — be cautious |
| 0-39 | F | Strong Over lean — fade the Under |

---

## What Merlin v2.2 Has Running

Every feature listed below is wired to real live data — nothing is defaulting:

**HUSI blocks (Hits Under):**
- OWC — Opponent contact weaknesses (BABIP, hard-hit, barrel, LD, xBA, lineup structure)
- PCS — Pitcher contact suppression (GB%, soft contact, barrel against, command)
- ENS — Park factor, wind, temperature, altitude, dome/roof, outfield dimensions
- OPS — Pitch count capacity, manager hook, WHIP-based traffic, TTO awareness, bullpen
- UHS — Umpire called-strike rate, zone accuracy, early-count tendencies
- DSC — OAA defense (total, infield, outfield), catcher framing, team sprint speed

**KUSI blocks (Strikeouts Under):**
- OCR — Opposing lineup K rate, contact rate, plate discipline
- PMR — Pitch arsenal whiff rates (primary/secondary/putaway), stolen base rate, lineup K rate, platoon advantage
- PER — K/BB efficiency, walk rate, avg IP per start, swinging strike rate
- KOP — Pitch count for Ks, hook tendency, TTO K awareness, bullpen support
- UKS — Umpire zone tightness (fixed: no double-counting)
- TLR — Top-4/6 lineup K resistance, veteran at-bat count

**Special engines active:**
- Merlin Simulation — N=2,000 Monte Carlo runs per pitcher
- SKU #37 — Catcher Framing (steal 3-5 Ks with elite catcher + wide zone umpire)
- SKU #38 — VAA & Extension (perceived velocity boost; flat-pitch contact penalty)
- SKU #39 — Swing Plane Collision (batter attack angle vs pitcher VAA)
- SKU #14 — Travel & Fatigue Index (cross-timezone or getaway day penalty)
- PFF — Pitcher Form Factor (last 3 starts trending Hot or Cold)
- ML Engine 2 — Residual drift detector (flags when formula and ML diverge)

---

## What the Next Chat Should Start With

Just paste this into the new chat to get moving:

> "Good morning. We are working on the Axiom pitcher scoring platform (Merlin v2.2).
> All code is deployed. I need to run today's morning report.
> The project is at `/Users/rac187/Documents/axiom`.
> My Cloud Run URL is `YOUR_URL`.
> Walk me through running the pipeline and reading the board."

---

## GitHub Status (as of April 25, 2026)

Repository: `github.com/cigar187/axiom` — branch `main`

Last 5 commits:
1. `7a858c9` — Fix two NameError crashes (defending_team_id + opp_batters scope)
2. `243fe38` — Wire dsc_align from team sprint speed — DSC block now 5/5 live
3. `e0a64d3` — Wire PMR block (6/6) and DSC defense (3/4) from real data
4. `2f0593e` — Fix team_id_numeric + UKS umpire multicollinearity
5. `8046e4e` — Merlin v2.2 — Swing Plane Collision, multicollinearity fixes

All features wired. Zero defaults. Clean codebase. Ready to run.
