# AXIOM BUILD LOG — Living Document
**Project:** Axiom Pitcher Intelligence API  
**Client App:** Tiltbox (live on Apple App Store — real users, real money)  
**Last updated:** April 26, 2026 — 4:29 PM Central  
**Purpose:** Full accountability record. Every decision, every mistake, every fix. Any agent or session that opens this project reads this first before touching anything.

---

## THE RULE BEFORE ANYTHING ELSE

This is a live production API. Tiltbox is a live iOS app with real users.  
Nothing is a test. Nothing is a hobby project.  
Every broken deploy costs real data and real money.  
**Read this entire document before writing a single line of code.**

---

## CURRENT STATE — April 26, 2026

### What is working right now
- API is live at: `https://axiom-api-965804388585.us-central1.run.app`
- `/v1/pitchers/today` returns 29 pitchers for April 26
- Tiltbox is connected and reading from the API
- `python3 show_report.py` runs and displays the daily board locally
- HUSI / KUSI / Merlin simulation engine logic is sound

### What is broken right now
- **Pipeline ran 7 hours late today** — data was not available at 9 AM Eastern as required
- **Cloud Scheduler has been removed** — deliberately, because it was the wrong architecture
- **No scheduler worker exists yet** — the rebuild has been planned but not started
- **Report column alignment** — LABEL column width was narrowed from 10 to 9 chars, causing [VOLATILE] to overflow and break alignment. Fix is partially done (2 of 3 edits completed — third edit was interrupted).
- **Lineup vault data never committed** — `axiom_game_lineup` writes execute after `db.commit()` but no second commit is ever called. All batter data silently discards on every run.
- **No pipeline run log** — there is no record anywhere of when the pipeline ran, whether it succeeded, or how many pitchers were processed. The system is flying blind.

---

## ROOT CAUSE ANALYSIS — Why April 26 Failed

### The real problem (not a symptom)
The pipeline was wired to a Cloud Scheduler job that called an HTTP endpoint on a Cloud Run container. Cloud Run is a serverless request handler — it is not designed to run a reliable 9 AM background job that makes 30+ API calls, runs 2,000 Monte Carlo simulations per pitcher, and writes to a database. This is architecturally wrong. It will fail again on any system built this way.

### Cascade of failures that made it worse
1. Cloud Scheduler was configured with a placeholder token (`YOUR_TOKEN_HERE`) — never worked correctly from day one
2. Cloud Scheduler pointed at wrong service name (`axiom-engine` instead of `axiom-api`) — also never worked
3. A "self-healing" auto-trigger was added to `routes.py` to compensate — this caused concurrent pipeline runs which corrupted database transactions (`InFailedSQLTransactionError`)
4. `ALTER TABLE` schema migrations were running inside the pipeline on every execution — caused lock conflicts when concurrent runs occurred
5. `update_axiom_pitcher_stats()` lacked proper rollback on failure — corrupted the SQLAlchemy session mid-pipeline
6. All of the above were band-aids applied to a broken foundation instead of fixing the foundation

### What was fixed today (confirmed working)
- Removed self-healing auto-trigger from `routes.py`
- Removed `ALTER TABLE` block from `pipeline.py`
- Added `db.rollback()` to `update_axiom_pitcher_stats` exception handler in `pipeline.py`
- Pipeline now runs cleanly when triggered manually

### What was NOT fixed (still needs proper rebuild)
- The scheduler — deleted, not yet replaced with proper architecture
- The infrastructure that makes the 9 AM run reliable and self-healing without database corruption

---

## INFRASTRUCTURE AUDIT — What Good APIs Do vs What We Built

### What The Rundown, MLB Stats, NFL.com, NBA.com all have in common
1. **Pipeline and API are two completely separate services** — data collection never runs inside the API server
2. **Data is pre-built and waiting** — before any user hits the endpoint, data already exists in the database
3. **Always-on background worker** — not a cron job poking an HTTP endpoint. A dedicated process with its own schedule
4. **API is read-only** — only queries the database. Never triggers computation. Never calls external APIs
5. **Usage monitoring** — rate limits tracked, data-point consumption logged, alerts before hitting caps
6. **Health checks tied to data freshness** — system knows if today's data is ready, alerts if not
7. **Graceful degradation** — if pipeline fails, serves last good data with staleness indicator

### What we built instead
- A cron job poking a serverless container that may or may not be warm
- Pipeline logic embedded inside the API server
- No monitoring of The Rundown API rate limits or data-point usage
- No health check that answers "did today's pipeline run?"
- No alert when 9 AM passes with no data

### API integration issues found
- **We go through RapidAPI as a middleman** — not directly to The Rundown. Extra latency, extra failure point, extra cost
- **We are on The Rundown V1 (legacy)** — they recommend V2 for all new integrations. V1 is missing newer features
- **Authentication header** — we use `x-rapidapi-key` / `x-rapidapi-host`. Direct auth uses `X-TheRundown-Key`
- **No response header logging** — `X-Datapoints-Remaining`, `X-Datapoints-Used` are never read or logged. We do not know if we are hitting our quota ceiling

---

## THE REBUILD PLAN — Agreed April 26, 2026

These steps must be completed in order. Each step must be reviewed and confirmed working before the next begins.

### Step 1 — Database Foundation
**Status: IN PROGRESS — approved, not yet built**

What needs to be added:
- `pipeline_run_log` table — records every pipeline execution with: timestamp, status (success/failed), pitchers_scored, elapsed_seconds, error_message
- `updated_at` column on `model_outputs_daily` — so we can see when data was last refreshed
- Fix the lineup vault commit bug — add `db.commit()` after the `axiom_game_lineup` writes in `pipeline.py`

What stays unchanged:
- All existing table structures — they are well designed
- All existing UniqueConstraints
- All existing indexes
- Connection pool settings in `base.py` (pool_size=10, max_overflow=20)

### Step 2 — Direct API Connections (no middlemen)
**Status: PENDING**

- Move The Rundown from RapidAPI to direct connection using `X-TheRundown-Key` header
- Upgrade from V1 to V2 endpoints
- Log `X-Datapoints-Remaining` and `X-Datapoints-Used` on every response
- Alert when remaining drops below 20%

### Step 3 — Data Layer (caching and storage)
**Status: PENDING**

- Upstream data fetched once per day and stored before any scoring runs
- If The Rundown is unavailable, serve from our cached prop data
- If MLB Stats API is slow, serve from our cached pitcher data
- No pipeline run should fail because an upstream API had a bad moment

### Step 4 — Always-On Background Worker
**Status: PENDING**

- Dedicated service, completely separate from the API server
- Runs on its own internal schedule (9 AM Eastern)
- If it fails, it retries with backoff and logs the failure
- Writes to `pipeline_run_log` on every attempt (success or failure)
- Never shares a process or container with the API server

### Step 5 — API Becomes Read-Only
**Status: PENDING**

- Remove all pipeline trigger endpoints from the API
- API only reads from the database
- No external API calls ever made from inside a route handler
- Response time becomes fast and predictable because it is just a database read

### Step 6 — Health and Monitoring
**Status: PENDING**

- `/health` endpoint returns: API status, last pipeline run time, data freshness, pitcher count for today
- If data is more than 2 hours stale, health check returns degraded status
- Alert fires when pipeline has not run successfully by 9:30 AM Eastern

---

## MISTAKE LOG — Never Repeat These

| Date | Mistake | What it cost |
|------|---------|--------------|
| Apr 25-26 | Built scheduler as Cloud Scheduler cron job instead of dedicated background worker | 7 hours of lost data, 15 games unscored |
| Apr 25-26 | Added "self-healing" auto-trigger to routes.py | Caused concurrent pipeline runs, corrupted database transactions |
| Apr 25-26 | Left ALTER TABLE migrations running inside the daily pipeline | Lock conflicts when concurrent runs occurred |
| Apr 25-26 | Deployed with --allow-unauthenticated while org policy blocked it | Wiped existing IAM bindings, caused HTTP 403 for all users |
| Apr 25-26 | Fixed symptoms instead of root cause (multiple times) | Each band-aid introduced a new failure |
| Apr 26 | Changed LABEL column width from 10 to 9 in show_report.py | [VOLATILE] overflows, all columns misalign |
| Apr 26 | Piped show_report.py to `less -S` | Stripped ANSI colors, showed raw escape codes instead |
| Apr 26 | Jumped to building/reading files without waiting for user direction | Lost user's ability to read and review before action |

---

## DECISIONS RECORD — What Was Chosen and Why

| Decision | What was chosen | Why |
|----------|----------------|-----|
| IP ceiling | 4.8 innings | MLB averaging 5.1-5.3 IP; 4.8 eliminates sportsbook's ghost .5 noise |
| Scheduler architecture | Dedicated background worker (not yet built) | Cloud Run + cron = unreliable for heavy background jobs |
| Bayesian shrinkage | 8 starts prior for H/9, 6 starts for K/9 | Stabilizes small-sample rookie/sophomore stats |
| Monte Carlo iterations | N=2,000 | Balance between accuracy and compute time |
| HUSI/KUSI correlation | ρ = -0.60 | Prevents mathematically impossible high-hits + high-K scenarios |
| Yank trigger | 6.0 hits (was 7.0) | More realistic early hook threshold |
| Fragility IP cap | 3.0 (EXTREME), 3.5 (HIGH) | Reflects early-exit risk for shell-prone pitchers |

---

## HOW TO USE THIS DOCUMENT

**If a new agent opens this project:**
Read this document completely before writing any code. The current state section tells you exactly where things stand. The rebuild plan tells you what comes next and in what order.

**If something breaks:**
Come to this document first. Check the mistake log. Check the current state. Do not touch code until you understand what was agreed and why.

**If the agent goes off the rails:**
Stop all work. Re-read the rebuild plan. Confirm with the user which step is currently in progress. Do not skip steps. Do not combine steps.

**Update this document:**
- Before starting any step: update status to IN PROGRESS
- After completing a step and confirming it works: update status to COMPLETE, add date
- After any mistake: add it to the mistake log immediately

---

## CONTACTS AND LOCATIONS

| Item | Value |
|------|-------|
| Project folder | `/Users/rac187/Documents/axiom` |
| Live API URL | `https://axiom-api-965804388585.us-central1.run.app` |
| GCP Project | `axiom-gtmvelo` |
| GCP Region | `us-central1` |
| GCP Service | `axiom-api` |
| Deploy command | `./deploy.sh` (from axiom folder) |
| Local report | `python3 show_report.py` (from axiom folder) |
| GitHub repo | `github.com/cigar187/axiom` — branch `main` |
