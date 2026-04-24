# Axiom — Pitcher Intelligence Platform

**By GTM Velo** | First customer: TiltBox iOS app

Axiom is a standalone data server that scores MLB starting pitchers daily using two proprietary indices — **HUSI** (Hits Under Score Index) and **KUSI** (Strikeouts Under Score Index). Results are exposed via a REST API and stored in PostgreSQL for historical backtesting.

---

## Architecture at a Glance

```
MLB Stats API ──┐
Rundown API  ──┤──► Feature Builder ──► HUSI Engine ──► PostgreSQL ──► REST API
Umpire Scraper ─┘                   └──► KUSI Engine ──►
```

The scoring engine is fully separated from data fetcher code. Every block score (OWC, PCS, ENS, OPS, UHS, DSC for HUSI; OCR, PMR, PER, KOP, UKS, TLR for KUSI) is logged individually for full transparency and is stored in the database for audit and tuning.

---

## Project Structure

```
/axiom
  /app
    /api          → FastAPI route handlers (all endpoints)
    /core         → HUSI + KUSI scoring engine (isolated from data sources)
      features.py   → PitcherFeatureSet data contract
      husi.py       → HUSI formula, blocks, interactions, penalties
      kusi.py       → KUSI formula, blocks, interactions, penalties
    /services     → Data fetchers (provider adapters)
      base.py       → Abstract BaseProvider interface
      mlb_stats.py  → MLB Stats API (real implementation)
      rundown.py    → The Rundown API (real implementation)
      umpire.py     → Umpire scraper (STUB — returns 50 neutral)
    /models       → SQLAlchemy database models
    /schemas      → Pydantic request/response shapes
    /tasks        → Daily pipeline runner
      feature_builder.py → Maps raw API data → PitcherFeatureSet
      pipeline.py        → Orchestrates all fetchers + scoring + DB write
    /utils        → Normalization, logging, CSV export
    config.py     → Settings loaded from environment variables
  main.py           → FastAPI app entry point
  requirements.txt
  Dockerfile
  deploy.sh         → Google Cloud Run deployment
  schema_migrations.sql
  .env.example
```

---

## Scoring Indices

### HUSI — Hits Under Score Index (0-100)
Measures the probability that a pitcher will allow fewer hits than the sportsbook line.

**Formula:**
```
HUSI_base = 0.27*OWC + 0.26*PCS + 0.16*ENS + 0.18*OPS + 0.08*UHS + 0.05*DSC
HUSI = clamp(HUSI_base + interaction_boosts - volatility_penalties, 0, 100)
```

**Grades:** A+ (82+), A (76-81.9), B (69-75.9), C (60-68.9), D (<60)

**Projected hits:** `base_hits × (1 - 0.21 × ((HUSI - 50) / 50))`

### KUSI — Strikeouts Under Score Index (0-100)
Measures the probability that a pitcher will record fewer strikeouts than the sportsbook line.

**Formula:**
```
KUSI_base = 0.28*OCR + 0.22*PMR + 0.18*PER + 0.14*KOP + 0.10*UKS + 0.08*TLR
KUSI = clamp(KUSI_base + interaction_boosts - volatility_penalties, 0, 100)
```

**Grades:** A+ (83+), A (76-82.9), B (69-75.9), C (61-68.9), D (<61)

**Projected Ks:** `base_ks × (1 - 0.25 × ((KUSI - 50) / 50))`

---

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Server health check |
| GET | `/v1/pitchers/today` | Full ranked table — all pitchers with HUSI, KUSI, grades, props |
| GET | `/v1/rankings/today` | Sorted by strongest under signal (stat_edge) |
| GET | `/v1/pitchers/{id}/profile` | Deep dive — one pitcher, all feature blocks |
| POST | `/v1/tasks/run-daily` | Manually trigger the pipeline (token protected) |
| GET | `/v1/exports/daily.csv` | Download full results as CSV |

Interactive docs available at `/docs` after the server starts.

---

## Setup — Local Development

### 1. Clone and install dependencies

```bash
git clone <your-repo>
cd axiom
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env with your actual values
```

Key variables to fill in:
- `DATABASE_URL` — your PostgreSQL connection string
- `RUNDOWN_API_KEY` — your Rundown API key (already set in .env.example)
- `AXIOM_INTERNAL_TOKEN` — generate a strong random string for pipeline protection

### 3. Create the database

Connect to your PostgreSQL instance and run:

```bash
psql -h localhost -U your_user -c "CREATE DATABASE axiom_db;"
psql -h localhost -U your_user -d axiom_db -f schema_migrations.sql
```

### 4. Start the server

```bash
python main.py
```

The server starts at `http://localhost:8080`. Visit `http://localhost:8080/docs` for the API explorer.

### 5. Run the daily pipeline manually

```bash
curl -X POST http://localhost:8080/v1/tasks/run-daily \
  -H "Content-Type: application/json" \
  -H "AXIOM-INTERNAL-TOKEN: your_token_here" \
  -d '{"dry_run": false}'
```

---

## Setup — Google Cloud Run

### Prerequisites
- Docker installed
- `gcloud` CLI installed and authenticated
- Cloud SQL instance created in project `tiltbox-82384`
- `axiom_db` database created on that instance

### Deploy

```bash
# Set these in your shell or .env file
export CLOUD_SQL_INSTANCE="tiltbox-82384:us-central1:your-instance-name"
export DATABASE_URL="postgresql+asyncpg://user:pass@/axiom_db?host=/cloudsql/tiltbox-82384:us-central1:your-instance-name"
export AXIOM_INTERNAL_TOKEN="your_strong_token_here"

bash deploy.sh
```

The script will:
1. Build the Docker image
2. Push it to Google Container Registry
3. Deploy to Cloud Run with all environment variables and the Cloud SQL connection

---

## Data Sources

| Source | Status | What it provides |
|--------|--------|-----------------|
| MLB Stats API | Live | Games, pitchers, season stats, bullpen logs, umpire assignments |
| The Rundown API | Live | Strikeout and hits-allowed prop lines from 7 sportsbooks |
| umpirescorecard.com | **STUB** | Umpire tendencies (returns 50 neutral until scraper is built) |

### Adding a new data source

1. Create a new class in `app/services/` that extends `BaseProvider`
2. Implement the `fetch(target_date)` method
3. Map the returned data to `PitcherFeatureSet` fields in `app/tasks/feature_builder.py`
4. No other code changes needed — the scoring engine is completely isolated

### Tuning weights

All HUSI and KUSI block weights are defined as constants at the top of each formula in `app/core/husi.py` and `app/core/kusi.py`. To tune weights:
1. Edit the multipliers in the relevant block scorer function
2. Re-run the pipeline against historical dates
3. Compare against `backtest_results` table

---

## Data Quality Flags

Every pitcher output includes a `data_quality_flag` field:

| Flag | Meaning |
|------|---------|
| `complete` | All data sources available |
| `partial:lineup` | Lineup not yet confirmed (−2.5 penalty applied) |
| `partial:umpire` | Umpire using neutral stub (−0.8/−1.0 penalty applied) |
| `partial:bullpen` | No bullpen log data (ops_bpen defaults to neutral) |
| `partial:lineup+umpire+bullpen` | All three missing (early morning run) |

---

## Live Test Results (April 21, 2026 — partial data)

These results were validated with only MLB Stats API + Rundown props (no umpire or lineup data yet):

| Pitcher | HUSI | KUSI | K Line | K Proj | Result |
|---------|------|------|--------|--------|--------|
| Parker Messick | 51.9 | 39.7 | 5.5 | 5.9 | — |
| Yoshinobu Yamamoto | 51.5 | 33.7 | 5.5 | 5.6 | — |
| Chase Burns | 43.0 | 47.1 | 5.5 | 4.9 | +0.6 under edge |
| Shota Imanaga | 52.2 | 20.1 | 6.5 | 9.0 | Correctly flagged OVER lean |
| Reynaldo Lopez | — | — | — | — | Went UNDER (directionally correct) |
