# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common commands

```bash
# Install dependencies (Anaconda env recommended)
pip install -r requirements.txt

# Full ETL: initialize DuckDB, pull batting + pitching + Statcast + draft data
python -m src.ingest.pull_data

# Populate contracts table (run after pull_data — requires players table to exist)
python -m src.ingest.scrape_contracts

# Open notebooks
jupyter lab

# Query the warehouse interactively
python -c "import duckdb; con = duckdb.connect('data/mariners.duckdb'); print(con.execute('SHOW TABLES').fetchdf())"

# Inspect Mariners player-seasons
python -c "
import duckdb
con = duckdb.connect('data/mariners.duckdb')
print(con.execute('SELECT * FROM player_seasons WHERE is_mariners LIMIT 10').fetchdf())
"

# Kill a stale DuckDB write lock
lsof data/mariners.duckdb   # find the PID
kill <PID>
```

There are no automated tests. Notebook correctness is verified by running cells top-to-bottom.

## Developer notes

**DuckDB write lock** — DuckDB allows only one write connection at a time. Notebooks must open with `get_db(read_only=True)`. Only the `__main__` blocks in `pull_data.py` and `scrape_contracts.py` open write connections. If a notebook raises `IOException: Could not set lock`, find and kill the holding PID with `lsof data/mariners.duckdb`.

**Preferred connection pattern** — use the `db_connection()` context manager (not bare `get_db()`) for write connections in scripts; it guarantees `con.close()` even on exception:
```python
from src.ingest.pull_data import db_connection
with db_connection() as con:
    ...
```

**Raw data cache** — all four `pull_*` functions write datestamped CSVs to `data/raw/` (e.g. `batting_2015_2026_2026-04-19.csv`). Re-running on the same calendar day hits the cache. To force a fresh API fetch, delete the relevant file from `data/raw/`.

**`pull_*` function pattern** — all four ingestion functions (`pull_batting`, `pull_pitching`, `pull_statcast`, `pull_draft`) follow the same shape: cache to CSV → if `con` is passed, normalize and upsert into DuckDB. Passing `con=None` returns the raw DataFrame without touching the DB.

**Statcast load order dependency** — `load_statcast()` filters rows to `mlbam_id`s already present in `players`. Run `pull_batting` (and optionally `pull_pitching`) before `pull_statcast` to ensure players are seeded.

**Contracts are a full reload** — `load_contracts()` executes `DELETE FROM contracts WHERE team = 'Mariners'` before inserting, so each `scrape_contracts` run completely replaces Mariners contract data. This is intentional: Spotrac is the authoritative source and contracts change frequently.

**PyTensor / PyMC compiler (macOS + Anaconda)** — Anaconda ships clang++ v14, which can't find C++ headers on macOS 15. The permanent fix is in `~/.zshrc` (`PYTENSOR_FLAGS`, `SDKROOT`, `CONDA_BUILD_SYSROOT`). Notebook `02_bayesian_payroll.ipynb` has a compiler-setup cell (currently cell 6) that must run before any PyMC cell; it wipes the PyTensor cache and re-points the compiler to `/usr/bin/clang++` with the correct `-I{sdk}/usr/include/c++/v1` flag.

**`src/models/` are stubs** — `bayesian_war.py` and `fa_projection.py` define interfaces and feature lists but the working model implementations live in `02_bayesian_payroll.ipynb` and `04_fa_projections.ipynb`. `fa_projection.FEATURES` is the canonical feature list for Pillar 3 — the notebook imports it.

**FanGraphs ingestion uses `curl_cffi`** — raw `requests` calls to `fangraphs.com` return 403 (Cloudflare). `pull_data.py` uses `curl_cffi.requests.Session(impersonate="chrome")` throughout; don't swap it for standard `requests`.

**FanGraphs column name quirks** — the API returns `playerid` (not `IDfg`) and `xMLBAMID` (not `mlbam_id`). `pull_data.py` renames `playerid → IDfg` before passing to `normalize_batting()`, which then maps `xMLBAMID → mlbam_id`. Multi-team rows ("2 Tms", "3 Tms") for traded players are dropped at fetch time.

**Contracts name matching** — `scrape_contracts.py` joins Spotrac names to `players` by normalized name (lowercase, punctuation stripped). Players with Unicode accents (e.g. José Ferrer) may fail to match and are printed as warnings; add them manually to `players` then re-run.

## Architecture & data flow

```
pybaseball (FanGraphs / Baseball Savant) + Spotrac
    ↓  src/ingest/pull_data.py  |  src/ingest/scrape_contracts.py
data/raw/  (datestamped CSVs — cache layer)
    ↓  src/transform/clean.py   (column normalization)
    ↓  src/transform/load.py    (idempotent upserts)
data/mariners.duckdb  (source of truth)  ←  sql/schema.sql
    ↓
notebooks/  (one per pillar — read-only DB access)
    ↓
dashboard/tableau_data_extracts/  (CSV exports → Tableau Public)
```

**Three analytical pillars:**
1. `02_bayesian_payroll.ipynb` — hierarchical PyMC model, player true-talent WAR with partial pooling toward position-level means; exports `payroll_efficiency.csv`
2. `03_draft_cohorts.ipynb` — pure SQL in DuckDB, draft classes 2013–2025 vs league; exports `draft_reach_rate.csv`, `draft_surplus.csv`, `draft_debut_lag.csv`, `pipeline_gaps.csv`, `draft_dev_curve.csv`
3. `04_fa_projections.ipynb` — XGBoost point + quantile (p10/p90) WAR projections; exports `fa_targets.csv`

**Key joins:**
- Pillar 1 core: `player_seasons JOIN contracts ON mlbam_id`, filtered by `player_seasons.is_mariners`
- Pillar 2 core: `draft_picks LEFT JOIN player_seasons ON mlbam_id` to get career WAR for draftees
- Pillar 3 feature pivot: self-join on `player_seasons` to build `war_y0/y1/y2` trailing windows, then join `statcast_quality` for contact metrics

**Model outputs** must go to `dashboard/tableau_data_extracts/` as CSVs — that's the only location Tableau reads from.

## Data model

```
players          — one row per MLB player (mlbam_id PK, name, position, bats/throws)
player_seasons   — one row per (mlbam_id, season, team); is_mariners flag; UNIQUE constraint
contracts        — Mariners contracts from Spotrac (mlbam_id, season_start); UNIQUE (mlbam_id, season_start)
team_seasons     — team-level aggregates per season
draft_picks      — draft classes 2013–2025; mlbam_id nullable (players who never reached majors)
statcast_quality — exit velo, barrel%, xwOBA per (mlbam_id, season); UNIQUE constraint
```

Historical window: 2015-present for player stats; 2013-2025 for draft cohorts; Statcast coverage from 2015.

## Project context

Portfolio project by Owen Duffy answering three front-office questions through a Bayesian + ML lens. Scope-creep guard: new analyses go in `BACKLOG.md`, not the notebooks. The `writeup/case_study.md` is the primary deliverable alongside the Tableau dashboard at `dashboard/tableau_data_extracts/`.

## Key references

- FanGraphs Library: https://library.fangraphs.com/ — sabermetrics definitions
- pybaseball docs: https://github.com/jldbc/pybaseball
- PyMC docs: https://www.pymc.io/
- Baseball Savant: https://baseballsavant.mlb.com/ — Statcast data
- Spotrac: https://www.spotrac.com/mlb/seattle-mariners/ — contract data
