# Seattle Mariners Front Office Analytics

Portfolio project by Owen Duffy. Analyzes a decade of Mariners roster decisions through a front-office lens — using Bayesian methods to separate true player value from noise, and producing specific offseason recommendations backed by uncertainty-aware valuation.

**Live dashboard:** [Tableau Public](https://public.tableau.com/app/profile/owen.duffy7637/viz/MarinersFrontOfficeAnalytics/MarinersFrontOfficeAnalytics)  
**Case study:** [owen-duffy.com](https://owenduffy.com/mariners)

---

## Three analytical pillars

| # | Question | Method | Notebook |
|---|----------|--------|----------|
| 1 | Which Mariners contracts are high-confidence over/underperforming? | Hierarchical Bayesian WAR model (PyMC) | `02_bayesian_payroll.ipynb` |
| 2 | Is the development pipeline producing value vs draft slot? | Pure SQL cohort analysis (DuckDB) | `03_draft_cohorts.ipynb` |
| 3 | Who should the Mariners target in the 2026 offseason? | Gradient-boosted WAR projection (XGBoost) | `04_fa_projections.ipynb` |

## Stack

- **Ingestion:** Python + `pybaseball`, scheduled via GitHub Actions
- **Storage:** DuckDB (local) + MotherDuck (cloud mirror for dashboard)
- **Analysis:** Python, pandas, SQL, PyMC (Bayesian), scikit-learn / XGBoost (projection)
- **Viz:** Tableau Public (primary); matplotlib / arviz inside notebooks
- **Hosting:** GitHub + Tableau Public + owen-duffy.com

## Architecture

```
pybaseball / Baseball Savant / Baseball Reference / Spotrac
      │
      ▼
src/ingest/pull_data.py   src/ingest/scrape_contracts.py
      │
      ▼
data/raw/  (datestamped CSVs)
      │
      ▼
src/transform/clean.py  →  data/mariners.duckdb  ←─ sql/schema.sql
                                   │
              ┌────────────────────┼───────────────────┐
              ▼                    ▼                   ▼
   notebooks/02_*.ipynb   notebooks/03_*.ipynb  notebooks/04_*.ipynb
              │                    │                   │
              └────────────────────┴───────────────────┘
                                   │
                        dashboard/tableau_data_extracts/
                                   │
                               Tableau Public → owen-duffy.com
```

## Running locally

```bash
# 1. Install deps
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 2. Pull batting, pitching, Statcast, and draft data; initialize DB
python -m src.ingest.pull_data

# 3. Populate contract data (requires players table from step 2)
python -m src.ingest.scrape_contracts

# 4. Open exploration notebook
jupyter notebook notebooks/01_exploration.ipynb
```

## Repo layout

```
mlb/
├── data/           DuckDB warehouse + raw/staging CSVs
├── sql/            Schema definitions and analytical views
├── src/            Ingestion, transform, and model modules
├── notebooks/      Four analysis notebooks (one per pillar + exploration)
├── dashboard/      Tableau data extracts (CSV)
└── writeup/        Memo-to-front-office case study
```

## Data sources

- [FanGraphs](https://www.fangraphs.com/) via `pybaseball` — WAR, wOBA, FIP, wRC+
- [Baseball Savant](https://baseballsavant.mlb.com/) via `pybaseball` — Statcast quality-of-contact
- [Spotrac](https://www.spotrac.com/mlb/seattle-mariners/) — contract data (manual CSV)
- [Baseball Reference](https://www.baseball-reference.com/) — draft data

Historical window: **2015–present** (10 seasons).
