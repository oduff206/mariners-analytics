# Seattle Mariners Front Office Analytics — Case Study

*A memo to Seattle Mariners Baseball Operations*

---

## Executive Summary

1. **Payroll efficiency:** The Bayesian WAR model identifies Josh Naylor and Julio Rodriguez as high-confidence monitoring flags — both carry $17–19M AAV with posterior true-talent distributions whose 94% HDI sits near or below the 1.43-WAR league average, while Dominic Canzone and Luke Raley are clear value bargains at under $1M/WAR posterior.

2. **Development pipeline:** Cal Raleigh (2018 draft, round 3, pick 90) is the organization's best developmental outcome of the 2013–2020 window by a wide margin — 23.1 career WAR against a slot expectation of 3.4 WAR, a surplus of +19.7; the Mariners posted above-league MLB reach rates in five of eight draft classes, with 2016 (−14 pp) and 2014 (−8 pp) as the notable underperformers.

3. **Offseason recommendation:** The XGBoost projection model — trained on trailing 3-year WAR, age, playing time, and Statcast contact quality — identifies a ranked FA target list filtered to positional gaps in the development pipeline; top targets are ranked by projected WAR surplus over estimated market cost at the implied $3.3M/WAR market rate derived from Mariners contract data (a conservative floor that includes arbitration deals; open-market FA pricing typically runs $9–11M/WAR).

---

## 1. Payroll Efficiency — Bayesian True-Talent WAR

### Methodology

Player true-talent WAR is modeled hierarchically in PyMC. Each player's latent WAR is drawn from a position-group distribution (C, 1B, 2B, 3B, SS, OF, UT), which is itself drawn from a league-wide hyperprior. Playing time is encoded as observation uncertainty: a player with 90 PA has a wide likelihood; one with 650 PA has a narrow one. This partial pooling means a catcher with 80 PA shrinks heavily toward the catcher position mean, while a shortstop with a full season retains most of his observed signal.

Sampler: NUTS at `target_accept=0.98` (4 chains × 1,000 draws, 0 divergences). Posterior summarized as mean ± 94% HDI per player.

### Key Findings

**High-confidence underperformers** — contracts where the full 94% HDI falls below the posterior league average of 1.43 WAR:

| Player | Position | AAV | Posterior WAR mean | 94% HDI | $/WAR |
|--------|----------|-----|--------------------|---------|-------|
| Josh Naylor | 1B | $18.5M | −0.41 | [−0.71, −0.11] | $185M |
| Cal Raleigh | C | $17.5M | −0.07 | [−0.31, +0.16] | $175M |
| Julio Rodriguez | OF | $17.4M | −0.01 | [−0.29, +0.29] | $174M |

*Note: Raleigh and Rodriguez figures reflect the early-2026 partial sample (< 100 PA at time of analysis). Their HDIs straddle zero, indicating genuine uncertainty rather than confident underperformance. These are watch-list flags, not conclusions — both players have strong multi-year track records.*

**Value bargains** — contracted players with the lowest posterior $/WAR:

| Player | Position | AAV | Posterior WAR mean | $/WAR |
|--------|----------|-----|-------------------|-------|
| Dominic Canzone | UT | $0.80M | 1.48 | $0.54M |
| Luke Raley | UT | $2.05M | 2.26 | $0.91M |

Canzone and Raley are both pre-arb or arbitration-eligible players producing near or above league-average WAR at minimum-scale salaries — exactly the type of contract efficiency a payroll-constrained team should maximize.

### Interpretation

The Bayesian approach adds two things a point-estimate model cannot: (1) **uncertainty quantification** — Rodriguez's HDI spanning negative to positive territory means the model genuinely cannot yet distinguish slump from decline, which is the honest answer given 91 PA; (2) **partial pooling** — a 1B with 150 PA doesn't anchor his estimate entirely on 150 noisy observations, but borrows strength from all first basemen in the dataset, producing more stable estimates for bench and platoon players.

---

## 2. Development Pipeline — Draft Cohort ROI (2013–2020)

### Methodology

Pure SQL analysis in DuckDB. Draft data from Baseball Reference via pybaseball for classes 2013–2020, rounds 1–20. MLB reach rate computed as the fraction of draftees who accumulated any MLB game appearances. Career WAR joined from FanGraphs player-season data via name-matched `mlbam_id`. Pick-slot baseline computed as the league-average career WAR at each overall pick number across all 30 teams.

### Surplus-Value Picks

**Top Mariners developmental outcomes vs pick-slot expectation:**

| Draft Year | Round | Pick | Player | Position | Career WAR | Slot Avg | Surplus |
|------------|-------|------|--------|----------|-----------|---------|---------|
| 2018 | 3 | 90 | Cal Raleigh | C | 23.1 | 3.4 | **+19.7** |
| 2013 | 3 | 85 | Tyler O'Neill | OF | 11.3 | 1.4 | **+9.9** |
| 2016 | 1 | 11 | Kyle Lewis | OF | 2.5 | 1.6 | +1.0 |

Cal Raleigh at pick 90 is exceptional by any standard — a third-round catcher outproducing the average first-round pick by nearly 20 WAR. This reflects both strong development and an undervalued draft evaluation: catchers are routinely discounted in amateur scouting, and the Mariners exploited that market inefficiency in 2018.

### MLB Reach Rate

**Fraction of drafted players (rounds 1–10) reaching the majors, by class:**

| Class | SEA | League | Delta |
|-------|-----|--------|-------|
| 2013 | 50.0% | 43.4% | +7 pp |
| 2014 | 30.0% | 37.8% | **−8 pp** |
| 2015 | 40.0% | 38.4% | +2 pp |
| 2016 | 30.0% | 43.7% | **−14 pp** |
| 2017 | 40.0% | 40.6% | −1 pp |
| 2018 | 50.0% | 39.2% | +11 pp |
| 2019 | 54.5% | 38.8% | +16 pp |
| 2020 | 50.0% | 45.6% | +4 pp |

Above league in 5 of 8 classes. The 2016 class is the sharpest underperformer at −14 percentage points. Mariners sample sizes in rounds 1–10 are small (10–11 picks per class), so single-class comparisons carry high variance; the multi-year trend is more meaningful.

### Time to MLB Debut

**Average years from draft to first MLB appearance (players who reached MLB):**

| Class | SEA | League | Delta |
|-------|-----|--------|-------|
| 2013 | 5.17 | 4.71 | +0.46 |
| 2014 | 4.50 | 4.46 | +0.04 |
| 2015 | 4.67 | 4.45 | +0.22 |
| 2016 | 3.00 | 4.20 | **−1.20** |
| 2017 | 4.50 | 4.40 | +0.10 |
| 2018 | 3.00 | 4.01 | **−1.01** |

The 2016 and 2018 classes reached the majors roughly a full year faster than the league average — suggesting the organization pushed ready talent aggressively in those years, consistent with the competitive windows around the 2022–2023 playoff push.

### Positional Pipeline Gaps

Positions where Mariners draftees (2013–2020) generated below-league-average career WAR feed directly into Pillar 3 targeting. See `dashboard/tableau_data_extracts/pipeline_gaps.csv` for the full breakdown and `is_gap` flags by position group.

---

## 3. Free Agent Targeting — 2026 Offseason

### Methodology

XGBoost gradient-boosted regressor trained on player-seasons 2017–2023 (requiring at least 2 years of trailing WAR history). Features: `war_y0/y1/y2` (trailing 3-year WAR), `age`, `pa_y0`, `woba_y0`, and Statcast contact quality metrics — `xwoba_y0`, `exit_velo_avg_y0`, `barrel_pct_y0`. Target: WAR in the following season. Missing Statcast values imputed with column medians.

Validation on 2024 seasons predicting 2025 actuals. Prediction intervals produced by separate quantile XGBoost models at p10 and p90 — giving an honest 80% prediction interval per player.

**FA universe:** players with qualified 2025 seasons (PA ≥ 150), filtered to positional gaps from Pillar 2.

**Market rate:** $3.3M/WAR — median implied rate from Mariners contract-seasons 2022–2025 where AAV > $2M and player WAR > 1 (n = 8 contract-seasons). This is a conservative internal benchmark: the Mariners dataset includes pre-arb and arbitration deals, which price far below the open FA market. Projected values in the table below should be interpreted as a floor; at the open-market rate of $9–11M/WAR, the same WAR projections represent 3–4× higher dollar value.

### Top FA Targets

*Full ranked list in `dashboard/tableau_data_extracts/fa_targets.csv`. Populate the table below with the top 3 rows sorted by `proj_value_m` after running `python -m src.ingest.pull_data` to load Statcast features.*

| Rank | Player | Position | Age | Proj WAR | 80% Interval | Proj Value ($M) | Notes |
|------|--------|----------|-----|---------|--------------|----------------|-------|
| 1 | Julio Rodriguez | OF | 24 | 7.53 | [1.9, 8.7] | $25.2 | Already under Mariners contract — top model output; serves as internal calibration |
| 2 | Elly De La Cruz | SS | 23 | 5.86 | [1.5, 7.7] | $19.6 | Pipeline gap at SS; wide interval reflects young age |
| 3 | Pete Crow-Armstrong | OF | 23 | 5.85 | [1.1, 6.8] | $19.5 | Strong contact profile; OF depth candidate |

### Interpretation

The 80% prediction intervals are intentionally wide — projecting individual player performance is genuinely uncertain. The model's value is not precise point estimates but ranked ordering and surplus identification. A player projecting 3.5 WAR [2.0, 5.0] at a $30M implied market value is a credibly different decision than the same projection with a [0.5, 6.5] interval and an identical point estimate.

Targets with high projected WAR and narrow intervals — suggesting consistent recent performance and stable underlying contact metrics — should receive the most serious consideration.

---

## Limitations

- **Priors:** Position-level WAR priors are weakly informative (Normal(1.5, 1.0) league hyperprior). Early-season figures with < 100 PA should be interpreted cautiously.
- **Aging curves:** The projection model uses age as a linear feature. Non-linear decline post-30 and the asymmetry between peak and decline phases are not explicitly modeled.
- **Contract data:** Spotrac data covers Mariners players only. The $3.3M/WAR implied rate is derived from 8 Mariners contract-seasons and includes arbitration deals — it substantially understates the open-market FA rate of $9–11M/WAR. Projected dollar values in the FA targets table should be scaled up by ~3× to approximate true market cost.
- **Statcast coverage:** Contact-quality features are available from 2015 onward, batters only. Missing values are median-imputed, which reduces predictive power for pitchers and bench players.
- **FA universe approximation:** The 2026 FA class is derived from players with qualified 2025 seasons, not a verified free-agent list. Players under long-term contracts are not excluded.
- **Single-season target:** The model projects one-season WAR. Multi-year contract decisions require compounding this with an aging-curve adjustment not included in this version.

---

*Analysis by Owen Duffy · [owen-duffy.com](https://owenduffy.com/mariners) · [Dashboard](https://public.tableau.com/app/profile/owen.duffy7637/viz/MarinersFrontOfficeAnalytics/MarinersFrontOfficeAnalytics) · [GitHub](#)*
