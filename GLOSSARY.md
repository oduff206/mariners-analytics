# Sabermetrics Glossary

Plain-English definitions of every stat used in this project. Reference: [FanGraphs Library](https://library.fangraphs.com/).

---

## Batting

**wOBA** (Weighted On-Base Average)  
A rate stat that assigns different weights to each way of reaching base (single, double, walk, HR, etc.) based on how many runs each is actually worth. Scale similar to OBP (~.320 is average). More accurate than OPS.

**wRC+** (Weighted Runs Created Plus)  
Park- and league-adjusted offensive value. 100 = league average. 120 = 20% above average. The most comprehensive single offensive rate stat.

**OBP** (On-Base Percentage)  
(H + BB + HBP) / (AB + BB + HBP + SF). How often a batter reaches base.

**SLG** (Slugging Percentage)  
Total bases / at-bats. Measures power but ignores walks.

**OPS**  
OBP + SLG. Quick summary of offense, but double-counts OBP's run value. wOBA/wRC+ are better.

**BABIP** (Batting Average on Balls in Play)  
(H − HR) / (AB − K − HR + SF). Measures luck/defense. League avg ~.300. Batters with high BABIP relative to career norm often regress.

---

## Pitching

**ERA** (Earned Run Average)  
Earned runs allowed per 9 innings. Affected by defense and luck.

**FIP** (Fielding Independent Pitching)  
Estimates ERA based only on outcomes the pitcher controls: strikeouts, walks, HBP, home runs. Better predictor of future ERA than ERA itself.

**xFIP**  
FIP with home run rate regressed to league average. Removes HR variance.

**WHIP**  
(Walks + Hits) per inning pitched. Simple context for run prevention.

**K/9, BB/9**  
Strikeouts and walks per 9 innings. Command metrics.

**K%** and **BB%**  
Strikeout and walk rate as % of batters faced. More stable than per-9 equivalents.

---

## Statcast (quality-of-contact)

**Exit Velocity**  
Speed of the ball off the bat (mph). Higher = harder contact = more expected value.

**Launch Angle**  
Vertical angle of the ball off the bat. 10–25° is the sweet spot (line drives and fly balls).

**Barrel**  
Batted ball with optimal exit velocity + launch angle combination, historically producing ≥1.500 SLG. Barrel% = barrels / batted ball events.

**Hard Hit%**  
% of batted balls with exit velocity ≥ 95 mph.

**xBA** (Expected Batting Average)  
Batting average expected from a batter's quality of contact, independent of defense and luck.

**xwOBA** (Expected wOBA)  
wOBA expected from quality of contact. Better leading indicator than wOBA when sample is small.

**xERA**  
ERA expected based on quality of contact allowed. FIP-like but uses Statcast data.

**Whiff%**  
% of swings that miss. Higher = harder to make contact against (pitchers).

---

## Value

**WAR** (Wins Above Replacement)  
All-in-one value metric. How many wins a player adds vs a freely available replacement-level player. Scale:
- < 0: below replacement
- 0–1: replacement level
- 1–2: bench / depth
- 2–3: average starter
- 3–5: above average
- 5+: All-Star
- 8+: MVP caliber

**bWAR vs fWAR**  
Baseball-Reference WAR and FanGraphs WAR differ mainly in pitching (bWAR uses RA/9-based runs, fWAR uses FIP-based). This project uses fWAR via `pybaseball`.

**AAV** (Average Annual Value)  
Total contract value ÷ years. Standard way to compare contracts of different lengths.

**$/WAR**  
Contract AAV divided by WAR produced. Lower = more efficient. League price per win on the FA market ~$8–10M (2024). Arbitration and pre-arb players are far cheaper.

---

## Draft

**Draft slot value**  
MLB assigns a bonus pool value to each draft pick based on its position. First overall is worth ~$10M; later picks are worth far less. Used to normalize draft ROI.

**WAR per draft slot**  
Career WAR accumulated by players drafted at a given pick range, divided by the number of picks. Measures development pipeline output vs league average.

**MLB reach rate**  
% of draftees from a class who reached the major leagues for at least one game.

---

## Bayesian & Statistical Methods

**Partial pooling**  
Hierarchical model technique where group-level estimates (e.g. position means) are informed by the overall dataset rather than fit independently. Shrinks noisy small-sample estimates toward the group mean — a catcher with 80 PA borrows strength from all catchers in the dataset rather than anchoring entirely on 80 noisy observations.

**Credible interval**  
Bayesian analog of a confidence interval. A 94% credible interval contains the true parameter value with 94% posterior probability, conditional on the model and data. Unlike a frequentist confidence interval, it makes a direct probability statement about the parameter.

**HDI (Highest Density Interval)**  
The shortest credible interval containing a given probability mass. Used in this project for posterior WAR distributions. An HDI entirely below the league-average WAR line is strong evidence of underperformance; one straddling zero indicates genuine uncertainty.

**Posterior distribution**  
The probability distribution over a parameter after updating prior beliefs with observed data. In this project, each player has a posterior distribution over their true-talent WAR — not just a point estimate.

**NUTS sampler**  
No-U-Turn Sampler; the MCMC algorithm used by PyMC to draw samples from posterior distributions. Adaptively tunes step size and trajectory length, making it well-suited to correlated high-dimensional posteriors. Run at `target_accept=0.98` in this project to minimize divergences.

**Quantile regression**  
Regression that predicts a specific quantile (e.g. p10, p90) of the outcome distribution rather than the mean. Used here to produce 80% prediction intervals around WAR projections — the p10 and p90 bounds capture honest uncertainty about individual player performance.
