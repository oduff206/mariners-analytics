# Backlog — Out of Scope for v1

Ideas parked here to avoid scope creep. Revisit after the portfolio ships (June 2026).

## Analysis ideas

- **In-game strategy analysis** — bullpen usage, lineup construction, infield shifting. Coaching lens, separate project.
- **Minor league / prospect pipeline** — prospect WAR, ETA modeling, depth chart projections. Would extend Pillar 2 meaningfully.
- **Attendance / ticket pricing / business ops** — fan-facing business lens, not front-office roster lens.
- **Trade market analysis** — what players could the Mariners acquire via trade? Needs trade value framework.
- **Injury analysis** — IL stints, workload signals, durability scoring.
- **Pitch-level modeling** — pitch mix optimization, tunneling, spin rate analysis. Richer than quality-of-contact aggregates.
- **Umpire / park factor deep-dive** — Safeco/T-Mobile park factors by batted ball type.

## Engineering ideas

- **MotherDuck live sync** — replace committing `.duckdb` to git with push to MotherDuck cloud.
- **dbt** — replace ad-hoc SQL views with a proper dbt project for lineage and testing.
- **Streamlit dashboard** — replace Tableau with a fully custom Streamlit app for more interactivity.
- **Retrosheet play-by-play** — game-level granularity for leverage index, WPA analysis.
