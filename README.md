# Shopping Sessions (Databricks track, Builders Buildathon)

Turn ~60M messy click-stream events into two exact daily Parquet tables
(`/output/user_daily/`, `/output/campaign_daily/`), as fast as possible.
Score = container run time, start to exit. A nearly correct answer scores 0.

- `DECISIONS.md`: what we decided and why (newest first)
- `SCORES.md`: every submission and its score
- `rules/`: the official rules.md
- `starter/`: the three official starters as zips, unchanged; the Polars one is copied to the root
- `fixtures/`: the 36 edge-case fixtures
- `pipeline/pipeline.py`: our pipeline; `scripts/`: the starter's run and diff scripts
- `data/`: the dev data (gitignored); `dev-answer/`: its expected output (gitignored)

## Plan
1. [x] Get rules.md, a starter, the fixtures and the dev data
2. [x] Choose an engine (log it in DECISIONS.md): Polars
3. [ ] Clean the logs
4. [ ] Join each person's IDs
5. [ ] Build sessions
6. [ ] Credit sessions to ad clicks, count assisting campaigns
7. [ ] All 36 fixtures pass → first submission
8. [ ] Profile and optimize, one change per submission
