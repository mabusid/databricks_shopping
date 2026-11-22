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
- `tests/`: `oracle.py`, a slow plain-Python reading of the rules, and the checks built on it

## Test
```sh
.venv/bin/python tests/check_steps.py fixtures/*/input data                          # each step vs the oracle
.venv/bin/python scripts/run_fixtures.py fixtures -- .venv/bin/python pipeline/pipeline.py   # the 36 fixtures, no Docker
make fixtures PLATFORM=linux/arm64 CPUS=4 MEM=3g                                       # the 36 fixtures in Docker
```

## Plan
1. [x] Get rules.md, a starter, the fixtures and the dev data
2. [x] Choose an engine (log it in DECISIONS.md): Polars
3. [x] Clean the logs (checked by `tests/check_steps.py`)
4. [x] Join each person's IDs (checked by `tests/check_steps.py`)
5. [x] Build sessions (`user_daily` matches all 36 fixtures and the dev answer)
6. [ ] Credit sessions to ad clicks, count assisting campaigns
7. [ ] All 36 fixtures pass → first submission
8. [ ] Profile and optimize, one change per submission
