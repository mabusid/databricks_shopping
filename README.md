# Shopping Sessions (Databricks track, Builders Buildathon)

Turn ~60M messy click-stream events into two exact daily Parquet tables
(`/output/user_daily/`, `/output/campaign_daily/`), as fast as possible.
Score = container run time, start to exit. A nearly correct answer scores 0.

## Scoring (track page, scorer version shopping-sessions-5)
- **Baseline:** 167.6152 s (PySpark, local mode).
- **Practice:** about 20M events. Capped at 2× the baseline and never over 5 min. Runs under 20 s are timed 3 times and the median counts. 2 submissions per hour per team.
- **Final:** the chosen commit is rebuilt and run 3 times on about 60M new events. The median counts, and any run over 3 min means no score. The top 30 are retimed 9 times across 3 fresh datasets, using the mean of the per-dataset medians. Ties go to the earlier submission.
- **Container:** linux/amd64 image up to 8 GB; 8 vCPUs, 24 GB RAM, no swap; network only during the build (up to 10 min).
  - Disk: `/output` + `/scratch` up to 50 GB; root is read-only except a 64 MB in-memory `/tmp`.
  - Process: runs as uid 1000, at most 4,096 processes or threads, stdout up to 50 MB.
- **Repo:** public, at most 50 MB, Dockerfile at the root, no data.
- **Deadline:** Sun 11:00 AM EST.

- `DECISIONS.md`: what we decided and why (newest first)
- `SCORES.md`: every submission and its score
- `rules/`: the official rules.md
- `starter/`: the three official starters as zips, unchanged; the Polars one is copied to the root
- `fixtures/`: the 36 edge-case fixtures
- `native/`: the pipeline as a Rust program; this is what the Dockerfile builds and runs
- `pipeline/pipeline.py`: the Polars pipeline, kept as a second, checked implementation (commit `82a5c68` runs it); `scripts/`: the starter's run and diff scripts
- `data/`: the dev data (gitignored); `dev-answer/`: its expected output (gitignored)
- `tests/`: `oracle.py`, a slow plain-Python reading of the rules, and the checks built on it

## Test
```sh
cargo build --release --manifest-path native/Cargo.toml                              # build the Rust binary
cargo test --release --manifest-path native/Cargo.toml                               # its unit tests
.venv/bin/python scripts/run_fixtures.py fixtures -- native/target/release/shopping  # the 36 fixtures, no Docker
DATA_DIR=data OUTPUT_DIR=out native/target/release/shopping && (cd scripts && ../.venv/bin/python compare.py ../out ../dev-answer)
# The Rust vs Polars output on any generated dataset: run both into two folders, then scripts/compare.py A B
.venv/bin/python tests/check_steps.py fixtures/*/input data                          # Polars, each step vs the oracle
make fixtures PLATFORM=linux/arm64 CPUS=4 MEM=3g                                       # the 36 fixtures in Docker
```

## Plan
1. [x] Get rules.md, a starter, the fixtures and the dev data
2. [x] Choose an engine (log it in DECISIONS.md): Polars
3. [x] Clean the logs (checked by `tests/check_steps.py`)
4. [x] Join each person's IDs (checked by `tests/check_steps.py`)
5. [x] Build sessions (`user_daily` matches all 36 fixtures and the dev answer)
6. [x] Credit sessions to ad clicks, count assisting campaigns (both tables match the dev answer)
7. [x] All 36 fixtures pass → first submission (attempt #1: 28.4397 s)
8. [ ] Profile and optimize, one change per submission
   - Attempt #2: Polars memory cuts (28.4511 s)
   - Rewrite in Rust (`native/`): matches the dev answer, 36/36 fixtures, and the Polars output on 13 generated datasets
