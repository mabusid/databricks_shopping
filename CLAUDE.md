# CLAUDE.md

Databricks "Shopping Sessions" track (Builders Buildathon). Turn ~60M messy
click-stream CSV events in `/data` into two exact daily Parquet tables:
`/output/user_daily/` (one row per person) and `/output/campaign_daily/`
(one row per campaign). We submit a Dockerfile. Score = container run time,
start to exit, lower is better. Hard limit 3 min. Baseline 169.6 s (PySpark).
Deadline: Sunday 11:00 AM EST.

## Rules of the road
- **Correct first.** A nearly correct answer scores 0. Nothing is "done"
  until all 36 starter fixtures pass. Never trade correctness for speed.
- **`rules/rules.md` is the source of truth.** Never guess a business rule,
  column name, type or column order. If something is unclear, read rules.md
  and the starter again; if it's still unclear, ask me.
- **Step by step.** Follow the plan in README.md one step at a time. Keep
  changes small, test them, and tick off the step in README.md when done.
- **One change per submission** when optimizing, so each score shows what
  that change did. Practice submissions are limited to 2 per hour.

## Logging (always)
- **DECISIONS.md:** add an entry, newest first, using the template at the
  top, whenever we choose between options (engine, algorithm, data layout,
  optimization, reverting something). Keep it short. When a submission
  tests a decision, fill in its **Result** line with the attempt number
  and score change.
- **SCORES.md:** add a row for every submission: date/time, commit,
  change tested, fixtures passed, local time, official score, notes.
- Propose the entry text to me before writing it if the decision was mine
  to make; record it directly if I already stated the choice.

## Layout
- `rules/`: official rules.md
- `starter/`: official starter, kept unchanged for reference
- `fixtures/`: the 36 edge-case fixtures
- `data/`: dev data (gitignored)

## Before each submission
1. All fixtures pass.
2. `docker build` works, and a cold `docker run` with `/data` and `/output`
   mounted produces both tables; time it.
3. Commit, then log the attempt in SCORES.md.
