# Scores

One row per submission, newest at the bottom. Score = container run time in seconds (lower is better; a run under 20 s is repeated 3 times and the median counts).
Practice limit: 2 submissions per hour. Link the decision each attempt tests.

Reference (track page, scorer version shopping-sessions-5): baseline 167.6152 s (PySpark, local mode).
- Practice runs use about 20M events, capped at 2× the baseline and never over 5 min.
- The final uses about 60M new events: 3 timed runs, median scored, and any run over 3 min means no score.
- The top 30 are retimed 9 times across 3 fresh datasets.
- Ties go to the earlier submission.

| # | Date / time | Commit | Change tested | Fixtures | Local time (s) | Score (s) | Notes |
|---|-------------|--------|---------------|----------|----------------|-----------|-------|
| 1 | 2026-11-21 ~22:00 EST | `5bfaa0e` | First full pipeline, steps 3–6 (Polars) | 36/36 | 2.2 (1M dev, Mac); 4.6 (Docker arm64, 4 CPUs) | 28.4397 | Practice data, ~20M events. Baseline is 167.6152. Times 3 for 60M ≈ 85 s; untested 60M memory is the risk |
