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
| 2 | 2026-11-21 ~22:35 EST | `82a5c68` | Memory cuts: compact event copies, cheaper dedup, free tables early | 36/36 | 1.0 (1M dev, Mac); 2.3 (Docker arm64, 4 CPUs) | 28.4511 | No real change from #1: at 20M the Python/Polars engine itself sets the time. Projected 60M memory ~16.7 GB (Linux anon peak 1.07/1.32/1.60 GB at 1/2/3M) |
| 3 | 2026-11-21 ~23:00 EST | `d1612e7` | Rewrite as a native Rust program (`native/`) | 36/36 | 0.15 (1M dev, Mac); 0.5 (3M, Mac); ~0.65 (Docker arm64 incl. container start) | 5.9099 | 4.8× faster than #2. The scorer runs ~1.8× slower than the Mac projection (3.2 s at 20M). Equals Polars on 13 generated sets |
| 4 | 2026-11-21 ~23:20 EST | `3d5df36` | Rust speed-ups round 1: parallel Parquet parts, hash once, lookups while parsing | 36/36 | 0.37 (3M, Mac) | 4.1392 | 30% faster than #3. All comparisons exact. Equals Polars on 10 seeds |
| 5 | 2026-11-21 ~23:40 EST | `bccd5bc` | Rust speed-ups round 2: one-pass delimiter scan, counting sort, buffered output, 2 MB chunks | 36/36 | ~0.30 (3M, Mac) | 3.4492 | 17% faster than #4. Equals Polars on 10 seeds |
| 6 | 2026-11-22 ~00:00 EST | `ee4e10a` | Rust round 3: cache-sized buckets (2048 at 20M), per-thread bucket lists, quote-counting scanner | 36/36 | ~0.26 (3M, Mac; still 256 buckets there) | 3.5092 | Slightly slower than #5 (3.4492). Likely the 2048-way scatter in parsing outweighs smaller dedup tables |
| 7 | 2026-11-22 after 00:02 EST | `9260d86` | 128-bit person identity (no name reads in hot paths) | 36/36 | ~0.242 (3M, Mac) | 3.2035 | 8.7% faster than #6 (3.5092); best so far. Still with 2048 buckets at 20M |
| 8 | 2026-11-22 after 00:08 EST | `cefd089` | Back to 256 buckets (keeps per-thread lists and quote-counting scanner) | 36/36 | ~0.24 (3M, Mac) | 3.0086 | 6% faster than #7 (3.2035); best so far. Confirms 2048 buckets hurt |
| 9 | 2026-11-22 after 00:17 EST | `8d57ae9` | Linked-ID table built in parallel (lock-free union-find) | 36/36 | ~0.23 (3M, Mac) | 2.8037 | 6.8% faster than #8 (3.0086); best so far |
