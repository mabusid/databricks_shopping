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
   - Attempts #3–#9: Rust speed-ups, 5.9099 → 2.8037 s (see SCORES.md and DECISIONS.md)
   - Branch `lean` (attempt #16 candidate): 24-byte records in a lock-free arena, hashbrown tag fix, own Parquet writer, two-phase scanner, huge-page link table (DECISIONS.md, "Lean data path, round 1"). Linux proxy: 0.227 -> 0.153 s (min of 12)
   - Probe #5 (branch `probe5`): the lean build on the scorer (default, 4 KB pages, 512 buckets) vs the old build at 20M, and lean at 60M
   - Next: the plan below

## Plan toward < 0.4 s (from 2.8037 s, attempt #9)

### The measuring stick
- **Linux in Docker, 4 threads, 3M events predicts the scorer.** Attempt #10's build takes 0.42 s there; × 6.67 for 20M gives 2.80 s, and attempt #9 scored 2.8037.
- **The target.** Under 0.4 s on the scorer means **≤ 0.06 s on that proxy**, 7× faster than today. Every change is measured there (fastest of N runs, `perf`) before it is submitted.
- **This Mac hides the costs.** The M1 has 16 KB pages and a 12 MB L2, so it runs dedup and the person pass 2× faster than Linux.

### Where the time goes (Linux `perf`, 3M events)
| Share | What |
|---|---|
| ~27% | Delimiter scanning and per-line glue. Building `[&[u8]; 7]`, bounds checks and 8-byte word loads are a big part |
| 18% | **Kernel**: zeroing fresh memory on page faults (~145k faults per 3M, ~1M at 20M) |
| ~8% | Timestamp parsing |
| ~7.5% | Linked-ID lookups (a ~60 MB table at 20M) |
| ~5% | Parquet writing (bit-packing of the nullable column) and merging per-thread tallies |
| rest | Dedup and person-pass hash tables, sorting, sessions |

### Floors (Linux, 4 threads, 3M events)
- Just reading all 266 MB of event input: **0.022 s** (12 GB/s). That is ~0.15 s on the scorer.
- Scattering 2.8M × 48-byte records into 256 buckets: **0.050 s with fresh memory, 0.011 s with reused memory**. Page faults make it 4.5× slower.
- So under 0.4 s means the whole pipeline must run close to the cost of reading its input once: one or two compact scatters, no page faults, parsing at ~1 GB/s per thread, and every stage parallel.

### Budget at 3M on the proxy (≤ 0.06 s total)
| Stage | Today | Budget | How |
|---|---|---|---|
| Read + parse events | 0.205 s | ≤ 0.030 | Lean parser v2 (below), overlapped with reading |
| Dedup | 0.059 | ≤ 0.008 | Compact records, pre-faulted memory, own prefetching table |
| Person pass (sessions, credit, assists) | 0.078 | ≤ 0.012 | Compact 24–32 B records, reused scratch space, no reallocation |
| Clicks | 0.029 | ≤ 0.004 | Same parser, run while dedup runs |
| Linked IDs | 0.025 | ≤ 0.003 | Pre-faulted memory; overlap with event parsing |
| Write | 0.023 | ≤ 0.003 | Own minimal Parquet writer (PLAIN pages, RLE definition levels) |

### Techniques, in order (one submission each, measured on the proxy first)
| # | Change | Targets | Expected on the proxy |
|---|---|---|---|
| 0 | `tests/bench_linux.sh` (proxy timing, `time -v`, `perf` with symbols) | measurement | — |
| A | Memory without page faults: a per-thread arena of 2 MB huge pages (`MADV_HUGEPAGE`), pre-faulted in parallel; append-only block lists instead of growing `Vec`s; scratch space reused between passes; nothing freed | the 18% kernel time and the Linux 2× in dedup, person pass and linked IDs | 0.42 → ~0.28 s |
| B | SIMD structural scan (ready as #11), plus a v2 line parser that works on byte positions (no `[&[u8]; 7]`, no bounds checks in the hot loop) | ~27% scanning and glue | → ~0.20 s |
| C | Compact records: copy ≤ 48 B, event ≤ 32 B, click ≤ 32 B (u32 ms offsets, packed price/type/category, 48-bit pointers); no payload for invalid copies | memory traffic between passes | → ~0.16 s |
| D | Fast fields: a fixed-position SWAR F1/F2 timestamp parser (`ingest_ts` is always F1), one 128-bit hash per key in one pass, branchless checks | ~8% timestamps, hashing | → ~0.13 s |
| E | Hiding memory latency: Bloom filter (ready as #10) and software-prefetched probes in batches of 8–16 lines for the linked-ID, dedup and person tables | ~7.5% lookups, plus the dedup/person tables | → ~0.11 s |
| F | Own Parquet writer; per-thread tallies merged in parallel | ~5% | → ~0.10 s |
| G | No idle threads: clicks parsed during dedup; small tables parsed in parallel; linked IDs built while events are read (person resolution after dedup) | idle at stage boundaries | → ~0.08 s |
| H | Start-up and exit: read input into reused huge-page buffers (no 4 KB file mappings to tear down); a static binary on a minimal image | fixed costs | → ~0.07 s |
| I | Several lines interleaved per thread (1BRC: −22%); AVX2/AVX-512 scanning on the scorer | the rest of the parse | → ≤ 0.06 s |

**Honest outlook.** A–F are well-understood techniques and should reach ~0.10 s on the proxy (~0.7 s on the scorer). Reaching ≤ 0.06 s (< 0.4 s) needs G–I as well, and every stage near its floor; that is what the leaders must have done. If an incremental change can't reach its budget, that stage is rewritten from scratch with this design (the current code stays as the fallback). The plan is re-checked after every score.
