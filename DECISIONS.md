# Decisions log

One entry per decision, newest first. Keep each entry short.
Scores live in SCORES.md; link the attempt number here when a decision is tested.

<!-- Template — copy above the newest entry
## <decision in a few words>
- **Context:** why this came up
- **Options:** A) … B) …
- **Chose:** A, because …
- **Pros:** …
- **Cons / risks:** …
- **Result:** attempt #N, score X → Y, or "reverted"
-->

## Measured on the scorer (probe #3): what to keep
- **Context:** Probe #3 timed 10 setups on 20M generated events on the scorer (best of 2; same output checksum for all)
- **Kept:**
  - Events: one shuffle instead of two, clicks in the same pass (`c2fc1b8`), word-at-a-time timestamps (`f51c5ab`), `user_daily` parts written per group of buckets during the person pass (`e18d907`). Together: 2.574 → 2.103 s (−18%). The overlapped write is most of it: person pass + write 0.83 → 0.40 s
  - The x86-64-v3 build via the launcher: 2.630 → 2.574 s (−2%); its AVX2 unit tests pass on the scorer
- **Dropped (off by default; knobs stay for probes):**
  - Compact Parquet encodings: 2.185 s, slower (encoding CPU costs more than the bytes saved)
  - pread input instead of mmap: 2.287 s, slower
  - Prefetch depth 16 (2.140), click prefetch ring (2.111): no gain
  - 2048 buckets (2.221), 16 threads (2.229): slower
- **Result:** attempt #15 (`99e28a7`): 2.0876 → 1.9925 s (−4.6%, less than the −18% on generated data). Probe #4 then put 35% of the parse stage (0.46 of 1.33 s at 20M) on writing records into fresh memory

## Scorer probes, and bundling changes they measured
- **Context:** Practice runs hide our logs, and the local proxy can't show the scorer's costs (huge pages, mmap faults, how /data is mounted, exit teardown, AVX2). We are at 2.0876 s; the top is 0.4293 s, with ~7.5 h and 2 submissions an hour left. When a submission fails on the dev set, the scorer returns our logs
- **Options:** A) Probe submissions: generate 20M (and 60M) events inside the container, time several builds and microbenchmarks there, then fail the dev check on purpose with one made-up `campaign_daily` row. B) Keep measuring only on the proxy
- **Chose:** A, the user's decision. Also the user's decision: a real submission may bundle several changes that probes measured one by one, instead of one change per submission
- **Pros:**
  - Real stage timings on the scorer, A/B of several builds per slot, and the 60M peak-memory check CLAUDE.md asks for
- **Cons / risks:**
  - Each probe uses one of the 2 slots an hour and scores nothing
  - `probe/Dockerfile` replaces the root Dockerfile only on a `probe` branch; main keeps building the real pipeline. `SHOPPING_PROBE` and `SHOPPING_FAKE_ROW` are off unless set
- **Result:** Probe #1 (`889fc97`) failed the dev check as planned, but the scorer showed only the differing row, not our logs. Fix: probes write their measurements as text in made-up rows (`SHOPPING_FAKE_ROWS`, `probe/run_rows.sh`), which the scorer shows in full

## Experiment: 1024 buckets for large inputs, with block lists
- **Context:** At 20M events, 256 buckets mean ~78k events per dedup table (~1.25 MB), beyond a Xeon core's 1 MB L2. Attempt #6 tried 2048 buckets with growing `Vec`s and lost ~0.06 s, mostly from scattering into fresh, pre-reserved memory. Block lists (#13) have since made the scatter cheap
- **Options:** A) 1024 buckets when the events exceed 1 GB (~10M+; ~20k events per table at 20M), else 256. B) Stay at 256
- **Chose:** A, as a one-change experiment. The M1 can't show the effect: its caches hold the tables, and our test data is below 1 GB
- **Pros:**
  - Dedup and person-pass tables should fit L2 on the scorer
- **Cons / risks:**
  - Wider scatter (1024 list tails per thread) and more partly filled blocks
  - If the score gets worse, revert to 256
  - Checks: 36/36 fixtures, dev answer, Rust = Polars on 10 seeds at 1024 buckets (forced) and at 256
- **Result:** Submitted with #16 (newest commit, `7642935` code): 2.1554 → 2.0876 s for both changes. Not worse overall, so 1024 buckets stays

## Tried and dropped: the prefetch pipeline for clicks
- Same technique as #16, applied to click lookups. Linux proxy (2M, 16 interleaved runs vs #16): best 0.212 vs 0.212 s, median 0.293 vs 0.244 s. No gain (click lookups are a small cost), so not kept

## Person lookups: flat link table, prefetched 8 lines ahead
- **Context:** In the parse-pass split, the person lookup added 0.030 s at 2M (~57 ns a line), mostly waiting on memory for the table probe. At 20M the linked-ID table is ~10× bigger than a Xeon core's cache
- **Options:** A) Our own open-addressing table (40-byte slots holding the ID key, person key and packed name; linear probing, at most half full; one table per part, built in parallel), plus a software pipeline: each parsed copy computes its key, checks the Bloom filter and prefetches its slot, then waits in an 8-slot ring; its person is filled in when it leaves. B) Keep hashbrown (its slots can't be prefetched from outside)
- **Chose:** A. A real (0, 0) key, practically impossible, is kept in its own field, so the empty-slot marker never makes a result wrong. Prefetch is SSE `_mm_prefetch` on x86-64 and `prfm` on arm64
- **Pros:**
  - Linux proxy (2M, 16 interleaved runs vs #15): best 0.207 → 0.203 s, p25 0.231 → 0.219 s, median 0.239 → 0.232 s
  - Likely more on the scorer, where the table doesn't fit in cache
- **Cons / risks:**
  - Records leave the parse in a slightly different order, which doesn't matter: dedup keeps the smallest ingest_ts, and tied copies are identical
  - Checks: unit tests, 36/36 fixtures, dev answer, Rust = Polars on 10 seeds + seed 15
- **Result:** Submitted with #17 (newest commit, `7642935` code): 2.1554 → 2.0876 s for both changes

## Compact records: 8-byte string slices
- **Context:** The parse-pass split (Linux, 2M) still put the biggest cost on writing records into fresh memory: + 0.09–0.13 s for fresh block lists against + 0.03–0.04 s reused. That cost scales with the bytes written
- **Options:** A) Pack each string slice into 8 bytes (48-bit address, 16-bit length), so copy 80 → 64 B, event 48 → 40 B, click 48 → 40 B. B) Also shrink timestamps to u32 offsets (more code, ~4 more bytes)
- **Chose:** A. Exactness is unchanged: a slice that can't be packed (65,535+ bytes, or an address above 2^48) goes to a side table; compile-time asserts pin the sizes
- **Pros:**
  - Linux proxy (2M, 20 interleaved runs each vs #14): best 0.209 → 0.206 s, p25 0.222 → 0.210 s, median 0.238 → 0.217 s
- **Cons / risks:**
  - Relies on 48-bit user-space addresses (true for x86-64 4-level paging and arm64 Linux), with the side table as a safe fallback
  - Checks: unit tests, 36/36 fixtures, dev answer, Rust = Polars on 10 seeds + seed 15
- **Result:** Attempt #15: 2.2333 → 2.1554 s

## Parse: branchless field quotes, one-check timestamps, XXH3-128 person keys
- **Context:** A line-level Linux profile of #13 put 14% of the whole run on one line: the per-field check of whether a field is wrapped in quotes (`quotes != 0 && e - s >= 2 && chunk[s] == '"' && chunk[e-1] == '"'`). ~20% of fields are quoted at random, so the branch mispredicts about once per line. Timestamps were ~10%
- **Options:** A) Rewrite these three hot spots in place. B) A full rewrite of the line loop
- **Chose:** A:
  - Quotes: read each field's first and last byte from clamped (always valid) positions, combine the checks with `&`, and strip arithmetically, with no branch
  - Timestamps: dispatch on shape (length 20/25 with `-` at 4 → ISO) so ISO strings skip the all-digits attempt; fold every digit, separator and range check into one flag
  - Person keys: XXH3-128 (`xxhash-rust`) gives both 64-bit halves in one pass, instead of two foldhash passes
- **Pros:**
  - Linux proxy (2M, interleaved with #13): parse done at 0.152 → 0.129 s in the best runs; mean 0.302 → 0.252 s
- **Cons / risks:**
  - Some unsafe indexing (clamped, commented)
  - Checks: unit tests (timestamp edge cases, 50k-chunk fuzz test), 36/36 fixtures, dev answer, Rust = Polars on 10 seeds + seed 15, and the emulated amd64 image (dev answer, 36/36)
- **Result:** Submitted with #13 as `004123b`: 2.4810 → 2.2333 s for both changes

## Memory: pooled block lists on 2 MB huge pages
- **Context:** A Linux experiment on 2M events (4 threads) split the parse pass: scan 0.029 s, + timestamps 0.053 s, + person lookup 0.077 s, full parse 0.083 s, **+ push into fresh memory 0.194 s**, + push into reused memory 0.115 s. Writing records into fresh memory cost more than all the parsing
- **Options:**
  - A) Bucket lists as chains of 64 KB blocks from one pool: never reallocated; a bucket's blocks go back to the pool after use and the next passes reuse them; slabs are 16 MB, 2 MB aligned, with `madvise(MADV_HUGEPAGE)`
  - B) Allocator settings only: glibc `hugetlb`/mmap threshold 0.159 s, mimalloc with large pages 0.171–0.175 s, against 0.11 s reused
  - C) One `mmap` huge-page region per thread (slower in this memory-starved VM: the kernel had to compact memory)
- **Chose:** A
- **Pros:**
  - Linux proxy (2M, interleaved with #12): best 0.24–0.25 → 0.21–0.22 s, mean 0.292–0.295 → 0.245–0.253 s
  - Page faults ~97–100k → ~18–22k. Holds whether the kernel's THP mode is `madvise` or `always`
- **Cons / risks:**
  - Some unsafe code (raw block pointers), kept in `blocks.rs`, with a unit test for push, iterate, release and reuse across types
  - Checks: 36/36 fixtures, dev answer, Rust = Polars on 10 seeds and at 2048 buckets
- **Result:** Submitted with #14 as `004123b`: 2.4810 → 2.2333 s for both changes

## Output: one tally per thread, null and non-null rows in separate parts
- **Context:** Linux `perf` on #11 (2M events): `Tally::merge` 2.7%; Parquet bit-packing of `top_category`'s null markers 3.5%. rayon's `fold` made a tally per work split, so dozens of ~1 MB campaign × day arrays were merged pairwise
- **Options:** A) One tally per worker thread (the `per_thread` helper), merging only the campaign counts once. User rows stay in each thread's lists, split into "has a top category" and "none", one Parquet part each. B) Our own Parquet writer
- **Chose:** A first; it's small. Row order doesn't matter, so each part's null markers form one run, which Parquet stores run-length encoded instead of bit-packed
- **Pros:**
  - Linux proxy (2M events, 4 threads, interleaved with #11's build): best 0.239 → 0.211 s, mean 0.289 → 0.266 s
- **Cons / risks:**
  - Up to 2 × threads part files (16), which the rules allow
  - Checks: 36/36 fixtures, dev answer, Rust = Polars on 10 seeds
  - Also tested and dropped: freeing the copy buffers after dedup, with and without glibc keeping freed memory. Interleaved A/B on 2M showed no difference (best 0.235 / 0.243 / 0.235 s)
- **Result:** Submitted together with #11 as `6ca30ff`: 2.6968 → 2.4810 s for both changes

## SIMD delimiter scan
- **Context:** Scanning and per-line glue were the biggest cost on Linux (~27%). simdjson's stage 1 builds structural bitmasks 64 bytes at a time
- **Options:** A) SSE2 on x86-64 (baseline, so no runtime check, and testable under emulation), NEON on arm64, 8-byte words elsewhere. B) AVX2/AVX-512 with runtime detection (untestable here: the emulator lacks AVX2)
- **Chose:** A. Each 64-byte block becomes comma, quote and newline masks; the 8-byte loop handles the tail
- **Pros:**
  - Parse ~0.102 → ~0.093 s on the M1. Linux proxy (3M) 0.42 → 0.37 s
- **Cons / risks:**
  - Arch-specific unsafe code. Fuzz test (50k random chunks up to 300 bytes) passes on arm64 and on x86-64 under emulation; the amd64 image matches the dev answer and 36/36 fixtures
- **Result:** Submitted together with #12 as `6ca30ff`: 2.6968 → 2.4810 s for both changes

## Bloom filter in front of the linked-ID table
- **Context:** Every event and click looks up its ID in the linked-ID table, which at 20M holds ~1.2M entries of 48 bytes (~60 MB), far beyond a Xeon core's cache. 38% of events (dev set and 3M set) come from IDs in no link, which find nothing
- **Options:** A) A blocked Bloom filter: 8+ bits per linked ID, 2 bits per key in one 512-bit block (one cache line). A clear bit means "not linked" for sure, so the table is skipped. B) Leave the lookups as they are
- **Chose:** A. Results can't change: a Bloom filter has no false negatives, and its ~2% false positives just do the normal lookup
- **Pros:**
  - Saves a probe of the big table for ~38% of events and clicks. At 20M the filter is ~2 MB
- **Cons / risks:**
  - Neutral on the M1 (the table fits its 12 MB L2), so the gain is a scorer-only theory
  - Linked IDs pay one extra cache-line read
  - Checks: 36/36 fixtures, dev answer, Rust = Polars on 10 seeds + seed 15
- **Result:** Attempt #10: 2.8037 → 2.6968 s. Memory latency matters on the scorer

## Linked IDs built in parallel
- **Context:** The linked-ID stage ran on one thread while the other 7 waited. At 3M it took ~21 ms (parse 2.6 ms, interning + union 10.7 ms, names + table 8.2 ms), so probably ~0.15 s locally at 20M and more on the scorer
- **Options:** A) Parallelize every step. B) Overlap it with event parsing (that would mean resolving persons after parsing, which earlier measured slower)
- **Chose:** A. IDs go to 64 parts by their 128-bit key and are numbered per part, then:
  - Joined with a lock-free union-find: compare-and-swap links a root only under a smaller root, so no loops; path halving
  - Each person's byte-wise smallest ID is chosen with an atomic compare-and-swap
  - One lookup table is built per part
- **Pros:**
  - 3M events: the stage drops 0.020 → 0.010 s (whole run ~0.285 → ~0.238 s)
- **Cons / risks:**
  - Lock-free code depends on thread timing. Checked by 8 repeated runs with identical output on 3M, plus 36/36 fixtures, the dev answer and Rust = Polars on 10 seeds
- **Result:** Attempt #9: 3.0086 → 2.8037 s

## Back to 256 buckets
- **Context:** Attempt #6 scaled buckets with the input (2048 at 20M) and was slower, 3.4492 → 3.5092 s. The scorer is consistent (the two Polars runs were 0.01 s apart), so the loss is real
- **Options:** A) A fixed 256 buckets, keeping round 3's per-thread lists and quote-counting scanner. B) Try 512/1024
- **Chose:** A. 256 is the count attempt #5 already measured. B would need several submissions to tune
- **Pros:**
  - Smaller scatter (256 lists per thread fit the L1 cache)
- **Cons / risks:**
  - Dedup tables at 20M (~78k events) exceed a Xeon's L2 again
  - `SHOPPING_BUCKET_BITS` still lets tests force other counts
- **Result:** Attempt #8: 3.2035 → 3.0086 s

## Person identity: 128-bit name hash instead of byte comparison
- **Context:** Telling people apart compared their names byte by byte. For one-ID people the bytes sit in each event's own line, scattered across the input, so most comparisons and linked-ID lookups read memory that isn't in cache; on a Xeon with ~1 MB L2 per core that likely costs a trip to main memory per event
- **Options:** A) Identify a person by two independent 64-bit hashes of the name (foldhash fast and quality variants, different seeds) and never read the name. B) Keep exact byte comparison
- **Chose:** A, the user's decision ("lets try to 128-bit identity next"):
  - Linked IDs are matched by the same 128-bit key, and each table entry holds the person's name and key, so a lookup is one memory access
  - Event dedup stays exact: it compares `event_id` bytes, but only when two hashes already match (true duplicates)
- **Pros:**
  - 3M events (M1): ~0.275 → ~0.242 s. Parse 0.121 → 0.100 s, person pass 0.051 → 0.038 s, clicks 0.022 → 0.018 s
  - Likely more on the scorer, whose caches are much smaller
- **Cons / risks:**
  - Not exact in principle: two different names sharing both hashes would merge two people. For a few million names that is about 1 in 10^26 per run, far below hardware error rates
  - Checks: 36/36 fixtures, dev answer, Rust = Polars on 10 seeds and at 2048 buckets
- **Result:** Attempt #7: 3.5092 → 3.2035 s

## Rust speed-ups, round 3: cache-sized buckets, per-thread lists
- **Context:** Attempt #5 scored 3.4492 s. This Mac (M1: 12 MB L2 on the fast cores) holds all our tables in cache at 3M events, but a scorer Xeon has ~1 MB L2 per core. At 20M, 256 buckets mean ~78k events per dedup table (~1.4 MB), over that limit
- **Options:** A) Scale the bucket count with the input (~16k events each, 256–4096), with bucket lists kept per worker thread so their number doesn't grow with the input. B) Keep 256 buckets
- **Chose:** A:
  - Buckets: 256 at 3M, 2048 at 20M, 4096 at 60M
  - Lists are per worker thread (rayon's `fold` made too many accumulators and was 2× slower, so it was dropped)
  - The delimiter scanner records commas and only counts quotes; a line is valid when its quote count equals 2 for every field that starts and ends with a quote, which is equivalent to the old per-field rule (fuzz test)
  - `SHOPPING_BUCKET_BITS` forces a bucket count for testing; checks pass at 256, 2048 and 4096
- **Pros:**
  - Per-person pass 0.064 → ~0.055 s at 3M (8 lists per bucket instead of ~150)
  - At 20M+ each dedup table should fit a Xeon's L2 (not measurable here)
- **Cons / risks:**
  - The cache benefit is a theory until the scorer confirms it
  - Tried and dropped: mimalloc (no gain on Linux: best 0.854 vs 0.864 s); largest-bucket-first scheduling (no gain); `taskpolicy -b` as a small-cache stand-in (memory throttling made it meaningless)
- **Result:** Attempt #6: 3.4492 → 3.5092 s (slower). The 2048-way scatter likely costs more than the smaller tables save; next, go back to 256 buckets and keep the per-thread lists

## Rust speed-ups, round 2 (after research)
- **Context:** Attempt #4 scored 4.1392 s. 1BRC write-ups (curiouscoding.nl, morling.dev) and simdjson (Langdale & Lemire, arXiv 1902.08318) say the top parsers find all delimiters in a block at once and run at ~1–2.4 GB/s per core; ours was ~0.25 GB/s. A profiler (`/usr/bin/sample`, with `SHOPPING_REPEAT`) then showed 36% idle threads, 11% unbuffered `write`, 6% freeing memory, 11% linked-ID lookup, 10% timestamps
- **Options:** A) Word-at-a-time (SWAR) delimiter scan. B) memchr3 SIMD scan. C) Keep the per-line memchr split
- **Chose:** A, plus the other fixes below:
  - Scanning: one pass per chunk finds commas, quotes and newlines 8 bytes at a time, splitting lines and fields together. It is fuzz-tested against the old splitter (20,000 random chunks). B was slower: 0.141 vs 0.127 s parse
  - Person pass: a counting sort by person (two linear passes) replaces the big sorts; only per-person time sorts are left
  - Parquet goes through an 8 MB `BufWriter`, flushed explicitly so an error stops the run
  - Big tables are never freed (`mem::forget`), so no madvise/munmap cost before exit
  - Chunks are 2 MB instead of 16 MB, which balances the threads better (clicks had only ~3 chunks at 3M)
- **Pros:**
  - 3M events: ~0.37 → ~0.30 s. Parse 0.147 → 0.119 s, person pass 0.082 → 0.062 s, dedup 0.033 → 0.025 s, clicks 0.038 → 0.024 s
- **Cons / risks:**
  - The Mac's timings are noisy (±0.03 s), so small gains are uncertain
  - Not yet done: parallel linked-ID building (sequential today), cheaper timestamp parsing, multi-stream parsing
- **Result:** Attempt #5: 4.1392 → 3.4492 s

## Rust speed-ups, round 1
- **Context:** Attempt #3 scored 5.9099 s. Stage timers on 3M events (Mac, best of 7) put the time in parse 0.10 s, dedup 0.13 s, the person pass 0.085 s and the Parquet write 0.10 s; the whole run took 0.479 s
- **Options:** A) The fixes below, keeping every comparison exact. B) Also tell people apart by a 128-bit name hash instead of comparing names. C) Also match linked IDs by a 128-bit hash
- **Chose:** A:
  - Parquet: no statistics, and `user_daily` written as one part file per thread in parallel
  - Each key is hashed once; `hashbrown::HashTable` reuses the stored hash
  - The person and category are looked up while parsing, when the line is in cache
  - Slices at the same address with the same length count as equal without reading their bytes
- **Pros:**
  - 3M events: ~0.479 → ~0.37 s. Write 0.10 → ~0.04 s, dedup 0.13 → ~0.035 s
- **Cons / risks:**
  - B saved only ~0.009 s and isn't exact (a 1-in-10^26 chance of merging two people), so it was reverted under "never trade correctness for speed"
  - C gave no gain (the cost is the table probe, not the byte check), so it was reverted
- **Result:** Attempt #4: 5.9099 → 4.1392 s

## Engine: rewrite as a native Rust binary
- **Context:** The leaderboard top is ~0.5 s on the ~20M practice set; we are at ~28 s with Polars, and halving our local time (attempt #2) did not move the score. Python + Polars start-up alone is ~0.3 s, so the top can't be reached from there
- **Options:** A) One Rust binary that reads the files directly from memory. It parses lines with a parser written for our CSV rules, runs hash-partitioned passes (dedup by `event_id`, then group by person), and does sessions, credit and assists per person in plain loops. B) Keep tuning Polars (maybe 10–15 s at best). C) C++
- **Chose:** A, because the user asked to work toward the top. Rust is installed, the generator is Rust, and crates (rayon, memmap2, parquet) cover the plumbing
- **Pros:**
  - Start-up in milliseconds, no dataframe overhead, and full control of memory
  - Checked by the oracle, the 36 fixtures, the dev answer, and the Polars version on any generated data
- **Cons / risks:**
  - A large rewrite with the deadline at Sun 11:00 AM EST
  - New code means new chances to be wrong, which scores 0
  - Mitigation: the Polars commit `82a5c68` (28.4511 s, correct) stays the fallback for the final, and the Dockerfile switches only once the Rust version passes every check
- **Result:** Attempt #3: 28.4511 → 5.9099 s

## Memory: compact event copies, cheaper dedup, free tables early
- **Context:** The final uses ~60M events in 24 GB with no swap, and practice runs (~20M) can't show that. Peak memory footprint grew ~0.5 GB per 1M events (1M→3M, macOS), which projects to ~30 GB at 60M: out of memory, so no score
- **Options:** A) Measure each stage and fix the biggest costs in place. B) Use integer codes for persons and campaigns throughout. C) Spill to `/scratch`
- **Chose:** A. It is the smallest change that fixes the measured costs:
  - Each event copy's values and validity are computed in one streamed pass into compact columns
  - Dedup splits off the duplicated `event_id`s and sorts only those, instead of a min over every `event_id`. At 3M: +0.16 GB vs +0.77 GB, 0.30 s vs 0.69 s; options B–E were also measured
  - `event_id` is dropped after dedup, and the events tables are freed before the click work starts
  - A session's start is carried down with `forward_fill` instead of a min over each session
- **Pros:**
  - Footprint at 1M 1.01 → 0.67 GB, at 3M 2.02 → 1.20 GB. Growth is ~0.27 GB per 1M events, about 16–17 GB at 60M
  - Docker dev run 4.6 → 2.3 s
- **Cons / risks:**
  - Still a projection from 3M on macOS, not a 60M measurement
  - Several small changes go in one submission, all aimed at memory
  - B is left in reserve. The assist join of runs × person-days (~1.25 rows per event) is the next-biggest cost
- **Result:** Attempt #2: 28.4511 s, no real change from #1 (28.4397). The memory headroom (projected 60M peak ~16.7 GB from Linux runs at 1–3M) was the goal

## Assists: count per day, don't list pairs
- **Context:** The dev set has 181,032 sessions but 1,958,238 (session, campaign) assist pairs. At 60× the data that is likely over 100M pairs, more for whales, so listing pairs is too costly
- **Options:** A) Per (person, campaign), merge the clicks' 7-day spans into disjoint runs. Cut each run by day, keeping only days with sessions, then count sessions per piece from a running count (`join_asof`). B) Join each session with the person's clicks in its 7 days and list distinct campaigns
- **Chose:** A
- **Pros:**
  - Work grows with clicks × session-days, not sessions × clicks
  - Merging runs counts each session once per campaign with no dedup step
- **Cons / risks:**
  - Harder to read than B, so `tests/oracle.py` does B by brute force as the check
- **Result:** Attempt #1 (28.4397 s), as part of the first full pipeline. Matches the oracle on 37/37, all 36 fixtures pass, and the dev answer matches exactly

## Credit: backward join_asof
- **Context:** Each session goes to the person's latest click at or before its start within 7 days, with ties going to the smallest `click_id`
- **Options:** A) First keep one click per (person, ts), the smallest `click_id`. Then a backward `join_asof` by person, nulling matches over 7 days old. B) Join sessions to all clicks in range and pick the best
- **Chose:** A. The tie is settled before the join, so the join is never ambiguous
- **Pros:**
  - One sort-merge pass
  - Exact 7-day check in our code, not Polars' `tolerance`
- **Cons / risks:**
  - Polars can't check sortedness with `by`. We sort on the key ourselves and pass `check_sortedness=False`
- **Result:** Attempt #1 (28.4397 s), as part of the first full pipeline. Fixtures A1–A12 and L4 pass

## Sessions: one sort, gap flags, running sum
- **Context:** Step 5 splits each person's events into sessions at gaps over 30 minutes; events with equal timestamps must share a session
- **Options:** A) Sort all events by (person, ts), flag a new session where the person changes or the gap to the previous row is over 30 min, then take a running sum of the flags as the session id. B) Window functions per person (`over("person")`)
- **Chose:** A
- **Pros:**
  - One sort for everyone, with no per-group windows
  - Equal timestamps have a gap of 0, so they share a session with no special case
- **Cons / risks:**
  - The sort is on a string key, which may be slow at 60M rows; numbering persons as integers could help (step 8)
- **Result:** Attempt #1 (28.4397 s), as part of the first full pipeline. `user_daily` matches all 36 fixtures and the dev answer exactly (118,107 rows)

## Linked IDs: scipy connected components
- **Context:** Step 4 must merge each person's IDs, through chains hundreds of links long and shared devices joining thousands of people
- **Options:** A) Number the IDs in byte order, then use scipy's `connected_components`. B) Repeated min-label passes in Polars. C) Union-find in plain Python
- **Chose:** A
- **Pros:**
  - One pass in compiled code, however long the chains
  - Numbering in byte order makes each person's name the first ID of its group, with no string comparisons
- **Cons / risks:**
  - Adds numpy and scipy to the image (about 0.15 s to import when warm)
  - B would need a pass per link of chain length, unless sped up with pointer jumping
  - C is a few seconds slower at full size (it is the oracle's method)
- **Result:** Attempt #1 (28.4397 s), as part of the first full pipeline. `tests/check_steps.py` matches the oracle's union-find on 37/37, and the L2 and L3 names match `fixtures/README.md`

## Test cleaning against a plain-Python oracle
- **Context:** The fixtures check only the final tables, so they can't test step 3 until steps 4–6 exist
- **Options:** A) A second, literal plain-Python version of the rules (`tests/oracle.py`), compared row by row (`tests/check_steps.py`). B) Wait for the fixtures at step 7
- **Chose:** A
- **Pros:**
  - Finds cleaning bugs now, on all fixtures and the 1M-event dev set
  - The oracle can grow with steps 4–6
- **Cons / risks:**
  - Both versions are ours and could share a misreading, so the counts were also checked against `fixtures/README.md`
- **Result:** Attempt #1 (28.4397 s), as part of the first full pipeline. The check passes 37/37 (36 fixtures + dev set)

## Our own timestamp parser
- **Context:** The rules reject inputs that common parsers accept: lowercase `z`, offsets without a colon, second 60, Unicode digits, F3 values beyond 64 bits
- **Options:** A) A regex for the shape, then range checks and date arithmetic in Polars expressions. B) Polars `str.to_datetime` with formats
- **Chose:** A, because every accepted form is spelled out in our code
- **Pros:**
  - Exact control, and still vectorized
- **Cons / risks:**
  - More code. Every branch is computed for every row, which may cost time (profile at step 8)
- **Result:** Attempt #1 (28.4397 s), as part of the first full pipeline

## Engine: the Polars starter
- **Context:** Only one starter can sit at the repo root, because the scorer builds one Dockerfile. Score is run time; the PySpark baseline is 169.6 s against a 180 s limit
- **Options:** A) Polars. B) DuckDB. C) PySpark
- **Chose:** A, because it was about 3× faster than DuckDB on the main CSV step. The test read 6M synthetic raw lines, checked quoting, split fields and deduped on `event_id`. Polars took 2.4–2.9 s and DuckDB 7.1–7.4 s, with the same row counts (8-core Mac)
- **Pros:**
  - Fastest at parsing, which touches every one of the ~60M rows
  - Fast sorts and `join_asof` suit sessions and click credit
  - The starter's test scripts already use Polars
- **Cons / risks:**
  - Neither engine's reader follows the CSV rules on `\r` (fixture E20). Polars strips a trailing `\r`; DuckDB splits lines on it. Fix for step 3: read whole lines, and split any file that contains a `\r` exactly in Python (the generator never writes one)
  - Polars does not spill to disk the way DuckDB does, so 60M rows must fit in 24 GB with no swap. Convert to compact types early
  - PySpark ruled out: JVM start-up, and its baseline is already near the limit
- **Result:** Attempt #1: 28.4397 s on ~20M practice events, against the 167.6152 s baseline

## Correct first, fast second
- **Context:** A nearly correct answer scores 0, whatever its speed. Baseline is 169.6 s; the hard limit is 3 min
- **Options:** A) Pass all 36 fixtures, then optimize. B) Design for speed from the start
- **Chose:** A
- **Pros:**
  - Every submission can score
  - The fixtures catch rule mistakes early
- **Cons / risks:**
  - The first version may need rework for speed
- **Result:** Attempt #1 scored (28.4397 s), so the first version was correct on the practice data

## Build step by step from an empty repo
- **Context:** The first scaffold guessed the schemas and rules before we had rules.md or the starter
- **Options:** A) Start empty and add one stage at a time. B) Fix up the guessed scaffold
- **Chose:** A
- **Pros:**
  - Every line is based on the real rules
  - Each step is small enough to understand and test
- **Cons / risks:**
  - Slower to a first submission; the deadline is Sunday 11:00 AM EST
- **Result:** Attempt #1 (28.4397 s) was the first submission, built one step at a time
