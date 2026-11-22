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
- **Result:** — (attempt #9 pending)

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
