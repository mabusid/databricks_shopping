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
- **Result:** — (attempt #2 pending)

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
