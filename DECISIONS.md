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

## Test cleaning against a plain-Python oracle
- **Context:** The fixtures check only the final tables, so they can't test step 3 until steps 4–6 exist
- **Options:** A) A second, literal plain-Python version of the rules (`tests/oracle.py`), compared row by row (`tests/check_clean.py`). B) Wait for the fixtures at step 7
- **Chose:** A
- **Pros:**
  - Finds cleaning bugs now, on all fixtures and the 1M-event dev set
  - The oracle can grow with steps 4–6
- **Cons / risks:**
  - Both versions are ours and could share a misreading, so the counts were also checked against `fixtures/README.md`
- **Result:** — (no submission yet). The check passes 37/37 (36 fixtures + dev set)

## Our own timestamp parser
- **Context:** The rules reject inputs that common parsers accept: lowercase `z`, offsets without a colon, second 60, Unicode digits, F3 values beyond 64 bits
- **Options:** A) A regex for the shape, then range checks and date arithmetic in Polars expressions. B) Polars `str.to_datetime` with formats
- **Chose:** A, because every accepted form is spelled out in our code
- **Pros:**
  - Exact control, and still vectorized
- **Cons / risks:**
  - More code. Every branch is computed for every row, which may cost time (profile at step 8)
- **Result:** —

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
- **Result:** —

## Correct first, fast second
- **Context:** A nearly correct answer scores 0, whatever its speed. Baseline is 169.6 s; the hard limit is 3 min
- **Options:** A) Pass all 36 fixtures, then optimize. B) Design for speed from the start
- **Chose:** A
- **Pros:**
  - Every submission can score
  - The fixtures catch rule mistakes early
- **Cons / risks:**
  - The first version may need rework for speed
- **Result:** —

## Build step by step from an empty repo
- **Context:** The first scaffold guessed the schemas and rules before we had rules.md or the starter
- **Options:** A) Start empty and add one stage at a time. B) Fix up the guessed scaffold
- **Chose:** A
- **Pros:**
  - Every line is based on the real rules
  - Each step is small enough to understand and test
- **Cons / risks:**
  - Slower to a first submission; the deadline is Sunday 11:00 AM EST
- **Result:** —
