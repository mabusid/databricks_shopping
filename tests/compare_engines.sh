#!/bin/sh
# Check the Rust binary against everything we trust:
#   1. the 36 fixtures,
#   2. the official dev answer (data/ vs dev-answer/),
#   3. the Polars pipeline's output on freshly generated datasets (one per seed).
# Usage: tests/compare_engines.sh [SEED...]   (default seeds 3-12; 400k events each)
# Needs: native/target/release/shopping built, .venv, generator/target/release/generate.
set -eu
cd "$(dirname "$0")/.."
PY=.venv/bin/python
BIN=${BIN:-native/target/release/shopping}  # BIN=path tests another build
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
fail=0

$PY scripts/run_fixtures.py fixtures -- $BIN | tail -1 | grep -q '^36/36' && echo "fixtures   36/36" || { echo "fixtures   FAIL"; fail=1; }

DATA_DIR=data OUTPUT_DIR="$TMP/dev" $BIN 2>/dev/null
(cd scripts && ../$PY compare.py "$TMP/dev" ../dev-answer) | grep -q '^MATCH' && echo "dev answer MATCH" || { echo "dev answer DIFFER"; fail=1; }

for seed in ${@:-3 4 5 6 7 8 9 10 11 12}; do
  d="$TMP/seed$seed"
  generator/target/release/generate --seed "$seed" --events 400000 --clicks 100000 --output "$d" >/dev/null
  DATA_DIR="$d" OUTPUT_DIR="$TMP/rust$seed" $BIN 2>/dev/null
  DATA_DIR="$d" OUTPUT_DIR="$TMP/polars$seed" $PY pipeline/pipeline.py >/dev/null 2>&1
  if (cd scripts && ../$PY compare.py "$TMP/rust$seed" "$TMP/polars$seed") | grep -q '^MATCH'; then
    echo "seed $seed    MATCH (Rust = Polars)"
  else
    echo "seed $seed    DIFFER"; fail=1
  fi
  rm -rf "$d" "$TMP/rust$seed" "$TMP/polars$seed"
done
[ $fail = 0 ] && echo "ALL OK" || { echo "SOMETHING DIFFERS"; exit 1; }
