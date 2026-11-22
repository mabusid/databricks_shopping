#!/bin/sh
# Time the Rust pipeline on Linux (Docker, arm64 native), the proxy for the scorer:
# 4 threads on 3M events here x 6.67 ~= the scorer's time on ~20M events.
#
# Usage: tests/bench_linux.sh DATA_DIR [RUNS] [SRC_DIR]
#   DATA_DIR  a generated dataset, e.g. one made with
#             generator/target/release/generate --seed 1 --events 3000000 --clicks 750000 --output /tmp/s1-3m
#   RUNS      runs to take the fastest of (default 7)
#   SRC_DIR   the crate to build (default native/)
# Prints the fastest time of each stage, then wall/user/kernel time and page faults.
set -eu
cd "$(dirname "$0")/.."
DATA=$(cd "$1" && pwd)
RUNS=${2:-7}
SRC=$(cd "${3:-native}" && pwd)
OUT=$(mktemp -d)
trap 'rm -rf "$OUT"' EXIT

# Build in a Rust container; the build directory and crate cache are Docker volumes, so rebuilds are quick.
docker run --rm --platform linux/arm64 -v "$SRC":/src:ro -v "$OUT":/out \
  -v shopping-bench-target:/t -v shopping-bench-cargo:/usr/local/cargo/registry \
  -e CARGO_TARGET_DIR=/t -w /src rust:1.96-bookworm \
  sh -c 'cargo build --release --quiet && cp /t/release/shopping /out/shopping'

# Run with the data copied into the container (Docker's shared folders are slow), like the scorer.
docker run --rm --platform linux/arm64 --cpus 4 --memory 3g -v "$DATA":/src:ro -v "$OUT":/x \
  -e NCPU=4 debian:bookworm-slim sh -c "
  apt-get update -qq >/dev/null 2>&1; apt-get install -y -qq time >/dev/null 2>&1
  cp -r /src /tmp/d; export DATA_DIR=/tmp/d OUTPUT_DIR=/tmp/o
  for i in \$(seq 1 $RUNS); do rm -rf /tmp/o; mkdir /tmp/o
    /usr/bin/time -f 'TIME %e %U %S %R' /x/shopping 2>&1; echo RUN; done" | awk '
  /^TIME/ { if (r > 0 && (!w || $2 < w)) { w = $2; u = $3; s = $4; f = $5 } next }
  /RUN/ { r++; prev = 0; next }
  { t = $1 + 0; sub(/^ *[0-9.]+s  /, ""); n = $0; sub(/ [0-9]+ user_daily rows/, "", n); d = t - prev; prev = t
    if (r > 0) { if (!(n in b) || d < b[n]) b[n] = d; if (!(n in seen)) { seen[n] = 1; o[++k] = n } } }
  END { for (i = 1; i <= k; i++) printf "  %-30s %.3fs\n", o[i], b[o[i]]
        printf "  fastest run: wall %ss  user %ss  kernel %ss  page faults %s\n", w, u, s, f }'
