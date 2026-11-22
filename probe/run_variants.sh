#!/bin/bash
# Probe #2+: time every build in /app/bin on generated 20M data, interleaved,
# with output checksums; then fail the dev check on purpose (one fake row).
exec 1>&2
T0=$EPOCHREALTIME
D=${DATA_DIR:-/data}; O=${OUTPUT_DIR:-/output}; S=${SCRATCH_DIR:-/scratch}
EV20=${PROBE_EV20:-20000000}; EV60=${PROBE_EV60:-60000000}; ROUNDS=${PROBE_ROUNDS:-3}
el() { awk "BEGIN{printf \"%.1f\", $EPOCHREALTIME - $T0}"; }
sec() { echo; echo "=== $* (at $(el)s)"; }
run() { # run LABEL BIN DATA [VAR=VALUE...]
  local label=$1 bin=$2 data=$3; shift 3
  rm -rf "$S/o"; mkdir -p "$S/o"
  local t=$EPOCHREALTIME
  env "$@" SHOPPING_CHECKSUM=1 DATA_DIR="$data" OUTPUT_DIR="$S/o" timeout 40 "$bin" 2>&1 | sed "s/^/[$label] /"
  echo "[$label] wall $(awk "BEGIN{printf \"%.3f\", $EPOCHREALTIME - $t}")s exit ${PIPESTATUS[0]}"
}
BINS=$(ls /app/bin | grep -v '^tests')
REF=$(ls /app/bin | grep -- '-base$' | head -1); [ -z "$REF" ] && REF=$(echo "$BINS" | head -1)

sec "dev output with one fake row (makes the dev check fail on purpose)"
SHOPPING_FAKE_ROW=1 DATA_DIR="$D" OUTPUT_DIR="$O" /app/bin/$REF 2>&1 | tail -2
grep -m1 "model name" /proc/cpuinfo; echo "builds: $(echo $BINS)"

sec "v3 unit tests on this CPU"
[ -x /app/bin/tests-v3 ] && [ "$(uname -m)" = x86_64 ] && timeout 60 /app/bin/tests-v3 2>&1 | tail -4

sec "dev set: every build once (checksums must match)"
for b in $BINS; do run "dev $b" /app/bin/$b "$D" | grep -E "checksum|wall"; done

sec "generate $EV20 events"
t=$EPOCHREALTIME
timeout 60 /app/generate --seed 1 --events "$EV20" --clicks $((EV20 / 4)) --output "$S/g20" >/dev/null
echo "generate exit $? in $(awk "BEGIN{printf \"%.2f\", $EPOCHREALTIME - $t}")s"

sec "20M: $ROUNDS interleaved rounds"
for i in $(seq 1 $ROUNDS); do for b in $BINS; do run "$b $i" /app/bin/$b "$S/g20"; done; done

if awk "BEGIN{exit !($(el) < 95)}"; then
  sec "generate $EV60 events (memory check, first build)"
  rm -rf "$S/g20"
  t=$EPOCHREALTIME
  timeout 60 /app/generate --seed 2 --events "$EV60" --clicks $((EV60 / 4)) --output "$S/g60" >/dev/null
  echo "generate exit $? in $(awk "BEGIN{printf \"%.2f\", $EPOCHREALTIME - $t}")s"
  first=$(echo "$BINS" | head -1)
  if awk "BEGIN{exit !($(el) < 145)}"; then run "60M $first" /app/bin/$first "$S/g60"; fi
else
  echo "skipping 60M (at $(el)s)"
fi
rm -rf "$S/g20" "$S/g60" "$S/o"
sec "done"
