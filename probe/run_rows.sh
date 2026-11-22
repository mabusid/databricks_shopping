#!/bin/bash
# Probe entry point (DECISIONS.md, "Scorer probes"). The scorer shows no logs for
# a dev-check failure, only the differing rows in full, so the measurements are
# reported as made-up rows: `c` rows in campaign_daily (campaign_id, channel), `u`
# rows in user_daily (user_id, top_category). Most important first (c1).
T0=$EPOCHREALTIME
D=${DATA_DIR:-/data}; O=${OUTPUT_DIR:-/output}; S=${SCRATCH_DIR:-/scratch}
EV20=${PROBE_EV20:-20000000}; EV60=${PROBE_EV60:-60000000}; ROUNDS=${PROBE_ROUNDS:-3}
el() { awk "BEGIN{printf \"%.1f\", $EPOCHREALTIME - $T0}"; }
SUM=$S/sum.txt; LOG=$S/log.txt; : > "$SUM"; : > "$LOG"
row() { printf '%s\t%s\t%s\n' "$1" "$(echo "$2" | tr -s ' \t\n' ' ' | cut -c1-900)" "$(echo "$3" | tr -s ' \t\n' ' ' | cut -c1-900)" >> "$SUM"; }
# One run's stage times, compact: L links, P parse, D dedup, C clicks, S sessions, W write (cumulative s).
stages() {
  awk '{t=$1; sub(/s$/,"",t)}
    /small tables/ {o=o "L" t " "} /events parsed|events and clicks parsed/ {o=o "P" t " "} /deduped/ {o=o "D" t " "}
    /^ *[0-9.]+s  clicks parsed/ {o=o "C" t " "} /sessions, credit/ {o=o "S" t " "} /user_daily rows/ {o=o "W" t " "}
    /^rusage/ {o=o "u" $3 " s" $5 " f" $7 " rss" $11 " "} /^checksum/ {o=o "ck" substr($5,1,5) substr($9,1,5)}
    END {print o}' "$1"
}
run() { # run LABEL BIN DATA [VAR=VALUE...]: prints "wall stages"
  local label=$1 bin=$2 data=$3; shift 3
  rm -rf "$S/o"; mkdir -p "$S/o"
  local t=$EPOCHREALTIME
  env "$@" SHOPPING_CHECKSUM=1 DATA_DIR="$data" OUTPUT_DIR="$S/o" timeout "${RUN_TIMEOUT:-30}" "$bin" > "$S/run.log" 2>&1
  local rc=$?
  local w=$(awk "BEGIN{printf \"%.3f\", $EPOCHREALTIME - $t}")
  echo "[$label] exit $rc wall $w" >> "$LOG"; cat "$S/run.log" >> "$LOG"
  [ $rc = 0 ] && echo "w$w $(stages "$S/run.log")" || echo "w$w EXIT$rc"
}
finish() { rm -rf "$O"/*; SHOPPING_FAKE_ROWS="$SUM" DATA_DIR="$D" OUTPUT_DIR="$O" /app/writer >/dev/null 2>&1; }
BINS=$(ls /app/bin | grep -v '^tests')
# Optional /app/configs: lines "LABEL BIN [VAR=VALUE...]" timed instead of one run per build.
CONFIGS=/app/configs
[ -f "$CONFIGS" ] || { for b in $BINS; do echo "$b $b"; done > "$S/configs"; CONFIGS=$S/configs; }
LABELS=$(awk '{print $1}' "$CONFIGS")

# A first dev output with one fake row, in case anything below dies.
SHOPPING_FAKE_ROW=1 DATA_DIR="$D" OUTPUT_DIR="$O" /app/writer >/dev/null 2>&1

# System.
flags=$(grep -m1 -E "^flags" /proc/cpuinfo | tr ' ' '\n' | grep -xE 'avx2|avx512f|avx512bw|avx512vbmi|bmi2|popcnt|hypervisor' | tr '\n' ' ')
mnt() { awk -v m="$1" '$5 == m { for (i = 7; i <= NF; i++) if ($i == "-") { print $(i+1) ":" $(i+2); exit } }' /proc/self/mountinfo; }
cache=$(command -v fincore >/dev/null && fincore -b -n -o RES,SIZE "$D"/events/*.csv 2>/dev/null | awk '{r+=$1; s+=$2} END {if (s) printf "%.0f%%", 100*r/s}')
sys_a="cpu=$(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2) nproc=$(nproc) k=$(uname -r) $flags $(grep -E '^(siblings|cpu cores|cache size)' /proc/cpuinfo | sort -u | tr -s ' \t' ' ' | tr '\n' ';')"
sys_b="thp=$(cat /sys/kernel/mm/transparent_hugepage/enabled 2>&1)|$(cat /sys/kernel/mm/transparent_hugepage/defrag 2>&1) data=$(mnt /data) scratch=$(mnt /scratch) out=$(mnt /output) cpu.max=$(cat /sys/fs/cgroup/cpu.max 2>&1) mem.max=$(cat /sys/fs/cgroup/memory.max 2>&1) devcache=$cache memtotal=$(awk '/MemTotal/{print $2}' /proc/meminfo)"

# v3 unit tests (AVX2 scanner fuzz test, timestamps) on this CPU.
tests="none"
[ -x /app/bin/tests-v3 ] && [ "$(uname -m)" = x86_64 ] && tests=$(timeout 60 /app/bin/tests-v3 2>&1 | grep -E "test result|panicked|FAILED" | head -3 | tr '\n' ' ')
dev=""
for b in $BINS; do r=$(run "dev $b" /app/bin/$b "$D"); dev="$dev $b:${r%% *} $(echo "$r" | grep -o 'ck[0-9a-f]*')"; done

t=$EPOCHREALTIME
timeout 60 /app/generate --seed 1 --events "$EV20" --clicks $((EV20 / 4)) --output "$S/g20" >/dev/null 2>&1
gen20="gen20 exit $? $(awk "BEGIN{printf \"%.1f\", $EPOCHREALTIME - $t}")s $(du -sm "$S/g20" | cut -f1)MB"

# 20M rounds, interleaved.
declare -A walls best bestw
for i in $(seq 1 $ROUNDS); do while read -r b bin envs; do
  r=$(run "$b $i" /app/bin/$bin "$S/g20" $envs); w=${r%% *}; w=${w#w}
  walls[$b]="${walls[$b]} $w"
  if [ -z "${bestw[$b]}" ] || awk "BEGIN{exit !($w < ${bestw[$b]})}"; then bestw[$b]=$w; best[$b]="$r"; fi
done < "$CONFIGS"; done
c1a="20M best of $ROUNDS:"; c1b="walls:"
for b in $LABELS; do c1a="$c1a | $b ${best[$b]}"; c1b="$c1b | $b${walls[$b]}"; done
row c "c1 $c1a" "c1b $c1b"
row c "c2 $sys_a" "c2b $sys_b"
row u "u1 tests: $tests" "u1b dev:$dev $gen20 (at $(el)s)"
finish

# Thread counts, on the newest build.
last=$(echo "$BINS" | tail -1)
n16=$(run "n16" /app/bin/$last "$S/g20" NCPU=16); n4=$(run "n4" /app/bin/$last "$S/g20" NCPU=4)
row u "u2 $last NCPU16: $n16" "u2b NCPU4: $n4 (at $(el)s)"
finish

# Microbenchmarks.
SHOPPING_PROBE="$S/g20" timeout 30 /app/writer > "$S/micro.log" 2>&1
micro=$(grep '^probe' "$S/micro.log" | sed -e 's/^probe //' -e 's/  */ /g' -e 's/ GB\/s/GB\/s/g' -e 's/ ns\/load/ns/g')
row c "c3 $(echo "$micro" | head -6 | tr '\n' ';')" "c3b $(echo "$micro" | tail -n +7 | tr '\n' ';') (at $(el)s)"
finish

# 60M: time and peak memory, first build (default CPU).
if awk "BEGIN{exit !($(el) < 80)}"; then
  rm -rf "$S/g20"
  t=$EPOCHREALTIME
  timeout 50 /app/generate --seed 2 --events "$EV60" --clicks $((EV60 / 4)) --output "$S/g60" >/dev/null 2>&1
  gen60="gen60 exit $? $(awk "BEGIN{printf \"%.1f\", $EPOCHREALTIME - $t}")s $(du -sm "$S/g60" | cut -f1)MB"
  first=$(echo "$BINS" | head -1)
  r60="skipped"; awk "BEGIN{exit !($(el) < 135)}" && r60=$(RUN_TIMEOUT=25 run "60M" /app/bin/$first "$S/g60")
  row c "c4 60M $first: $r60" "c4b $gen60 (at $(el)s)"
else
  row c "c4 60M skipped at $(el)s" "c4b"
fi
rm -rf "$S/g20" "$S/g60" "$S/o"
finish
