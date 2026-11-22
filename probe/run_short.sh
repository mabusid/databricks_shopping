#!/bin/bash
# Probe entry point, short report (DECISIONS.md, "Scorer probes"): the scorer shows
# only 2 differing rows of the first failing table, cut at ~2,400 characters. So:
#   u1: 20M stage times per config (best of ROUNDS) | walls per config
#   u2: 60M time and peak memory (PROBE_60)          | dev-set checksums per config
T0=$EPOCHREALTIME
D=${DATA_DIR:-/data}; O=${OUTPUT_DIR:-/output}; S=${SCRATCH_DIR:-/scratch}
[ -f /app/probe60 ] && PROBE_60=${PROBE_60:-$(cat /app/probe60)}
EV20=${PROBE_EV20:-20000000}; EV60=${PROBE_EV60:-60000000}; ROUNDS=${PROBE_ROUNDS:-2}
el() { awk "BEGIN{printf \"%.0f\", $EPOCHREALTIME - $T0}"; }
SUM=$S/sum.txt; : > "$SUM"
row() { printf '%s\t%s\t%s\n' "$1" "$(echo "$2" | tr -s ' \t\n' ' ' | cut -c1-1000)" "$(echo "$3" | tr -s ' \t\n' ' ' | cut -c1-700)" >> "$SUM"; }
# P parse end minus L links end, D dedup, S person pass + user_daily, all as durations (s, 2 decimals).
stages() {
  awk '{t=$1; sub(/s$/,"",t)}
    /small tables/ {l=t} /events parsed|events and clicks parsed/ {p=t} /deduped/ {d=t} /sessions, credit/ {s=t} /user_daily rows/ {w=t}
    /^rusage/ {rss=$11} /^checksum/ {ck=substr($5,1,4)}
    END {printf "L%.2f P%.2f D%.2f S%.2f r%d %s", l, p-l, d-p, s-d, rss/1000, ck}' "$1"
}
run() { # run BIN DATA [VAR=VALUE...]: prints "wall stages" or "wall EXITn"
  local bin=$1 data=$2; shift 2
  rm -rf "$S/o"; mkdir -p "$S/o"
  local t=$EPOCHREALTIME
  env "$@" SHOPPING_CHECKSUM=1 DATA_DIR="$data" OUTPUT_DIR="$S/o" timeout "${RUN_TIMEOUT:-30}" /app/bin/$bin > "$S/run.log" 2>&1 < /dev/null
  local rc=$? w=$(awk "BEGIN{printf \"%.2f\", $EPOCHREALTIME - $t}")
  [ $rc = 0 ] && echo "$w $(stages "$S/run.log")" || echo "$w EXIT$rc"
}
finish() { rm -rf "$O"/*; SHOPPING_FAKE_ROWS="$SUM" DATA_DIR="$D" OUTPUT_DIR="$O" /app/writer >/dev/null 2>&1; }
CONFIGS=/app/configs
SHOPPING_FAKE_ROW=1 DATA_DIR="$D" OUTPUT_DIR="$O" /app/writer >/dev/null 2>&1   # in case anything below dies

dev=""
while read -r b bin envs; do r=$(run $bin "$D" $envs); dev="$dev $b:${r##* }"; done < "$CONFIGS"
timeout 60 /app/generate --seed 1 --events "$EV20" --clicks $((EV20 / 4)) --output "$S/g20" >/dev/null 2>&1
declare -A walls best bestw
for i in $(seq 1 $ROUNDS); do while read -r b bin envs; do
  r=$(run $bin "$S/g20" $envs); w=${r%% *}
  walls[$b]="${walls[$b]} $w"
  if [ -z "${bestw[$b]}" ] || awk "BEGIN{exit !($w < ${bestw[$b]})}"; then bestw[$b]=$w; best[$b]="$r"; fi
done < "$CONFIGS"; done
a="u1 20M:"; c="u1b walls:"
for b in $(awk '{print $1}' "$CONFIGS"); do a="$a |$b ${best[$b]}"; c="$c |$b${walls[$b]}"; done
row u "$a" "$c (at $(el)s)"
finish

r60="skipped at $(el)s"
if awk "BEGIN{exit !($(el) < 100)}"; then
  rm -rf "$S/g20"
  t=$EPOCHREALTIME
  timeout 40 /app/generate --seed 2 --events "$EV60" --clicks $((EV60 / 4)) --output "$S/g60" >/dev/null 2>&1
  g="gen $(awk "BEGIN{printf \"%.0f\", $EPOCHREALTIME - $t}")s $(du -sm "$S/g60" | cut -f1)MB"
  set -- ${PROBE_60:-$(awk 'NR==1 {print $2}' "$CONFIGS")}
  b60=$1; shift
  awk "BEGIN{exit !($(el) < 140)}" && r60="$b60 $* $(RUN_TIMEOUT=25 run $b60 "$S/g60" "$@") $g"
fi
row u "u2 60M: $r60 (at $(el)s)" "u2b dev:$dev"
rm -rf "$S/g20" "$S/g60" "$S/o"
finish
