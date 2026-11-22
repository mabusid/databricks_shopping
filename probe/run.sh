#!/bin/bash
# Probe submission entry point (DECISIONS.md, "Scorer probes"). It measures the
# scorer machine and our builds on generated data, then fails the dev check on
# purpose (one made-up campaign_daily row) so the scorer shows these logs.
# Everything goes to stderr. A watchdog keeps it far under the dev set's 3 minutes.
exec 1>&2
T0=$EPOCHREALTIME
D=${DATA_DIR:-/data}; O=${OUTPUT_DIR:-/output}; S=${SCRATCH_DIR:-/scratch}
EV20=${PROBE_EV20:-20000000}; EV60=${PROBE_EV60:-60000000}
el() { awk "BEGIN{printf \"%.1f\", $EPOCHREALTIME - $T0}"; }
sec() { echo; echo "=== $* (at $(el)s)"; }
run() { # run LABEL BIN DATA [VAR=VALUE...]
  local label=$1 bin=$2 data=$3; shift 3
  rm -rf "$S/o"; mkdir -p "$S/o"
  local t=$EPOCHREALTIME
  env "$@" SHOPPING_CHECKSUM=1 DATA_DIR="$data" OUTPUT_DIR="$S/o" timeout 40 "$bin" 2>&1 | sed "s/^/[$label] /"
  echo "[$label] wall $(awk "BEGIN{printf \"%.3f\", $EPOCHREALTIME - $t}")s exit ${PIPESTATUS[0]}"
}

sec "dev output with one fake row (makes the dev check fail on purpose)"
t=$EPOCHREALTIME
SHOPPING_FAKE_ROW=1 DATA_DIR="$D" OUTPUT_DIR="$O" /app/shopping-base
echo "dev run wall $(awk "BEGIN{printf \"%.3f\", $EPOCHREALTIME - $t}")s"

sec "system"
uname -a; cat /proc/version 2>&1
echo "nproc $(nproc)  NCPU=$NCPU MEM_GB=$MEM_GB  uid $(id -u)"
grep -m1 "model name" /proc/cpuinfo
grep -m1 -E "^flags" /proc/cpuinfo | tr ' ' '\n' | grep -xE 'avx2|avx512f|avx512bw|avx512vl|avx512vbmi|bmi1|bmi2|popcnt|lzcnt|sse4_2|erms|fsrm|hypervisor' | tr '\n' ' '; echo
grep -E "^(cpu MHz|cache size|siblings|cpu cores|physical id|core id)" /proc/cpuinfo | sort | uniq -c
lscpu 2>/dev/null | grep -iE "model name|socket|core|thread|cache|numa|mhz|hypervisor|virtualization"
for f in enabled defrag khugepaged/defrag shmem_enabled; do echo "thp $f: $(cat /sys/kernel/mm/transparent_hugepage/$f 2>&1)"; done
echo "fault_around_bytes: $(cat /sys/kernel/mm/transparent_hugepage/../../debug/fault_around_bytes 2>&1 | head -1)"
for f in cpu.max cpu.weight memory.max memory.high memory.oom.group cpuset.cpus.effective; do echo "cgroup $f: $(cat /sys/fs/cgroup/$f 2>&1)"; done
head -4 /proc/meminfo; grep -iE "hugepages|anonhuge" /proc/meminfo
grep -E " (/data|/scratch|/output|/tmp|/) " /proc/self/mountinfo
ulimit -a | grep -iE "open|proc|stack|locked"
du -sh "$D"/* 2>&1; ls "$D/events" | wc -l
command -v fincore >/dev/null && fincore "$D"/events/*.csv | tail -4 || echo "no fincore"

sec "v3 unit tests on this CPU (scanner fuzz test, timestamps)"
[ "$(uname -m)" = x86_64 ] && timeout 60 /app/shopping-v3-tests 2>&1 | tail -8

sec "dev set, both builds, twice"
for i in 1 2; do run "dev base $i" /app/shopping-base "$D"; run "dev v3 $i" /app/shopping-v3 "$D"; done

sec "generate $EV20 events"
t=$EPOCHREALTIME
timeout 60 /app/generate --seed 1 --events "$EV20" --clicks $((EV20 / 4)) --output "$S/g20" >/dev/null
echo "generate exit $? in $(awk "BEGIN{printf \"%.2f\", $EPOCHREALTIME - $t}")s"; du -sh "$S/g20"/*

sec "microbenchmarks"
SHOPPING_PROBE="$S/g20" timeout 30 /app/shopping-base

sec "20M runs, interleaved"
for i in 1 2 3; do run "base $i" /app/shopping-base "$S/g20"; run "v3 $i" /app/shopping-v3 "$S/g20"; done
run "base NCPU16" /app/shopping-base "$S/g20" NCPU=16
run "v3 NCPU16" /app/shopping-v3 "$S/g20" NCPU=16
run "base NCPU4" /app/shopping-base "$S/g20" NCPU=4

if awk "BEGIN{exit !($(el) < 85)}"; then
  sec "generate $EV60 events (memory check)"
  rm -rf "$S/g20"
  t=$EPOCHREALTIME
  timeout 70 /app/generate --seed 2 --events "$EV60" --clicks $((EV60 / 4)) --output "$S/g60" >/dev/null
  echo "generate exit $? in $(awk "BEGIN{printf \"%.2f\", $EPOCHREALTIME - $t}")s"; du -sh "$S/g60"/*
  if awk "BEGIN{exit !($(el) < 140)}"; then run "60M v3" /app/shopping-v3 "$S/g60"; fi
else
  echo "skipping 60M (at $(el)s)"
fi
rm -rf "$S/g20" "$S/g60" "$S/o"
sec "done"
