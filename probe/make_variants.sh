#!/bin/sh
# probe/make_variants.sh NAME=COMMIT[:base][:v4] ...   e.g. a_main=5af9be4:base b_step2=f51c5ab
# Fills probe/variants/<NAME>/src with that commit's native/src (":base" also
# builds it for the default CPU). Names sort in build order; the first one's
# v3 unit tests run on the scorer. Then the probe branch's root Dockerfile is
# probe/Dockerfile.variants.
set -eu
cd "$(dirname "$0")/.."
rm -rf probe/variants; mkdir -p probe/variants
for spec in "$@"; do
  name=${spec%%=*}; rest=${spec#*=}; commit=${rest%%:*}
  mkdir -p probe/variants/$name
  git archive "$commit" native/src | tar -x -C probe/variants/$name --strip-components=1
  case "$rest" in *:base*) touch probe/variants/$name/base;; esac
  case "$rest" in *:v4*) touch probe/variants/$name/v4;; esac
  echo "$name: $(git log --oneline -1 $commit)"
done
