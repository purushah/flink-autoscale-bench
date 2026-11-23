#!/usr/bin/env bash
# Eleventh chain (2026-11-21). Chain 10 ended at 05:14 PST on 2026-11-21 without running its last
# eight campaigns: the host had 8.0 GiB of disk free and bench.py refuses to start a run below 8 GiB
# (ISSUES.md #108). The VM's disk file had grown with blocks the VM no longer used. This chain runs
# what chain 10 left, in the same order, and trims the VM's disk before every campaign.
#   e5 (7 of 10 workloads are left; finished runs are skipped)
#   main to 8 repetitions            the last 20 runs of the main campaign
#   lit, repetition 2
#   e9, e4, repeats of e2 and e8 (runs in which a JobManager was killed), e5b
# The main campaign is analysed only when it has ended, at 80 runs or, if time runs out, at the last
# complete repetition of both arms (paper/outline.md).
# BENCH_HOURS (for example 18-07) limits the hours in which a run may start; unset means no limit.
cd "$(dirname "$0")/../.." || exit 1
if pgrep -f "bench/bench\.py" > /dev/null || pgrep -f "bench/campaign" > /dev/null; then
  echo "a bench process is running, not starting: $(date '+%F %T')"; exit 1
fi
if ! kubectl --context kind-jev-bench get --raw /readyz --request-timeout=10s > /dev/null 2>&1; then
  echo "the cluster does not answer, not starting: $(date '+%F %T')"; exit 1
fi
bad=0
trim() {  # give the VM's freed disk blocks back to the host (ISSUES.md #108); nothing is deleted
  perl -e 'alarm shift; exec @ARGV' 300 docker exec jev-bench-control-plane fstrim /var > /dev/null 2>&1
  echo "free disk after trim: $(df -g /System/Volumes/Data | awk 'NR==2 {print $4}') GiB: $(date '+%F %T')"
}
note() {  # $1 log tag: count its failed runs; two bad campaigns in a row stop the chain
  failed=$(grep -c "^!!! " "campaign-$1.log")
  echo "$1 over, failed runs $failed: $(date '+%F %T')"
  if grep -qs "does not answer; stopping the campaign" "campaign-$1.log"; then
    echo "stopping the chain: the cluster is gone"; exit 1
  fi
  if [ "$failed" -gt 2 ]; then bad=$((bad + 1)); else bad=0; fi
  if [ "$bad" -ge 2 ]; then echo "stopping the chain: two campaigns in a row with failed runs"; exit 1; fi
}
run() {  # $1 campaign, $2 log tag (default: the campaign's name); settings come from the environment
  local tag="${2:-$1}"
  trim
  bench/campaign.sh "$1" > "campaign-$tag.log" 2>&1; note "$tag"
}
own() {  # $1 name, $2 launcher, $3 pattern of its bench processes; give it up after two failed runs
  trim
  "$2" > "campaign-$1.log" 2>&1 &
  pid=$!
  while kill -0 "$pid" 2>/dev/null; do
    if [ "$(grep -c "^!!! " "campaign-$1.log")" -ge 2 ]; then
      echo "$1: two failed runs, giving it up: $(date '+%F %T')"
      pkill -TERM -P "$pid"; kill -TERM "$pid"; sleep 5; pkill -KILL -f "$3" 2>/dev/null
      break
    fi
    sleep 30
  done
  wait "$pid" 2>/dev/null
  note "$1"
}
echo "chain 11 starts: $(date '+%F %T') (BENCH_HOURS=${BENCH_HOURS:-unset})"
run e5 e5-part2
REPS=8 run main main-part3
REPS_LIT=2 run lit lit-rep2
run e9
run e4
# One job alone (e2): chain 10 already moved the 6 runs with a killed JobManager to results-e2-jm1g/.
run e2
own e8r bench/campaign_e8r.sh "results-e8-repeat/"
run e5b
echo "chain finished: $(date '+%F %T')"
