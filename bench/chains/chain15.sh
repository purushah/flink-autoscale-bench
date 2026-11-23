#!/usr/bin/env bash
# Fifteenth chain (2026-11-22, late evening). Chain 14 was stopped by hand at 20:58 PST because another
# project's workload had taken the laptop (ISSUES.md #139, plan entry of 20:59 PST). This chain resumes
# the last night once the laptop is free: E4's runs that were measured next to that workload move to
# results-e4-first/ and its failed and unfinished runs to results-e4-incomplete/, then E4 makes what is
# missing, then the campaigns that had not started, in the order of chain 14: e2c (identical jobs in
# skew, ramp and drop), model faults, single jobs, the four repeats of E8, the further held-out arms.
# Finished runs are skipped by the campaigns.
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
echo "chain 15 starts: $(date '+%F %T') (BENCH_HOURS=${BENCH_HOURS:-unset})"
aside() {  # $1 run directory under results-e4/, $2 target root; nothing is deleted or overwritten
  local src="results-e4/$1" dst="$2/$1"
  if [ -d "$src" ] && [ ! -e "$dst" ]; then
    mkdir -p "$(dirname "$dst")" && mv "$src" "$dst" && echo "moved $src -> $dst"
  fi
}
for r in spike_long/rep3 spike_long/rep4 spike_long/rep5 ramp_long/rep3; do aside "default/$r" results-e4-first; done
for r in ramp_long/rep4 ramp_long/rep5; do
  [ -f "results-e4/default/$r/meta.json" ] || aside "default/$r" results-e4-incomplete
done
run e4 e4-part2
run e2c
run e9
# One job alone (e2): chain 10 already moved the 6 runs with a killed JobManager to results-e2-jm1g/.
run e2
own e8r bench/campaign_e8r.sh "results-e8-repeat/"
run e5b
echo "chain finished: $(date '+%F %T')"
