#!/usr/bin/env bash
# E10: the four controllers of the main campaign in the sustained overload (scenario surge), with the
# default backlog signal and with backlog and arrival rate taken from the broker (the fix of the
# paper, option +lagfix, for every controller). Arms alternate per repetition.
#
#   bench/campaign_e10.sh
#
# It replaces the e10 of bench/campaign.sh, whose second arm used a 4 KiB fetch; it is a file of its
# own so that it could be changed while bench/campaign.sh was in use.
set -uo pipefail
cd "$(dirname "$0")/.."
echo "### campaign e10 starts: $(date '+%F %T')"
# 12 repetitions per arm (plan amendment of 2026-11-18): with 8 pairs an interval cannot lie within
# plus or minus 10% at the noise of identical jobs.
reps="${REPS_E10:-12}"
for pass in 1 2; do
  for rep in $(seq 1 "$reps"); do
    first=default; second=broker
    if (( rep % 2 == 0 )); then first=broker; second=default; fi
    for arm in "$first" "$second"; do
      if [[ "$arm" == default ]]; then
        jobs="builtin+max70,rule,jev,llm"
      else
        jobs="builtin+max70+lagfix,rule+lagfix,jev+lagfix,llm+lagfix"
      fi
      python3 bench/bench.py all --source-parallelism 2 --jitter 60 --reps "$rep" --jobs "$jobs" \
        --scenarios surge --out "results-e10/$arm" || exit 1
    done
  done
done
echo "### campaign e10 finished: $(date '+%F %T')"
