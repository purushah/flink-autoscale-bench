#!/usr/bin/env bash
# E8s: the shipped (stock) band with the default backlog signal, with the broker-side signal, and
# with two smaller fetch sizes of the consumer (64 KiB and 4 KiB), which need no code.
#   part A: the sustained overload to 2,400 records/s (surge), 6 repetitions
#   part B: the three short scenarios of E8 (spike, ramp, capacity), 8 repetitions
#
#   bench/campaign_e8s.sh
set -uo pipefail
cd "$(dirname "$0")/.."
echo "### campaign e8s starts: $(date '+%F %T')"
jobs="builtin,builtin+lagfix,builtin+f65536,builtin+f4096"
for pass in 1 2; do
  python3 bench/bench.py all --source-parallelism 2 --jitter 60 --reps "${REPS_E8S_SURGE:-6}" \
    --jobs "$jobs" --scenarios surge --out results-e8s/default || exit 1
  python3 bench/bench.py all --source-parallelism 2 --jitter 60 --reps "${REPS_E8S:-8}" \
    --jobs "$jobs" --scenarios spike,ramp,capacity --out results-e8s/default || exit 1
done
echo "### campaign e8s finished: $(date '+%F %T')"
