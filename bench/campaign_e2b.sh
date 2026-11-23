#!/usr/bin/env bash
# E2b: the noise floor in the sustained overload. Four identical policy-in-code jobs side by side in
# scenario surge (200 to 2,400 records/s for 8 minutes), default backlog signal. Gives the margin
# that the four-controller overload campaign (E10) is read against.
#
#   bench/campaign_e2b.sh
set -uo pipefail
cd "$(dirname "$0")/.."
echo "### campaign e2b starts: $(date '+%F %T')"
for pass in 1 2; do
  python3 bench/bench.py all --source-parallelism 2 --jitter 60 --reps "${REPS_E2B:-5}" \
    --jobs "rule+a,rule+b,rule+c,rule+d" --scenarios surge --out results-e2b/aa-rule || exit 1
done
echo "### campaign e2b finished: $(date '+%F %T')"
