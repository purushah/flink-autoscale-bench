#!/usr/bin/env bash
# E8c: the sustained overload of E8b with windows four to five times longer (a 5-minute metric window,
# 2-minute stabilization, 5-minute catch-up), which is closer to the operator's defaults. Stock band
# and band at the target, each with the default backlog signal and with the backlog taken from the
# broker. One run takes about an hour. Finished runs are skipped; the second pass retries failed ones.
#
#   bench/campaign_e8c.sh
#
# It is a file of its own so that it could be added while bench/campaign.sh was in use.
set -uo pipefail
cd "$(dirname "$0")/.."
echo "### campaign e8c starts: $(date '+%F %T')"
for pass in 1 2; do
  python3 bench/bench.py all --source-parallelism 2 --jitter 60 --windows long \
    --reps "${REPS_E8C:-6}" --jobs "builtin,builtin+lagfix,builtin+max70,builtin+max70+lagfix" \
    --scenarios surge_long --out results-e8c/long || exit 1
done
echo "### campaign e8c finished: $(date '+%F %T')"
