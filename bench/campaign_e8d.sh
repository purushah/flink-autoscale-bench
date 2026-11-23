#!/usr/bin/env bash
# E8d: the sustained overload of E8b at half the rate (200 to 1,200 records/s for 8 minutes), where the
# size the rule's formula gives for the true load (5) is below the maximum of 8. Stock band and band at
# the target, each with the default backlog signal and with the backlog taken from the broker.
#
#   bench/campaign_e8d.sh
#
# It is a file of its own so that it could be added while bench/campaign.sh was in use.
set -uo pipefail
cd "$(dirname "$0")/.."
echo "### campaign e8d starts: $(date '+%F %T')"
for pass in 1 2; do
  python3 bench/bench.py all --source-parallelism 2 --jitter 60 \
    --reps "${REPS_E8D:-8}" --jobs "builtin,builtin+lagfix,builtin+max70,builtin+max70+lagfix" \
    --scenarios surge_mid --out results-e8d/default || exit 1
  # The smallest overload (200 to 600 records/s), four repetitions, reported without tests.
  python3 bench/bench.py all --source-parallelism 2 --jitter 60 \
    --reps "${REPS_E8D_LOW:-4}" --jobs "builtin,builtin+lagfix,builtin+max70,builtin+max70+lagfix" \
    --scenarios surge_low --out results-e8d/default || exit 1
done
echo "### campaign e8d finished: $(date '+%F %T')"
