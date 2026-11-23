#!/usr/bin/env bash
# E3b: the shipped band next to the policy in code and a model in the sustained overload. Jobs: the
# built-in rule with its stock band, the built-in rule with its upper bound at the target, the policy
# in code and the LLM; scenario surge, default backlog signal.
#
#   bench/campaign_e3b.sh
set -uo pipefail
cd "$(dirname "$0")/.."
echo "### campaign e3b starts: $(date '+%F %T')"
for pass in 1 2; do
  python3 bench/bench.py all --source-parallelism 2 --jitter 60 --reps "${REPS_E3B:-6}" \
    --jobs "builtin,builtin+max70,rule,llm" --scenarios surge --out results-e3b/default || exit 1
done
echo "### campaign e3b finished: $(date '+%F %T')"
