#!/usr/bin/env bash
# E8r: the four runs of E8 (short scenarios) in which the kernel killed a JobManager, run again on
# the repaired testbed (ISSUES.md #61). The originals stay where they are in results-e8/, which is
# the reported result; the repeats go to results-e8-repeat/ and are reported next to it.
#
#   bench/campaign_e8r.sh
set -uo pipefail
cd "$(dirname "$0")/.."
echo "### campaign e8r starts: $(date '+%F %T')"
jobs="builtin+max70,builtin+max70+lagfix,builtin+max70+f4096,builtin"
for pass in 1 2; do
  for pair in capacity:4 capacity:5 spike:5 spike:7; do
    sc="${pair%%:*}"; rep="${pair##*:}"
    if [ -f "results-e8-repeat/default/$sc/rep$rep/meta.json" ]; then
      echo "=== skip $sc rep $rep (already done)"
      continue
    fi
    python3 bench/bench.py run --scenario "$sc" --rep "$rep" --jobs "$jobs" --source-parallelism 2 --jitter 60 \
      --out results-e8-repeat/default || echo "!!! $sc rep $rep failed"
  done
done
echo "### campaign e8r finished: $(date '+%F %T')"
