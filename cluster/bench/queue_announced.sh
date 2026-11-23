#!/bin/sh
# The announced-load experiment, run inside the driver pod. It needs the operator image that has the
# plugin of cluster/plugin (notes and schedule).
#
#   sh queue_announced.sh [scale] [reps]
#
# Spike runs in which the time of the spike is fixed before the jobs start (--planned), so that a
# job can be told about it. Five jobs side by side on the same load:
#   builtin+max70   the built-in autoscaler
#   rule            the policy in code
#   rule+a          the policy in code with the spike as a schedule entry: start, end, rate
#   llm             the generative model
#   llm+a           the generative model with the spike announced in a free-text note
# A run whose meta.json is there is skipped. Two failed runs in a row stop the queue.
set -u
SCALE="${1:-2}"
REPS="${2:-6}"
R="${CLUSTER_RESULTS:-/results}/announced"
Q="$R/queue.log"
JOBS="builtin+max70,rule,rule+a,llm,llm+a"
mkdir -p "$R"
cd "$(dirname "$0")"

say() { echo "$(date -u +%FT%TZ) $*" >> "$Q"; }

say "queue start: jobs $JOBS, scale $SCALE, reps $REPS"
fails=0
for rep in $(seq 1 "$REPS"); do
  d="$R/spike/rep$rep"
  if [ -f "$d/meta.json" ]; then say "skip spike rep $rep (done)"; continue; fi
  if [ -d "$d" ]; then mv "$d" "$d.failed-$(date +%s)"; fi
  say "start spike rep $rep, scale $SCALE"
  python3 bench_cluster.py run --scenario spike --rep "$rep" --jobs "$JOBS" --scale "$SCALE" --out "$R" \
    --source-parallelism 2 --jitter 60 --planned --arm rule+a=schedule --arm llm+a=note >> "$R/runs.log" 2>&1
  if [ -f "$d/meta.json" ]; then
    say "done spike rep $rep"; fails=0
    if [ "$rep" = 1 ]; then
      # The first run shows whether the operator has the plugin with notes and schedule.
      n=$(grep 'bench-llm-a' "$d/decisions.jsonl" | grep -c '"status":"DECIDED"')
      p=$(grep 'bench-rule-a' "$d/decisions.jsonl" | grep -c 'planned_rate')
      say "check: llm+a has $n answered calls, rule+a saw the planned load in $p cycles"
      if [ "$n" -lt 3 ] || [ "$p" -lt 1 ]; then say "STOP the operator does not seem to have the new plugin"; exit 1; fi
    fi
    continue
  fi
  fails=$((fails + 1))
  say "FAILED spike rep $rep: $(grep -v '^    t=' "$R/runs.log" | tail -1 | cut -c1-300)"
  if [ "$fails" -ge 2 ]; then say "STOP two runs in a row failed"; exit 1; fi
done
say "QUEUE OVER"
