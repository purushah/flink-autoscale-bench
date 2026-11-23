#!/bin/sh
# The flapping-load experiment, run inside the driver pod. It needs the operator image that has the
# plugin of cluster/plugin (history and scale-down window).
#
#   sh queue_flapping.sh [scale] [reps] [scenarios]        default: 2 3 onoff
#
# Load that drops and comes back, so that a scale-down is soon undone. Five jobs side by side:
#   builtin+max70   the built-in autoscaler (it delays a scale-down by 2 minutes)
#   rule            the policy in code
#   rule+b          the policy in code, a scale-down sized for the highest demand of the last 10 minutes
#   llm             the generative model
#   llm+b           the generative model with the demand of the last half hour and what earlier
#                   changes cost
# A run whose meta.json is there is skipped. Two failed runs in a row stop the queue.
set -u
SCALE="${1:-2}"
REPS="${2:-3}"
SCENARIOS="${3:-onoff}"
R="${CLUSTER_RESULTS:-/results}/flapping"
Q="$R/queue.log"
JOBS="builtin+max70,rule,rule+b,llm,llm+b"
mkdir -p "$R"
cd "$(dirname "$0")"

say() { echo "$(date -u +%FT%TZ) $*" >> "$Q"; }

say "queue start: jobs $JOBS, scale $SCALE, reps $REPS, scenarios $SCENARIOS"
fails=0
for rep in $(seq 1 "$REPS"); do
  for sc in $(echo "$SCENARIOS" | tr ',' ' '); do
    d="$R/$sc/rep$rep"
    if [ -f "$d/meta.json" ]; then say "skip $sc rep $rep (done)"; continue; fi
    if [ -d "$d" ]; then mv "$d" "$d.failed-$(date +%s)"; fi
    say "start $sc rep $rep, scale $SCALE"
    python3 bench_cluster.py run --scenario "$sc" --rep "$rep" --jobs "$JOBS" --scale "$SCALE" --out "$R" \
      --source-parallelism 2 --jitter 60 --arm rule+b=window:600 --arm llm+b=history >> "$R/runs.log" 2>&1
    if [ -f "$d/meta.json" ]; then say "done $sc rep $rep"; fails=0; continue; fi
    fails=$((fails + 1))
    say "FAILED $sc rep $rep: $(grep -v '^    t=' "$R/runs.log" | tail -1 | cut -c1-300)"
    if [ "$fails" -ge 2 ]; then say "STOP two runs in a row failed"; exit 1; fi
  done
done
say "QUEUE OVER"
