#!/bin/sh
# Flapping load with the two additions to the policy of ISSUES.md #107, run inside the driver pod. It
# needs the operator image that has them (cluster/plugin with scale-down.window-caps-trend and
# scale-down.min-slots).
#
#   sh queue_policyfix.sh [scale] [reps] [scenarios]        default: 2 5 onoff
#   POLICYFIX_DIR=policyfix2 sh queue_policyfix.sh     results in another folder (the runs with the
#                                                      corrected cap of ISSUES.md #119, image cluster10)
#
# Five jobs side by side:
#   builtin+max70   the built-in autoscaler (it delays a scale-down by 2 minutes)
#   rule            the policy in code
#   rule+b          the policy with the look-back: a scale-down sized for the highest demand of the last
#                   10 minutes (as in queue_flapping.sh)
#   rule+c          rule+b, and the trend projection stops at the highest demand the window saw before
#                   the current rise (plus 10%) while the demand is not above that level, and a plan that
#                   only scales down is dropped unless it frees 2 slots
#   llm+c           the generative model with the history in its prompt (llm+b of queue_flapping.sh),
#                   and the same rule about plans that free fewer than 2 slots
# A run whose meta.json is there is skipped. Two failed runs in a row stop the queue.
set -u
SCALE="${1:-2}"
REPS="${2:-5}"
SCENARIOS="${3:-onoff}"
R="${CLUSTER_RESULTS:-/results}/${POLICYFIX_DIR:-policyfix}"
Q="$R/queue.log"
JOBS="builtin+max70,rule,rule+b,rule+c,llm+c"
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
      --source-parallelism 2 --jitter 60 --arm rule+b=window:600 --arm rule+c=window:600,capped,minslots:2 \
      --arm llm+c=history,minslots:2 >> "$R/runs.log" 2>&1
    if [ -f "$d/meta.json" ]; then say "done $sc rep $rep"; fails=0; continue; fi
    fails=$((fails + 1))
    say "FAILED $sc rep $rep: $(grep -v '^    t=' "$R/runs.log" | tail -1 | cut -c1-300)"
    if [ "$fails" -ge 2 ]; then say "STOP two runs in a row failed"; exit 1; fi
  done
done
say "QUEUE OVER"
