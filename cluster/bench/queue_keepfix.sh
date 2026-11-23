#!/bin/sh
# Test D (plan entry of 2026-11-23 00:31 PST, ISSUES.md #119): the on-off load with a margin on the
# size a scale-down keeps, run inside the driver pod. It needs the operator image that has the option
# (cluster/plugin with scale-down.window-keeps-margin, image cluster11).
#
#   sh queue_keepfix.sh [scale] [reps] [scenarios]        default: 2 5 onoff
#
# Five jobs side by side:
#   builtin+max70   the built-in autoscaler (it delays a scale-down by 2 minutes)
#   rule            the policy in code
#   rule+c          the policy with the look-back of 10 minutes, the cap on the trend projection, and
#                   the rule that a plan that only scales down must free 2 slots (as in Test B)
#   rule+d          rule+c, and a scale-down keeps what the window's highest demand plus the cap's
#                   margin (10%) needs
#   llm+c           the generative model with the history in its prompt and the two-slot rule (as in
#                   Test B)
# A run whose meta.json is there is skipped. Two failed runs in a row stop the queue.
set -u
SCALE="${1:-2}"
REPS="${2:-5}"
SCENARIOS="${3:-onoff}"
R="${CLUSTER_RESULTS:-/results}/keepfix"
Q="$R/queue.log"
JOBS="builtin+max70,rule,rule+c,rule+d,llm+c"
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
      --source-parallelism 2 --jitter 60 --arm rule+c=window:600,capped,minslots:2 \
      --arm rule+d=window:600,capped,keepmargin,minslots:2 --arm llm+c=history,minslots:2 >> "$R/runs.log" 2>&1
    if [ -f "$d/meta.json" ]; then say "done $sc rep $rep"; fails=0; continue; fi
    fails=$((fails + 1))
    say "FAILED $sc rep $rep: $(grep -v '^    t=' "$R/runs.log" | tail -1 | cut -c1-300)"
    if [ "$fails" -ge 2 ]; then say "STOP two runs in a row failed"; exit 1; fi
  done
done
say "QUEUE OVER"
