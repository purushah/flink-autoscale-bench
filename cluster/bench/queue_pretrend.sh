#!/bin/sh
# On-off load with the look-back and no trend projection, run inside the driver pod. A test of the cause
# of ISSUES.md #119 that needs no new operator image: the look-back job scales up again at each rise
# because the trend projection asks for more than the look-back kept. Here one job has the look-back and
# its projection switched off by a setting (trend-horizon-seconds 0).
#
#   sh queue_pretrend.sh [scale] [reps] [scenarios]        default: 2 2 onoff
#
# Five jobs side by side:
#   builtin+max70   the built-in autoscaler
#   rule            the policy in code
#   rule+b          the policy with the look-back (scale-down sized for the highest demand of the last
#                   10 minutes), as in queue_flapping.sh and queue_policyfix.sh
#   rule+c          rule+b with the cap in the form the running operator image has, and a plan that only
#                   scales down dropped unless it frees 2 slots, as in queue_policyfix.sh
#   rule+d          rule+b with the trend projection off
# The registered tests of the fixes go first: before each run the queue stops if the flag they wait for
# (/results/cluster10.pushed) is there. A run whose meta.json is there is skipped. Two failed runs in a
# row stop the queue.
set -u
SCALE="${1:-2}"
REPS="${2:-2}"
SCENARIOS="${3:-onoff}"
R="${CLUSTER_RESULTS:-/results}/pretrend"
Q="$R/queue.log"
JOBS="builtin+max70,rule,rule+b,rule+c,rule+d"
mkdir -p "$R"
cd "$(dirname "$0")"

say() { echo "$(date -u +%FT%TZ) $*" >> "$Q"; }

say "queue start: jobs $JOBS, scale $SCALE, reps $REPS, scenarios $SCENARIOS"
fails=0
for rep in $(seq 1 "$REPS"); do
  for sc in $(echo "$SCENARIOS" | tr ',' ' '); do
    d="$R/$sc/rep$rep"
    if [ -f "$d/meta.json" ]; then say "skip $sc rep $rep (done)"; continue; fi
    if [ -f /results/cluster10.pushed ]; then say "STOP the new operator image is in the registry, the registered tests go first"; exit 0; fi
    if [ -d "$d" ]; then mv "$d" "$d.failed-$(date +%s)"; fi
    say "start $sc rep $rep, scale $SCALE"
    python3 bench_cluster.py run --scenario "$sc" --rep "$rep" --jobs "$JOBS" --scale "$SCALE" --out "$R" \
      --source-parallelism 2 --jitter 60 --arm rule+b=window:600 --arm rule+c=window:600,capped,minslots:2 \
      --arm rule+d=window:600,trend:0 >> "$R/runs.log" 2>&1
    if [ -f "$d/meta.json" ]; then say "done $sc rep $rep"; fails=0; continue; fi
    fails=$((fails + 1))
    say "FAILED $sc rep $rep: $(grep -v '^    t=' "$R/runs.log" | tail -1 | cut -c1-300)"
    if [ "$fails" -ge 2 ]; then say "STOP two runs in a row failed"; exit 1; fi
  done
done
say "QUEUE OVER"
