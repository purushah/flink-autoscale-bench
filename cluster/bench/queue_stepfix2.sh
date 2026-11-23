#!/bin/sh
# Test E (plan entry of 2026-11-23 08:20 PST): the step-aware demand with the reading at the first step
# of a rise taken over two seconds (ISSUES.md #141), run inside the driver pod. The eight-minute overload
# of queue_stepfix.sh with four built-in jobs that all read backlog and arrival rate from the brokers
# and all have lag.step-aware, so that a run gives four first step lines. It needs the operator image
# cluster12.
#
#   sh queue_stepfix2.sh [scale] [reps] [scenarios] [windows]        default: 2 5 surge short
#
#   builtin+lagfix+b, builtin+lagfix+c                shipped band, broker-side signal, lag.step-aware
#   builtin+max70+lagfix+b, builtin+max70+lagfix+c    band with its upper bound at the target, the same
# A run whose meta.json is there is skipped. Two failed runs in a row stop the queue.
set -u
SCALE="${1:-2}"
REPS="${2:-5}"
SCENARIOS="${3:-surge}"
WINDOWS="${4:-short}"
R="${CLUSTER_RESULTS:-/results}/stepfix2"
Q="$R/queue.log"
JOBS="builtin+lagfix+b,builtin+lagfix+c,builtin+max70+lagfix+b,builtin+max70+lagfix+c"
mkdir -p "$R"
cd "$(dirname "$0")"

say() { echo "$(date -u +%FT%TZ) $*" >> "$Q"; }

say "queue start: jobs $JOBS, scale $SCALE, reps $REPS, scenarios $SCENARIOS, windows $WINDOWS"
fails=0
for rep in $(seq 1 "$REPS"); do
  for sc in $(echo "$SCENARIOS" | tr ',' ' '); do
    d="$R/$sc/rep$rep"
    if [ -f "$d/meta.json" ]; then say "skip $sc rep $rep (done)"; continue; fi
    if [ -d "$d" ]; then mv "$d" "$d.failed-$(date +%s)"; fi
    say "start $sc rep $rep, scale $SCALE"
    python3 bench_cluster.py run --scenario "$sc" --rep "$rep" --jobs "$JOBS" --scale "$SCALE" --out "$R" \
      --source-parallelism 2 --jitter 60 --windows "$WINDOWS" --arm builtin+lagfix+b=stepaware --arm builtin+lagfix+c=stepaware \
      --arm builtin+max70+lagfix+b=stepaware --arm builtin+max70+lagfix+c=stepaware \
      >> "$R/runs.log" 2>&1
    if [ -f "$d/meta.json" ]; then say "done $sc rep $rep"; fails=0; continue; fi
    fails=$((fails + 1))
    say "FAILED $sc rep $rep: $(grep -v '^    t=' "$R/runs.log" | tail -1 | cut -c1-300)"
    if [ "$fails" -ge 2 ]; then say "STOP two runs in a row failed"; exit 1; fi
  done
done
say "QUEUE OVER"
