#!/bin/sh
# The backlog signal in closed loop on the cluster at the operator's default time settings, run inside the
# driver pod: 15-minute metric window, 5-minute stabilization, 30-minute catch-up, 5-minute restart time,
# 1-hour scale-down interval (bench_cluster.py --windows default). Scenario surge_default: 200 to 2,400
# records/s times the scale for 90 minutes, after a warm-up of 21 minutes. One run takes about 2 h 10 min.
#
#   sh queue_defaultwin.sh [scale] [reps] [end epoch]        default: 2 6 and no end
#
# Four jobs side by side, all with the built-in rule, as in queue_signal.sh:
#   builtin                 shipped band, default signal (pendingRecords)
#   builtin+lagfix          shipped band, backlog and arrival rate from the brokers
#   builtin+max70           band with its upper bound at the target, default signal
#   builtin+max70+lagfix    the same band, broker-side signal
# The queue starts when the pre-test queue (queue_pretrend.sh) is over. The registered tests of the fixes
# go first: if their flag (/results/cluster10.pushed) is there before a run, the queue waits until their
# chain has ended and then goes on. No run starts after the end epoch (the end of data collection). A run
# whose meta.json is there is skipped. Two failed runs in a row stop the queue.
set -u
SCALE="${1:-2}"
REPS="${2:-6}"
END="${3:-0}"
SC=surge_default
R="${CLUSTER_RESULTS:-/results}/defaultwin"
Q="$R/queue.log"
JOBS="builtin,builtin+lagfix,builtin+max70,builtin+max70+lagfix"
mkdir -p "$R"
cd "$(dirname "$0")"

say() { echo "$(date -u +%FT%TZ) $*" >> "$Q"; }
idle() { until python3 /tmp/ps.py | grep -q "harness processes: 0"; do sleep 15; done; }

say "queue start: jobs $JOBS, scale $SCALE, reps $REPS, scenario $SC, windows default, end epoch $END"
if [ -f /results/pretrend/queue.log ]; then
  until grep -qE "QUEUE OVER|STOP " /results/pretrend/queue.log; do sleep 30; done
fi
idle
fails=0
for rep in $(seq 1 "$REPS"); do
  d="$R/$SC/rep$rep"
  if [ -f "$d/meta.json" ]; then say "skip $SC rep $rep (done)"; continue; fi
  if [ -f /results/cluster10.pushed ] && ! grep -qE "ALL OVER|STOP " /results/next14.log 2>/dev/null; then
    say "waiting: the registered tests of the fixes run first"
    until grep -qE "ALL OVER|STOP " /results/next14.log 2>/dev/null; do sleep 60; done
    idle
    say "the registered tests are over, going on"
  fi
  if [ "$END" -gt 0 ] && [ "$(date +%s)" -ge "$END" ]; then say "STOP end of data collection, no run after it"; exit 0; fi
  if [ -d "$d" ]; then mv "$d" "$d.failed-$(date +%s)"; fi
  say "start $SC rep $rep, scale $SCALE"
  python3 bench_cluster.py run --scenario "$SC" --rep "$rep" --jobs "$JOBS" --scale "$SCALE" --out "$R" \
    --source-parallelism 2 --jitter 60 --windows default >> "$R/runs.log" 2>&1
  if [ -f "$d/meta.json" ]; then say "done $SC rep $rep"; fails=0; continue; fi
  fails=$((fails + 1))
  say "FAILED $SC rep $rep: $(grep -v '^    t=' "$R/runs.log" | tail -1 | cut -c1-300)"
  if [ "$fails" -ge 2 ]; then say "STOP two runs in a row failed"; exit 1; fi
done
say "QUEUE OVER"
