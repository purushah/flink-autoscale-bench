#!/bin/sh
# The backlog signal in closed loop on the cluster, run inside the driver pod: the eight-minute overload
# of the laptop's campaign E8b (surge: 200 to 2,400 records/s times the scale, for 8 minutes). It needs
# the operator image whose plugin can log in to the brokers (cluster/plugin with lag.auth).
#
#   sh queue_signal.sh [scale] [reps] [scenarios] [windows]        default: 2 6 surge short
#   sh queue_signal.sh 2 6 surge_long long     the 40-minute overload with five-minute windows (E8c)
#
# Four jobs side by side, all with the built-in rule:
#   builtin                 shipped band, default signal (pendingRecords)
#   builtin+lagfix          shipped band, backlog and arrival rate from the brokers
#   builtin+max70           band with its upper bound at the target, default signal
#   builtin+max70+lagfix    the same band, broker-side signal
# A run whose meta.json is there is skipped. Two failed runs in a row stop the queue.
set -u
SCALE="${1:-2}"
REPS="${2:-6}"
SCENARIOS="${3:-surge}"
WINDOWS="${4:-short}"
R="${CLUSTER_RESULTS:-/results}/signal"
Q="$R/queue.log"
JOBS="builtin,builtin+lagfix,builtin+max70,builtin+max70+lagfix"
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
      --source-parallelism 2 --jitter 60 --windows "$WINDOWS" >> "$R/runs.log" 2>&1
    if [ -f "$d/meta.json" ]; then say "done $sc rep $rep"; fails=0; continue; fi
    fails=$((fails + 1))
    say "FAILED $sc rep $rep: $(grep -v '^    t=' "$R/runs.log" | tail -1 | cut -c1-300)"
    if [ "$fails" -ge 2 ]; then say "STOP two runs in a row failed"; exit 1; fi
  done
done
say "QUEUE OVER"
