#!/bin/sh
# The step-aware demand of the broker-side signal (ISSUES.md #123), run inside the driver pod: the
# eight-minute overload of queue_signal.sh with four built-in jobs that all read backlog and arrival
# rate from the brokers. It needs the operator image whose plugin has lag.step-aware (cluster/plugin,
# image cluster10; cluster9 had the same step-aware code and was never pushed).
#
#   sh queue_stepfix.sh [scale] [reps] [scenarios] [windows]        default: 2 10 surge short
#   sh queue_stepfix.sh 2 6 surge_long long     the 40-minute overload with five-minute windows
#
#   builtin+lagfix            shipped band, broker-side signal
#   builtin+lagfix+b          the same, and a jump in the arrival rate that the job does not keep up with
#                             is sized at the newest rate (lag.step-aware)
#   builtin+max70+lagfix      band with its upper bound at the target, broker-side signal
#   builtin+max70+lagfix+b    the same with lag.step-aware
# A run whose meta.json is there is skipped. Two failed runs in a row stop the queue.
set -u
SCALE="${1:-2}"
REPS="${2:-10}"
SCENARIOS="${3:-surge}"
WINDOWS="${4:-short}"
R="${CLUSTER_RESULTS:-/results}/stepfix"
Q="$R/queue.log"
JOBS="builtin+lagfix,builtin+lagfix+b,builtin+max70+lagfix,builtin+max70+lagfix+b"
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
      --source-parallelism 2 --jitter 60 --windows "$WINDOWS" --arm builtin+lagfix+b=stepaware --arm builtin+max70+lagfix+b=stepaware \
      >> "$R/runs.log" 2>&1
    if [ -f "$d/meta.json" ]; then say "done $sc rep $rep"; fails=0; continue; fi
    fails=$((fails + 1))
    say "FAILED $sc rep $rep: $(grep -v '^    t=' "$R/runs.log" | tail -1 | cut -c1-300)"
    if [ "$fails" -ge 2 ]; then say "STOP two runs in a row failed"; exit 1; fi
  done
done
say "QUEUE OVER"
