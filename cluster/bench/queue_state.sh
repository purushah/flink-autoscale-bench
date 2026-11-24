#!/bin/sh
# The backlog signal on a job with real state, run inside the driver pod: the eight-minute overload of
# queue_signal.sh (surge: 200 to 2,400 records/s times the scale, for 8 minutes) with jobs that hold
# N GiB of keyed state in RocksDB (job option +stN, incremental checkpoints). A restart restores that
# state from the checkpoint store, so a rescale costs what a restore of that size costs. Local recovery
# is off (Flink's default): every restart downloads.
#
#   sh queue_state.sh [scale] [reps] [gib] [scenarios] [folder] [jobs] [extra]     default: 2 8 4 surge state
#
# extra: further options for every run, e.g. --apart (each job on nodes of its own).
#
# Two jobs side by side, both with the built-in rule as shipped:
#   builtin+stN             default signal (pendingRecords)
#   builtin+lagfix+stN      backlog and arrival rate from the brokers
# Both read the same records and so write the same state in the warm-up. The measured phase begins when
# both run at the scenario's size and a completed checkpoint of each holds the state (--start-wait; the
# warm-up may take up to --drain-max longer than planned, because writing the state builds a backlog).
# A run whose meta.json is there is skipped. Two failed runs in a row stop the queue.
set -u
SCALE="${1:-2}"
REPS="${2:-8}"
GIB="${3:-4}"
SCENARIOS="${4:-surge}"
R="${CLUSTER_RESULTS:-/results}/${5:-state}"
JOBS="${6:-builtin+st$GIB,builtin+lagfix+st$GIB}"
EXTRA="${7:-}"
Q="$R/queue.log"
mkdir -p "$R"
cd "$(dirname "$0")"

say() { echo "$(date -u +%FT%TZ) $*" >> "$Q"; }

say "queue start: jobs $JOBS, scale $SCALE, reps $REPS, scenarios $SCENARIOS, windows short${EXTRA:+, extra $EXTRA}"
fails=0
for rep in $(seq 1 "$REPS"); do
  for sc in $(echo "$SCENARIOS" | tr ',' ' '); do
    d="$R/$sc/rep$rep"
    if [ -f "$d/meta.json" ]; then say "skip $sc rep $rep (done)"; continue; fi
    if [ -d "$d" ]; then mv "$d" "$d.failed-$(date +%s)"; fi
    say "start $sc rep $rep, scale $SCALE"
    python3 bench_cluster.py run --scenario "$sc" --rep "$rep" --jobs "$JOBS" --scale "$SCALE" --out "$R" \
      --source-parallelism 2 --jitter 60 --windows short --start-wait --drain-max 1200 $EXTRA >> "$R/runs.log" 2>&1
    if [ -f "$d/meta.json" ]; then say "done $sc rep $rep"; fails=0; continue; fi
    fails=$((fails + 1))
    say "FAILED $sc rep $rep: $(grep -v '^    t=' "$R/runs.log" | tail -1 | cut -c1-300)"
    if [ "$fails" -ge 2 ]; then say "STOP two runs in a row failed"; exit 1; fi
  done
done
say "QUEUE OVER"
