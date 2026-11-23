#!/bin/sh
# The cluster test from start to end, run inside the driver pod. After the start it needs nothing
# from the laptop: the pod talks to the cluster with its own service account.
#
#   sh queue.sh "<jobs>" [scale] [reps]        e.g.  sh queue.sh builtin+max70,rule,jev,llm 4 3
#
# Steps. A run whose meta.json is already there is skipped, so the queue can be started again.
#   1. smoke   spike at scale 1 with the two controllers that need no model. The queue stops when
#              it fails.
#   2. models  only when <jobs> has jev or llm: spike at scale 1 with all jobs. The queue stops
#              when a model controller has fewer than three answered calls.
#   3. main    spike, ramp, scale_down at <scale>, <reps> times each
#   4. levels  levels1 at <scale>, twice
# Two failed runs in a row stop the queue. Progress is in /results/queue.log.
set -u
JOBS="${1:?jobs, e.g. builtin+max70,rule,jev,llm}"
SCALE="${2:-4}"
REPS="${3:-3}"
R="${CLUSTER_RESULTS:-/results}"
Q="$R/queue.log"
# The settings of the laptop campaigns (bench/campaign.sh).
COMMON="--source-parallelism 2 --jitter 60"
cd "$(dirname "$0")"

say() { echo "$(date -u +%FT%TZ) $*" >> "$Q"; }

fails=0
one() {  # out-dir scenario rep jobs scale
  d="$1/$2/rep$3"
  if [ -f "$d/meta.json" ]; then say "skip $2 rep $3 in $1 (done)"; return 0; fi
  if [ -d "$d" ]; then mv "$d" "$d.failed-$(date +%s)"; fi
  say "start $2 rep $3, scale $5, jobs $4"
  python3 bench_cluster.py run --scenario "$2" --rep "$3" --jobs "$4" --scale "$5" --out "$1" $COMMON >> "$1.log" 2>&1
  if [ -f "$d/meta.json" ]; then say "done $2 rep $3"; fails=0; return 0; fi
  fails=$((fails + 1))
  say "FAILED $2 rep $3: $(grep -v '^    t=' "$1.log" | tail -1 | cut -c1-300)"
  if [ "$fails" -ge 2 ]; then say "STOP two runs in a row failed"; exit 1; fi
  return 1
}

say "queue start: jobs $JOBS, scale $SCALE, reps $REPS"

one "$R/smoke" spike 1 builtin+max70,rule 1 || { say "STOP the smoke run failed"; exit 1; }

case ",$JOBS," in *,jev*|*,llm*)
  one "$R/models" spike 1 "$JOBS" 1 || { say "STOP the model smoke run failed"; exit 1; }
  for m in jev llm; do
    case ",$JOBS," in *",$m"*) ;; *) continue ;; esac
    n=$(grep -c "bench-$m.*\"status\":\"DECIDED\"" "$R/models/spike/rep1/decisions.jsonl" 2>/dev/null)
    n="${n:-0}"
    say "model smoke: $m has $n answered calls"
    if [ "$n" -lt 3 ]; then say "STOP $m got no answers from its model"; exit 1; fi
  done ;;
esac

for rep in $(seq 1 "$REPS"); do
  for sc in spike ramp scale_down; do one "$R/main" "$sc" "$rep" "$JOBS" "$SCALE" || true; done
done
for rep in 1 2; do one "$R/main" levels1 "$rep" "$JOBS" "$SCALE" || true; done

say "QUEUE OVER"
