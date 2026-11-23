#!/bin/sh
# The false-alarm experiment, run inside the driver pod. It needs the operator image that has the
# plugin of cluster/plugin (notes and schedule).
#
#   sh queue_falsealarm.sh [scale] [reps]        default: 2 3
#
# The runs of queue_announced.sh with one change: the announced spike does not come (--no-event). The
# load stays at its level before the event, while the schedule entry and the note still name the event.
# Four jobs side by side on the same load:
#   rule            the policy in code
#   rule+a          the policy in code with the spike as a schedule entry: start, end, rate
#   llm             the generative model
#   llm+a           the generative model with the spike announced in a free-text note
# A run whose meta.json is there is skipped. Two failed runs in a row stop the queue.
# Optional, from the environment: FA_NAME (results folder, default falsealarm) and FA_JOBS (default the
# four jobs above). Every rule job that ends in +a gets the schedule entry and every llm job with +a the
# note, so FA_JOBS=rule+a,llm+a,llm+sonnet+a,llm+opus+a puts three models side by side on the same note.
set -u
SCALE="${1:-2}"
REPS="${2:-3}"
R="${CLUSTER_RESULTS:-/results}/${FA_NAME:-falsealarm}"
Q="$R/queue.log"
JOBS="${FA_JOBS:-rule,rule+a,llm,llm+a}"
ARMS=""
for j in $(echo "$JOBS" | tr ',' ' '); do
  case "$j" in
    rule*+a) ARMS="$ARMS --arm $j=schedule" ;;
    llm*+a) ARMS="$ARMS --arm $j=note" ;;
  esac
done
mkdir -p "$R"
cd "$(dirname "$0")"

say() { echo "$(date -u +%FT%TZ) $*" >> "$Q"; }

say "queue start: jobs $JOBS, scale $SCALE, reps $REPS, arms:$ARMS"
fails=0
for rep in $(seq 1 "$REPS"); do
  d="$R/spike/rep$rep"
  if [ -f "$d/meta.json" ]; then say "skip spike rep $rep (done)"; continue; fi
  if [ -d "$d" ]; then mv "$d" "$d.failed-$(date +%s)"; fi
  say "start spike rep $rep, scale $SCALE, no event"
  python3 bench_cluster.py run --scenario spike --rep "$rep" --jobs "$JOBS" --scale "$SCALE" --out "$R" \
    --source-parallelism 2 --jitter 60 --planned --no-event $ARMS >> "$R/runs.log" 2>&1
  if [ -f "$d/meta.json" ]; then say "done spike rep $rep"; fails=0; continue; fi
  fails=$((fails + 1))
  say "FAILED spike rep $rep: $(grep -v '^    t=' "$R/runs.log" | tail -1 | cut -c1-300)"
  if [ "$fails" -ge 2 ]; then say "STOP two runs in a row failed"; exit 1; fi
done
say "QUEUE OVER"
