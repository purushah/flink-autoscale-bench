#!/bin/sh
# Other wordings of the note of the announced-load experiment, run inside the driver pod. It needs
# the operator image that has the plugin of cluster/plugin (notes).
#
#   sh queue_notes.sh [scale] [reps]
#
# Spike runs in which the time of the spike is fixed before the jobs start (--planned). Five jobs
# side by side on the same load:
#   rule     the policy in code, told nothing
#   llm+a    the generative model, the note of the announced-load experiment (time in UTC, rates)
#   llm+c    the same note with the time on a 12-hour clock in Pacific Standard Time
#   llm+d    the same note without rates: "about 5 times its normal level"
#   llm+e    a note with the same time in it that says nothing about load
# A run whose meta.json is there is skipped. Two failed runs in a row stop the queue.
set -u
SCALE="${1:-2}"
REPS="${2:-3}"
R="${CLUSTER_RESULTS:-/results}/notes"
Q="$R/queue.log"
JOBS="rule,llm+a,llm+c,llm+d,llm+e"
mkdir -p "$R"
cd "$(dirname "$0")"

say() { echo "$(date -u +%FT%TZ) $*" >> "$Q"; }

say "queue start: jobs $JOBS, scale $SCALE, reps $REPS"
fails=0
for rep in $(seq 1 "$REPS"); do
  d="$R/spike/rep$rep"
  if [ -f "$d/meta.json" ]; then say "skip spike rep $rep (done)"; continue; fi
  if [ -d "$d" ]; then mv "$d" "$d.failed-$(date +%s)"; fi
  say "start spike rep $rep, scale $SCALE"
  python3 bench_cluster.py run --scenario spike --rep "$rep" --jobs "$JOBS" --scale "$SCALE" --out "$R" \
    --source-parallelism 2 --jitter 60 --planned --arm llm+a=note --arm llm+c=note:zone \
    --arm llm+d=note:relative --arm llm+e=note:unrelated >> "$R/runs.log" 2>&1
  if [ -f "$d/meta.json" ]; then say "done spike rep $rep"; fails=0; continue; fi
  fails=$((fails + 1))
  say "FAILED spike rep $rep: $(grep -v '^    t=' "$R/runs.log" | tail -1 | cut -c1-300)"
  if [ "$fails" -ge 2 ]; then say "STOP two runs in a row failed"; exit 1; fi
done
say "QUEUE OVER"
