#!/bin/sh
# Follows the logs of the test's own TaskManager pods (bench-*-taskmanager-*) while the queues run, so
# that the last lines of a TaskManager that stops are kept (ISSUES #105): the pod is deleted at once.
# One follower per pod (name and uid, names are used again in the next run); a follower ends when its
# pod is gone.
#   sh tmlogs.sh        stops starting followers when /results/jmlogs.stop exists
set -u
D=/results/tmlogs
NS="${NS:?the namespace of the test, e.g. NS=my-namespace sh tmlogs.sh}"
mkdir -p "$D"
while [ ! -f /results/jmlogs.stop ]; do
  kubectl -n "$NS" get pods -l component=taskmanager --field-selector=status.phase=Running \
    -o jsonpath='{range .items[*]}{.metadata.name} {.metadata.uid}{"\n"}{end}' 2>/dev/null |
  grep '^bench-' | while read -r n u; do
    f="$D/$n.$u.log"
    if [ ! -f "$f" ]; then
      : > "$f"
      kubectl -n "$NS" logs -f "pod/$n" -c flink-main-container --timestamps > "$f" 2>/dev/null &
    fi
  done
  sleep 10
done
