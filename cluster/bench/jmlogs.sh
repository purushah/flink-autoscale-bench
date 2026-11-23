#!/bin/sh
# Saves the JobManager logs of the test's own jobs (pods named bench-*) while the queues run, so that
# a rare event in a run can be explained afterwards: the pods are gone when a run ends.
#   sh jmlogs.sh        stops when /results/jmlogs.stop exists
set -u
D=/results/jmlogs
NS="${NS:?the namespace of the test, e.g. NS=my-namespace sh jmlogs.sh}"
mkdir -p "$D"
prev=$(date -u +%FT%TZ)
while [ ! -f /results/jmlogs.stop ]; do
  sleep 45
  now=$(date -u +%FT%TZ)
  for p in $(kubectl -n "$NS" get pods -l component=jobmanager -o name 2>/dev/null | grep '^pod/bench-'); do
    n=${p#pod/}
    kubectl -n "$NS" logs "$p" -c flink-main-container --since-time="$prev" --timestamps >> "$D/$n.log" 2>/dev/null
  done
  prev=$now
done
