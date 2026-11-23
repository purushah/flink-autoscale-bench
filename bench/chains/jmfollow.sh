#!/bin/bash
# Read-only on the cluster: every minute copy the scheduler lines of the test jobs' JobManager logs from
# the kind node into the project. The laptop harness saves no JobManager log, and these lines show whether
# a TaskManager stopped by itself at a rescale (ISSUES.md #105). Kept per JobManager pod: the lines about
# a restart of the job, a release of workers and a closed TaskExecutor connection.
#   bash bench/chains/jmfollow.sh                           # ends when no chain script is alive
#   python3 cluster/bench/tmstops.py testbed/jm-follow        # restarts and TaskManagers gone without a release
OUT=/path/to/repo/testbed/jm-follow
NODE=jev-bench-control-plane
PAT='need release|Stopping worker|switched from state RUNNING to CANCELLING|Closing TaskExecutor connection'
mkdir -p "$OUT"
echo "$(date '+%F %T') start" >> "$OUT/follower.note"
while pgrep -f "chains/chain[0-9]*\.sh" > /dev/null; do
  for d in $(docker exec $NODE sh -c 'ls -d /var/log/pods/flink_bench-* 2>/dev/null | grep -v taskmanager'); do
    name=$(basename "$d")
    # the node's files carry the runtime's prefix (time, stream, F or P) before each line of the log
    docker exec $NODE sh -c "cat $d/*/[0-9]*.log 2>/dev/null" | sed -E 's/^[^ ]+ (stdout|stderr) [FP] //' \
      | grep -a -E "$PAT" > "$OUT/$name.tmp" 2>/dev/null
    # a pod's file only grows while the pod lives: keep the longest copy
    if [ ! -e "$OUT/$name.log" ] || [ "$(wc -c < "$OUT/$name.tmp")" -ge "$(wc -c < "$OUT/$name.log")" ]; then
      mv "$OUT/$name.tmp" "$OUT/$name.log"
    else
      rm -f "$OUT/$name.tmp"
    fi
  done
  sleep 60
done
echo "$(date '+%F %T') chain gone, $(ls "$OUT" | grep -c '\.log$') JobManager logs" >> "$OUT/follower.note"
