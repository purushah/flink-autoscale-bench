#!/bin/bash
# Read-only on the cluster: every 5 minutes copy the operator's log files from the kind node into
# the project, so that the kubelet's rotation (10 MiB per file, 5 files kept) cannot lose any of
# them (ISSUES.md #69). Rotated files have unique names and are copied once. A file named N.log is
# the live file of the container's N-th start (N grows when the container restarts, ISSUES.md #83)
# and is copied again each time, as N.log.current.
OUT=/path/to/repo/testbed/operator-logs
while pgrep -f "chains/chain[0-9]*\.sh" > /dev/null; do
  D=$(docker exec jev-bench-control-plane sh -c 'ls -d /var/log/pods/flink-operator_*/flink-kubernetes-operator' 2>/dev/null)
  if [ -n "$D" ]; then
    for f in $(docker exec jev-bench-control-plane ls "$D" 2>/dev/null); do
      if [[ "$f" =~ ^[0-9]+\.log$ ]]; then
        docker exec jev-bench-control-plane cat "$D/$f" > "$OUT/$f.current.tmp" 2>/dev/null && mv "$OUT/$f.current.tmp" "$OUT/$f.current"
      elif [ ! -s "$OUT/$f" ]; then
        docker exec jev-bench-control-plane cat "$D/$f" > "$OUT/$f.tmp" 2>/dev/null && mv "$OUT/$f.tmp" "$OUT/$f"
        # a rotated file is first kept uncompressed and gzipped at the next rotation: drop the plain copy then
        case "$f" in *.gz) rm -f "$OUT/${f%.gz}";; esac
      fi
    done
    echo "$(date '+%F %T') $(ls "$OUT" | wc -l | tr -d ' ') files" >> "$OUT/collector.log"
  fi
  sleep 300
done
echo "chain gone: $(date '+%F %T')"; ls -la "$OUT" | tail -8
