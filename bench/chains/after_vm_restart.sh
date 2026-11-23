#!/usr/bin/env bash
# Checks to run once the VM is back, before a chain is started. Read-only except for starting the
# kind node container if it is stopped. Prints the state that the paper's log needs.
CTX=kind-jev-bench
echo "== $(date '+%F %T %Z')"
echo "== node container"; docker ps -a --filter name=jev-bench-control-plane --format '{{.Names}} {{.Status}}'
if [ -n "$(docker ps -a --filter name=jev-bench-control-plane --filter status=exited -q)" ]; then
  echo "starting the node container"; docker start jev-bench-control-plane
fi
for i in $(seq 1 60); do kubectl --context $CTX get --raw /readyz --request-timeout=5s > /dev/null 2>&1 && break; sleep 5; done
echo "== api server"; kubectl --context $CTX get --raw /readyz --request-timeout=5s; echo
echo "== nodes"; kubectl --context $CTX get nodes --no-headers
echo "== pods not Running"; kubectl --context $CTX get pods -A --no-headers | grep -v " Running \| Completed " || echo none
echo "== operator pod (restarts, last finish)"; kubectl --context $CTX -n flink-operator get pods -o 'custom-columns=NAME:.metadata.name,RESTARTS:.status.containerStatuses[*].restartCount,LAST:.status.containerStatuses[*].lastState.terminated.finishedAt' --no-headers
echo "== secrets (names only)"; kubectl --context $CTX -n flink-operator get secrets --no-headers -o custom-columns=NAME:.metadata.name | grep -c "jev-api-key\|anthropic-api-key"
echo "== flink deployments left over"; kubectl --context $CTX get flinkdeployments -A --no-headers 2>/dev/null | wc -l
echo "== kernel kills since boot"; docker exec jev-bench-control-plane sh -c 'dmesg | grep -c "Memory cgroup out of memory"'
echo "== clocks (host, node)"; date -u '+%F %T'; docker exec jev-bench-control-plane date -u '+%F %T'
echo "== proxy on 8001"; pgrep -fl "kubectl.*proxy --port=8001" | cut -c1-80 || echo none
echo "== host disk"; df -h /System/Volumes/Data | tail -1
