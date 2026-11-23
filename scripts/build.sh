#!/usr/bin/env bash
# Builds the plugin and the job, bakes them into images and loads the images into kind.
set -euo pipefail
source "$(dirname "$0")/env.sh"
[ -n "$JAVA17_HOME" ] || { echo "JDK 17 not found; set JAVA17_HOME" >&2; exit 1; }
export JAVA_HOME="$JAVA17_HOME"

(cd "$ROOT/jev-autoscaler-plugin" && mvn -q -B package)
(cd "$ROOT/flink-job" && mvn -q -B -DskipTests package)

docker build -q -t jev-operator:dev -f "$ROOT/deploy/Dockerfile.operator" "$ROOT"
docker build -q -t bench-job:dev -f "$ROOT/deploy/Dockerfile.job" "$ROOT"

if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  kind load docker-image jev-operator:dev bench-job:dev --name "$CLUSTER"
fi
echo "build done"
