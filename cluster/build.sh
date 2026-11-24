#!/usr/bin/env bash
# Builds the linux/amd64 images of the cluster test. Does not push and does not touch the laptop
# testbed's images (jev-operator:dev, bench-job:dev) or its jars.
#
#   cluster/build.sh            job and operator
#   cluster/build.sh driver     the harness image (cluster/bench)
#   PUSH=1 cluster/build.sh     also push (needs: docker login registry.example.com)
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REG="${REG:-registry.example.com/user}"
TAG="${TAG:-cluster3}"
OTEL_JAR="$ROOT/cluster/deploy/plugins/flink-metrics-otel-2.0.0.2610261802.jar"
STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT

build() {  # name dockerfile
  docker buildx build --platform linux/amd64 --load -q -t "$REG/$1:$TAG" -f "$2" "$STAGE" >/dev/null
  echo "built $REG/$1:$TAG"
  if [ "${PUSH:-0}" = 1 ]; then docker push -q "$REG/$1:$TAG"; fi
}

what="${1:-flink}"
if [ "$what" = flink ]; then
  cp "$ROOT/cluster/flink-job/target/bench-job-0.1.0-cluster-flink2.jar" "$STAGE/bench-job.jar"
  # PLUGIN_JAR: another build of the plugin, e.g. cluster/plugin (notes, history, schedule).
  cp "${PLUGIN_JAR:-$ROOT/jev-autoscaler-plugin/target/jev-autoscaler-plugin-0.1.0.jar}" "$STAGE/jev-autoscaler-plugin.jar"
  cp "$OTEL_JAR" "$STAGE/flink-metrics-otel.jar"
  build flink-autoscale-bench-job "$ROOT/cluster/deploy/Dockerfile.job"
  build flink-autoscale-bench-operator "$ROOT/cluster/deploy/Dockerfile.operator"
elif [ "$what" = driver ]; then
  mkdir -p "$STAGE/bench"
  cp "$ROOT/cluster/bench/"*.py "$STAGE/bench/"
  # Everything bench.py and bench_cluster.py import from bench/ (bench/tests/test_wiring.py checks this list).
  cp "$ROOT/bench/scenarios.py" "$ROOT/bench/bench.py" "$ROOT/bench/pacing.py" "$ROOT/bench/startcheck.py" "$STAGE/bench/"
  build flink-autoscale-bench-driver "$ROOT/cluster/deploy/Dockerfile.driver"
else
  echo "unknown target $what" >&2; exit 1
fi
