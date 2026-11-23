#!/usr/bin/env bash
# Creates the local environment: kind cluster, Kafka, and the operator with the Jev plugin.
# Requires TYPESAFE_API_KEY in the environment (the key is stored only in a Kubernetes Secret).
set -euo pipefail
source "$(dirname "$0")/env.sh"
: "${TYPESAFE_API_KEY:?Set TYPESAFE_API_KEY first, e.g. export TYPESAFE_API_KEY=\"\$(cat /path/to/key)\"}"

if ! kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  kind create cluster --config "$ROOT/deploy/kind-config.yaml" --wait 120s
fi
K="kubectl --context $KCTX"

"$ROOT/scripts/build.sh"

$K apply -f "$ROOT/deploy/kafka.yaml"
$K create namespace flink --dry-run=client -o yaml | $K apply -f -
$K create namespace flink-operator --dry-run=client -o yaml | $K apply -f -
$K -n flink-operator create secret generic jev-api-key \
  --from-literal=api-key="$TYPESAFE_API_KEY" --dry-run=client -o yaml | $K apply -f -
# Optional: only needed for jobs that use a Claude model (job.autoscaler.jev.provider: anthropic).
if [ -n "${ANTHROPIC_API_KEY:-}" ]; then
  $K -n flink-operator create secret generic anthropic-api-key \
    --from-literal=api-key="$ANTHROPIC_API_KEY" --dry-run=client -o yaml | $K apply -f -
fi
$K apply -f "$ROOT/deploy/flink-pvc.yaml"

helm repo add flink-operator-${OPERATOR_VERSION} \
  "https://archive.apache.org/dist/flink/flink-kubernetes-operator-${OPERATOR_VERSION}/" --force-update >/dev/null
helm --kube-context "$KCTX" upgrade --install flink-kubernetes-operator \
  "flink-operator-${OPERATOR_VERSION}/flink-kubernetes-operator" \
  --namespace flink-operator -f "$ROOT/deploy/operator-values.yaml" --wait --timeout 5m

$K -n kafka rollout status statefulset/kafka --timeout=5m
# Restart the operator so a rebuilt plugin image or a changed key is picked up.
$K -n flink-operator rollout restart deployment/flink-kubernetes-operator
$K -n flink-operator rollout status deployment/flink-kubernetes-operator --timeout=5m
$K -n flink-operator logs deployment/flink-kubernetes-operator | grep -i "Discovered custom" || {
  echo "WARNING: the operator did not report the Jev plugin" >&2; }
echo "setup done"
