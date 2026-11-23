#!/usr/bin/env bash
# Deletes the kind cluster and everything in it.
set -euo pipefail
source "$(dirname "$0")/env.sh"
kind delete cluster --name "$CLUSTER"
