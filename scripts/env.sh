#!/usr/bin/env bash
# Shared settings for the scripts. Source this file; do not run it.
export ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export CLUSTER=jev-bench
export KCTX="kind-${CLUSTER}"
export OPERATOR_VERSION=1.16.1
# The plugin and the job target Java 17. Override JAVA17_HOME if yours lives elsewhere.
if [ -z "${JAVA17_HOME:-}" ]; then
  JAVA17_HOME="$(/usr/libexec/java_home -v 17 2>/dev/null || true)"
fi
export JAVA17_HOME
