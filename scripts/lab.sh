#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
LOGS_DIR="${LOGS_DIR:-${REPO_ROOT}/logs}"
ANSWER_ROOT="${ANSWER_ROOT:-/tmp}"
NOTIFICATION_PORT="${NOTIFICATION_PORT:-8888}"
HEALTH_PROBE_BIND_ADDRESS="${HEALTH_PROBE_BIND_ADDRESS:-0}"
export GOCACHE="${GOCACHE:-/tmp/logparser-operator-go-build}"
export GOPATH="${GOPATH:-/tmp/logparser-operator-gopath}"
export GOMODCACHE="${GOMODCACHE:-/tmp/logparser-operator-gomod}"
CMD="${1:-up}"

usage() {
  cat <<'USAGE'
Usage:
  scripts/lab.sh [up|reset|down]

Environment:
  LOGS_DIR           Override the logs directory. Default: <repo>/logs
  ANSWER_ROOT        Override the answer root path. Default: /tmp (so the default answer file becomes /tmp/<lab>/answer.txt)
  NOTIFICATION_PORT  Override the notification UI port. Default: 8888
  HEALTH_PROBE_BIND_ADDRESS  Override the controller health probe bind address. Default: 0 (disabled for local lab runs)
USAGE
}

require_kubectl_ready() {
  if ! kubectl get --raw=/readyz >/dev/null 2>&1; then
    echo "kubectl is not ready; check your kubeconfig and cluster" >&2
    exit 1
  fi
}

cleanup_lab() {
  LOGS_DIR="${LOGS_DIR}" ANSWER_ROOT="${ANSWER_ROOT}" "${SCRIPT_DIR}/cleanup-k3s-resources.sh" --keep-crd
}

install_crds() {
  kubectl apply -f "${REPO_ROOT}/config/crd/bases/lab.learning.io_logparserlabs.yaml"
}

seed_default_cr() {
  kubectl apply -f - <<'YAML'
apiVersion: v1
kind: Namespace
metadata:
  name: log-lab
---
apiVersion: lab.learning.io/v1alpha1
kind: LogParserLab
metadata:
  name: log-lab
  namespace: log-lab
spec:
  activity: random
YAML
}

run_operator_foreground() {
  mkdir -p "${GOCACHE}"
  mkdir -p "${GOPATH}"
  mkdir -p "${GOMODCACHE}"
  mkdir -p "${LOGS_DIR}"
  echo "Starting operator locally with answer root ${ANSWER_ROOT}"
  echo "Logs directory: ${LOGS_DIR}"
  echo "Notification feed: http://localhost:${NOTIFICATION_PORT}"
  (cd "${REPO_ROOT}" && go run ./cmd/main.go --logs-dir="${LOGS_DIR}" --answer-root="${ANSWER_ROOT}" --notification-port="${NOTIFICATION_PORT}" --health-probe-bind-address="${HEALTH_PROBE_BIND_ADDRESS}")
}

case "${CMD}" in
  up)
    require_kubectl_ready
    cleanup_lab
    install_crds
    seed_default_cr
    run_operator_foreground
    ;;
  reset|down)
    require_kubectl_ready
    cleanup_lab
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    echo "Unknown command: ${CMD}" >&2
    usage >&2
    exit 1
    ;;
esac
