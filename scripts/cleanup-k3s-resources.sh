#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
CRD_NAME="logparserlabs.lab.learning.io"
LAB_RESOURCE="logparserlabs.lab.learning.io"
DEFAULT_LAB_NAME="log-lab"
LEGACY_LAB_NAME="test-lab"
OPERATOR_NAMESPACE="logparser-operator-system"
DELETE_CRD=1
FAILURES=0
LOGS_DIR_RAW="${LOGS_DIR:-${REPO_ROOT}/logs}"
ANSWER_ROOT_RAW="${ANSWER_ROOT:-/tmp}"

declare -A SEEN_LAB_NAMES=()
declare -A SEEN_NAMESPACES=()
declare -A SEEN_LOG_DIRS=()
declare -A SEEN_ANSWER_FILES=()
LAB_NAMES=()
LAB_NAMESPACES=()
LOG_DIRS=()
ANSWER_OVERRIDE_FILES=()

OPERATOR_CLUSTER_ROLES=(
  "logparser-operator-manager-role"
  "logparser-operator-metrics-auth-role"
  "logparser-operator-metrics-reader"
  "logparser-operator-logparserlab-admin-role"
  "logparser-operator-logparserlab-editor-role"
  "logparser-operator-logparserlab-viewer-role"
)

OPERATOR_CLUSTER_ROLE_BINDINGS=(
  "logparser-operator-manager-rolebinding"
  "logparser-operator-metrics-auth-rolebinding"
)

resolve_process_path() {
  local path="${1:-}"

  if [[ -z "${path}" ]]; then
    return
  fi

  if [[ "${path}" = /* ]]; then
    printf '%s\n' "${path}"
    return
  fi

  printf '%s\n' "${REPO_ROOT}/${path}"
}

LOGS_DIR="$(resolve_process_path "${LOGS_DIR_RAW}")"
ANSWER_ROOT="$(resolve_process_path "${ANSWER_ROOT_RAW}")"

usage() {
  cat <<'USAGE'
Usage:
  scripts/cleanup-k3s-resources.sh [--lab-name NAME] [--keep-crd]

Environment:
  LOGS_DIR     Override the managed logs directory. Default: <repo>/logs
  ANSWER_ROOT  Override the answer root path. Default: /tmp (so the default answer file becomes /tmp/<lab>/answer.txt)

Options:
  --lab-name NAME  Additional lab name/namespace to clean. May be repeated.
  --keep-crd       Keep the LogParserLab CRD installed for faster reruns.
  -h, --help       Show this help.

This removes:
- LogParserLab custom resources
- The discovered lab namespaces (defaults to log-lab and cleans legacy test-lab leftovers)
- Operator deployment resources in logparser-operator-system
- Answer directories under ANSWER_ROOT for each discovered lab name
- Any discovered answer-file overrides
- .log files from the managed logs directory and any discovered log-dir overrides
- The LogParserLab CRD, unless --keep-crd is set
USAGE
}

append_lab_name() {
  local lab_name="${1:-}"

  if [[ -z "${lab_name}" ]]; then
    return
  fi

  if [[ -n "${SEEN_LAB_NAMES[${lab_name}]+x}" ]]; then
    return
  fi

  SEEN_LAB_NAMES["${lab_name}"]=1
  LAB_NAMES+=("${lab_name}")
}

append_namespace() {
  local namespace="${1:-}"

  if [[ -z "${namespace}" ]]; then
    return
  fi

  if [[ -n "${SEEN_NAMESPACES[${namespace}]+x}" ]]; then
    return
  fi

  SEEN_NAMESPACES["${namespace}"]=1
  LAB_NAMESPACES+=("${namespace}")
}

append_log_dir() {
  local logs_dir="${1:-}"
  local resolved

  if [[ -z "${logs_dir}" ]]; then
    resolved="${LOGS_DIR}"
  else
    resolved="$(resolve_process_path "${logs_dir}")"
  fi

  if [[ -z "${resolved}" ]]; then
    return
  fi

  if [[ -n "${SEEN_LOG_DIRS[${resolved}]+x}" ]]; then
    return
  fi

  SEEN_LOG_DIRS["${resolved}"]=1
  LOG_DIRS+=("${resolved}")
}

append_answer_override() {
  local answer_file="${1:-}"
  local mode="${2:-runtime}"
  local resolved

  if [[ -z "${answer_file}" ]]; then
    return
  fi

  if [[ "${answer_file}" = /* ]]; then
    resolved="${answer_file}"
  elif [[ "${mode}" == "spec" ]]; then
    resolved="${ANSWER_ROOT}/${answer_file}"
  else
    resolved="$(resolve_process_path "${answer_file}")"
  fi

  if [[ -n "${SEEN_ANSWER_FILES[${resolved}]+x}" ]]; then
    return
  fi

  SEEN_ANSWER_FILES["${resolved}"]=1
  ANSWER_OVERRIDE_FILES+=("${resolved}")
}

require_command() {
  local command_name="$1"

  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "Missing required command: ${command_name}" >&2
    exit 1
  fi
}

require_kubectl_ready() {
  if ! kubectl get --raw=/readyz >/dev/null 2>&1; then
    echo "kubectl is not ready; check your kubeconfig and cluster" >&2
    exit 1
  fi
}

discover_lab_details() {
  local lab_namespace
  local lab_name
  local spec_logs_dir
  local status_logs_dir
  local spec_answer_file
  local status_answer_file

  append_lab_name "${DEFAULT_LAB_NAME}"
  append_lab_name "${LEGACY_LAB_NAME}"
  append_namespace "${DEFAULT_LAB_NAME}"
  append_namespace "${LEGACY_LAB_NAME}"
  append_log_dir

  if ! kubectl get crd "${CRD_NAME}" >/dev/null 2>&1; then
    return
  fi

  while IFS=$'\t' read -r     lab_namespace     lab_name     spec_logs_dir     status_logs_dir     spec_answer_file     status_answer_file; do
    append_namespace "${lab_namespace}"
    append_lab_name "${lab_name}"

    if [[ -n "${status_logs_dir}" ]]; then
      append_log_dir "${status_logs_dir}"
    elif [[ -n "${spec_logs_dir}" ]]; then
      append_log_dir "${spec_logs_dir}"
    fi

    if [[ -n "${status_answer_file}" ]]; then
      append_answer_override "${status_answer_file}" "runtime"
    elif [[ -n "${spec_answer_file}" ]]; then
      append_answer_override "${spec_answer_file}" "spec"
    fi
  done < <(
    kubectl get "${LAB_RESOURCE}" --all-namespaces       -o jsonpath='{range .items[*]}{.metadata.namespace}{"\t"}{.metadata.name}{"\t"}{.spec.logsDir}{"\t"}{.status.logsDir}{"\t"}{.spec.answerFile}{"\t"}{.status.answerFile}{"\n"}{end}' 2>/dev/null || true
  )
}

delete_logparserlabs() {
  local lab_namespace
  local lab_name

  if ! kubectl get crd "${CRD_NAME}" >/dev/null 2>&1; then
    return
  fi

  echo "Deleting LogParserLab custom resources..."

  while IFS=$'\t' read -r lab_namespace lab_name; do
    if [[ -z "${lab_namespace}" || -z "${lab_name}" ]]; then
      continue
    fi

    kubectl delete "${LAB_RESOURCE}" "${lab_name}"       -n "${lab_namespace}"       --ignore-not-found       --timeout=30s >/dev/null 2>&1 || true
  done < <(
    kubectl get "${LAB_RESOURCE}" --all-namespaces       -o jsonpath='{range .items[*]}{.metadata.namespace}{"\t"}{.metadata.name}{"\n"}{end}' 2>/dev/null || true
  )
}

force_finalize_namespace() {
  local namespace="$1"

  if ! kubectl get namespace "${namespace}" >/dev/null 2>&1; then
    return
  fi

  if ! command -v jq >/dev/null 2>&1; then
    echo "jq is required to force finalize namespace ${namespace}" >&2
    FAILURES=1
    return
  fi

  echo "Force finalizing namespace ${namespace}..."
  kubectl get namespace "${namespace}" -o json     | jq '.spec.finalizers = []'     | kubectl replace --raw "/api/v1/namespaces/${namespace}/finalize" -f - >/dev/null 2>&1 || true
}

delete_namespace() {
  local namespace="$1"

  if ! kubectl get namespace "${namespace}" >/dev/null 2>&1; then
    return
  fi

  echo "Deleting namespace ${namespace}..."
  kubectl delete pods --all -n "${namespace}"     --grace-period=0     --force     --ignore-not-found >/dev/null 2>&1 || true

  kubectl delete namespace "${namespace}"     --ignore-not-found     --timeout=20s >/dev/null 2>&1 || true

  if kubectl get namespace "${namespace}" >/dev/null 2>&1; then
    force_finalize_namespace "${namespace}"
  fi

  if kubectl get namespace "${namespace}" >/dev/null 2>&1; then
    echo "Warning: namespace ${namespace} still exists" >&2
    FAILURES=1
  fi
}

delete_operator_resources() {
  echo "Deleting operator deployment resources..."

  kubectl delete clusterrole "${OPERATOR_CLUSTER_ROLES[@]}"     --ignore-not-found >/dev/null 2>&1 || true

  kubectl delete clusterrolebinding "${OPERATOR_CLUSTER_ROLE_BINDINGS[@]}"     --ignore-not-found >/dev/null 2>&1 || true

  delete_namespace "${OPERATOR_NAMESPACE}"
}

delete_local_workspace() {
  local lab_name
  local logs_dir
  local answer_file

  echo "Deleting local answer directories..."
  for lab_name in "${LAB_NAMES[@]}"; do
    rm -rf "${ANSWER_ROOT}/${lab_name}"
  done

  echo "Deleting answer-file overrides..."
  for answer_file in "${ANSWER_OVERRIDE_FILES[@]}"; do
    rm -f "${answer_file}"
  done

  echo "Deleting log files..."
  for logs_dir in "${LOG_DIRS[@]}"; do
    if [[ ! -d "${logs_dir}" ]]; then
      continue
    fi

    find "${logs_dir}" -maxdepth 1 -type f -name '*.log' -delete
  done
}

delete_crd() {
  if [[ "${DELETE_CRD}" -ne 1 ]]; then
    return
  fi

  if ! kubectl get crd "${CRD_NAME}" >/dev/null 2>&1; then
    return
  fi

  echo "Deleting CRD ${CRD_NAME}..."
  kubectl delete crd "${CRD_NAME}" --ignore-not-found --timeout=30s >/dev/null 2>&1 || true

  if kubectl get crd "${CRD_NAME}" >/dev/null 2>&1; then
    echo "Warning: CRD ${CRD_NAME} still exists" >&2
    FAILURES=1
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --lab-name)
      if [[ $# -lt 2 ]]; then
        echo "--lab-name requires a value" >&2
        usage >&2
        exit 1
      fi
      append_lab_name "$2"
      append_namespace "$2"
      shift 2
      ;;
    --keep-crd)
      DELETE_CRD=0
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

require_command kubectl
require_kubectl_ready
discover_lab_details

delete_logparserlabs

for namespace in "${LAB_NAMESPACES[@]}"; do
  delete_namespace "${namespace}"
done

delete_operator_resources
delete_local_workspace
delete_crd

if [[ "${FAILURES}" -ne 0 ]]; then
  echo "Cleanup finished with warnings." >&2
  exit 1
fi

echo "Cleanup complete."
