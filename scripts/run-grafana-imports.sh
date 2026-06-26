#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

GRAFANA_TARGET="${GRAFANA_TARGET:-k8s}"
GRAFANA_URL="${GRAFANA_URL:-}"
API_BASE_URL="${API_BASE_URL:-}"
VERIFY_API_BASE_URL="${VERIFY_API_BASE_URL:-}"
APP_NAMESPACE="${APP_NAMESPACE:-default}"
MONITORING_NAMESPACE="${MONITORING_NAMESPACE:-monitoring}"
GRAFANA_SERVICE_NAME="${GRAFANA_SERVICE_NAME:-kube-prometheus-stack-grafana}"
API_SERVICE_NAME="${API_SERVICE_NAME:-nasa-mission-intelligence-api}"
AUTO_PORT_FORWARD="${AUTO_PORT_FORWARD:-}"

TMP_DIR=""
PF_GRAFANA_PID=""
PF_API_PID=""

log() {
  printf "[run-grafana-imports] %s\n" "$*"
}

die() {
  printf "[run-grafana-imports] ERROR: %s\n" "$*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Required command not found: $1"
}

resolve_runtime_defaults() {
  case "${GRAFANA_TARGET}" in
    local)
      : "${GRAFANA_URL:=http://127.0.0.1:3000}"
      : "${API_BASE_URL:=http://127.0.0.1:8000}"
      : "${VERIFY_API_BASE_URL:=${API_BASE_URL}}"
      : "${AUTO_PORT_FORWARD:=false}"
      ;;
    k8s|cluster|port-forward)
      : "${GRAFANA_URL:=http://127.0.0.1:33000}"
      : "${API_BASE_URL:=http://nasa-mission-intelligence-api.default.svc.cluster.local:8000}"
      : "${VERIFY_API_BASE_URL:=http://127.0.0.1:18000}"
      : "${AUTO_PORT_FORWARD:=true}"
      ;;
    *)
      die "Unsupported GRAFANA_TARGET='${GRAFANA_TARGET}'. Use local or k8s."
      ;;
  esac
}

cleanup() {
  [[ -n "${PF_GRAFANA_PID}" ]] && kill "${PF_GRAFANA_PID}" >/dev/null 2>&1 || true
  [[ -n "${PF_API_PID}" ]] && kill "${PF_API_PID}" >/dev/null 2>&1 || true
  [[ -n "${TMP_DIR}" && -d "${TMP_DIR}" ]] && rm -rf "${TMP_DIR}" || true
}

wait_for_url() {
  local url="$1"
  local label="$2"
  local remaining=20

  while (( remaining > 0 )); do
    if curl -fsS "${url}" >/dev/null 2>&1; then
      log "Ready: ${label}"
      return 0
    fi
    remaining=$((remaining - 1))
    sleep 1
  done

  die "Timed out waiting for ${label} at ${url}"
}

run_step() {
  local name="$1"
  local command="$2"

  log "Running ${name}"
  if eval "${command}"; then
    printf "%-28s %s\n" "${name}" "OK"
    return 0
  fi

  local exit_code=$?
  printf "%-28s %s (%s)\n" "${name}" "FAILED" "exit ${exit_code}"
  return "${exit_code}"
}

main() {
  require_cmd curl
  resolve_runtime_defaults

  if [[ "${AUTO_PORT_FORWARD}" == "true" ]]; then
    require_cmd kubectl
  fi

  TMP_DIR="$(mktemp -d)"
  trap cleanup EXIT

  printf '#!/bin/sh\nexit 0\n' >"${TMP_DIR}/open"
  chmod +x "${TMP_DIR}/open"
  export PATH="${TMP_DIR}:$PATH"

  log "Grafana target: ${GRAFANA_TARGET}"
  log "Grafana URL: ${GRAFANA_URL}"
  log "Dashboard API base URL: ${API_BASE_URL}"
  log "Verification API base URL: ${VERIFY_API_BASE_URL}"
  log "Auto port-forward: ${AUTO_PORT_FORWARD}"

  if [[ "${AUTO_PORT_FORWARD}" == "true" ]]; then
    log "Starting port-forwards for Grafana and API"
    kubectl -n "${MONITORING_NAMESPACE}" port-forward svc/"${GRAFANA_SERVICE_NAME}" 33000:80 >"${TMP_DIR}/grafana-port-forward.log" 2>&1 &
    PF_GRAFANA_PID="$!"
    kubectl -n "${APP_NAMESPACE}" port-forward svc/"${API_SERVICE_NAME}" 18000:8000 >"${TMP_DIR}/api-port-forward.log" 2>&1 &
    PF_API_PID="$!"
  fi

  wait_for_url "${GRAFANA_URL}/api/health" "Grafana"
  wait_for_url "${VERIFY_API_BASE_URL}/monitoring/worker-pools/series" "API observability endpoint"

  local failures=0

  run_step "evidently dashboard" "cd '${ROOT_DIR}' && GRAFANA_URL='${GRAFANA_URL}' API_BASE_URL='${API_BASE_URL}' VERIFY_API_BASE_URL='${VERIFY_API_BASE_URL}' ./scripts/import-grafana-evidently-dashboard.sh" || failures=$((failures + 1))
  run_step "security dashboard" "cd '${ROOT_DIR}' && GRAFANA_URL='${GRAFANA_URL}' ./scripts/import-grafana-security-dashboard.sh" || failures=$((failures + 1))
  run_step "worker pool dashboard" "cd '${ROOT_DIR}' && GRAFANA_URL='${GRAFANA_URL}' API_BASE_URL='${API_BASE_URL}' VERIFY_API_BASE_URL='${VERIFY_API_BASE_URL}' ./scripts/import-grafana-worker-pool-dashboard.sh" || failures=$((failures + 1))
  run_step "stage latency dashboard" "cd '${ROOT_DIR}' && GRAFANA_URL='${GRAFANA_URL}' API_BASE_URL='${API_BASE_URL}' VERIFY_API_BASE_URL='${VERIFY_API_BASE_URL}' ./scripts/import-grafana-stage-latency-dashboard.sh" || failures=$((failures + 1))
  run_step "security alert rules" "cd '${ROOT_DIR}' && GRAFANA_URL='${GRAFANA_URL}' ./scripts/import-grafana-security-alert-rules.sh" || failures=$((failures + 1))

  if [[ "${failures}" -gt 0 ]]; then
    die "${failures} Grafana import step(s) failed"
  fi

  log "All Grafana import steps completed successfully"
}

main "$@"