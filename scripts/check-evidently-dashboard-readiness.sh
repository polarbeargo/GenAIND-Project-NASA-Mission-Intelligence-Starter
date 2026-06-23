#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

APP_NAMESPACE="${APP_NAMESPACE:-default}"
MONITORING_NAMESPACE="${MONITORING_NAMESPACE:-monitoring}"
GRAFANA_SERVICE_NAME="${GRAFANA_SERVICE_NAME:-kube-prometheus-stack-grafana}"
PROMETHEUS_SERVICE_NAME="${PROMETHEUS_SERVICE_NAME:-kube-prometheus-stack-prometheus}"
GRAFANA_SECRET_NAME="${GRAFANA_SECRET_NAME:-kube-prometheus-stack-grafana}"
SERVICEMONITOR_NAME="${SERVICEMONITOR_NAME:-nasa-evidently-monitor}"
DASHBOARD_UID="${DASHBOARD_UID:-evidently-monitor}"
GRAFANA_LOCAL_PORT="${GRAFANA_LOCAL_PORT:-39300}"
PROMETHEUS_LOCAL_PORT="${PROMETHEUS_LOCAL_PORT:-39390}"
GRAFANA_PORT_FORWARD_READY_PATH="${GRAFANA_PORT_FORWARD_READY_PATH:-/api/health}"
PROMETHEUS_PORT_FORWARD_READY_PATH="${PROMETHEUS_PORT_FORWARD_READY_PATH:-/-/healthy}"
PROMETHEUS_DATASOURCE_UID="${PROMETHEUS_DATASOURCE_UID:-}"

KEY_METRIC_QUERIES=(
  "nasa_monitoring_requests_total"
  "nasa_monitoring_error_rate_percent"
  "nasa_monitoring_rag_retrieval_quality_avg"
  "nasa_monitoring_sink_queue_depth"
)

TMP_DIR=""
PF_PIDS=()

log() {
  printf "[check-evidently-dashboard-readiness] %s\n" "$*"
}

die() {
  printf "[check-evidently-dashboard-readiness] ERROR: %s\n" "$*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Required command not found: $1"
}

ensure_file() {
  [[ -f "$1" ]] || die "Required file not found: $1"
}

cleanup() {
  local pid
  for pid in "${PF_PIDS[@]:-}"; do
    kill "${pid}" >/dev/null 2>&1 || true
  done

  if [[ -n "${TMP_DIR}" && -d "${TMP_DIR}" ]]; then
    rm -rf "${TMP_DIR}"
  fi
}

trap cleanup EXIT

start_port_forward() {
  local namespace="$1"
  local service_name="$2"
  local local_port="$3"
  local remote_port="$4"
  local ready_url="$5"
  local log_file="$6"

  kubectl -n "${namespace}" port-forward "svc/${service_name}" "${local_port}:${remote_port}" >"${log_file}" 2>&1 &
  local pid="$!"
  PF_PIDS+=("${pid}")

  local attempt
  for attempt in 1 2 3 4 5 6 7 8 9 10 11 12; do
    if curl -fsS "${ready_url}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  die "Port-forward for ${namespace}/${service_name} did not become ready. See ${log_file}"
}

resolve_grafana_credentials() {
  local grafana_user=""
  local grafana_password=""

  if command -v kubectl >/dev/null 2>&1; then
    grafana_user="$(kubectl get secret -n "${MONITORING_NAMESPACE}" "${GRAFANA_SECRET_NAME}" -o jsonpath='{.data.admin-user}' 2>/dev/null | base64 --decode 2>/dev/null || true)"
    grafana_password="$(kubectl get secret -n "${MONITORING_NAMESPACE}" "${GRAFANA_SECRET_NAME}" -o jsonpath='{.data.admin-password}' 2>/dev/null | base64 --decode 2>/dev/null || true)"
  fi

  [[ -n "${grafana_user}" && -n "${grafana_password}" ]] || die "Unable to resolve Grafana credentials from secret ${MONITORING_NAMESPACE}/${GRAFANA_SECRET_NAME}"
  GRAFANA_USER="${grafana_user}"
  GRAFANA_PASSWORD="${grafana_password}"
}

grafana_api() {
  local grafana_user="$1"
  local grafana_password="$2"
  local method="$3"
  local path="$4"
  shift 4

  curl -fsS -u "${grafana_user}:${grafana_password}" \
    -X "${method}" \
    "http://127.0.0.1:${GRAFANA_LOCAL_PORT}${path}" \
    "$@"
}

prometheus_api() {
  local path="$1"
  shift

  curl -fsS "http://127.0.0.1:${PROMETHEUS_LOCAL_PORT}${path}" "$@"
}

check_servicemonitor_presence() {
  log "Checking ServiceMonitor presence"
  local count
  count="$(kubectl get servicemonitor -A -o json | jq -r --arg name "${SERVICEMONITOR_NAME}" '[.items[] | select(.metadata.name == $name)] | length')"
  [[ "${count}" -gt 0 ]] || die "ServiceMonitor ${SERVICEMONITOR_NAME} is missing"
  log "ServiceMonitor found: ${SERVICEMONITOR_NAME}"
}

discover_prometheus_datasource_uid() {
  local grafana_user="$1"
  local grafana_password="$2"

  if [[ -n "${PROMETHEUS_DATASOURCE_UID}" ]]; then
    log "Using PROMETHEUS_DATASOURCE_UID override: ${PROMETHEUS_DATASOURCE_UID}"
    return 0
  fi

  PROMETHEUS_DATASOURCE_UID="$(grafana_api "${grafana_user}" "${grafana_password}" GET "/api/datasources" | jq -r 'map(select(.type == "prometheus")) | .[0].uid // empty')"
  [[ -n "${PROMETHEUS_DATASOURCE_UID}" ]] || die "No Prometheus datasource found in Grafana"
  log "Detected Prometheus datasource UID: ${PROMETHEUS_DATASOURCE_UID}"
}

check_dashboard_binding() {
  local grafana_user="$1"
  local grafana_password="$2"

  log "Validating dashboard datasource bindings"
  local dashboard_json
  dashboard_json="$(grafana_api "${grafana_user}" "${grafana_password}" GET "/api/dashboards/uid/${DASHBOARD_UID}")"

  local panel_count
  panel_count="$(jq -r '.dashboard.panels | length' <<<"${dashboard_json}")"
  [[ "${panel_count}" -gt 0 ]] || die "Dashboard ${DASHBOARD_UID} has no panels"

  local bad_bindings
  bad_bindings="$(jq -r --arg ds "${PROMETHEUS_DATASOURCE_UID}" '
    [
      .dashboard.panels[]
      | select(.datasource.type? == "prometheus")
      | select((.datasource.uid // "") != $ds)
    ]
    | length
  ' <<<"${dashboard_json}")"

  [[ "${bad_bindings}" == "0" ]] || die "Dashboard panels are bound to an invalid Prometheus datasource UID"
  log "Dashboard datasource bindings are valid"
}

check_target_health() {
  log "Checking Prometheus target health"
  local targets_json
  targets_json="$(prometheus_api '/api/v1/targets')"

  local healthy_count
  healthy_count="$(jq -r '
    .data.activeTargets
    | map(select((.scrapeUrl // "") | contains("/monitoring/analytics/prometheus")) | select(.health == "up"))
    | length
  ' <<<"${targets_json}")"

  [[ "${healthy_count}" -gt 0 ]] || {
    local diagnostics
    diagnostics="$(jq -r '
      .data.activeTargets
      | map(select((.scrapeUrl // "") | contains("/monitoring/analytics/prometheus")))
      | map({job: .labels.job, health: .health, scrapeUrl: .scrapeUrl, lastError: .lastError})
    ' <<<"${targets_json}")"
    die "Evidently analytics target is down or missing. Details: ${diagnostics}"
  }

  log "Evidently analytics target is healthy"
}

check_key_metric_queries() {
  log "Checking key Prometheus queries through Prometheus directly"
  local query
  for query in "${KEY_METRIC_QUERIES[@]}"; do
    local result
    result="$(prometheus_api '/api/v1/query' --get --data-urlencode "query=${query}")"

    local status
    status="$(jq -r '.status // empty' <<<"${result}")"
    [[ "${status}" == "success" ]] || die "Prometheus query failed: ${query}"

    local series_count
    series_count="$(jq -r '.data.result | length' <<<"${result}")"
    [[ "${series_count}" -gt 0 ]] || die "Prometheus query returned no series: ${query}"
    log "Query OK: ${query} (series=${series_count})"
  done
}

main() {
  require_cmd kubectl
  require_cmd curl
  require_cmd jq
  ensure_file "${ROOT_DIR}/monitoring/grafana/evidently_monitor_dashboard.json"

  TMP_DIR="$(mktemp -d)"

  check_servicemonitor_presence

  resolve_grafana_credentials
  local grafana_user="${GRAFANA_USER}"
  local grafana_password="${GRAFANA_PASSWORD}"

  start_port_forward "${MONITORING_NAMESPACE}" "${GRAFANA_SERVICE_NAME}" "${GRAFANA_LOCAL_PORT}" 80 "http://127.0.0.1:${GRAFANA_LOCAL_PORT}${GRAFANA_PORT_FORWARD_READY_PATH}" "${TMP_DIR}/grafana-port-forward.log"
  start_port_forward "${MONITORING_NAMESPACE}" "${PROMETHEUS_SERVICE_NAME}" "${PROMETHEUS_LOCAL_PORT}" 9090 "http://127.0.0.1:${PROMETHEUS_LOCAL_PORT}${PROMETHEUS_PORT_FORWARD_READY_PATH}" "${TMP_DIR}/prometheus-port-forward.log"

  grafana_api "${grafana_user}" "${grafana_password}" GET "/api/health" >/dev/null
  log "Grafana API is healthy"

  discover_prometheus_datasource_uid "${grafana_user}" "${grafana_password}"
  check_dashboard_binding "${grafana_user}" "${grafana_password}"
  check_target_health
  check_key_metric_queries

  log "Dashboard readiness check passed"
}

main "$@"