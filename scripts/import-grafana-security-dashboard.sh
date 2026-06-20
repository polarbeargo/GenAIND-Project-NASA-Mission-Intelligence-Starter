#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

GRAFANA_URL="${GRAFANA_URL:-http://127.0.0.1:3000}"
GRAFANA_USER="${GRAFANA_USER:-admin}"
GRAFANA_PASSWORD="${GRAFANA_PASSWORD:-admin}"
DASHBOARD_FILE="${DASHBOARD_FILE:-${ROOT_DIR}/monitoring/grafana/security_dashboard.json}"
PROMETHEUS_DATASOURCE_UID="${PROMETHEUS_DATASOURCE_UID:-}"
VERIFY_DASHBOARD_FUNCTIONS="${VERIFY_DASHBOARD_FUNCTIONS:-true}"

TMP_DIR=""

log() {
  printf "[import-grafana-security-dashboard] %s\n" "$*"
}

die() {
  printf "[import-grafana-security-dashboard] ERROR: %s\n" "$*" >&2
  exit 1
}

cleanup() {
  if [[ -n "${TMP_DIR}" && -d "${TMP_DIR}" ]]; then
    rm -rf "${TMP_DIR}"
  fi
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Required command not found: $1"
}

ensure_file() {
  [[ -f "$1" ]] || die "Required file not found: $1"
}

grafana_api() {
  local method="$1"
  local path="$2"
  shift 2

  curl -fsS -u "${GRAFANA_USER}:${GRAFANA_PASSWORD}" \
    -X "${method}" \
    "${GRAFANA_URL}${path}" \
    "$@"
}

validate_grafana_access() {
  grafana_api GET "/api/health" >/dev/null || die "Unable to reach Grafana at ${GRAFANA_URL}"

  local login
  login="$(grafana_api GET "/api/user" | jq -r '.login // empty' || true)"
  [[ -n "${login}" ]] || die "Grafana authentication failed for ${GRAFANA_USER}@${GRAFANA_URL}"
  log "Authenticated to Grafana as ${login}"
}

discover_prometheus_uid() {
  if [[ -n "${PROMETHEUS_DATASOURCE_UID}" ]]; then
    log "Using PROMETHEUS_DATASOURCE_UID override: ${PROMETHEUS_DATASOURCE_UID}"
    return
  fi

  PROMETHEUS_DATASOURCE_UID="$(
    grafana_api GET "/api/datasources" \
      | jq -r 'map(select(.type == "prometheus")) | .[0].uid // empty'
  )"

  [[ -n "${PROMETHEUS_DATASOURCE_UID}" ]] || die "No Prometheus datasource found in Grafana"
  log "Detected Prometheus datasource UID: ${PROMETHEUS_DATASOURCE_UID}"
}

build_import_payload() {
  local bound_dashboard_path="$1"
  local payload_path="$2"

  jq --arg ds "${PROMETHEUS_DATASOURCE_UID}" '
    .panels |= map(
      if (.datasource.type? == "prometheus")
      then .datasource.uid = $ds
      else .
      end
    )
  ' "${DASHBOARD_FILE}" >"${bound_dashboard_path}"

  jq -n --argjson dashboard "$(cat "${bound_dashboard_path}")" '{dashboard:$dashboard,folderId:0,overwrite:true}' >"${payload_path}"
}

import_dashboard() {
  local payload_path="$1"

  local response
  response="$(grafana_api POST "/api/dashboards/db" -H "Content-Type: application/json" --data-binary "@${payload_path}")"

  local status
  status="$(jq -r '.status // empty' <<<"${response}")"
  [[ "${status}" == "success" ]] || die "Grafana import failed: $(jq -r '.message // .error // "unknown error"' <<<"${response}")"

  log "Dashboard imported successfully"
  log "Dashboard URL: ${GRAFANA_URL}$(jq -r '.url' <<<"${response}")"
}

verify_dashboard_binding() {
  local uid
  uid="$(jq -r '.uid' "${DASHBOARD_FILE}")"
  [[ -n "${uid}" && "${uid}" != "null" ]] || die "Dashboard UID missing in ${DASHBOARD_FILE}"

  local dashboard_json
  dashboard_json="$(grafana_api GET "/api/dashboards/uid/${uid}")"

  local missing
  missing="$(jq -r --arg ds "${PROMETHEUS_DATASOURCE_UID}" '
    [
      .dashboard.panels[]
      | select(.datasource.type? == "prometheus")
      | select(.datasource.uid != $ds)
      | .title
    ] | length
  ' <<<"${dashboard_json}")"

  [[ "${missing}" == "0" ]] || die "Some Prometheus panels are not bound to datasource UID ${PROMETHEUS_DATASOURCE_UID}"
  log "Verified all Prometheus panels are bound to datasource UID ${PROMETHEUS_DATASOURCE_UID}"
}

verify_panel_queries() {
  [[ "${VERIFY_DASHBOARD_FUNCTIONS}" == "true" ]] || {
    log "Skipping panel query verification (VERIFY_DASHBOARD_FUNCTIONS=${VERIFY_DASHBOARD_FUNCTIONS})"
    return
  }

  local queries=(
    "nasa_security_active_threats"
    "nasa_security_rate_limit_events_last_hour"
    "nasa_security_critical_events_last_hour"
    "sum by (event_type,severity) (nasa_security_event_total)"
  )

  for query in "${queries[@]}"; do
    local result
    result="$(
      curl -fsS -u "${GRAFANA_USER}:${GRAFANA_PASSWORD}" --get \
        "${GRAFANA_URL}/api/datasources/proxy/uid/${PROMETHEUS_DATASOURCE_UID}/api/v1/query" \
        --data-urlencode "query=${query}"
    )"

    local status
    status="$(jq -r '.status // empty' <<<"${result}")"
    [[ "${status}" == "success" ]] || die "Prometheus query failed through Grafana proxy: ${query}"

    local series
    series="$(jq -r '.data.result | length' <<<"${result}")"
    log "Query OK: ${query} (series=${series})"
  done
}

main() {
  require_cmd curl
  require_cmd jq
  ensure_file "${DASHBOARD_FILE}"

  TMP_DIR="$(mktemp -d)"
  trap cleanup EXIT

  validate_grafana_access
  discover_prometheus_uid

  local bound_dashboard_path="${TMP_DIR}/security-dashboard-bound.json"
  local payload_path="${TMP_DIR}/security-dashboard-import.json"

  build_import_payload "${bound_dashboard_path}" "${payload_path}"
  import_dashboard "${payload_path}"
  verify_dashboard_binding
  verify_panel_queries

  log "Done"
}

main "$@"
