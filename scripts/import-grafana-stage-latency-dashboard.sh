#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

GRAFANA_URL="${GRAFANA_URL:-http://127.0.0.1:3000}"
GRAFANA_USER="${GRAFANA_USER:-admin}"
GRAFANA_PASSWORD="${GRAFANA_PASSWORD:-admin}"
DASHBOARD_FILE="${DASHBOARD_FILE:-${ROOT_DIR}/monitoring/latency_sli_dashboard.json}"
API_BASE_URL="${API_BASE_URL:-http://127.0.0.1:8000}"
INFINITY_DATASOURCE_UID="${INFINITY_DATASOURCE_UID:-}"
VERIFY_DASHBOARD_FUNCTIONS="${VERIFY_DASHBOARD_FUNCTIONS:-true}"

TMP_DIR=""

log() {
  printf "[import-grafana-stage-latency-dashboard] %s\n" "$*"
}

die() {
  printf "[import-grafana-stage-latency-dashboard] ERROR: %s\n" "$*" >&2
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

discover_infinity_uid() {
  if [[ -n "${INFINITY_DATASOURCE_UID}" ]]; then
    log "Using INFINITY_DATASOURCE_UID override: ${INFINITY_DATASOURCE_UID}"
    return
  fi

  INFINITY_DATASOURCE_UID="$(
    grafana_api GET "/api/datasources" \
      | jq -r 'map(select(.type == "yesoreyeram-infinity-datasource")) | .[0].uid // empty'
  )"

  [[ -n "${INFINITY_DATASOURCE_UID}" ]] || die "No Infinity datasource found in Grafana"
  log "Detected Infinity datasource UID: ${INFINITY_DATASOURCE_UID}"
}

build_import_payload() {
  local bound_dashboard_path="$1"
  local payload_path="$2"

  jq \
    --arg ds_inf "${INFINITY_DATASOURCE_UID}" \
    --arg api_base_url "${API_BASE_URL}" \
    '
      .panels |= map(
        if (.datasource.type? == "yesoreyeram-infinity-datasource") then
          .datasource.uid = $ds_inf
        else
          .
        end
      )
      | .templating.list |= map(
          if .name == "api_base_url" then
            .current.text = $api_base_url
            | .current.value = $api_base_url
            | .query = $api_base_url
          else
            .
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

  local api_var
  api_var="$(jq -r '.dashboard.templating.list[] | select(.name=="api_base_url") | .current.value // empty' <<<"${dashboard_json}")"
  [[ "${api_var}" == "${API_BASE_URL}" ]] || die "api_base_url variable not set to ${API_BASE_URL}"

  local wrong_inf
  wrong_inf="$(jq -r --arg ds "${INFINITY_DATASOURCE_UID}" '[.dashboard.panels[] | select(.datasource.type? == "yesoreyeram-infinity-datasource") | select(.datasource.uid != $ds)] | length' <<<"${dashboard_json}")"
  [[ "${wrong_inf}" == "0" ]] || die "Some Infinity panels are bound to unexpected datasource UID"

  log "Verified datasource bindings and api_base_url variable"
}

verify_panel_queries() {
  [[ "${VERIFY_DASHBOARD_FUNCTIONS}" == "true" ]] || {
    log "Skipping panel query verification (VERIFY_DASHBOARD_FUNCTIONS=${VERIFY_DASHBOARD_FUNCTIONS})"
    return
  }

  local url="${API_BASE_URL}/monitoring/latency-sli/timeseries?window_minutes=60&bucket_seconds=300"
  local payload
  payload="$(curl -fsS "${url}")"

  local points_count
  points_count="$(jq -r '.points | length // 0' <<<"${payload}")"
  [[ "${points_count}" =~ ^[0-9]+$ ]] || die "Latency timeseries response is malformed"
  log "Latency timeseries endpoint OK: ${url} (points=${points_count})"
}

main() {
  require_cmd curl
  require_cmd jq
  ensure_file "${DASHBOARD_FILE}"

  TMP_DIR="$(mktemp -d)"
  trap cleanup EXIT

  validate_grafana_access
  discover_infinity_uid

  local bound_dashboard_path="${TMP_DIR}/latency-sli-dashboard-bound.json"
  local payload_path="${TMP_DIR}/latency-sli-dashboard-import.json"

  build_import_payload "${bound_dashboard_path}" "${payload_path}"
  import_dashboard "${payload_path}"
  verify_dashboard_binding
  verify_panel_queries

  log "Done"
}

main "$@"
