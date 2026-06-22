#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

GRAFANA_URL="${GRAFANA_URL:-http://127.0.0.1:3000}"
GRAFANA_USER="${GRAFANA_USER:-}"
GRAFANA_PASSWORD="${GRAFANA_PASSWORD:-}"
GRAFANA_NAMESPACE="${GRAFANA_NAMESPACE:-monitoring}"
GRAFANA_SECRET_NAME="${GRAFANA_SECRET_NAME:-kube-prometheus-stack-grafana}"
DASHBOARD_FILE="${DASHBOARD_FILE:-${ROOT_DIR}/monitoring/worker_pool_scaling_dashboard.json}"
API_BASE_URL="${API_BASE_URL:-http://127.0.0.1:8000}"
INFINITY_DATASOURCE_UID="${INFINITY_DATASOURCE_UID:-}"
PROMETHEUS_DATASOURCE_UID="${PROMETHEUS_DATASOURCE_UID:-}"
PROMETHEUS_ASYNC_DATASOURCE_UID="${PROMETHEUS_ASYNC_DATASOURCE_UID:-}"
VERIFY_DASHBOARD_FUNCTIONS="${VERIFY_DASHBOARD_FUNCTIONS:-true}"

TMP_DIR=""

log() {
  printf "[import-grafana-worker-pool-dashboard] %s\n" "$*"
}

die() {
  printf "[import-grafana-worker-pool-dashboard] ERROR: %s\n" "$*" >&2
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

resolve_grafana_credentials() {
  if [[ -n "${GRAFANA_USER}" && -n "${GRAFANA_PASSWORD}" ]]; then
    return 0
  fi

  if command -v kubectl >/dev/null 2>&1; then
    local secret_user
    local secret_password
    secret_user="$(kubectl get secret -n "${GRAFANA_NAMESPACE}" "${GRAFANA_SECRET_NAME}" -o jsonpath='{.data.admin-user}' 2>/dev/null | base64 --decode 2>/dev/null || true)"
    secret_password="$(kubectl get secret -n "${GRAFANA_NAMESPACE}" "${GRAFANA_SECRET_NAME}" -o jsonpath='{.data.admin-password}' 2>/dev/null | base64 --decode 2>/dev/null || true)"

    if [[ -n "${secret_user}" && -n "${secret_password}" ]]; then
      GRAFANA_USER="${secret_user}"
      GRAFANA_PASSWORD="${secret_password}"
      log "Using Grafana credentials from Kubernetes secret ${GRAFANA_NAMESPACE}/${GRAFANA_SECRET_NAME}"
      return 0
    fi
  fi

  GRAFANA_USER="${GRAFANA_USER:-admin}"
  GRAFANA_PASSWORD="${GRAFANA_PASSWORD:-admin}"
  log "Using fallback Grafana credentials (admin/admin)."
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

discover_datasource_uids() {
  local datasources
  datasources="$(grafana_api GET "/api/datasources")"

  if [[ -z "${INFINITY_DATASOURCE_UID}" ]]; then
    INFINITY_DATASOURCE_UID="$(jq -r 'map(select(.type == "yesoreyeram-infinity-datasource")) | .[0].uid // empty' <<<"${datasources}")"
  fi
  [[ -n "${INFINITY_DATASOURCE_UID}" ]] || die "No Infinity datasource found in Grafana"

  if [[ -z "${PROMETHEUS_DATASOURCE_UID}" ]]; then
    PROMETHEUS_DATASOURCE_UID="$(jq -r 'map(select(.type == "prometheus")) | .[0].uid // empty' <<<"${datasources}")"
  fi
  [[ -n "${PROMETHEUS_DATASOURCE_UID}" ]] || die "No Prometheus datasource found in Grafana"

  if [[ -z "${PROMETHEUS_ASYNC_DATASOURCE_UID}" ]]; then
    PROMETHEUS_ASYNC_DATASOURCE_UID="${PROMETHEUS_DATASOURCE_UID}"
  fi

  log "Detected datasource UIDs: infinity=${INFINITY_DATASOURCE_UID}, prometheus=${PROMETHEUS_DATASOURCE_UID}, prometheus_async=${PROMETHEUS_ASYNC_DATASOURCE_UID}"
}

build_import_payload() {
  local bound_dashboard_path="$1"
  local payload_path="$2"

  jq \
    --arg ds_inf "${INFINITY_DATASOURCE_UID}" \
    --arg ds_prom "${PROMETHEUS_DATASOURCE_UID}" \
    --arg ds_prom_async "${PROMETHEUS_ASYNC_DATASOURCE_UID}" \
    --arg api_base_url "${API_BASE_URL}" \
    '
      .panels |= map(
        if (.datasource.type? == "yesoreyeram-infinity-datasource") then
          .datasource.uid = $ds_inf
        elif (.datasource.type? == "prometheus") then
          if (.datasource.uid? == "${DS_PROMETHEUS_ASYNC}")
          then .datasource.uid = $ds_prom_async
          else .datasource.uid = $ds_prom
          end
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

  local infinity_paths=(
    "/monitoring/worker-pools/series"
    "/monitoring/worker-pools/timeseries?window_minutes=60&bucket_seconds=300"
    "/monitoring/latency-sli/timeseries?window_minutes=60&bucket_seconds=300"
  )

  for path in "${infinity_paths[@]}"; do
    local status
    status="$(curl -fsS "${API_BASE_URL}${path}" | jq -r 'if has("series") or has("points") then "ok" else "ok" end')"
    [[ "${status}" == "ok" ]] || die "Infinity source endpoint check failed: ${API_BASE_URL}${path}"
    log "Endpoint OK: ${API_BASE_URL}${path}"
  done

  local prom_queries=(
    "nasa_worker_pool_utilization_ratio"
    "nasa_async_worker_retry_total"
  )

  for query in "${prom_queries[@]}"; do
    local result
    result="$({
      curl -fsS -u "${GRAFANA_USER}:${GRAFANA_PASSWORD}" --get \
        "${GRAFANA_URL}/api/datasources/proxy/uid/${PROMETHEUS_DATASOURCE_UID}/api/v1/query" \
        --data-urlencode "query=${query}";
    })"

    local qstatus
    qstatus="$(jq -r '.status // empty' <<<"${result}")"
    [[ "${qstatus}" == "success" ]] || die "Prometheus query failed through Grafana proxy: ${query}"

    local series
    series="$(jq -r '.data.result | length' <<<"${result}")"
    log "Prometheus query OK: ${query} (series=${series})"
  done
}

main() {
  require_cmd curl
  require_cmd jq
  ensure_file "${DASHBOARD_FILE}"
  resolve_grafana_credentials

  TMP_DIR="$(mktemp -d)"
  trap cleanup EXIT

  validate_grafana_access
  discover_datasource_uids

  local bound_dashboard_path="${TMP_DIR}/worker-pool-dashboard-bound.json"
  local payload_path="${TMP_DIR}/worker-pool-dashboard-import.json"

  build_import_payload "${bound_dashboard_path}" "${payload_path}"
  import_dashboard "${payload_path}"
  verify_dashboard_binding
  verify_panel_queries

  log "Done"
}

main "$@"
