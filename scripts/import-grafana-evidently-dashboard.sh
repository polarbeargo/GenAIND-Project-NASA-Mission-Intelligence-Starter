#!/usr/bin/env bash
# Import Evidently Monitor dashboard into Grafana
# Supports both Docker-based and in-cluster Grafana instances

set -Eeuo pipefail

GRAFANA_URL="${GRAFANA_URL:-http://127.0.0.1:3000}"
GRAFANA_USER="${GRAFANA_USER:-}"
GRAFANA_PASSWORD="${GRAFANA_PASSWORD:-}"
GRAFANA_NAMESPACE="${GRAFANA_NAMESPACE:-monitoring}"
GRAFANA_SECRET_NAME="${GRAFANA_SECRET_NAME:-kube-prometheus-stack-grafana}"
DASHBOARD_JSON_PATH="${DASHBOARD_JSON_PATH:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../monitoring/grafana" && pwd)/evidently_monitor_dashboard.json}"
PROMETHEUS_UID="${PROMETHEUS_UID:-prometheus}"  # Standard uid in kube-prometheus-stack
DASHBOARD_UID="evidently-monitor"

log() {
  printf "[import-grafana-evidently-dashboard] %s\n" "$*"
}

die() {
  printf "[import-grafana-evidently-dashboard] ERROR: %s\n" "$*" >&2
  exit 1
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

  # Final fallback for local Docker Grafana defaults.
  GRAFANA_USER="${GRAFANA_USER:-admin}"
  GRAFANA_PASSWORD="${GRAFANA_PASSWORD:-admin}"
  log "Using fallback Grafana credentials (admin/admin)."
}

get_or_create_datasource_uid() {
  local ds_name="$1"
  printf "[import-grafana-evidently-dashboard] Checking for Prometheus datasource: %s\n" "${ds_name}" >&2
  
  local response
  response=$(curl -s -X GET \
    -H "Content-Type: application/json" \
    -u "${GRAFANA_USER}:${GRAFANA_PASSWORD}" \
    "${GRAFANA_URL}/api/datasources/name/${ds_name}" || echo "{}")
  
  local uid
  uid=$(echo "$response" | jq -r '.uid // empty' 2>/dev/null || echo "")
  
  if [[ -z "$uid" || "$uid" == "null" ]]; then
    printf "[import-grafana-evidently-dashboard] Prometheus datasource not found, using fallback UID: %s\n" "${PROMETHEUS_UID}" >&2
    echo "${PROMETHEUS_UID}"
  else
    echo "$uid"
  fi
}

import_dashboard() {
  log "Reading dashboard JSON from: ${DASHBOARD_JSON_PATH}"
  ensure_file "${DASHBOARD_JSON_PATH}"
  
  local dashboard_json=$(cat "${DASHBOARD_JSON_PATH}")
  
  # Resolve datasource UID
  local ds_uid=$(get_or_create_datasource_uid "Prometheus")
  log "Using Prometheus datasource UID: ${ds_uid}"
  
  # Substitute datasource placeholder
  dashboard_json=$(echo "$dashboard_json" | jq \
    --arg ds_uid "$ds_uid" \
    '.panels |= map(
      if (.datasource.type? == "prometheus") then
        .datasource.uid = $ds_uid
      else
        .
      end
    )' \
    2>/dev/null || echo "$dashboard_json")
  
  # Prepare import payload (overwrite=true allows re-import)
  local import_payload=$(cat <<EOF
{
  "dashboard": $dashboard_json,
  "overwrite": true,
  "folderId": 0
}
EOF
)
  
  log "Importing dashboard to Grafana: ${GRAFANA_URL}"
  local response=$(curl -s -X POST \
    -H "Content-Type: application/json" \
    -u "${GRAFANA_USER}:${GRAFANA_PASSWORD}" \
    -d "$import_payload" \
    "${GRAFANA_URL}/api/dashboards/db" || echo "{}")
  
  local dash_id=$(echo "$response" | jq -r '.id // .dashboard.id // empty' 2>/dev/null || echo "")
  local status=$(echo "$response" | jq -r '.status // .message // empty' 2>/dev/null || echo "")
  
  if [[ -z "$dash_id" || "$dash_id" == "null" ]]; then
    die "Failed to import dashboard. Response: $(echo "$response" | jq . 2>/dev/null || echo "$response")"
  fi
  
  log "✅ Dashboard imported successfully!"
  log "   Dashboard ID: ${dash_id}"
  log "   Dashboard UID: ${DASHBOARD_UID}"
  log "   URL: ${GRAFANA_URL}/d/${DASHBOARD_UID}"
  
  # Try to open in browser if possible
  if command -v open >/dev/null 2>&1; then
    log "Opening dashboard in browser..."
    open "${GRAFANA_URL}/d/${DASHBOARD_UID}"
  elif command -v xdg-open >/dev/null 2>&1; then
    xdg-open "${GRAFANA_URL}/d/${DASHBOARD_UID}"
  fi
}

verify_grafana_connectivity() {
  log "Checking Grafana connectivity: ${GRAFANA_URL}"
  local response=$(curl -s -o /dev/null -w "%{http_code}" \
    -u "${GRAFANA_USER}:${GRAFANA_PASSWORD}" \
    "${GRAFANA_URL}/api/health" || echo "000")
  
  if [[ "$response" != "200" ]]; then
    die "Cannot connect to Grafana at ${GRAFANA_URL} (HTTP ${response}). Check GRAFANA_URL, GRAFANA_USER, GRAFANA_PASSWORD."
  fi
  log "✅ Grafana connectivity verified"
}

main() {
  require_cmd curl
  require_cmd jq
  resolve_grafana_credentials
  
  log "Evidently Monitor Dashboard Import"
  log "Dashboard: ${DASHBOARD_JSON_PATH}"
  log "Target: ${GRAFANA_URL} (user: ${GRAFANA_USER})"
  
  verify_grafana_connectivity
  import_dashboard
  
  log "Import complete!"
}

main "$@"
