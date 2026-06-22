#!/usr/bin/env bash
# Import Evidently Monitor dashboard into Grafana
# Supports both Docker-based and in-cluster Grafana instances

set -Eeuo pipefail

GRAFANA_URL="${GRAFANA_URL:-http://127.0.0.1:3000}"
GRAFANA_USER="${GRAFANA_USER:-admin}"
GRAFANA_PASSWORD="${GRAFANA_PASSWORD:-admin}"
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

get_or_create_datasource_uid() {
  local ds_name="$1"
  log "Checking for Prometheus datasource: ${ds_name}"
  
  local response=$(curl -s -X GET \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $(get_grafana_token)" \
    "${GRAFANA_URL}/api/datasources/name/${ds_name}" || echo "{}")
  
  local uid=$(echo "$response" | jq -r '.uid // empty' 2>/dev/null || echo "")
  
  if [[ -z "$uid" || "$uid" == "null" ]]; then
    log "Prometheus datasource not found, attempting to create default reference"
    echo "prometheus"
  else
    echo "$uid"
  fi
}

get_grafana_token() {
  local response=$(curl -s -X POST \
    -H "Content-Type: application/json" \
    -d "{\"name\": \"import-token-$(date +%s)\", \"role\": \"Admin\"}" \
    "http://${GRAFANA_USER}:${GRAFANA_PASSWORD}@127.0.0.1:3000/api/auth/keys" 2>/dev/null || echo "{}")
  
  local key=$(echo "$response" | jq -r '.key // empty' 2>/dev/null || echo "")
  
  if [[ -z "$key" ]]; then
    # Fallback: basic auth with password
    echo ""
  else
    echo "$key"
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
    '.panels[].datasource.uid = $ds_uid' \
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
  
  log "Evidently Monitor Dashboard Import"
  log "Dashboard: ${DASHBOARD_JSON_PATH}"
  log "Target: ${GRAFANA_URL} (user: ${GRAFANA_USER})"
  
  verify_grafana_connectivity
  import_dashboard
  
  log "Import complete!"
}

main "$@"
