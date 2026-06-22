#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

GRAFANA_URL="${GRAFANA_URL:-http://127.0.0.1:3000}"
GRAFANA_USER="${GRAFANA_USER:-}"
GRAFANA_PASSWORD="${GRAFANA_PASSWORD:-}"
GRAFANA_NAMESPACE="${GRAFANA_NAMESPACE:-monitoring}"
GRAFANA_SECRET_NAME="${GRAFANA_SECRET_NAME:-kube-prometheus-stack-grafana}"
ALERT_RULES_FILE="${ALERT_RULES_FILE:-${ROOT_DIR}/monitoring/grafana/security_alert_rules.yaml}"
PROMETHEUS_DATASOURCE_UID="${PROMETHEUS_DATASOURCE_UID:-}"
VERIFY_ALERT_RULES="${VERIFY_ALERT_RULES:-true}"
ALERT_FOLDER_UID="${ALERT_FOLDER_UID:-nasa-security}"
ALERT_FOLDER_TITLE="${ALERT_FOLDER_TITLE:-NASA Security}"
PYTHON_BIN=""

TMP_DIR=""

log() {
  printf "[import-grafana-security-alert-rules] %s\n" "$*"
}

die() {
  printf "[import-grafana-security-alert-rules] ERROR: %s\n" "$*" >&2
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

discover_prometheus_uid() {
  if [[ -n "${PROMETHEUS_DATASOURCE_UID}" ]]; then
    log "Using PROMETHEUS_DATASOURCE_UID override: ${PROMETHEUS_DATASOURCE_UID}"
    return
  fi

  PROMETHEUS_DATASOURCE_UID="$({
    grafana_api GET "/api/datasources" \
      | jq -r 'map(select(.type == "prometheus")) | .[0].uid // empty';
  })"

  [[ -n "${PROMETHEUS_DATASOURCE_UID}" ]] || die "No Prometheus datasource found in Grafana"
  log "Detected Prometheus datasource UID: ${PROMETHEUS_DATASOURCE_UID}"
}

prepare_rules_json() {
  local rules_json_path="$1"

  "${PYTHON_BIN}" - <<'PY' "${ALERT_RULES_FILE}" "${PROMETHEUS_DATASOURCE_UID}" "${rules_json_path}"
import json
import sys
from pathlib import Path

import yaml

rules_file = Path(sys.argv[1])
prom_uid = sys.argv[2]
out_file = Path(sys.argv[3])

data = yaml.safe_load(rules_file.read_text(encoding="utf-8"))
for group in data.get("groups", []):
    for rule in group.get("rules", []):
        for entry in rule.get("data", []):
            ds_uid = entry.get("datasourceUid")
            if isinstance(ds_uid, str):
                entry["datasourceUid"] = ds_uid.replace("${DS_PROMETHEUS}", prom_uid)

            model = entry.get("model", {})
            ds = model.get("datasource")
            if isinstance(ds, dict) and isinstance(ds.get("uid"), str):
                ds["uid"] = ds["uid"].replace("${DS_PROMETHEUS}", prom_uid)

out_file.write_text(json.dumps(data, ensure_ascii=True), encoding="utf-8")
PY
}

discover_python_with_yaml() {
  local candidates=(
    "${ROOT_DIR}/.venv/bin/python"
    "${ROOT_DIR}/.venv/bin/python3"
    "python3"
    "python"
  )

  for candidate in "${candidates[@]}"; do
    command -v "${candidate}" >/dev/null 2>&1 || continue
    if "${candidate}" - <<'PY' >/dev/null 2>&1
import yaml
PY
    then
      PYTHON_BIN="${candidate}"
      log "Using Python interpreter: ${PYTHON_BIN}"
      return
    fi
  done

  die "PyYAML is required. Install with: pip install pyyaml (or source .venv with PyYAML)"
}

ensure_alert_folder() {
  local folders
  folders="$(grafana_api GET "/api/folders")"

  local exists
  exists="$(jq -r --arg uid "${ALERT_FOLDER_UID}" '[.[] | select(.uid == $uid)] | length' <<<"${folders}")"
  if [[ "${exists}" == "0" ]]; then
    grafana_api POST "/api/folders" \
      -H "Content-Type: application/json" \
      --data "$(jq -nc --arg uid "${ALERT_FOLDER_UID}" --arg title "${ALERT_FOLDER_TITLE}" '{uid:$uid,title:$title}')" >/dev/null
    log "Created Grafana folder ${ALERT_FOLDER_TITLE} (${ALERT_FOLDER_UID})"
  else
    log "Using existing Grafana folder ${ALERT_FOLDER_UID}"
  fi
}

delete_existing_rule_if_any() {
  local rule_uid="$1"
  grafana_api DELETE "/api/v1/provisioning/alert-rules/${rule_uid}" >/dev/null 2>&1 || true
}

import_alert_rules() {
  local rules_json_path="$1"

  local groups_len
  groups_len="$(jq -r '.groups | length' "${rules_json_path}")"
  [[ "${groups_len}" -ge 1 ]] || die "No groups found in ${ALERT_RULES_FILE}"

  while IFS= read -r payload; do
    local rule_uid
    rule_uid="$(jq -r '.uid // empty' <<<"${payload}")"
    [[ -n "${rule_uid}" ]] || die "Alert rule payload is missing uid"

    delete_existing_rule_if_any "${rule_uid}"
    local response
    response="$(grafana_api POST "/api/v1/provisioning/alert-rules" -H "Content-Type: application/json" --data "${payload}")"
    local created_uid
    created_uid="$(jq -r '.uid // empty' <<<"${response}")"
    [[ "${created_uid}" == "${rule_uid}" ]] || die "Failed to import rule ${rule_uid}"
    log "Imported alert rule ${rule_uid}"
  done < <(
    jq -c --arg folder_uid "${ALERT_FOLDER_UID}" '
      .groups[] as $g
      | $g.rules[]
      | {
          uid: .uid,
          title: .title,
          ruleGroup: $g.name,
          folderUID: $folder_uid,
          condition: .condition,
          data: .data,
          noDataState: .noDataState,
          execErrState: .execErrState,
          for: .for,
          annotations: (.annotations // {}),
          labels: (.labels // {}),
          isPaused: false
        }
    ' "${rules_json_path}"
  )

  log "Security alert rules imported"
}

verify_alert_rules() {
  [[ "${VERIFY_ALERT_RULES}" == "true" ]] || {
    log "Skipping alert rule verification (VERIFY_ALERT_RULES=${VERIFY_ALERT_RULES})"
    return
  }

  local response
  response="$(grafana_api GET "/api/v1/provisioning/alert-rules")"

  local filtered
  filtered="$(jq -c --arg folder_uid "${ALERT_FOLDER_UID}" '[.[] | select(.folderUID == $folder_uid)]' <<<"${response}")"

  local rule_count
  rule_count="$(jq -r 'length' <<<"${filtered}")"
  [[ "${rule_count}" == "2" ]] || die "Expected 2 security alert rules, got ${rule_count}"

  local expected_uids=("nasa-rate-limit-spike" "nasa-critical-security-spike")
  for uid in "${expected_uids[@]}"; do
    local found
    found="$(jq -r --arg uid "${uid}" '[.[] | select(.uid == $uid)] | length' <<<"${filtered}")"
    [[ "${found}" == "1" ]] || die "Expected alert rule uid ${uid} not found"
  done

  local bad_ds
  bad_ds="$(jq -r --arg ds "${PROMETHEUS_DATASOURCE_UID}" '[.[] .data[] | select(.datasourceUid != "__expr__") | select(.datasourceUid != $ds)] | length' <<<"${filtered}")"
  [[ "${bad_ds}" == "0" ]] || die "Some alert queries are bound to unexpected datasource UID"

  log "Verified alert rules and datasource bindings"
}

main() {
  require_cmd curl
  require_cmd jq
  ensure_file "${ALERT_RULES_FILE}"
  resolve_grafana_credentials
  discover_python_with_yaml

  TMP_DIR="$(mktemp -d)"
  trap cleanup EXIT

  validate_grafana_access
  discover_prometheus_uid
  ensure_alert_folder

  local rules_json_path="${TMP_DIR}/security-alert-rules.json"

  prepare_rules_json "${rules_json_path}"
  import_alert_rules "${rules_json_path}"
  verify_alert_rules

  log "Done"
}

main "$@"
