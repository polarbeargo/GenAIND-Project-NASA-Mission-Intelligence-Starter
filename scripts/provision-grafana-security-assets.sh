#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MONITORING_NAMESPACE="${MONITORING_NAMESPACE:-monitoring}"
KUBE_PROM_STACK_RELEASE="${KUBE_PROM_STACK_RELEASE:-kube-prometheus-stack}"
DASHBOARD_FILE="${DASHBOARD_FILE:-${ROOT_DIR}/monitoring/grafana/security_dashboard.json}"
ALERT_RULES_FILE="${ALERT_RULES_FILE:-${ROOT_DIR}/monitoring/grafana/security_alert_rules.yaml}"
DASHBOARD_CONFIGMAP_NAME="${DASHBOARD_CONFIGMAP_NAME:-nasa-security-dashboard}"
ALERT_RULES_CONFIGMAP_NAME="${ALERT_RULES_CONFIGMAP_NAME:-nasa-security-alert-rules}"

log() {
  printf "[provision-grafana-security-assets] %s\n" "$*"
}

die() {
  printf "[provision-grafana-security-assets] ERROR: %s\n" "$*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Required command not found: $1"
}

ensure_file() {
  [[ -f "$1" ]] || die "Required file not found: $1"
}

main() {
  require_cmd kubectl
  ensure_file "${DASHBOARD_FILE}"
  ensure_file "${ALERT_RULES_FILE}"

  log "Applying Grafana dashboard ConfigMap (${DASHBOARD_CONFIGMAP_NAME}) from ${DASHBOARD_FILE}"
  kubectl -n "${MONITORING_NAMESPACE}" create configmap "${DASHBOARD_CONFIGMAP_NAME}" \
    --from-file=security_dashboard.json="${DASHBOARD_FILE}" \
    --dry-run=client -o yaml \
    | kubectl label --local -f - \
      grafana_dashboard=1 \
      release="${KUBE_PROM_STACK_RELEASE}" \
      app.kubernetes.io/part-of=nasa-mission-intelligence \
      app.kubernetes.io/component=security-observability \
      -o yaml \
    | kubectl apply -f - >/dev/null
  kubectl -n "${MONITORING_NAMESPACE}" annotate configmap "${DASHBOARD_CONFIGMAP_NAME}" \
    grafana_folder="NASA Security" --overwrite >/dev/null

  log "Applying Grafana alert rules ConfigMap (${ALERT_RULES_CONFIGMAP_NAME}) from ${ALERT_RULES_FILE}"
  kubectl -n "${MONITORING_NAMESPACE}" create configmap "${ALERT_RULES_CONFIGMAP_NAME}" \
    --from-file=security_alert_rules.yaml="${ALERT_RULES_FILE}" \
    --dry-run=client -o yaml \
    | kubectl label --local -f - \
      grafana_alert=1 \
      release="${KUBE_PROM_STACK_RELEASE}" \
      app.kubernetes.io/part-of=nasa-mission-intelligence \
      app.kubernetes.io/component=security-observability \
      -o yaml \
    | kubectl apply -f - >/dev/null

  log "Provisioning objects applied."
  log "If your Grafana chart has sidecar dashboard/alert provisioning enabled, assets auto-load shortly."
  log "Otherwise, import ${DASHBOARD_FILE} and ${ALERT_RULES_FILE} manually in Grafana."
}

main "$@"
