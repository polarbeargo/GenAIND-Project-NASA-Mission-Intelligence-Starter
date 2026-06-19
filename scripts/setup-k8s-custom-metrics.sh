#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

APP_NAMESPACE="${APP_NAMESPACE:-default}"
MONITORING_NAMESPACE="${MONITORING_NAMESPACE:-monitoring}"
DEPLOYMENT_NAME="${DEPLOYMENT_NAME:-nasa-mission-intelligence-api}"
HPA_NAME="${HPA_NAME:-nasa-mission-intelligence-api}"
KUBE_PROM_STACK_RELEASE="${KUBE_PROM_STACK_RELEASE:-kube-prometheus-stack}"
ADAPTER_RELEASE="${ADAPTER_RELEASE:-prometheus-adapter}"
ENABLE_TRACING_PROFILE="${ENABLE_TRACING_PROFILE:-false}"
TRACING_PATCH_PATH="${TRACING_PATCH_PATH:-${ROOT_DIR}/deploy/k8s/api-tracing-opt-in-patch.yaml}"
ENABLE_TRACING_VERIFICATION="${ENABLE_TRACING_VERIFICATION:-false}"
TRACING_VERIFY_SCRIPT_PATH="${TRACING_VERIFY_SCRIPT_PATH:-${ROOT_DIR}/scripts/verify-k8s-tracing.sh}"

API_MANIFEST_PATH="${API_MANIFEST_PATH:-${ROOT_DIR}/deploy/k8s/api-deployment.yaml}"
SERVICEMONITOR_PATH="${SERVICEMONITOR_PATH:-${ROOT_DIR}/deploy/k8s/servicemonitor-worker-pools.yaml}"
SECURITY_SERVICEMONITOR_PATH="${SECURITY_SERVICEMONITOR_PATH:-${ROOT_DIR}/deploy/k8s/servicemonitor-security-metrics.yaml}"
ADAPTER_VALUES_PATH="${ADAPTER_VALUES_PATH:-${ROOT_DIR}/deploy/k8s/prometheus-adapter-values.yaml}"
HPA_PATH="${HPA_PATH:-${ROOT_DIR}/deploy/k8s/hpa-api-worker-pools.yaml}"
WORKER_RELIABILITY_RULES_PATH="${WORKER_RELIABILITY_RULES_PATH:-${ROOT_DIR}/deploy/k8s/prometheus-rules-worker-reliability.yaml}"
ENABLE_WORKER_RELIABILITY_ALERTS="${ENABLE_WORKER_RELIABILITY_ALERTS:-true}"
ENABLE_SECURITY_GRAFANA_PROVISIONING="${ENABLE_SECURITY_GRAFANA_PROVISIONING:-true}"
GRAFANA_SECURITY_PROVISION_SCRIPT_PATH="${GRAFANA_SECURITY_PROVISION_SCRIPT_PATH:-${ROOT_DIR}/scripts/provision-grafana-security-assets.sh}"

SMOKE_SCRIPT_PATH="${ROOT_DIR}/scripts/smoke-k8s-custom-metrics.sh"

log() {
  printf "[setup-k8s-custom-metrics] %s\n" "$*"
}

die() {
  printf "[setup-k8s-custom-metrics] ERROR: %s\n" "$*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Required command not found: $1"
}

ensure_file() {
  [[ -f "$1" ]] || die "Required file not found: $1"
}

wait_for_rollout() {
  log "Waiting for deployment rollout: ${DEPLOYMENT_NAME} (namespace: ${APP_NAMESPACE})"
  kubectl rollout status deployment/"${DEPLOYMENT_NAME}" -n "${APP_NAMESPACE}" --timeout=180s >/dev/null
}

main() {
  require_cmd kubectl
  require_cmd helm
  require_cmd jq

  ensure_file "${SERVICEMONITOR_PATH}"
  ensure_file "${SECURITY_SERVICEMONITOR_PATH}"
  ensure_file "${ADAPTER_VALUES_PATH}"
  ensure_file "${HPA_PATH}"
  ensure_file "${API_MANIFEST_PATH}"
  ensure_file "${SMOKE_SCRIPT_PATH}"
  if [[ "${ENABLE_SECURITY_GRAFANA_PROVISIONING}" == "true" ]]; then
    ensure_file "${GRAFANA_SECURITY_PROVISION_SCRIPT_PATH}"
  fi
  if [[ "${ENABLE_WORKER_RELIABILITY_ALERTS}" == "true" ]]; then
    ensure_file "${WORKER_RELIABILITY_RULES_PATH}"
  fi
  if [[ "${ENABLE_TRACING_PROFILE}" == "true" ]]; then
    ensure_file "${TRACING_PATCH_PATH}"
    if [[ "${ENABLE_TRACING_VERIFICATION}" == "true" ]]; then
      ensure_file "${TRACING_VERIFY_SCRIPT_PATH}"
    fi
  fi

  log "Current kube context: $(kubectl config current-context)"

  log "Adding/updating Helm repo: prometheus-community"
  helm repo add prometheus-community https://prometheus-community.github.io/helm-charts >/dev/null 2>&1 || true
  helm repo update >/dev/null

  log "Installing/upgrading kube-prometheus-stack"
  helm upgrade --install "${KUBE_PROM_STACK_RELEASE}" prometheus-community/kube-prometheus-stack \
    --namespace "${MONITORING_NAMESPACE}" --create-namespace >/dev/null

  log "Applying API deployment/service manifest: ${API_MANIFEST_PATH}"
  kubectl apply -f "${API_MANIFEST_PATH}" >/dev/null

  if ! kubectl get deployment "${DEPLOYMENT_NAME}" -n "${APP_NAMESPACE}" >/dev/null 2>&1; then
    die "Deployment ${DEPLOYMENT_NAME} was not found in namespace ${APP_NAMESPACE} after applying ${API_MANIFEST_PATH}."
  fi

  wait_for_rollout

  if [[ "${ENABLE_TRACING_PROFILE}" == "true" ]]; then
    log "Applying opt-in tracing profile patch: ${TRACING_PATCH_PATH}"
    kubectl patch deployment "${DEPLOYMENT_NAME}" -n "${APP_NAMESPACE}" \
      --type strategic --patch-file "${TRACING_PATCH_PATH}" >/dev/null
    wait_for_rollout
  fi

  log "Applying ServiceMonitor"
  kubectl apply -f "${SERVICEMONITOR_PATH}" >/dev/null

  log "Applying security ServiceMonitor"
  kubectl apply -f "${SECURITY_SERVICEMONITOR_PATH}" >/dev/null

  if [[ "${ENABLE_SECURITY_GRAFANA_PROVISIONING}" == "true" ]]; then
    log "Provisioning Grafana security dashboard and alert files"
    MONITORING_NAMESPACE="${MONITORING_NAMESPACE}" \
    KUBE_PROM_STACK_RELEASE="${KUBE_PROM_STACK_RELEASE}" \
    "${GRAFANA_SECURITY_PROVISION_SCRIPT_PATH}"
  fi

  if [[ "${ENABLE_WORKER_RELIABILITY_ALERTS}" == "true" ]]; then
    log "Applying worker reliability PrometheusRule: ${WORKER_RELIABILITY_RULES_PATH}"
    kubectl apply -f "${WORKER_RELIABILITY_RULES_PATH}" >/dev/null
  fi

  log "Installing/upgrading Prometheus Adapter"
  helm upgrade --install "${ADAPTER_RELEASE}" prometheus-community/prometheus-adapter \
    --namespace "${MONITORING_NAMESPACE}" --create-namespace \
    -f "${ADAPTER_VALUES_PATH}" >/dev/null

  log "Applying HPA manifest"
  kubectl apply -f "${HPA_PATH}" >/dev/null

  log "Running smoke validation (includes custom metrics + HPA + observability endpoints)"
  APP_NAMESPACE="${APP_NAMESPACE}" \
  MONITORING_NAMESPACE="${MONITORING_NAMESPACE}" \
  DEPLOYMENT_NAME="${DEPLOYMENT_NAME}" \
  HPA_NAME="${HPA_NAME}" \
  "${SMOKE_SCRIPT_PATH}"

  if [[ "${ENABLE_TRACING_PROFILE}" == "true" && "${ENABLE_TRACING_VERIFICATION}" == "true" ]]; then
    log "Running tracing verification gate"
    APP_NAMESPACE="${APP_NAMESPACE}" \
    DEPLOYMENT_NAME="${DEPLOYMENT_NAME}" \
    "${TRACING_VERIFY_SCRIPT_PATH}"
  fi

  log "Automation complete. Custom metrics and HPA validation passed."
}

main "$@"
