#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

APP_NAMESPACE="${APP_NAMESPACE:-default}"
MONITORING_NAMESPACE="${MONITORING_NAMESPACE:-monitoring}"
DEPLOYMENT_NAME="${DEPLOYMENT_NAME:-nasa-mission-intelligence-api}"
HPA_NAME="${HPA_NAME:-nasa-mission-intelligence-api}"
KUBE_PROM_STACK_RELEASE="${KUBE_PROM_STACK_RELEASE:-kube-prometheus-stack}"
ADAPTER_RELEASE="${ADAPTER_RELEASE:-prometheus-adapter}"

API_MANIFEST_PATH="${API_MANIFEST_PATH:-${ROOT_DIR}/deploy/k8s/api-deployment.yaml}"
SERVICEMONITOR_PATH="${SERVICEMONITOR_PATH:-${ROOT_DIR}/deploy/k8s/servicemonitor-worker-pools.yaml}"
ADAPTER_VALUES_PATH="${ADAPTER_VALUES_PATH:-${ROOT_DIR}/deploy/k8s/prometheus-adapter-values.yaml}"
HPA_PATH="${HPA_PATH:-${ROOT_DIR}/deploy/k8s/hpa-api-worker-pools.yaml}"

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
  ensure_file "${ADAPTER_VALUES_PATH}"
  ensure_file "${HPA_PATH}"
  ensure_file "${API_MANIFEST_PATH}"
  ensure_file "${SMOKE_SCRIPT_PATH}"

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

  log "Applying ServiceMonitor"
  kubectl apply -f "${SERVICEMONITOR_PATH}" >/dev/null

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

  log "Automation complete. Custom metrics and HPA validation passed."
}

main "$@"
