#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

APP_NAMESPACE="${APP_NAMESPACE:-default}"
MONITORING_NAMESPACE="${MONITORING_NAMESPACE:-monitoring}"
DEPLOYMENT_NAME="${DEPLOYMENT_NAME:-nasa-mission-intelligence-api}"
HPA_NAME="${HPA_NAME:-nasa-mission-intelligence-api}"

SETUP_METRICS_SCRIPT_PATH="${ROOT_DIR}/scripts/setup-k8s-custom-metrics.sh"
API_MANIFEST_PATH="${API_MANIFEST_PATH:-${ROOT_DIR}/deploy/k8s/api-deployment-chroma-pvc.yaml}"
CHROMA_SEED_JOB_PATH="${CHROMA_SEED_JOB_PATH:-${ROOT_DIR}/deploy/k8s/chroma-seed-job.yaml}"
STREAMLIT_MANIFEST_PATH="${STREAMLIT_MANIFEST_PATH:-${ROOT_DIR}/deploy/k8s/streamlit-deployment.yaml}"
STREAMLIT_HPA_PATH="${STREAMLIT_HPA_PATH:-${ROOT_DIR}/deploy/k8s/hpa-streamlit.yaml}"

STREAMLIT_DEPLOYMENT_NAME="${STREAMLIT_DEPLOYMENT_NAME:-nasa-mission-intelligence-streamlit}"
ENABLE_STREAMLIT_CHECKS="${ENABLE_STREAMLIT_CHECKS:-true}"
ENABLE_TRACING_PROFILE="${ENABLE_TRACING_PROFILE:-false}"
TRACING_PATCH_PATH="${TRACING_PATCH_PATH:-${ROOT_DIR}/deploy/k8s/api-tracing-opt-in-patch.yaml}"
ENABLE_TRACING_VERIFICATION="${ENABLE_TRACING_VERIFICATION:-false}"
TRACING_VERIFY_SCRIPT_PATH="${TRACING_VERIFY_SCRIPT_PATH:-${ROOT_DIR}/scripts/verify-k8s-tracing.sh}"

log() {
  printf "[setup-k8s-production-parity] %s\n" "$*"
}

die() {
  printf "[setup-k8s-production-parity] ERROR: %s\n" "$*" >&2
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
  ensure_file "${SETUP_METRICS_SCRIPT_PATH}"
  ensure_file "${API_MANIFEST_PATH}"
  ensure_file "${CHROMA_SEED_JOB_PATH}"
  ensure_file "${STREAMLIT_MANIFEST_PATH}"
  ensure_file "${STREAMLIT_HPA_PATH}"
  if [[ "${ENABLE_TRACING_PROFILE}" == "true" ]]; then
    ensure_file "${TRACING_PATCH_PATH}"
    if [[ "${ENABLE_TRACING_VERIFICATION}" == "true" ]]; then
      ensure_file "${TRACING_VERIFY_SCRIPT_PATH}"
    fi
  fi

  log "Current kube context: $(kubectl config current-context)"

  log "Applying PVC-backed API manifest for full RAG parity: ${API_MANIFEST_PATH}"
  kubectl apply -f "${API_MANIFEST_PATH}" >/dev/null

  log "Recreating Chroma seed job for idempotent collection bootstrap"
  kubectl delete job nasa-chroma-seed -n "${APP_NAMESPACE}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl apply -f "${CHROMA_SEED_JOB_PATH}" >/dev/null

  log "Waiting for Chroma seed job completion"
  kubectl wait --for=condition=complete job/nasa-chroma-seed -n "${APP_NAMESPACE}" --timeout=600s >/dev/null

  log "Running metrics/API automated setup and smoke checks"
  API_MANIFEST_PATH="${API_MANIFEST_PATH}" \
  APP_NAMESPACE="${APP_NAMESPACE}" \
  MONITORING_NAMESPACE="${MONITORING_NAMESPACE}" \
  DEPLOYMENT_NAME="${DEPLOYMENT_NAME}" \
  HPA_NAME="${HPA_NAME}" \
  ENABLE_TRACING_PROFILE="${ENABLE_TRACING_PROFILE}" \
  TRACING_PATCH_PATH="${TRACING_PATCH_PATH}" \
  ENABLE_TRACING_VERIFICATION="${ENABLE_TRACING_VERIFICATION}" \
  TRACING_VERIFY_SCRIPT_PATH="${TRACING_VERIFY_SCRIPT_PATH}" \
  "${SETUP_METRICS_SCRIPT_PATH}"

  log "Applying Streamlit deployment/service manifest: ${STREAMLIT_MANIFEST_PATH}"
  kubectl apply -f "${STREAMLIT_MANIFEST_PATH}" >/dev/null

  log "Waiting for Streamlit rollout: ${STREAMLIT_DEPLOYMENT_NAME}"
  kubectl rollout status deployment/"${STREAMLIT_DEPLOYMENT_NAME}" -n "${APP_NAMESPACE}" --timeout=180s >/dev/null

  log "Applying Streamlit HPA: ${STREAMLIT_HPA_PATH}"
  kubectl apply -f "${STREAMLIT_HPA_PATH}" >/dev/null

  if [[ "${ENABLE_STREAMLIT_CHECKS}" == "true" ]]; then
    log "Running smoke checks with Streamlit health verification"
    ENABLE_STREAMLIT_CHECKS=true \
    APP_NAMESPACE="${APP_NAMESPACE}" \
    MONITORING_NAMESPACE="${MONITORING_NAMESPACE}" \
    DEPLOYMENT_NAME="${DEPLOYMENT_NAME}" \
    HPA_NAME="${HPA_NAME}" \
    STREAMLIT_DEPLOYMENT_NAME="${STREAMLIT_DEPLOYMENT_NAME}" \
    "${ROOT_DIR}/scripts/smoke-k8s-custom-metrics.sh"
  fi

  log "Production parity setup complete (API + Streamlit + HPA + observability stack)."
}

main "$@"
