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
EVALUATION_WORKER_MANIFEST_PATH="${EVALUATION_WORKER_MANIFEST_PATH:-${ROOT_DIR}/deploy/k8s/evaluation-worker-deployment.yaml}"
REDIS_MANIFEST_PATH="${REDIS_MANIFEST_PATH:-${ROOT_DIR}/deploy/k8s/redis-deployment.yaml}"
KEDA_SCALER_PATH="${KEDA_SCALER_PATH:-${ROOT_DIR}/deploy/k8s/keda-scaledobject-evaluation-worker.yaml}"

STREAMLIT_DEPLOYMENT_NAME="${STREAMLIT_DEPLOYMENT_NAME:-nasa-mission-intelligence-streamlit}"
EVALUATION_WORKER_DEPLOYMENT_NAME="${EVALUATION_WORKER_DEPLOYMENT_NAME:-nasa-evaluation-worker}"
REDIS_DEPLOYMENT_NAME="${REDIS_DEPLOYMENT_NAME:-nasa-redis}"
ENABLE_STREAMLIT_CHECKS="${ENABLE_STREAMLIT_CHECKS:-true}"
ENABLE_EVALUATION_WORKER="${ENABLE_EVALUATION_WORKER:-false}"
ENABLE_TRACING_PROFILE="${ENABLE_TRACING_PROFILE:-false}"
TRACING_PATCH_PATH="${TRACING_PATCH_PATH:-${ROOT_DIR}/deploy/k8s/api-tracing-opt-in-patch.yaml}"
ENABLE_TRACING_VERIFICATION="${ENABLE_TRACING_VERIFICATION:-false}"
TRACING_VERIFY_SCRIPT_PATH="${TRACING_VERIFY_SCRIPT_PATH:-${ROOT_DIR}/scripts/verify-k8s-tracing.sh}"
REDIS_ENABLED="${REDIS_ENABLED:-false}"
REDIS_HOST="${REDIS_HOST:-}"
REDIS_PORT="${REDIS_PORT:-6379}"
REDIS_DB="${REDIS_DB:-0}"
REDIS_PASSWORD="${REDIS_PASSWORD:-}"
ENABLE_METRICS_SERVER="${ENABLE_METRICS_SERVER:-true}"
ENABLE_KEDA="${ENABLE_KEDA:-false}"
KEDA_NAMESPACE="${KEDA_NAMESPACE:-keda}"
KEDA_HELM_REPO="${KEDA_HELM_REPO:-https://kedacore.github.io/charts}"

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

install_metrics_server() {
  log "Checking metrics-server availability"
  if kubectl get apiservice v1beta1.metrics.k8s.io >/dev/null 2>&1; then
    log "metrics-server already installed, skipping"
    return 0
  fi
  require_cmd minikube
  log "Installing metrics-server via Minikube addon"
  minikube addons enable metrics-server >/dev/null 2>&1 || die "Failed to enable metrics-server addon"
  log "Waiting for metrics-server deployment"
  kubectl wait --for=condition=available --timeout=180s \
    -n kube-system deployment/metrics-server >/dev/null 2>&1 || \
    log "Warn: metrics-server may not be ready yet (non-fatal for initial setup)"
}

install_keda() {
  require_cmd helm
  log "Adding KEDA Helm repository: ${KEDA_HELM_REPO}"
  helm repo add kedacore "${KEDA_HELM_REPO}" 2>/dev/null || true
  helm repo update kedacore >/dev/null 2>&1 || die "Failed to update KEDA Helm repo"
  
  log "Checking if KEDA is already installed"
  if kubectl get ns "${KEDA_NAMESPACE}" >/dev/null 2>&1; then
    if helm list -n "${KEDA_NAMESPACE}" | grep -q keda; then
      log "KEDA already installed in namespace '${KEDA_NAMESPACE}', skipping"
      return 0
    fi
  fi
  
  log "Installing KEDA via Helm: namespace=${KEDA_NAMESPACE}"
  helm upgrade --install keda kedacore/keda \
    --namespace "${KEDA_NAMESPACE}" \
    --create-namespace \
    --wait \
    --timeout=180s >/dev/null || die "Failed to install KEDA"
  log "KEDA installed successfully"
}

main() {
  require_cmd kubectl
  ensure_file "${SETUP_METRICS_SCRIPT_PATH}"
  ensure_file "${API_MANIFEST_PATH}"
  ensure_file "${CHROMA_SEED_JOB_PATH}"
  ensure_file "${STREAMLIT_MANIFEST_PATH}"
  ensure_file "${STREAMLIT_HPA_PATH}"
  if [[ "${ENABLE_EVALUATION_WORKER}" == "true" ]]; then
    ensure_file "${EVALUATION_WORKER_MANIFEST_PATH}"
    ensure_file "${KEDA_SCALER_PATH}"
    if [[ -z "${REDIS_HOST}" ]]; then
      ensure_file "${REDIS_MANIFEST_PATH}"
      REDIS_ENABLED="true"
      REDIS_HOST="nasa-redis"
      if [[ -n "${REDIS_PASSWORD}" ]]; then
        die "Provisioned in-cluster Redis does not configure REDIS_PASSWORD; leave REDIS_PASSWORD empty or use external Redis"
      fi
    else
      [[ "${REDIS_ENABLED}" == "true" ]] || die "External Redis requires REDIS_ENABLED=true"
    fi
  fi
  if [[ "${ENABLE_TRACING_PROFILE}" == "true" ]]; then
    ensure_file "${TRACING_PATCH_PATH}"
    if [[ "${ENABLE_TRACING_VERIFICATION}" == "true" ]]; then
      ensure_file "${TRACING_VERIFY_SCRIPT_PATH}"
    fi
  fi

  log "Current kube context: $(kubectl config current-context)"

  if [[ "${ENABLE_METRICS_SERVER}" == "true" ]]; then
    install_metrics_server
  fi

  if [[ "${ENABLE_EVALUATION_WORKER}" == "true" || "${ENABLE_KEDA}" == "true" ]]; then
    install_keda
  fi

  if [[ "${ENABLE_EVALUATION_WORKER}" == "true" && "${REDIS_HOST}" == "nasa-redis" ]]; then
    log "Provisioning in-cluster Redis: ${REDIS_MANIFEST_PATH}"
    kubectl apply -f "${REDIS_MANIFEST_PATH}" >/dev/null
    log "Waiting for Redis rollout: ${REDIS_DEPLOYMENT_NAME}"
    kubectl rollout status deployment/"${REDIS_DEPLOYMENT_NAME}" -n "${APP_NAMESPACE}" --timeout=180s >/dev/null
  fi

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

  if [[ "${ENABLE_EVALUATION_WORKER}" == "true" ]]; then
    log "Enabling Redis-backed evaluation broker on API deployment"
    kubectl set env deployment/"${DEPLOYMENT_NAME}" -n "${APP_NAMESPACE}" \
      REDIS_ENABLED="${REDIS_ENABLED}" \
      REDIS_HOST="${REDIS_HOST}" \
      REDIS_PORT="${REDIS_PORT}" \
      REDIS_DB="${REDIS_DB}" \
      EVALUATION_BROKER_ENABLED=true \
      EVALUATION_LOCAL_FALLBACK_ENABLED=false >/dev/null
    if [[ -n "${REDIS_PASSWORD}" ]]; then
      kubectl set env deployment/"${DEPLOYMENT_NAME}" -n "${APP_NAMESPACE}" REDIS_PASSWORD="${REDIS_PASSWORD}" >/dev/null
    fi

    log "Applying evaluation worker deployment manifest: ${EVALUATION_WORKER_MANIFEST_PATH}"
    kubectl apply -f "${EVALUATION_WORKER_MANIFEST_PATH}" >/dev/null
    kubectl set env deployment/"${EVALUATION_WORKER_DEPLOYMENT_NAME}" -n "${APP_NAMESPACE}" \
      REDIS_ENABLED="${REDIS_ENABLED}" \
      REDIS_HOST="${REDIS_HOST}" \
      REDIS_PORT="${REDIS_PORT}" \
      REDIS_DB="${REDIS_DB}" >/dev/null
    if [[ -n "${REDIS_PASSWORD}" ]]; then
      kubectl set env deployment/"${EVALUATION_WORKER_DEPLOYMENT_NAME}" -n "${APP_NAMESPACE}" REDIS_PASSWORD="${REDIS_PASSWORD}" >/dev/null
    fi

    log "Waiting for API rollout after broker env update: ${DEPLOYMENT_NAME}"
    kubectl rollout status deployment/"${DEPLOYMENT_NAME}" -n "${APP_NAMESPACE}" --timeout=180s >/dev/null

    log "Waiting for evaluation worker rollout: ${EVALUATION_WORKER_DEPLOYMENT_NAME}"
    kubectl rollout status deployment/"${EVALUATION_WORKER_DEPLOYMENT_NAME}" -n "${APP_NAMESPACE}" --timeout=180s >/dev/null

    log "Applying KEDA ScaledObject for evaluation worker auto-scaling: ${KEDA_SCALER_PATH}"
    kubectl apply -f "${KEDA_SCALER_PATH}" >/dev/null
  fi

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
