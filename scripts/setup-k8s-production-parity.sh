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
JUDGE_WORKER_MANIFEST_PATH="${JUDGE_WORKER_MANIFEST_PATH:-${ROOT_DIR}/deploy/k8s/judge-worker-deployment.yaml}"
REDIS_MANIFEST_PATH="${REDIS_MANIFEST_PATH:-${ROOT_DIR}/deploy/k8s/redis-deployment.yaml}"
KEDA_SCALER_PATH="${KEDA_SCALER_PATH:-${ROOT_DIR}/deploy/k8s/keda-scaledobject-evaluation-worker.yaml}"
JUDGE_KEDA_SCALER_PATH="${JUDGE_KEDA_SCALER_PATH:-${ROOT_DIR}/deploy/k8s/keda-scaledobject-judge-worker.yaml}"
WORKER_RELIABILITY_RULES_PATH="${WORKER_RELIABILITY_RULES_PATH:-${ROOT_DIR}/deploy/k8s/prometheus-rules-worker-reliability.yaml}"
ENABLE_WORKER_RELIABILITY_ALERTS="${ENABLE_WORKER_RELIABILITY_ALERTS:-true}"

STREAMLIT_DEPLOYMENT_NAME="${STREAMLIT_DEPLOYMENT_NAME:-nasa-mission-intelligence-streamlit}"
EVALUATION_WORKER_DEPLOYMENT_NAME="${EVALUATION_WORKER_DEPLOYMENT_NAME:-nasa-evaluation-worker}"
JUDGE_WORKER_DEPLOYMENT_NAME="${JUDGE_WORKER_DEPLOYMENT_NAME:-nasa-judge-worker}"
REDIS_DEPLOYMENT_NAME="${REDIS_DEPLOYMENT_NAME:-nasa-redis}"
ENABLE_STREAMLIT_CHECKS="${ENABLE_STREAMLIT_CHECKS:-true}"
ENABLE_EVALUATION_WORKER="${ENABLE_EVALUATION_WORKER:-false}"
ENABLE_JUDGE_WORKER="${ENABLE_JUDGE_WORKER:-false}"
ENABLE_TRACING_PROFILE="${ENABLE_TRACING_PROFILE:-false}"
TRACING_PATCH_PATH="${TRACING_PATCH_PATH:-${ROOT_DIR}/deploy/k8s/api-tracing-opt-in-patch.yaml}"
ENABLE_TRACING_VERIFICATION="${ENABLE_TRACING_VERIFICATION:-false}"
TRACING_VERIFY_SCRIPT_PATH="${TRACING_VERIFY_SCRIPT_PATH:-${ROOT_DIR}/scripts/verify-k8s-tracing.sh}"
REDIS_ENABLED="${REDIS_ENABLED:-false}"
REDIS_HOST="${REDIS_HOST:-}"
REDIS_PORT="${REDIS_PORT:-6379}"
REDIS_DB="${REDIS_DB:-0}"
REDIS_PASSWORD="${REDIS_PASSWORD:-}"
EVALUATION_BROKER_STREAM="${EVALUATION_BROKER_STREAM:-eval:jobs}"
EVALUATION_BROKER_GROUP="${EVALUATION_BROKER_GROUP:-eval-workers}"
JUDGE_BROKER_STREAM="${JUDGE_BROKER_STREAM:-judge:jobs}"
JUDGE_BROKER_GROUP="${JUDGE_BROKER_GROUP:-judge-workers}"
ENABLE_METRICS_SERVER="${ENABLE_METRICS_SERVER:-true}"
ENABLE_KEDA="${ENABLE_KEDA:-false}"
KEDA_NAMESPACE="${KEDA_NAMESPACE:-keda}"
KEDA_HELM_REPO="${KEDA_HELM_REPO:-https://kedacore.github.io/charts}"
ENABLE_MONITORING_POSTGRES="${ENABLE_MONITORING_POSTGRES:-false}"
POSTGRES_MANIFEST_PATH="${POSTGRES_MANIFEST_PATH:-${ROOT_DIR}/deploy/k8s/postgres-deployment.yaml}"
POSTGRES_DEPLOYMENT_NAME="${POSTGRES_DEPLOYMENT_NAME:-nasa-postgres}"
MONITORING_POSTGRES_DSN="${MONITORING_POSTGRES_DSN:-}"
MONITORING_POSTGRES_HOST="${MONITORING_POSTGRES_HOST:-nasa-postgres}"
MONITORING_POSTGRES_PORT="${MONITORING_POSTGRES_PORT:-5432}"
MONITORING_POSTGRES_DB="${MONITORING_POSTGRES_DB:-nasa_monitoring}"
MONITORING_POSTGRES_USER="${MONITORING_POSTGRES_USER:-postgres}"
MONITORING_POSTGRES_PASSWORD="${MONITORING_POSTGRES_PASSWORD:-postgres}"
MONITORING_POSTGRES_SSLMODE="${MONITORING_POSTGRES_SSLMODE:-prefer}"
MONITORING_PRIMARY_SINK="${MONITORING_PRIMARY_SINK:-file}"
SERVICEMONITOR_EVIDENTLY_PATH="${SERVICEMONITOR_EVIDENTLY_PATH:-${ROOT_DIR}/deploy/k8s/servicemonitor-evidently-monitor.yaml}"
ENABLE_EVIDENTLY_DASHBOARD_READINESS_CHECK="${ENABLE_EVIDENTLY_DASHBOARD_READINESS_CHECK:-true}"
EVIDENTLY_DASHBOARD_READINESS_SCRIPT_PATH="${EVIDENTLY_DASHBOARD_READINESS_SCRIPT_PATH:-${ROOT_DIR}/scripts/check-evidently-dashboard-readiness.sh}"

is_loopback_redis_host() {
  case "${1:-}" in
    ""|localhost|127.0.0.1|::1)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

validate_broker_lane_isolation() {
  if [[ "${EVALUATION_BROKER_STREAM}" == "${JUDGE_BROKER_STREAM}" ]]; then
    die "Broker lane collision: EVALUATION_BROKER_STREAM and JUDGE_BROKER_STREAM must be distinct"
  fi
  if [[ "${EVALUATION_BROKER_GROUP}" == "${JUDGE_BROKER_GROUP}" ]]; then
    die "Broker lane collision: EVALUATION_BROKER_GROUP and JUDGE_BROKER_GROUP must be distinct"
  fi
}

is_loopback_postgres_host() {
  case "${1:-}" in
    ""|localhost|127.0.0.1|nasa-postgres)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

install_postgres() {
  log "Provisioning in-cluster PostgreSQL for centralized monitoring: ${POSTGRES_MANIFEST_PATH}"
  kubectl apply -f "${POSTGRES_MANIFEST_PATH}" >/dev/null
  log "Waiting for PostgreSQL rollout: ${POSTGRES_DEPLOYMENT_NAME}"
  kubectl rollout status deployment/"${POSTGRES_DEPLOYMENT_NAME}" -n "${APP_NAMESPACE}" --timeout=180s >/dev/null || \
    die "PostgreSQL deployment failed to become ready"
  log "PostgreSQL ready. Monitoring sink will use: postgresql://${MONITORING_POSTGRES_USER}@${MONITORING_POSTGRES_HOST}:${MONITORING_POSTGRES_PORT}/${MONITORING_POSTGRES_DB}"
}

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
  if [[ "${ENABLE_MONITORING_POSTGRES}" == "true" ]]; then
    ensure_file "${POSTGRES_MANIFEST_PATH}"
  fi
  if [[ "${ENABLE_EVIDENTLY_DASHBOARD_READINESS_CHECK}" == "true" ]]; then
    ensure_file "${EVIDENTLY_DASHBOARD_READINESS_SCRIPT_PATH}"
  fi
  if [[ "${ENABLE_WORKER_RELIABILITY_ALERTS}" == "true" ]]; then
    ensure_file "${WORKER_RELIABILITY_RULES_PATH}"
  fi
  if [[ "${ENABLE_EVALUATION_WORKER}" == "true" || "${ENABLE_JUDGE_WORKER}" == "true" ]]; then
    validate_broker_lane_isolation
    ensure_file "${EVALUATION_WORKER_MANIFEST_PATH}"
    ensure_file "${JUDGE_WORKER_MANIFEST_PATH}"
    ensure_file "${KEDA_SCALER_PATH}"
    ensure_file "${JUDGE_KEDA_SCALER_PATH}"
    if is_loopback_redis_host "${REDIS_HOST}"; then
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

  log "Applying Prometheus ServiceMonitor for Evidently metrics: ${SERVICEMONITOR_EVIDENTLY_PATH}"
  kubectl apply -f "${SERVICEMONITOR_EVIDENTLY_PATH}" >/dev/null || \
    log "Warn: ServiceMonitor deployment may require kube-prometheus-stack (non-fatal if using standalone Prometheus)"

  if [[ "${ENABLE_EVALUATION_WORKER}" == "true" || "${ENABLE_JUDGE_WORKER}" == "true" || "${ENABLE_KEDA}" == "true" ]]; then
    install_keda
  fi

  if [[ "${ENABLE_MONITORING_POSTGRES}" == "true" ]]; then
    install_postgres
    MONITORING_PRIMARY_SINK="postgres"
    if [[ -z "${MONITORING_POSTGRES_DSN}" ]]; then
      MONITORING_POSTGRES_DSN="postgresql://${MONITORING_POSTGRES_USER}:${MONITORING_POSTGRES_PASSWORD}@${MONITORING_POSTGRES_HOST}:${MONITORING_POSTGRES_PORT}/${MONITORING_POSTGRES_DB}?sslmode=${MONITORING_POSTGRES_SSLMODE}"
    fi
  fi

  if [[ ( "${ENABLE_EVALUATION_WORKER}" == "true" || "${ENABLE_JUDGE_WORKER}" == "true" ) && "${REDIS_HOST}" == "nasa-redis" ]]; then
    log "Provisioning in-cluster Redis: ${REDIS_MANIFEST_PATH}"
    kubectl apply -f "${REDIS_MANIFEST_PATH}" >/dev/null
    log "Waiting for Redis rollout: ${REDIS_DEPLOYMENT_NAME}"
    kubectl rollout status deployment/"${REDIS_DEPLOYMENT_NAME}" -n "${APP_NAMESPACE}" --timeout=180s >/dev/null
  fi

  log "Applying PVC-backed API manifest for full RAG parity: ${API_MANIFEST_PATH}"
  kubectl apply -f "${API_MANIFEST_PATH}" >/dev/null

  log "Waiting for API to become ready before wiring monitoring configuration"
  kubectl rollout status deployment/"${DEPLOYMENT_NAME}" -n "${APP_NAMESPACE}" --timeout=180s >/dev/null

  if [[ "${ENABLE_MONITORING_POSTGRES}" == "true" || "${MONITORING_PRIMARY_SINK}" != "file" ]]; then
    log "Wiring Evidently monitoring sink configuration to API deployment"
    kubectl set env deployment/"${DEPLOYMENT_NAME}" -n "${APP_NAMESPACE}" \
      MONITORING_PRIMARY_SINK="${MONITORING_PRIMARY_SINK}" \
      MONITORING_POSTGRES_DSN="${MONITORING_POSTGRES_DSN}" \
      MONITORING_POSTGRES_HOST="${MONITORING_POSTGRES_HOST}" \
      MONITORING_POSTGRES_PORT="${MONITORING_POSTGRES_PORT}" \
      MONITORING_POSTGRES_DB="${MONITORING_POSTGRES_DB}" \
      MONITORING_POSTGRES_USER="${MONITORING_POSTGRES_USER}" \
      MONITORING_POSTGRES_SSLMODE="${MONITORING_POSTGRES_SSLMODE}" >/dev/null
    log "Waiting for API rollout after monitoring sink env update: ${DEPLOYMENT_NAME}"
    kubectl rollout status deployment/"${DEPLOYMENT_NAME}" -n "${APP_NAMESPACE}" --timeout=180s >/dev/null
  fi

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
  ENABLE_WORKER_RELIABILITY_ALERTS="${ENABLE_WORKER_RELIABILITY_ALERTS}" \
  WORKER_RELIABILITY_RULES_PATH="${WORKER_RELIABILITY_RULES_PATH}" \
  ENABLE_TRACING_PROFILE="${ENABLE_TRACING_PROFILE}" \
  TRACING_PATCH_PATH="${TRACING_PATCH_PATH}" \
  ENABLE_TRACING_VERIFICATION="${ENABLE_TRACING_VERIFICATION}" \
  TRACING_VERIFY_SCRIPT_PATH="${TRACING_VERIFY_SCRIPT_PATH}" \
  "${SETUP_METRICS_SCRIPT_PATH}"

  if [[ "${ENABLE_EVALUATION_WORKER}" == "true" || "${ENABLE_JUDGE_WORKER}" == "true" ]]; then
    log "Enabling Redis-backed broker configuration on API deployment"
    kubectl set env deployment/"${DEPLOYMENT_NAME}" -n "${APP_NAMESPACE}" \
      REDIS_ENABLED="${REDIS_ENABLED}" \
      REDIS_HOST="${REDIS_HOST}" \
      REDIS_PORT="${REDIS_PORT}" \
      REDIS_DB="${REDIS_DB}" \
      EVALUATION_BROKER_STREAM="${EVALUATION_BROKER_STREAM}" \
      EVALUATION_BROKER_GROUP="${EVALUATION_BROKER_GROUP}" \
      JUDGE_BROKER_STREAM="${JUDGE_BROKER_STREAM}" \
      JUDGE_BROKER_GROUP="${JUDGE_BROKER_GROUP}" \
      EVALUATION_BROKER_ENABLED="${ENABLE_EVALUATION_WORKER}" \
      EVALUATION_LOCAL_FALLBACK_ENABLED="$([[ "${ENABLE_EVALUATION_WORKER}" == "true" ]] && printf 'false' || printf 'true')" \
      JUDGE_BROKER_ENABLED="${ENABLE_JUDGE_WORKER}" >/dev/null
    if [[ -n "${REDIS_PASSWORD}" ]]; then
      kubectl set env deployment/"${DEPLOYMENT_NAME}" -n "${APP_NAMESPACE}" REDIS_PASSWORD="${REDIS_PASSWORD}" >/dev/null
    fi

    log "Waiting for API rollout after broker env update: ${DEPLOYMENT_NAME}"
    kubectl rollout status deployment/"${DEPLOYMENT_NAME}" -n "${APP_NAMESPACE}" --timeout=180s >/dev/null
  fi

  if [[ "${ENABLE_EVALUATION_WORKER}" == "true" ]]; then
    log "Applying evaluation worker deployment manifest: ${EVALUATION_WORKER_MANIFEST_PATH}"
    kubectl apply -f "${EVALUATION_WORKER_MANIFEST_PATH}" >/dev/null
    kubectl set env deployment/"${EVALUATION_WORKER_DEPLOYMENT_NAME}" -n "${APP_NAMESPACE}" \
      REDIS_ENABLED="${REDIS_ENABLED}" \
      REDIS_HOST="${REDIS_HOST}" \
      REDIS_PORT="${REDIS_PORT}" \
      REDIS_DB="${REDIS_DB}" \
      EVALUATION_BROKER_STREAM="${EVALUATION_BROKER_STREAM}" \
      EVALUATION_BROKER_GROUP="${EVALUATION_BROKER_GROUP}" >/dev/null
    if [[ "${ENABLE_MONITORING_POSTGRES}" == "true" || "${MONITORING_PRIMARY_SINK}" != "file" ]]; then
      kubectl set env deployment/"${EVALUATION_WORKER_DEPLOYMENT_NAME}" -n "${APP_NAMESPACE}" \
        MONITORING_PRIMARY_SINK="${MONITORING_PRIMARY_SINK}" \
        MONITORING_POSTGRES_DSN="${MONITORING_POSTGRES_DSN}" \
        MONITORING_POSTGRES_HOST="${MONITORING_POSTGRES_HOST}" \
        MONITORING_POSTGRES_PORT="${MONITORING_POSTGRES_PORT}" \
        MONITORING_POSTGRES_DB="${MONITORING_POSTGRES_DB}" \
        MONITORING_POSTGRES_USER="${MONITORING_POSTGRES_USER}" \
        MONITORING_POSTGRES_SSLMODE="${MONITORING_POSTGRES_SSLMODE}" >/dev/null
    fi
    if [[ -n "${REDIS_PASSWORD}" ]]; then
      kubectl set env deployment/"${EVALUATION_WORKER_DEPLOYMENT_NAME}" -n "${APP_NAMESPACE}" REDIS_PASSWORD="${REDIS_PASSWORD}" >/dev/null
    fi

    log "Waiting for evaluation worker rollout: ${EVALUATION_WORKER_DEPLOYMENT_NAME}"
    kubectl rollout status deployment/"${EVALUATION_WORKER_DEPLOYMENT_NAME}" -n "${APP_NAMESPACE}" --timeout=180s >/dev/null

    log "Applying KEDA ScaledObject for evaluation worker auto-scaling: ${KEDA_SCALER_PATH}"
    kubectl apply -f "${KEDA_SCALER_PATH}" >/dev/null
  fi

  if [[ "${ENABLE_JUDGE_WORKER}" == "true" ]]; then
    log "Applying judge worker deployment manifest: ${JUDGE_WORKER_MANIFEST_PATH}"
    kubectl apply -f "${JUDGE_WORKER_MANIFEST_PATH}" >/dev/null
    kubectl set env deployment/"${JUDGE_WORKER_DEPLOYMENT_NAME}" -n "${APP_NAMESPACE}" \
      REDIS_ENABLED="${REDIS_ENABLED}" \
      REDIS_HOST="${REDIS_HOST}" \
      REDIS_PORT="${REDIS_PORT}" \
      REDIS_DB="${REDIS_DB}" \
      JUDGE_BROKER_STREAM="${JUDGE_BROKER_STREAM}" \
      JUDGE_BROKER_GROUP="${JUDGE_BROKER_GROUP}" >/dev/null
    if [[ -n "${REDIS_PASSWORD}" ]]; then
      kubectl set env deployment/"${JUDGE_WORKER_DEPLOYMENT_NAME}" -n "${APP_NAMESPACE}" REDIS_PASSWORD="${REDIS_PASSWORD}" >/dev/null
    fi

    log "Waiting for judge worker rollout: ${JUDGE_WORKER_DEPLOYMENT_NAME}"
    kubectl rollout status deployment/"${JUDGE_WORKER_DEPLOYMENT_NAME}" -n "${APP_NAMESPACE}" --timeout=180s >/dev/null

    log "Applying KEDA ScaledObject for judge worker auto-scaling: ${JUDGE_KEDA_SCALER_PATH}"
    kubectl apply -f "${JUDGE_KEDA_SCALER_PATH}" >/dev/null
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

  if [[ "${ENABLE_EVIDENTLY_DASHBOARD_READINESS_CHECK}" == "true" ]]; then
    log "Running Evidently dashboard readiness gate"
    APP_NAMESPACE="${APP_NAMESPACE}" \
    MONITORING_NAMESPACE="${MONITORING_NAMESPACE}" \
    "${EVIDENTLY_DASHBOARD_READINESS_SCRIPT_PATH}"
  else
    log "Skipping Evidently dashboard readiness gate (ENABLE_EVIDENTLY_DASHBOARD_READINESS_CHECK=${ENABLE_EVIDENTLY_DASHBOARD_READINESS_CHECK})"
  fi

  log "Production parity setup complete (API + Streamlit + HPA + observability stack)."
}

main "$@"
