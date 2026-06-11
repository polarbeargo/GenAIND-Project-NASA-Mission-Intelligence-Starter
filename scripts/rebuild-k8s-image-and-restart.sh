#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

APP_NAMESPACE="${APP_NAMESPACE:-default}"
MINIKUBE_PROFILE="${MINIKUBE_PROFILE:-minikube}"
IMAGE_NAME="${IMAGE_NAME:-nasa-mission-intelligence-api:latest}"
API_DEPLOYMENT_NAME="${API_DEPLOYMENT_NAME:-nasa-mission-intelligence-api}"
STREAMLIT_DEPLOYMENT_NAME="${STREAMLIT_DEPLOYMENT_NAME:-nasa-mission-intelligence-streamlit}"
RESTART_API="${RESTART_API:-true}"
RESTART_STREAMLIT="${RESTART_STREAMLIT:-true}"
SKIP_BUILD="${SKIP_BUILD:-false}"
ROLLOUT_TIMEOUT_SECONDS="${ROLLOUT_TIMEOUT_SECONDS:-240}"

log() {
  printf "[rebuild-k8s-image-and-restart] %s\n" "$*"
}

die() {
  printf "[rebuild-k8s-image-and-restart] ERROR: %s\n" "$*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Required command not found: $1"
}

ensure_deployment_exists() {
  local deployment_name="$1"
  kubectl get deployment "${deployment_name}" -n "${APP_NAMESPACE}" >/dev/null 2>&1 || \
    die "Deployment ${deployment_name} not found in namespace ${APP_NAMESPACE}"
}

restart_and_wait() {
  local deployment_name="$1"
  ensure_deployment_exists "${deployment_name}"

  log "Restarting deployment/${deployment_name}"
  kubectl rollout restart deployment/"${deployment_name}" -n "${APP_NAMESPACE}" >/dev/null

  log "Waiting for rollout deployment/${deployment_name}"
  kubectl rollout status deployment/"${deployment_name}" -n "${APP_NAMESPACE}" \
    --timeout="${ROLLOUT_TIMEOUT_SECONDS}s" >/dev/null
}

main() {
  require_cmd kubectl
  require_cmd minikube
  require_cmd docker

  log "Kubernetes context: $(kubectl config current-context)"

  if [[ "${SKIP_BUILD}" != "true" ]]; then
    log "Configuring docker client to minikube profile: ${MINIKUBE_PROFILE}"
    eval "$(minikube -p "${MINIKUBE_PROFILE}" docker-env)"

    log "Building image ${IMAGE_NAME} from ${ROOT_DIR}"
    docker build -t "${IMAGE_NAME}" "${ROOT_DIR}"
  else
    log "SKIP_BUILD=true, skipping docker build"
  fi

  if [[ "${RESTART_API}" == "true" ]]; then
    restart_and_wait "${API_DEPLOYMENT_NAME}"
  else
    log "RESTART_API=false, skipping API restart"
  fi

  if [[ "${RESTART_STREAMLIT}" == "true" ]]; then
    restart_and_wait "${STREAMLIT_DEPLOYMENT_NAME}"
  else
    log "RESTART_STREAMLIT=false, skipping Streamlit restart"
  fi

  log "Done. Current deployment status:"
  kubectl get deployment "${API_DEPLOYMENT_NAME}" "${STREAMLIT_DEPLOYMENT_NAME}" \
    -n "${APP_NAMESPACE}" -o wide || true
}

main "$@"
