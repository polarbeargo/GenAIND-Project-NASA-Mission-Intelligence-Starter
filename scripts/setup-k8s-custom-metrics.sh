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
ENABLE_GRAFANA_INFINITY_SETUP="${ENABLE_GRAFANA_INFINITY_SETUP:-true}"
GRAFANA_DEPLOYMENT_NAME="${GRAFANA_DEPLOYMENT_NAME:-${KUBE_PROM_STACK_RELEASE}-grafana}"
GRAFANA_SERVICE_NAME="${GRAFANA_SERVICE_NAME:-${KUBE_PROM_STACK_RELEASE}-grafana}"
GRAFANA_SECRET_NAME="${GRAFANA_SECRET_NAME:-${KUBE_PROM_STACK_RELEASE}-grafana}"
GRAFANA_PORT_FORWARD_LOCAL_PORT="${GRAFANA_PORT_FORWARD_LOCAL_PORT:-33300}"

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

append_csv_unique() {
  local existing="$1"
  local item="$2"

  if [[ -z "${existing}" ]]; then
    printf '%s' "${item}"
    return 0
  fi

  local found="false"
  local token
  IFS=',' read -r -a tokens <<<"${existing}"
  for token in "${tokens[@]}"; do
    token="${token//[[:space:]]/}"
    if [[ "${token}" == "${item}" ]]; then
      found="true"
      break
    fi
  done

  if [[ "${found}" == "true" ]]; then
    printf '%s' "${existing}"
  else
    printf '%s,%s' "${existing}" "${item}"
  fi
}

ensure_infinity_plugin_and_datasource() {
  local grafana_deploy="${GRAFANA_DEPLOYMENT_NAME}"
  local grafana_svc="${GRAFANA_SERVICE_NAME}"
  local grafana_secret="${GRAFANA_SECRET_NAME}"
  local local_port="${GRAFANA_PORT_FORWARD_LOCAL_PORT}"
  local infinity_plugin="yesoreyeram-infinity-datasource"

  if ! kubectl get deployment "${grafana_deploy}" -n "${MONITORING_NAMESPACE}" >/dev/null 2>&1; then
    log "Warn: Grafana deployment ${MONITORING_NAMESPACE}/${grafana_deploy} not found; skipping Infinity provisioning"
    return 0
  fi

  if ! kubectl get service "${grafana_svc}" -n "${MONITORING_NAMESPACE}" >/dev/null 2>&1; then
    log "Warn: Grafana service ${MONITORING_NAMESPACE}/${grafana_svc} not found; skipping Infinity provisioning"
    return 0
  fi

  if ! kubectl get secret "${grafana_secret}" -n "${MONITORING_NAMESPACE}" >/dev/null 2>&1; then
    log "Warn: Grafana secret ${MONITORING_NAMESPACE}/${grafana_secret} not found; skipping Infinity provisioning"
    return 0
  fi

  local current_plugins
  current_plugins="$(kubectl get deployment "${grafana_deploy}" -n "${MONITORING_NAMESPACE}" -o json \
    | jq -r '.spec.template.spec.containers[] | select(.name=="grafana") | (.env // [])[] | select(.name=="GF_INSTALL_PLUGINS") | .value' \
    | head -n 1)"
  current_plugins="${current_plugins:-}"

  local merged_plugins
  merged_plugins="$(append_csv_unique "${current_plugins}" "${infinity_plugin}")"

  if [[ "${merged_plugins}" != "${current_plugins}" ]]; then
    log "Enabling Grafana Infinity plugin on ${grafana_deploy}"
    kubectl set env deployment/"${grafana_deploy}" -n "${MONITORING_NAMESPACE}" GF_INSTALL_PLUGINS="${merged_plugins}" >/dev/null
    log "Waiting for Grafana rollout after plugin update"
    kubectl rollout status deployment/"${grafana_deploy}" -n "${MONITORING_NAMESPACE}" --timeout=300s >/dev/null
  else
    log "Grafana Infinity plugin already configured"
  fi

  local grafana_user
  local grafana_password
  grafana_user="$(kubectl get secret -n "${MONITORING_NAMESPACE}" "${grafana_secret}" -o jsonpath='{.data.admin-user}' | base64 --decode 2>/dev/null || true)"
  grafana_password="$(kubectl get secret -n "${MONITORING_NAMESPACE}" "${grafana_secret}" -o jsonpath='{.data.admin-password}' | base64 --decode 2>/dev/null || true)"

  if [[ -z "${grafana_user}" || -z "${grafana_password}" ]]; then
    log "Warn: Grafana admin credentials are empty in secret ${MONITORING_NAMESPACE}/${grafana_secret}; skipping datasource provisioning"
    return 0
  fi

  log "Checking Grafana Infinity datasource via API"
  local pf_log_file
  local pf_pid=""
  pf_log_file="$(mktemp)"

  cleanup_pf() {
    if [[ -n "${pf_pid}" ]]; then
      kill "${pf_pid}" >/dev/null 2>&1 || true
    fi
    rm -f "${pf_log_file}" >/dev/null 2>&1 || true
  }
  trap cleanup_pf RETURN

  kubectl -n "${MONITORING_NAMESPACE}" port-forward svc/"${grafana_svc}" "${local_port}:80" >"${pf_log_file}" 2>&1 &
  pf_pid="$!"

  local ok="false"
  local attempt
  for attempt in 1 2 3 4 5 6 7 8 9 10; do
    if curl -fsS -u "${grafana_user}:${grafana_password}" "http://127.0.0.1:${local_port}/api/health" >/dev/null 2>&1; then
      ok="true"
      break
    fi
    sleep 1
  done

  if [[ "${ok}" != "true" ]]; then
    log "Warn: Grafana API did not become reachable on localhost:${local_port}; skipping datasource provisioning"
    return 0
  fi

  local infinity_uid
  infinity_uid="$(curl -fsS -u "${grafana_user}:${grafana_password}" "http://127.0.0.1:${local_port}/api/datasources" \
    | jq -r 'map(select(.type=="yesoreyeram-infinity-datasource")) | .[0].uid // empty')"

  if [[ -n "${infinity_uid}" ]]; then
    log "Infinity datasource already present (uid=${infinity_uid})"
    return 0
  fi

  local create_resp
  create_resp="$(curl -fsS -u "${grafana_user}:${grafana_password}" -H 'Content-Type: application/json' \
    -X POST "http://127.0.0.1:${local_port}/api/datasources" \
    -d '{"name":"Infinity","type":"yesoreyeram-infinity-datasource","access":"proxy","isDefault":false}')"

  local created_message
  created_message="$(jq -r '.message // .status // "created"' <<<"${create_resp}")"
  log "Infinity datasource provisioning: ${created_message}"
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

  if [[ "${ENABLE_GRAFANA_INFINITY_SETUP}" == "true" ]]; then
    ensure_infinity_plugin_and_datasource
  else
    log "Skipping Grafana Infinity plugin/datasource setup (ENABLE_GRAFANA_INFINITY_SETUP=${ENABLE_GRAFANA_INFINITY_SETUP})"
  fi

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
