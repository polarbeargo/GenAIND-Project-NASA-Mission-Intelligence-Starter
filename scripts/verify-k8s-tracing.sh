#!/usr/bin/env bash
set -Eeuo pipefail

APP_NAMESPACE="${APP_NAMESPACE:-default}"
DEPLOYMENT_NAME="${DEPLOYMENT_NAME:-nasa-mission-intelligence-api}"
APP_LABEL_NAME="${APP_LABEL_NAME:-app.kubernetes.io/name}"
APP_LABEL_VALUE="${APP_LABEL_VALUE:-${DEPLOYMENT_NAME}}"

API_LOCAL_PORT="${API_LOCAL_PORT:-18000}"
READY_TIMEOUT_SECONDS="${READY_TIMEOUT_SECONDS:-180}"
SLEEP_SECONDS="${SLEEP_SECONDS:-5}"

HTTP_RETRY_ATTEMPTS="${HTTP_RETRY_ATTEMPTS:-8}"
HTTP_RETRY_DELAY_SECONDS="${HTTP_RETRY_DELAY_SECONDS:-2}"
TRACE_TRIGGER_COUNT="${TRACE_TRIGGER_COUNT:-5}"
REQUIRE_TRACE_EXPORTER="${REQUIRE_TRACE_EXPORTER:-true}"

PF_PIDS=()
PF_LOG_FILES=()

log() {
  printf "[verify-k8s-tracing] %s\n" "$*"
}

die() {
  emit_port_forward_diagnostics
  printf "[verify-k8s-tracing] ERROR: %s\n" "$*" >&2
  exit 1
}

emit_port_forward_diagnostics() {
  local tail_lines="${PF_LOG_TAIL_LINES:-80}"
  for log_file in "${PF_LOG_FILES[@]:-}"; do
    if [[ -n "${log_file}" && -f "${log_file}" ]]; then
      printf "[verify-k8s-tracing] --- port-forward log tail: %s (last %s lines) ---\n" "${log_file}" "${tail_lines}" >&2
      tail -n "${tail_lines}" "${log_file}" >&2 || true
    fi
  done
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Required command not found: $1"
}

cleanup() {
  for pid in "${PF_PIDS[@]:-}"; do
    if [[ -n "${pid}" ]] && kill -0 "${pid}" >/dev/null 2>&1; then
      kill "${pid}" >/dev/null 2>&1 || true
    fi
  done
}
trap cleanup EXIT

wait_for_api_rollout() {
  local deadline=$((SECONDS + READY_TIMEOUT_SECONDS))
  while (( SECONDS < deadline )); do
    if kubectl get deployment "${DEPLOYMENT_NAME}" -n "${APP_NAMESPACE}" >/dev/null 2>&1; then
      if kubectl rollout status deployment/"${DEPLOYMENT_NAME}" -n "${APP_NAMESPACE}" --timeout=15s >/dev/null 2>&1; then
        return 0
      fi
    fi
    sleep "${SLEEP_SECONDS}"
  done
  return 1
}

get_running_pod_name() {
  kubectl get pods -n "${APP_NAMESPACE}" \
    -l "${APP_LABEL_NAME}=${APP_LABEL_VALUE}" \
    --field-selector=status.phase=Running \
    -o jsonpath='{.items[0].metadata.name}'
}

get_pod_env_value() {
  local pod_name="$1"
  local env_name="$2"
  kubectl exec -n "${APP_NAMESPACE}" "${pod_name}" -- python - "$env_name" <<'PY'
import os
import sys

name = sys.argv[1]
print(os.getenv(name, ""))
PY
}

start_port_forward() {
  local namespace="$1"
  local resource="$2"
  local local_port="$3"
  local remote_port="$4"
  local log_file="$5"

  kubectl -n "${namespace}" port-forward "${resource}" "${local_port}:${remote_port}" >"${log_file}" 2>&1 &
  local pf_pid=$!
  PF_PIDS+=("${pf_pid}")
  PF_LOG_FILES+=("${log_file}")

  sleep 1
  if ! kill -0 "${pf_pid}" >/dev/null 2>&1; then
    die "Port-forward failed for ${resource} in namespace ${namespace}. Check ${log_file}"
  fi
}

wait_for_http_ready() {
  local url="$1"
  local attempts="${2:-${HTTP_RETRY_ATTEMPTS}}"
  local delay_seconds="${3:-${HTTP_RETRY_DELAY_SECONDS}}"

  for ((attempt = 1; attempt <= attempts; attempt++)); do
    if curl -fsS --max-time 5 "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep "$delay_seconds"
  done
  return 1
}

check_endpoint_reachable_from_pod() {
  local pod_name="$1"
  local endpoint_url="$2"

  kubectl exec -n "${APP_NAMESPACE}" "${pod_name}" -- python - "$endpoint_url" <<'PY'
import sys
import urllib.request

url = sys.argv[1]
request = urllib.request.Request(url=url, method="GET")
try:
    with urllib.request.urlopen(request, timeout=5) as response:
        code = int(response.getcode())
except Exception:
    sys.exit(2)

# For collectors, 404/405 still confirms endpoint/network is reachable.
if code < 200 or code >= 500:
    sys.exit(2)
PY
}

extract_json_field() {
  local payload="$1"
  local field="$2"
  jq -r "${field} // empty" <<<"${payload}"
}

trigger_traces() {
  local base_url="$1"
  local count="$2"

  for ((i = 1; i <= count; i++)); do
    curl -fsS --max-time 10 "${base_url}/tracing/status" >/dev/null
  done
}

check_exporter_errors_since() {
  local since_time="$1"
  local logs

  logs="$(kubectl logs deployment/"${DEPLOYMENT_NAME}" -n "${APP_NAMESPACE}" --since-time="${since_time}" --tail=400 2>/dev/null || true)"

  if grep -Eqi 'failed to export|exporterror|otlp.*(error|failed)|span.*(error|failed)|connection refused|traceback' <<<"${logs}"; then
    return 1
  fi
  return 0
}

main() {
  require_cmd kubectl
  require_cmd curl
  require_cmd jq

  log "Waiting for API deployment readiness"
  wait_for_api_rollout || die "Deployment ${DEPLOYMENT_NAME} in ${APP_NAMESPACE} did not become ready in time"

  local pod_name
  pod_name="$(get_running_pod_name)"
  [[ -n "${pod_name}" ]] || die "No running pod found for label ${APP_LABEL_NAME}=${APP_LABEL_VALUE}"
  log "Using pod for verification: ${pod_name}"

  local otel_sdk_disabled
  otel_sdk_disabled="$(get_pod_env_value "${pod_name}" "OTEL_SDK_DISABLED" | tr '[:upper:]' '[:lower:]' | xargs)"
  if [[ "${otel_sdk_disabled}" == "true" || "${otel_sdk_disabled}" == "1" || "${otel_sdk_disabled}" == "yes" || "${otel_sdk_disabled}" == "on" ]]; then
    die "Tracing is disabled in the running pod (OTEL_SDK_DISABLED=true). Enable the tracing profile first (ENABLE_TRACING_PROFILE=true)."
  fi

  log "Port-forwarding API deployment for tracing checks"
  start_port_forward "${APP_NAMESPACE}" "deployment/${DEPLOYMENT_NAME}" "${API_LOCAL_PORT}" "8000" "/tmp/nasa-tracing-port-forward.log"

  local base_url="http://127.0.0.1:${API_LOCAL_PORT}"
  wait_for_http_ready "${base_url}/health" || die "API health endpoint did not become reachable via port-forward"

  local status_json
  status_json="$(curl -fsS --max-time 10 "${base_url}/tracing/status")"

  local initialized exporter endpoint fastapi_instrumented requests_instrumented
  initialized="$(extract_json_field "${status_json}" '.initialized')"
  exporter="$(extract_json_field "${status_json}" '.exporter')"
  endpoint="$(extract_json_field "${status_json}" '.endpoint')"
  fastapi_instrumented="$(extract_json_field "${status_json}" '.fastapi_instrumented')"
  requests_instrumented="$(extract_json_field "${status_json}" '.requests_instrumented')"

  [[ "${initialized}" == "true" ]] || die "Tracing is not initialized (.initialized=false)"
  [[ "${fastapi_instrumented}" == "true" ]] || die "FastAPI tracing instrumentation is not active"
  [[ "${requests_instrumented}" == "true" ]] || die "Requests tracing instrumentation is not active"

  if [[ "${REQUIRE_TRACE_EXPORTER}" == "true" ]]; then
    if [[ "${exporter}" == "none" || -z "${exporter}" ]]; then
      die "Tracing exporter is disabled (exporter=${exporter:-unset})"
    fi
  fi

  if [[ "${exporter}" == "phoenix" || "${exporter}" == "otlp" ]]; then
    [[ -n "${endpoint}" ]] || die "Exporter ${exporter} is active but endpoint is empty"
    log "Checking exporter endpoint reachability from pod network"
    check_endpoint_reachable_from_pod "${pod_name}" "${endpoint}" \
      || die "Exporter endpoint is not reachable from pod: ${endpoint}"
  fi

  local marker_time
  marker_time="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  log "Triggering sampled traced requests (${TRACE_TRIGGER_COUNT}x /tracing/status)"
  trigger_traces "${base_url}" "${TRACE_TRIGGER_COUNT}"

  check_exporter_errors_since "${marker_time}" || die "Exporter error signals detected in API logs after trace trigger"

  log "Tracing verification passed: exporter=${exporter}, endpoint=${endpoint:-n/a}, fastapi_instrumented=${fastapi_instrumented}"
}

main "$@"