#!/usr/bin/env bash
set -Eeuo pipefail

APP_NAMESPACE="${APP_NAMESPACE:-default}"
MONITORING_NAMESPACE="${MONITORING_NAMESPACE:-monitoring}"
DEPLOYMENT_NAME="${DEPLOYMENT_NAME:-nasa-mission-intelligence-api}"
HPA_NAME="${HPA_NAME:-nasa-mission-intelligence-api}"
KUBE_PROM_STACK_RELEASE="${KUBE_PROM_STACK_RELEASE:-kube-prometheus-stack}"
PROMETHEUS_SERVICE_NAME="${PROMETHEUS_SERVICE_NAME:-${KUBE_PROM_STACK_RELEASE}-prometheus}"

API_LOCAL_PORT="${API_LOCAL_PORT:-18000}"
PROM_LOCAL_PORT="${PROM_LOCAL_PORT:-19090}"

READY_TIMEOUT_SECONDS="${READY_TIMEOUT_SECONDS:-180}"
SLEEP_SECONDS="${SLEEP_SECONDS:-5}"
KUBECTL_REQUEST_TIMEOUT_SECONDS="${KUBECTL_REQUEST_TIMEOUT_SECONDS:-5}"
HTTP_RETRY_ATTEMPTS="${HTTP_RETRY_ATTEMPTS:-6}"
HTTP_RETRY_DELAY_SECONDS="${HTTP_RETRY_DELAY_SECONDS:-2}"

kubectl_timeout_arg() {
  echo "--request-timeout=${KUBECTL_REQUEST_TIMEOUT_SECONDS}s"
}

METRICS=(
  nasa_worker_pool_queue_depth_ratio
  nasa_worker_pool_oldest_queue_age_seconds
  nasa_worker_pool_rejected_rate
  nasa_worker_pool_error_rate
  nasa_worker_pool_utilization_ratio
  nasa_worker_pool_rejected_total
)

PF_PIDS=()
PF_LOG_FILES=()

log() {
  printf "[smoke-k8s-custom-metrics] %s\n" "$*"
}

die() {
  emit_port_forward_diagnostics
  printf "[smoke-k8s-custom-metrics] ERROR: %s\n" "$*" >&2
  exit 1
}

emit_port_forward_diagnostics() {
  local tail_lines="${PF_LOG_TAIL_LINES:-80}"
  local had_logs=0

  for log_file in "${PF_LOG_FILES[@]:-}"; do
    if [[ -n "${log_file}" && -f "${log_file}" ]]; then
      had_logs=1
      printf "[smoke-k8s-custom-metrics] --- port-forward log tail: %s (last %s lines) ---\n" "${log_file}" "${tail_lines}" >&2
      tail -n "${tail_lines}" "${log_file}" >&2 || true
    fi
  done

  if [[ "${had_logs}" -eq 0 ]]; then
    printf "[smoke-k8s-custom-metrics] No port-forward logs captured yet.\n" >&2
  fi
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

wait_for_api_service() {
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

wait_for_custom_metrics_api() {
  local deadline=$((SECONDS + READY_TIMEOUT_SECONDS))
  while (( SECONDS < deadline )); do
    if kubectl get apiservice v1beta1.custom.metrics.k8s.io -o json "$(kubectl_timeout_arg)" \
      | jq -e '.status.conditions[]? | select(.type=="Available" and .status=="True")' >/dev/null; then
      return 0
    fi
    sleep "${SLEEP_SECONDS}"
  done
  return 1
}

wait_for_metric_payload() {
  local metric_name="$1"
  local deadline=$((SECONDS + READY_TIMEOUT_SECONDS))
  local path="/apis/custom.metrics.k8s.io/v1beta1/namespaces/${APP_NAMESPACE}/pods/*/${metric_name}"

  while (( SECONDS < deadline )); do
    if kubectl get --raw "${path}" "$(kubectl_timeout_arg)" 2>/dev/null \
      | jq -e '(.items // []) | length > 0' >/dev/null; then
      return 0
    fi
    sleep "${SLEEP_SECONDS}"
  done
  return 1
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

curl_json_with_retries() {
  local url="$1"
  local jq_expr="$2"
  local attempts="${3:-${HTTP_RETRY_ATTEMPTS}}"
  local delay_seconds="${4:-${HTTP_RETRY_DELAY_SECONDS}}"

  for ((attempt = 1; attempt <= attempts; attempt++)); do
    if curl -fsS --max-time 10 "$url" | jq -e "$jq_expr" >/dev/null; then
      return 0
    fi
    sleep "$delay_seconds"
  done
  return 1
}

check_api_observability_endpoints() {
  local base_url="http://127.0.0.1:${API_LOCAL_PORT}"

  wait_for_http_ready "${base_url}/monitoring/worker-pools/series" \
    || die "API endpoint did not become reachable via port-forward: ${base_url}"

  log "Checking worker-pool series endpoint"
  curl_json_with_retries "${base_url}/monitoring/worker-pools/series" '.stages and (.stages | length) > 0' \
    || die "Worker-pool series endpoint check failed"

  log "Checking worker-pool timeseries endpoint"
  curl_json_with_retries "${base_url}/monitoring/worker-pools/timeseries?stage=retrieval&window_minutes=60&bucket_seconds=300" '.series != null' \
    || die "Worker-pool timeseries endpoint check failed"

  log "Checking latency SLI timeseries endpoint"
  curl_json_with_retries "${base_url}/monitoring/latency-sli/timeseries?stage=retrieval&window_minutes=60&bucket_seconds=300" '.series != null' \
    || die "Latency SLI timeseries endpoint check failed"
}

check_prometheus_query() {
  local prom_url="http://127.0.0.1:${PROM_LOCAL_PORT}"

  log "Checking Prometheus query for worker-pool utilization"
  curl -fsS "${prom_url}/api/v1/query?query=nasa_worker_pool_utilization_ratio" \
    | jq -e '.status == "success" and (.data.result | type == "array")' >/dev/null
}

wait_for_hpa_current_metrics() {
  local deadline=$((SECONDS + READY_TIMEOUT_SECONDS))
  while (( SECONDS < deadline )); do
    if kubectl get hpa "${HPA_NAME}" -n "${APP_NAMESPACE}" -o json "$(kubectl_timeout_arg)" \
      | jq -e '
        (.status.currentMetrics // []) as $m
        | ($m | length) > 0
        and (
          [
            $m[]
            | if .type == "Pods" then (.pods.current.averageValue // "")
              elif .type == "Resource" then ((.resource.current.averageUtilization // "") | tostring)
              elif .type == "Object" then (.object.current.value // "")
              else ""
              end
          ]
          | all(. != "" and . != "<unknown>")
        )
      ' >/dev/null; then
      return 0
    fi
    sleep "${SLEEP_SECONDS}"
  done
  return 1
}

main() {
  require_cmd kubectl
  require_cmd jq
  require_cmd curl

  log "Waiting for API deployment readiness"
  wait_for_api_service || die "Deployment ${DEPLOYMENT_NAME} in ${APP_NAMESPACE} did not become ready in time"

  log "Waiting for custom metrics API availability"
  wait_for_custom_metrics_api || die "custom.metrics.k8s.io APIService did not become Available=True"

  log "Checking custom metrics payloads"
  for metric in "${METRICS[@]}"; do
    wait_for_metric_payload "${metric}" || die "Custom metric payload empty for ${metric}"
    log "Metric ready: ${metric}"
  done

  log "Checking HPA current metrics"
  wait_for_hpa_current_metrics || die "HPA ${HPA_NAME} does not report non-empty current metrics"

  log "Port-forwarding API deployment for runtime observability checks"
  start_port_forward "${APP_NAMESPACE}" "deployment/${DEPLOYMENT_NAME}" "${API_LOCAL_PORT}" "8000" "/tmp/nasa-api-port-forward.log"
  check_api_observability_endpoints || die "API observability endpoint checks failed"

  if kubectl get svc "${PROMETHEUS_SERVICE_NAME}" -n "${MONITORING_NAMESPACE}" >/dev/null 2>&1; then
    log "Port-forwarding Prometheus service for dashboard query parity checks"
    start_port_forward "${MONITORING_NAMESPACE}" "svc/${PROMETHEUS_SERVICE_NAME}" "${PROM_LOCAL_PORT}" "9090" "/tmp/nasa-prom-port-forward.log"
    check_prometheus_query || die "Prometheus query check failed"
  else
    die "Prometheus service ${PROMETHEUS_SERVICE_NAME} not found in namespace ${MONITORING_NAMESPACE}"
  fi

  log "All custom metrics, HPA, and observability smoke checks passed"
}

main "$@"
