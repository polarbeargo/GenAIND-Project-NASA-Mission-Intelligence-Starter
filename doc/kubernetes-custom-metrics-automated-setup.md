# Kubernetes Custom Metrics Automated Setup

Use the bootstrap script to install/update Prometheus stack + Adapter, apply canonical API deployment/service + ServiceMonitor + HPA, then run smoke checks.

Default one-command flow:

```bash
./scripts/setup-k8s-custom-metrics.sh
```

By default this applies [deploy/k8s/api-deployment.yaml](../deploy/k8s/api-deployment.yaml).

```bash
# Optional: override with a custom API manifest.
API_MANIFEST_PATH=deploy/k8s/your-api-manifest.yaml ./scripts/setup-k8s-custom-metrics.sh
```

Run smoke checks only (custom metrics API, metric payloads, HPA current metrics, Latency SLI and Worker Pool observability endpoints, Prometheus query parity):

```bash
./scripts/smoke-k8s-custom-metrics.sh
```

Optional overrides:

```bash
APP_NAMESPACE=default \
MONITORING_NAMESPACE=monitoring \
DEPLOYMENT_NAME=nasa-mission-intelligence-api \
HPA_NAME=nasa-mission-intelligence-api \
./scripts/smoke-k8s-custom-metrics.sh
```

## Opt-in tracing profile (Phoenix/OTLP)

Default manifests keep tracing disabled (`OTEL_SDK_DISABLED=true`) to minimize baseline overhead.
Enable tracing only when you intentionally deploy a collector endpoint.

1. Create/update tracing secret with at least one endpoint key:

```bash
kubectl create secret generic nasa-tracing \
  --from-literal=PHOENIX_ENDPOINT="https://<phoenix-host>/v1/traces" \
  --from-literal=OTEL_EXPORTER_OTLP_ENDPOINT="" \
  --dry-run=client -o yaml | kubectl apply -f -
```

1. Run setup with tracing profile enabled:

```bash
ENABLE_TRACING_PROFILE=true ./scripts/setup-k8s-custom-metrics.sh
```

Optional: run tracing verification automatically after the metrics smoke checks:

```bash
ENABLE_TRACING_PROFILE=true ENABLE_TRACING_VERIFICATION=true ./scripts/setup-k8s-custom-metrics.sh
```

Or for full production parity (PVC + seed job + Streamlit + HPA):

```bash
ENABLE_TRACING_PROFILE=true ./scripts/setup-k8s-production-parity.sh
```

Optional with automated tracing verification gate:

```bash
ENABLE_TRACING_PROFILE=true ENABLE_TRACING_VERIFICATION=true ./scripts/setup-k8s-production-parity.sh
```

What the tracing profile applies:

- sets `OTEL_SDK_DISABLED=false` on API pods
- injects endpoint env vars from secret `nasa-tracing`
- sets bounded export tuning (`OTEL_TRACES_SAMPLE_RATE=0.20`, batch queue/export limits)
- keeps embedding vectors hidden from traces (`OTEL_OPENAI_HIDE_EMBEDDING_VECTORS=true`)

Patch manifest: [deploy/k8s/api-tracing-opt-in-patch.yaml](../deploy/k8s/api-tracing-opt-in-patch.yaml)

### Verify tracing end to end after rollout

Run the dedicated smoke verifier:

```bash
./scripts/verify-k8s-tracing.sh
```

What it validates:

- deployment rollout and API health are ready
- effective runtime telemetry config via `/tracing/status`
- instrumentation is active (`fastapi_instrumented`, `requests_instrumented`)
- exporter is enabled (unless `REQUIRE_TRACE_EXPORTER=false`)
- for `phoenix|otlp`, exporter endpoint is reachable from pod network
- sampled traced requests do not produce export error signals in recent API logs

Useful overrides:

```bash
APP_NAMESPACE=default \
DEPLOYMENT_NAME=nasa-mission-intelligence-api \
TRACE_TRIGGER_COUNT=8 \
./scripts/verify-k8s-tracing.sh
```

- User Manual: [doc/kubernetes-custom-metrics-user-manual.md](kubernetes-custom-metrics-user-manual.md)
- Fast troubleshooting guide: [doc/kubernetes-custom-metrics-fast-failure-checklist.md](kubernetes-custom-metrics-fast-failure-checklist.md)

## Full RAG in Kubernetes (PVC-backed Chroma, production pattern)

Use this pattern to run full retrieval behavior in Kubernetes without baking Chroma data into the image. API pods block on a readiness gate until the collection is confirmed present - no manual coordination needed.

### Apply order (first-time setup)

1. Create the PVC and Deployment (pods will block in `init: wait-for-collection` until data is ready):

	```bash
	kubectl apply -f deploy/k8s/api-deployment-chroma-pvc.yaml
	```

2. Run the bootstrap seed Job (one-time per PVC lifecycle; idempotent - safe to re-run):

	```bash
	kubectl apply -f deploy/k8s/chroma-seed-job.yaml
	```

	Watch progress - API pods advance automatically when the Job completes:

	```bash
	kubectl get job nasa-chroma-seed -w
	kubectl get pods -l app.kubernetes.io/name=nasa-mission-intelligence-api -w
	```

3. Verify full RAG retrieval:

	```bash
	kubectl port-forward -n default svc/nasa-mission-intelligence-api 8000:8000
	curl -X POST http://127.0.0.1:8000/chat \
	  -H "Content-Type: application/json" \
	  -d '{"question":"What happened during Apollo 13 oxygen tank failure?"}'
	```

	Expected: answer contains mission context, not a degraded fallback.

	This verifies the Kubernetes API service directly. It does not verify which path the Streamlit UI used.

	Optional: confirm the API pod actually served the request:

	```bash
	kubectl logs deployment/nasa-mission-intelligence-api -n default --since=5m | grep 'POST /chat'
	```

	Expected log line:

	```text
	INFO: ... "POST /chat HTTP/1.1" 200 OK
	```

	If you want to verify the in-cluster Streamlit UI route instead, use the Streamlit section below and confirm the UI debug line shows `Request Route: http://nasa-mission-intelligence-api:8000` rather than `local://legacy`.

### Re-seed after adding new missions

```bash
kubectl delete job nasa-chroma-seed
kubectl apply -f deploy/k8s/chroma-seed-job.yaml
```

The `--update-mode incremental` flag skips unchanged chunks, so only new files are processed.

### Architecture notes

| Component | Role |
|---|---|
| PVC `nasa-chroma-pvc` | Persistent Chroma storage shared across all replicas |
| Job `nasa-chroma-seed` | One-shot idempotent embedding seeder; retries up to 3x on failure |
| initContainer `init-chroma-dirs` | Creates PVC subdirs before any writer or reader starts |
| initContainer `wait-for-collection` | Blocks API container startup until `nasa_space_missions_text` exists (10s poll, 10min timeout) |
| `OTEL_SDK_DISABLED=true` | Suppresses Phoenix OTLP span-export noise when no collector is deployed |

## Streamlit in Kubernetes

Use this when you want Streamlit to run in-cluster and call the API over Kubernetes service DNS.
This removes local laptop networking drift and keeps UI/API runtime parity.

1. Apply Streamlit deployment + service:

	```bash
	kubectl apply -f deploy/k8s/streamlit-deployment.yaml
	kubectl rollout status deployment/nasa-mission-intelligence-streamlit --timeout=180s
	```

2. Access Streamlit locally via port-forward:

	```bash
	kubectl port-forward svc/nasa-mission-intelligence-streamlit 8501:8501
	```

	Open: `http://127.0.0.1:8501`

3. Verify in-cluster API routing from UI debug line:
- Expected route label contains `http://nasa-mission-intelligence-api:8000`
- If fallback appears, inspect Streamlit pod logs:

	```bash
	kubectl logs deployment/nasa-mission-intelligence-streamlit --tail=120
	```

Notes:
- `deploy/k8s/streamlit-deployment.yaml` sets `API_BASE_URL=http://nasa-mission-intelligence-api:8000` explicitly.
- `deploy/k8s/hpa-streamlit.yaml` provides conservative autoscaling (`minReplicas: 1`, `maxReplicas: 4`, CPU+Memory targets) for UI tier stability.
- Existing API integration tests remain unchanged; this deployment is additive and does not modify test contracts.

### Troubleshoot Image Drift

When local integration tests and in-cluster Streamlit show different behavior, first assume image drift (`latest` tag with `IfNotPresent` keeps old node-local image layers).

Rebuild and restart both deployments with one command:

```bash
./scripts/rebuild-k8s-image-and-restart.sh
```

Useful overrides:

```bash
APP_NAMESPACE=default \
MINIKUBE_PROFILE=minikube \
IMAGE_NAME=nasa-mission-intelligence-api:latest \
./scripts/rebuild-k8s-image-and-restart.sh
```

Fast checks:

```bash
kubectl -n default get deploy nasa-mission-intelligence-streamlit -o jsonpath='{.spec.template.spec.containers[0].image}{"\n"}{.spec.template.spec.containers[0].imagePullPolicy}{"\n"}'
kubectl -n default get endpoints nasa-mission-intelligence-streamlit -o wide
kubectl -n default rollout status deploy/nasa-mission-intelligence-streamlit --timeout=180s
```
