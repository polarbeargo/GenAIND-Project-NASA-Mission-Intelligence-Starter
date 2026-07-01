# Kubernetes Custom Metrics Automated Setup

Use the bootstrap script to install/update Prometheus stack + Adapter, apply canonical API deployment/service + ServiceMonitors (worker-pools + security) + HPA, provision Grafana security assets, apply worker reliability PrometheusRule alerts, then run smoke checks.

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

Worker reliability alert rule automation (default enabled):

```bash
ENABLE_WORKER_RELIABILITY_ALERTS=true \
WORKER_RELIABILITY_RULES_PATH=deploy/k8s/prometheus-rules-worker-reliability.yaml \
./scripts/setup-k8s-custom-metrics.sh
```

Disable reliability PrometheusRule apply for lightweight demo runs:

```bash
ENABLE_WORKER_RELIABILITY_ALERTS=false ./scripts/setup-k8s-custom-metrics.sh
```

Disable Grafana security asset provisioning if your Grafana deployment does not use sidecar/provisioning labels:

```bash
ENABLE_SECURITY_GRAFANA_PROVISIONING=false ./scripts/setup-k8s-custom-metrics.sh
```

- User Manual: [doc/kubernetes-custom-metrics-user-manual.md](kubernetes-custom-metrics-user-manual.md)
- Fast troubleshooting guide: [doc/kubernetes-custom-metrics-fast-failure-checklist.md](kubernetes-custom-metrics-fast-failure-checklist.md)

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

### Troubleshoot low or missing traces (temporary high-signal mode)

If traces are sparse during incident diagnosis, temporarily force higher trace volume and faster batch flush on the API deployment:

```bash
kubectl -n default set env deploy/nasa-mission-intelligence-api OTEL_TRACES_SAMPLE_RATE=1.0 OTEL_BSP_SCHEDULE_DELAY_MS=100 \
	&& kubectl -n default rollout status deploy/nasa-mission-intelligence-api --timeout=240s
```

After troubleshooting, restore baseline production tuning:

```bash
kubectl -n default set env deploy/nasa-mission-intelligence-api OTEL_TRACES_SAMPLE_RATE=0.2 OTEL_BSP_SCHEDULE_DELAY_MS=500 \
	&& kubectl -n default rollout status deploy/nasa-mission-intelligence-api --timeout=240s
```

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

## Async Evaluation and Judge Workers with KEDA Auto-scaling

Decouples evaluation and judge jobs from the API request path by running dedicated worker tiers that scale independently on Redis stream backlog depth. This keeps API response latency low under scoring traffic spikes and improves resource utilization.

### Setup

1. Enable async worker brokers and in-cluster Redis provisioning:

```bash
ENABLE_EVALUATION_WORKER=true ENABLE_JUDGE_WORKER=true ./scripts/setup-k8s-production-parity.sh
```

The automation will:
- Create in-cluster Redis deployment + PVC for persistent job state
- Deploy `nasa-evaluation-worker` consuming from `eval:jobs` Redis stream
- Deploy `nasa-judge-worker` consuming from `judge:jobs` Redis stream
- Apply KEDA `ScaledObject` using the `redis-streams` trigger to auto-scale workers from consumer-group pending entries
- Wire broker env vars (`REDIS_HOST`, `REDIS_ENABLED`) into both API and worker deployments
- Enforce `EVALUATION_BROKER_ENABLED=true` and `JUDGE_BROKER_ENABLED=true` on API so async jobs are queued instead of run inline
- Preserve reliability by falling back to the API's local bounded judge executor when the judge broker has no active consumers yet

2. Optional: use external Redis instead of built-in cluster Redis:

```bash
REDIS_ENABLED=true \
REDIS_HOST=<your-redis-host> \
REDIS_PORT=<your-port> \
ENABLE_EVALUATION_WORKER=true ./scripts/setup-k8s-production-parity.sh
ENABLE_JUDGE_WORKER=true ./scripts/setup-k8s-production-parity.sh
```

### Scaling behavior (KEDA ScaledObject)

Manifests:
- [deploy/k8s/keda-scaledobject-evaluation-worker.yaml](../deploy/k8s/keda-scaledobject-evaluation-worker.yaml)
- [deploy/k8s/keda-scaledobject-judge-worker.yaml](../deploy/k8s/keda-scaledobject-judge-worker.yaml)

**Scaling policy:**
- Minimum replicas: 1 (always at least one consumer available)
- Maximum replicas: 10 (prevent cost explosion)
- Target backlog: 5 pending entries per worker (`pendingEntriesCount: "5"`)
- Trigger type: `redis-streams` against the `eval:jobs` stream and `eval-workers` consumer group
- Service addressing: use full Redis service FQDN (`nasa-redis.default.svc.cluster.local:6379`) because the KEDA controller runs in the `keda` namespace
- Scale-up latency: typically ~15-30s (KEDA poll + HPA reconcile + pod startup)
- Scale-down behavior: governed by the generated HPA because `minReplicaCount: 1` keeps the worker tier warm

**Why this works:**
- Redis stream pending-entry backlog is the truest signal of demand for evaluation work (unlike CPU which lags behind load)
- Independent from API/Streamlit HPA, so one tier's spike does not starve another
- CPU fallback trigger (75% utilization) prevents sustained CPU spike from escaping notice even if backlog is low

### Verify worker health and auto-scaling

1. Check worker pod count:

```bash
kubectl get deploy nasa-evaluation-worker -w
```

Watch the replica count climb as you submit more chat requests. Each evaluation job adds to the `eval:jobs` stream backlog.

2. Check KEDA metrics:

```bash
kubectl get scaledobject nasa-evaluation-worker-scaler
kubectl describe scaledobject nasa-evaluation-worker-scaler
kubectl get hpa keda-hpa-nasa-evaluation-worker-scaler
```

Expected output shows `READY=True`, `ACTIVE=True` under the ScaledObject and a current/target value such as `10/5` on `keda-hpa-nasa-evaluation-worker-scaler`.

3. Monitor stream backlog directly:

```bash
kubectl exec -it svc/nasa-redis -- redis-cli XLEN eval:jobs
kubectl exec -it svc/nasa-redis -- redis-cli XINFO STREAM eval:jobs
kubectl exec -it svc/nasa-redis -- redis-cli XPENDING eval:jobs eval-workers
```

Expected: `XLEN` shows total stream length, while `XPENDING` shows the consumer-group backlog that drives KEDA scaling.

4. Check worker processing logs:

```bash
kubectl logs -l app.kubernetes.io/component=evaluation-worker --tail=50 -f
```

Expected log lines show job consumption, retry attempts, and dead-letter routing.

### Failure modes and recovery

| Scenario | Behavior | Recovery |
|----------|----------|----------|
| Redis unavailable | API falls back to in-process evaluation; worker deployment blocks | Restore Redis health; workers resume automatically |
| Worker pod crash | Job sits in `eval:jobs` PEL (pending entry list); XAUTOCLAIM reclaims after 5 min idle | Worker restarts and reclaims stale jobs; no manual intervention needed |
| Stuck evaluation job | Retries up to 3× with exponential backoff; sent to `eval:jobs:dlq` if max retries exceeded | Inspect DLQ with `redis-cli XRANGE eval:jobs:dlq - +`; fix root cause; resubmit manually |
| KEDA unavailable | Workers stay at last known replica count; backlog may accumulate | Install/restore KEDA; scale manually with `kubectl scale deploy/nasa-evaluation-worker --replicas=N` |

### KEDA troubleshooting

| Symptom | Cause | Fix |
|----------|-------|-----|
| `lookup nasa-redis ... no such host` in ScaledObject events | KEDA resolves service names from the `keda` namespace, not `default` | Use the full Redis service FQDN: `nasa-redis.default.svc.cluster.local:6379` |
| `ERR unsupported key type: stream` in KEDA operator logs | Manifest used the `redis` scaler, which reads Redis lists via `LLEN`, but `eval:jobs` is a Redis Stream | Use `type: redis-streams` with `stream`, `consumerGroup`, and `pendingEntriesCount` |
| `PollingInterval is configured but is not relevant` or `CooldownPeriod is configured but is not relevant` | These settings are only meaningful for scale-to-zero (`minReplicaCount: 0`) | Remove those fields when keeping `minReplicaCount: 1` |

### Tuning for your workload

Edit the KEDA `ScaledObject` to change scaling parameters:

```bash
kubectl edit scaledobject nasa-evaluation-worker-scaler
```

Key fields:
- `pendingEntriesCount: "5"` — pending jobs per worker target; increase for higher utilization, decrease for lower latency
- `maxReplicaCount: 10` — absolute cap; increase if you expect high sustained load
- `consumerGroup: "eval-workers"` — must match the worker deployment consumer group exactly

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
