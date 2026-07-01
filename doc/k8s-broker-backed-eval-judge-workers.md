# Broker-Backed Evaluation and Judge Workers on Kubernetes

Enable broker-backed async evaluation and judge workers when Redis is available.

Use this page as the canonical quickstart runbook for enablement commands and required flags.
For deep architecture details, scaling internals, and extended troubleshooting playbooks,
see [Kubernetes Evaluation and Judge Worker Setup with KEDA Autoscaling](k8s-evaluation-worker-setup.md).

This moves async evaluation and LLM-as-a-Judge work off the API pod and enables independent scaling/recovery.
The setup automatically installs metrics-server and KEDA if not already present. If you do not provide `REDIS_HOST`,
the automation provisions an in-cluster Redis service (`nasa-redis`) automatically.

```bash
eval "$(minikube docker-env)"
docker build -t nasa-mission-intelligence-api:latest .
ENABLE_EVALUATION_WORKER=true \
ENABLE_JUDGE_WORKER=true \
ENABLE_KEDA=true \
ENABLE_METRICS_SERVER=true \
ENABLE_WORKER_RELIABILITY_ALERTS=true \
./scripts/setup-k8s-production-parity.sh
```

Use an external Redis instead of the built-in in-cluster deployment:

```bash
eval "$(minikube docker-env)"
docker build -t nasa-mission-intelligence-api:latest .
REDIS_ENABLED=true \
REDIS_HOST=<your-redis-host> \
ENABLE_EVALUATION_WORKER=true \
ENABLE_JUDGE_WORKER=true \
ENABLE_KEDA=true \
ENABLE_METRICS_SERVER=true \
ENABLE_WORKER_RELIABILITY_ALERTS=true \
./scripts/setup-k8s-production-parity.sh
```

## What this does

- applies [deploy/k8s/redis-deployment.yaml](../deploy/k8s/redis-deployment.yaml) when `REDIS_HOST` is not provided
- applies [deploy/k8s/evaluation-worker-deployment.yaml](../deploy/k8s/evaluation-worker-deployment.yaml)
- applies [deploy/k8s/judge-worker-deployment.yaml](../deploy/k8s/judge-worker-deployment.yaml)
- applies [deploy/k8s/keda-scaledobject-evaluation-worker.yaml](../deploy/k8s/keda-scaledobject-evaluation-worker.yaml) to scale the worker tier with KEDA's `redis-streams` trigger
- applies [deploy/k8s/keda-scaledobject-judge-worker.yaml](../deploy/k8s/keda-scaledobject-judge-worker.yaml) to scale the judge tier with the same `redis-streams` trigger type
- applies [deploy/k8s/prometheus-rules-worker-reliability.yaml](../deploy/k8s/prometheus-rules-worker-reliability.yaml) when `ENABLE_WORKER_RELIABILITY_ALERTS=true` (default)
- updates the API deployment to `EVALUATION_BROKER_ENABLED=true`
- updates the API deployment to `JUDGE_BROKER_ENABLED=true`
- disables local async evaluation fallback on the API (`EVALUATION_LOCAL_FALLBACK_ENABLED=false`)
- waits for Redis, API, evaluation-worker, and judge-worker rollouts to complete

## Important

- Built-in Redis provisioning is intended for local production-parity and small-cluster testing.
- If you use external Redis, set both `REDIS_ENABLED=true` and `REDIS_HOST`.
- Built-in Redis provisioning does not configure `REDIS_PASSWORD`; use external Redis if password-based auth is required.
- The evaluation autoscaler must use KEDA's `redis-streams` trigger because `eval:jobs` is a Redis Stream created with `XADD`, not a Redis list.
- The judge autoscaler must also use KEDA's `redis-streams` trigger because `judge:jobs` is a Redis Stream created with `XADD`.
- The KEDA trigger must point at the full Redis service FQDN (`nasa-redis.default.svc.cluster.local:6379`) because the KEDA controller runs outside the `default` namespace.
- If the judge broker is enabled but no external consumers are active yet, the API now waits briefly for consumer registration and then falls back to its local bounded judge executor instead of silently black-holing jobs.
- Reliability PrometheusRule automation can be disabled with `ENABLE_WORKER_RELIABILITY_ALERTS=false` for lightweight demo runs.

## Troubleshooting

- If ScaledObject events show `lookup nasa-redis ... no such host`, the trigger is using a short service name instead of the full FQDN.
- If KEDA operator logs show `ERR unsupported key type: stream`, the manifest is using the `redis` list scaler instead of `redis-streams`.

For complete diagnostics (HPA/KEDA status checks, manual setup path, and failure triage),
see [Kubernetes Evaluation and Judge Worker Setup with KEDA Autoscaling](k8s-evaluation-worker-setup.md).
