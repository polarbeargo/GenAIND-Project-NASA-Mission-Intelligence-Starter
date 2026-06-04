# Kubernetes Custom Metrics User Manual

This manual documents the step-by-step setup and verification flow for Kubernetes custom metrics used by the API HPA.

## Mandatory Dependencies

- Prometheus stack must be installed in the cluster (Prometheus backend + Operator CRDs).
- API deployment manifest must be applied so `nasa-mission-intelligence-api` exists as the HPA scale target.

## 0) Prerequisites (required before adapter + HPA checks)

```bash
# Minikube/cluster must be running and your kube-context must point to it
kubectl config current-context

# Prometheus stack must exist (install if missing)
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm upgrade --install kube-prometheus-stack prometheus-community/kube-prometheus-stack \
   --namespace monitoring --create-namespace

# API deployment must exist (HPA scale target)
kubectl get deploy nasa-mission-intelligence-api -n default

# Pods should be running
kubectl get pods -n default -l app.kubernetes.io/name=nasa-mission-intelligence-api

# Prometheus Operator CRD must exist for ServiceMonitor
kubectl get crd servicemonitors.monitoring.coreos.com
```

In a separate terminal, expose and verify the raw Prometheus endpoint from the API pod:

```bash
kubectl port-forward deploy/nasa-mission-intelligence-api 8000:8000 -n default
```

```bash
curl -s http://127.0.0.1:8000/monitoring/worker-pools/prometheus | grep nasa_worker_pool_
```

## 1) Apply ServiceMonitor so Prometheus scrapes worker-pool metrics

```bash
kubectl apply -f deploy/k8s/servicemonitor-worker-pools.yaml
```

## 2) Deploy/update Prometheus Adapter with project rules

```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm upgrade --install prometheus-adapter prometheus-community/prometheus-adapter \
   --namespace monitoring --create-namespace \
   -f deploy/k8s/prometheus-adapter-values.yaml
```

## 3) Verify Custom Metrics API is available

```bash
kubectl get apiservice v1beta1.custom.metrics.k8s.io
kubectl get --raw "/apis/custom.metrics.k8s.io/v1beta1" | jq .
```

## 4) Verify all HPA worker-pool metrics are exposed

```bash
for m in \
  nasa_worker_pool_queue_depth_ratio \
  nasa_worker_pool_oldest_queue_age_seconds \
  nasa_worker_pool_rejected_rate \
  nasa_worker_pool_error_rate \
  nasa_worker_pool_utilization_ratio \
  nasa_worker_pool_rejected_total
 do
  echo "===== ${m} ====="
  kubectl get --raw "/apis/custom.metrics.k8s.io/v1beta1/namespaces/default/pods/*/${m}" | jq .
 done
```

## 5) Apply HPA and confirm metrics are being consumed

```bash
kubectl apply -f deploy/k8s/hpa-api-worker-pools.yaml
kubectl describe hpa nasa-mission-intelligence-api
```

If app namespace is not `default`, replace `default` in the verification paths above.

## Troubleshooting

- Fast troubleshooting checklist: [kubernetes-custom-metrics-fast-failure-checklist.md](kubernetes-custom-metrics-fast-failure-checklist.md)
