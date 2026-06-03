## Kubernetes Custom Metrics Fast Failure Checklist

1. **Error:** `deployments/scale.apps "nasa-mission-intelligence-api" not found`
   **Fix:** create/apply the API deployment, then re-check.
   ```bash
   kubectl get deploy nasa-mission-intelligence-api -n default
   kubectl apply -f <your-api-deployment-manifest>.yaml
   kubectl rollout status deploy/nasa-mission-intelligence-api -n default
   ```

2. **Error:** `the server could not find the metric nasa_worker_pool_* for pods`
   **Fix:** ensure API metrics endpoint is live, ServiceMonitor is applied, and adapter is upgraded.
   ```bash
   kubectl port-forward deploy/nasa-mission-intelligence-api 8000:8000 -n default
   curl -s http://127.0.0.1:8000/monitoring/worker-pools/prometheus | grep nasa_worker_pool_
   kubectl apply -f deploy/k8s/servicemonitor-worker-pools.yaml
   helm upgrade --install prometheus-adapter prometheus-community/prometheus-adapter \
      --namespace monitoring --create-namespace \
      -f deploy/k8s/prometheus-adapter-values.yaml
   ```

3. **Error:** `kubectl get --raw "/apis/custom.metrics.k8s.io/v1beta1"` returns `"resources": []`
   **Fix:** wait for scrape + adapter sync, then query again.
   ```bash
   kubectl get apiservice v1beta1.custom.metrics.k8s.io
   kubectl get pods -n monitoring -l app.kubernetes.io/name=prometheus-adapter
   kubectl get --raw "/apis/custom.metrics.k8s.io/v1beta1" | jq .
   ```

4. **Error:** `v1beta1.custom.metrics.k8s.io` is not `Available=True`
   **Fix:** inspect and restart adapter deployment.
   ```bash
   kubectl describe apiservice v1beta1.custom.metrics.k8s.io
   kubectl logs -n monitoring deploy/prometheus-adapter --tail=200
   kubectl rollout restart deploy/prometheus-adapter -n monitoring
   ```

5. **Error:** `error: the server doesn't have a resource type "servicemonitors"`
   **Fix:** install Prometheus Operator stack CRDs, then re-apply ServiceMonitor.
   ```bash
   kubectl get crd servicemonitors.monitoring.coreos.com
   helm upgrade --install kube-prometheus-stack prometheus-community/kube-prometheus-stack \
      --namespace monitoring --create-namespace
   kubectl apply -f deploy/k8s/servicemonitor-worker-pools.yaml
   ```

6. **Error:** all HPA metric values show `<unknown>`
   **Fix:** verify scale target exists and custom metrics endpoints return values.
   ```bash
   kubectl get deploy nasa-mission-intelligence-api -n default
   kubectl describe hpa nasa-mission-intelligence-api
   kubectl get --raw "/apis/custom.metrics.k8s.io/v1beta1/namespaces/default/pods/*/nasa_worker_pool_queue_depth_ratio" | jq .
   ```

7. **Error:** `dial tcp 127.0.0.1:<port>: connect: connection refused`
   **Fix:** start your local cluster and confirm context.
   ```bash
   minikube start
   kubectl config current-context
   kubectl cluster-info
   ```

8. **Error:** namespace mismatch (commands use `default` but app is elsewhere)
   **Fix:** replace namespace in all raw metric paths and resource checks.
   ```bash
   kubectl get deploy -A | grep nasa-mission-intelligence-api
   kubectl get pods -n <your-namespace> -l app.kubernetes.io/name=nasa-mission-intelligence-api
   kubectl get --raw "/apis/custom.metrics.k8s.io/v1beta1/namespaces/<your-namespace>/pods/*/nasa_worker_pool_queue_depth_ratio" | jq .
   ```
