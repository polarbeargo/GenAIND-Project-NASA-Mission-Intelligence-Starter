# Worker Pool Scaling Dashboard

Use this dashboard to monitor queue pressure and utilization trends per worker stage, and correlate those trends with latency SLI over the same time window.

1. Start Docker Prometheus (required for NASA worker pool metrics):
   ```bash
   cat >/tmp/nasa-prometheus.yml <<'EOF'
   global:
     scrape_interval: 5s

   scrape_configs:
     - job_name: nasa-api
       metrics_path: /monitoring/worker-pools/prometheus
       static_configs:
         - targets: ["host.docker.internal:8000"]
   EOF

   docker run -d --name nasa-prometheus -p 9090:9090 \
     -v /tmp/nasa-prometheus.yml:/etc/prometheus/prometheus.yml \
     prom/prometheus:latest
   ```

2. *(Kubernetes only)* Port-forward in-cluster Prometheus for async worker metrics (KEDA/HPA panels):
   ```bash
   # Run in a separate terminal to keep port-forward alive
   kubectl -n monitoring port-forward svc/kube-prometheus-stack-prometheus 39090:9090
   ```
   This exposes in-cluster Prometheus on `http://127.0.0.1:39090` for panels 8-9 (async Redis stream backlog and HPA replica metrics).
   If panels 8-9 show no data, first confirm this command is still running and restart it if the port-forward session was closed.

3. Import dashboard:
   - Open Grafana at `http://127.0.0.1:3000` (Docker), or for in-cluster Grafana port-forward it first:
     ```bash
     # Run in a separate terminal to keep port-forward alive
     kubectl -n monitoring port-forward svc/kube-prometheus-stack-grafana 33000:80
     ```
     then open `http://127.0.0.1:33000`
   - Go to Dashboards -> Import
   - Upload [monitoring/worker_pool_scaling_dashboard.json](../monitoring/worker_pool_scaling_dashboard.json)
   - Datasources will be auto-mapped if already configured; otherwise:
     - Map `DS_INFINITY` to your Infinity datasource
     - Map `DS_PROMETHEUS` to your Prometheus datasource (Docker Prometheus at `http://host.docker.internal:9090`)
   - *(Kubernetes only)* Ensure in-cluster Prometheus datasource exists at `http://host.docker.internal:39090` (created automatically if missing)

    Pull-safe one-command import (auto-detects Infinity + Prometheus datasource UIDs, binds `api_base_url`, and verifies endpoint + Prometheus queries):

    ```bash
    GRAFANA_URL=http://127.0.0.1:3000 GRAFANA_USER=admin GRAFANA_PASSWORD=admin API_BASE_URL=http://127.0.0.1:8000 ./scripts/import-grafana-worker-pool-dashboard.sh
    ```

    For in-cluster Grafana, bind the dashboard to the Kubernetes service DNS name and keep local verification on the port-forward:

    ```bash
    GRAFANA_URL=http://127.0.0.1:33000 API_BASE_URL=http://nasa-mission-intelligence-api.default.svc.cluster.local:8000 VERIFY_API_BASE_URL=http://127.0.0.1:18000 ./scripts/import-grafana-worker-pool-dashboard.sh
    ```

4. Set dashboard variables:
  - `API Base URL`: use `http://host.docker.internal:8000` when Grafana runs in Docker, or `http://nasa-mission-intelligence-api.default.svc.cluster.local:8000` for in-cluster Grafana
   - `Stage`: select specific stage or "All" to view all stages
   - `Worker Stage`: `safety|retrieval|generation|judge|evaluation`
   - `Latency Stage`: `preflight|retrieval|generation|evaluation`

5. Verify APIs and Prometheus:
   ```bash
   # Check worker pool data (panels 1-6)
   curl "http://127.0.0.1:8000/monitoring/worker-pools/series"
   curl "http://127.0.0.1:8000/monitoring/worker-pools/timeseries?stage=retrieval&window_minutes=60&bucket_seconds=300"
   
   # Check NASA metrics in Docker Prometheus (panel 7)
   curl "http://127.0.0.1:9090/api/v1/query?query=nasa_worker_pool_utilization_ratio"
   
   # Check KEDA metrics in in-cluster Prometheus (panels 8-9, Kubernetes only)
   curl "http://127.0.0.1:39090/api/v1/query?query=kube_horizontalpodautoscaler_status_current_replicas"

   # Check reliability metrics (panels 10-12)
   curl "http://127.0.0.1:9090/api/v1/query?query=nasa_async_worker_retry_total"
   curl "http://127.0.0.1:9090/api/v1/query?query=nasa_async_worker_dlq_total"
   curl "http://127.0.0.1:9090/api/v1/query?query=nasa_async_worker_reclaim_total"
   curl "http://127.0.0.1:9090/api/v1/query?query=nasa_async_worker_lock_acquire_fail_total"
   ```

6. *(Kubernetes optional)* Apply sample reliability alerts:
   ```bash
   kubectl apply -f deploy/k8s/prometheus-rules-worker-reliability.yaml
   ```
   The sample rule set alerts on high retry rate, any DLQ activity, elevated stale reclaim age/rate, and sustained lock-acquire failures.

## Worker-Pool SLI Environment Knobs

- `WORKER_POOL_SLI_LOG_FILE`: path for NDJSON worker-pool snapshots (default `./monitoring/worker_pool_events.jsonl`)
- `WORKER_POOL_SLI_RETENTION_HOURS`: retention horizon in hours (default `168`)
- `WORKER_POOL_SLI_MAX_FILE_BYTES`: rotate threshold in bytes (default `20971520`)
- `WORKER_POOL_SLI_MAX_ROTATED_FILES`: number of rotated files to retain (default `10`)
- `WORKER_POOL_SLI_MAINTENANCE_SECONDS`: prune/rotate maintenance interval (default `60`)
- `WORKER_POOL_SLI_SAMPLE_INTERVAL_SECONDS`: minimum write interval for snapshot persistence (default `10`, set `0` to persist every capture)

## Demonstration

![Worker Pool Scaling Grafana Dashboard](../images/worker_pool.gif)
