# Evidently Monitor Dashboard - RAG Quality & Drift Detection

Real-time visibility into RAG quality metrics, drift detection, and sink health. This dashboard correlates evidently monitoring data with Prometheus metrics for comprehensive observability.

## Key Panels

- **Request Traffic & Error Rate**: Requests/sec, errors/sec trending over 5m window
- **Error Rate Gauge**: Threshold-based alerting (yellow: 5%, red: 10%)
- **RAG Quality Metrics**: Retrieval quality, faithfulness, response relevancy, context precision (drift detector)
- **Latency Distribution**: Average vs P95 latency trends
- **Sink Health**: Queue utilization, dropped records, write failures (primary + mirror sinks)
- **Queue Depth Real-time**: Instant visibility into async write buffer pressure

## Requirements

- Prometheus datasource configured in Grafana
- kube-prometheus-stack ServiceMonitor discovery (automatic) OR standalone Prometheus scraping `/monitoring/analytics/prometheus`

## Import Dashboard

### Option 1 (Kubernetes with kube-prometheus-stack)

```bash
# Full setup automatically includes ServiceMonitor
ENABLE_METRICS_SERVER=true ./scripts/setup-k8s-production-parity.sh

# Then import dashboard
GRAFANA_URL=http://127.0.0.1:33000 GRAFANA_USER="$(kubectl get secret -n monitoring kube-prometheus-stack-grafana -o jsonpath='{.data.admin-user}' | base64 --decode)" GRAFANA_PASSWORD="$(kubectl get secret -n monitoring kube-prometheus-stack-grafana -o jsonpath='{.data.admin-password}' | base64 --decode)" \
  ./scripts/import-grafana-evidently-dashboard.sh
```

### Option 2 (Docker Grafana with Infinity datasource)

```bash
# Start Grafana with Prometheus datasource
docker run -d --name nasa-grafana -p 3000:3000 \
  -e "GF_INSTALL_PLUGINS=grafana-clock-panel,grafana-simple-json-datasource" \
  grafana/grafana:latest

# Configure Prometheus datasource at http://host.docker.internal:9090

# Then import
GRAFANA_URL=http://127.0.0.1:3000 GRAFANA_USER=admin GRAFANA_PASSWORD=admin \
  ./scripts/import-grafana-evidently-dashboard.sh
```

## Manual Import (GUI)

1. In Grafana, go to **Dashboards -> Import**
2. Upload [monitoring/grafana/evidently_monitor_dashboard.json](../monitoring/grafana/evidently_monitor_dashboard.json)
3. Select Prometheus as datasource
4. Click Import

## How to Use

- **Monitor drift trends**: RAG quality panel shows 4-metric trend (should stay > 0.75)
- **Detect anomalies**: Error rate spike or latency jump -> check queue depth and sink failures
- **Investigate failures**: Click on low-quality spike -> `/monitoring/report` HTML artifact for deep dive
- **Optimize**: Correlate queue depth with worker pool dashboard; scale workers if queue utilization > 80%

## Post-setup Readiness Check

Run this after the dashboard is imported to fail fast on missing scrape registration, down targets, invalid datasource bindings, or empty queries:

```bash
./scripts/check-evidently-dashboard-readiness.sh
```

This check is read-only and uses secret-based Grafana authentication plus temporary port-forwards only.

Default behavior during automated setup:

- If the Grafana dashboard has not been imported yet, the readiness check now logs a warning and continues.
- ServiceMonitor discovery, Prometheus target health, datasource discovery, and key `nasa_monitoring_*` query checks still remain required.
- This prevents fresh-clone automated setup from failing only because the dashboard has not been provisioned in Grafana yet.

Strict mode:

```bash
DASHBOARD_BINDING_REQUIRED=true ./scripts/check-evidently-dashboard-readiness.sh
```

Use strict mode when you want setup or CI validation to fail unless the Grafana dashboard is present and correctly bound to the Prometheus datasource.

## Metrics Scraped Every 30 Seconds

```bash
curl http://127.0.0.1:8000/monitoring/analytics/prometheus
```

Outputs Prometheus-format metrics: `nasa_monitoring_*` (traffic, errors, latency, RAG quality, sink health)

![Evidently Monitor Dashboard](../images/evidently.gif)
