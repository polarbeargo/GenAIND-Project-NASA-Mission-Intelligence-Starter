# Evidently Central Sink + Curated Prometheus Metrics

`EvidentlyMonitor` now supports pluggable sink adapters while preserving all existing monitoring endpoints and HTML report generation.

Recommended durable production layout:
- Primary sink: Postgres for cluster-wide read/write consistency and rollups.
- Optional mirrors: OTLP logs and object storage for downstream observability/archive without changing API behavior.
- Fallback/simple mode: shared NDJSON file on PVC when you do not want a database yet.

Install optional sink dependencies with uv when you enable them:

```bash
uv sync --group monitoring-postgres
uv sync --group monitoring-object-storage
uv sync --group monitoring-otlp
```

Environment knobs:
- `MONITORING_PRIMARY_SINK`: `file` or `postgres`.
- `MONITORING_CENTRAL_SINK_PATH`: shared/centralized NDJSON file path (for example a mounted PVC path) when `MONITORING_PRIMARY_SINK=file`.
- `MONITORING_INTERACTIONS_LOG_PATH`: override local/default path when central sink is not set.
- `MONITORING_POSTGRES_DSN`: full Postgres DSN for the durable primary sink.
- `MONITORING_POSTGRES_TABLE`: Postgres table name for interaction events (default `monitoring_interactions`).
- `MONITORING_POSTGRES_HOST`, `MONITORING_POSTGRES_PORT`, `MONITORING_POSTGRES_DB`, `MONITORING_POSTGRES_USER`, `MONITORING_POSTGRES_PASSWORD`, `MONITORING_POSTGRES_SSLMODE`: optional DSN components if you do not set `MONITORING_POSTGRES_DSN`.
- `MONITORING_MIRROR_SINKS`: comma-separated optional mirrors: `otlp`, `s3`, `azure_blob`.
- `MONITORING_OTLP_LOGS_ENDPOINT`: OTLP/HTTP logs endpoint for the OTLP mirror.
- `MONITORING_S3_BUCKET`, `MONITORING_S3_PREFIX`, `MONITORING_S3_ENDPOINT_URL`: S3 archive mirror settings.
- `MONITORING_AZURE_BLOB_CONNECTION_STRING`, `MONITORING_AZURE_BLOB_CONTAINER`, `MONITORING_AZURE_BLOB_PREFIX`: Azure Blob archive mirror settings.
- `MONITORING_WRITE_QUEUE_MAXSIZE`: async write queue capacity (default `5000`).
- `MONITORING_WRITE_BATCH_SIZE`: max batched lines per flush (default `64`).
- `MONITORING_WRITE_FLUSH_SECONDS`: max flush interval seconds (default `0.25`).

Example Postgres-first configuration:

```dotenv
MONITORING_PRIMARY_SINK=postgres
MONITORING_POSTGRES_DSN=postgresql://postgres:postgres@postgres.monitoring.svc.cluster.local:5432/nasa_monitoring?sslmode=prefer
MONITORING_POSTGRES_TABLE=monitoring_interactions
MONITORING_MIRROR_SINKS=otlp
MONITORING_OTLP_LOGS_ENDPOINT=http://otel-collector.monitoring.svc.cluster.local:4318/v1/logs
```

## Centralized Evidently Monitoring in Kubernetes

For cluster-wide consistent analytics and drift reports, enable PostgreSQL as the centralized monitoring sink:

```bash
# Full production parity with Postgres-backed monitoring analytics
ENABLE_MONITORING_POSTGRES=true \
ENABLE_EVALUATION_WORKER=true \
ENABLE_JUDGE_WORKER=true \
ENABLE_KEDA=true \
ENABLE_METRICS_SERVER=true \
ENABLE_WORKER_RELIABILITY_ALERTS=true \
ENABLE_TRACING_PROFILE=true \
./scripts/setup-k8s-production-parity.sh
```

This automatically:
1. Provisions `nasa-postgres` deployment with PVC storage
2. Wires API/workers to use Postgres as primary monitoring sink
3. Maintains fallback file-based behavior if `ENABLE_MONITORING_POSTGRES=false` (default)

Verify Postgres is reachable and ready:

```bash
# Check deployment status
kubectl get deployment nasa-postgres
kubectl logs -l app=nasa-postgres

# Query monitoring interactions table directly
kubectl exec -it svc/nasa-postgres -- psql -U postgres -d nasa_monitoring \
  -c "SELECT COUNT(*) as interactions, MAX(created_at) as latest FROM monitoring_interactions;"

# Test Evidently endpoints still work
curl -s http://127.0.0.1:8000/monitoring/analytics | jq .
curl -s http://127.0.0.1:8000/monitoring/rag | jq .
```

**Non-breaking defaults**: If `ENABLE_MONITORING_POSTGRES=false`, the system uses in-memory file-based monitoring (default). Postgres integration is **additive** and does not affect existing behavior.

## Curated Prometheus Endpoint

- `GET /monitoring/analytics/prometheus`

This endpoint exports a compact metric set for Grafana panels/alerts:
- traffic and reliability: `nasa_monitoring_requests_total`, `nasa_monitoring_errors_total`, `nasa_monitoring_error_rate_percent`
- latency: `nasa_monitoring_latency_avg_ms`, `nasa_monitoring_latency_p95_ms`
- RAG quality: `nasa_monitoring_rag_retrieval_quality_avg`, `nasa_monitoring_rag_faithfulness_avg`, `nasa_monitoring_rag_response_relevancy_avg`, `nasa_monitoring_rag_context_precision_avg`
- sink health: `nasa_monitoring_sink_queue_depth`, `nasa_monitoring_sink_dropped_total`, `nasa_monitoring_sink_write_failures_total`, `nasa_monitoring_mirror_write_failures_total`, `nasa_monitoring_sink_info`
