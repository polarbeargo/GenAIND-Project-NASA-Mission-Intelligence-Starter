# Monitoring Read Latency Benchmark (File vs Postgres Rollup)

Use the benchmark script below to compare the old canonicalized-read path and the new materialized/rollup read path under the same fixed writer profile.

```bash
source .venv/bin/activate && python3 scripts/benchmark_monitoring_read_latency.py --duration-seconds 4 --read-iterations 60 --target-rows 1500 --writer-rate-rps 500 --sink file
```

```bash
kubectl -n default port-forward svc/nasa-postgres 15432:5432
```

```bash
source .venv/bin/activate && export MONITORING_POSTGRES_DSN='postgresql://postgres:postgres@127.0.0.1:15432/nasa_monitoring?sslmode=disable' && export MONITORING_POSTGRES_INCREMENTAL_AGGREGATES=true && export MONITORING_POSTGRES_TABLE='monitoring_interactions_bench_live_ttl' && export MONITORING_POSTGRES_ROLLUP_CACHE_TTL_SECONDS=1.0 && export MONITORING_POSTGRES_P95_REFRESH_SECONDS=5.0 && python3 scripts/benchmark_monitoring_read_latency.py --duration-seconds 4 --read-iterations 60 --target-rows 1500 --writer-rate-rps 500 --sink postgres
```

| Sink | Writes Observed | Old Avg ms | Old P95 ms | New Avg ms | New P95 ms | Avg Speedup |
|---|---:|---:|---:|---:|---:|---:|
| File | 1,500 | 353.6118 | 510.1262 | 14.6803 | 13.8361 | 24.09x |
| Postgres (rollup) | 1,500 | 56.1198 | 109.7236 | 1.7205 | 23.5067 | 32.62x |

![Monitoring Read Latency Benchmark](../images/benchmark_monitoring_read_latency.png)

Production tuning profile moved to: [doc/monitoring-postgres-rollup-production-tuning.md](monitoring-postgres-rollup-production-tuning.md)
