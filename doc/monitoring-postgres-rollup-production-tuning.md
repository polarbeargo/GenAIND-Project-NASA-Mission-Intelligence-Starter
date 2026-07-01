# Monitoring Postgres Rollup Production Tuning Profile

Use these ranges to tune read-path freshness vs database load for monitoring rollups.

| Traffic level | Rollup cache TTL | P95 refresh interval | Notes |
|---|---:|---:|---|
| Low traffic | 0.5 to 1.0s | 3 to 5s | Favor fresher metrics. |
| Medium traffic | 1.0 to 2.0s | 5 to 10s | Balanced freshness and DB load. |
| High traffic | 2.0 to 5.0s | 15 to 30s | Prioritize read stability and DB protection. |

## Recommended Starting Points

1. Low: TTL 1.0s, P95 refresh 5.0s
2. Medium: TTL 2.0s, P95 refresh 10.0s
3. High: TTL 3.0s, P95 refresh 20.0s
