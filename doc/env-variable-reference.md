# Env Variable Reference (Baseline `.env`)

| Variable | Baseline value | Description |
| --- | --- | --- |
| `OPENAI_API_KEY` | `(set in your local .env)` | OpenAI credential used by generation and embeddings. |
| `PHOENIX_ENDPOINT` | `http://localhost:6006/v1/traces` | Phoenix tracing endpoint. |
| `PHOENIX_PROJECT_NAME` | `nasa-mission-intelligence` | Trace project name in Phoenix. |
| `REDIS_ENABLED` | `true` | Enable Redis integration for cache and async broker paths. |
| `REDIS_HOST` | `localhost` | Redis host for cache and broker connections. |
| `REDIS_PORT` | `6379` | Redis port for cache and broker connections. |
| `REDIS_DB` | `0` | Redis logical database index. |
| `EVALUATION_BROKER_ENABLED` | `true` | Route evaluation jobs to Redis broker workers when available. |
| `JUDGE_BROKER_ENABLED` | `true` | Route judge jobs to Redis broker workers when available. |
| `OTEL_SERVICE_NAME` | `nasa-mission-intelligence` | OpenTelemetry service identifier. |
| `ALLOWED_ORIGINS` | `localhost:3000,localhost:8000` | Comma-separated allowed origins for API CORS. |
| `JUDGE_MODE_DEFAULT` | `async` | Default judge mode for evaluation flow. |
| `JUDGE_TIMEOUT_SECONDS` | `2.5` | Timeout budget for judge task. |
| `EVALUATION_MODE` | `async` | Evaluation execution mode. |
| `PREFLIGHT_RETRIEVAL_MODE` | `strict` | Preflight/retrieval execution mode (`strict` or `fastest`). |
| `EVALUATION_LOCAL_FALLBACK_ENABLED` | `true` | Allow local async evaluation fallback when broker is unavailable or has no active consumers. |
| `RETRIEVAL_TIMEOUT_SECONDS` | `3.5` | Retrieval stage timeout. |
| `GENERATION_TIMEOUT_SECONDS` | `6.5` | Generation stage timeout. |
| `EVALUATION_TIMEOUT_SECONDS` | `3.0` | Evaluation stage timeout. |
| `STAGE_BREAKER_FAILURE_THRESHOLD` | `5` | Stage circuit-breaker failure threshold. |
| `STAGE_BREAKER_RECOVERY_SECONDS` | `30` | Stage circuit-breaker recovery window. |
| `RETRIEVAL_FACTOID_N_RESULTS` | `2` | Retrieval depth for factoid-style queries. |
| `RETRIEVAL_BROAD_N_RESULTS` | `4` | Retrieval depth for broad/exploratory queries. |
| `CONTEXT_MAX_TOKENS` | `2000` | Max context tokens before compression/truncation. |
| `CONTEXT_DEDUP_THRESHOLD` | `0.85` | Similarity threshold for context deduplication. |
| `PREFLIGHT_BUDGET_MS` | `20` | Latency budget for preflight stage. |
| `RETRIEVAL_BUDGET_MS` | `1500` | Latency budget for retrieval stage. |
| `GENERATION_BUDGET_MS` | `5000` | Latency budget for generation stage. |
| `SAFETY_WORKERS` | `3` | Safety stage worker count. |
| `RETRIEVAL_WORKERS` | `8` | Retrieval stage worker count. |
| `GENERATION_WORKERS` | `8` | Generation stage worker count. |
| `JUDGE_WORKERS` | `2` | Judge stage worker count. |
| `EVALUATION_WORKERS` | `2` | Evaluation stage worker count. |
| `SAFETY_QUEUE_LIMIT` | `240` | Safety stage queue capacity. |
| `RETRIEVAL_QUEUE_LIMIT` | `600` | Retrieval stage queue capacity. |
| `GENERATION_QUEUE_LIMIT` | `600` | Generation stage queue capacity. |
| `JUDGE_QUEUE_LIMIT` | `160` | Judge stage queue capacity. |
| `EVALUATION_QUEUE_LIMIT` | `300` | Evaluation stage queue capacity. |
| `STAGE_QUEUE_SUBMIT_TIMEOUT_SECONDS` | `0.05` | Queue submit timeout before backpressure handling. |
| `STAGE_SLI_LOG_FILE` | `./monitoring/stage_latency_events.jsonl` | Stage latency/SLI event log path. |
| `STAGE_SLI_RETENTION_HOURS` | `168` | SLI retention period in hours. |
| `STAGE_SLI_MAX_FILE_BYTES` | `20971520` | Max size per SLI log file before rotation. |
| `STAGE_SLI_MAX_ROTATED_FILES` | `10` | Number of rotated SLI files to keep. |
| `STAGE_SLI_MAINTENANCE_SECONDS` | `60` | SLI maintenance interval for cleanup/rotation. |
