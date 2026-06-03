# NASA RAG Chat Project - NASA Mission Intelligence System 

## Overview

This system is a **multi-agent RAG (Retrieval-Augmented Generation) pipeline** built on FastAPI. It answers questions about NASA mission transcripts (Apollo 11, Apollo 13, Challenger) using ChromaDB for vector retrieval and OpenAI for generation. Security guards, a configurable **JudgeWorker** (`sync|async|off`), observability tracing, and red/blue-team evaluations are first-class components.

## Getting Started

### Prerequisites
- Python 3.8+
- uv
- OpenAI API key


### Installation

**Navigate to the project folder**:
   ```bash
   git clone https://github.com/polarbeargo/Project-NASA-Mission-Intelligence-Starter.git

   cd Project-NASA-Mission-Intelligence-Starter
   ```

**Install dependencies with `uv`**:
   ```bash
   uv sync
   ```

**Activate the virtual environment (optional)**:
   ```bash
   source .venv/bin/activate
   ```


### Environment Profiles

The project uses `.env` as the scalable baseline profile (medium traffic), with two preset overrides:

- `.env`: baseline profile used by default (medium pool/queue sizing)
- `env/.env.small`: lower concurrency and queue limits for local demos or small traffic
- `env/.env.high`: higher concurrency and queue limits for load testing and high traffic

Quick profile switches:

```bash
# Optional: keep a copy of your current .env before switching
cp .env env/.env.backup

# Small traffic
cp env/.env.small .env

# High traffic
cp env/.env.high .env

# Restore previous settings
cp env/.env.backup .env
```

[Env Variable Reference (Baseline `.env`)](doc/env-variable-reference.md).

Production-ready runtime mode matrix for the two new controls:

| Profile | `PREFLIGHT_RETRIEVAL_MODE` | `EVALUATION_LOCAL_FALLBACK_ENABLED` | Recommended when |
| --- | --- | --- | --- |
| Interactive | `fastest` | `true` | Lowest perceived latency for user-facing chat UX. |
| Balanced | `strict` | `true` | Default production baseline for stable latency and reliability. |
| Throughput | `strict` | `false` | Highest sustained QPS with broker workers handling eval asynchronously. |

Profile snippets:

```dotenv
# Interactive
PREFLIGHT_RETRIEVAL_MODE=fastest
EVALUATION_LOCAL_FALLBACK_ENABLED=true

# Balanced
PREFLIGHT_RETRIEVAL_MODE=strict
EVALUATION_LOCAL_FALLBACK_ENABLED=true

# Throughput
PREFLIGHT_RETRIEVAL_MODE=strict
EVALUATION_LOCAL_FALLBACK_ENABLED=false
```

Throughput guardrail: when `EVALUATION_LOCAL_FALLBACK_ENABLED=false`, ensure async evaluation broker consumers are healthy; otherwise jobs can be marked as skipped when the broker is unavailable or has no active consumers.

### **Integration Testing**


   ```bash
   # Run embedding pipeline (configurable CLI)
   uv run python embedding_pipeline.py --data-path ./data_text --chroma-dir ./chroma_db_openai --collection-name nasa_space_missions_text

   # Run quick embedding setup (one-command defaults)
   uv run python setup_embeddings.py

   # Start Phoenix observability server
   uv run python -m phoenix.server.main serve

   # Start NASA FastAPI server
   uv run uvicorn api_server:app --host 0.0.0.0 --port 8000

   # Start async evaluation worker (required when EVALUATION_MODE=async and broker is enabled)
   uv run python evaluation_worker.py

   # Run all unittest test files
   uv run python -m unittest discover -s test -p 'test_*.py' -v
   
   # Run all pytest-based tests 
   uv run pytest test/ -v 2>&1

   # Launch chat interface
   uv run streamlit run chat.py
   ```

   Usage note: run this in a separate terminal alongside the API server so queued evaluation jobs are consumed and `/evaluation/{job_id}` can transition from `pending` to `completed`.

### Embedding with `uv run`

Use one of these approaches depending on your goal:

1. **Quick setup (recommended for first run)**
   ```bash
   uv run python setup_embeddings.py
   ```
   This uses built-in defaults:
   - Data path: `./data_text`
   - Chroma directory: `./chroma_db_openai`
   - Collection: `nasa_space_missions_text`
   - Update mode: incremental

2. **Flexible pipeline CLI (custom paths/options)**
   ```bash
   # Full processing with explicit options
   uv run python embedding_pipeline.py \
     --data-path ./data_text \
     --chroma-dir ./chroma_db_openai \
     --collection-name nasa_space_missions_text \
     --update-mode incremental

   # Mission-scoped incremental run (reliable resume with per-file checkpoints)
   uv run python embedding_pipeline.py \
     --data-path ./data_text \
     --chroma-dir ./chroma_db_openai \
     --collection-name nasa_space_missions_text \
     --missions challenger apollo_13 \
     --update-mode incremental \
     --checkpoint-manifest-each-file

   # Non-incremental fast path (batch existence checks + batch upsert)
   uv run python embedding_pipeline.py \
     --data-path ./data_text \
     --chroma-dir ./chroma_db_openai \
     --collection-name nasa_space_missions_text \
     --missions challenger \
     --update-mode skip \
     --fast-upsert

   # Stats only (no processing)
   uv run python embedding_pipeline.py --stats-only

   # Optional: test a retrieval query after processing
   uv run python embedding_pipeline.py --test-query "apollo 11 landing"
   ```

3. **Targeted mission-only helper (fastest for backfilling missing missions)**
   ```bash
   # Challenger-only incremental backfill with per-file checkpointing
   uv run python ingest_missing_missions.py \
     --missions challenger \
     --data-path ./data_text \
     --chroma-dir ./chroma_db_openai \
     --collection-name nasa_space_missions_text \
     --update-mode incremental \
     --checkpoint-manifest-each-file

   # Multi-mission targeted backfill
   uv run python ingest_missing_missions.py \
     --missions challenger apollo_13 \
     --data-path ./data_text \
     --chroma-dir ./chroma_db_openai \
     --collection-name nasa_space_missions_text \
     --update-mode incremental \
     --checkpoint-manifest-each-file
   ```

4. **Embedding ingestion benchmark (normal vs `fast_upsert`)**

    Script: [benchmarks/benchmark_embedding_fast_upsert.py](benchmarks/benchmark_embedding_fast_upsert.py)

    ```bash
    uv run python benchmarks/benchmark_embedding_fast_upsert.py --mission challenger --runs 3
    ```

After embeddings are ready, launch chat:
```bash
uv run streamlit run chat.py
```

## Data Requirements

See [doc/data-requirements.md](doc/data-requirements.md) for the expected directory structure and supported document types.


## Context Compression Benchmark

`benchmarks/benchmark_context_compression.py` measures the naive baseline dedup path against the optimized (blocked/cached/short-circuit) path side-by-side, with correctness assertions that fail fast if both paths diverge.

```bash
# Quick smoke run: 10 rounds × 2 dataset sizes
uv run python benchmarks/benchmark_context_compression.py --runs 10 --sizes 512,1024 --equivalence once
```

**CLI flags:** [Context Compression Benchmark CLI Flags](doc/context-compression-cli-flags.md)

**Sample output (10 runs × 512 and 1024 chunks):**

![Context Compression Benchmark](images/context_compression_benchmark.png)


> The optimized path (`use_optimized_dedup=True` in `CompressionConfig`) is **gated off by default** in production.
> Enable it only if your dataset shows a consistent speedup above ~1.1× before switching.

## Balanced Production Profile

The hybrid semantic+keyword retrieval has been validated with the **Balanced Production Profile** — a production-ready tuning configuration optimized for 15–20 QPS throughput with ~250–700ms latency.

**Run the Balanced Profile validation:**

```bash
RETRIEVAL_FIRST_PASS_MULTIPLIER=4 RETRIEVAL_FIRST_PASS_MAX_CANDIDATES=24 RETRIEVAL_HYBRID_ENABLED=true RETRIEVAL_KEYWORD_TERM_LIMIT=3 RETRIEVAL_KEYWORD_CANDIDATES_PER_TERM=4 CONTEXT_MAX_TOKENS=2000 CONTEXT_DEDUP_THRESHOLD=0.85 RETRIEVAL_TIMEOUT_SECONDS=1.8 uv run python benchmarks/benchmark_hybrid_retrieval.py
```

```bash
RETRIEVAL_FIRST_PASS_MULTIPLIER=4 \
RETRIEVAL_FIRST_PASS_MAX_CANDIDATES=24 \
RETRIEVAL_HYBRID_ENABLED=true \
RETRIEVAL_KEYWORD_TERM_LIMIT=3 \
RETRIEVAL_KEYWORD_CANDIDATES_PER_TERM=4 \
CONTEXT_MAX_TOKENS=2000 \
CONTEXT_DEDUP_THRESHOLD=0.85 \
RETRIEVAL_TIMEOUT_SECONDS=1.8 \
uv run python -m unittest discover -s test -p 'test_two_stage_retrieval.py' -v
```

![Hybrid Retrieval Benchmark](images/benchmark_hybrid_retrieval.gif)

**Expected Output:**
- ✅ 5/5 tests PASS
- ✅ Sub-millisecond latency (< 0.001s per query)
- ✅ All retrieval, determinism, and fallback tests passing

**What this profile does:**
- Expands semantic candidates by 4× before keyword probing
- Limits keyword term extraction to 3 high-signal terms
- Enforces a 24-document hard cap before deterministic reranking
- Combines lexical overlap (65%) + vector distance (35%) scoring
- Ensures bounded, predictable retrieval performance

**Next: Deploy to Staging**

The Balanced profile is ready for staging deployment:

```bash
# staging/.env
cat HYBRID_RETRIEVAL_TUNING.md > BALANCED_PROFILE.env
# Deploy with those vars
```

Monitor with: `curl http://localhost:8000/monitoring/latency-sli`

**For detailed tuning profiles and tradeoff analysis**, see [HYBRID_RETRIEVAL_TUNING.md](HYBRID_RETRIEVAL_TUNING.md) for High-Throughput and High-Quality profile options.

## Kubernetes Custom Metrics Quick Runbook

If use the provided Kubernetes autoscaling manifests, run this once so HPA can read worker-pool custom metrics end-to-end.

0. **Prerequisites (required before adapter + HPA checks)**
   ```bash
   # Minikube/cluster must be running and your kube-context must point to it
   kubectl config current-context

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

1. **Apply ServiceMonitor so Prometheus scrapes worker-pool metrics**
   ```bash
   kubectl apply -f deploy/k8s/servicemonitor-worker-pools.yaml
   ```

2. **Deploy/update Prometheus Adapter with project rules**
    ```bash
    helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
    helm upgrade --install prometheus-adapter prometheus-community/prometheus-adapter \
       --namespace monitoring --create-namespace \
       -f deploy/k8s/prometheus-adapter-values.yaml
    ```

3. **Verify Custom Metrics API is available**
    ```bash
    kubectl get apiservice v1beta1.custom.metrics.k8s.io
    kubectl get --raw "/apis/custom.metrics.k8s.io/v1beta1" | jq .
    ```

4. **Verify all HPA worker-pool metrics are exposed**
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

5. **Apply HPA and confirm metrics are being consumed**
    ```bash
    kubectl apply -f deploy/k8s/hpa-api-worker-pools.yaml
    kubectl describe hpa nasa-mission-intelligence-api
    ```

If app namespace is not `default`, replace `default` in the verification paths above.

Fast troubleshooting guide: [Kubernetes Custom Metrics Fast Failure Checklist](doc/kubernetes-custom-metrics-fast-failure-checklist.md)

## Latency SLI Usage

1. Start API:
   ```bash
   uv run uvicorn api_server:app --host 0.0.0.0 --port 8000
   ```
2. Start Grafana (Docker with Infinity plugin):
   ```bash
   docker run -d --name nasa-grafana -p 3000:3000 \
     -e GF_SECURITY_ADMIN_USER=admin \
     -e GF_SECURITY_ADMIN_PASSWORD=admin \
     -e GF_INSTALL_PLUGINS=yesoreyeram-infinity-datasource \
     grafana/grafana:latest
   ```
3. Import dashboard:
   - Open `http://127.0.0.1:3000` and sign in with `admin` / `admin`
   - Go to Dashboards -> Import
   - Upload [monitoring/latency_sli_dashboard.json](monitoring/latency_sli_dashboard.json)
   - Map `DS_INFINITY` to your Infinity datasource
4. Verify data endpoint and first chart render:
   ```bash
   curl "http://127.0.0.1:8000/monitoring/latency-sli/timeseries?stage=retrieval&window_minutes=60&bucket_seconds=300"
   ```
   - Confirm `series` is not empty in the curl response.
   - In Grafana, open "NASA Stage Latency SLI" and confirm "Stage Latency (p50/p95)" shows lines.

### Demostration:
![Latency SLI Grafana Dashboard](images/SLI.png)

## Worker Pool Scaling Dashboard

Use this dashboard to monitor queue pressure and utilization trends per worker stage, and correlate those trends with latency SLI over the same time window.

1. Start Prometheus (required for the Prometheus panel and stage auto-discovery variable):
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
2. Import dashboard:
   - Open Grafana at `http://127.0.0.1:3000`
   - Go to Dashboards -> Import
   - Upload [monitoring/worker_pool_scaling_dashboard.json](monitoring/worker_pool_scaling_dashboard.json)
   - Map `DS_INFINITY` to your Infinity datasource
   - Map `DS_PROMETHEUS` to your Prometheus datasource
   - If Grafana runs in Docker, set Prometheus datasource URL to `http://host.docker.internal:9090`
3. Set dashboard variables:
   - `API Base URL`: default `http://host.docker.internal:8000` when Grafana runs in Docker
   - `Stage (Prometheus, empty=all)`: leave empty to view all stages, or choose one stage
   - `Worker Stage`: `safety|retrieval|generation|judge|evaluation`
   - `Latency Stage`: `preflight|retrieval|generation|evaluation`
4. Verify APIs and Prometheus:
   ```bash
   curl "http://127.0.0.1:8000/monitoring/worker-pools/series"
   curl "http://127.0.0.1:8000/monitoring/worker-pools/timeseries?stage=retrieval&window_minutes=60&bucket_seconds=300"
   curl "http://127.0.0.1:9090/api/v1/query?query=nasa_worker_pool_utilization_ratio"
   ```

### Worker-Pool SLI Environment Knobs

- `WORKER_POOL_SLI_LOG_FILE`: path for NDJSON worker-pool snapshots (default `./monitoring/worker_pool_events.jsonl`)
- `WORKER_POOL_SLI_RETENTION_HOURS`: retention horizon in hours (default `168`)
- `WORKER_POOL_SLI_MAX_FILE_BYTES`: rotate threshold in bytes (default `20971520`)
- `WORKER_POOL_SLI_MAX_ROTATED_FILES`: number of rotated files to retain (default `10`)
- `WORKER_POOL_SLI_MAINTENANCE_SECONDS`: prune/rotate maintenance interval (default `60`)
- `WORKER_POOL_SLI_SAMPLE_INTERVAL_SECONDS`: minimum write interval for snapshot persistence (default `10`, set `0` to persist every capture)

### Demonstration:
![Worker Pool Scaling Grafana Dashboard](images/worker_pool.gif)

## Concurrency Design

```mermaid
gantt
    title Single /chat request — cache-miss critical paths (strict vs fastest)
    dateFormat  x
    axisFormat  %L ms

    section Strict mode - safety executor
    SafetyWorker.preflight          : s_strict, 0, 20

    section Strict mode - main thread
    Await preflight                 : m_strict_1, 0, 20
    Answer cache lookup             : m_strict_2, 20, 25
    Submit + await retrieval        : m_strict_3, 25, 625
    Await generation                : m_strict_4, 625, 1425
    SafetyWorker.postflight         : m_strict_5, 1425, 1465
    Judge enqueue / sync dispatch   : m_strict_6, 1465, 1480

    section Strict mode - retrieval executor
    RetrievalWorker (cache miss)    : r_strict, 25, 625

    section Strict mode - generation executor
    AnalysisWorker.generate_answer  : g_strict, 625, 1425

    section Fastest mode - safety executor
    SafetyWorker.preflight          : s_fast, 0, 20

    section Fastest mode - retrieval executor
    RetrievalWorker (prestarted)    : r_fast, 0, 600
    Cancel prestarted retrieval (best effort on preflight block): r_fast_cancel, 20, 35

    section Fastest mode - main thread
    Await preflight                 : m_fast_1, 0, 20
    Blocked return path             : crit, m_fast_blocked, 20, 40
    Answer cache lookup             : m_fast_2, 20, 25
    Await prestarted retrieval      : m_fast_3, 25, 405
    Await generation                : m_fast_4, 405, 1205
    SafetyWorker.postflight         : m_fast_5, 1205, 1245
    Judge enqueue / sync dispatch   : m_fast_6, 1245, 1260

    section Strict mode - downstream branches
    JudgeWorker (sync mode)         : j_strict, 1480, 1740
    Evaluation (sync mode)          : e_strict, 1740, 2140
    Judge broker enqueue (async)    : a_strict_1, 1465, 1480
    Evaluation broker enqueue (async): a_strict_2, 1480, 1495

    section Fastest mode - downstream branches
    JudgeWorker (sync mode)         : j_fast, 1260, 1520
    Evaluation (sync mode)          : e_fast, 1520, 1920
    Judge broker enqueue (async)    : a_fast_1, 1245, 1260
    Evaluation broker enqueue (async): a_fast_2, 1260, 1275
```

This diagram reflects the strict-mode cache-miss critical path (`PREFLIGHT_RETRIEVAL_MODE=strict`).

Preflight/retrieval mode behavior:
- `strict` (default): preflight runs first on the dedicated safety executor. Retrieval is submitted only after preflight passes.
- `fastest`: retrieval is prestarted in parallel with preflight to overlap latency. If preflight blocks, retrieval is canceled/ignored before response return.

Mode tradeoff:
- `strict`: strongest safety/cost posture (no speculative retrieval work before safety pass).
- `fastest`: lowest latency on cache-miss path, with possible speculative retrieval work when preflight later blocks.

Fast-path differences from the cache-miss path above:
- Answer-cache hit skips retrieval, generation, and postflight, and returns the cached answer directly.
- Retrieval breaker-open, timeout, or overload returns a degraded fallback response instead of continuing to generation.

Judge concurrency behavior:
- `judge_mode=sync`: judge runs on the request critical path after postflight.
- `judge_mode=async`: workflow attempts Redis broker enqueue first. If enqueue fails/unavailable, it falls back to the local bounded judge executor. If the local judge queue is saturated, the request returns a non-fatal `source=overload` skipped judge payload. Results are queryable via `/monitoring/judge` and `/judge/last`.
- `judge_mode=off`: judge is skipped.

Evaluation concurrency behavior:
- `EVALUATION_MODE=sync`: evaluation runs on the dedicated evaluation executor and stays on the request critical path.
- `EVALUATION_MODE=async`: workflow records a pending job, attempts Redis broker enqueue first, then applies fallback gating:
    - if `EVALUATION_LOCAL_FALLBACK_ENABLED=true`, local bounded async execution is used when broker enqueue fails or when no broker consumers are active;
    - if `EVALUATION_LOCAL_FALLBACK_ENABLED=false`, the job is marked skipped with explicit source (`broker_unavailable` or `no_consumers`) instead of running locally.
    - if local async queue is saturated, the job is marked skipped with `source=overload`.
- `EVALUATION_MODE=off`: evaluation is skipped.

Shutdown behavior (reliability detail):
- Worker pools use a two-phase shutdown: first stop accepting submissions and soft-drain judge/evaluation pools briefly, then cancel pending async futures. Request-path pools remain fast non-blocking on stop.

---


## References

- Norvig, P. (n.d.). *Inference in Text Understanding*. Computer Science Dept., Evans Hall, University of California, Berkeley, Berkeley, CA 94720. Academia.edu. Retrieved May 30, 2026, from https://www.academia.edu/121229122/Inference_in_text_understanding?email_work_card=view-paper
- Norvig, P. (n.d.). *Remote Agent Experiment*. Computer Science Dept., Evans Hall, University of California, Berkeley, Berkeley, CA 94720. Academia.edu. Retrieved May 30, 2026, from https://www.academia.edu/120976021/Remote_Agent_Experiment?email_work_card=view-paper
- Russell, S., and Norvig, P. (2020). Artificial Intelligence: A Modern Approach (4th US ed., p. 712). Pearson. Retrieved May 30, 2026, from https://aima.cs.berkeley.edu/
- [Udacity Generative AI Nanodegree](https://www.udacity.com/course/generative-ai--nd608)
- [Udacity Cloud Native Application Architecture Nanodegree](https://www.udacity.com/blog/kick-off-your-cloud-native-application-architecture-career-with-the-launch-of-our-latest-nanodegree-program/)
