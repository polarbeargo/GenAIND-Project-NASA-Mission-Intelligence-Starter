# NASA RAG Chat Project - NASA Mission Intelligence System 

## Overview

This system is a **multi-agent RAG (Retrieval-Augmented Generation) pipeline** built on FastAPI. It answers questions about NASA mission transcripts (Apollo 11, Apollo 13, Challenger) using ChromaDB for vector retrieval and OpenAI for generation. Security guards, a configurable **JudgeWorker** and **EvaluationWorker** (each with `sync|async|off` modes), observability tracing, and red/blue-team evaluations are first-class components.

---

[Evidently Monitor Dashboard - RAG Quality & Drift Detection](doc/evidently-monitor-dashboard.md)  

![Evidently Monitor Dashboard](images/evidently.gif)

[NASA Security Runtime Dashboard](doc/security-observability.md) you can use promptfoo to generate security events for testing the dashboard.
![Security Metrics Grafana Dashboard](images/NASA_Security_Runtime_Dashboard.gif)
[NASA Stage Latency SLI Dashboard](doc/latency-sli-usage.md)
![Latency SLI Grafana Dashboard](images/SLI.png)
[NASA Worker Pool Scaling Dashboard](doc/worker-pool-scaling-dashboard.md)
![Worker Pool Scaling Grafana Dashboard](images/worker_pool.gif)
[NASA Phoenix Tracing Dashboard](doc/kubernetes-custom-metrics-automated-setup.md#opt-in-tracing-profile-phoenixotlp)
![Phoenix Tracing Dashboard](images/Phoenix.gif)
[Context Compression Benchmark](doc/context-compression-benchmark.md)
![Context Compression Benchmark](images/context_compression_benchmark.png)
[Balanced Production Profile hybrid semantic+keyword first-pass with deterministic rerank](doc/balanced-production-profile.md)
![Hybrid Retrieval Benchmark](images/benchmark_hybrid_retrieval.gif)
[Batch API Evaluation (RAGAS Async Job Flow)](doc/batch-evaluation-results.md)
![Batch Evaluation Results](images/batch_evaluation.gif)
[Evidently Monitoring Read Latency Benchmark (File vs Postgres Rollup)](doc/monitoring-read-latency-benchmark.md)
![Monitoring Read Latency Benchmark](images/benchmark_monitoring_read_latency.png)
| Sink | Writes Observed | Old Avg ms | Old P95 ms | New Avg ms | New P95 ms | Avg Speedup |
|---|---:|---:|---:|---:|---:|---:|
| File | 1,500 | 353.6118 | 510.1262 | 14.6803 | 13.8361 | 24.09x |
| Postgres (rollup) | 1,500 | 56.1198 | 109.7236 | 1.7205 | 23.5067 | 32.62x |

[Full production parity with Postgres-backed monitoring analytics](https://github.com/polarbeargo/GenAIND-Project-NASA-Mission-Intelligence-Starter/tree/main#kubernetes-runbooks)
![Production parity with async workers](images/all.gif)

## Getting Started

### Prerequisites
- Python 3.10+
- uv
- OpenAI-compatible API key in one of these env vars: `OPENAI_API_KEY` (preferred) or `CHROMA_OPENAI_API_KEY` (fallback)

### Kubernetes Prerequisites
- Docker
- kubectl
- minikube
- helm


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

## Kubernetes Runbooks

Use the provided Kubernetes runbooks when running a full production-like cluster setup with worker-pool custom metrics end to end.

Runbooks:

- [Automated custom-metrics setup](doc/kubernetes-custom-metrics-automated-setup.md)
- [Opt-in tracing profile (Phoenix/OTLP)](doc/kubernetes-custom-metrics-automated-setup.md#opt-in-tracing-profile-phoenixotlp)
- [Production parity setup (API + Streamlit + HPA)](doc/kubernetes-custom-metrics-automated-setup.md#automated-setup-production-parity-api--streamlit--hpa)
- [Evidently Central Sink + Curated Prometheus Metrics](doc/evidently-central-sink-prometheus-metrics.md)
- [Full RAG in Kubernetes (PVC-backed Chroma)](doc/kubernetes-custom-metrics-automated-setup.md#full-rag-in-kubernetes-pvc-backed-chroma-production-pattern)
- [Async Evaluation Worker with KEDA Auto-scaling](doc/k8s-evaluation-worker-setup.md)
- [Broker-backed Evaluation and Judge Workers](doc/k8s-broker-backed-eval-judge-workers.md)
- [Streamlit in Kubernetes](doc/kubernetes-custom-metrics-automated-setup.md#streamlit-in-kubernetes)
- [Troubleshoot Image Drift](doc/kubernetes-custom-metrics-automated-setup.md#troubleshoot-image-drift)

Quick start: build the local Minikube image first, then run full production parity setup. After local code changes, use `rebuild-k8s-image-and-restart.sh` to avoid image drift.

```bash
minikube -p minikube start

eval "$(minikube docker-env)"
docker build -t nasa-mission-intelligence-api:latest .

# Full production parity with Postgres-backed monitoring analytics
ROLLOUT_TIMEOUT_SECONDS=600 \
DASHBOARD_BINDING_REQUIRED=true \
ENABLE_MONITORING_POSTGRES=true \
ENABLE_EVALUATION_WORKER=true \
ENABLE_JUDGE_WORKER=true \
ENABLE_KEDA=true \
ENABLE_METRICS_SERVER=true \
ENABLE_WORKER_RELIABILITY_ALERTS=true \
ENABLE_TRACING_PROFILE=true \
./scripts/setup-k8s-production-parity.sh
```

![Rebuild and restart flow](images/rebuilt-restart.png)

![Production parity with async workers](images/production_parity.gif)

Import all Grafana dashboards and alert rules with one command:

```bash
bash ./scripts/run-grafana-imports.sh
```

Choose the Grafana target explicitly when needed:

```bash
GRAFANA_TARGET=k8s bash ./scripts/run-grafana-imports.sh
```

- `GRAFANA_TARGET=local` defaults to `http://127.0.0.1:3000` and `http://127.0.0.1:8000`
- `GRAFANA_TARGET=k8s` defaults to `http://127.0.0.1:33000` and binds dashboards to the in-cluster API service
- `GRAFANA_URL`, `API_BASE_URL`, `VERIFY_API_BASE_URL`, and `AUTO_PORT_FORWARD` still override the target defaults when you need a custom setup

Optional: run Phoenix in a separate terminal to inspect traces locally.

```bash
uv run python -m phoenix.server.main serve
```

Then port-forward the following services to access the API, Streamlit, Prometheus, and Grafana dashboards:

```
kubectl port-forward -n default svc/nasa-mission-intelligence-streamlit 8501:8501

kubectl port-forward deploy/nasa-mission-intelligence-api 8000:8000 -n default 

kubectl -n monitoring port-forward svc/kube-prometheus-stack-prometheus 39090:9090

kubectl -n monitoring port-forward svc/kube-prometheus-stack-grafana 33000:80
```

- Open each dashboard at http://127.0.0.1:33000/dashboards and set API Base URL to:

    `http://nasa-mission-intelligence-api.default.svc.cluster.local:8000`

### **Integration Testing**

See [Integration Testing Runbook](doc/integration-testing.md).

### **How to Operate NASA Intelligence Chat System**

![Test Chat](images/test.gif)

## Security Model (OWASP LLM Top 10 Mapping)

| OWASP LLM Risk | Guard | Stage |
|---|---|---|
| LLM01 Prompt Injection | `PromptInjectionDetector` | Preflight + vector doc scan |
| LLM02 Sensitive Information Disclosure | `SensitiveInfoFilter` | Postflight |
| LLM05 Improper Output Handling | `OutputValidator` | Postflight |
| LLM07 System Prompt Leakage | Jailbreak deny-list + strict output filtering | Preflight + postflight |
| LLM08 Vector and Embedding Weaknesses | `VectorSecurityValidator` | Preflight |
| LLM10 Unbounded Consumption | `ResourceLimitEnforcer` + Redis sliding-window API limiter | Preflight + middleware |
| Cross-cutting audit telemetry (non-risk-specific) | `SecurityEventSink` (`DashboardSecurityEventSink`, `LoggerSecurityEventSink`) | Both stages |

Jailbreak keyword detection (hardcoded deny-list) runs before any external library call, ensuring zero-cost early exit on obvious attacks.

## Evaluation Strategy

### Blue-team (correctness, groundedness, policy)
Config: [`promptfoo/blueTeam.yaml`](promptfoo/blueTeam.yaml) — `promptfooconfig.yaml`

Tests include:
- Factual questions with `contains-any` assertions on expected keywords
- Injection probes asserting refusal patterns in the response
- Grounded answers verified against known mission facts

Run:
```bash
npx promptfoo eval --config promptfoo/blueTeam.yaml
```
![Blue-team evaluation results](images/blueTeam.gif)

### Red-team (adversarial auto-generation)
Config: [`promptfoo/redteam.yaml`](promptfoo/redteam.yaml) — `promptfoo-redteam.local.yaml`

Plugins: `indirect-prompt-injection`, `prompt-extraction`, `rag-document-exfiltration`, `rag-poisoning`, `rag-source-attribution`, `pii:direct`, `pii:social`, `system-prompt-override`

Run:
```bash
npx promptfoo redteam run --config promptfoo/redteam.yaml
```
![Red-team evaluation results](images/redTeam.gif)

## System Architecture

This project follows a strong combination of architecture and object-oriented patterns, including broker-first async control lanes and stage-isolated scaling controls:

### High-Level Architecture Overview

```mermaid
flowchart LR
    subgraph Clients[Clients]
        UI[Streamlit UI]
        APIClient[REST and promptfoo]
    end

    subgraph Service[NASA Mission Intelligence API Service]
        FastAPI[FastAPI endpoints]
        Workflow[MultiAgentChatWorkflow orchestrator]
        StagePools[Stage-isolated bounded pools\nsafety retrieval generation judge evaluation]
        L1Cache[L1 in-process caches\nretrieval and answer]
        Security[OWASP guardrail stack\npreflight and postflight]
        Obs[In-process observability\ntracing security and SLI emitters]
    end

    subgraph DataPlane[Data and Model Plane]
        Chroma[ChromaDB retrieval store]
        OpenAI[OpenAI chat and embedding APIs]
        Monitor[Evidently monitoring sink\nanalytics and curated metrics]
    end

    subgraph ControlPlane[Redis Control Plane]
        RedisCache[Redis L2 cache]
        RedisJobs[Redis async job store]
        JudgeStream[judge:jobs stream]
        EvalStream[eval:jobs stream]
    end

    subgraph AsyncWorkers[Broker-backed worker deployments]
        JudgeWorker[judge_worker deployment\nretry DLQ idempotency]
        EvalWorker[evaluation_worker deployment\nretry DLQ idempotency]
    end

    subgraph Ops[Scalability and Operations]
        WorkerMetrics[worker-pools and latency SLI\nJSON and Prometheus endpoints]
        HPA[HPA for API tier]
        KEDA[KEDA-scaled async workers\nbacklog-driven scaling]
    end

    UI --> FastAPI
    APIClient --> FastAPI
    FastAPI --> Workflow
    Workflow --> StagePools
    Workflow --> L1Cache
    Workflow --> Security
    Workflow --> Chroma
    Workflow --> OpenAI
    Workflow --> Monitor
    Workflow --> RedisCache
    Workflow --> RedisJobs

    Workflow --> JudgeStream
    Workflow --> EvalStream
    JudgeStream --> JudgeWorker
    EvalStream --> EvalWorker
    JudgeWorker --> RedisJobs
    EvalWorker --> RedisJobs
    RedisJobs --> FastAPI

    Workflow --> Obs
    Obs --> WorkerMetrics
    WorkerMetrics --> HPA
    WorkerMetrics --> KEDA
```

### High-Level Layer Diagram

```mermaid
flowchart TD
    subgraph Client["Client Layer"]
        UI["Streamlit Chat UI\n(chat.py)"]
        API_CALL["REST Client\n(curl / promptfoo)"]
    end

    subgraph API["API Layer — api_server.py (FastAPI)"]
        CHAT["/chat endpoint"]
        HEALTH["/health"]
        MONITOR["/monitoring/report\n/monitoring/analytics\n/monitoring/analytics/prometheus"]
        JMON["/monitoring/judge"]
        JLAST["/judge/last"]
        EMON["/monitoring/evaluation\n/evaluation/{job_id}"]
        WMON["/monitoring/worker-pools\n/monitoring/worker-pools/prometheus"]
        CMON["/monitoring/client-caches"]
    end

    subgraph Orchestrator["Orchestrator — multi_agent/workflow.py"]
        EXECUTOR["Stage-isolated bounded pools\n(safety/retrieval/generation/judge/eval)\npreflight_retrieval_mode strict|fastest\ntimeouts + circuit breakers"]
        L1C["L1 caches\n(retrieval + answer, LRU+TTL)"]
    end

    subgraph Workers["In-Process Stage Workers"]
        SW_PRE["SafetyWorker.preflight\nmulti_agent/workers.py\n─────────────────\n① Jailbreak keyword check\n② Token + rate limit\n③ Prompt injection detection\n④ Vector doc validation"]
        RW["RetrievalWorker\nmulti_agent/workers.py\n─────────────────\nChromaDB vector search\nrag_client.retrieve_documents\nrag_client.format_context"]
        AW["AnalysisWorker.generate_answer\nmulti_agent/workers.py\n─────────────────\nllm_client.generate_response\n(OpenAI Chat Completion)"]
        SW_POST["SafetyWorker.postflight\n─────────────────\n⑤ Output validation\n⑥ Sensitive info filter\n⑦ Security audit log"]
        JW_SYNC["JudgeWorker (sync mode)\n─────────────────\nScore groundedness, safety,\nand task success\nDecide pass/fail + low confidence"]
        EVAL_SYNC["AnalysisWorker.evaluate (sync mode)\nragas_evaluator"]
        JLOCAL["Local async judge fallback\n(bounded judge pool)"]
        ELOCAL["Local async eval fallback\n(bounded eval pool)"]
    end

    subgraph Control["Redis Control Plane"]
        L2C["Redis L2 cache\n(infra/redis_cache.py)"]
        JOBS["Redis async job store\n(infra/redis_job_store.py)"]
        JSTREAM["judge:jobs stream\nconsumer group"]
        ESTREAM["eval:jobs stream\nconsumer group"]
    end

    subgraph AsyncWorkers["External Async Worker Deployments"]
        JW_ASYNC["judge_worker.py\nRedis consumer -> JudgeWorker.judge()\nDLQ/retry/idempotency"]
        EW_ASYNC["evaluation_worker.py\nRedis consumer -> AnalysisWorker.evaluate()\nDLQ/retry/idempotency"]
    end

    subgraph Security["Security Package — security/llm_security.py"]
        PID["PromptInjectionDetector\n(OWASP LLM01)"]
        SIF["SensitiveInfoFilter\n(OWASP LLM02/LLM07)"]
        OV["OutputValidator\n(OWASP LLM05)"]
        RLE["ResourceLimitEnforcer\n(OWASP LLM10)"]
        VSV["VectorSecurityValidator\n(OWASP LLM08)"]
        SA["SecurityEventSink\n(Dashboard/Logger adapters)"]
    end

    subgraph Observability["Observability — observability.py / tracing.py"]
        OTL["OTLP Trace Exporter\n(Phoenix or generic collector)"]
        EVI["Evidently Monitor\n(evidently_monitor.py)"]
        SLI["StageLatencyEventStore\n(stage SLI timeseries)"]
        WPSLI["WorkerPoolEventStore\n(stage pool saturation timeseries)"]
    end

    subgraph Data["Data Layer"]
        CHROMA["ChromaDB\n(./chroma_db)"]
        EMBED["Embedding Pipeline\n(embedding_pipeline.py)\ntext-embedding-3-small"]
        TXT["Raw Text Corpus\n(data_text/apollo11,\napollo13, challenger)"]
    end

    UI --> CHAT
    API_CALL --> CHAT
    CHAT --> EXECUTOR
    EXECUTOR --> L1C
    EXECUTOR --> SW_PRE
    SW_PRE --> PID
    SW_PRE --> RLE
    SW_PRE --> VSV
    SW_PRE --> RW
    RW --> CHROMA
    CHROMA --> EMBED
    EMBED --> TXT

    RW --> |"retrieval_result"| AW
    SW_PRE --> |"preflight_result (blocked?)"| AW
    AW --> SW_POST
    SW_POST --> SIF
    SW_POST --> OV
    SW_POST --> SA
    SW_POST --> JW_SYNC
    JW_SYNC --> CHAT
    SW_POST -. judge_mode=async .-> JSTREAM
    JSTREAM --> JW_ASYNC
    JW_ASYNC --> JOBS
    JOBS --> JMON
    JOBS --> JLAST

    SW_POST -. evaluate=true, mode=sync .-> EVAL_SYNC
    EVAL_SYNC --> CHAT
    SW_POST -. evaluate=true, mode=async .-> ESTREAM
    ESTREAM --> EW_ASYNC
    EW_ASYNC --> JOBS
    JOBS --> EMON

    JSTREAM -. broker unavailable/no consumer .-> JLOCAL
    ESTREAM -. broker unavailable/no consumer .-> ELOCAL
    JLOCAL --> JOBS
    ELOCAL --> JOBS

    L1C --> L2C
    L2C --> JOBS
    AW --> OTL
    CHAT --> EVI
    EXECUTOR --> SLI
    EXECUTOR --> WPSLI
    WPSLI --> WMON
    L1C --> CMON
```

## Concurrency Design

```mermaid
gantt
    title Single /chat request — illustrative critical path (strict vs fastest)
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

This diagram shows an illustrative strict-mode cache-miss critical path (`PREFLIGHT_RETRIEVAL_MODE=strict`). The time spans are directional, not exact measurements.

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
- See [judge-api-contract.md](doc/judge-api-contract.md).

Evaluation concurrency behavior:
- `EVALUATION_MODE=sync`: evaluation runs on the dedicated evaluation executor and stays on the request critical path.
- `EVALUATION_MODE=async`: workflow records a pending job, attempts Redis broker enqueue first, then applies fallback gating:
    - if `EVALUATION_LOCAL_FALLBACK_ENABLED=true`, local bounded async execution is used when broker enqueue fails or when no broker consumers are active;
    - if `EVALUATION_LOCAL_FALLBACK_ENABLED=false`, the job is marked skipped with explicit source (`broker_unavailable` or `no_consumers`) instead of running locally.
    - if local async queue is saturated, the job is marked skipped with `source=overload`.
- `EVALUATION_MODE=off`: evaluation is skipped.

Shutdown behavior (reliability detail):
- Worker pools use a two-phase shutdown: first stop accepting submissions and soft-drain judge/evaluation pools briefly, then cancel pending async futures. Request-path pools remain fast non-blocking on stop.

## API Routing Architecture

```mermaid
flowchart TD
    APP[FastAPI app\napi_server.py]

    subgraph Boot[Startup and shutdown]
        LIFESPAN["lifespan app<br/>mission warmup + graceful shutdown"]
        TELEMETRY["init_telemetry app<br/>Phoenix / OTLP / console"]
        WARMSEC["Security pattern verification<br/>pre-compiled regex counts"]
        WARMCOL["warm_collection_index<br/>prime collection metadata"]
        SHUTDOWN["chat_workflow shutdown<br/>monitor shutdown"]
    end

    subgraph Middleware[Request middleware]
        RATE["Rate limiting + security headers<br/>Redis sliding window"]
        CORS["CORS middleware<br/>allow list from env"]
    end

    RateLimitRedis[(Redis sliding-window rate-limit store)]

    subgraph Core[Core API routes]
        HEALTH["/health"]
        TRACE["/tracing/status"]
        CHAT["/chat"]
    end

    subgraph Ops[Operational routes]
        MON1["/monitoring/report<br/>/monitoring/analytics<br/>/monitoring/analytics/prometheus"]
        MON2["/monitoring/rag<br/>/monitoring/rag/report"]
        MON3["/monitoring/judge<br/>/judge/last<br/>/monitoring/evaluation<br/>/evaluation/{job_id}"]
        MON4["/monitoring/security<br/>/monitoring/security/alerts<br/>/monitoring/security/events<br/>/monitoring/security/coverage<br/>/monitoring/security/prometheus"]
        MON5["/monitoring/latency-sli<br/>/monitoring/latency-sli/timeseries"]
        MON6["/monitoring/worker-pools<br/>/monitoring/worker-pools/series<br/>/monitoring/worker-pools/timeseries<br/>/monitoring/worker-pools/prometheus"]
        MON7["/monitoring/cache<br/>/monitoring/cache/stats<br/>/monitoring/client-caches<br/>/monitoring/config"]
    end

    subgraph CacheOps[Cache management]
        CLEAR["/collections/clear-cache"]
        WARM["/collections/warm-cache"]
    end

    subgraph Dependencies[Primary downstream services]
        WORKFLOW["MultiAgentChatWorkflow<br/>request orchestration"]
        MONITOR["EvidentlyMonitor<br/>interaction analytics + RAG reports"]
        SECURITY["security_dashboard<br/>alerts + event telemetry"]
        SLI["StageLatencyEventStore<br/>latency timeseries"]
        POOL["WorkerPoolEventStore<br/>worker-pool timeseries"]
        RAGINIT["_cached_rag_init + rag_client.warm_collection_index<br/>collection bootstrap"]
        CLIENTS["Client cache metrics<br/>llm_client / rag_client / ragas_evaluator"]
    end

    APP --> LIFESPAN
    LIFESPAN --> TELEMETRY
    LIFESPAN --> WARMSEC
    LIFESPAN --> WARMCOL
    LIFESPAN --> SHUTDOWN

    APP --> RATE
    APP --> CORS
    RATE --> RateLimitRedis

    RATE --> HEALTH
    RATE --> TRACE
    RATE --> CHAT
    RATE --> MON1
    RATE --> MON2
    RATE --> MON3
    RATE --> MON4
    RATE --> MON5
    RATE --> MON6
    RATE --> MON7
    RATE --> CLEAR
    RATE --> WARM

    CHAT --> WORKFLOW
    MON1 --> MONITOR
    MON2 --> MONITOR
    MON3 --> WORKFLOW
    MON4 --> SECURITY
    MON5 --> SLI
    MON6 --> POOL
    MON7 --> CLIENTS
    CLEAR --> RAGINIT
    WARM --> RAGINIT
```

This high-level layer diagram matches the current code shape in `api_server.py`:

1. A single FastAPI app owns all routes directly; there is no separate APIRouter layer.
2. Middleware is applied before routing so rate limiting and response headers are enforced consistently.
3. Startup warmup is deterministic: telemetry initializes during app setup, and bounded RAG warm targets run inside lifespan before serving traffic.
4. The route surface is intentionally split into request handling, monitoring, cache management, and operational introspection endpoints so scalable workloads can be observed and tuned without touching the main `/chat` path.

---

### Request Lifecycle

```mermaid
flowchart TD
    A(["POST /chat"])
    A --> B["Validate request\nbuild ChatWorkflowInput"]

    B --> C["Submit preflight to safety pool"]
    C --> MODE{preflight_retrieval_mode}
    MODE -->|strict| PONLY["Run preflight first"]
    MODE -->|fastest| PRESTART["Prestart retrieval future\nin retrieval pool"]

    PONLY --> PREF
    PRESTART --> PREF
    PREF["SafetyWorker.preflight()\nkeyword/rate/injection/vector checks"] --> BLOCK{blocked?}

    BLOCK -->|yes| RESP_BLOCKED["Return policy-blocked response\n(no LLM call)"]
    BLOCK -->|no| ACHECK["Answer cache check\nL1 then Redis L2"]

    ACHECK -->|hit| POST["SafetyWorker.postflight(answer)"]
    ACHECK -->|miss| RBREAK{retrieval breaker open\nor queue overload/timeout?}
    RBREAK -->|yes| DEG_RET["Return degraded retrieval fallback answer"]
    RBREAK -->|no| RETR["RetrievalWorker.run()\n(cache + compression aware)"]

    RETR --> GBREAK{generation breaker open\nor timeout/error?}
    GBREAK -->|yes| DEG_GEN["Return guarded generation fallback answer"]
    GBREAK -->|no| GEN["AnalysisWorker.generate_answer()"]
    GEN --> POST

    POST --> JMODE{judge_mode}
    JMODE -->|sync| JSYNC["JudgeWorker.judge() inline"]
    JMODE -->|async| JBROKER["Broker-first enqueue to judge:jobs"]
    JMODE -->|off| JOFF["Judge disabled"]

    JBROKER --> JCONS{active broker consumers?}
    JCONS -->|yes| JPENDING["Return judge status=pending"]
    JCONS -->|no or enqueue fail| JLOCAL["Local async judge fallback\n(if saturated -> skipped source=overload)"]

    JSYNC --> EVALQ
    JPENDING --> EVALQ
    JLOCAL --> EVALQ
    JOFF --> EVALQ

    EVALQ{evaluate=true?}
    EVALQ -->|no| RESP
    EVALQ -->|yes| EMODE{EVALUATION_MODE}

    EMODE -->|sync| ESYNC["AnalysisWorker.evaluate() inline"]
    EMODE -->|async| EBROKER["Create pending job\nenqueue eval:jobs"]
    EMODE -->|off| EOFF["Evaluation disabled"]

    EBROKER --> ECONS{active consumers?}
    ECONS -->|yes| EPENDING["Return evaluation pending"]
    ECONS -->|no or enqueue fail| EFALLBACK{local fallback enabled?}
    EFALLBACK -->|yes| ELOCAL["Local async eval fallback\n(if saturated -> skipped)"]
    EFALLBACK -->|no| ESKIP["Mark skipped\nsource=no_consumers or broker_unavailable"]

    ESYNC --> RESP
    EPENDING --> RESP
    ELOCAL --> RESP
    ESKIP --> RESP
    EOFF --> RESP

    RESP(["Return ChatWorkflowResult JSON"])
    RESP_BLOCKED --> RESP
    DEG_RET --> RESP
    DEG_GEN --> RESP

    subgraph AsyncWorkers["Out-of-band async completion"]
        JW["judge_worker.py consumes judge:jobs\nwrites Redis job store"]
        EW["evaluation_worker.py consumes eval:jobs\nwrites Redis job store"]
    end

    JBROKER -. async lane .-> JW
    EBROKER -. async lane .-> EW
```

Pipeline Pattern
- The `/chat` request flows through well-defined stages: input normalization, preflight safety gate, retrieval, generation, postflight validation, optional evaluation, and response serialization.
- This keeps each stage focused and makes performance tuning stage-specific.

### Cache Interaction (Client Session -> Workflow L1 -> Redis L2)

```mermaid
flowchart LR
    subgraph Client["Streamlit Session Cache (chat.py)"]
        CK["History-aware cache keys"]
        CE["Preserved evaluation payload"]
        CP["Avoid false pending state"]
    end

    subgraph API["Workflow Cache Layer (multi_agent/workflow.py)"]
        L1R["L1 retrieval cache"]
        L1A["L1 answer cache"]
        CST["Unified cache stats"]
    end

    subgraph Redis["L2 Redis Cache (optional shared layer)"]
        RSHARE["Cross-pod cache reuse"]
        RFLAG["Explicit enable flag"]
        RFALL["Graceful fallback if unavailable"]
    end

    CK --> L1R
    CK --> L1A
    CE --> CP
    L1R --> RSHARE
    L1A --> RSHARE
    CST --> MC["/monitoring/cache/stats"]
    RFLAG --> RSHARE
    RFALL --> RSHARE
```

This is a high-level cache interaction view spanning client-session reuse, server-side workflow caches, and optional Redis L2 sharing.

---

## Kubernetes Evaluation and Judge Worker Setup with KEDA Autoscaling Architecture

```mermaid
flowchart TD
  subgraph Clients[Client-facing tier]
    UI[Streamlit UI]
    APIConsumer[REST and promptfoo clients]
  end

  subgraph APITier[API and orchestrator tier]
    API[FastAPI and MultiAgentChatWorkflow]
    Pools[Bounded stage pools\nsafety retrieval generation judge evaluation]
    AsyncDecision[Broker-first async lanes\njudge and evaluation]
  end

  subgraph RedisPlane[Redis control plane]
    Redis[(Redis)]
    JudgeStream[judge:jobs stream\nconsumer group judge-workers]
    EvalStream[eval:jobs stream\nconsumer group eval-workers]
    JobStore[Shared async job store\nresult status and idempotency state]
    Backlog[Backlog metrics\nXPENDING and stream depth]
  end

  subgraph WorkerTier[Async worker deployments]
    JudgeDeploy[nasa-judge-worker Deployment\n1-10 replicas\njudge_worker.py]
    EvalDeploy[nasa-evaluation-worker Deployment\n1-10 replicas\nevaluation_worker.py]
  end

  subgraph Scaling[Autoscaling control]
    APIHPA[HPA for API and Streamlit tier\nrequest and resource driven]
    KEDA[KEDA ScaledObjects\nredis-streams primary trigger]
    CPUFallback[metrics-server CPU fallback]
  end

  UI --> API
  APIConsumer --> API
  API --> Pools
  Pools --> AsyncDecision

  AsyncDecision -->|enqueue async judge| JudgeStream
  AsyncDecision -->|enqueue async evaluation| EvalStream

  JudgeStream --> Redis
  EvalStream --> Redis
  Redis --> JobStore
  Redis --> Backlog

  JudgeStream --> JudgeDeploy
  EvalStream --> EvalDeploy

  JudgeDeploy -->|consume judge jobs| Redis
  EvalDeploy -->|consume evaluation jobs| Redis
  JudgeDeploy -->|write results status retries DLQ metadata| JobStore
  EvalDeploy -->|write results status retries DLQ metadata| JobStore
  JobStore --> API

  Backlog --> KEDA
  CPUFallback --> KEDA
  KEDA --> JudgeDeploy
  KEDA --> EvalDeploy
  API --> APIHPA
```

---

## Scalable Architecture Class Diagrams

### Workflow and Control Plane Abstraction

```mermaid
classDiagram
        class ChatWorkflowInput
        class ChatWorkflowResult
        class RetrievalResult
    class SafetyPreflightResult

        class MultiAgentChatWorkflow {
            +run(workflow_input, openai_key) ChatWorkflowResult
            +get_worker_pool_report() Dict
            +get_latency_sli_report() Dict
            +get_evaluation_job(job_id) Dict | None
            +get_recent_judge_results(limit=20) List
            +get_cache_stats() Dict
            +shutdown() None
        }

        class BoundedExecutor {
            +submit(fn, *args, **kwargs) Future
            +snapshot() Dict
            +begin_shutdown() None
            +wait_for_drain(timeout_seconds, poll_interval_seconds=0.01) bool
            +shutdown(wait=False, cancel_futures=False) None
        }

        class StageCircuitBreaker {
            +allow() bool
            +record_success() None
            +record_failure() None
        }

        class StageSLITracker {
            +record(latency_ms, timed_out) None
            +snapshot(budget_ms) Dict
        }

        class RetrievalWorker
        class SafetyWorker
        class AnalysisWorker
        class JudgeWorker

        class RedisL2Cache {
            +get_retrieval(...) RetrievalResult
            +set_retrieval(...) None
            +get_response(...) str
            +set_response(...) None
            +stats() Dict
        }

        class RedisAsyncJobStore {
            +create_job(job_id, job_type, request_id) bool
            +get_job(job_id) Dict | None
            +set_result(job_id, result) bool
            +get_result(job_id) Dict | None
            +is_completed(job_id) bool
            +acquire_processing(job_id, processing_ttl_seconds=300, worker_type) str | None
            +release_processing(job_id, token) bool
        }

        class RedisEvaluationBroker {
            +enqueue(job_id, payload) bool
            +has_active_consumers() bool
            +consume(consumer_name, count=1, block_ms=5000) List
            +dead_letter(message_id, payload, reason, consumer_name, attempt) bool
            +ack(message_id) bool
            +reclaim_stale(consumer_name, min_idle_ms=300000, count=10) List
        }

        class RedisJudgeBroker {
            +enqueue(job_id, payload) bool
            +has_active_consumers(timeout_seconds=0.0, poll_interval_seconds=0.05) bool
            +consume(consumer_name, count=1, block_ms=5000) List
            +dead_letter(message_id, payload, reason, consumer_name, attempt) bool
            +ack(message_id) bool
            +reclaim_stale(consumer_name, min_idle_ms=300000, count=10) List
        }

        class StageLatencyEventStore

        MultiAgentChatWorkflow ..> ChatWorkflowInput
        MultiAgentChatWorkflow ..> ChatWorkflowResult
        MultiAgentChatWorkflow ..> RetrievalResult
        MultiAgentChatWorkflow ..> SafetyPreflightResult
        SafetyWorker ..> SafetyPreflightResult

        MultiAgentChatWorkflow *-- RetrievalWorker
        MultiAgentChatWorkflow *-- SafetyWorker
        MultiAgentChatWorkflow *-- AnalysisWorker
        MultiAgentChatWorkflow *-- JudgeWorker

        MultiAgentChatWorkflow *-- BoundedExecutor : safety pool
        MultiAgentChatWorkflow *-- BoundedExecutor : retrieval pool
        MultiAgentChatWorkflow *-- BoundedExecutor : generation pool
        MultiAgentChatWorkflow *-- BoundedExecutor : judge pool
        MultiAgentChatWorkflow *-- BoundedExecutor : eval pool

        MultiAgentChatWorkflow *-- StageCircuitBreaker : retrieval
        MultiAgentChatWorkflow *-- StageCircuitBreaker : generation
        MultiAgentChatWorkflow *-- StageCircuitBreaker : evaluation
        MultiAgentChatWorkflow *-- StageSLITracker : preflight retrieval generation evaluation
        MultiAgentChatWorkflow *-- StageLatencyEventStore

        MultiAgentChatWorkflow *-- RedisL2Cache
        MultiAgentChatWorkflow *-- RedisAsyncJobStore
        MultiAgentChatWorkflow *-- RedisEvaluationBroker
        MultiAgentChatWorkflow *-- RedisJudgeBroker
```

This diagram is a high-level abstraction of the workflow/control plane. The method names are representative of the current code shape rather than a complete API inventory.

Orchestrator Pattern
- `MultiAgentChatWorkflow` in `multi_agent/workflow.py` coordinates worker execution and decision points (such as early exit on blocked safety checks).
- The orchestration logic is centralized, while worker logic is separated.

Worker Pattern (Specialized Agents)
- `RetrievalWorker`, `SafetyWorker`, `AnalysisWorker`, and `JudgeWorker` in `multi_agent/workers.py` each have a single responsibility.
- This improves testability and allows targeted scaling (for example, retrieval optimization without touching safety logic).

Data Transfer Object (DTO) Pattern
- `ChatWorkflowInput`, `RetrievalResult`, `SafetyPreflightResult`, and `ChatWorkflowResult` in `multi_agent/models.py` provide typed workflow contracts.
- These explicit contracts reduce coupling and make endpoint behavior easier to reason about.

Ports-and-Adapters (Partial Hexagonal)
- Centers the domain flow in multi-agent orchestration and workers (the inner hexagon side) also shows boundary-facing dependencies like Redis cache/job store and broker components (adapter side).

### Monitoring and Sink Abstractions

```mermaid
classDiagram
        class EvidentlyMonitor {
            +log_interaction(...) None
            +get_analytics_summary() Dict
            +get_rag_dashboard_summary(limit) Dict
            +get_prometheus_curated_snapshot() Dict
            +build_drift_report(reference_rows, output_html) Dict
            +build_rag_report(reference_rows, output_html) Dict
            +shutdown(timeout_seconds) None
        }

        class PrimaryInteractionSink {
            <<interface>>
            +persist_batch(records) None
            +load_dataframe() DataFrame
            +get_signature() Tuple
            +describe() Dict
            +shutdown() None
            +native_ndjson_path() Path
        }

        class MirrorInteractionSink {
            <<interface>>
            +persist_batch(records) None
            +describe() Dict
            +shutdown() None
        }

        class FileInteractionSink
        class PostgresInteractionSink {
            +supports_incremental_rollups() bool
            +load_incremental_rollups() Dict
        }
        class S3ObjectStorageMirrorSink
        class AzureBlobObjectStorageMirrorSink
        class OtlpLogMirrorSink

        EvidentlyMonitor *-- PrimaryInteractionSink
        EvidentlyMonitor *-- MirrorInteractionSink

        PrimaryInteractionSink <|.. FileInteractionSink
        PrimaryInteractionSink <|.. PostgresInteractionSink

        MirrorInteractionSink <|.. S3ObjectStorageMirrorSink
        MirrorInteractionSink <|.. AzureBlobObjectStorageMirrorSink
        MirrorInteractionSink <|.. OtlpLogMirrorSink
```

Ports-and-Adapters (Partial Hexagonal)
- Domain flow lives in multi-agent modules, while API, storage, and model-provider access remain in boundary modules.
- The clearest explicit ports/adapters style in class form (interfaces with concrete implementations), hence it reinforces the same architectural idea from another boundary slice.
- This project is already close to a full hexagonal architecture and can evolve there incrementally.

### Broker Worker Reliability Internals (Retry, DLQ, Idempotency)

```mermaid
classDiagram
        class EvaluationWorkerProcess {
            +run() int
            +_process_one_message(...) None
            +_backoff_seconds(base, max_backoff, attempt) float
            +_drain_requested() bool
            +_consumer_name() str
        }

        class JudgeWorkerProcess {
            +run() int
            +_backoff_seconds(base, max_backoff, attempt) float
            +_drain_requested() bool
            +_consumer_name() str
        }

        class RedisEvaluationBroker {
            +consume(consumer_name, count, block_ms) List
            +enqueue(job_id, payload) bool
            +ack(message_id) bool
            +dead_letter(message_id, payload, reason, consumer_name, attempt) bool
            +reclaim_stale(consumer_name, min_idle_ms, count) List
        }

        class RedisJudgeBroker {
            +consume(consumer_name, count, block_ms) List
            +enqueue(job_id, payload) bool
            +ack(message_id) bool
            +dead_letter(message_id, payload, reason, consumer_name, attempt) bool
            +reclaim_stale(consumer_name, min_idle_ms, count) List
        }

        class RedisAsyncJobStore {
            +is_completed(job_id) bool
            +acquire_processing(job_id, processing_ttl_seconds, worker_type) bool
            +release_processing(job_id) bool
            +set_result(job_id, result) bool
            +get_result(job_id) Dict
        }

        class AnalysisWorker {
            +evaluate(workflow_input, answer, contexts) Dict
        }

        class JudgeWorker {
            +judge(openai_key, workflow_input, answer, contexts) Dict
        }

        class AsyncReliabilityMetrics {
            +record_retry(worker, reason) None
            +record_dlq(worker, reason) None
            +record_reclaim(worker, reclaimed_count, min_idle_ms) None
            +record_lock_acquire_fail(worker, reason) None
        }

        EvaluationWorkerProcess *-- RedisEvaluationBroker
        EvaluationWorkerProcess *-- RedisAsyncJobStore
        EvaluationWorkerProcess *-- AnalysisWorker

        JudgeWorkerProcess *-- RedisJudgeBroker
        JudgeWorkerProcess *-- RedisAsyncJobStore
        JudgeWorkerProcess *-- JudgeWorker

        RedisEvaluationBroker ..> AsyncReliabilityMetrics
        RedisJudgeBroker ..> AsyncReliabilityMetrics
        RedisAsyncJobStore ..> AsyncReliabilityMetrics

        EvaluationWorkerProcess ..> RedisEvaluationBroker : retry enqueue on failure
        JudgeWorkerProcess ..> RedisJudgeBroker : retry enqueue on failure

        EvaluationWorkerProcess ..> RedisEvaluationBroker : DLQ on poison/retry exhausted
        JudgeWorkerProcess ..> RedisJudgeBroker : DLQ on poison/retry exhausted

        EvaluationWorkerProcess ..> RedisAsyncJobStore : idempotency lock and completed marker
        JudgeWorkerProcess ..> RedisAsyncJobStore : idempotency lock and completed marker

        RedisEvaluationBroker ..> RedisEvaluationBroker : reclaim_stale (XAUTOCLAIM)
        RedisJudgeBroker ..> RedisJudgeBroker : reclaim_stale (XAUTOCLAIM)
```

```mermaid
sequenceDiagram
    autonumber
    participant B as RedisEvaluationBroker
    participant W as EvaluationWorkerProcess
    participant J as RedisAsyncJobStore
    participant A as AnalysisWorker
    participant D as eval jobs DLQ stream

    B->>W: consume(consumer_name,count=1)
    W->>J: acquire_processing(job_id, ttl, worker_type=evaluation)
    J-->>W: lock acquired
    W->>A: evaluate(workflow_input, answer, contexts)
    A-->>W: Exception (processing error)
    W->>J: set_result(status=retrying, attempt=n+1)
    W->>B: enqueue(job_id, payload with _attempt=n+1)
    B-->>W: enqueue failed
    W->>J: set_result(status=dead_lettered, reason=retry enqueue failed)
    W->>B: dead_letter(message_id, reason=retry_enqueue_failed)
    B->>D: xadd(dead letter message)
    W->>B: ack(message_id)
```

### Security Guards and OWASP Compliance

```mermaid
classDiagram
        class PromptInjectionDetector {
            +__init__(threshold, debug) None
            +detect_injection(prompt) bool
            +get_score(prompt) float
        }

        class SensitiveInfoFilter {
            +__init__(pattern_list, redaction_char) None
            +filter_output(text) Tuple[str, List]
            +matches(text) List
        }

        class OutputValidator {
            +__init__(policy, severity) None
            +validate(text, original_prompt) Tuple[bool, str]
        }

        class ResourceLimitEnforcer {
            +__init__(token_limit, rate_limit_per_minute) None
            +check_tokens(prompt, response) bool
            +check_rate(client_id) bool
        }

        class VectorSecurityValidator {
            +__init__(embedding_client, safety_threshold) None
            +validate_retrieval_context(documents, query) Tuple[bool, List]
        }

        class SecurityEventSink {
            <<interface>>
            +log_event(event_type, severity, details) None
            +flush() None
            +shutdown() None
        }

        class DashboardSecurityEventSink {
            +log_event(event_type, severity, details) None
            +get_dashboard_events(limit) List
            +flush() None
        }

        class LoggerSecurityEventSink {
            +log_event(event_type, severity, details) None
            +flush() None
        }

        class SafetyWorker {
            +preflight(workflow_input) Tuple[bool, str]
            +postflight(answer, contexts) Tuple[str, Dict]
        }

        SafetyWorker *-- PromptInjectionDetector
        SafetyWorker *-- SensitiveInfoFilter
        SafetyWorker *-- OutputValidator
        SafetyWorker *-- ResourceLimitEnforcer
        SafetyWorker *-- VectorSecurityValidator
        SafetyWorker *-- SecurityEventSink

        SecurityEventSink <|.. DashboardSecurityEventSink
        SecurityEventSink <|.. LoggerSecurityEventSink
```

Strategy Pattern (Policy-by-Composition)
- Safety controls are injected into `SafetyWorker` as composable components (`PromptInjectionDetector`, `OutputValidator`, `SensitiveInfoFilter`, etc.).
- This enables swapping stricter or looser policies by environment without changing orchestration code.

Guard Clause Pattern
- Safety preflight applies fast-fail checks before expensive operations and returns early when blocked.
- This is cost-efficient and reduces LLM exposure to adversarial prompts.

### Stage SLI and Worker Pool Observability

```mermaid
classDiagram
        class StageLatencyEvent {
            +stage: str
            +latency_ms: float
            +timed_out: bool
            +mission: str
            +backend: str
            +model: str
            +timestamp: datetime
        }

        class StageLatencyEventStore {
            +__init__(db_path) None
            +record(event: StageLatencyEvent) None
            +get_stage_events(stage, limit) List
            +get_timeseries(stage, start_time, end_time) DataFrame
            +get_latency_percentiles(stage, window_minutes) Dict
            +shutdown() None
        }

        class StageSLITracker {
            +record(latency_ms, timed_out) None
            +snapshot(budget_ms) Dict
            +reset() None
        }

        class MultiAgentChatWorkflow {
            +run(workflow_input, openai_key) ChatWorkflowResult
            +get_latency_sli_report() Dict
            +get_worker_pool_report() Dict
        }

        class BoundedExecutor {
            +submit(fn, args) Future
            +snapshot() Dict
            +queue_size() int
            +active_count() int
        }

        class WorkerPoolMetrics {
            +active_threads: int
            +queue_size: int
            +completed_tasks: int
            +failed_tasks: int
            +avg_task_latency_ms: float
            +p95_task_latency_ms: float
            +p99_task_latency_ms: float
        }

        MultiAgentChatWorkflow *-- StageSLITracker : preflight retrieval generation judge eval
        MultiAgentChatWorkflow *-- StageLatencyEventStore
        MultiAgentChatWorkflow *-- BoundedExecutor : 5 stage pools
        BoundedExecutor ..> WorkerPoolMetrics : emits metrics
        StageLatencyEventStore ..> StageLatencyEvent : persists
```

### Worker Pool Infrastructure and Circuit Breakers

```mermaid
classDiagram
        class BoundedExecutor {
            +__init__(max_workers, queue_limit, submit_timeout_seconds, thread_name_prefix) None
            +submit(fn, *args, **kwargs) Future
            +snapshot() Dict
            +begin_shutdown() None
            +wait_for_drain(timeout_seconds, poll_interval_seconds=0.01) bool
            +shutdown(wait=False, cancel_futures=False) None
        }

        class StageCircuitBreaker {
            +failure_threshold: int
            +recovery_seconds: float
            +consecutive_failures: int
            +opened_until: float
            +allow() bool
            +record_success() None
            +record_failure() None
        }

        class MultiAgentChatWorkflow {
            +shutdown() None
            +get_worker_pool_report() Dict
        }

        MultiAgentChatWorkflow *-- BoundedExecutor : safety pool
        MultiAgentChatWorkflow *-- BoundedExecutor : retrieval pool
        MultiAgentChatWorkflow *-- BoundedExecutor : generation pool
        MultiAgentChatWorkflow *-- BoundedExecutor : judge pool
        MultiAgentChatWorkflow *-- BoundedExecutor : eval pool

        MultiAgentChatWorkflow *-- StageCircuitBreaker : retrieval breaker
        MultiAgentChatWorkflow *-- StageCircuitBreaker : generation breaker
        MultiAgentChatWorkflow *-- StageCircuitBreaker : evaluation breaker
```

This diagram reflects the concrete workflow concurrency classes in code. Worker-pool and breaker telemetry are emitted as dictionary snapshots rather than dedicated `PoolStats` / `CircuitBreakerStats` classes.

### Facade Wrappers Used by Workers + /chat Runtime Call Order

```mermaid
classDiagram
    class RetrievalWorker
    class AnalysisWorker

    class rag_client_py {
        <<Adapter/Facade>>
        +retrieve_documents(collection, question, n_results, mission_filter, chroma_dir) Dict
        +format_context(documents, metadatas) str
    }

    class llm_client_py {
        <<Adapter/Facade>>
        +generate_response(openai_key, user_message, context, conversation_history, model) str
    }

    class ragas_evaluator_py {
        <<Adapter/Facade>>
        +evaluate_response_quality(question, answer, contexts) Dict
    }

    class ChromaDB
    class OpenAI_API
    class RAGAS

    RetrievalWorker ..> rag_client_py : uses
    AnalysisWorker ..> llm_client_py : uses
    AnalysisWorker ..> ragas_evaluator_py : uses for sync evaluation

    rag_client_py ..> ChromaDB : wraps
    llm_client_py ..> OpenAI_API : wraps
    ragas_evaluator_py ..> RAGAS : wraps
```

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant API as api_server.py /chat
    participant WF as MultiAgentChatWorkflow
    participant RW as RetrievalWorker
    participant RAG as rag_client.py facade
    participant AW as AnalysisWorker
    participant LLM as llm_client.py facade
    participant EVAL as ragas_evaluator.py facade

    C->>API: POST /chat
    API->>WF: run(workflow_input, openai_key)
    WF->>WF: safety preflight passes

    alt answer cache miss
        WF->>RW: run(workflow_input)
        RW->>RAG: retrieve_documents(...)
        RAG-->>RW: docs + metadata
        RW->>RAG: format_context(contexts, metadatas)
        RAG-->>RW: context_text
        RW-->>WF: RetrievalResult
    else answer cache hit
        WF->>WF: skip retrieval facade calls
    end

    WF->>AW: generate_answer(..., context_text)
    AW->>LLM: generate_response(...)
    LLM-->>AW: answer
    AW-->>WF: answer
    WF->>WF: safety postflight

    alt evaluate=true and EVALUATION_MODE=sync and contexts available
        WF->>AW: evaluate(workflow_input, answer, contexts)
        AW->>EVAL: evaluate_response_quality(...)
        EVAL-->>AW: evaluation scores
        AW-->>WF: evaluation payload
    else evaluate disabled/off/async or no contexts
        WF->>WF: no inline ragas_evaluator call on request path
    end

    WF-->>API: ChatWorkflowResult
    API-->>C: ChatResponse
```

Adapter/Facade Pattern
- `llm_client.py`, `rag_client.py`, and `ragas_evaluator.py` wrap external providers and libraries.
- This isolates external API details from workflow code and reduces blast radius when vendor SDKs change.

### Phoenix Tracing and OpenTelemetry Pipeline

```mermaid
classDiagram
        class tracing_py {
            +configure_phoenix_tracing(project_name, endpoint) bool
            +phoenix_status() Dict
        }

        class observability_py {
            +init_telemetry(app, service_name) Tracer
            +telemetry_status() Dict
            -_fastapi_excluded_urls() str
            -_as_bool(value, default) bool
            -_float_env(name, default) float
            -_int_env(name, default) int
        }

        class _TELEMETRY_STATE {
            +initialized: bool
            +service_name: str
            +exporter: phoenix|otlp|console|none
            +endpoint: str
            +project: str
            +requests_instrumented: bool
            +fastapi_instrumented: bool
            +openai_instrumented: bool
        }

        class PhoenixRegister {
            +register(endpoint, project_name, batch, resource, sampler) TracerProvider
        }

        class TracerProvider {
            +add_span_processor(processor) None
        }

        class OTLPSpanExporter
        class ConsoleSpanExporter
        class BatchSpanProcessor
        class FastAPIInstrumentor {
            +instrument_app(app, tracer_provider, excluded_urls) None
        }
        class RequestsInstrumentor {
            +instrument() None
        }
        class OpenAIInstrumentor {
            +instrument(config) None
        }
        class TraceConfig {
            +hide_embedding_vectors: bool
            +hide_embeddings_vectors: bool
        }

        tracing_py ..> observability_py : phoenix_status delegates
        tracing_py ..> _TELEMETRY_STATE : reads status snapshot
        tracing_py ..> observability_py : sets PHOENIX_* env and defers init

        observability_py *-- _TELEMETRY_STATE : singleton runtime state

        observability_py ..> PhoenixRegister : when PHOENIX_ENDPOINT
        observability_py ..> TracerProvider : fallback provider
        observability_py ..> OTLPSpanExporter : when OTEL endpoint
        observability_py ..> ConsoleSpanExporter : console fallback
        observability_py ..> BatchSpanProcessor : for otlp/console exporters

        observability_py ..> RequestsInstrumentor : outbound HTTP spans
        observability_py ..> FastAPIInstrumentor : inbound API spans
        observability_py ..> OpenAIInstrumentor : LLM spans
        OpenAIInstrumentor ..> TraceConfig : redact embedding vectors
```

---

## Data Requirements

See [doc/data-requirements.md](doc/data-requirements.md) for the expected directory structure and supported document types.

## References

- Norvig, P. (n.d.). *Inference in Text Understanding*. Computer Science Dept., Evans Hall, University of California, Berkeley, Berkeley, CA 94720. Academia.edu. Retrieved May 30, 2026, from https://www.academia.edu/121229122/Inference_in_text_understanding?email_work_card=view-paper
- Norvig, P. (n.d.). *Remote Agent Experiment*. Computer Science Dept., Evans Hall, University of California, Berkeley, Berkeley, CA 94720. Academia.edu. Retrieved May 30, 2026, from https://www.academia.edu/120976021/Remote_Agent_Experiment?email_work_card=view-paper
- Russell, S., and Norvig, P. (2020). Artificial Intelligence: A Modern Approach (4th US ed., p. 712). Pearson. Retrieved May 30, 2026, from https://aima.cs.berkeley.edu/
- [Udacity Generative AI Nanodegree Program](https://www.udacity.com/course/generative-ai--nd608)
- [Udacity Security Engineer Nanodegree Program](https://www.udacity.com/course/security-engineer-nanodegree--nd698)
- [Udacity Cloud Native Application Architecture Nanodegree Program](https://www.udacity.com/blog/kick-off-your-cloud-native-application-architecture-career-with-the-launch-of-our-latest-nanodegree-program/)
- [Coursera Introduction to Machine Learning in Production](https://www.coursera.org/learn/introduction-to-machine-learning-in-production)
- [MLOps Guide by Huyen Chip](https://huyenchip.com/mlops/)
