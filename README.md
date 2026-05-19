# NASA RAG Chat Project - NASA Mission Intelligence System 

## Overview

This system is a **multi-agent RAG (Retrieval-Augmented Generation) pipeline** built on FastAPI. It answers questions about NASA mission transcripts (Apollo 11, Apollo 13, Challenger) using ChromaDB for vector retrieval and OpenAI for generation. Security guards, a configurable **JudgeWorker** (`sync|async|off`), observability tracing, and red/blue-team evaluations are first-class components.


## References

- [Inference in Text Understanding (Academia.edu)](https://www.academia.edu/121229122/Inference_in_text_understanding?email_work_card=view-paper)
- [Remote Agent Experiment (Academia.edu)](https://www.academia.edu/120976021/Remote_Agent_Experiment?email_work_card=view-paper)
- [Artificial Intelligence: A Modern Approach, 4th US ed.](https://aima.cs.berkeley.edu/)
- [Udacity Generative AI Nanodegree](https://www.udacity.com/course/generative-ai--nd608)
- [Udacity Cloud Native Application Architecture Nanodegree](https://www.udacity.com/blog/kick-off-your-cloud-native-application-architecture-career-with-the-launch-of-our-latest-nanodegree-program/)


## 🚀 Getting Started

### Prerequisites
- Python 3.8+
- uv
- OpenAI API key


### Installation

1. **Navigate to the project folder**:
   ```bash
   git clone https://github.com/polarbeargo/Project-NASA-Mission-Intelligence-Starter.git

   cd Project-NASA-Mission-Intelligence-Starter
   ```

2. **Install dependencies with `uv`**:
   ```bash
   uv sync
   ```

3. **Activate the virtual environment (optional)**:
   ```bash
   source .venv/bin/activate
   ```

4. **Set up environment variables in `.env`**:
   ```bash
   # Choose a traffic profile (small | high)
   cp .env.small .env
   # or
   cp .env.high .env
   ```

### Environment Profiles

The project uses `.env` as the scalable baseline profile (medium traffic), with two preset overrides:

- `.env`: baseline profile used by default (medium pool/queue sizing)
- `.env.small`: lower concurrency and queue limits for local demos or small traffic
- `.env.high`: higher concurrency and queue limits for load testing and high traffic

Quick profile switches:

```bash
# Optional: keep a copy of your current .env before switching
cp .env .env.backup

# Small traffic
cp .env.small .env

# High traffic
cp .env.high .env

# Restore previous settings
cp .env.backup .env
```

### Env Variable Reference (Baseline `.env`)

| Variable | Baseline value | Description |
| --- | --- | --- |
| `OPENAI_API_KEY` | `(set in your local .env)` | OpenAI credential used by generation and embeddings. |
| `PHOENIX_ENDPOINT` | `http://localhost:6006/v1/traces` | Phoenix tracing endpoint. |
| `PHOENIX_PROJECT_NAME` | `nasa-mission-intelligence` | Trace project name in Phoenix. |
| `OTEL_SERVICE_NAME` | `nasa-mission-intelligence` | OpenTelemetry service identifier. |
| `ALLOWED_ORIGINS` | `localhost:3000,localhost:8000` | Comma-separated allowed origins for API CORS. |
| `JUDGE_MODE_DEFAULT` | `async` | Default judge mode for evaluation flow. |
| `JUDGE_TIMEOUT_SECONDS` | `2.5` | Timeout budget for judge task. |
| `EVALUATION_MODE` | `async` | Evaluation execution mode. |
| `RETRIEVAL_TIMEOUT_SECONDS` | `0.9` | Retrieval stage timeout. |
| `GENERATION_TIMEOUT_SECONDS` | `2.2` | Generation stage timeout. |
| `EVALUATION_TIMEOUT_SECONDS` | `3.0` | Evaluation stage timeout. |
| `STAGE_BREAKER_FAILURE_THRESHOLD` | `5` | Stage circuit-breaker failure threshold. |
| `STAGE_BREAKER_RECOVERY_SECONDS` | `30` | Stage circuit-breaker recovery window. |
| `RETRIEVAL_FACTOID_N_RESULTS` | `2` | Retrieval depth for factoid-style queries. |
| `RETRIEVAL_BROAD_N_RESULTS` | `4` | Retrieval depth for broad/exploratory queries. |
| `CONTEXT_MAX_TOKENS` | `2000` | Max context tokens before compression/truncation. |
| `CONTEXT_DEDUP_THRESHOLD` | `0.85` | Similarity threshold for context deduplication. |
| `PREFLIGHT_BUDGET_MS` | `20` | Latency budget for preflight stage. |
| `RETRIEVAL_BUDGET_MS` | `700` | Latency budget for retrieval stage. |
| `GENERATION_BUDGET_MS` | `1800` | Latency budget for generation stage. |
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

5. **Common `uv run` commands**:

   ```bash
   # Run embedding pipeline (configurable CLI)
   uv run python embedding_pipeline.py --data-path ./data_text --chroma-dir ./chroma_db_openai --collection-name nasa_space_missions_text

   # Run quick embedding setup (one-command defaults)
   uv run python setup_embeddings.py

   # Start Phoenix observability server
   uv run python -m phoenix.server.main serve

   # Start NASA FastAPI server
   uv run uvicorn api_server:app --host 0.0.0.0 --port 8000

   # Run all unittest test files
   uv run python -m unittest discover -s test -p 'test_*.py' -v
   
   # Run all pytest-based tests 
   uv run pytest test/ -v 2>&1

   # Launch chat interface
   uv run streamlit run chat.py
   ```

### Embedding with `uv run`

Use one of these two approaches depending on your goal:

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

   # Stats only (no processing)
   uv run python embedding_pipeline.py --stats-only

   # Optional: test a retrieval query after processing
   uv run python embedding_pipeline.py --test-query "apollo 11 landing"
   ```

After embeddings are ready, launch chat:
```bash
uv run streamlit run chat.py
```

## 📊 Data Requirements

### **Expected Data Structure**
The system expects NASA document data organized in folders:
```
data/
├── apollo11/           # Apollo 11 mission documents
│   ├── *.txt          # Text files with mission data
├── apollo13/           # Apollo 13 mission documents
│   ├── *.txt          # Text files with mission data
└── challenger/         # Challenger mission documents
    ├── *.txt          # Text files with mission data
```

### **Supported Document Types**
- Plain text files (.txt)
- Mission transcripts
- Technical documents
- Audio transcriptions
- Flight plans and procedures


## Context Compression Benchmark

`benchmarks/benchmark_context_compression.py` measures the naive baseline dedup path against the optimized (blocked/cached/short-circuit) path side-by-side, with correctness assertions that fail fast if both paths diverge.

```bash
# Quick smoke run: 10 rounds × 2 dataset sizes
uv run python benchmarks/benchmark_context_compression.py --runs 10 --sizes 512,1024 --equivalence once
```

**CLI flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--runs` | `30` | Benchmark iterations per dataset size |
| `--sizes` | `512,1024,2048,4096` | Comma-separated input sizes (number of chunks) |
| `--seed` | `42` | Random seed for reproducible datasets |
| `--equivalence` | `once` | Run output parity checks `once`, `per-run`, or turn them `off` outside the timed region |
| `--warmup` | `0` | Untimed warmup iterations per dataset size before samples are collected |

Recommended modes:

- `--equivalence once`: good default for benchmark runs with low overhead.
- `--equivalence per-run`: strict correctness verification for every sampled dataset.
- `--equivalence off`: pure timing mode when parity has already been validated.
- `--warmup 1` or `--warmup 2`: useful when you want cleaner repeated-run timing.

**Sample output (10 runs × 512 and 1024 chunks):**

![Context Compression Benchmark](images/context_compression_benchmark.png)


> The optimized path (`use_optimized_dedup=True` in `CompressionConfig`) is **gated off by default** in production.
> Enable it only if your dataset shows a consistent speedup above ~1.1× before switching.

### **Integration Testing**

1. **Run the complete pipeline**:
   ```bash
   # Process documents
   uv run python embedding_pipeline.py --openai-key YOUR_KEY --data-path ./data
   
   # Launch chat interface using uv command
   uv run streamlit run chat.py
   ```

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

If you use the provided Kubernetes autoscaling manifests, run this once so HPA can read worker-pool custom metrics.

1. **Deploy/update Prometheus Adapter with project rules**
    ```bash
    helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
    helm upgrade --install prometheus-adapter prometheus-community/prometheus-adapter \
       --namespace monitoring --create-namespace \
       -f deploy/k8s/prometheus-adapter-values.yaml
    ```

2. **Verify Custom Metrics API is available**
    ```bash
    kubectl get apiservice v1beta1.custom.metrics.k8s.io
    kubectl get --raw "/apis/custom.metrics.k8s.io/v1beta1" | jq .
    ```

3. **Verify worker-pool metrics are exposed**
    ```bash
    kubectl get --raw \
       "/apis/custom.metrics.k8s.io/v1beta1/namespaces/default/pods/*/nasa_worker_pool_queue_depth_ratio" | jq .

    kubectl get --raw \
       "/apis/custom.metrics.k8s.io/v1beta1/namespaces/default/pods/*/nasa_worker_pool_utilization_ratio" | jq .

    kubectl get --raw \
       "/apis/custom.metrics.k8s.io/v1beta1/namespaces/default/pods/*/nasa_worker_pool_rejected_total" | jq .
    ```

4. **Apply HPA and confirm metrics are being consumed**
    ```bash
    kubectl apply -f deploy/k8s/hpa-api-worker-pools.yaml
    kubectl describe hpa nasa-mission-intelligence-api
    ```

If your app namespace is not `default`, replace `default` in the verification paths above.

## Latency SLI Usage

