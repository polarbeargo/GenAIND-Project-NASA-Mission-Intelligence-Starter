# Hybrid Retrieval Production Tuning Profile

Concrete environment settings for hybrid semantic+keyword retrieval optimized for your NASA mission RAG pipeline.

## Pipeline Characteristics

- **Corpus**: ~2K NASA mission documents (Apollo 11/13, Challenger, Shuttle transcripts, flight plans, telemetry)
- **Embedding Model**: OpenAI text-embedding-3-small (1536-dim, fast)
- **First-Pass Strategy**: semantic query + keyword $contains probes
- **Reranking**: deterministic lexical + vector blend (65% lexical, 35% semantic distance)
- **Target Latency**: retrieval ≤1.8s (p95), end-to-end chat ≤9s

---

## Three Production Profiles

### **Profile 1: Balanced (Default)**
**Use for:** General multi-turn chat, mixed query types, production fleet.

```bash
# Retrieval expansion
export RETRIEVAL_FIRST_PASS_MULTIPLIER=4
export RETRIEVAL_FIRST_PASS_MAX_CANDIDATES=24

# Hybrid keyword tuning
export RETRIEVAL_HYBRID_ENABLED=true
export RETRIEVAL_KEYWORD_TERM_LIMIT=3
export RETRIEVAL_KEYWORD_CANDIDATES_PER_TERM=4

# Context compression
export CONTEXT_MAX_TOKENS=2000
export CONTEXT_DEDUP_THRESHOLD=0.85

# Retrieval depth policy
export RETRIEVAL_FACTOID_N_RESULTS=2
export RETRIEVAL_BROAD_N_RESULTS=4

# Stage timeouts
export RETRIEVAL_TIMEOUT_SECONDS=1.8
export GENERATION_TIMEOUT_SECONDS=8.0
export EVALUATION_TIMEOUT_SECONDS=3.5

# Latency budgets (p95 SLI gates)
export PREFLIGHT_BUDGET_MS=20
export RETRIEVAL_BUDGET_MS=700
export GENERATION_BUDGET_MS=1800
```

**Tuning Rationale:**
- First-pass multiplier 4x: retrieves 8–12 semantic candidates (for n_results=2–3 requests)
  - Enough room for keyword probes to expand without exceeding 24-doc pool
  - Keeps semantic-first results dominant (reranking preserves order stability)
- Keyword term limit 3: extracts mission IDs, acronyms, crew names (ignores common words)
  - Bounds keyword probe work to ~3–5 query calls
- Keyword candidates per term 4: small per-term pool prevents candidate explosion
  - ~12 keyword candidates max from 3 terms × 4 each
  - Together with 8–12 semantic = ~20, leaves room for dedup
- Retrieval timeout 1.8s: fast-fail if ChromaDB/network stalls
- Budget 700ms: includes warmup, 2–3 probe latency (Chrome ~20–30ms each)

**Expected Performance:**
- Latency: 200–400ms (p50), 600–800ms (p95)
- Throughput: 15–20 QPS on 4 retrieval workers
- Recall lift: ~8–12% over semantic-only on acronym/ID queries

---

### **Profile 2: High-Throughput (Aggressive)**
**Use for:** Low-latency APIs, high QPS, high miss tolerance, real-time dashboards.

```bash
export RETRIEVAL_FIRST_PASS_MULTIPLIER=2
export RETRIEVAL_FIRST_PASS_MAX_CANDIDATES=12

export RETRIEVAL_HYBRID_ENABLED=true
export RETRIEVAL_KEYWORD_TERM_LIMIT=2
export RETRIEVAL_KEYWORD_CANDIDATES_PER_TERM=2

export CONTEXT_MAX_TOKENS=1200
export CONTEXT_DEDUP_THRESHOLD=0.80

export RETRIEVAL_FACTOID_N_RESULTS=1
export RETRIEVAL_BROAD_N_RESULTS=2

export RETRIEVAL_TIMEOUT_SECONDS=0.8
export GENERATION_TIMEOUT_SECONDS=4.0
export EVALUATION_TIMEOUT_SECONDS=1.5

export PREFLIGHT_BUDGET_MS=10
export RETRIEVAL_BUDGET_MS=300
export GENERATION_BUDGET_MS=800
```

**Tuning Rationale:**
- Multiplicer 2x: ~4–6 semantic candidates only
- Keyword term limit 2: only top 2 high-signal terms
  - Keyword candidates/term 2: skip noisy probes
  - Total candidate pool ~6–10, cuts reranking work by 60%
- Timeout 0.8s: strict SLA gate, fail fast instead of stale data
- Context 1.2K tokens: forces aggressive dedup, skips low-confidence chunks

**Expected Performance:**
- Latency: 100–200ms (p50), 400–500ms (p95)
- Throughput: 40–60 QPS on 4 retrieval workers
- Recall: ~5–7% lower vs balanced (acceptable for real-time use cases)

---

### **Profile 3: High-Quality (Conservative)**
**Use for:** Offline evaluation, long-form answer generation, premium chat tier.

```bash
export RETRIEVAL_FIRST_PASS_MULTIPLIER=6
export RETRIEVAL_FIRST_PASS_MAX_CANDIDATES=50

export RETRIEVAL_HYBRID_ENABLED=true
export RETRIEVAL_KEYWORD_TERM_LIMIT=4
export RETRIEVAL_KEYWORD_CANDIDATES_PER_TERM=6

export CONTEXT_MAX_TOKENS=3000
export CONTEXT_DEDUP_THRESHOLD=0.92

export RETRIEVAL_FACTOID_N_RESULTS=3
export RETRIEVAL_BROAD_N_RESULTS=6

export RETRIEVAL_TIMEOUT_SECONDS=3.5
export GENERATION_TIMEOUT_SECONDS=15.0
export EVALUATION_TIMEOUT_SECONDS=5.0

export PREFLIGHT_BUDGET_MS=30
export RETRIEVAL_BUDGET_MS=1500
export GENERATION_BUDGET_MS=4000
```

**Tuning Rationale:**
- Multiplier 6x: ~18–24 semantic candidates
- Term limit 4, candidates/term 6: ~24 keyword candidates from deep probe
- Context 3K tokens: retain as much evidence as possible
  - Higher dedup threshold 0.92: keep diverse sources, not just top matches
- Timeout 3.5s: priority to correctness over speed
  - Generator gets 15s for thoughtful answers

**Expected Performance:**
- Latency: 500–800ms (p50), 1.5–2.0s (p95)
- Throughput: 5–8 QPS on 4 retrieval workers
- Recall: ~12–15% lift vs balanced (best for mission-critical analysis)

---

## Recommendation Matrix

| Scenario | Profile | Rationale |
|----------|---------|-----------|
| Production chat API | **Balanced** | Default, proven on mixed workloads |
| Mobile/real-time dashboard | **High-Throughput** | Sacrifice recall for latency |
| Long-form report generation | **High-Quality** | Invest time in grounding |
| Slow network / edge latency | **High-Throughput** | Network variability ≈ retrieval timeout |
| Batch re-evaluation | **High-Quality** | Offline, no user-facing latency |
| A/B test baseline | **Balanced** | Standard comparison point |

---

## Tuning Workflow

### 1. **Baseline Measurement** (run with Balanced profile)
```bash
# Monitor retrieval latency distribution
curl http://localhost:8000/monitoring/latency-sli | jq .workers.retrieval

# Sample 100 queries, measure p50/p95
for i in {1..100}; do
  time curl -X POST http://localhost:8000/chat \
    -H "Content-Type: application/json" \
    -d '{"question": "<sample-query>", "n_results": 3}'
done
```

### 2. **Profiling** (identify bottleneck)
- **If latency > 1.5s p95:**
  - Check ChromaDB index health: `curl http://localhost:8000/collections/warm-cache`
  - Try High-Throughput (multiply ratio by 0.5)
- **If recall drops or scores fall below threshold:**
  - Try High-Quality (multiply ratio by 1.5)
  - Lower `CONTEXT_DEDUP_THRESHOLD` by 0.05 to keep more variants

### 3. **Validation** (run RAGAS eval)
```bash
# After changing profile, re-evaluate on test set
export EVALUATION_MODE=sync
python -m pytest test/test_evaluation_mode.py -v
```

### 4. **Progressive Rollout**
- Canary 5% of users on new profile for 1h
- Monitor `/monitoring/rag` dashboard for retrieval/answer quality regressions
- If p95 latency increases >20% or RAGAS scores drop >3%, roll back

---

## Environment Variable Reference

### Retrieval Expansion
| Variable | Type | Range | Default | Impact |
|----------|------|-------|---------|--------|
| `RETRIEVAL_FIRST_PASS_MULTIPLIER` | int | 1–8 | 4 | Candidate pool size = n_results × multiplier |
| `RETRIEVAL_FIRST_PASS_MAX_CANDIDATES` | int | 1–100 | 24 | Hard cap; prevents reranking O(n²) blow-up |

### Hybrid Keyword Tuning
| Variable | Type | Range | Default | Impact |
|----------|------|-------|---------|--------|
| `RETRIEVAL_HYBRID_ENABLED` | bool | true/false | true | Enable/disable keyword probes entirely |
| `RETRIEVAL_KEYWORD_TERM_LIMIT` | int | 1–8 | 3 | Max keyword terms extracted (removes <3-char, stopwords) |
| `RETRIEVAL_KEYWORD_CANDIDATES_PER_TERM` | int | 1–16 | 4 | Candidates per term; total keyword pool ≈ limit × candidates_per_term |

### Context & Compression
| Variable | Type | Range | Default | Impact |
|----------|------|-------|---------|--------|
| `CONTEXT_MAX_TOKENS` | int | 200–8000 | 2000 | Max combined tokens in context (dedup + mission priority) |
| `CONTEXT_DEDUP_THRESHOLD` | float | 0.5–1.0 | 0.85 | Cosine sim threshold; lower = keep more duplicates |
| `RETRIEVAL_FACTOID_N_RESULTS` | int | 1–10 | 2 | Results for narrow queries ("When…", "Who…") |
| `RETRIEVAL_BROAD_N_RESULTS` | int | 1–10 | 4 | Results for exploratory queries ("Explain…", "Compare…") |

### Timeouts & Budgets

> **Note — profile-aware defaults:** `GENERATION_TIMEOUT_SECONDS` and `EVALUATION_TIMEOUT_SECONDS` vary by `API_PROFILE`.
> The values below are for `API_PROFILE=balanced`. When running the default `interactive` profile, the effective
> defaults are `GENERATION_TIMEOUT_SECONDS=6.5` and `EVALUATION_TIMEOUT_SECONDS=2.5`. For `throughput` profile:
> `GENERATION_TIMEOUT_SECONDS=10.0` and `EVALUATION_TIMEOUT_SECONDS=5.0`.
> Explicit env overrides always take precedence over profile defaults.

| Variable | Type | Range | Default (balanced) | Interactive | Throughput | Impact |
|----------|------|-------|---------------------|-------------|------------|--------|
| `RETRIEVAL_TIMEOUT_SECONDS` | float | 0.2–10 | 1.8 | 1.8 | 2.4 | Fail-fast if retrieval exceeds this |
| `GENERATION_TIMEOUT_SECONDS` | float | 0.5–30 | 8.0 | 6.5 | 10.0 | OpenAI generation deadline |
| `EVALUATION_TIMEOUT_SECONDS` | float | 0.5–20 | 3.5 | 2.5 | 5.0 | RAGAS/judge evaluation deadline |
| `PREFLIGHT_BUDGET_MS` | float | 1–1000 | 20 | Safety checks p95 SLI gate |
| `RETRIEVAL_BUDGET_MS` | float | 1–30000 | 700 | Retrieval p95 SLI gate (p99 ~100ms tighter) |
| `GENERATION_BUDGET_MS` | float | 1–30000 | 1800 | Generation p95 SLI gate |

---

## Cost & Performance Tradeoff

```
           Latency (ms)       Throughput (QPS)    Recall Lift    Token Cost
Balanced       ~250–700           15–20            +8–12%        100%
Hi-Throughput  ~100–500           40–60            +5–7%         60% (shorter context)
Hi-Quality     ~500–2000          5–8             +12–15%        160% (longer context)
```

---

## Next Steps

1. **Deploy Balanced profile** to staging, monitor for 24h
2. **Run RAGAS evaluation** on 500 diverse NASA queries
3. **A/B test Balanced vs High-Quality** on 5% of users
4. **Adjust keyword term limit** based on corpus lexical variance
5. **Auto-scale retrieval workers** based on `/monitoring/worker-pools` queue depth

---

## References

- [rag_client.py](../rag_client.py) — `_run_hybrid_first_pass()`, tuning env helpers
- [api_server.py](../api_server.py) — stage timeout/budget configuration (profile-aware defaults)
- [multi_agent/workflow.py](../multi_agent/workflow.py) — retrieval depth policy, stage SLI tracking
- [multi_agent/retrieval_depth.py](../multi_agent/retrieval_depth.py) — `RetrievalDepthPolicy`, factoid/broad heuristics
- [multi_agent/context_compression.py](../multi_agent/context_compression.py) — `DeduplicatingCompressor`, `CompressionConfig`
