# Integration Testing

Run these commands to validate embeddings, API service, worker flow, and UI end to end.

### Embedding Workflows with uv run

Use one of these approaches depending on your goal:

1. Quick setup (recommended for first run)

```bash
uv run python setup_embeddings.py
```

This uses built-in defaults:
- Data path: `./data_text`
- Chroma directory: `./chroma_db_openai`
- Collection: `nasa_space_missions_text`
- Update mode: incremental

2. Flexible pipeline CLI (custom paths/options)

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

3. Targeted mission-only helper (fastest for backfilling missing missions)

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

4. Embedding ingestion benchmark (normal vs fast_upsert)

Script: [benchmarks/benchmark_embedding_fast_upsert.py](../benchmarks/benchmark_embedding_fast_upsert.py)

```bash
uv run python benchmarks/benchmark_embedding_fast_upsert.py --mission challenger --runs 3
```

After embeddings are ready, run the following commands:

```bash

# Start Phoenix observability server
uv run python -m phoenix.server.main serve

# Start NASA FastAPI server
uv run uvicorn api_server:app --host 0.0.0.0 --port 8000

or

API_PROFILE=interactive uv run uvicorn api_server:app --host 0.0.0.0 --port 8000

# Start async evaluation worker (required when EVALUATION_MODE=async and broker is enabled)
uv run python evaluation_worker.py

# Run all unittest test files
uv run python -m unittest discover -s test -p 'test_*.py' -v

# Run all pytest-based tests
uv run pytest test/ -v 2>&1

# Launch chat interface
uv run streamlit run chat.py
```

Usage note: run the evaluation worker in a separate terminal alongside the API server so queued jobs are consumed and `/evaluation/{job_id}` can transition from `pending` to `completed`.

