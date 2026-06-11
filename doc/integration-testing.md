# Integration Testing

Run these commands to validate embeddings, API service, worker flow, and UI end to end.

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

Usage note: run the evaluation worker in a separate terminal alongside the API server so queued jobs are consumed and `/evaluation/{job_id}` can transition from `pending` to `completed`.