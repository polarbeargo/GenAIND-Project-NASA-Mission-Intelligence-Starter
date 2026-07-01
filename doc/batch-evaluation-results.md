# Batch API Evaluation (RAGAS Async Job Flow)

Use the built-in batch runner to execute end-to-end `/chat` + `/evaluation/{job_id}` checks over a dataset.

Default dataset: [`test_questions.json`](../test_questions.json) (10 tagged queries)  
Runner: [`scripts/run_batch_evaluation.py`](../scripts/run_batch_evaluation.py)

Run the following in 4 separate terminals:
```bash
API_PROFILE=interactive uv run uvicorn api_server:app --host 0.0.0.0 --port 8000

uv run python evaluation_worker.py

uv run python -m phoenix.server.main serve

python3 scripts/run_batch_evaluation.py --dataset test_questions.json --base-url http://127.0.0.1:8000 --output-json monitoring/batch_eval_report.json 2>&1 | tail -n 100
```

Supported dataset formats:
- `JSON` array of objects with `question` and optional `id`, `category`, `mission`
- `TXT` lines as `category|mission|question` (or `question` only)

Fast CI dry-run (no live API required):
```bash
python scripts/run_batch_evaluation.py \
    --dataset test_questions.json \
    --dry-run \
    --min-questions 10
```

PR automation:
- [`.github/workflows/batch-eval-dry-run.yml`](../.github/workflows/batch-eval-dry-run.yml) runs this dry-run path on pull requests and fails if fewer than 10 valid tagged queries are present.

Malformed input handling:
- Invalid rows are skipped with row-level warnings.
- If no valid questions remain, the runner exits with a clear non-zero error.
- API or polling failures are recorded per question and the batch continues.

Output includes:
- Per-question status, mission, context count, and core metrics (`faithfulness`, `response_relevancy`, `context_precision`)
- Aggregate metric summary with mean/min/max plus `zero_count` and `null_count`

## Evaluation Results

See [BATCH_EVALUATION_RESULTS.md](../BATCH_EVALUATION_RESULTS.md) for detailed results from the 10-query regression test, including per-query scores, aggregate metrics, and performance analysis.

Generated live evaluation JSON report:
- [monitoring/batch_eval_report.json](../monitoring/batch_eval_report.json)
