#!/usr/bin/env python3
"""Run batch end-to-end evaluation against the /chat and /evaluation APIs.

Features:
- Loads JSON or TXT question datasets.
- Handles malformed dataset rows with clear non-crashing errors.
- Produces per-question metric summaries and aggregate stats.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class QuestionItem:
    row_index: int
    item_id: str
    category: str
    mission: str
    question: str


@dataclass
class QuestionResult:
    row_index: int
    item_id: str
    category: str
    mission: str
    question: str
    status: str
    error: Optional[str]
    context_count: int
    metrics: Dict[str, Optional[float]]


def _read_json_dataset(path: Path) -> List[Any]:
    with path.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    if not isinstance(payload, list):
        raise ValueError("JSON dataset must be a top-level array of question objects")
    return payload


def _read_txt_dataset(path: Path) -> List[Any]:
    rows: List[Any] = []
    with path.open("r", encoding="utf-8") as f:
        for idx, raw in enumerate(f, start=1):
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            parts = [part.strip() for part in line.split("|", 2)]
            if len(parts) == 1:
                rows.append({"question": parts[0]})
            elif len(parts) == 2:
                rows.append({"category": parts[0], "question": parts[1]})
            else:
                rows.append({"category": parts[0], "mission": parts[1], "question": parts[2]})
    return rows


def _load_dataset(path: Path) -> List[Any]:
    suffix = path.suffix.lower()
    if suffix == ".json":
        return _read_json_dataset(path)
    if suffix == ".txt":
        return _read_txt_dataset(path)
    raise ValueError("Unsupported dataset format. Use .json or .txt")


def _normalize_dataset(raw_rows: List[Any]) -> Tuple[List[QuestionItem], List[str]]:
    items: List[QuestionItem] = []
    errors: List[str] = []

    for idx, row in enumerate(raw_rows, start=1):
        if not isinstance(row, dict):
            errors.append(f"row {idx}: malformed row (expected object, got {type(row).__name__})")
            continue

        question = str(row.get("question", "")).strip()
        if not question:
            errors.append(f"row {idx}: missing non-empty 'question'")
            continue

        item_id = str(row.get("id") or f"q{idx}")
        category = str(row.get("category") or "uncategorized").strip() or "uncategorized"
        mission = str(row.get("mission") or "all").strip() or "all"

        items.append(
            QuestionItem(
                row_index=idx,
                item_id=item_id,
                category=category,
                mission=mission,
                question=question,
            )
        )

    return items, errors


def _urlopen_json(url: str, timeout: float) -> Any:
    with urllib.request.urlopen(url, timeout=timeout) as response:
        payload = response.read().decode("utf-8")
    return json.loads(payload)


def _post_chat(base_url: str, item: QuestionItem, timeout: float) -> Dict[str, Any]:
    payload = {
        "question": item.question,
        "mission_filter": item.mission,
        "evaluate": True,
        "mode": "comprehensive",
    }
    request = urllib.request.Request(
        f"{base_url}/chat",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        content = response.read().decode("utf-8")
    return json.loads(content)


def _poll_evaluation_result(base_url: str, job_id: str, timeout_s: float, interval_s: float) -> Dict[str, Any]:
    deadline = time.time() + timeout_s
    last_result: Dict[str, Any] = {}

    while time.time() < deadline:
        payload = _urlopen_json(f"{base_url}/evaluation/{job_id}", timeout=max(2.0, interval_s + 1.0))
        result = payload.get("result") if isinstance(payload, dict) else {}
        if isinstance(result, dict):
            last_result = result
        status = str(last_result.get("status", "")).lower()
        if status in {"completed", "error", "skipped", "dead_lettered"}:
            return last_result
        time.sleep(interval_s)

    timeout_result = dict(last_result)
    timeout_result["status"] = timeout_result.get("status") or "timeout"
    timeout_result["error"] = timeout_result.get("error") or "evaluation poll timeout"
    return timeout_result


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _evaluate_one(base_url: str, item: QuestionItem, request_timeout: float, poll_timeout: float, poll_interval: float) -> QuestionResult:
    try:
        chat_payload = _post_chat(base_url=base_url, item=item, timeout=request_timeout)
    except urllib.error.HTTPError as error:
        return QuestionResult(
            row_index=item.row_index,
            item_id=item.item_id,
            category=item.category,
            mission=item.mission,
            question=item.question,
            status="http_error",
            error=f"HTTP {error.code}: {error.reason}",
            context_count=0,
            metrics={},
        )
    except Exception as error:
        return QuestionResult(
            row_index=item.row_index,
            item_id=item.item_id,
            category=item.category,
            mission=item.mission,
            question=item.question,
            status="request_error",
            error=str(error),
            context_count=0,
            metrics={},
        )

    contexts = chat_payload.get("contexts") if isinstance(chat_payload, dict) else []
    context_count = len(contexts) if isinstance(contexts, list) else 0

    eval_stub = chat_payload.get("evaluation") if isinstance(chat_payload, dict) else {}
    job_id = eval_stub.get("job_id") if isinstance(eval_stub, dict) else None

    if not job_id:
        return QuestionResult(
            row_index=item.row_index,
            item_id=item.item_id,
            category=item.category,
            mission=item.mission,
            question=item.question,
            status="missing_job_id",
            error="chat response did not include evaluation.job_id",
            context_count=context_count,
            metrics={},
        )

    result = _poll_evaluation_result(
        base_url=base_url,
        job_id=str(job_id),
        timeout_s=poll_timeout,
        interval_s=poll_interval,
    )

    metrics = {
        "faithfulness": _to_float(result.get("faithfulness")),
        "response_relevancy": _to_float(result.get("response_relevancy")),
        "context_precision": _to_float(result.get("context_precision")),
        "bleu_score": _to_float(result.get("bleu_score")),
        "rouge_score": _to_float(result.get("rouge_score")),
    }

    return QuestionResult(
        row_index=item.row_index,
        item_id=item.item_id,
        category=item.category,
        mission=item.mission,
        question=item.question,
        status=str(result.get("status") or "unknown"),
        error=str(result.get("error")) if result.get("error") else None,
        context_count=context_count,
        metrics=metrics,
    )


def _evaluate_one_dry_run(item: QuestionItem) -> QuestionResult:
    # CI dry-run mode validates dataset normalization and summary plumbing
    # without depending on network or service availability.
    return QuestionResult(
        row_index=item.row_index,
        item_id=item.item_id,
        category=item.category,
        mission=item.mission,
        question=item.question,
        status="dry_run",
        error=None,
        context_count=0,
        metrics={
            "faithfulness": None,
            "response_relevancy": None,
            "context_precision": None,
            "bleu_score": None,
            "rouge_score": None,
        },
    )


def _summarize(results: List[QuestionResult]) -> Dict[str, Any]:
    metric_names = ["faithfulness", "response_relevancy", "context_precision", "bleu_score", "rouge_score"]
    aggregate: Dict[str, Any] = {
        "total_questions": len(results),
        "status_counts": {},
        "metrics": {},
    }

    for result in results:
        aggregate["status_counts"][result.status] = aggregate["status_counts"].get(result.status, 0) + 1

    for metric in metric_names:
        values = [r.metrics.get(metric) for r in results]
        numeric = [v for v in values if isinstance(v, float)]
        zeros = [v for v in numeric if v == 0.0]
        aggregate["metrics"][metric] = {
            "mean": round(statistics.mean(numeric), 6) if numeric else None,
            "min": round(min(numeric), 6) if numeric else None,
            "max": round(max(numeric), 6) if numeric else None,
            "count": len(numeric),
            "zero_count": len(zeros),
            "null_count": len(values) - len(numeric),
        }

    return aggregate


def _print_human_summary(results: List[QuestionResult], aggregate: Dict[str, Any], dataset_errors: List[str]) -> None:
    if dataset_errors:
        print("\nDataset validation warnings:")
        for error in dataset_errors:
            print(f"- {error}")

    print("\nPer-question summary:")
    for r in results:
        print(
            f"- [{r.item_id}] category={r.category} mission={r.mission} status={r.status} "
            f"contexts={r.context_count} faithfulness={r.metrics.get('faithfulness')} "
            f"relevancy={r.metrics.get('response_relevancy')} cp={r.metrics.get('context_precision')}"
        )
        if r.error:
            print(f"  error: {r.error}")

    print("\nAggregate summary:")
    print(json.dumps(aggregate, indent=2, ensure_ascii=True))


def main() -> int:
    parser = argparse.ArgumentParser(description="Batch end-to-end evaluator for NASA Mission Intelligence API")
    parser.add_argument(
        "--dataset",
        default="test_questions.json",
        help="Path to dataset file (.json or .txt). Default: test_questions.json",
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000", help="API base URL")
    parser.add_argument("--request-timeout", type=float, default=120.0, help="Timeout for /chat request in seconds")
    parser.add_argument("--poll-timeout", type=float, default=60.0, help="Max polling time per evaluation job in seconds")
    parser.add_argument("--poll-interval", type=float, default=0.5, help="Polling interval in seconds")
    parser.add_argument("--output-json", default="", help="Optional path to write full result JSON")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate dataset + reporting flow without calling /chat or /evaluation",
    )
    parser.add_argument(
        "--min-questions",
        type=int,
        default=1,
        help="Fail if fewer than this many valid questions are loaded",
    )
    args = parser.parse_args()

    dataset_path = Path(args.dataset)
    if not dataset_path.exists():
        print(f"Dataset load error: file not found: {dataset_path}", file=sys.stderr)
        return 2

    try:
        raw_rows = _load_dataset(dataset_path)
        items, dataset_errors = _normalize_dataset(raw_rows)
    except Exception as error:
        print(f"Dataset load error: {error}", file=sys.stderr)
        return 2

    if not items:
        print("Dataset load error: no valid questions found after validation", file=sys.stderr)
        if dataset_errors:
            for error in dataset_errors:
                print(f"- {error}", file=sys.stderr)
        return 2

    min_questions = max(1, int(args.min_questions))
    if len(items) < min_questions:
        print(
            f"Dataset load error: expected at least {min_questions} valid questions, got {len(items)}",
            file=sys.stderr,
        )
        return 2

    results: List[QuestionResult] = []
    for item in items:
        if args.dry_run:
            results.append(_evaluate_one_dry_run(item))
        else:
            results.append(
                _evaluate_one(
                    base_url=args.base_url.rstrip("/"),
                    item=item,
                    request_timeout=max(5.0, args.request_timeout),
                    poll_timeout=max(5.0, args.poll_timeout),
                    poll_interval=max(0.2, args.poll_interval),
                )
            )

    aggregate = _summarize(results)
    _print_human_summary(results, aggregate, dataset_errors)

    if args.output_json:
        output_payload = {
            "dataset": str(dataset_path),
            "base_url": args.base_url,
            "dataset_errors": dataset_errors,
            "results": [
                {
                    "row_index": r.row_index,
                    "id": r.item_id,
                    "category": r.category,
                    "mission": r.mission,
                    "question": r.question,
                    "status": r.status,
                    "error": r.error,
                    "context_count": r.context_count,
                    "metrics": r.metrics,
                }
                for r in results
            ],
            "aggregate": aggregate,
        }
        out = Path(args.output_json)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(output_payload, indent=2, ensure_ascii=True), encoding="utf-8")
        print(f"\nWrote JSON report: {out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
