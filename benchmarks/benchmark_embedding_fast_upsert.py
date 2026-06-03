#!/usr/bin/env python3
"""Benchmark normal mode vs fast_upsert ingestion for a selected mission."""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from embedding_pipeline import ChromaEmbeddingPipelineTextOnly
from openai_config import get_openai_api_key


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark embedding ingestion: normal vs fast_upsert on one mission"
    )
    parser.add_argument("--mission", required=True, help="Mission to benchmark (e.g. challenger, apollo13)")
    parser.add_argument("--data-path", default="./data_text", help="Path to mission data folders")
    parser.add_argument("--embedding-model", default="text-embedding-3-small", help="Embedding model")
    parser.add_argument("--collection-name", default="nasa_space_missions_bench", help="Collection name prefix")
    parser.add_argument("--batch-size", type=int, default=50, help="Batch size")
    parser.add_argument(
        "--update-mode",
        choices=["skip", "update", "replace"],
        default="skip",
        help="Non-incremental update mode used for both benchmark paths",
    )
    parser.add_argument("--runs", type=int, default=2, help="How many paired runs to execute")
    return parser.parse_args()


def _run_one_mode(
    *,
    openai_key: str,
    data_path: str,
    mission: str,
    embedding_model: str,
    collection_name: str,
    batch_size: int,
    update_mode: str,
    fast_upsert: bool,
) -> Tuple[float, Dict[str, Any]]:
    with tempfile.TemporaryDirectory(prefix="nasa_embed_bench_") as temp_chroma_dir:
        pipeline = ChromaEmbeddingPipelineTextOnly(
            openai_api_key=openai_key,
            chroma_persist_directory=temp_chroma_dir,
            collection_name=collection_name,
            embedding_model=embedding_model,
        )

        start = time.perf_counter()
        stats = pipeline.process_all_text_data(
            base_path=data_path,
            update_mode=update_mode,
            batch_size=batch_size,
            missions=[mission],
            checkpoint_manifest_each_file=False,
            fast_upsert=fast_upsert,
        )
        elapsed_seconds = time.perf_counter() - start
        return elapsed_seconds, stats


def _summary(values: List[float]) -> Dict[str, float]:
    return {
        "min": min(values),
        "max": max(values),
        "mean": statistics.mean(values),
        "median": statistics.median(values),
    }


def _workload_signature(stats: Dict[str, Any]) -> Dict[str, Any]:
    """Return counters that should match across benchmark modes."""
    keys = [
        "files_processed",
        "total_chunks",
        "documents_added",
        "documents_updated",
        "documents_skipped",
        "errors",
    ]
    return {key: stats.get(key) for key in keys}


def _assert_equivalent_workload(
    *,
    run_idx: int,
    normal_stats: Dict[str, Any],
    fast_stats: Dict[str, Any],
) -> None:
    normal_signature = _workload_signature(normal_stats)
    fast_signature = _workload_signature(fast_stats)
    if normal_signature != fast_signature:
        raise RuntimeError(
            "Benchmark workload mismatch between normal and fast_upsert "
            f"at run={run_idx}. normal={normal_signature} fast_upsert={fast_signature}"
        )


def main() -> None:
    args = _parse_args()

    openai_key = get_openai_api_key(include_chroma_fallback=False)
    if not openai_key:
        raise RuntimeError("OpenAI API key not found. Set OPENAI_API_KEY in .env")

    normal_times: List[float] = []
    fast_times: List[float] = []
    normal_stats: List[Dict[str, Any]] = []
    fast_stats: List[Dict[str, Any]] = []
    run_orders: List[str] = []

    for run_idx in range(1, args.runs + 1):
        if run_idx % 2 == 1:
            run_orders.append("normal_then_fast")
            normal_elapsed, normal_result = _run_one_mode(
                openai_key=openai_key,
                data_path=args.data_path,
                mission=args.mission,
                embedding_model=args.embedding_model,
                collection_name=f"{args.collection_name}_normal_{run_idx}",
                batch_size=args.batch_size,
                update_mode=args.update_mode,
                fast_upsert=False,
            )
            fast_elapsed, fast_result = _run_one_mode(
                openai_key=openai_key,
                data_path=args.data_path,
                mission=args.mission,
                embedding_model=args.embedding_model,
                collection_name=f"{args.collection_name}_fast_{run_idx}",
                batch_size=args.batch_size,
                update_mode=args.update_mode,
                fast_upsert=True,
            )
        else:
            run_orders.append("fast_then_normal")
            fast_elapsed, fast_result = _run_one_mode(
                openai_key=openai_key,
                data_path=args.data_path,
                mission=args.mission,
                embedding_model=args.embedding_model,
                collection_name=f"{args.collection_name}_fast_{run_idx}",
                batch_size=args.batch_size,
                update_mode=args.update_mode,
                fast_upsert=True,
            )
            normal_elapsed, normal_result = _run_one_mode(
                openai_key=openai_key,
                data_path=args.data_path,
                mission=args.mission,
                embedding_model=args.embedding_model,
                collection_name=f"{args.collection_name}_normal_{run_idx}",
                batch_size=args.batch_size,
                update_mode=args.update_mode,
                fast_upsert=False,
            )

        _assert_equivalent_workload(
            run_idx=run_idx,
            normal_stats=normal_result,
            fast_stats=fast_result,
        )

        normal_times.append(normal_elapsed)
        normal_stats.append(normal_result)
        fast_times.append(fast_elapsed)
        fast_stats.append(fast_result)

        print(
            f"run={run_idx} mission={args.mission} "
            f"normal={normal_elapsed:.3f}s fast_upsert={fast_elapsed:.3f}s"
        )

    normal_mean = statistics.mean(normal_times)
    fast_mean = statistics.mean(fast_times)
    speedup = (normal_mean / fast_mean) if fast_mean > 0 else 0.0

    result = {
        "mission": args.mission,
        "runs": args.runs,
        "run_order_strategy": "alternating_per_run",
        "run_orders": run_orders,
        "update_mode": args.update_mode,
        "batch_size": args.batch_size,
        "normal_seconds": _summary(normal_times),
        "fast_upsert_seconds": _summary(fast_times),
        "speedup_x": speedup,
        "normal_last_stats": normal_stats[-1] if normal_stats else {},
        "fast_upsert_last_stats": fast_stats[-1] if fast_stats else {},
    }

    print("\nBenchmark summary:")
    print(json.dumps(result, indent=2, ensure_ascii=True))


if __name__ == "__main__":
    main()
