#!/usr/bin/env python3
"""Micro-benchmark for context dedup optimization.

Run:
    python test/benchmark_context_compression.py --runs 30 --sizes 512,1024,2048,4096
"""

from __future__ import annotations

import argparse
import random
import string
import sys
import time
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from multi_agent.context_compression import CompressionConfig, DeduplicatingCompressor, _jaccard


def _random_word(k: int = 6) -> str:
    return "".join(random.choices(string.ascii_lowercase, k=k))


def _build_dataset(n: int = 3000) -> Tuple[List[str], List[Dict]]:
    base_chunks: List[str] = []
    for _ in range(300):
        words = [_random_word() for _ in range(14)]
        base_chunks.append(" ".join(words))

    contexts: List[str] = []
    metadatas: List[Dict] = []
    missions = ["apollo11", "apollo13", "challenger"]

    for idx in range(n):
        seed = base_chunks[idx % len(base_chunks)]
        # Inject repeats and slight near-duplicates to stress dedup path.
        if idx % 5 == 0:
            ctx = seed
        elif idx % 7 == 0:
            ctx = seed + " " + _random_word()
        else:
            ctx = seed + " " + _random_word() + " " + _random_word()
        contexts.append(ctx)
        metadatas.append({"mission": missions[idx % len(missions)]})

    return contexts, metadatas


def _baseline_deduplicate(
    contexts: List[str], metadatas: List[Dict], threshold: float
) -> Tuple[List[str], List[Dict]]:
    """Naive pre-optimization dedup baseline for side-by-side benchmarking."""
    kept_c: List[str] = []
    kept_m: List[Dict] = []
    kept_sets: List[frozenset] = []

    for ctx, meta in zip(contexts, metadatas):
        words: frozenset = frozenset((ctx or "").lower().split())
        if not words:
            continue
        if any(_jaccard(words, existing) >= threshold for existing in kept_sets):
            continue
        kept_c.append(ctx)
        kept_m.append(meta)
        kept_sets.append(words)

    return kept_c, kept_m


def _sort_by_mission(
    contexts: List[str], metadatas: List[Dict], mission_filter: str | None
) -> Tuple[List[str], List[Dict]]:
    if not mission_filter:
        return contexts, metadatas
    mission_key = mission_filter.strip().lower().replace(" ", "_")
    paired = list(zip(contexts, metadatas))
    paired.sort(key=lambda pair: 0 if str(pair[1].get("mission", "")).lower() == mission_key else 1)
    if not paired:
        return contexts, metadatas
    out_c, out_m = zip(*paired)
    return list(out_c), list(out_m)


def _apply_token_cap(
    contexts: List[str], metadatas: List[Dict], max_tokens: int
) -> Tuple[List[str], List[Dict]]:
    max_chars = max_tokens * 4
    total = 0
    kept_c: List[str] = []
    kept_m: List[Dict] = []

    for ctx, meta in zip(contexts, metadatas):
        chunk_chars = len(ctx or "")
        if kept_c and total + chunk_chars > max_chars:
            break
        kept_c.append(ctx)
        kept_m.append(meta)
        total += chunk_chars

    return kept_c, kept_m


def _baseline_compress(
    contexts: List[str], metadatas: List[Dict], config: CompressionConfig, mission_filter: str | None
) -> Tuple[List[str], List[Dict]]:
    deduped_c, deduped_m = _baseline_deduplicate(
        contexts,
        metadatas,
        threshold=config.similarity_threshold,
    )
    if config.mission_boost:
        deduped_c, deduped_m = _sort_by_mission(deduped_c, deduped_m, mission_filter)
    return _apply_token_cap(deduped_c, deduped_m, max_tokens=config.max_tokens)


def _assert_equivalent_outputs(
    baseline: Tuple[List[str], List[Dict]], optimized: Tuple[List[str], List[Dict]], dataset_size: int, run_idx: int
) -> None:
    baseline_c, baseline_m = baseline
    optimized_c, optimized_m = optimized

    if len(baseline_c) != len(optimized_c) or len(baseline_m) != len(optimized_m):
        raise AssertionError(
            f"equivalence failed size={dataset_size} run={run_idx}: "
            f"length mismatch baseline=({len(baseline_c)},{len(baseline_m)}) "
            f"optimized=({len(optimized_c)},{len(optimized_m)})"
        )

    if baseline_c != optimized_c:
        raise AssertionError(
            f"equivalence failed size={dataset_size} run={run_idx}: context ordering/content mismatch"
        )

    if baseline_m != optimized_m:
        raise AssertionError(
            f"equivalence failed size={dataset_size} run={run_idx}: metadata ordering/content mismatch"
        )


def run_once(dataset_size: int, run_idx: int) -> tuple[float, float]:
    config = CompressionConfig(
        max_tokens=2000,
        similarity_threshold=0.85,
        mission_boost=True,
        use_optimized_dedup=True,
    )
    compressor = DeduplicatingCompressor(config)
    contexts, metas = _build_dataset(n=dataset_size)

    start_baseline = time.perf_counter()
    baseline_result = _baseline_compress(contexts, metas, config=config, mission_filter="apollo13")
    baseline_ms = (time.perf_counter() - start_baseline) * 1000.0

    start = time.perf_counter()
    optimized_result = compressor.compress(contexts, metas, mission_filter="apollo13")
    optimized_ms = (time.perf_counter() - start) * 1000.0

    _assert_equivalent_outputs(baseline_result, optimized_result, dataset_size=dataset_size, run_idx=run_idx)

    return baseline_ms, optimized_ms


def _p95(values: Sequence[float]) -> float:
    return sorted(values)[max(0, int(round(0.95 * (len(values) - 1))))]


def _parse_sizes(raw_sizes: str) -> List[int]:
    sizes: List[int] = []
    for part in raw_sizes.split(","):
        value = int(part.strip())
        if value <= 0:
            raise ValueError(f"size must be > 0, got {value}")
        sizes.append(value)
    if not sizes:
        raise ValueError("at least one dataset size is required")
    return sizes


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark context compression baseline vs optimized")
    parser.add_argument("--runs", type=int, default=30, help="Number of benchmark runs per dataset size")
    parser.add_argument(
        "--sizes",
        type=str,
        default="512,1024,2048,4096",
        help="Comma-separated dataset sizes to benchmark",
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    if args.runs <= 0:
        raise ValueError(f"runs must be > 0, got {args.runs}")

    random.seed(args.seed)
    sizes = _parse_sizes(args.sizes)

    print("context_compression benchmark")
    print(f"runs_per_size={args.runs} sizes={','.join(str(s) for s in sizes)} seed={args.seed}")

    for dataset_size in sizes:
        results = [run_once(dataset_size=dataset_size, run_idx=idx) for idx in range(1, args.runs + 1)]
        baseline_ms = [item[0] for item in results]
        optimized_ms = [item[1] for item in results]
        baseline_avg = sum(baseline_ms) / len(baseline_ms)
        optimized_avg = sum(optimized_ms) / len(optimized_ms)
        baseline_p95 = _p95(baseline_ms)
        optimized_p95 = _p95(optimized_ms)
        speedup = (baseline_avg / optimized_avg) if optimized_avg > 0 else 0.0

        print(
            "summary "
            f"size={dataset_size} "
            f"baseline_avg_ms={baseline_avg:.2f} baseline_p95_ms={baseline_p95:.2f} "
            f"optimized_avg_ms={optimized_avg:.2f} optimized_p95_ms={optimized_p95:.2f} "
            f"speedup_x={speedup:.2f}"
        )


if __name__ == "__main__":
    main()
