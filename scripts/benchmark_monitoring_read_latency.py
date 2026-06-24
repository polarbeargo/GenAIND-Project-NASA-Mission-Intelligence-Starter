#!/usr/bin/env python3
"""Benchmark old canonicalized reads vs new materialized reads under write load.

Usage examples:
  python scripts/benchmark_monitoring_read_latency.py
  python scripts/benchmark_monitoring_read_latency.py --duration-seconds 8 --read-iterations 150
"""

from __future__ import annotations

import argparse
import os
import statistics
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Dict, List

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from evidently_monitor import EvidentlyMonitor


def _old_canonicalized_read_latency_ms(monitor: EvidentlyMonitor) -> float:
    started = time.perf_counter()
    dataset = monitor.load_dataframe()
    if not dataset.empty:
        canonical = monitor._canonicalize_dataset(dataset)
        if "is_error" not in canonical.columns:
            canonical["is_error"] = 0.0
        if "latency_ms" not in canonical.columns:
            canonical["latency_ms"] = None
        _ = int(pd.to_numeric(canonical["is_error"], errors="coerce").fillna(0.0).sum())
        _ = float(pd.to_numeric(canonical["latency_ms"], errors="coerce").mean() or 0.0)
    return (time.perf_counter() - started) * 1000.0


def _new_materialized_read_latency_ms(monitor: EvidentlyMonitor) -> float:
    started = time.perf_counter()
    _ = monitor.get_analytics_summary()
    return (time.perf_counter() - started) * 1000.0


def _writer_loop(monitor: EvidentlyMonitor, stop_event: threading.Event, write_counter: Dict[str, int]) -> None:
    idx = 0
    while not stop_event.is_set():
        interaction_id = f"bench-{idx}"
        monitor.log_interaction(
            question=f"Q{idx}: Why did Apollo 13 abort the landing?",
            answer="Apollo 13 had an oxygen tank explosion.",
            model="gpt-4o-mini",
            backend="./chroma_db_openai:nasa_space_missions_text",
            context_count=3,
            mission="apollo_13",
            evaluation=None,
            error=(idx % 31 == 0),
            latency_ms=500.0 + (idx % 1200),
            interaction_id=interaction_id,
            synchronous=False,
        )

        # Emit a keyed update record to mimic async evaluation enrichment.
        if idx % 3 == 0:
            monitor.log_interaction(
                question=f"Q{idx}: Why did Apollo 13 abort the landing?",
                answer="Apollo 13 had an oxygen tank explosion.",
                model="gpt-4o-mini",
                backend="./chroma_db_openai:nasa_space_missions_text",
                context_count=3,
                mission="apollo_13",
                evaluation={
                    "faithfulness": 0.7 + ((idx % 30) / 100.0),
                    "response_relevancy": 0.68 + ((idx % 20) / 100.0),
                    "context_precision": 0.66 + ((idx % 10) / 100.0),
                },
                error=False,
                latency_ms=None,
                interaction_id=interaction_id,
                record_kind="evaluation_update",
                synchronous=False,
            )

        idx += 1
        write_counter["count"] = idx

        # Tiny jitter prevents CPU pegging while maintaining high pressure.
        if idx % 250 == 0:
            time.sleep(0.001)


def _summarize(samples: List[float]) -> Dict[str, float]:
    if not samples:
        return {"count": 0, "avg_ms": 0.0, "p95_ms": 0.0, "max_ms": 0.0}
    ordered = sorted(samples)
    p95_index = int(0.95 * (len(ordered) - 1))
    return {
        "count": float(len(samples)),
        "avg_ms": float(statistics.mean(samples)),
        "p95_ms": float(ordered[p95_index]),
        "max_ms": float(max(samples)),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Benchmark monitoring read latency under high write load")
    parser.add_argument("--duration-seconds", type=int, default=6, help="Duration for the benchmark window")
    parser.add_argument("--read-iterations", type=int, default=120, help="Number of reads per path")
    parser.add_argument(
        "--sink",
        choices=["file", "postgres", "auto"],
        default="auto",
        help="Sink to use. auto follows MONITORING_PRIMARY_SINK if set, else file.",
    )
    args = parser.parse_args()

    sink = args.sink
    if sink == "auto":
        sink = (os.getenv("MONITORING_PRIMARY_SINK") or "file").strip().lower() or "file"
    if sink not in {"file", "postgres"}:
        sink = "file"

    with tempfile.TemporaryDirectory() as temp_dir:
        log_path = Path(temp_dir) / "benchmark_interactions.jsonl"
        monitor = EvidentlyMonitor(
            log_path=str(log_path),
            sink_type=sink,
            mirror_sink_types=[],
        )

        stop_event = threading.Event()
        write_counter = {"count": 0}
        writer = threading.Thread(target=_writer_loop, args=(monitor, stop_event, write_counter), daemon=True)
        writer.start()

        # Warmup to build initial state and caches.
        time.sleep(0.5)
        _ = monitor.get_analytics_summary()

        old_samples: List[float] = []
        new_samples: List[float] = []

        started = time.monotonic()
        while time.monotonic() - started < max(1, args.duration_seconds):
            old_samples.append(_old_canonicalized_read_latency_ms(monitor))
            new_samples.append(_new_materialized_read_latency_ms(monitor))
            if len(old_samples) >= args.read_iterations and len(new_samples) >= args.read_iterations:
                break

        stop_event.set()
        writer.join(timeout=2.0)
        monitor.shutdown()

        old_summary = _summarize(old_samples)
        new_summary = _summarize(new_samples)

        speedup = (old_summary["avg_ms"] / new_summary["avg_ms"]) if new_summary["avg_ms"] > 0 else 0.0

        print("benchmark_sink=", sink)
        print("writes_observed=", write_counter["count"])
        print("old_read_avg_ms=", round(old_summary["avg_ms"], 4))
        print("old_read_p95_ms=", round(old_summary["p95_ms"], 4))
        print("new_read_avg_ms=", round(new_summary["avg_ms"], 4))
        print("new_read_p95_ms=", round(new_summary["p95_ms"], 4))
        print("avg_speedup_x=", round(speedup, 2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
