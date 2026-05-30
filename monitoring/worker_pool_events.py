"""NDJSON-backed storage and aggregation for worker-pool saturation snapshots."""

from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict


class WorkerPoolEventStore:
    """Append-only NDJSON store for historical worker-pool snapshots."""

    VALID_STAGES = ("safety", "retrieval", "generation", "judge", "evaluation")

    def __init__(
        self,
        log_file: Path | None = None,
        retention_hours: float = 168.0,
        max_file_bytes: int = 20 * 1024 * 1024,
        max_rotated_files: int = 10,
        maintenance_interval_seconds: float = 60.0,
    ):
        self.log_file = log_file or Path(__file__).parent / "worker_pool_events.jsonl"
        self.log_file.parent.mkdir(parents=True, exist_ok=True)
        self._write_lock = threading.Lock()
        self._retention_hours = max(1.0, float(retention_hours))
        self._max_file_bytes = max(1024 * 1024, int(max_file_bytes))
        self._max_rotated_files = max(1, int(max_rotated_files))
        self._maintenance_interval_seconds = max(1.0, float(maintenance_interval_seconds))
        self._last_maintenance_at = 0.0

    @staticmethod
    def _avg(values: list[float]) -> float:
        if not values:
            return 0.0
        return sum(values) / len(values)

    @staticmethod
    def _counter_delta(values: list[float]) -> float:
        if not values:
            return 0.0
        if len(values) == 1:
            # A single observation has no interval to compare against.
            return 0.0

        delta = 0.0
        previous = values[0]
        for current in values[1:]:
            if current >= previous:
                delta += current - previous
            else:
                delta += max(0.0, current)
            previous = current
        return delta

    def record_snapshot(self, report: Dict[str, Any]) -> Dict[str, Any]:
        timestamp_ms = int(report.get("generated_at_ms", round(time.time() * 1000)))
        workers = report.get("workers", {})
        events: list[Dict[str, Any]] = []

        for stage, snapshot in workers.items():
            safe_stage = str(stage).strip().lower()
            if safe_stage not in self.VALID_STAGES:
                continue

            queue_limit = max(0.0, float(snapshot.get("queue_limit", 0.0)))
            capacity = max(0.0, float(snapshot.get("capacity", 0.0)))
            queued_estimate = max(0.0, float(snapshot.get("queued_estimate", 0.0)))
            inflight = max(0.0, float(snapshot.get("inflight", 0.0)))

            events.append(
                {
                    "timestamp_ms": timestamp_ms,
                    "stage": safe_stage,
                    "max_workers": max(0.0, float(snapshot.get("max_workers", 0.0))),
                    "queue_limit": queue_limit,
                    "capacity": capacity,
                    "inflight": inflight,
                    "queued_estimate": queued_estimate,
                    "submitted": max(0.0, float(snapshot.get("submitted", 0.0))),
                    "completed": max(0.0, float(snapshot.get("completed", 0.0))),
                    "rejected": max(0.0, float(snapshot.get("rejected", 0.0))),
                    "failed": max(0.0, float(snapshot.get("failed", 0.0))),
                    "oldest_queue_age_seconds": max(0.0, float(snapshot.get("oldest_queue_age_seconds", 0.0))),
                    "rejected_rate": max(0.0, float(snapshot.get("rejected_rate", 0.0))),
                    "error_rate": max(0.0, float(snapshot.get("error_rate", 0.0))),
                    "queue_depth_ratio": (queued_estimate / queue_limit) if queue_limit > 0.0 else 0.0,
                    "utilization_ratio": (inflight / capacity) if capacity > 0.0 else 0.0,
                }
            )

        if not events:
            return {
                "generated_at_ms": timestamp_ms,
                "sample_count": 0,
            }

        with self._write_lock:
            with self.log_file.open("a", encoding="utf-8") as handle:
                for event in events:
                    handle.write(json.dumps(event, separators=(",", ":")) + "\n")
            self._maintenance_if_due()

        return {
            "generated_at_ms": timestamp_ms,
            "sample_count": len(events),
        }

    def _maintenance_if_due(self) -> None:
        now = time.time()
        if (now - self._last_maintenance_at) < self._maintenance_interval_seconds:
            return
        self._last_maintenance_at = now
        self._prune_expired_events()
        self._rotate_if_needed()
        self._cleanup_rotated_files()

    def _prune_expired_events(self) -> None:
        if not self.log_file.exists():
            return
        cutoff_ms = round(time.time() * 1000) - int(self._retention_hours * 3600 * 1000)
        kept_lines: list[str] = []
        changed = False

        with self.log_file.open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    changed = True
                    continue
                timestamp_ms = int(payload.get("timestamp_ms", 0))
                if timestamp_ms >= cutoff_ms:
                    kept_lines.append(json.dumps(payload, separators=(",", ":")))
                else:
                    changed = True

        if changed:
            tmp_path = self.log_file.with_suffix(".jsonl.tmp")
            with tmp_path.open("w", encoding="utf-8") as handle:
                if kept_lines:
                    handle.write("\n".join(kept_lines) + "\n")
            tmp_path.replace(self.log_file)

    def _rotate_if_needed(self) -> None:
        if not self.log_file.exists():
            return
        if self.log_file.stat().st_size <= self._max_file_bytes:
            return

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        rotated_path = self.log_file.with_name(f"{self.log_file.stem}.{timestamp}.jsonl")
        self.log_file.replace(rotated_path)
        self.log_file.touch()

    def _cleanup_rotated_files(self) -> None:
        pattern = f"{self.log_file.stem}.*.jsonl"
        rotated = sorted(
            [path for path in self.log_file.parent.glob(pattern) if path.is_file()],
            key=lambda item: item.stat().st_mtime,
            reverse=True,
        )
        for stale in rotated[self._max_rotated_files :]:
            try:
                stale.unlink()
            except OSError:
                continue

    def _read_recent_events(self, window_minutes: int) -> list[Dict[str, Any]]:
        safe_window_minutes = max(1, min(int(window_minutes), 7 * 24 * 60))
        cutoff_ms = round(time.time() * 1000) - (safe_window_minutes * 60 * 1000)
        if not self.log_file.exists():
            return []

        events: list[Dict[str, Any]] = []
        try:
            with self.log_file.open("r", encoding="utf-8") as handle:
                for raw_line in handle:
                    line = raw_line.strip()
                    if not line:
                        continue
                    try:
                        payload = json.loads(line)
                    except json.JSONDecodeError:
                        continue

                    timestamp_ms = int(payload.get("timestamp_ms", 0))
                    stage = str(payload.get("stage", "")).strip().lower()
                    if timestamp_ms < cutoff_ms or stage not in self.VALID_STAGES:
                        continue

                    events.append(
                        {
                            "timestamp_ms": timestamp_ms,
                            "stage": stage,
                            "max_workers": max(0.0, float(payload.get("max_workers", 0.0))),
                            "queue_limit": max(0.0, float(payload.get("queue_limit", 0.0))),
                            "capacity": max(0.0, float(payload.get("capacity", 0.0))),
                            "inflight": max(0.0, float(payload.get("inflight", 0.0))),
                            "queued_estimate": max(0.0, float(payload.get("queued_estimate", 0.0))),
                            "submitted": max(0.0, float(payload.get("submitted", 0.0))),
                            "completed": max(0.0, float(payload.get("completed", 0.0))),
                            "rejected": max(0.0, float(payload.get("rejected", 0.0))),
                            "failed": max(0.0, float(payload.get("failed", 0.0))),
                            "oldest_queue_age_seconds": max(0.0, float(payload.get("oldest_queue_age_seconds", 0.0))),
                            "rejected_rate": max(0.0, float(payload.get("rejected_rate", 0.0))),
                            "error_rate": max(0.0, float(payload.get("error_rate", 0.0))),
                            "queue_depth_ratio": max(0.0, float(payload.get("queue_depth_ratio", 0.0))),
                            "utilization_ratio": max(0.0, float(payload.get("utilization_ratio", 0.0))),
                        }
                    )
        except OSError:
            # Rotation/replacement can race with reads; return best effort instead of failing monitoring.
            return []
        return events

    def _build_series(self, events: list[Dict[str, Any]], bucket_seconds: int) -> list[Dict[str, Any]]:
        safe_bucket_seconds = max(10, min(int(bucket_seconds), 3600))
        bucket_ms = safe_bucket_seconds * 1000
        grouped: Dict[int, list[Dict[str, Any]]] = {}

        for event in events:
            bucket_start_ms = (int(event["timestamp_ms"]) // bucket_ms) * bucket_ms
            grouped.setdefault(bucket_start_ms, []).append(event)

        series: list[Dict[str, Any]] = []
        for bucket_start_ms in sorted(grouped.keys()):
            bucket_events = sorted(grouped[bucket_start_ms], key=lambda item: int(item["timestamp_ms"]))
            inflight_values = [float(item["inflight"]) for item in bucket_events]
            queued_values = [float(item["queued_estimate"]) for item in bucket_events]
            queue_ratio_values = [float(item["queue_depth_ratio"]) for item in bucket_events]
            util_ratio_values = [float(item["utilization_ratio"]) for item in bucket_events]
            queue_age_values = [float(item["oldest_queue_age_seconds"]) for item in bucket_events]
            rejected_values = [float(item["rejected"]) for item in bucket_events]
            failed_values = [float(item["failed"]) for item in bucket_events]
            submitted_values = [float(item["submitted"]) for item in bucket_events]
            completed_values = [float(item["completed"]) for item in bucket_events]

            series.append(
                {
                    "bucket_start_ms": bucket_start_ms,
                    "bucket_end_ms": bucket_start_ms + bucket_ms,
                    "sample_count": len(bucket_events),
                    "inflight_avg": round(self._avg(inflight_values), 4),
                    "inflight_max": round(max(inflight_values, default=0.0), 4),
                    "queued_estimate_avg": round(self._avg(queued_values), 4),
                    "queued_estimate_max": round(max(queued_values, default=0.0), 4),
                    "queue_depth_ratio_avg": round(self._avg(queue_ratio_values), 6),
                    "queue_depth_ratio_max": round(max(queue_ratio_values, default=0.0), 6),
                    "utilization_ratio_avg": round(self._avg(util_ratio_values), 6),
                    "utilization_ratio_max": round(max(util_ratio_values, default=0.0), 6),
                    "oldest_queue_age_seconds_max": round(max(queue_age_values, default=0.0), 4),
                    "submitted_delta": round(self._counter_delta(submitted_values), 4),
                    "completed_delta": round(self._counter_delta(completed_values), 4),
                    "rejected_delta": round(self._counter_delta(rejected_values), 4),
                    "failed_delta": round(self._counter_delta(failed_values), 4),
                }
            )
        return series

    def get_timeseries(
        self,
        stage: str | None = None,
        window_minutes: int = 60,
        bucket_seconds: int = 300,
    ) -> Dict[str, Any]:
        safe_window_minutes = max(1, min(int(window_minutes), 7 * 24 * 60))
        safe_bucket_seconds = max(10, min(int(bucket_seconds), 3600))
        events = self._read_recent_events(safe_window_minutes)

        if stage is not None:
            safe_stage = stage.strip().lower()
            if safe_stage not in self.VALID_STAGES:
                raise ValueError(f"Unsupported stage: {stage}")
            stage_events = [item for item in events if item["stage"] == safe_stage]
            return {
                "generated_at_ms": round(time.time() * 1000),
                "window_minutes": safe_window_minutes,
                "bucket_seconds": safe_bucket_seconds,
                "stage": safe_stage,
                "series": self._build_series(stage_events, safe_bucket_seconds),
            }

        workers = {
            worker: self._build_series([item for item in events if item["stage"] == worker], safe_bucket_seconds)
            for worker in self.VALID_STAGES
        }
        return {
            "generated_at_ms": round(time.time() * 1000),
            "window_minutes": safe_window_minutes,
            "bucket_seconds": safe_bucket_seconds,
            "workers": workers,
        }