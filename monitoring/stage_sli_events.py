"""NDJSON-backed storage and aggregation for stage latency SLI events."""

from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict


class StageLatencyEventStore:
    """Append-only NDJSON event store with lightweight time-series aggregation."""

    VALID_STAGES = ("preflight", "retrieval", "generation", "evaluation")

    def __init__(
        self,
        log_file: Path | None = None,
        retention_hours: float = 168.0,
        max_file_bytes: int = 20 * 1024 * 1024,
        max_rotated_files: int = 10,
        maintenance_interval_seconds: float = 60.0,
    ):
        self.log_file = log_file or Path(__file__).parent / "stage_latency_events.jsonl"
        self.log_file.parent.mkdir(parents=True, exist_ok=True)
        self._write_lock = threading.Lock()
        self._retention_hours = max(1.0, float(retention_hours))
        self._max_file_bytes = max(1024 * 1024, int(max_file_bytes))
        self._max_rotated_files = max(1, int(max_rotated_files))
        self._maintenance_interval_seconds = max(1.0, float(maintenance_interval_seconds))
        self._last_maintenance_at = 0.0

    @staticmethod
    def _percentile(sorted_values: list[float], percentile: float) -> float:
        if not sorted_values:
            return 0.0
        if len(sorted_values) == 1:
            return sorted_values[0]
        idx = int(round((percentile / 100.0) * (len(sorted_values) - 1)))
        idx = max(0, min(idx, len(sorted_values) - 1))
        return sorted_values[idx]

    def record(
        self,
        stage: str,
        latency_ms: float,
        timed_out: bool,
        budget_ms: float,
        status: str = "ok",
        mission: str | None = None,
        backend: str | None = None,
        model: str | None = None,
    ) -> Dict[str, Any]:
        safe_stage = stage.strip().lower()
        if safe_stage not in self.VALID_STAGES:
            raise ValueError(f"Unsupported stage: {stage}")

        safe_latency = max(0.0, float(latency_ms))
        safe_budget = max(0.0, float(budget_ms))
        event = {
            "timestamp_ms": round(time.time() * 1000),
            "stage": safe_stage,
            "latency_ms": round(safe_latency, 4),
            "timed_out": bool(timed_out),
            "status": status.strip().lower() if status and status.strip() else "ok",
            "budget_ms": round(safe_budget, 4),
            "within_budget": bool((not timed_out) and safe_latency <= safe_budget),
            "mission": (mission or "").strip().lower(),
            "backend": (backend or "").strip().lower(),
            "model": (model or "").strip().lower(),
        }

        with self._write_lock:
            with self.log_file.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(event, separators=(",", ":")) + "\n")
            self._maintenance_if_due()
        return event

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
                        "latency_ms": max(0.0, float(payload.get("latency_ms", 0.0))),
                        "timed_out": bool(payload.get("timed_out", False)),
                        "status": str(payload.get("status", "ok")).strip().lower() or "ok",
                        "budget_ms": max(0.0, float(payload.get("budget_ms", 0.0))),
                        "within_budget": bool(payload.get("within_budget", False)),
                        "mission": str(payload.get("mission", "")).strip().lower(),
                        "backend": str(payload.get("backend", "")).strip().lower(),
                        "model": str(payload.get("model", "")).strip().lower(),
                    }
                )
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
            bucket_events = grouped[bucket_start_ms]
            latencies = sorted(float(item["latency_ms"]) for item in bucket_events)
            total_requests = len(bucket_events)
            timeouts = sum(1 for item in bucket_events if item["timed_out"])
            timeout_rate = (timeouts / total_requests) if total_requests else 0.0
            within_budget = sum(1 for item in bucket_events if item["within_budget"])
            within_budget_rate = (within_budget / total_requests) if total_requests else 0.0
            budget_ms = max((float(item.get("budget_ms", 0.0)) for item in bucket_events), default=0.0)

            series.append(
                {
                    "bucket_start_ms": bucket_start_ms,
                    "bucket_end_ms": bucket_start_ms + bucket_ms,
                    "total_requests": total_requests,
                    "timeouts": timeouts,
                    "timeout_rate": round(timeout_rate, 4),
                    "timeout_rate_percent": round(timeout_rate * 100.0, 2),
                    "p50_ms": round(self._percentile(latencies, 50.0), 2),
                    "p95_ms": round(self._percentile(latencies, 95.0), 2),
                    "budget_ms": round(budget_ms, 2),
                    "within_budget_rate": round(within_budget_rate, 4),
                    "within_budget_rate_percent": round(within_budget_rate * 100.0, 2),
                }
            )
        return series

    def get_timeseries(
        self,
        stage: str | None = None,
        window_minutes: int = 60,
        bucket_seconds: int = 300,
        mission: str | None = None,
        backend: str | None = None,
        model: str | None = None,
    ) -> Dict[str, Any]:
        safe_window_minutes = max(1, min(int(window_minutes), 7 * 24 * 60))
        safe_bucket_seconds = max(10, min(int(bucket_seconds), 3600))
        events = self._read_recent_events(safe_window_minutes)

        mission_filter = (mission or "").strip().lower()
        backend_filter = (backend or "").strip().lower()
        model_filter = (model or "").strip().lower()

        def _matches(event: Dict[str, Any]) -> bool:
            if mission_filter and str(event.get("mission", "")).strip().lower() != mission_filter:
                return False
            if backend_filter and str(event.get("backend", "")).strip().lower() != backend_filter:
                return False
            if model_filter and str(event.get("model", "")).strip().lower() != model_filter:
                return False
            return True

        filtered_events = [item for item in events if _matches(item)]

        filters = {
            "mission": mission_filter or None,
            "backend": backend_filter or None,
            "model": model_filter or None,
        }

        if stage is not None:
            safe_stage = stage.strip().lower()
            if safe_stage not in self.VALID_STAGES:
                raise ValueError(f"Unsupported stage: {stage}")
            stage_events = [item for item in filtered_events if item["stage"] == safe_stage]
            return {
                "generated_at_ms": round(time.time() * 1000),
                "window_minutes": safe_window_minutes,
                "bucket_seconds": safe_bucket_seconds,
                "stage": safe_stage,
                "filters": filters,
                "series": self._build_series(stage_events, safe_bucket_seconds),
            }

        workers = {
            worker: self._build_series([item for item in filtered_events if item["stage"] == worker], safe_bucket_seconds)
            for worker in self.VALID_STAGES
        }
        return {
            "generated_at_ms": round(time.time() * 1000),
            "window_minutes": safe_window_minutes,
            "bucket_seconds": safe_bucket_seconds,
            "filters": filters,
            "workers": workers,
        }