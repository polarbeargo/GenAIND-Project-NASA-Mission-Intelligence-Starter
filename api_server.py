#!/usr/bin/env python3
"""FastAPI server for NASA RAG + telemetry + monitoring."""

from __future__ import annotations

import logging
import math
import os
import time
import uuid
from contextlib import asynccontextmanager
from functools import lru_cache
from pathlib import Path
from threading import Lock
from typing import Dict, List, Optional, Any, Tuple

from env_utils import load_project_env
from fastapi import FastAPI, HTTPException, status, Request, Response
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool

import rag_client
import llm_client
import ragas_evaluator
from phoenix_annotations import collect_annotation_scores, post_span_annotations
from infra.async_reliability_metrics import get_async_reliability_metrics
from infra.redis_client import get_redis_client
from openai_config import get_openai_api_key, get_openai_chat_model
from evidently_monitor import EvidentlyMonitor
from observability import init_telemetry, telemetry_status
from multi_agent import ChatWorkflowInput, MultiAgentChatWorkflow, WorkflowError
from monitoring.security_dashboard import get_dashboard
from monitoring.security_event_sink import DashboardSecurityEventSink
from monitoring.stage_sli_events import StageLatencyEventStore
from monitoring.worker_pool_events import WorkerPoolEventStore

try:
    from security import (
        PromptInjectionDetector,
        SensitiveInfoFilter,
        OutputValidator,
        ResourceLimitEnforcer,
        VectorSecurityValidator,
        SecurityLevel,
        SecurityViolation,
    )
except ImportError:
    PromptInjectionDetector = None
    SensitiveInfoFilter = None
    OutputValidator = None
    ResourceLimitEnforcer = None
    VectorSecurityValidator = None
    SecurityLevel = None
    SecurityViolation = Exception

load_project_env(__file__)

logger = logging.getLogger(__name__)
security_dashboard = get_dashboard()


def _get_api_profile() -> str:
    profile = os.getenv("API_PROFILE", "interactive").strip().lower()
    return profile if profile in {"interactive", "balanced", "throughput"} else "interactive"


def _profile_default(interactive_value: Any, balanced_value: Any, throughput_value: Any | None = None) -> Any:
    profile = _get_api_profile()
    if profile == "interactive":
        return interactive_value
    if profile == "throughput" and throughput_value is not None:
        return throughput_value
    return balanced_value


def _parse_int_range(name: str, default: int, min_val: int = 1, max_val: int = 64) -> int:
    """Parse integer env var with bounds checking. Profile-aware: respects explicit env settings."""
    if name in os.environ:
        try:
            return max(min_val, min(int(os.getenv(name)), max_val))
        except (ValueError, TypeError):
            pass
    return max(min_val, min(default, max_val))


def _parse_float_range(name: str, default: float, min_val: float = 0.0, max_val: float = 1000.0) -> float:
    """Parse float env var with bounds checking. Profile-aware: respects explicit env settings."""
    if name in os.environ:
        try:
            return max(min_val, min(float(os.getenv(name)), max_val))
        except (ValueError, TypeError):
            pass
    return max(min_val, min(default, max_val))


def _profiled_int(name: str, interactive: int, balanced: int, throughput: int | None = None) -> int:
    """Profile-aware int with explicit env override (respects API_PROFILE setting for defaults only)."""
    profile = _get_api_profile()
    default = interactive if profile == "interactive" else (throughput if profile == "throughput" and throughput is not None else balanced)
    return _parse_int_range(name, default, min_val=1, max_val=64)


def _profiled_float(name: str, interactive: float, balanced: float, throughput: float | None = None, min_val: float = 0.0, max_val: float = 1000.0) -> float:
    """Profile-aware float with explicit env override (respects API_PROFILE setting for defaults only)."""
    profile = _get_api_profile()
    default = interactive if profile == "interactive" else (throughput if profile == "throughput" and throughput is not None else balanced)
    return _parse_float_range(name, default, min_val, max_val)

security_event_sink = DashboardSecurityEventSink(security_dashboard)


def _get_default_judge_mode() -> str:
    """Get default judge mode with profile-aware defaults and explicit env override."""
    profile = _get_api_profile()
    default = "sync" if profile == "interactive" else "async"
    mode = os.getenv("JUDGE_MODE_DEFAULT", default).strip().lower()
    return mode if mode in {"sync", "async", "off"} else default


def _get_judge_timeout_seconds() -> float:
    return _profiled_float("JUDGE_TIMEOUT_SECONDS", 2.5, 2.5, 3.5, min_val=1.5, max_val=10.0)





def _get_stage_submit_timeout_seconds() -> float:
    return _parse_float_range("STAGE_QUEUE_SUBMIT_TIMEOUT_SECONDS", 0.05, min_val=0.0, max_val=5.0)


def _get_bool_env(name: str, default: bool = False) -> bool:
    """Parse boolean env var (respects explicit True/False settings)."""
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _get_rate_limit_enabled() -> bool:
    return _get_bool_env("RATE_LIMIT_ENABLED", default=True)


def _get_rate_limit_requests_per_period() -> int:
    return _profiled_int("RATE_LIMIT_REQUESTS_PER_PERIOD", 20, 60, 120)


def _get_rate_limit_period_seconds() -> int:
    return _parse_int_range("RATE_LIMIT_PERIOD_SECONDS", 60, min_val=1, max_val=3600)


def _get_rate_limit_paths() -> List[str]:
    # Default to endpoints that are either expensive or mutate shared runtime state.
    default_paths = ",".join([
        "/chat",
        "/collections/clear-cache",
        "/collections/warm-cache",
        "/monitoring/report",
        "/monitoring/rag/report",
    ])
    configured = os.getenv("RATE_LIMIT_PATHS", default_paths).strip()
    paths = [path.strip() for path in configured.split(",") if path.strip()]
    return paths or ["/chat"]


def _get_evaluation_broker_stream() -> str:
    value = os.getenv("EVALUATION_BROKER_STREAM", "eval:jobs").strip()
    return value or "eval:jobs"


def _get_evaluation_broker_group() -> str:
    value = os.getenv("EVALUATION_BROKER_GROUP", "eval-workers").strip()
    return value or "eval-workers"


def _get_judge_broker_stream() -> str:
    value = os.getenv("JUDGE_BROKER_STREAM", "judge:jobs").strip()
    return value or "judge:jobs"


def _get_judge_broker_group() -> str:
    value = os.getenv("JUDGE_BROKER_GROUP", "judge-workers").strip()
    return value or "judge-workers"


def _validate_broker_lane_isolation(
    *,
    evaluation_broker_enabled: bool,
    evaluation_stream: str,
    evaluation_group: str,
    judge_broker_enabled: bool,
    judge_stream: str,
    judge_group: str,
) -> None:
    """Fail fast when async broker lanes collide across critical workloads.

    Evaluation and judge jobs have different SLO and failure profiles. If both
    broker paths are enabled but share a stream/group lane, one workload can
    starve the other under burst traffic. Enforce lane isolation at startup hence
    misconfiguration is detected before serving traffic.
    """
    if not (evaluation_broker_enabled and judge_broker_enabled):
        return

    collisions = []
    if evaluation_stream == judge_stream:
        collisions.append(f"stream={evaluation_stream!r}")
    if evaluation_group == judge_group:
        collisions.append(f"consumer_group={evaluation_group!r}")

    if collisions:
        raise RuntimeError(
            "Broker lane collision detected between evaluation and judge: "
            + ", ".join(collisions)
            + ". Configure distinct EVALUATION_BROKER_STREAM/JUDGE_BROKER_STREAM "
            + "and EVALUATION_BROKER_GROUP/JUDGE_BROKER_GROUP."
        )


def _get_breaker_failure_threshold() -> int:
    return _parse_int_range("STAGE_BREAKER_FAILURE_THRESHOLD", 3, min_val=1, max_val=10)


def _get_breaker_recovery_seconds() -> float:
    return _parse_float_range("STAGE_BREAKER_RECOVERY_SECONDS", 20.0, min_val=1.0, max_val=120.0)


def _judge_timed_out(judge: Dict[str, Any]) -> bool:
    rationale = str(judge.get("rationale", "")).lower()
    source = str(judge.get("source", "")).lower()
    explicit = bool(judge.get("timed_out", False))
    return explicit or (source == "heuristic" and "timeout" in rationale)


def _get_compression_max_tokens() -> int:
    return _parse_int_range("CONTEXT_MAX_TOKENS", 2000, min_val=200, max_val=8000)


def _get_compression_dedup_threshold() -> float:
    return _parse_float_range("CONTEXT_DEDUP_THRESHOLD", 0.85, min_val=0.5, max_val=1.0)


def _get_depth_threshold(name: str, default: int) -> int:
    return _parse_int_range(name, default, min_val=1, max_val=10)


def _get_evaluation_mode() -> str:
    """Get evaluation mode with profile defaults but explicit env override."""
    profile = _get_api_profile()
    default = "sync" if profile == "interactive" else "async"
    mode = os.getenv("EVALUATION_MODE", str(default)).strip().lower()
    return mode if mode in {"async", "sync", "off"} else default


def _get_profiled_stage_timeout(
    name: str,
    interactive_default: float,
    balanced_default: float,
    throughput_default: float,
    min_value: float,
    max_value: float,
) -> float:
    """Profile-aware stage timeout with explicit env override and bounds checking."""
    return _profiled_float(name, interactive_default, balanced_default, throughput_default, min_value, max_value)


def _get_preflight_timeout_seconds() -> float:
    return _get_profiled_stage_timeout(
        "PREFLIGHT_TIMEOUT_SECONDS",
        interactive_default=0.5,
        balanced_default=0.5,
        throughput_default=0.8,
        min_value=0.05,
        max_value=5.0,
    )


def _get_preflight_retrieval_mode() -> str:
    mode = os.getenv("PREFLIGHT_RETRIEVAL_MODE", "strict").strip().lower()
    return mode if mode in {"strict", "fastest"} else "strict"


def _get_evaluation_local_fallback_enabled() -> bool:
    return _get_bool_env("EVALUATION_LOCAL_FALLBACK_ENABLED", default=True)


def _get_profiled_stage_worker_count(
    name: str,
    interactive_default: int,
    balanced_default: int,
    throughput_default: int,
) -> int:
    """Profile-aware stage worker count with explicit env override."""
    return _profiled_int(name, interactive_default, balanced_default, throughput_default)


def _get_profiled_stage_queue_limit(
    name: str,
    interactive_default: int,
    balanced_default: int,
    throughput_default: int,
) -> int:
    """Profile-aware stage queue limit with explicit env override."""
    profile = _get_api_profile()
    default = interactive_default if profile == "interactive" else (throughput_default if profile == "throughput" else balanced_default)
    return _parse_int_range(name, default, min_val=1, max_val=5000)


def _get_latency_budget_ms(name: str, default: float) -> float:
    return _parse_float_range(name, default, min_val=1.0, max_val=30000.0)


def _get_stage_sli_retention_hours() -> float:
    return _parse_float_range("STAGE_SLI_RETENTION_HOURS", 168.0, min_val=1.0, max_val=24.0 * 365.0)


def _get_stage_sli_max_file_bytes() -> int:
    return _parse_int_range("STAGE_SLI_MAX_FILE_BYTES", 20 * 1024 * 1024, min_val=1024 * 1024, max_val=512 * 1024 * 1024)


def _get_stage_sli_max_rotated_files() -> int:
    return _parse_int_range("STAGE_SLI_MAX_ROTATED_FILES", 10, min_val=1, max_val=200)


def _get_stage_sli_maintenance_seconds() -> float:
    return _parse_float_range("STAGE_SLI_MAINTENANCE_SECONDS", 60.0, min_val=1.0, max_val=3600.0)


def _get_stage_sli_log_path() -> Path:
    configured = os.getenv("STAGE_SLI_LOG_FILE", "./monitoring/stage_latency_events.jsonl").strip()
    path = Path(configured) if configured else Path("./monitoring/stage_latency_events.jsonl")
    if not path.is_absolute():
        path = Path.cwd() / path
    return path


def _get_worker_pool_sli_retention_hours() -> float:
    return _parse_float_range("WORKER_POOL_SLI_RETENTION_HOURS", 168.0, min_val=1.0, max_val=24.0 * 30.0)


def _get_worker_pool_sli_max_file_bytes() -> int:
    return _parse_int_range("WORKER_POOL_SLI_MAX_FILE_BYTES", 20 * 1024 * 1024, min_val=1024 * 1024, max_val=200 * 1024 * 1024)


def _get_worker_pool_sli_max_rotated_files() -> int:
    return _parse_int_range("WORKER_POOL_SLI_MAX_ROTATED_FILES", 10, min_val=1, max_val=100)


def _get_worker_pool_sli_maintenance_seconds() -> float:
    return _parse_float_range("WORKER_POOL_SLI_MAINTENANCE_SECONDS", 60.0, min_val=1.0, max_val=3600.0)


def _get_worker_pool_sli_log_path() -> Path:
    configured = os.getenv("WORKER_POOL_SLI_LOG_FILE", "./monitoring/worker_pool_events.jsonl").strip()
    path = Path(configured) if configured else Path("./monitoring/worker_pool_events.jsonl")
    if not path.is_absolute():
        path = Path.cwd() / path
    return path


def _get_worker_pool_sli_sample_interval_seconds() -> float:
    return _parse_float_range("WORKER_POOL_SLI_SAMPLE_INTERVAL_SECONDS", 10.0, min_val=0.0, max_val=300.0)


def _phoenix_base_url() -> str:
    configured = (os.getenv("PHOENIX_BASE_URL") or "").strip()
    if configured:
        return configured.rstrip("/")

    endpoint = (os.getenv("PHOENIX_ENDPOINT") or "http://localhost:6006/v1/traces").strip()
    if endpoint.endswith("/v1/traces"):
        return endpoint[:-len("/v1/traces")].rstrip("/")
    return endpoint.rstrip("/")


def _collect_numeric_scores(*payloads: Any) -> Dict[str, float]:
    return collect_annotation_scores(*payloads, passthrough_keys={"latency_ms"})


def _post_phoenix_annotations(span_id: str, scores: Dict[str, float]) -> None:
    post_span_annotations(span_id=span_id, scores=scores, base_url=_phoenix_base_url(), logger=logger)


def _prometheus_escape_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _format_worker_pool_prometheus(report: Dict[str, Any]) -> str:
    """Render worker-pool saturation metrics in Prometheus exposition format."""
    lines: List[str] = [
        "# HELP nasa_worker_pool_max_workers Configured max workers per stage.",
        "# TYPE nasa_worker_pool_max_workers gauge",
        "# HELP nasa_worker_pool_queue_limit Configured queue size limit per stage.",
        "# TYPE nasa_worker_pool_queue_limit gauge",
        "# HELP nasa_worker_pool_capacity Total bounded capacity (workers + queue) per stage.",
        "# TYPE nasa_worker_pool_capacity gauge",
        "# HELP nasa_worker_pool_inflight Current in-flight tasks per stage.",
        "# TYPE nasa_worker_pool_inflight gauge",
        "# HELP nasa_worker_pool_queued_estimate Estimated queued tasks per stage.",
        "# TYPE nasa_worker_pool_queued_estimate gauge",
        "# HELP nasa_worker_pool_submitted_total Cumulative submitted tasks per stage.",
        "# TYPE nasa_worker_pool_submitted_total counter",
        "# HELP nasa_worker_pool_completed_total Cumulative completed tasks per stage.",
        "# TYPE nasa_worker_pool_completed_total counter",
        "# HELP nasa_worker_pool_rejected_total Cumulative rejected submissions per stage.",
        "# TYPE nasa_worker_pool_rejected_total counter",
        "# HELP nasa_worker_pool_failed_total Cumulative task execution failures per stage.",
        "# TYPE nasa_worker_pool_failed_total counter",
        "# HELP nasa_worker_pool_oldest_queue_age_seconds Age in seconds of oldest queued task per stage.",
        "# TYPE nasa_worker_pool_oldest_queue_age_seconds gauge",
        "# HELP nasa_worker_pool_rejected_rate Rejected submissions ratio per stage.",
        "# TYPE nasa_worker_pool_rejected_rate gauge",
        "# HELP nasa_worker_pool_error_rate Task execution error ratio per stage.",
        "# TYPE nasa_worker_pool_error_rate gauge",
        "# HELP nasa_worker_pool_queue_depth_ratio Queue depth ratio (queued_estimate / queue_limit).",
        "# TYPE nasa_worker_pool_queue_depth_ratio gauge",
        "# HELP nasa_worker_pool_utilization_ratio Capacity utilization ratio (inflight / capacity).",
        "# TYPE nasa_worker_pool_utilization_ratio gauge",
        "# HELP nasa_worker_pool_generated_at_ms Report generation epoch milliseconds.",
        "# TYPE nasa_worker_pool_generated_at_ms gauge",
    ]

    workers = report.get("workers", {})
    for stage, snapshot in workers.items():
        label = _prometheus_escape_label(str(stage))
        max_workers = float(snapshot.get("max_workers", 0))
        queue_limit = float(snapshot.get("queue_limit", 0))
        capacity = float(snapshot.get("capacity", 0))
        inflight = float(snapshot.get("inflight", 0))
        queued_estimate = float(snapshot.get("queued_estimate", 0))
        submitted = float(snapshot.get("submitted", 0))
        completed = float(snapshot.get("completed", 0))
        rejected = float(snapshot.get("rejected", 0))
        failed = float(snapshot.get("failed", 0))
        oldest_queue_age = float(snapshot.get("oldest_queue_age_seconds", 0.0))
        rejected_rate = float(snapshot.get("rejected_rate", 0.0))
        error_rate = float(snapshot.get("error_rate", 0.0))
        queue_ratio = (queued_estimate / queue_limit) if queue_limit > 0 else 0.0
        util_ratio = (inflight / capacity) if capacity > 0 else 0.0

        lines.extend(
            [
                f'nasa_worker_pool_max_workers{{stage="{label}"}} {max_workers}',
                f'nasa_worker_pool_queue_limit{{stage="{label}"}} {queue_limit}',
                f'nasa_worker_pool_capacity{{stage="{label}"}} {capacity}',
                f'nasa_worker_pool_inflight{{stage="{label}"}} {inflight}',
                f'nasa_worker_pool_queued_estimate{{stage="{label}"}} {queued_estimate}',
                f'nasa_worker_pool_submitted_total{{stage="{label}"}} {submitted}',
                f'nasa_worker_pool_completed_total{{stage="{label}"}} {completed}',
                f'nasa_worker_pool_rejected_total{{stage="{label}"}} {rejected}',
                f'nasa_worker_pool_failed_total{{stage="{label}"}} {failed}',
                f'nasa_worker_pool_oldest_queue_age_seconds{{stage="{label}"}} {oldest_queue_age:.6f}',
                f'nasa_worker_pool_rejected_rate{{stage="{label}"}} {rejected_rate:.6f}',
                f'nasa_worker_pool_error_rate{{stage="{label}"}} {error_rate:.6f}',
                f'nasa_worker_pool_queue_depth_ratio{{stage="{label}"}} {queue_ratio:.6f}',
                f'nasa_worker_pool_utilization_ratio{{stage="{label}"}} {util_ratio:.6f}',
            ]
        )

    generated_at_ms = float(report.get("generated_at_ms", 0))
    lines.append(f"nasa_worker_pool_generated_at_ms {generated_at_ms}")
    return "\n".join(lines) + "\n"


def _format_runtime_config_prometheus(config: Dict[str, Any]) -> str:
    """Render runtime config snapshot fields as Prometheus metrics."""
    api_profile = _prometheus_escape_label(str(config.get("api_profile", "unknown")))
    runtime_modes = config.get("runtime_modes", {}) or {}
    timeouts = config.get("timeouts_seconds", {}) or {}
    breaker = config.get("breaker", {}) or {}
    stage_pools = config.get("stage_pools", {}) or {}

    lines: List[str] = [
        "# HELP nasa_runtime_config_info Runtime configuration labels.",
        "# TYPE nasa_runtime_config_info gauge",
        "# HELP nasa_runtime_mode_info Runtime mode labels.",
        "# TYPE nasa_runtime_mode_info gauge",
        "# HELP nasa_runtime_timeout_seconds Effective timeout values in seconds by stage.",
        "# TYPE nasa_runtime_timeout_seconds gauge",
        "# HELP nasa_runtime_breaker_failure_threshold Effective stage breaker consecutive failure threshold.",
        "# TYPE nasa_runtime_breaker_failure_threshold gauge",
        "# HELP nasa_runtime_breaker_recovery_seconds Effective stage breaker recovery duration in seconds.",
        "# TYPE nasa_runtime_breaker_recovery_seconds gauge",
        "# HELP nasa_runtime_stage_pool_workers Configured worker count per stage.",
        "# TYPE nasa_runtime_stage_pool_workers gauge",
        "# HELP nasa_runtime_stage_pool_queue_limit Configured queue limit per stage.",
        "# TYPE nasa_runtime_stage_pool_queue_limit gauge",
    ]

    lines.append(f'nasa_runtime_config_info{{api_profile="{api_profile}"}} 1')
    preflight_mode = _prometheus_escape_label(str(runtime_modes.get("preflight_retrieval", "strict")))
    eval_local_fallback = _prometheus_escape_label(
        str(bool(runtime_modes.get("evaluation_local_fallback_enabled", True))).lower()
    )
    lines.append(
        "nasa_runtime_mode_info"
        f'{{preflight_retrieval_mode="{preflight_mode}",evaluation_local_fallback_enabled="{eval_local_fallback}"}} 1'
    )

    for stage, timeout_value in timeouts.items():
        safe_stage = _prometheus_escape_label(str(stage))
        try:
            safe_timeout = float(timeout_value)
        except (TypeError, ValueError):
            continue
        lines.append(f'nasa_runtime_timeout_seconds{{stage="{safe_stage}"}} {safe_timeout:.6f}')

    try:
        failure_threshold = float(breaker.get("failure_threshold", 0.0))
    except (TypeError, ValueError):
        failure_threshold = 0.0
    try:
        recovery_seconds = float(breaker.get("recovery_seconds", 0.0))
    except (TypeError, ValueError):
        recovery_seconds = 0.0

    lines.append(f"nasa_runtime_breaker_failure_threshold {failure_threshold:.6f}")
    lines.append(f"nasa_runtime_breaker_recovery_seconds {recovery_seconds:.6f}")

    for stage, sizing in stage_pools.items():
        if not isinstance(sizing, dict):
            continue
        safe_stage = _prometheus_escape_label(str(stage))
        try:
            workers = float(sizing.get("workers", 0.0))
        except (TypeError, ValueError):
            workers = 0.0
        try:
            queue_limit = float(sizing.get("queue_limit", 0.0))
        except (TypeError, ValueError):
            queue_limit = 0.0

        lines.append(f'nasa_runtime_stage_pool_workers{{stage="{safe_stage}"}} {workers:.6f}')
        lines.append(f'nasa_runtime_stage_pool_queue_limit{{stage="{safe_stage}"}} {queue_limit:.6f}')

    return "\n".join(lines) + "\n"


def _format_async_reliability_prometheus() -> str:
    """Render async worker reliability counters/gauges from Redis-backed metrics."""
    lines: List[str] = [
        "# HELP nasa_async_worker_retry_total Total async worker retries by worker and reason.",
        "# TYPE nasa_async_worker_retry_total counter",
        "# HELP nasa_async_worker_dlq_total Total async worker dead-letter events by worker and reason.",
        "# TYPE nasa_async_worker_dlq_total counter",
        "# HELP nasa_async_worker_reclaim_total Total reclaimed stale pending messages by worker.",
        "# TYPE nasa_async_worker_reclaim_total counter",
        "# HELP nasa_async_worker_reclaim_age_lower_bound_ms Lower-bound reclaimed idle age in milliseconds by worker.",
        "# TYPE nasa_async_worker_reclaim_age_lower_bound_ms gauge",
        "# HELP nasa_async_worker_lock_acquire_fail_total Total processing lock acquisition failures by worker and reason.",
        "# TYPE nasa_async_worker_lock_acquire_fail_total counter",
    ]

    baseline: Dict[str, List[Tuple[Dict[str, str], float]]] = {
        "nasa_async_worker_retry_total": [
            ({"worker": "evaluation", "reason": "processing_error"}, 0.0),
            ({"worker": "judge", "reason": "processing_error"}, 0.0),
        ],
        "nasa_async_worker_dlq_total": [
            ({"worker": "evaluation", "reason": "max_retries_exhausted"}, 0.0),
            ({"worker": "judge", "reason": "max_retries_exhausted"}, 0.0),
        ],
        "nasa_async_worker_reclaim_total": [
            ({"worker": "evaluation"}, 0.0),
            ({"worker": "judge"}, 0.0),
        ],
        "nasa_async_worker_reclaim_age_lower_bound_ms": [
            ({"worker": "evaluation"}, 0.0),
            ({"worker": "judge"}, 0.0),
        ],
        "nasa_async_worker_lock_acquire_fail_total": [
            ({"worker": "evaluation", "reason": "contended"}, 0.0),
            ({"worker": "judge", "reason": "contended"}, 0.0),
            ({"worker": "evaluation_local", "reason": "contended"}, 0.0),
        ],
    }

    emitted: set[Tuple[str, Tuple[Tuple[str, str], ...]]] = set()

    def _append_sample(metric: str, labels: Dict[str, str], value: float) -> None:
        label_items = [
            f'{_prometheus_escape_label(str(k))}="{_prometheus_escape_label(str(v))}"'
            for k, v in sorted(labels.items())
        ]
        label_text = "{" + ",".join(label_items) + "}" if label_items else ""
        lines.append(f"{metric}{label_text} {float(value):.6f}")
        emitted.add((metric, tuple(sorted((str(k), str(v)) for k, v in labels.items()))))

    snapshot = get_async_reliability_metrics().snapshot()
    for metric, samples in snapshot.items():
        for labels, value in samples:
            _append_sample(metric, labels, value)

    for metric, samples in baseline.items():
        for labels, value in samples:
            key = (metric, tuple(sorted((str(k), str(v)) for k, v in labels.items())))
            if key in emitted:
                continue
            _append_sample(metric, labels, value)

    return "\n".join(lines) + "\n"


def _format_security_prometheus(snapshot: Dict[str, Any]) -> str:
    """Render security dashboard state in Prometheus exposition format."""
    lines: List[str] = [
        "# HELP nasa_security_event_total Cumulative security events by event_type and severity.",
        "# TYPE nasa_security_event_total counter",
        "# HELP nasa_security_events_last_hour Security events observed over the last hour.",
        "# TYPE nasa_security_events_last_hour gauge",
        "# HELP nasa_security_critical_events_last_hour Critical security events observed over the last hour.",
        "# TYPE nasa_security_critical_events_last_hour gauge",
        "# HELP nasa_security_rate_limit_events_last_hour Rate limit exceeded events observed over the last hour.",
        "# TYPE nasa_security_rate_limit_events_last_hour gauge",
        "# HELP nasa_security_active_threats Current active high/critical unresolved threats.",
        "# TYPE nasa_security_active_threats gauge",
        "# HELP nasa_security_alerts_recent Number of recent raised alerts stored by the dashboard.",
        "# TYPE nasa_security_alerts_recent gauge",
        "# HELP nasa_security_coverage OWASP LLM Top 10 coverage observed from runtime events (1=true,0=false).",
        "# TYPE nasa_security_coverage gauge",
        "# HELP nasa_security_generated_at_unix Snapshot generation unix timestamp in seconds.",
        "# TYPE nasa_security_generated_at_unix gauge",
    ]

    event_severity_counts = snapshot.get("event_severity_counts", {}) or {}
    for key, count in sorted(event_severity_counts.items()):
        if isinstance(key, tuple) and len(key) == 2:
            event_type, severity = key
        else:
            event_type, severity = str(key), "unknown"
        safe_event_type = _prometheus_escape_label(str(event_type))
        safe_severity = _prometheus_escape_label(str(severity))
        lines.append(
            f'nasa_security_event_total{{event_type="{safe_event_type}",severity="{safe_severity}"}} {float(count):.6f}'
        )

    lines.extend(
        [
            f'nasa_security_events_last_hour {float(snapshot.get("events_last_hour", 0.0)):.6f}',
            f'nasa_security_critical_events_last_hour {float(snapshot.get("critical_events_last_hour", 0.0)):.6f}',
            f'nasa_security_rate_limit_events_last_hour {float(snapshot.get("rate_limit_events_last_hour", 0.0)):.6f}',
            f'nasa_security_active_threats {float(snapshot.get("active_threats", 0.0)):.6f}',
            f'nasa_security_alerts_recent {float(snapshot.get("alert_count", 0.0)):.6f}',
        ]
    )

    coverage = snapshot.get("coverage", {}) or {}
    for vulnerability, covered in sorted(coverage.items()):
        safe_vulnerability = _prometheus_escape_label(str(vulnerability))
        lines.append(f'nasa_security_coverage{{vulnerability="{safe_vulnerability}"}} {1.0 if covered else 0.0:.6f}')

    lines.append(f'nasa_security_generated_at_unix {float(snapshot.get("generated_at_unix", 0.0)):.6f}')
    return "\n".join(lines) + "\n"


def _format_analytics_prometheus(snapshot: Dict[str, Any]) -> str:
    """Render curated Evidently analytics and sink health in Prometheus format."""
    lines: List[str] = [
        "# HELP nasa_monitoring_requests_total Total monitored chat interactions.",
        "# TYPE nasa_monitoring_requests_total counter",
        "# HELP nasa_monitoring_errors_total Total monitored interactions marked as errors.",
        "# TYPE nasa_monitoring_errors_total counter",
        "# HELP nasa_monitoring_error_rate_percent Monitored error rate percentage.",
        "# TYPE nasa_monitoring_error_rate_percent gauge",
        "# HELP nasa_monitoring_latency_avg_ms Average monitored latency in milliseconds.",
        "# TYPE nasa_monitoring_latency_avg_ms gauge",
        "# HELP nasa_monitoring_latency_p95_ms P95 monitored latency in milliseconds.",
        "# TYPE nasa_monitoring_latency_p95_ms gauge",
        "# HELP nasa_monitoring_rag_scored_requests_total Total monitored requests carrying RAG scores.",
        "# TYPE nasa_monitoring_rag_scored_requests_total counter",
        "# HELP nasa_monitoring_rag_retrieval_quality_avg Average retrieval quality score from monitored RAG requests.",
        "# TYPE nasa_monitoring_rag_retrieval_quality_avg gauge",
        "# HELP nasa_monitoring_rag_faithfulness_avg Average faithfulness score from monitored RAG requests.",
        "# TYPE nasa_monitoring_rag_faithfulness_avg gauge",
        "# HELP nasa_monitoring_rag_response_relevancy_avg Average response relevancy score from monitored RAG requests.",
        "# TYPE nasa_monitoring_rag_response_relevancy_avg gauge",
        "# HELP nasa_monitoring_rag_context_precision_avg Average context precision score from monitored RAG requests.",
        "# TYPE nasa_monitoring_rag_context_precision_avg gauge",
        "# HELP nasa_monitoring_sink_queue_depth Pending records in monitoring sink write queue.",
        "# TYPE nasa_monitoring_sink_queue_depth gauge",
        "# HELP nasa_monitoring_sink_queue_capacity Maximum records in monitoring sink write queue.",
        "# TYPE nasa_monitoring_sink_queue_capacity gauge",
        "# HELP nasa_monitoring_sink_dropped_total Monitoring records that overflowed queue and required synchronous fallback.",
        "# TYPE nasa_monitoring_sink_dropped_total counter",
        "# HELP nasa_monitoring_sink_write_failures_total Monitoring sink write failures.",
        "# TYPE nasa_monitoring_sink_write_failures_total counter",
        "# HELP nasa_monitoring_mirror_write_failures_total Monitoring mirror sink write failures.",
        "# TYPE nasa_monitoring_mirror_write_failures_total counter",
        "# HELP nasa_monitoring_sink_info Monitoring sink metadata labels.",
        "# TYPE nasa_monitoring_sink_info gauge",
        "# HELP nasa_monitoring_generated_at_unix Snapshot generation unix timestamp in seconds.",
        "# TYPE nasa_monitoring_generated_at_unix gauge",
    ]

    sink_path = _prometheus_escape_label(str(snapshot.get("sink_path", "unknown")))
    sink_type = _prometheus_escape_label(str(snapshot.get("sink_type", "unknown")))
    mirror_sinks = _prometheus_escape_label(str(snapshot.get("mirror_sinks", "")))
    lines.extend(
        [
            f'nasa_monitoring_requests_total {float(snapshot.get("requests_total", 0.0)):.6f}',
            f'nasa_monitoring_errors_total {float(snapshot.get("errors_total", 0.0)):.6f}',
            f'nasa_monitoring_error_rate_percent {float(snapshot.get("error_rate_percent", 0.0)):.6f}',
            f'nasa_monitoring_latency_avg_ms {float(snapshot.get("avg_latency_ms", 0.0)):.6f}',
            f'nasa_monitoring_latency_p95_ms {float(snapshot.get("p95_latency_ms", 0.0)):.6f}',
            f'nasa_monitoring_rag_scored_requests_total {float(snapshot.get("rag_scored_requests", 0.0)):.6f}',
            f'nasa_monitoring_rag_retrieval_quality_avg {float(snapshot.get("rag_avg_retrieval_quality", 0.0)):.6f}',
            f'nasa_monitoring_rag_faithfulness_avg {float(snapshot.get("rag_avg_faithfulness", 0.0)):.6f}',
            f'nasa_monitoring_rag_response_relevancy_avg {float(snapshot.get("rag_avg_response_relevancy", 0.0)):.6f}',
            f'nasa_monitoring_rag_context_precision_avg {float(snapshot.get("rag_avg_context_precision", 0.0)):.6f}',
            f'nasa_monitoring_sink_queue_depth {float(snapshot.get("sink_queue_depth", 0.0)):.6f}',
            f'nasa_monitoring_sink_queue_capacity {float(snapshot.get("sink_queue_capacity", 0.0)):.6f}',
            f'nasa_monitoring_sink_dropped_total {float(snapshot.get("sink_dropped_total", 0.0)):.6f}',
            f'nasa_monitoring_sink_write_failures_total {float(snapshot.get("sink_write_failures_total", 0.0)):.6f}',
            f'nasa_monitoring_mirror_write_failures_total {float(snapshot.get("mirror_write_failures_total", 0.0)):.6f}',
            f'nasa_monitoring_sink_info{{path="{sink_path}",type="{sink_type}",mirrors="{mirror_sinks}"}} 1',
            f'nasa_monitoring_generated_at_unix {float(snapshot.get("generated_at_unix", 0.0)):.6f}',
        ]
    )
    return "\n".join(lines) + "\n"


def _worker_pool_series(report: Dict[str, Any]) -> Dict[str, Any]:
    """Convert worker-pool snapshots into a row-oriented JSON series for dashboards."""
    rows: List[Dict[str, Any]] = []
    workers = report.get("workers", {})
    for stage, snapshot in workers.items():
        queue_limit = float(snapshot.get("queue_limit", 0))
        capacity = float(snapshot.get("capacity", 0))
        queued_estimate = float(snapshot.get("queued_estimate", 0))
        inflight = float(snapshot.get("inflight", 0))
        rows.append(
            {
                "stage": str(stage),
                "max_workers": float(snapshot.get("max_workers", 0)),
                "queue_limit": queue_limit,
                "capacity": capacity,
                "inflight": inflight,
                "queued_estimate": queued_estimate,
                "submitted": float(snapshot.get("submitted", 0)),
                "completed": float(snapshot.get("completed", 0)),
                "rejected": float(snapshot.get("rejected", 0)),
                "failed": float(snapshot.get("failed", 0)),
                "oldest_queue_age_seconds": float(snapshot.get("oldest_queue_age_seconds", 0.0)),
                "rejected_rate": float(snapshot.get("rejected_rate", 0.0)),
                "error_rate": float(snapshot.get("error_rate", 0.0)),
                "queue_depth_ratio": (queued_estimate / queue_limit) if queue_limit > 0 else 0.0,
                "utilization_ratio": (inflight / capacity) if capacity > 0 else 0.0,
            }
        )

    return {
        "generated_at_ms": report.get("generated_at_ms", 0),
        "series": rows,
    }


class RedisSlidingWindowRateLimiter:
    """Distributed sliding-window limiter backed by Redis sorted sets."""

    LUA_SCRIPT = """
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
local request_id = ARGV[3]

local now_parts = redis.call("TIME")
local now_ms = (tonumber(now_parts[1]) * 1000) + math.floor(tonumber(now_parts[2]) / 1000)
local window_start = now_ms - window_ms

redis.call("ZREMRANGEBYSCORE", key, 0, window_start)

local current = redis.call("ZCARD", key)
local oldest = redis.call("ZRANGE", key, 0, 0, "WITHSCORES")
local retry_after_ms = window_ms
if oldest[2] then
    retry_after_ms = window_ms - (now_ms - tonumber(oldest[2]))
end
if retry_after_ms < 1 then
    retry_after_ms = 1
end

if current >= limit then
    return {0, current, retry_after_ms}
end

redis.call("ZADD", key, now_ms, request_id)
redis.call("PEXPIRE", key, window_ms)
current = current + 1
return {1, current, retry_after_ms}
"""

    def __init__(self, requests_per_period: int, period_seconds: int, paths: List[str], enabled: bool = True):
        self.requests_per_period = max(1, int(requests_per_period))
        self.period_seconds = max(1, int(period_seconds))
        exact_paths = set()
        prefix_paths = []
        for raw_path in paths:
            normalized = self._normalize_path(raw_path)
            if not normalized:
                continue
            if normalized.endswith("*"):
                prefix = normalized[:-1].rstrip("/")
                if prefix:
                    prefix_paths.append(prefix)
            else:
                exact_paths.add(normalized)
        self.paths = frozenset(exact_paths)
        self.path_prefixes = tuple(sorted(set(prefix_paths), key=len, reverse=True))
        self.enabled = enabled

    @staticmethod
    def _normalize_path(path: str) -> str:
        normalized = (path or "").strip()
        if not normalized:
            return ""
        if not normalized.startswith("/"):
            normalized = f"/{normalized}"
        if len(normalized) > 1 and normalized.endswith("/"):
            normalized = normalized.rstrip("/")
        return normalized

    def should_limit_path(self, path: str) -> bool:
        if not self.enabled:
            return False
        normalized = self._normalize_path(path)
        if normalized in self.paths:
            return True
        for prefix in self.path_prefixes:
            if normalized.startswith(prefix):
                return True
        return False

    def check(self, client_ip: str, path: str) -> Optional[Dict[str, Any]]:
        normalized_path = self._normalize_path(path)
        if not self.should_limit_path(normalized_path):
            return None

        redis_client = get_redis_client()
        if not redis_client.is_available():
            logger.warning("Rate limiting disabled for %s because Redis is unavailable", path)
            return None

        key = f"rate_limit:{normalized_path.lstrip('/').replace('/', ':')}:{client_ip}"
        window_ms = self.period_seconds * 1000
        request_id = str(uuid.uuid4())
        result = redis_client.eval(self.LUA_SCRIPT, 1, key, self.requests_per_period, window_ms, request_id)

        if not result:
            logger.warning("Rate limiting failed open for %s because Redis eval returned no result", path)
            return None

        allowed = bool(int(result[0]))
        current = int(result[1])
        retry_after_ms = max(1, int(result[2]))
        remaining = max(0, self.requests_per_period - current)
        reset_after_seconds = max(1, math.ceil(retry_after_ms / 1000))

        return {
            "allowed": allowed,
            "limit": self.requests_per_period,
            "current": current,
            "remaining": remaining,
            "retry_after_seconds": reset_after_seconds,
            "window_seconds": self.period_seconds,
            "key": key,
        }


def _apply_security_headers(response: Response) -> Response:
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    response.headers["Content-Security-Policy"] = "default-src 'self'"
    return response


def _apply_rate_limit_headers(response: Response, rate_limit: Dict[str, Any]) -> Response:
    response.headers["X-RateLimit-Limit"] = str(rate_limit["limit"])
    response.headers["X-RateLimit-Remaining"] = str(rate_limit["remaining"])
    response.headers["X-RateLimit-Reset"] = str(rate_limit["retry_after_seconds"])
    response.headers["Retry-After"] = str(rate_limit["retry_after_seconds"])
    return response

class CacheStats:
    """Track cache performance metrics for monitoring."""

    def __init__(self):
        self.hits = 0
        self.misses = 0
        self.init_times: List[float] = []

    def record_hit(self):
        self.hits += 1

    def record_miss(self, duration_ms: float):
        self.misses += 1
        self.init_times.append(duration_ms)
        if len(self.init_times) > 100:
            self.init_times = self.init_times[-100:]

    @property
    def hit_rate(self) -> float:
        total = self.hits + self.misses
        return (self.hits / total * 100) if total > 0 else 0.0

    @property
    def avg_init_ms(self) -> float:
        return sum(self.init_times) / len(self.init_times) if self.init_times else 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate_percent": round(self.hit_rate, 2),
            "avg_init_ms": round(self.avg_init_ms, 2),
            "total_requests": self.hits + self.misses,
        }


cache_stats = CacheStats()


@lru_cache(maxsize=16)  # Increased from 8 for multi-backend scenarios
def _cached_rag_init(chroma_dir: str, collection_name: str):
    """Cache RAG collection initialization with performance tracking."""
    init_start = time.perf_counter()
    result = rag_client.initialize_rag_system(chroma_dir, collection_name)
    duration_ms = (time.perf_counter() - init_start) * 1000
    cache_stats.record_miss(duration_ms)
    return result


def _get_cached_rag_init(chroma_dir: str, collection_name: str):
    """Wrapper to track cache hits/misses separately."""
    cache_info_before = _cached_rag_init.cache_info()
    collection, success, error = _cached_rag_init(chroma_dir, collection_name)
    cache_info_after = _cached_rag_init.cache_info()
    if cache_info_after.hits > cache_info_before.hits:
        cache_stats.record_hit()
    return collection, success, error


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle: startup (mission-scoped warmup) and shutdown.

    Warmup phases (executed in order):
    1. Security rule compilation — confirm all regex patterns are pre-compiled
       at module-import time so the first request incurs zero JIT cost.
    2. Collection cache — open each target ChromaDB collection and prime the
       LRU cache so every request hits immediately.
    3. Index metadata — call count()+peek() on each collection to load the
       HNSW index and SQLite metadata tables into the process cache before any
       real traffic arrives.
    """

    if PromptInjectionDetector is not None:
        n_injection = len(PromptInjectionDetector.INJECTION_PATTERNS)
        n_doc = len(PromptInjectionDetector.RETRIEVED_DOC_PATTERNS)
        n_sanitize = len(PromptInjectionDetector._SANITIZE_PATTERNS)
        n_sensitive = len(SensitiveInfoFilter.SENSITIVE_PATTERNS)
        n_strict = len(SensitiveInfoFilter.STRICT_SENSITIVE_PATTERNS)
        n_harmful = len(OutputValidator._HARMFUL_PATTERNS)
        total = n_injection + n_doc + n_sanitize + n_sensitive + n_strict + n_harmful
        logger.info(
            "Security patterns pre-compiled: %d total "
            "(injection=%d, doc=%d, sanitize=%d, sensitive=%d, strict=%d, harmful=%d)",
            total, n_injection, n_doc, n_sanitize, n_sensitive, n_strict, n_harmful,
        )
    else:
        logger.warning("Security module unavailable — pattern precompilation skipped")

    logger.info("Pre-warming RAG collection cache and index metadata...")
    backends_to_warm = [
        ("./chroma_db", "nasa_space_missions_test"),
        ("./chroma_db_openai", "nasa_space_missions_text"),
    ]
    for chroma_dir, collection_name in backends_to_warm:
        try:
            collection, success, error = _cached_rag_init(chroma_dir, collection_name)
            if success and collection is not None:
                index_info = rag_client.warm_collection_index(collection)
                if "error" in index_info:
                    logger.warning(
                        "  ~ Partial warm %s/%s: index metadata unavailable — %s",
                        chroma_dir, collection_name, index_info["error"],
                    )
                else:
                    logger.info(
                        "  ✓ Ready: %s/%s  docs=%d  index_primed=%s",
                        chroma_dir, collection_name,
                        index_info["count"], index_info["index_primed"],
                    )
            else:
                logger.warning(
                    "  - Skip (optional): %s/%s — %s",
                    chroma_dir, collection_name, error or "no collection",
                )
        except Exception as exc:
            logger.warning("  - Skip (optional): %s — %s", chroma_dir, str(exc)[:60])

    logger.info("Startup warmup complete. Cache stats: %s", cache_stats.to_dict())
    yield
    try:
        chat_workflow.shutdown()
        logger.info("Workflow executors shut down")
    except Exception as error:
        logger.warning("Workflow shutdown encountered an error: %s", str(error)[:120])
    try:
        monitor.shutdown()
        logger.info("Monitoring sink writer shut down")
    except Exception as error:
        logger.warning("Monitoring sink shutdown encountered an error: %s", str(error)[:120])
    logger.info("Shutting down NASA RAG API")


app = FastAPI(title="NASA Mission Intelligence API", version="1.0.0", lifespan=lifespan)
tracer = init_telemetry(app, service_name="nasa-mission-intelligence-api")
monitor = EvidentlyMonitor()


def _record_async_evaluation_completion(
    workflow_input: ChatWorkflowInput,
    answer: str,
    contexts: List[str],
    payload: Dict[str, Any],
) -> None:
    if not isinstance(payload, dict):
        return
    if str(payload.get("status", "")).strip().lower() != "completed":
        return

    interaction_id = (workflow_input.interaction_id or "").strip()
    if not interaction_id:
        return

    monitor.log_interaction(
        question=workflow_input.question,
        answer=answer,
        model=workflow_input.model,
        backend=f"{workflow_input.chroma_dir}:{workflow_input.collection_name}",
        context_count=len(contexts),
        mission=workflow_input.mission_filter,
        evaluation=payload,
        error=False,
        interaction_id=interaction_id,
        record_kind="evaluation_update",
        synchronous=True,
    )

rate_limiter = RedisSlidingWindowRateLimiter(
    requests_per_period=_get_rate_limit_requests_per_period(),
    period_seconds=_get_rate_limit_period_seconds(),
    paths=_get_rate_limit_paths(),
    enabled=_get_rate_limit_enabled(),
)

# Initialize security controls (LLM10: Resource limiting)
resource_limiter = ResourceLimitEnforcer(
    max_input_tokens=2000,
    max_output_tokens=1000,
    max_queries_per_minute=10,
    max_embedding_batch=100,
) if ResourceLimitEnforcer else None

# Jailbreak keywords (LLM07: System prompt protection)
JAILBREAK_KEYWORDS = [
    "system prompt", "system message", "original instructions",
    "developer mode", "admin mode", "bypass", "jailbreak",
    "ignore previous", "disregard", "forget", "override",
]

_PREFLIGHT_TIMEOUT_SECONDS = _get_preflight_timeout_seconds()
_PREFLIGHT_RETRIEVAL_MODE = _get_preflight_retrieval_mode()
_RETRIEVAL_TIMEOUT_SECONDS = _get_profiled_stage_timeout(
    "RETRIEVAL_TIMEOUT_SECONDS",
    interactive_default=1.8,
    balanced_default=1.8,
    throughput_default=2.4,
    min_value=0.2,
    max_value=10.0,
)
_GENERATION_TIMEOUT_SECONDS = _get_profiled_stage_timeout(
    "GENERATION_TIMEOUT_SECONDS",
    interactive_default=6.5,
    balanced_default=8.0,
    throughput_default=10.0,
    min_value=0.5,
    max_value=30.0,
)
_EVALUATION_TIMEOUT_SECONDS = _get_profiled_stage_timeout(
    "EVALUATION_TIMEOUT_SECONDS",
    interactive_default=2.5,
    balanced_default=3.5,
    throughput_default=5.0,
    min_value=0.5,
    max_value=20.0,
)
_JUDGE_TIMEOUT_SECONDS = _get_judge_timeout_seconds()
_QUEUE_SUBMIT_TIMEOUT_SECONDS = _get_stage_submit_timeout_seconds()
_BREAKER_FAILURE_THRESHOLD = _get_breaker_failure_threshold()
_BREAKER_RECOVERY_SECONDS = _get_breaker_recovery_seconds()
_EVALUATION_LOCAL_FALLBACK_ENABLED = _get_evaluation_local_fallback_enabled()
_EVALUATION_BROKER_ENABLED = _get_bool_env("EVALUATION_BROKER_ENABLED", default=False)
_JUDGE_BROKER_ENABLED = _get_bool_env("JUDGE_BROKER_ENABLED", default=False)
_EVALUATION_BROKER_STREAM = _get_evaluation_broker_stream()
_EVALUATION_BROKER_GROUP = _get_evaluation_broker_group()
_JUDGE_BROKER_STREAM = _get_judge_broker_stream()
_JUDGE_BROKER_GROUP = _get_judge_broker_group()

_validate_broker_lane_isolation(
    evaluation_broker_enabled=_EVALUATION_BROKER_ENABLED,
    evaluation_stream=_EVALUATION_BROKER_STREAM,
    evaluation_group=_EVALUATION_BROKER_GROUP,
    judge_broker_enabled=_JUDGE_BROKER_ENABLED,
    judge_stream=_JUDGE_BROKER_STREAM,
    judge_group=_JUDGE_BROKER_GROUP,
)

_STAGE_WORKER_COUNTS = {
    "safety": _get_profiled_stage_worker_count("SAFETY_WORKERS", 2, 3, 4),
    "retrieval": _get_profiled_stage_worker_count("RETRIEVAL_WORKERS", 4, 8, 12),
    "generation": _get_profiled_stage_worker_count("GENERATION_WORKERS", 4, 8, 12),
    "judge": _get_profiled_stage_worker_count("JUDGE_WORKERS", 1, 2, 4),
    "evaluation": _get_profiled_stage_worker_count("EVALUATION_WORKERS", 1, 2, 4),
}

_STAGE_QUEUE_LIMITS = {
    "safety": _get_profiled_stage_queue_limit("SAFETY_QUEUE_LIMIT", 120, 240, 400),
    "retrieval": _get_profiled_stage_queue_limit("RETRIEVAL_QUEUE_LIMIT", 160, 600, 1200),
    "generation": _get_profiled_stage_queue_limit("GENERATION_QUEUE_LIMIT", 160, 600, 1200),
    "judge": _get_profiled_stage_queue_limit("JUDGE_QUEUE_LIMIT", 80, 160, 240),
    "evaluation": _get_profiled_stage_queue_limit("EVALUATION_QUEUE_LIMIT", 120, 300, 500),
}

chat_workflow = MultiAgentChatWorkflow(
    get_collection_fn=_get_cached_rag_init,
    logger=logger,
    jailbreak_keywords=JAILBREAK_KEYWORDS,
    resource_limiter=resource_limiter,
    prompt_injection_detector=PromptInjectionDetector,
    vector_security_validator=VectorSecurityValidator,
    output_validator=OutputValidator,
    sensitive_info_filter=SensitiveInfoFilter,
    security_violation=SecurityViolation,
    security_auditor=security_event_sink,
    security_level=SecurityLevel,
    judge_timeout_seconds=_JUDGE_TIMEOUT_SECONDS,
    factoid_n_results=_get_depth_threshold("RETRIEVAL_FACTOID_N_RESULTS", 2),
    broad_n_results=_get_depth_threshold("RETRIEVAL_BROAD_N_RESULTS", 4),
    context_max_tokens=_get_compression_max_tokens(),
    context_dedup_threshold=_get_compression_dedup_threshold(),
    retrieval_timeout_seconds=_RETRIEVAL_TIMEOUT_SECONDS,
    preflight_timeout_seconds=_PREFLIGHT_TIMEOUT_SECONDS,
    generation_timeout_seconds=_GENERATION_TIMEOUT_SECONDS,
    evaluation_timeout_seconds=_EVALUATION_TIMEOUT_SECONDS,
    breaker_failure_threshold=_BREAKER_FAILURE_THRESHOLD,
    breaker_recovery_seconds=_BREAKER_RECOVERY_SECONDS,
    preflight_budget_ms=_get_latency_budget_ms("PREFLIGHT_BUDGET_MS", 20.0),
    retrieval_budget_ms=_get_latency_budget_ms("RETRIEVAL_BUDGET_MS", 700.0),
    generation_budget_ms=_get_latency_budget_ms("GENERATION_BUDGET_MS", 1800.0),
    evaluation_mode=_get_evaluation_mode(),
    safety_workers=_STAGE_WORKER_COUNTS["safety"],
    retrieval_workers=_STAGE_WORKER_COUNTS["retrieval"],
    generation_workers=_STAGE_WORKER_COUNTS["generation"],
    judge_workers=_STAGE_WORKER_COUNTS["judge"],
    evaluation_workers=_STAGE_WORKER_COUNTS["evaluation"],
    safety_queue_limit=_STAGE_QUEUE_LIMITS["safety"],
    retrieval_queue_limit=_STAGE_QUEUE_LIMITS["retrieval"],
    generation_queue_limit=_STAGE_QUEUE_LIMITS["generation"],
    judge_queue_limit=_STAGE_QUEUE_LIMITS["judge"],
    evaluation_queue_limit=_STAGE_QUEUE_LIMITS["evaluation"],
    queue_submit_timeout_seconds=_QUEUE_SUBMIT_TIMEOUT_SECONDS,
    preflight_retrieval_mode=_PREFLIGHT_RETRIEVAL_MODE,
    evaluation_broker_enabled=_EVALUATION_BROKER_ENABLED,
    evaluation_local_fallback_enabled=_EVALUATION_LOCAL_FALLBACK_ENABLED,
    evaluation_broker_stream=_EVALUATION_BROKER_STREAM,
    evaluation_broker_group=_EVALUATION_BROKER_GROUP,
    judge_broker_enabled=_JUDGE_BROKER_ENABLED,
    judge_broker_stream=_JUDGE_BROKER_STREAM,
    judge_broker_group=_JUDGE_BROKER_GROUP,
    redis_l2_cache_enabled=_get_bool_env("REDIS_L2_CACHE_ENABLED", default=True),
    evaluation_completion_callback=_record_async_evaluation_completion,
    stage_event_store=StageLatencyEventStore(
        log_file=_get_stage_sli_log_path(),
        retention_hours=_get_stage_sli_retention_hours(),
        max_file_bytes=_get_stage_sli_max_file_bytes(),
        max_rotated_files=_get_stage_sli_max_rotated_files(),
        maintenance_interval_seconds=_get_stage_sli_maintenance_seconds(),
    ),
)

worker_pool_event_store = WorkerPoolEventStore(
    log_file=_get_worker_pool_sli_log_path(),
    retention_hours=_get_worker_pool_sli_retention_hours(),
    max_file_bytes=_get_worker_pool_sli_max_file_bytes(),
    max_rotated_files=_get_worker_pool_sli_max_rotated_files(),
    maintenance_interval_seconds=_get_worker_pool_sli_maintenance_seconds(),
)
worker_pool_sli_sample_interval_seconds = _get_worker_pool_sli_sample_interval_seconds()
_worker_pool_sli_last_write_monotonic = 0.0
_worker_pool_sli_write_lock = Lock()


def _capture_worker_pool_report() -> Dict[str, Any]:
    global _worker_pool_sli_last_write_monotonic
    report = chat_workflow.get_worker_pool_report()

    should_write = False
    now_monotonic = time.monotonic()
    with _worker_pool_sli_write_lock:
        if worker_pool_sli_sample_interval_seconds <= 0.0:
            should_write = True
        elif (now_monotonic - _worker_pool_sli_last_write_monotonic) >= worker_pool_sli_sample_interval_seconds:
            should_write = True
        if should_write:
            _worker_pool_sli_last_write_monotonic = now_monotonic

    if should_write:
        worker_pool_event_store.record_snapshot(report)
    return report

@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    client_ip = request.client.host if request.client else "unknown"
    rate_limit_result = None

    if rate_limiter.should_limit_path(request.url.path):
        try:
            rate_limit_result = await run_in_threadpool(rate_limiter.check, client_ip, request.url.path)
        except Exception as error:
            logger.warning("Rate limit check failed open for %s %s: %s", request.method, request.url.path, error)

        if rate_limit_result and not rate_limit_result["allowed"]:
            security_dashboard.log_event(
                event_type="rate_limit_exceeded",
                severity="medium",
                user_id=client_ip,
                ip_address=client_ip,
                details={
                    "path": request.url.path,
                    "limit": rate_limit_result["limit"],
                    "window_seconds": rate_limit_result["window_seconds"],
                    "retry_after_seconds": rate_limit_result["retry_after_seconds"],
                },
            )
            response = JSONResponse(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content={
                    "detail": "Rate limit exceeded",
                    "limit": rate_limit_result["limit"],
                    "window_seconds": rate_limit_result["window_seconds"],
                    "retry_after_seconds": rate_limit_result["retry_after_seconds"],
                },
            )
            _apply_security_headers(response)
            _apply_rate_limit_headers(response, rate_limit_result)
            return response

    response = await call_next(request)
    _apply_security_headers(response)
    if rate_limit_result:
        _apply_rate_limit_headers(response, rate_limit_result)
    return response

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("ALLOWED_ORIGINS", "localhost:3000,localhost:8000").split(","),
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type"],
)


class ChatRequest(BaseModel):
    question: str = Field(..., min_length=1)
    chroma_dir: str = "./chroma_db_openai"
    collection_name: str = "nasa_space_missions_text"
    n_results: int = Field(default=3, ge=1, le=10)
    mission_filter: Optional[str] = None
    model: str = Field(default_factory=get_openai_chat_model)
    evaluate: bool = True
    judge_mode: str = Field(default_factory=_get_default_judge_mode, pattern="^(sync|async|off)$")
    conversation_history: List[Dict[str, Any]] = Field(default_factory=list)
    # Optional session id for Phoenix Sessions grouping; auto-generated when absent
    session_id: Optional[str] = None


def _normalize_conversation_history(history: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    """Keep only chat-role/content pairs and sanitize + filter for workflow safety."""
    normalized: List[Dict[str, str]] = []
    for item in history or []:
        if not isinstance(item, dict):
            continue
        role = item.get("role")
        content = item.get("content")
        if role not in {"user", "assistant", "system"}:
            continue
        if not isinstance(content, str) or not content.strip():
            continue
        normalized.append({"role": str(role), "content": content})
    return normalized


class ChatResponse(BaseModel):
    answer: str
    contexts: List[str]
    evaluation: Dict[str, Any]
    judge: Dict[str, Any]
    latency_ms: float
    backend: str
    session_id: str


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/tracing/status")
def tracing_status() -> Dict[str, Any]:
    """Return unified tracing configuration and availability status."""
    return telemetry_status()


@app.post("/chat", response_model=ChatResponse)
def chat(request: ChatRequest, http_request: Request) -> ChatResponse:
    """RAG chat endpoint with comprehensive OWASP LLM security controls.
    
    Implements:
    - LLM01: Prompt Injection Detection
    - LLM02: Sensitive Information Filtering
    - LLM05: Output Validation
    - LLM07: System Prompt Protection
    - LLM08: Vector Security Validation
    - LLM10: Rate Limiting & Resource Enforcement
    """
    openai_key = get_openai_api_key(include_chroma_fallback=False)
    if not openai_key:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="OPENAI_API_KEY is not configured",
        )

    backend_name = f"{request.chroma_dir}:{request.collection_name}"
    started = time.perf_counter()
    error_msg = None
    client_ip = http_request.client.host if http_request.client else "unknown"
    session_id = (request.session_id or "").strip() or str(uuid.uuid4())
    interaction_id = uuid.uuid4().hex

    with tracer.start_as_current_span("nasa.rag.chat") as span:
        span.set_attribute("model", request.model)
        span.set_attribute("n_results", request.n_results)
        span.set_attribute("backend", backend_name)
        # OpenInference attributes required for Phoenix Sessions page
        span.set_attribute("session.id", session_id)
        span.set_attribute("user.id", client_ip)
        span.set_attribute("openinference.span.kind", "CHAIN")
        span_context = span.get_span_context()
        trace_span_id = format(span_context.span_id, "016x") if span_context and span_context.is_valid else None

        workflow_input = ChatWorkflowInput(
            question=request.question,
            chroma_dir=request.chroma_dir,
            collection_name=request.collection_name,
            n_results=request.n_results,
            mission_filter=request.mission_filter,
            model=request.model,
            evaluate=request.evaluate,
            judge_mode=request.judge_mode,
            conversation_history=_normalize_conversation_history(request.conversation_history),
            client_ip=client_ip,
            trace_span_id=trace_span_id,
            session_id=session_id,
            interaction_id=interaction_id,
        )

        try:
            workflow_result = chat_workflow.run(
                workflow_input=workflow_input,
                openai_key=openai_key,
            )

            latency_ms = (time.perf_counter() - started) * 1000.0
            span.set_attribute("latency_ms", latency_ms)
            span.set_attribute("context_count", len(workflow_result.contexts))
            span.set_attribute("judge_mode", request.judge_mode)
            span.set_attribute("judge_source", str(workflow_result.judge.get("source", "unknown")))
            span.set_attribute("judge_timeout", _judge_timed_out(workflow_result.judge))
            span.set_attribute("judge_passed", bool(workflow_result.judge.get("passed", True)))
            span.set_attribute("error", False)

            if trace_span_id:
                annotation_scores = _collect_numeric_scores(
                    workflow_result.evaluation,
                    workflow_result.judge,
                    {"latency_ms": latency_ms},
                )
                _post_phoenix_annotations(
                    span_id=trace_span_id,
                    scores=annotation_scores,
                )

            monitor.log_interaction(
                question=request.question,
                answer=workflow_result.answer,
                model=request.model,
                backend=backend_name,
                context_count=len(workflow_result.contexts),
                mission=request.mission_filter,
                evaluation=workflow_result.evaluation if isinstance(workflow_result.evaluation, dict) else None,
                error=False,
                latency_ms=latency_ms,
                interaction_id=interaction_id,
            )

            return ChatResponse(
                answer=workflow_result.answer,
                contexts=workflow_result.contexts,
                evaluation=workflow_result.evaluation,
                judge=workflow_result.judge,
                latency_ms=latency_ms,
                backend=backend_name,
                session_id=session_id,
            )

        except WorkflowError as error:
            if error.status_code in {
                status.HTTP_400_BAD_REQUEST,
                status.HTTP_403_FORBIDDEN,
                status.HTTP_429_TOO_MANY_REQUESTS,
            }:
                security_dashboard.log_event(
                    event_type="security_violation",
                    severity="high" if error.status_code == status.HTTP_403_FORBIDDEN else "medium",
                    user_id=client_ip,
                    ip_address=client_ip,
                    details={
                        "status_code": error.status_code,
                        "detail": error.detail,
                        "backend": backend_name,
                    },
                )
            raise HTTPException(status_code=error.status_code, detail=error.detail)
        except HTTPException:
            raise
        except Exception as error:
            error_msg = str(error)
            logger.error(f"Unexpected error in /chat: {error_msg}")
            latency_ms = (time.perf_counter() - started) * 1000.0
            span.set_attribute("error", True)
            span.set_attribute("error_message", error_msg[:100])
            monitor.log_interaction(
                question=request.question,
                answer="[ERROR] Request failed",
                model=request.model,
                backend=backend_name,
                context_count=0,
                mission=request.mission_filter,
                evaluation={"error": error_msg[:200]},
                error=True,
                latency_ms=latency_ms,
                interaction_id=interaction_id,
            )
            security_dashboard.log_event(
                event_type="api_error",
                severity="high",
                user_id=client_ip,
                ip_address=client_ip,
                details={
                    "backend": backend_name,
                    "error": error_msg[:200],
                    "route": "/chat",
                },
            )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Internal server error: {error_msg[:100]}",
            )


@app.get("/monitoring/report")
def monitoring_report(reference_rows: int = 100) -> Dict[str, str]:
    """Generate Evidently drift report from interaction logs."""
    return monitor.build_drift_report(reference_rows=reference_rows)


@app.get("/monitoring/analytics")
def monitoring_analytics() -> Dict[str, Any]:
    """Return latency/error rollups from monitoring logs."""
    return monitor.get_analytics_summary()


@app.get("/monitoring/analytics/prometheus", response_class=Response)
def monitoring_analytics_prometheus() -> Response:
    """Return curated analytics and sink health metrics in Prometheus text format."""
    payload = _format_analytics_prometheus(monitor.get_prometheus_curated_snapshot())
    return Response(content=payload, media_type="text/plain; version=0.0.4; charset=utf-8")


@app.get("/monitoring/rag")
def monitoring_rag(recent_failures_limit: int = 20) -> Dict[str, Any]:
    """Return RAG-specific rollups built from RAGAS scores and retrieval metadata."""
    return monitor.get_rag_dashboard_summary(recent_failures_limit=recent_failures_limit)


@app.get("/monitoring/rag/report")
def monitoring_rag_report(reference_rows: int = 100) -> Dict[str, str]:
    """Generate an Evidently HTML report for RAG-specific score trends."""
    return monitor.build_rag_report(reference_rows=reference_rows)


@app.get("/monitoring/judge")
def monitoring_judge(limit: int = 20) -> Dict[str, Any]:
    """Return recent async judge results from in-memory workflow buffer."""
    results = chat_workflow.get_recent_judge_results(limit=limit)
    return {
        "count": len(results),
        "results": results,
    }


@app.get("/judge/last")
def judge_last() -> Dict[str, Any]:
    """Return latest async judge result."""
    last = chat_workflow.get_last_judge_result()
    return {
        "available": bool(last),
        "result": last,
    }


@app.get("/monitoring/evaluation")
def monitoring_evaluation(limit: int = 20) -> Dict[str, Any]:
    """Return recent async evaluation jobs from in-memory workflow buffer."""
    results = chat_workflow.get_recent_evaluation_jobs(limit=limit)
    return {
        "count": len(results),
        "results": results,
    }


@app.get("/monitoring/cache")
def monitoring_cache() -> Dict[str, Any]:
    """Return cache statistics and effectiveness metrics for observability."""
    init_stats = cache_stats.to_dict()
    lru_info = _cached_rag_init.cache_info()
    workflow_cache_stats = chat_workflow.get_cache_stats()
    
    return {
        "rag_init": init_stats,
        "lru_cache": {
            "hits": lru_info.hits,
            "misses": lru_info.misses,
            "currsize": lru_info.currsize,
            "maxsize": lru_info.maxsize,
            "hit_rate_percent": round(
                (lru_info.hits / (lru_info.hits + lru_info.misses) * 100)
                if (lru_info.hits + lru_info.misses) > 0
                else 0.0,
                2
            ),
        },
        "workflow": workflow_cache_stats,
        "timestamp_utc": time.time(),
    }


@app.get("/evaluation/{job_id}")
def evaluation_job(job_id: str) -> Dict[str, Any]:
    """Return one async evaluation job by id."""
    result = chat_workflow.get_evaluation_job(job_id)
    return {
        "available": bool(result),
        "job_id": job_id,
        "result": result,
    }


@app.get("/collections/clear-cache")
def clear_cache_endpoint() -> Dict[str, str]:
    """Clear the LRU cache for RAG collection initialization."""
    _cached_rag_init.cache_clear()
    logger.info("Cache cleared by request")
    return {"status": "cache cleared"}


@app.get("/cache/stats")
def cache_stats_endpoint() -> Dict[str, Any]:
    """Get cache performance statistics and LRU info."""
    stats = cache_stats.to_dict()
    lru_info = _cached_rag_init.cache_info()
    stats["lru_info"] = {
        "hits": lru_info.hits,
        "misses": lru_info.misses,
        "maxsize": lru_info.maxsize,
        "currsize": lru_info.currsize,
    }
    return stats


@app.get("/monitoring/client-caches")
def monitoring_client_caches() -> Dict[str, Any]:
    """Return lightweight reuse metrics for process-level client/resource caches."""
    return {
        "openai_client": llm_client.get_openai_client_cache_metrics(),
        "rag_client": rag_client.get_client_cache_metrics(),
        "ragas_evaluator": ragas_evaluator.get_evaluator_cache_metrics(),
    }


@app.get("/monitoring/config")
def monitoring_config() -> Dict[str, Any]:
    """Return effective runtime config values relevant to operations."""
    stage_pools = {
        stage: {
            "workers": _STAGE_WORKER_COUNTS[stage],
            "queue_limit": _STAGE_QUEUE_LIMITS[stage],
        }
        for stage in _STAGE_WORKER_COUNTS
    }

    return {
        "generated_at_ms": round(time.time() * 1000),
        "api_profile": _get_api_profile(),
        "runtime_modes": {
            "preflight_retrieval": _PREFLIGHT_RETRIEVAL_MODE,
            "evaluation_local_fallback_enabled": _EVALUATION_LOCAL_FALLBACK_ENABLED,
        },
        "timeouts_seconds": {
            "preflight": _PREFLIGHT_TIMEOUT_SECONDS,
            "retrieval": _RETRIEVAL_TIMEOUT_SECONDS,
            "generation": _GENERATION_TIMEOUT_SECONDS,
            "evaluation": _EVALUATION_TIMEOUT_SECONDS,
            "judge": _JUDGE_TIMEOUT_SECONDS,
            "queue_submit": _QUEUE_SUBMIT_TIMEOUT_SECONDS,
        },
        "breaker": {
            "failure_threshold": _BREAKER_FAILURE_THRESHOLD,
            "recovery_seconds": _BREAKER_RECOVERY_SECONDS,
        },
        "stage_pools": stage_pools,
    }


@app.get("/monitoring/latency-sli")
def monitoring_latency_sli() -> Dict[str, Any]:
    """Return per-stage latency SLIs with budget compliance and timeout rate."""
    return chat_workflow.get_latency_sli_report()


@app.get("/monitoring/worker-pools")
def monitoring_worker_pools() -> Dict[str, Any]:
    """Return bounded stage worker-pool utilization and saturation counters."""
    return _capture_worker_pool_report()


@app.get("/monitoring/worker-pools/series")
def monitoring_worker_pools_series() -> Dict[str, Any]:
    """Return row-oriented worker-pool stage metrics for dashboard consumption."""
    report = _capture_worker_pool_report()
    return _worker_pool_series(report)


@app.get("/monitoring/worker-pools/timeseries")
def monitoring_worker_pools_timeseries(
    stage: Optional[str] = None,
    window_minutes: int = 60,
    bucket_seconds: int = 300,
) -> Dict[str, Any]:
    """Return bucketed worker-pool saturation snapshots for dashboard correlation."""
    _capture_worker_pool_report()
    try:
        return worker_pool_event_store.get_timeseries(
            stage=stage,
            window_minutes=window_minutes,
            bucket_seconds=bucket_seconds,
        )
    except ValueError as error:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(error)) from error


@app.get("/monitoring/worker-pools/prometheus", response_class=Response)
def monitoring_worker_pools_prometheus() -> Response:
    """Return worker-pool saturation metrics in Prometheus text format."""
    report = _capture_worker_pool_report()
    payload = _format_worker_pool_prometheus(report)
    payload += _format_runtime_config_prometheus(monitoring_config())
    payload += _format_async_reliability_prometheus()
    return Response(content=payload, media_type="text/plain; version=0.0.4; charset=utf-8")


@app.get("/monitoring/cache/stats")
def monitoring_cache_stats() -> Dict[str, Any]:
    """Return workflow L1 and L2 cache statistics."""
    return chat_workflow.get_cache_stats()


@app.get("/monitoring/latency-sli/timeseries")
def monitoring_latency_sli_timeseries(
    stage: Optional[str] = None,
    window_minutes: int = 60,
    bucket_seconds: int = 300,
    mission: Optional[str] = None,
    backend: Optional[str] = None,
    model: Optional[str] = None,
) -> Dict[str, Any]:
    """Return bucketed time-series stage SLIs from persisted NDJSON events."""
    try:
        return chat_workflow.get_latency_sli_timeseries(
            stage=stage,
            window_minutes=window_minutes,
            bucket_seconds=bucket_seconds,
            mission=mission,
            backend=backend,
            model=model,
        )
    except ValueError as error:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(error)) from error


@app.get("/monitoring/security")
def monitoring_security_overview() -> Dict[str, Any]:
    """Return high-level security dashboard telemetry."""
    return {
        "statistics": security_dashboard.get_statistics(),
        "threat_summary": security_dashboard.get_threat_summary(),
    }


@app.get("/monitoring/security/alerts")
def monitoring_security_alerts() -> Dict[str, Any]:
    """Return recent security alerts raised by threshold rules."""
    alerts = security_dashboard.get_alerts()
    return {
        "count": len(alerts),
        "alerts": alerts,
    }


@app.get("/monitoring/security/events")
def monitoring_security_events(
    limit: int = 50,
    severity: Optional[str] = None,
) -> Dict[str, Any]:
    """Return recent security events with optional severity filtering."""
    events = security_dashboard.get_events(limit=max(1, min(limit, 500)), severity=severity)
    return {
        "count": len(events),
        "events": events,
    }


@app.get("/monitoring/security/coverage")
def monitoring_security_coverage() -> Dict[str, Any]:
    """Return OWASP LLM Top 10 coverage based on observed event types."""
    return security_dashboard.get_vulnerability_coverage()


@app.get("/monitoring/security/prometheus", response_class=Response)
def monitoring_security_prometheus() -> Response:
    """Return security telemetry in Prometheus text format."""
    payload = _format_security_prometheus(security_dashboard.get_metrics_snapshot())
    return Response(content=payload, media_type="text/plain; version=0.0.4; charset=utf-8")


@app.post("/collections/warm-cache")
def warm_cache_endpoint(backends: Optional[List[Dict[str, str]]] = None) -> Dict[str, Any]:
    """Pre-warm cache for backends (bulk initialization)."""
    if backends is None:
        backends = [
            {"chroma_dir": "./chroma_db", "collection_name": "nasa_space_missions_test"},
            {"chroma_dir": "./chroma_db_openai", "collection_name": "nasa_space_missions_text"},
        ]
    
    results = {}
    for backend in backends:
        chroma_dir = backend.get("chroma_dir")
        collection_name = backend.get("collection_name")
        if not chroma_dir or not collection_name:
            continue
        try:
            _cached_rag_init(chroma_dir, collection_name)
            results[f"{chroma_dir}:{collection_name}"] = "warmed"
        except Exception as e:
            results[f"{chroma_dir}:{collection_name}"] = f"error: {str(e)[:50]}"
    
    return {
        "status": "warmup complete",
        "backends_warmed": results,
        "cache_stats": cache_stats.to_dict(),
    }
