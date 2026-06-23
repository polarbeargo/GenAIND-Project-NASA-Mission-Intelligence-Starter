"""Orchestrator for parallel multi-agent chat processing."""

from __future__ import annotations

import hashlib
import logging
import re
import time
import uuid
from collections import OrderedDict, deque
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from dataclasses import dataclass, field, replace
from threading import Lock, Semaphore
from typing import Any, Callable, Dict, List

import rag_client
from monitoring.stage_sli_events import StageLatencyEventStore
from infra.redis_cache import RedisL2Cache
from infra.redis_job_store import RedisAsyncJobStore
from infra.redis_evaluation_broker import RedisEvaluationBroker
from infra.redis_judge_broker import RedisJudgeBroker
from infra.redis_client import get_redis_client
from multi_agent.context_compression import (
    CompressionConfig,
    ContextCompressor,
    DeduplicatingCompressor,
)
from multi_agent.models import (
    ChatWorkflowInput,
    ChatWorkflowResult,
    RetrievalResult,
    WorkflowError,
)
from phoenix_annotations import collect_annotation_scores, phoenix_base_url, post_span_annotations
from multi_agent.retrieval_depth import (
    HeuristicRetrievalDepthConfig,
    HeuristicRetrievalDepthPolicy,
    RetrievalDepthPolicy,
)
from multi_agent.workers import AnalysisWorker, JudgeWorker, RetrievalWorker, SafetyWorker


@dataclass
class StageCircuitBreaker:
    """Minimal circuit breaker for stage-level failure isolation."""

    failure_threshold: int
    recovery_seconds: float
    consecutive_failures: int = 0
    opened_until: float = 0.0
    lock: Lock = field(default_factory=Lock)

    def allow(self) -> bool:
        with self.lock:
            return time.time() >= self.opened_until

    def record_success(self) -> None:
        with self.lock:
            self.consecutive_failures = 0
            self.opened_until = 0.0

    def record_failure(self) -> None:
        with self.lock:
            self.consecutive_failures += 1
            if self.consecutive_failures >= self.failure_threshold:
                self.opened_until = time.time() + self.recovery_seconds


@dataclass
class StageSLITracker:
    """Thread-safe in-memory latency/timeout samples for one workflow stage."""

    max_samples: int = 1000
    samples: deque[tuple[float, bool]] = field(default_factory=deque)
    lock: Lock = field(default_factory=Lock)
    total_requests: int = 0
    timeout_count: int = 0

    def record(self, latency_ms: float, timed_out: bool = False) -> None:
        safe_latency = max(0.0, float(latency_ms))
        with self.lock:
            self.total_requests += 1
            if timed_out:
                self.timeout_count += 1
            self.samples.append((safe_latency, bool(timed_out)))
            while len(self.samples) > self.max_samples:
                self.samples.popleft()

    @staticmethod
    def _percentile(sorted_values: list[float], percentile: float) -> float:
        if not sorted_values:
            return 0.0
        if len(sorted_values) == 1:
            return sorted_values[0]
        idx = int(round((percentile / 100.0) * (len(sorted_values) - 1)))
        idx = max(0, min(idx, len(sorted_values) - 1))
        return sorted_values[idx]

    def snapshot(self, budget_ms: float) -> Dict[str, Any]:
        safe_budget = max(0.0, float(budget_ms))
        with self.lock:
            sample_copy = list(self.samples)
            total = self.total_requests
            timeouts = self.timeout_count

        latencies = sorted(item[0] for item in sample_copy)
        within_budget = sum(1 for latency, timed_out in sample_copy if (not timed_out and latency <= safe_budget))
        sample_count = len(sample_copy)
        timeout_rate = (timeouts / total) if total else 0.0
        within_budget_rate = (within_budget / sample_count) if sample_count else 0.0

        return {
            "total_requests": total,
            "sample_count": sample_count,
            "timeouts": timeouts,
            "timeout_rate": round(timeout_rate, 4),
            "timeout_rate_percent": round(timeout_rate * 100.0, 2),
            "p50_ms": round(self._percentile(latencies, 50.0), 2),
            "p95_ms": round(self._percentile(latencies, 95.0), 2),
            "budget_ms": round(safe_budget, 2),
            "within_budget_rate": round(within_budget_rate, 4),
            "within_budget_rate_percent": round(within_budget_rate * 100.0, 2),
        }


class StageOverloadError(RuntimeError):
    """Raised when a stage queue is saturated and cannot accept more work."""


class BoundedExecutor:
    """ThreadPoolExecutor wrapper with bounded in-flight + queued tasks.

    The semaphore enforces backpressure so high-latency downstream calls cannot
    create unbounded queue growth under burst traffic.
    """

    def __init__(
        self,
        *,
        max_workers: int,
        queue_limit: int,
        submit_timeout_seconds: float,
        thread_name_prefix: str,
    ):
        self._executor = ThreadPoolExecutor(
            max_workers=max(1, int(max_workers)),
            thread_name_prefix=thread_name_prefix,
        )
        self.max_workers = max(1, int(max_workers))
        self.queue_limit = max(1, int(queue_limit))
        self.capacity = self.max_workers + self.queue_limit
        self.submit_timeout_seconds = max(0.0, float(submit_timeout_seconds))
        self._permits = Semaphore(self.capacity)
        self._inflight = 0
        self._submitted = 0
        self._completed = 0
        self._rejected = 0
        self._failed = 0
        self._queued_submitted_at: deque[float] = deque()
        self._lock = Lock()
        self._accepting_submissions = True

    def submit(self, fn, *args, **kwargs):
        with self._lock:
            if not self._accepting_submissions:
                self._rejected += 1
                raise StageOverloadError("stage executor is shutting down")

        acquired = self._permits.acquire(timeout=self.submit_timeout_seconds)
        if not acquired:
            with self._lock:
                self._rejected += 1
            raise StageOverloadError("stage queue is saturated")

        with self._lock:
            self._submitted += 1
            self._inflight += 1
            if self._inflight > self.max_workers:
                self._queued_submitted_at.append(time.time())

        future = self._executor.submit(fn, *args, **kwargs)

        def _release(_future):
            self._permits.release()
            with self._lock:
                self._inflight = max(0, self._inflight - 1)
                self._completed += 1
                if self._queued_submitted_at:
                    # Each completion allows one queued task to start running.
                    self._queued_submitted_at.popleft()
                try:
                    if _future.exception() is not None:
                        self._failed += 1
                except Exception:
                    # Cancelled futures can raise here; treat as non-success.
                    self._failed += 1

        future.add_done_callback(_release)
        return future

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            now = time.time()
            queued = max(0, self._inflight - self.max_workers)
            # Keep queue timestamp tracking bounded to active queued tasks.
            while len(self._queued_submitted_at) > queued:
                self._queued_submitted_at.popleft()
            if queued == 0:
                self._queued_submitted_at.clear()

            oldest_queue_age_seconds = 0.0
            if self._queued_submitted_at:
                oldest_queue_age_seconds = max(0.0, now - self._queued_submitted_at[0])

            submission_total = self._submitted + self._rejected
            rejected_rate = (self._rejected / submission_total) if submission_total else 0.0
            error_rate = (self._failed / self._completed) if self._completed else 0.0

            return {
                "max_workers": self.max_workers,
                "queue_limit": self.queue_limit,
                "capacity": self.capacity,
                "accepting_submissions": self._accepting_submissions,
                "inflight": self._inflight,
                "queued_estimate": queued,
                "submitted": self._submitted,
                "completed": self._completed,
                "rejected": self._rejected,
                "failed": self._failed,
                "oldest_queue_age_seconds": round(oldest_queue_age_seconds, 4),
                "rejected_rate": round(rejected_rate, 6),
                "error_rate": round(error_rate, 6),
            }

    def begin_shutdown(self) -> None:
        """Stop accepting new submissions while allowing in-flight work to finish."""
        with self._lock:
            self._accepting_submissions = False

    def wait_for_drain(self, timeout_seconds: float, poll_interval_seconds: float = 0.01) -> bool:
        """Wait briefly for in-flight/queued work to drain.

        Returns True when drained before timeout, else False.
        """
        deadline = time.monotonic() + max(0.0, float(timeout_seconds))
        poll_interval = max(0.001, float(poll_interval_seconds))
        while time.monotonic() <= deadline:
            with self._lock:
                if self._inflight <= 0:
                    return True
            time.sleep(poll_interval)
        return False

    def shutdown(self, wait: bool = False, cancel_futures: bool = False) -> None:
        self._executor.shutdown(wait=wait, cancel_futures=cancel_futures)


class MultiAgentChatWorkflow:
    """Coordinates retrieval, safety, and analysis workers."""

    def __init__(
        self,
        get_collection_fn,
        logger: logging.Logger,
        jailbreak_keywords,
        resource_limiter,
        prompt_injection_detector,
        vector_security_validator,
        output_validator,
        sensitive_info_filter,
        security_violation,
        security_auditor,
        security_level,
        judge_timeout_seconds: float = 2.5,
        retrieval_cache_ttl_seconds: int = 180,
        answer_cache_ttl_seconds: int = 240,
        retrieval_cache_max_entries: int = 500,
        answer_cache_max_entries: int = 500,
        retrieval_depth_policy: RetrievalDepthPolicy | None = None,
        factoid_n_results: int = 2,
        broad_n_results: int = 4,
        context_compressor: ContextCompressor | None = None,
        context_max_tokens: int = 2000,
        context_dedup_threshold: float = 0.85,
        retrieval_timeout_seconds: float = 1.8,
        preflight_timeout_seconds: float = 0.5,
        generation_timeout_seconds: float = 8.0,
        evaluation_timeout_seconds: float = 3.5,
        breaker_failure_threshold: int = 3,
        breaker_recovery_seconds: float = 20.0,
        preflight_budget_ms: float = 20.0,
        retrieval_budget_ms: float = 700.0,
        generation_budget_ms: float = 1800.0,
        evaluation_mode: str = "async",
        evaluation_buffer_size: int = 500,
        stage_event_store: StageLatencyEventStore | None = None,
        safety_workers: int = 2,
        retrieval_workers: int = 4,
        generation_workers: int = 4,
        judge_workers: int = 2,
        evaluation_workers: int = 2,
        safety_queue_limit: int = 200,
        retrieval_queue_limit: int = 200,
        generation_queue_limit: int = 200,
        judge_queue_limit: int = 100,
        evaluation_queue_limit: int = 200,
        queue_submit_timeout_seconds: float = 0.05,
        preflight_retrieval_mode: str = "strict",
        evaluation_broker_enabled: bool = False,
        evaluation_local_fallback_enabled: bool = True,
        evaluation_broker_stream: str = "eval:jobs",
        evaluation_broker_group: str = "eval-workers",
        judge_broker_enabled: bool = False,
        judge_broker_stream: str = "judge:jobs",
        judge_broker_group: str = "judge-workers",
        redis_l2_cache_enabled: bool = False,
        evaluation_completion_callback: Callable[[ChatWorkflowInput, str, List[str], Dict[str, Any]], None] | None = None,
    ):
        self.retrieval_worker = RetrievalWorker(get_collection_fn=get_collection_fn)
        self.safety_worker = SafetyWorker(
            logger=logger,
            jailbreak_keywords=jailbreak_keywords,
            resource_limiter=resource_limiter,
            prompt_injection_detector=prompt_injection_detector,
            vector_security_validator=vector_security_validator,
            output_validator=output_validator,
            sensitive_info_filter=sensitive_info_filter,
            security_violation=security_violation,
            security_auditor=security_auditor,
            security_level=security_level,
        )
        self.analysis_worker = AnalysisWorker(
            logger=logger,
            security_violation=security_violation,
        )
        self.judge_worker = JudgeWorker(
            logger=logger,
            output_validator=output_validator,
            sensitive_info_filter=sensitive_info_filter,
            judge_timeout_seconds=judge_timeout_seconds,
        )
        self._safety_executor = BoundedExecutor(
            max_workers=safety_workers,
            queue_limit=safety_queue_limit,
            submit_timeout_seconds=queue_submit_timeout_seconds,
            thread_name_prefix="nasa-safety-worker",
        )
        self._retrieval_executor = BoundedExecutor(
            max_workers=retrieval_workers,
            queue_limit=retrieval_queue_limit,
            submit_timeout_seconds=queue_submit_timeout_seconds,
            thread_name_prefix="nasa-retrieval-worker",
        )
        self._generation_executor = BoundedExecutor(
            max_workers=generation_workers,
            queue_limit=generation_queue_limit,
            submit_timeout_seconds=queue_submit_timeout_seconds,
            thread_name_prefix="nasa-generation-worker",
        )
        self._judge_executor = BoundedExecutor(
            max_workers=judge_workers,
            queue_limit=judge_queue_limit,
            submit_timeout_seconds=queue_submit_timeout_seconds,
            thread_name_prefix="nasa-judge-worker",
        )
        self._eval_executor = BoundedExecutor(
            max_workers=evaluation_workers,
            queue_limit=evaluation_queue_limit,
            submit_timeout_seconds=queue_submit_timeout_seconds,
            thread_name_prefix="nasa-eval-worker",
        )
        self._eval_job_executor = BoundedExecutor(
            max_workers=evaluation_workers,
            queue_limit=evaluation_queue_limit,
            submit_timeout_seconds=queue_submit_timeout_seconds,
            thread_name_prefix="nasa-eval-job-worker",
        )
        self._judge_results = deque(maxlen=200)
        self._evaluation_results: OrderedDict[str, Dict[str, Any]] = OrderedDict()
        self._judge_lock = Lock()
        self._evaluation_lock = Lock()
        self._evaluation_completion_callback = evaluation_completion_callback
        self._retrieval_cache_ttl = max(60, int(retrieval_cache_ttl_seconds))
        self._answer_cache_ttl = max(60, int(answer_cache_ttl_seconds))
        self._retrieval_cache_max_entries = max(100, int(retrieval_cache_max_entries))
        self._answer_cache_max_entries = max(100, int(answer_cache_max_entries))
        self._retrieval_cache: OrderedDict[str, tuple[float, Any]] = OrderedDict()
        self._answer_cache: OrderedDict[str, tuple[float, str]] = OrderedDict()
        self._cache_lock = Lock()
        
        # Cache statistics for observability
        self._cache_stats_lock = Lock()
        self._retrieval_cache_hits = 0
        self._retrieval_cache_misses = 0
        self._answer_cache_hits = 0
        self._answer_cache_misses = 0
        self._redis_cache_hits = 0
        self._redis_cache_misses = 0
        
        # Redis L2 cache: shared across pods, fallback gracefully if unavailable
        redis_client = get_redis_client()
        self._redis_l2_cache = RedisL2Cache(
            redis_client,
            retrieval_ttl_seconds=self._retrieval_cache_ttl,
            response_ttl_seconds=self._answer_cache_ttl,
        )
        self._redis_l2_cache_enabled = bool(redis_l2_cache_enabled)
        
        # Redis async job store: track judge/evaluation results shared across pods
        self._redis_job_store = RedisAsyncJobStore(redis_client, retention_ttl_seconds=3600)
        self._evaluation_broker = RedisEvaluationBroker(
            redis_client,
            stream_name=evaluation_broker_stream,
            consumer_group=evaluation_broker_group,
            enabled=evaluation_broker_enabled,
        )
        self._judge_broker = RedisJudgeBroker(
            redis_client,
            stream_name=judge_broker_stream,
            consumer_group=judge_broker_group,
            enabled=judge_broker_enabled,
        )
        self._retrieval_depth_policy = retrieval_depth_policy or HeuristicRetrievalDepthPolicy(
            HeuristicRetrievalDepthConfig(
                factoid_n_results=max(1, int(factoid_n_results)),
                broad_n_results=max(1, int(broad_n_results)),
            )
        )
        self._context_compressor: ContextCompressor = context_compressor or DeduplicatingCompressor(
            CompressionConfig(
                max_tokens=max(200, int(context_max_tokens)),
                similarity_threshold=max(0.5, min(1.0, float(context_dedup_threshold))),
            )
        )
        self._retrieval_timeout_seconds = max(0.2, float(retrieval_timeout_seconds))
        self._preflight_timeout_seconds = max(0.05, float(preflight_timeout_seconds))
        self._generation_timeout_seconds = max(0.5, float(generation_timeout_seconds))
        self._evaluation_timeout_seconds = max(0.5, float(evaluation_timeout_seconds))
        threshold = max(1, int(breaker_failure_threshold))
        recovery_seconds = max(1.0, float(breaker_recovery_seconds))
        self._retrieval_breaker = StageCircuitBreaker(threshold, recovery_seconds)
        self._generation_breaker = StageCircuitBreaker(threshold, recovery_seconds)
        self._evaluation_breaker = StageCircuitBreaker(threshold, recovery_seconds)
        self._stage_sli = {
            "preflight": StageSLITracker(),
            "retrieval": StageSLITracker(),
            "generation": StageSLITracker(),
            "evaluation": StageSLITracker(),
        }
        self._stage_budgets_ms = {
            "preflight": max(1.0, float(preflight_budget_ms)),
            "retrieval": max(1.0, float(retrieval_budget_ms)),
            "generation": max(1.0, float(generation_budget_ms)),
            "evaluation": max(1.0, float(self._evaluation_timeout_seconds * 1000.0)),
        }
        self._evaluation_mode = (
            evaluation_mode.strip().lower() if evaluation_mode and evaluation_mode.strip() else "async"
        )
        if self._evaluation_mode not in {"async", "sync", "off"}:
            self._evaluation_mode = "async"
        normalized_preflight_mode = (preflight_retrieval_mode or "strict").strip().lower()
        if normalized_preflight_mode not in {"strict", "fastest"}:
            normalized_preflight_mode = "strict"
        self._preflight_retrieval_mode = normalized_preflight_mode
        self._evaluation_local_fallback_enabled = bool(evaluation_local_fallback_enabled)
        self._evaluation_buffer_size = max(100, int(evaluation_buffer_size))
        self._stage_event_store = stage_event_store or StageLatencyEventStore()

    def _record_stage_metric(
        self,
        stage: str,
        latency_ms: float,
        timed_out: bool = False,
        status: str = "ok",
        mission: str | None = None,
        backend: str | None = None,
        model: str | None = None,
    ) -> None:
        self._stage_sli[stage].record(latency_ms, timed_out=timed_out)
        self._stage_event_store.record(
            stage=stage,
            latency_ms=latency_ms,
            timed_out=timed_out,
            budget_ms=self._stage_budgets_ms.get(stage, 0.0),
            status=status,
            mission=mission,
            backend=backend,
            model=model,
        )

    def run(self, workflow_input: ChatWorkflowInput, openai_key: str) -> ChatWorkflowResult:
        effective_n_results = self._effective_retrieval_depth(workflow_input)
        effective_input = replace(workflow_input, n_results=effective_n_results)
        backend_name = f"{effective_input.chroma_dir}:{effective_input.collection_name}".lower()

        retrieval_key = self._retrieval_cache_key(effective_input)
        answer_key = self._answer_cache_key(effective_input)
        prestarted_retrieval_future = None
        prestarted_retrieval_started = 0.0
        prestarted_retrieval_submit_error: str | None = None

        if self._preflight_retrieval_mode == "fastest":
            if not self._retrieval_breaker.allow():
                prestarted_retrieval_submit_error = "retrieval circuit breaker open"
            else:
                prestarted_retrieval_started = time.perf_counter()
                try:
                    prestarted_retrieval_future = self._retrieval_executor.submit(
                        self.retrieval_worker.run,
                        effective_input,
                    )
                except StageOverloadError:
                    prestarted_retrieval_submit_error = "retrieval queue saturated"
                    self._record_stage_metric(
                        "retrieval",
                        latency_ms=0.0,
                        timed_out=False,
                        status="overload",
                        mission=effective_input.mission_filter,
                        backend=backend_name,
                        model=effective_input.model,
                    )

        preflight_started = time.perf_counter()
        try:
            preflight_future = self._safety_executor.submit(self.safety_worker.preflight, workflow_input)
            preflight_result = self._await_result(preflight_future, timeout=self._preflight_timeout_seconds)
        except StageOverloadError as error:
            preflight_latency_ms = (time.perf_counter() - preflight_started) * 1000.0
            self._record_stage_metric(
                "preflight",
                preflight_latency_ms,
                timed_out=False,
                status="overload",
                mission=effective_input.mission_filter,
                backend=backend_name,
                model=effective_input.model,
            )
            raise WorkflowError(status_code=429, detail="Safety stage is overloaded. Please retry.") from error
        except TimeoutError as error:
            preflight_latency_ms = (time.perf_counter() - preflight_started) * 1000.0
            self._record_stage_metric(
                "preflight",
                preflight_latency_ms,
                timed_out=True,
                status="timeout",
                mission=effective_input.mission_filter,
                backend=backend_name,
                model=effective_input.model,
            )
            raise WorkflowError(status_code=503, detail="Safety preflight timed out. Please retry.") from error
        except WorkflowError:
            preflight_latency_ms = (time.perf_counter() - preflight_started) * 1000.0
            self._record_stage_metric(
                "preflight",
                preflight_latency_ms,
                timed_out=False,
                status="error",
                mission=effective_input.mission_filter,
                backend=backend_name,
                model=effective_input.model,
            )
            raise
        except Exception as error:
            preflight_latency_ms = (time.perf_counter() - preflight_started) * 1000.0
            self._record_stage_metric(
                "preflight",
                preflight_latency_ms,
                timed_out=False,
                status="error",
                mission=effective_input.mission_filter,
                backend=backend_name,
                model=effective_input.model,
            )
            raise WorkflowError(status_code=500, detail="Safety preflight failed") from error
        preflight_latency_ms = (time.perf_counter() - preflight_started) * 1000.0
        self._record_stage_metric(
            "preflight",
            preflight_latency_ms,
            timed_out=False,
            status="blocked" if preflight_result.blocked_response else "ok",
            mission=effective_input.mission_filter,
            backend=backend_name,
            model=effective_input.model,
        )

        if preflight_result.blocked_response:
            if prestarted_retrieval_future is not None:
                try:
                    prestarted_retrieval_future.cancel()
                except Exception:
                    pass
            return ChatWorkflowResult(
                answer=preflight_result.blocked_response,
                contexts=[],
                evaluation={},
                judge={
                    "groundedness_score": 0.0,
                    "safety_score": 1.0,
                    "task_success_score": 0.0,
                    "overall_score": 0.35,
                    "confidence": 1.0,
                    "passed": True,
                    "low_confidence": True,
                    "rationale": "Blocked by safety preflight before LLM generation.",
                    "source": "policy",
                },
                blocked=True,
            )

        # Check answer cache before retrieval to avoid unnecessary query embeddings.
        answer = self._cache_get(self._answer_cache, answer_key, cache_type="answer")
        if answer is None and self._redis_l2_cache_enabled:
            # L2 cache (Redis) if L1 miss:
            try:
                answer = self._redis_l2_cache.get_response(
                    effective_input.question,
                    effective_input.mission_filter,
                    effective_input.collection_name,
                    effective_input.model,
                    effective_input.evaluate,
                )
                if answer is not None:
                    with self._cache_stats_lock:
                        self._redis_cache_hits += 1
            except Exception as _l2_err:
                logging.getLogger(__name__).debug("L2 answer cache read skipped: %s", _l2_err)
                answer = None
                if answer is None:
                    with self._cache_stats_lock:
                        self._redis_cache_misses += 1
            if answer is not None:
                # Populate L1 on L2 hit
                self._cache_set(
                    self._answer_cache,
                    answer_key,
                    answer,
                    ttl_seconds=self._answer_cache_ttl,
                    max_entries=self._answer_cache_max_entries,
                )

        cached_answer_hit = answer is not None
        mission_specific_request = self._normalize_mission(effective_input.mission_filter) not in {
            "",
            "all",
            "any",
            "*",
            "none",
        }

        # For mission-filtered requests, require fresh retrieval evidence and
        # avoid serving mission answers from answer-cache alone.
        if cached_answer_hit and mission_specific_request:
            answer = None
            cached_answer_hit = False

        retrieval_result = RetrievalResult(contexts=[], metadatas=[], context_text="")
        if not cached_answer_hit:
            # L1 cache (in-process):
            retrieval_result = self._normalize_retrieval_payload(
                self._cache_get(self._retrieval_cache, retrieval_key, cache_type="retrieval")
            )

            # L2 cache (Redis) if L1 miss:
            if retrieval_result is None and self._redis_l2_cache_enabled:
                try:
                    retrieval_result = self._normalize_retrieval_payload(
                        self._redis_l2_cache.get_retrieval(
                            effective_input.question,
                            effective_input.mission_filter,
                            effective_input.collection_name,
                        )
                    )
                except Exception as _l2_err:
                    logging.getLogger(__name__).debug("L2 retrieval cache read skipped: %s", _l2_err)
                    retrieval_result = None
                if retrieval_result is not None:
                    # Populate L1 on L2 hit
                    self._cache_set(
                        self._retrieval_cache,
                        retrieval_key,
                        retrieval_result,
                        ttl_seconds=self._retrieval_cache_ttl,
                        max_entries=self._retrieval_cache_max_entries,
                    )
        
        retrieval_failed = False
        retrieval_failure_reason = ""

        if not cached_answer_hit and retrieval_result is None:
            if prestarted_retrieval_submit_error:
                retrieval_result = RetrievalResult(contexts=[], metadatas=[], context_text="")
                retrieval_failed = True
                retrieval_failure_reason = prestarted_retrieval_submit_error
            elif prestarted_retrieval_future is not None:
                retrieval_started = prestarted_retrieval_started or time.perf_counter()
                try:
                    retrieval_result = self._await_result(
                        prestarted_retrieval_future,
                        timeout=self._retrieval_timeout_seconds,
                    )
                    retrieval_latency_ms = (time.perf_counter() - retrieval_started) * 1000.0
                    self._record_stage_metric(
                        "retrieval",
                        retrieval_latency_ms,
                        timed_out=False,
                        status="ok",
                        mission=effective_input.mission_filter,
                        backend=backend_name,
                        model=effective_input.model,
                    )
                    self._retrieval_breaker.record_success()
                    self._cache_set(
                        self._retrieval_cache,
                        retrieval_key,
                        retrieval_result,
                        ttl_seconds=self._retrieval_cache_ttl,
                        max_entries=self._retrieval_cache_max_entries,
                    )
                except TimeoutError:
                    self._retrieval_breaker.record_failure()
                    retrieval_latency_ms = (time.perf_counter() - retrieval_started) * 1000.0
                    self._record_stage_metric(
                        "retrieval",
                        retrieval_latency_ms,
                        timed_out=True,
                        status="timeout",
                        mission=effective_input.mission_filter,
                        backend=backend_name,
                        model=effective_input.model,
                    )
                    retrieval_result = RetrievalResult(contexts=[], metadatas=[], context_text="")
                    retrieval_failed = True
                    retrieval_failure_reason = "retrieval timeout"
                    logging.getLogger(__name__).warning(
                        "Retrieval timed out after %.2fs",
                        self._retrieval_timeout_seconds,
                    )
                except Exception as error:
                    self._retrieval_breaker.record_failure()
                    retrieval_latency_ms = (time.perf_counter() - retrieval_started) * 1000.0
                    self._record_stage_metric(
                        "retrieval",
                        retrieval_latency_ms,
                        timed_out=False,
                        status="error",
                        mission=effective_input.mission_filter,
                        backend=backend_name,
                        model=effective_input.model,
                    )
                    retrieval_result = RetrievalResult(contexts=[], metadatas=[], context_text="")
                    retrieval_failed = True
                    retrieval_failure_reason = str(error)[:120]
                    logging.getLogger(__name__).warning(
                        "Retrieval failed, using fallback: %s",
                        retrieval_failure_reason,
                    )
            elif not self._retrieval_breaker.allow():
                retrieval_result = RetrievalResult(contexts=[], metadatas=[], context_text="")
                retrieval_failed = True
                retrieval_failure_reason = "retrieval circuit breaker open"
            else:
                retrieval_started = time.perf_counter()
                try:
                    retrieval_future = self._retrieval_executor.submit(self.retrieval_worker.run, effective_input)
                except StageOverloadError:
                    retrieval_result = RetrievalResult(contexts=[], metadatas=[], context_text="")
                    retrieval_failed = True
                    retrieval_failure_reason = "retrieval queue saturated"
                    self._record_stage_metric(
                        "retrieval",
                        latency_ms=0.0,
                        timed_out=False,
                        status="overload",
                        mission=effective_input.mission_filter,
                        backend=backend_name,
                        model=effective_input.model,
                    )
            if retrieval_result is None:
                try:
                    retrieval_result = self._await_result(retrieval_future, timeout=self._retrieval_timeout_seconds)
                    retrieval_latency_ms = (time.perf_counter() - retrieval_started) * 1000.0
                    self._record_stage_metric(
                        "retrieval",
                        retrieval_latency_ms,
                        timed_out=False,
                        status="ok",
                        mission=effective_input.mission_filter,
                        backend=backend_name,
                        model=effective_input.model,
                    )
                    self._retrieval_breaker.record_success()
                    self._cache_set(
                        self._retrieval_cache,
                        retrieval_key,
                        retrieval_result,
                        ttl_seconds=self._retrieval_cache_ttl,
                        max_entries=self._retrieval_cache_max_entries,
                    )
                except TimeoutError:
                    self._retrieval_breaker.record_failure()
                    retrieval_latency_ms = (time.perf_counter() - retrieval_started) * 1000.0
                    self._record_stage_metric(
                        "retrieval",
                        retrieval_latency_ms,
                        timed_out=True,
                        status="timeout",
                        mission=effective_input.mission_filter,
                        backend=backend_name,
                        model=effective_input.model,
                    )
                    retrieval_result = RetrievalResult(contexts=[], metadatas=[], context_text="")
                    retrieval_failed = True
                    retrieval_failure_reason = "retrieval timeout"
                    logging.getLogger(__name__).warning("Retrieval timed out after %.2fs", self._retrieval_timeout_seconds)
                except Exception as error:
                    self._retrieval_breaker.record_failure()
                    retrieval_latency_ms = (time.perf_counter() - retrieval_started) * 1000.0
                    self._record_stage_metric(
                        "retrieval",
                        retrieval_latency_ms,
                        timed_out=False,
                        status="error",
                        mission=effective_input.mission_filter,
                        backend=backend_name,
                        model=effective_input.model,
                    )
                    retrieval_result = RetrievalResult(contexts=[], metadatas=[], context_text="")
                    retrieval_failed = True
                    retrieval_failure_reason = str(error)[:120]
                    logging.getLogger(__name__).warning("Retrieval failed, using fallback: %s", retrieval_failure_reason)
        else:
            self._retrieval_breaker.record_success()

        # Write successful retrieval to L2 cache outside the retrieval try/except so
        # a Redis error cannot retroactively mark a good retrieval as failed.
        if (
            self._redis_l2_cache_enabled
            and not retrieval_failed
            and retrieval_result is not None
            and not cached_answer_hit
        ):
            try:
                self._redis_l2_cache.set_retrieval(
                    effective_input.question,
                    effective_input.mission_filter,
                    effective_input.collection_name,
                    [
                        {"context": c, "metadata": m}
                        for c, m in zip(retrieval_result.contexts, retrieval_result.metadatas)
                    ],
                )
            except Exception as _l2_err:
                logging.getLogger(__name__).debug("L2 retrieval cache write skipped: %s", _l2_err)

        if retrieval_result is None:
            retrieval_result = RetrievalResult(contexts=[], metadatas=[], context_text="")

        # Apply context compression (dedup + mission priority + token cap) on the
        # raw retrieval result before passing context_text to generation.  The raw
        # result remains in the retrieval cache such that compression config changes do not
        # require a cache flush.
        retrieval_result = self._compress_retrieval_result(
            retrieval_result, effective_input.mission_filter
        )

        # Keep cached-answer behavior explicit: no retrieval contexts are relied on
        # for judge/evaluation when answer cache satisfies the request.
        contexts_for_quality = [] if cached_answer_hit else retrieval_result.contexts

        # Hard-grounding policy for mission-filtered queries:
        # do not generate from priors when retrieval has no mission-matching evidence.
        if (
            answer is None
            and not self._has_grounded_mission_context(effective_input.mission_filter, retrieval_result.metadatas)
        ):
            mission_label = (effective_input.mission_filter or "requested mission").strip() or "requested mission"
            return ChatWorkflowResult(
                answer=(
                    f"I don't have enough grounded sources for mission '{mission_label}' in the current collection. "
                    "Please switch collections or broaden the mission filter and try again."
                ),
                contexts=[],
                evaluation={},
                judge={
                    "groundedness_score": 0.0,
                    "safety_score": 1.0,
                    "task_success_score": 0.0,
                    "overall_score": 0.3,
                    "confidence": 1.0,
                    "passed": True,
                    "low_confidence": True,
                    "rationale": "Mission filter requested but no mission-matching grounded sources were retrieved.",
                    "source": "policy",
                },
                blocked=False,
            )

        if retrieval_failed:
            return ChatWorkflowResult(
                answer=(
                    "I can help with NASA mission questions, but I could not retrieve trusted mission "
                    "sources right now. Please retry in a moment."
                ),
                contexts=[],
                evaluation={},
                judge={
                    "groundedness_score": 0.0,
                    "safety_score": 1.0,
                    "task_success_score": 0.0,
                    "overall_score": 0.3,
                    "confidence": 0.2,
                    "passed": True,
                    "low_confidence": True,
                    "rationale": f"Degraded response due to retrieval failure: {retrieval_failure_reason}",
                    "source": "degraded",
                },
                blocked=False,
            )
        
        if answer is None:
            if not self._generation_breaker.allow():
                answer = (
                    "I can help with NASA mission questions, but answer generation is temporarily "
                    "degraded. Please retry shortly."
                )
            else:
                generation_started = time.perf_counter()
                try:
                    generation_future = self._generation_executor.submit(
                        self.analysis_worker.generate_answer,
                        openai_key,
                        effective_input,
                        retrieval_result.context_text,
                    )
                    answer = self._await_result(generation_future, timeout=self._generation_timeout_seconds)
                    generation_latency_ms = (time.perf_counter() - generation_started) * 1000.0
                    self._record_stage_metric(
                        "generation",
                        generation_latency_ms,
                        timed_out=False,
                        status="ok",
                        mission=effective_input.mission_filter,
                        backend=backend_name,
                        model=effective_input.model,
                    )
                    self._generation_breaker.record_success()
                except TimeoutError:
                    self._generation_breaker.record_failure()
                    generation_latency_ms = (time.perf_counter() - generation_started) * 1000.0
                    self._record_stage_metric(
                        "generation",
                        generation_latency_ms,
                        timed_out=True,
                        status="timeout",
                        mission=effective_input.mission_filter,
                        backend=backend_name,
                        model=effective_input.model,
                    )
                    answer = (
                        "I can help with NASA mission questions, but answer generation timed out. "
                        "Please retry in a moment."
                    )
                except Exception as error:
                    if isinstance(error, StageOverloadError):
                        self._record_stage_metric(
                            "generation",
                            latency_ms=0.0,
                            timed_out=False,
                            status="overload",
                            mission=effective_input.mission_filter,
                            backend=backend_name,
                            model=effective_input.model,
                        )
                        answer = (
                            "I can help with NASA mission questions, but generation capacity is saturated. "
                            "Please retry shortly."
                        )
                        self._generation_breaker.record_failure()
                    else:
                        self._generation_breaker.record_failure()
                        generation_latency_ms = (time.perf_counter() - generation_started) * 1000.0
                        self._record_stage_metric(
                            "generation",
                            generation_latency_ms,
                            timed_out=False,
                            status="error",
                            mission=effective_input.mission_filter,
                            backend=backend_name,
                            model=effective_input.model,
                        )
                        logging.getLogger(__name__).warning(
                            "Generation failed, returning fallback: %s", str(error)[:120]
                        )
                        answer = (
                            "I can help with NASA mission questions, but answer generation is temporarily "
                            "unavailable. Please retry shortly."
                        )

            answer = self.safety_worker.postflight(
                answer=answer,
                contexts=contexts_for_quality,
                client_ip=workflow_input.client_ip,
            )

            self._cache_set(
                self._answer_cache,
                answer_key,
                answer,
                ttl_seconds=self._answer_cache_ttl,
                max_entries=self._answer_cache_max_entries,
            )
            
            # Also set in L2 (Redis) for cross-pod sharing
            if self._redis_l2_cache_enabled:
                try:
                    self._redis_l2_cache.set_response(
                        effective_input.question,
                        effective_input.mission_filter,
                        effective_input.collection_name,
                        effective_input.model,
                        effective_input.evaluate,
                        answer,
                    )
                except Exception as _l2_err:
                    logging.getLogger(__name__).debug("L2 answer cache write skipped: %s", _l2_err)

        judge_mode = (workflow_input.judge_mode or "async").lower()
        if judge_mode == "off":
            judge = {
                "groundedness_score": 0.0,
                "safety_score": 0.0,
                "task_success_score": 0.0,
                "overall_score": 0.0,
                "confidence": 0.0,
                "passed": True,
                "low_confidence": True,
                "rationale": "Judge skipped by configuration.",
                "source": "disabled",
            }
        elif judge_mode == "async":
            judge_job_id = str(uuid.uuid4())
            enqueue_payload = {
                "job_id": judge_job_id,
                "question": effective_input.question,
                "mission_filter": effective_input.mission_filter,
                "chroma_dir": effective_input.chroma_dir,
                "collection_name": effective_input.collection_name,
                "model": effective_input.model,
                "answer": answer,
                "contexts": contexts_for_quality,
                "client_ip": effective_input.client_ip,
                "trace_span_id": effective_input.trace_span_id,
                "session_id": effective_input.session_id,
                "phoenix_base_url": phoenix_base_url(),
            }
            # Phase 2: try broker first; fall back to in-process executor.
            queued = self._judge_broker.enqueue(judge_job_id, enqueue_payload)
            judge_overloaded = False
            use_local_fallback = not queued
            if queued and not self._judge_broker.has_active_consumers(timeout_seconds=0.5):
                logging.getLogger(__name__).warning(
                    "Judge broker has no active consumers; running local async fallback for job %s",
                    judge_job_id,
                )
                use_local_fallback = True

            if use_local_fallback:
                try:
                    self._judge_executor.submit(
                        self._run_async_judge,
                        judge_job_id,
                        openai_key,
                        effective_input,
                        answer,
                        contexts_for_quality,
                        effective_input.trace_span_id,
                    )
                except StageOverloadError:
                    judge_overloaded = True
            if judge_overloaded:
                judge = {
                    "job_id": judge_job_id,
                    "status": "skipped",
                    "groundedness_score": None,
                    "safety_score": None,
                    "task_success_score": None,
                    "overall_score": None,
                    "confidence": None,
                    "passed": True,
                    "low_confidence": True,
                    "source": "overload",
                    "rationale": "Judge skipped due to queue saturation.",
                }
            else:
                judge = {
                    "job_id": judge_job_id,
                    "status": "pending",
                    "groundedness_score": None,
                    "safety_score": None,
                    "task_success_score": None,
                    "overall_score": None,
                    "confidence": None,
                    "passed": True,
                    "low_confidence": True,
                    "source": "async",
                    "rationale": "Judge running asynchronously.",
                }
        else:
            judge = self.judge_worker.judge(
                openai_key=openai_key,
                workflow_input=effective_input,
                answer=answer,
                contexts=contexts_for_quality,
            )

        evaluation = self._evaluate(
            workflow_input=effective_input,
            answer=answer,
            contexts=contexts_for_quality,
        )

        return ChatWorkflowResult(
            answer=answer,
            contexts=contexts_for_quality,
            evaluation=evaluation,
            judge=judge,
            blocked=False,
        )

    def _evaluate(
        self,
        workflow_input: ChatWorkflowInput,
        answer: str,
        contexts,
    ) -> Dict[str, Any]:
        if not workflow_input.evaluate:
            return {}

        if self._evaluation_mode == "off":
            return {
                "status": "disabled",
                "source": "disabled",
                "rationale": "Evaluation disabled by configuration.",
            }

        if self._evaluation_mode == "sync":
            if not self._evaluation_breaker.allow():
                return {}
            started = time.perf_counter()
            try:
                eval_future = self._eval_executor.submit(
                    self.analysis_worker.evaluate,
                    workflow_input,
                    answer,
                    contexts,
                )
                result = self._await_result(eval_future, timeout=self._evaluation_timeout_seconds)
                latency_ms = (time.perf_counter() - started) * 1000.0
                self._record_stage_metric(
                    "evaluation",
                    latency_ms,
                    timed_out=False,
                    status="ok",
                    mission=workflow_input.mission_filter,
                    backend=f"{workflow_input.chroma_dir}:{workflow_input.collection_name}".lower(),
                    model=workflow_input.model,
                )
                self._evaluation_breaker.record_success()
            except TimeoutError:
                self._evaluation_breaker.record_failure()
                latency_ms = (time.perf_counter() - started) * 1000.0
                self._record_stage_metric(
                    "evaluation",
                    latency_ms,
                    timed_out=True,
                    status="timeout",
                    mission=workflow_input.mission_filter,
                    backend=f"{workflow_input.chroma_dir}:{workflow_input.collection_name}".lower(),
                    model=workflow_input.model,
                )
                logging.getLogger(__name__).warning(
                    "Synchronous evaluation timed out after %.2fs", self._evaluation_timeout_seconds
                )
                return {}
            except Exception as error:
                self._evaluation_breaker.record_failure()
                latency_ms = (time.perf_counter() - started) * 1000.0
                self._record_stage_metric(
                    "evaluation",
                    latency_ms,
                    timed_out=False,
                    status="error",
                    mission=workflow_input.mission_filter,
                    backend=f"{workflow_input.chroma_dir}:{workflow_input.collection_name}".lower(),
                    model=workflow_input.model,
                )
                logging.getLogger(__name__).warning("Synchronous evaluation failed: %s", str(error)[:120])
                return {}
            if isinstance(result, dict):
                if result.get("error"):
                    return {}
                result = dict(result)
                result.setdefault("status", "completed")
                result.setdefault("source", "sync")
                result.setdefault("latency_ms", round(latency_ms, 2))
            return result if isinstance(result, dict) else {}

        job_id = str(uuid.uuid4())
        submitted_at_ms = round(time.time() * 1000)
        pending = {
            "job_id": job_id,
            "status": "pending",
            "source": "async",
            "submitted_at_ms": submitted_at_ms,
            "question": workflow_input.question,
        }
        self._record_evaluation_job(job_id, pending)

        enqueue_payload = {
            "job_id": job_id,
            "question": workflow_input.question,
            "mission_filter": workflow_input.mission_filter,
            "chroma_dir": workflow_input.chroma_dir,
            "collection_name": workflow_input.collection_name,
            "model": workflow_input.model,
            "evaluate": workflow_input.evaluate,
            "interaction_id": workflow_input.interaction_id,
            "answer": answer,
            "contexts": contexts,
            "submitted_at_ms": submitted_at_ms,
            "evaluation_timeout_seconds": self._evaluation_timeout_seconds,
        }

        # Phase 1 externalization: enqueue to Redis broker for dedicated workers.
        # If broker is disabled/unavailable, fall back to local bounded async executor.
        queued = self._evaluation_broker.enqueue(job_id, enqueue_payload)
        if not queued:
            if not self._evaluation_local_fallback_enabled:
                skipped = {
                    "job_id": job_id,
                    "status": "skipped",
                    "source": "broker_unavailable",
                    "submitted_at_ms": submitted_at_ms,
                    "finished_at_ms": round(time.time() * 1000),
                    "question": workflow_input.question,
                    "error": "evaluation broker unavailable and local fallback disabled",
                }
                self._record_evaluation_job(job_id, skipped)
                return skipped
            try:
                self._eval_job_executor.submit(
                    self._run_async_evaluation,
                    job_id,
                    workflow_input,
                    answer,
                    contexts,
                )
            except StageOverloadError:
                skipped = {
                    "job_id": job_id,
                    "status": "skipped",
                    "source": "overload",
                    "submitted_at_ms": submitted_at_ms,
                    "finished_at_ms": round(time.time() * 1000),
                    "question": workflow_input.question,
                    "error": "evaluation queue saturated",
                }
                self._record_evaluation_job(job_id, skipped)
                return skipped
        elif not self._evaluation_broker.has_active_consumers():
            if not self._evaluation_local_fallback_enabled:
                skipped = {
                    "job_id": job_id,
                    "status": "skipped",
                    "source": "no_consumers",
                    "submitted_at_ms": submitted_at_ms,
                    "finished_at_ms": round(time.time() * 1000),
                    "question": workflow_input.question,
                    "error": "evaluation broker has no active consumers and local fallback disabled",
                }
                self._record_evaluation_job(job_id, skipped)
                return skipped
            logging.getLogger(__name__).warning(
                "Evaluation broker has no active consumers; running local async fallback for job %s",
                job_id,
            )
            try:
                self._eval_job_executor.submit(
                    self._run_async_evaluation,
                    job_id,
                    workflow_input,
                    answer,
                    contexts,
                )
            except StageOverloadError:
                skipped = {
                    "job_id": job_id,
                    "status": "skipped",
                    "source": "overload",
                    "submitted_at_ms": submitted_at_ms,
                    "finished_at_ms": round(time.time() * 1000),
                    "question": workflow_input.question,
                    "error": "evaluation queue saturated",
                }
                self._record_evaluation_job(job_id, skipped)
                return skipped
        return pending

    def _run_async_evaluation(
        self,
        job_id: str,
        workflow_input: ChatWorkflowInput,
        answer: str,
        contexts,
    ) -> None:
        started = time.perf_counter()

        # Cross-pod idempotency: only one worker/pod should execute a job.
        if self._redis_job_store.is_completed(job_id):
            return
        if not self._redis_job_store.acquire_processing(
            job_id,
            processing_ttl_seconds=300,
            worker_type="evaluation_local",
        ):
            return

        if not self._evaluation_breaker.allow():
            payload = {
                "job_id": job_id,
                "status": "error",
                "source": "async",
                "finished_at_ms": round(time.time() * 1000),
                "question": workflow_input.question,
                "error": "evaluation circuit breaker open",
            }
            self._record_evaluation_job(job_id, payload)
            self._redis_job_store.release_processing(job_id)
            return

        try:
            eval_future = self._eval_executor.submit(
                self.analysis_worker.evaluate,
                workflow_input,
                answer,
                contexts,
            )
        except StageOverloadError:
            self._evaluation_breaker.record_failure()
            self._record_stage_metric(
                "evaluation",
                latency_ms=0.0,
                timed_out=False,
                status="overload",
                mission=workflow_input.mission_filter,
                backend=f"{workflow_input.chroma_dir}:{workflow_input.collection_name}".lower(),
                model=workflow_input.model,
            )
            payload = {
                "job_id": job_id,
                "status": "skipped",
                "source": "overload",
                "finished_at_ms": round(time.time() * 1000),
                "question": workflow_input.question,
                "error": "evaluation queue saturated",
            }
            self._record_evaluation_job(job_id, payload)
            self._redis_job_store.release_processing(job_id)
            return

        try:
            result = self._await_result(eval_future, timeout=self._evaluation_timeout_seconds)
            latency_ms = (time.perf_counter() - started) * 1000.0
            self._record_stage_metric(
                "evaluation",
                latency_ms,
                timed_out=False,
                status="ok",
                mission=workflow_input.mission_filter,
                backend=f"{workflow_input.chroma_dir}:{workflow_input.collection_name}".lower(),
                model=workflow_input.model,
            )
            if isinstance(result, dict) and result.get("error"):
                raise RuntimeError(str(result.get("error")))
            self._evaluation_breaker.record_success()
            payload = dict(result) if isinstance(result, dict) else {}
            payload.update(
                {
                    "job_id": job_id,
                    "status": "completed",
                    "source": "async",
                    "latency_ms": round(latency_ms, 2),
                    "finished_at_ms": round(time.time() * 1000),
                    "question": workflow_input.question,
                }
            )
            self._record_evaluation_job(job_id, payload)
            if self._evaluation_completion_callback is not None:
                try:
                    self._evaluation_completion_callback(workflow_input, answer, contexts, dict(payload))
                except Exception as callback_error:
                    logging.getLogger(__name__).warning(
                        "Async evaluation completion callback failed: %s",
                        str(callback_error)[:120],
                    )
        except Exception as error:
            if isinstance(error, TimeoutError):
                try:
                    eval_future.cancel()
                except Exception:
                    pass
            self._evaluation_breaker.record_failure()
            latency_ms = (time.perf_counter() - started) * 1000.0
            timed_out = isinstance(error, TimeoutError)
            self._record_stage_metric(
                "evaluation",
                latency_ms,
                timed_out=timed_out,
                status="timeout" if timed_out else "error",
                mission=workflow_input.mission_filter,
                backend=f"{workflow_input.chroma_dir}:{workflow_input.collection_name}".lower(),
                model=workflow_input.model,
            )
            payload = {
                "job_id": job_id,
                "status": "error",
                "source": "async",
                "latency_ms": round(latency_ms, 2),
                "finished_at_ms": round(time.time() * 1000),
                "question": workflow_input.question,
                "error": (
                    f"evaluation timed out after {self._evaluation_timeout_seconds:.2f}s"
                    if timed_out
                    else str(error)[:200]
                ),
            }
            self._record_evaluation_job(job_id, payload)
            logging.getLogger(__name__).warning("Async evaluation failed: %s", str(error)[:120])
        finally:
            self._redis_job_store.release_processing(job_id)

    def _record_evaluation_job(self, job_id: str, payload: Dict[str, Any]) -> None:
        # Store in L1 (in-process)
        with self._evaluation_lock:
            self._evaluation_results[job_id] = payload
            self._evaluation_results.move_to_end(job_id)
            while len(self._evaluation_results) > self._evaluation_buffer_size:
                self._evaluation_results.popitem(last=False)
        
        # Also store in L2 (Redis) for cross-pod access
        self._redis_job_store.set_result(job_id, payload)

    def get_evaluation_job(self, job_id: str) -> Dict[str, Any] | None:
        # Read current L1 snapshot first.
        with self._evaluation_lock:
            l1_payload = self._evaluation_results.get(job_id)

        l1_status = str((l1_payload or {}).get("status", "")).strip().lower()
        l1_terminal = l1_status in {"completed", "error", "dead_lettered", "poisoned", "skipped"}

        # If L1 is missing or non-terminal (e.g., pending), consult shared L2 so
        # broker-backed worker completions are visible across polling requests.
        if (l1_payload is None) or (not l1_terminal):
            l2_payload = self._redis_job_store.get_result(job_id)
            if isinstance(l2_payload, dict):
                l2_status = str(l2_payload.get("status", "")).strip().lower()
                if l2_status and l2_status != l1_status:
                    # Hydrate L1 with the freshest state for subsequent reads.
                    with self._evaluation_lock:
                        self._evaluation_results[job_id] = dict(l2_payload)
                        self._evaluation_results.move_to_end(job_id)
                        while len(self._evaluation_results) > self._evaluation_buffer_size:
                            self._evaluation_results.popitem(last=False)
                return dict(l2_payload)

        return dict(l1_payload) if isinstance(l1_payload, dict) else None

    def get_recent_evaluation_jobs(self, limit: int = 20):
        safe_limit = max(1, min(limit, self._evaluation_buffer_size))
        with self._evaluation_lock:
            values = list(self._evaluation_results.values())
            tail = values[-safe_limit:]
            return [dict(item) for item in reversed(tail)]

    def _run_async_judge(
        self,
        judge_job_id: str,
        openai_key: str,
        workflow_input: ChatWorkflowInput,
        answer: str,
        contexts,
        trace_span_id: str | None = None,
    ) -> None:
        started = time.perf_counter()
        
        try:
            result = self.judge_worker.judge(
                openai_key=openai_key,
                workflow_input=workflow_input,
                answer=answer,
                contexts=contexts,
            )
            latency_ms = (time.perf_counter() - started) * 1000.0
            payload = {
                "job_id": judge_job_id,
                "timestamp_ms": round(time.time() * 1000),
                "question": workflow_input.question,
                "client_ip": workflow_input.client_ip,
                "trace_span_id": trace_span_id,
                "session_id": workflow_input.session_id,
                "judge": result,
                "latency_ms": round(latency_ms, 2),
            }
            
            # Store in L1 (in-process)
            with self._judge_lock:
                self._judge_results.appendleft(payload)
            
            # Store in L2 (Redis)
            self._redis_job_store.set_result(judge_job_id, payload)

            # When async judge runs in-process fallback mode, post annotations to
            # the originating request span so Phoenix metrics include final scores.
            if trace_span_id:
                annotation_scores = collect_annotation_scores(
                    result,
                    {"latency_ms": latency_ms},
                    passthrough_keys={"latency_ms"},
                )
                post_span_annotations(trace_span_id, annotation_scores, logger=logging.getLogger(__name__))
            
            logging.getLogger(__name__).info(
                "Async judge completed: passed=%s low_confidence=%s overall=%s",
                result.get("passed"),
                result.get("low_confidence"),
                result.get("overall_score"),
            )
        except Exception as error:
            latency_ms = (time.perf_counter() - started) * 1000.0
            payload = {
                "job_id": judge_job_id,
                "timestamp_ms": round(time.time() * 1000),
                "question": workflow_input.question,
                "client_ip": workflow_input.client_ip,
                "trace_span_id": trace_span_id,
                "session_id": workflow_input.session_id,
                "judge": {
                    "status": "error",
                    "passed": False,
                    "low_confidence": True,
                    "source": "async",
                    "rationale": str(error)[:200],
                },
                "latency_ms": round(latency_ms, 2),
            }
            
            # Store in L1 (in-process)
            with self._judge_lock:
                self._judge_results.appendleft(payload)
            
            # Store in L2 (Redis)
            self._redis_job_store.set_result(judge_job_id, payload)
            
            logging.getLogger(__name__).warning("Async judge failed: %s", str(error)[:120])

    def get_last_judge_result(self):
        """Return the most recent async judge result if available."""
        with self._judge_lock:
            return self._judge_results[0] if self._judge_results else None

    def get_recent_judge_results(self, limit: int = 20):
        """Return a bounded list of recent async judge results."""
        safe_limit = max(1, min(limit, 200))
        with self._judge_lock:
            return list(self._judge_results)[:safe_limit]

    def get_latency_sli_report(self) -> Dict[str, Any]:
        """Return p50/p95 latency and timeout SLI rollups per stage."""
        workers: Dict[str, Any] = {}
        for name, tracker in self._stage_sli.items():
            workers[name] = tracker.snapshot(self._stage_budgets_ms.get(name, 0.0))
        return {
            "generated_at_ms": round(time.time() * 1000),
            "workers": workers,
        }

    def get_latency_sli_timeseries(
        self,
        stage: str | None = None,
        window_minutes: int = 60,
        bucket_seconds: int = 300,
        mission: str | None = None,
        backend: str | None = None,
        model: str | None = None,
    ) -> Dict[str, Any]:
        """Return bucketed time-series SLI rollups from persisted stage events."""
        return self._stage_event_store.get_timeseries(
            stage=stage,
            window_minutes=window_minutes,
            bucket_seconds=bucket_seconds,
            mission=mission,
            backend=backend,
            model=model,
        )

    def _compress_retrieval_result(
        self, result: RetrievalResult, mission_filter: str | None
    ) -> RetrievalResult:
        """Apply dedup, mission priority, and token cap to a raw retrieval result."""
        if not result.contexts:
            return result
        compressed_c, compressed_m = self._context_compressor.compress(
            result.contexts, result.metadatas, mission_filter
        )
        context_text = rag_client.format_context(compressed_c, compressed_m)
        return RetrievalResult(
            contexts=compressed_c,
            metadatas=compressed_m,
            context_text=context_text,
        )

    def _effective_retrieval_depth(self, workflow_input: ChatWorkflowInput) -> int:
        """Choose retrieval depth through an injected policy."""
        return max(1, int(self._retrieval_depth_policy.resolve_n_results(workflow_input)))

    def shutdown(self) -> None:
        """Stop background executors used by the workflow."""
        self._safety_executor.begin_shutdown()
        self._retrieval_executor.begin_shutdown()
        self._generation_executor.begin_shutdown()
        self._judge_executor.begin_shutdown()
        self._eval_executor.begin_shutdown()
        self._eval_job_executor.begin_shutdown()

        # Phase 1: allow judge/evaluation pools a short soft-drain window so
        # terminal async writes complete during controlled shutdown.
        soft_drain_seconds = 0.25
        deadline = time.monotonic() + soft_drain_seconds
        for pool in (self._judge_executor, self._eval_executor, self._eval_job_executor):
            remaining = max(0.0, deadline - time.monotonic())
            if remaining <= 0:
                break
            pool.wait_for_drain(remaining)

        # Phase 2: cancel any pending async futures to avoid long tail teardown.
        self._judge_executor.shutdown(wait=False, cancel_futures=True)
        self._eval_executor.shutdown(wait=False, cancel_futures=True)
        self._eval_job_executor.shutdown(wait=False, cancel_futures=True)

        # Keep request-path pools fast to stop; in-flight work exits naturally.
        self._safety_executor.shutdown(wait=False, cancel_futures=False)
        self._retrieval_executor.shutdown(wait=False, cancel_futures=False)
        self._generation_executor.shutdown(wait=False, cancel_futures=False)

    def get_worker_pool_report(self) -> Dict[str, Any]:
        """Return bounded worker-pool saturation metrics for autoscaling decisions."""
        return {
            "generated_at_ms": round(time.time() * 1000),
            "workers": {
                "safety": self._safety_executor.snapshot(),
                "retrieval": self._retrieval_executor.snapshot(),
                "generation": self._generation_executor.snapshot(),
                "judge": self._judge_executor.snapshot(),
                "evaluation": self._eval_executor.snapshot(),
            },
        }

    def get_cache_stats(self) -> Dict[str, Any]:
        """Return cache capacity and hit/miss effectiveness for L1 and L2."""
        with self._cache_lock:
            retrieval_entries = len(self._retrieval_cache)
            answer_entries = len(self._answer_cache)

        with self._cache_stats_lock:
            retrieval_hits = self._retrieval_cache_hits
            retrieval_misses = self._retrieval_cache_misses
            answer_hits = self._answer_cache_hits
            answer_misses = self._answer_cache_misses
            redis_hits = self._redis_cache_hits
            redis_misses = self._redis_cache_misses

        retrieval_total = retrieval_hits + retrieval_misses
        answer_total = answer_hits + answer_misses
        redis_total = redis_hits + redis_misses
        combined_hits = retrieval_hits + answer_hits + redis_hits
        combined_total = retrieval_total + answer_total + redis_total

        retrieval_hit_rate = (retrieval_hits / retrieval_total * 100.0) if retrieval_total else 0.0
        answer_hit_rate = (answer_hits / answer_total * 100.0) if answer_total else 0.0
        redis_hit_rate = (redis_hits / redis_total * 100.0) if redis_total else 0.0
        combined_hit_rate = (combined_hits / combined_total * 100.0) if combined_total else 0.0

        return {
            "generated_at_ms": round(time.time() * 1000),
            "l1_retrieval": {
                "entries": retrieval_entries,
                "max_entries": self._retrieval_cache_max_entries,
                "hits": retrieval_hits,
                "misses": retrieval_misses,
                "total": retrieval_total,
                "hit_rate_percent": round(retrieval_hit_rate, 2),
            },
            "l1_answer": {
                "entries": answer_entries,
                "max_entries": self._answer_cache_max_entries,
                "hits": answer_hits,
                "misses": answer_misses,
                "total": answer_total,
                "hit_rate_percent": round(answer_hit_rate, 2),
            },
            "l2_redis": {
                **self._redis_l2_cache.stats(),
                "enabled": self._redis_l2_cache_enabled,
                "hits": redis_hits,
                "misses": redis_misses,
                "total": redis_total,
                "hit_rate_percent": round(redis_hit_rate, 2),
            },
            "combined": {
                "hits": combined_hits,
                "misses": combined_total - combined_hits,
                "total": combined_total,
                "hit_rate_percent": round(combined_hit_rate, 2),
            },
            "timestamp_utc": time.time(),
        }

    def __del__(self):
        # Best-effort cleanup for interpreter shutdown paths.
        try:
            self.shutdown()
        except Exception:
            pass

    @staticmethod
    def _normalize_query(text: str) -> str:
        """Normalize query text for cache keys."""
        collapsed = re.sub(r"\s+", " ", (text or "").strip().lower())
        return collapsed

    @staticmethod
    def _normalize_mission(value: str | None) -> str:
        """Normalize mission aliases to metadata-compatible keys."""
        raw = (value or "").strip().lower()
        aliases = {
            "apollo11": "apollo_11",
            "apollo_11": "apollo_11",
            "apollo 11": "apollo_11",
            "apollo-11": "apollo_11",
            "apollo13": "apollo_13",
            "apollo_13": "apollo_13",
            "apollo 13": "apollo_13",
            "apollo-13": "apollo_13",
            "challenger": "challenger",
        }
        if raw in aliases:
            return aliases[raw]
        return raw.replace(" ", "_").replace("-", "_")

    def _has_grounded_mission_context(self, mission_filter: str | None, metadatas: list[Dict[str, Any]]) -> bool:
        """Return True when retrieval metadata includes at least one matching mission."""
        normalized_target = self._normalize_mission(mission_filter)
        if not normalized_target or normalized_target in {"all", "any", "*", "none"}:
            return True
        if not metadatas:
            return False
        for metadata in metadatas:
            if not isinstance(metadata, dict):
                continue
            if self._normalize_mission(str(metadata.get("mission", ""))) == normalized_target:
                return True
        return False

    @staticmethod
    def _await_result(submission: Any, timeout: float | None = None):
        """Return executor submission result, supporting direct-value test stubs."""
        result_fn = getattr(submission, "result", None)
        if callable(result_fn):
            return result_fn(timeout=timeout)
        return submission

    def _retrieval_cache_key(self, workflow_input: ChatWorkflowInput) -> str:
        normalized_question = self._normalize_query(workflow_input.question)
        mission_filter = (workflow_input.mission_filter or "").strip().lower()
        raw_key = (
            f"{workflow_input.chroma_dir}|{workflow_input.collection_name}|"
            f"{workflow_input.n_results}|{mission_filter}|{normalized_question}"
        )
        return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()

    def _answer_cache_key(self, workflow_input: ChatWorkflowInput) -> str:
        normalized_question = self._normalize_query(workflow_input.question)
        mission_filter = (workflow_input.mission_filter or "").strip().lower()
        # Keep required tuple dimensions while including backend/model guards.
        raw_key = (
            f"{normalized_question}|{mission_filter}|{workflow_input.collection_name}|"
            f"{workflow_input.chroma_dir}|{workflow_input.model}|{workflow_input.n_results}"
        )
        return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()

    def _cache_get(self, cache: OrderedDict[str, tuple[float, Any]], key: str, cache_type: str = "unknown"):
        now = time.time()
        with self._cache_lock:
            item = cache.get(key)
            if not item:
                with self._cache_stats_lock:
                    if cache_type == "retrieval":
                        self._retrieval_cache_misses += 1
                    elif cache_type == "answer":
                        self._answer_cache_misses += 1
                return None
            expires_at, value = item
            if expires_at < now:
                cache.pop(key, None)
                with self._cache_stats_lock:
                    if cache_type == "retrieval":
                        self._retrieval_cache_misses += 1
                    elif cache_type == "answer":
                        self._answer_cache_misses += 1
                return None
            # Mark as recently used for LRU semantics.
            cache.move_to_end(key)
            with self._cache_stats_lock:
                if cache_type == "retrieval":
                    self._retrieval_cache_hits += 1
                elif cache_type == "answer":
                    self._answer_cache_hits += 1
            return value

    def _cache_set(
        self,
        cache: OrderedDict[str, tuple[float, Any]],
        key: str,
        value: Any,
        ttl_seconds: int,
        max_entries: int,
    ) -> None:
        expires_at = time.time() + ttl_seconds
        with self._cache_lock:
            cache[key] = (expires_at, value)
            cache.move_to_end(key)

            # Evict until size is within capacity.
            while len(cache) > max_entries:
                cache.popitem(last=False)

    def _normalize_retrieval_payload(self, payload: Any) -> RetrievalResult | None:
        """Normalize retrieval payloads from L1/L2 caches into RetrievalResult."""
        if payload is None:
            return None
        if isinstance(payload, RetrievalResult):
            return payload

        contexts: list[str] = []
        metadatas: list[Dict[str, Any]] = []

        if isinstance(payload, list):
            for item in payload:
                if not isinstance(item, dict):
                    continue
                context = item.get("context")
                if not isinstance(context, str) or not context:
                    continue
                metadata = item.get("metadata")
                contexts.append(context)
                metadatas.append(metadata if isinstance(metadata, dict) else {})
        elif isinstance(payload, dict):
            raw_contexts = payload.get("contexts")
            raw_metadatas = payload.get("metadatas")
            if isinstance(raw_contexts, list):
                for idx, context in enumerate(raw_contexts):
                    if not isinstance(context, str) or not context:
                        continue
                    metadata = {}
                    if isinstance(raw_metadatas, list) and idx < len(raw_metadatas):
                        candidate = raw_metadatas[idx]
                        if isinstance(candidate, dict):
                            metadata = candidate
                    contexts.append(context)
                    metadatas.append(metadata)
            elif isinstance(payload.get("context"), str):
                context = payload.get("context")
                metadata = payload.get("metadata")
                contexts.append(context)
                metadatas.append(metadata if isinstance(metadata, dict) else {})
        else:
            return None

        context_text = rag_client.format_context(contexts, metadatas) if contexts else ""
        return RetrievalResult(contexts=contexts, metadatas=metadatas, context_text=context_text)
