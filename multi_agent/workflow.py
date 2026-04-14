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
from threading import Lock
from typing import Any, Dict

import rag_client
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
        generation_timeout_seconds: float = 8.0,
        evaluation_timeout_seconds: float = 3.5,
        breaker_failure_threshold: int = 3,
        breaker_recovery_seconds: float = 20.0,
        evaluation_mode: str = "async",
        evaluation_buffer_size: int = 500,
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
        self._io_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="nasa-io-worker")
        self._judge_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="nasa-judge-worker")
        self._eval_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="nasa-eval-worker")
        self._judge_results = deque(maxlen=200)
        self._evaluation_results: OrderedDict[str, Dict[str, Any]] = OrderedDict()
        self._judge_lock = Lock()
        self._evaluation_lock = Lock()
        self._retrieval_cache_ttl = max(60, int(retrieval_cache_ttl_seconds))
        self._answer_cache_ttl = max(60, int(answer_cache_ttl_seconds))
        self._retrieval_cache_max_entries = max(100, int(retrieval_cache_max_entries))
        self._answer_cache_max_entries = max(100, int(answer_cache_max_entries))
        self._retrieval_cache: OrderedDict[str, tuple[float, Any]] = OrderedDict()
        self._answer_cache: OrderedDict[str, tuple[float, str]] = OrderedDict()
        self._cache_lock = Lock()
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
        self._generation_timeout_seconds = max(0.5, float(generation_timeout_seconds))
        self._evaluation_timeout_seconds = max(0.5, float(evaluation_timeout_seconds))
        threshold = max(1, int(breaker_failure_threshold))
        recovery_seconds = max(1.0, float(breaker_recovery_seconds))
        self._retrieval_breaker = StageCircuitBreaker(threshold, recovery_seconds)
        self._generation_breaker = StageCircuitBreaker(threshold, recovery_seconds)
        self._evaluation_breaker = StageCircuitBreaker(threshold, recovery_seconds)
        self._evaluation_mode = (
            evaluation_mode.strip().lower() if evaluation_mode and evaluation_mode.strip() else "async"
        )
        if self._evaluation_mode not in {"async", "sync", "off"}:
            self._evaluation_mode = "async"
        self._evaluation_buffer_size = max(100, int(evaluation_buffer_size))

    def run(self, workflow_input: ChatWorkflowInput, openai_key: str) -> ChatWorkflowResult:
        effective_n_results = self._effective_retrieval_depth(workflow_input)
        effective_input = replace(workflow_input, n_results=effective_n_results)

        retrieval_key = self._retrieval_cache_key(effective_input)
        answer_key = self._answer_cache_key(effective_input)

        retrieval_result = self._cache_get(self._retrieval_cache, retrieval_key)
        retrieval_failed = False
        retrieval_failure_reason = ""

        if retrieval_result is None:
            if not self._retrieval_breaker.allow():
                retrieval_result = RetrievalResult(contexts=[], metadatas=[], context_text="")
                retrieval_failed = True
                retrieval_failure_reason = "retrieval circuit breaker open"
            else:
                retrieval_future = self._io_executor.submit(self.retrieval_worker.run, effective_input)
            preflight_future = self._io_executor.submit(self.safety_worker.preflight, workflow_input)

            preflight_result = preflight_future.result()
            if retrieval_result is None:
                try:
                    retrieval_result = retrieval_future.result(timeout=self._retrieval_timeout_seconds)
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
                    retrieval_result = RetrievalResult(contexts=[], metadatas=[], context_text="")
                    retrieval_failed = True
                    retrieval_failure_reason = "retrieval timeout"
                    logging.getLogger(__name__).warning("Retrieval timed out after %.2fs", self._retrieval_timeout_seconds)
                except Exception as error:
                    self._retrieval_breaker.record_failure()
                    retrieval_result = RetrievalResult(contexts=[], metadatas=[], context_text="")
                    retrieval_failed = True
                    retrieval_failure_reason = str(error)[:120]
                    logging.getLogger(__name__).warning("Retrieval failed, using fallback: %s", retrieval_failure_reason)
        else:
            preflight_result = self.safety_worker.preflight(workflow_input)
            self._retrieval_breaker.record_success()

        # Apply context compression (dedup + mission priority + token cap) on the
        # raw retrieval result before passing context_text to generation.  The raw
        # result remains in the retrieval cache such that compression config changes do not
        # require a cache flush.
        retrieval_result = self._compress_retrieval_result(
            retrieval_result, effective_input.mission_filter
        )

        if preflight_result.blocked_response:
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

        answer = self._cache_get(self._answer_cache, answer_key)
        if answer is None:
            if not self._generation_breaker.allow():
                answer = (
                    "I can help with NASA mission questions, but answer generation is temporarily "
                    "degraded. Please retry shortly."
                )
            else:
                try:
                    generation_future = self._io_executor.submit(
                        self.analysis_worker.generate_answer,
                        openai_key,
                        effective_input,
                        retrieval_result.context_text,
                    )
                    answer = generation_future.result(timeout=self._generation_timeout_seconds)
                    self._generation_breaker.record_success()
                except TimeoutError:
                    self._generation_breaker.record_failure()
                    answer = (
                        "I can help with NASA mission questions, but answer generation timed out. "
                        "Please retry in a moment."
                    )
                except Exception as error:
                    self._generation_breaker.record_failure()
                    logging.getLogger(__name__).warning(
                        "Generation failed, returning fallback: %s", str(error)[:120]
                    )
                    answer = (
                        "I can help with NASA mission questions, but answer generation is temporarily "
                        "unavailable. Please retry shortly."
                    )

            answer = self.safety_worker.postflight(
                answer=answer,
                contexts=retrieval_result.contexts,
                client_ip=workflow_input.client_ip,
            )

            self._cache_set(
                self._answer_cache,
                answer_key,
                answer,
                ttl_seconds=self._answer_cache_ttl,
                max_entries=self._answer_cache_max_entries,
            )

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
            self._judge_executor.submit(
                self._run_async_judge,
                openai_key,
                effective_input,
                answer,
                retrieval_result.contexts,
            )
            judge = {
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
                contexts=retrieval_result.contexts,
            )

        evaluation = self._evaluate(
            workflow_input=effective_input,
            answer=answer,
            contexts=retrieval_result.contexts,
        )

        return ChatWorkflowResult(
            answer=answer,
            contexts=retrieval_result.contexts,
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
                eval_future = self._io_executor.submit(
                    self.analysis_worker.evaluate,
                    workflow_input,
                    answer,
                    contexts,
                )
                result = eval_future.result(timeout=self._evaluation_timeout_seconds)
                self._evaluation_breaker.record_success()
            except TimeoutError:
                self._evaluation_breaker.record_failure()
                logging.getLogger(__name__).warning(
                    "Synchronous evaluation timed out after %.2fs", self._evaluation_timeout_seconds
                )
                return {}
            except Exception as error:
                self._evaluation_breaker.record_failure()
                logging.getLogger(__name__).warning("Synchronous evaluation failed: %s", str(error)[:120])
                return {}
            latency_ms = (time.perf_counter() - started) * 1000.0
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
        self._eval_executor.submit(
            self._run_async_evaluation,
            job_id,
            workflow_input,
            answer,
            contexts,
        )
        return pending

    def _run_async_evaluation(
        self,
        job_id: str,
        workflow_input: ChatWorkflowInput,
        answer: str,
        contexts,
    ) -> None:
        started = time.perf_counter()
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
            return
        try:
            eval_future = self._io_executor.submit(
                self.analysis_worker.evaluate,
                workflow_input,
                answer,
                contexts,
            )
            result = eval_future.result(timeout=self._evaluation_timeout_seconds)
            if isinstance(result, dict) and result.get("error"):
                raise RuntimeError(str(result.get("error")))
            self._evaluation_breaker.record_success()
            latency_ms = (time.perf_counter() - started) * 1000.0
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
        except Exception as error:
            self._evaluation_breaker.record_failure()
            latency_ms = (time.perf_counter() - started) * 1000.0
            payload = {
                "job_id": job_id,
                "status": "error",
                "source": "async",
                "latency_ms": round(latency_ms, 2),
                "finished_at_ms": round(time.time() * 1000),
                "question": workflow_input.question,
                "error": str(error)[:200],
            }
            self._record_evaluation_job(job_id, payload)
            logging.getLogger(__name__).warning("Async evaluation failed: %s", str(error)[:120])

    def _record_evaluation_job(self, job_id: str, payload: Dict[str, Any]) -> None:
        with self._evaluation_lock:
            self._evaluation_results[job_id] = payload
            self._evaluation_results.move_to_end(job_id)
            while len(self._evaluation_results) > self._evaluation_buffer_size:
                self._evaluation_results.popitem(last=False)

    def get_evaluation_job(self, job_id: str) -> Dict[str, Any] | None:
        with self._evaluation_lock:
            payload = self._evaluation_results.get(job_id)
            if payload is None:
                return None
            return dict(payload)

    def get_recent_evaluation_jobs(self, limit: int = 20):
        safe_limit = max(1, min(limit, self._evaluation_buffer_size))
        with self._evaluation_lock:
            values = list(self._evaluation_results.values())
            tail = values[-safe_limit:]
            return [dict(item) for item in reversed(tail)]

    def _run_async_judge(
        self,
        openai_key: str,
        workflow_input: ChatWorkflowInput,
        answer: str,
        contexts,
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
                "timestamp_ms": round(time.time() * 1000),
                "question": workflow_input.question,
                "client_ip": workflow_input.client_ip,
                "judge": result,
                "latency_ms": round(latency_ms, 2),
            }
            with self._judge_lock:
                self._judge_results.appendleft(payload)
            logging.getLogger(__name__).info(
                "Async judge completed: passed=%s low_confidence=%s overall=%s",
                result.get("passed"),
                result.get("low_confidence"),
                result.get("overall_score"),
            )
        except Exception as error:
            latency_ms = (time.perf_counter() - started) * 1000.0
            payload = {
                "timestamp_ms": round(time.time() * 1000),
                "question": workflow_input.question,
                "client_ip": workflow_input.client_ip,
                "judge": {
                    "status": "error",
                    "passed": False,
                    "low_confidence": True,
                    "source": "async",
                    "rationale": str(error)[:200],
                },
                "latency_ms": round(latency_ms, 2),
            }
            with self._judge_lock:
                self._judge_results.appendleft(payload)
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
        self._io_executor.shutdown(wait=False, cancel_futures=False)
        self._judge_executor.shutdown(wait=False, cancel_futures=False)
        self._eval_executor.shutdown(wait=False, cancel_futures=False)

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

    def _cache_get(self, cache: OrderedDict[str, tuple[float, Any]], key: str):
        now = time.time()
        with self._cache_lock:
            item = cache.get(key)
            if not item:
                return None
            expires_at, value = item
            if expires_at < now:
                cache.pop(key, None)
                return None
            # Mark as recently used for LRU semantics.
            cache.move_to_end(key)
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
