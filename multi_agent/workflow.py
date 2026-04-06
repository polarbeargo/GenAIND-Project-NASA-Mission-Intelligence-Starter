"""Orchestrator for parallel multi-agent chat processing."""

from __future__ import annotations

import logging
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

from multi_agent.models import (
    ChatWorkflowInput,
    ChatWorkflowResult,
    WorkflowError,
)
from multi_agent.workers import AnalysisWorker, JudgeWorker, RetrievalWorker, SafetyWorker


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
        self._judge_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="nasa-judge-worker")
        self._judge_results = deque(maxlen=200)
        self._judge_lock = Lock()

    def run(self, workflow_input: ChatWorkflowInput, openai_key: str) -> ChatWorkflowResult:
        with ThreadPoolExecutor(max_workers=2, thread_name_prefix="nasa-chat-agents") as executor:
            retrieval_future = executor.submit(self.retrieval_worker.run, workflow_input)
            preflight_future = executor.submit(self.safety_worker.preflight, workflow_input)

            preflight_result = preflight_future.result()
            retrieval_result = retrieval_future.result()

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

        answer = self.analysis_worker.generate_answer(
            openai_key=openai_key,
            workflow_input=workflow_input,
            context_text=retrieval_result.context_text,
        )

        answer = self.safety_worker.postflight(
            answer=answer,
            contexts=retrieval_result.contexts,
            client_ip=workflow_input.client_ip,
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
                workflow_input,
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
                workflow_input=workflow_input,
                answer=answer,
                contexts=retrieval_result.contexts,
            )

        evaluation = self.analysis_worker.evaluate(
            workflow_input=workflow_input,
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
