#!/usr/bin/env python3
"""Broker-backed judge worker for async LLM-as-Judge evaluation.

Phase 2 externalization target:
1) API enqueues judge job to Redis stream
2) This worker consumes stream entries
3) Worker calls JudgeWorker.judge() and writes result to Redis job store

OpenAI key is always read from the worker's own environment (OPENAI_API_KEY),
never carried in the stream payload, to avoid exposing credentials in Redis.
"""

from __future__ import annotations

import logging
import os
import signal
import socket
import time
from typing import Any, Dict

from env_utils import load_project_env
from infra.async_reliability_metrics import get_async_reliability_metrics
from infra.redis_client import get_redis_client
from infra.redis_judge_broker import RedisJudgeBroker
from infra.redis_job_store import RedisAsyncJobStore
from multi_agent.models import ChatWorkflowInput
from multi_agent.workers import JudgeWorker

load_project_env(__file__)

# Import security validators so heuristic judge scores match in-process quality.
# Both components have None-safe guard clauses in JudgeWorker._heuristic_scores().
try:
    from security import OutputValidator, SensitiveInfoFilter
except ImportError:
    OutputValidator = None  # type: ignore[assignment]
    SensitiveInfoFilter = None  # type: ignore[assignment]

logger = logging.getLogger("judge_worker")
logging.basicConfig(
    level=os.getenv("JUDGE_WORKER_LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

_RUNNING = True


def _stop_worker(*_args) -> None:
    global _RUNNING
    _RUNNING = False


def _consumer_name() -> str:
    configured = os.getenv("JUDGE_WORKER_NAME", "").strip()
    if configured:
        return configured
    return f"judge-{socket.gethostname()}-{os.getpid()}"


def _coerce_workflow_input(payload: dict) -> ChatWorkflowInput:
    return ChatWorkflowInput(
        question=str(payload.get("question", "")),
        chroma_dir=str(payload.get("chroma_dir", "./chroma_db")),
        collection_name=str(payload.get("collection_name", "nasa_space_missions_test")),
        n_results=1,
        mission_filter=payload.get("mission_filter"),
        model=str(payload.get("model", "gpt-3.5-turbo")),
        evaluate=False,
        judge_mode="off",
        conversation_history=[],
        client_ip=str(payload.get("client_ip", "worker")),
    )


def _to_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _backoff_seconds(base: float, max_backoff: float, attempt: int) -> float:
    safe_attempt = max(0, int(attempt))
    return min(max_backoff, base * (2 ** safe_attempt))


def run() -> int:
    signal.signal(signal.SIGINT, _stop_worker)
    signal.signal(signal.SIGTERM, _stop_worker)

    redis_client = get_redis_client()
    if not redis_client.is_available():
        logger.error("Redis is not available. Judge worker exiting.")
        return 1

    # OpenAI key from worker's own env — never from stream payload.
    from openai_config import get_openai_api_key

    openai_key = get_openai_api_key(include_chroma_fallback=False)
    if not openai_key:
        logger.error("OPENAI_API_KEY not configured. Judge worker exiting.")
        return 1

    broker = RedisJudgeBroker(
        redis_client,
        stream_name=os.getenv("JUDGE_BROKER_STREAM", "judge:jobs"),
        consumer_group=os.getenv("JUDGE_BROKER_GROUP", "judge-workers"),
        dead_letter_stream=os.getenv("JUDGE_BROKER_DLQ_STREAM", "judge:jobs:dlq"),
        enabled=True,
    )
    job_store = RedisAsyncJobStore(
        redis_client,
        retention_ttl_seconds=int(os.getenv("ASYNC_JOB_RETENTION_SECONDS", "3600")),
    )
    judge_worker = JudgeWorker(
        logger=logger,
        output_validator=OutputValidator,
        sensitive_info_filter=SensitiveInfoFilter,
    )

    max_retries = max(0, int(os.getenv("JUDGE_WORKER_MAX_RETRIES", "3")))
    backoff_base = max(0.0, float(os.getenv("JUDGE_WORKER_BACKOFF_BASE_SECONDS", "0.5")))
    backoff_max = max(backoff_base, float(os.getenv("JUDGE_WORKER_BACKOFF_MAX_SECONDS", "8.0")))
    processing_ttl = max(30, int(os.getenv("JUDGE_WORKER_PROCESSING_TTL_SECONDS", "300")))
    reclaim_enabled = os.getenv("JUDGE_WORKER_RECLAIM_ENABLED", "true").strip().lower() in {"1", "true", "yes"}
    reclaim_min_idle_ms = max(30_000, int(os.getenv("JUDGE_WORKER_RECLAIM_MIN_IDLE_MS", "300000")))
    reclaim_count = max(1, int(os.getenv("JUDGE_WORKER_RECLAIM_COUNT", "10")))
    reclaim_idle_cycles: int = 0

    consumer_name = _consumer_name()
    logger.info("Judge worker started as consumer=%s", consumer_name)

    while _RUNNING:
        messages = broker.consume(consumer_name=consumer_name, count=1, block_ms=3000)
        if not messages:
            if reclaim_enabled:
                reclaim_idle_cycles += 1
                if reclaim_idle_cycles >= 20:  # ~60 s at block_ms=3000
                    reclaim_idle_cycles = 0
                    stale = broker.reclaim_stale(
                        consumer_name=consumer_name,
                        min_idle_ms=reclaim_min_idle_ms,
                        count=reclaim_count,
                    )
                    for message_id, payload in stale:
                        messages.append((message_id, payload))
            if not messages:
                continue

        for message_id, payload in messages:
            job_id = str(payload.get("job_id", ""))
            attempt = max(0, _to_int(payload.get("_attempt", 0), 0))

            if payload.get("_decode_error"):
                broker.dead_letter(
                    message_id=message_id,
                    payload=payload,
                    reason="payload_decode_error",
                    consumer_name=consumer_name,
                    attempt=attempt,
                )
                broker.ack(message_id)
                continue

            if not job_id:
                broker.dead_letter(
                    message_id=message_id,
                    payload=payload,
                    reason="missing_job_id",
                    consumer_name=consumer_name,
                    attempt=attempt,
                )
                broker.ack(message_id)
                continue

            if job_store.is_completed(job_id):
                logger.info("Skipping already-completed judge job_id=%s", job_id)
                broker.ack(message_id)
                continue

            if not job_store.acquire_processing(
                job_id,
                processing_ttl_seconds=processing_ttl,
                worker_type="judge",
            ):
                logger.info("Skipping duplicate in-flight judge job_id=%s", job_id)
                broker.ack(message_id)
                continue

            started = time.perf_counter()
            try:
                workflow_input = _coerce_workflow_input(payload)
                answer = str(payload.get("answer", ""))
                contexts = payload.get("contexts") or []
                if not workflow_input.question:
                    raise ValueError("missing question")
                if not isinstance(contexts, list):
                    contexts = []

                result = judge_worker.judge(
                    openai_key=openai_key,
                    workflow_input=workflow_input,
                    answer=answer,
                    contexts=contexts,
                )
                latency_ms = (time.perf_counter() - started) * 1000.0
                final_payload = {
                    "job_id": job_id,
                    "timestamp_ms": round(time.time() * 1000),
                    "question": workflow_input.question,
                    "client_ip": str(payload.get("client_ip", "worker")),
                    "judge": result,
                    "latency_ms": round(latency_ms, 2),
                }
                logger.info(
                    "Judge job completed job_id=%s passed=%s overall=%.3f latency_ms=%.1f",
                    job_id,
                    result.get("passed"),
                    result.get("overall_score", 0.0),
                    latency_ms,
                )
                job_store.set_result(job_id, final_payload)
                broker.ack(message_id)
            except Exception as error:
                latency_ms = (time.perf_counter() - started) * 1000.0
                retry_error = str(error)[:200]

                if attempt < max_retries:
                    next_attempt = attempt + 1
                    backoff = _backoff_seconds(backoff_base, backoff_max, attempt)
                    retry_payload: Dict[str, Any] = dict(payload)
                    retry_payload["_attempt"] = next_attempt
                    retry_payload["_last_error"] = retry_error

                    job_store.set_result(
                        job_id,
                        {
                            "job_id": job_id,
                            "status": "retrying",
                            "source": "async",
                            "timestamp_ms": round(time.time() * 1000),
                            "question": str(payload.get("question", "")),
                            "client_ip": str(payload.get("client_ip", "worker")),
                            "judge": {
                                "status": "retrying",
                                "passed": False,
                                "low_confidence": True,
                                "source": "async",
                                "rationale": retry_error,
                            },
                            "latency_ms": round(latency_ms, 2),
                            "attempt": next_attempt,
                            "max_retries": max_retries,
                            "next_retry_in_seconds": round(backoff, 3),
                        },
                    )

                    if backoff > 0.0:
                        time.sleep(backoff)

                    if broker.enqueue(job_id, retry_payload):
                        get_async_reliability_metrics().record_retry(worker="judge", reason="processing_error")
                        broker.ack(message_id)
                        job_store.release_processing(job_id)
                        logger.warning(
                            "Judge job retry scheduled job_id=%s attempt=%s/%s",
                            job_id,
                            next_attempt,
                            max_retries,
                        )
                    else:
                        terminal = {
                            "job_id": job_id,
                            "status": "dead_lettered",
                            "source": "async",
                            "timestamp_ms": round(time.time() * 1000),
                            "question": str(payload.get("question", "")),
                            "client_ip": str(payload.get("client_ip", "worker")),
                            "judge": {
                                "status": "dead_lettered",
                                "passed": False,
                                "low_confidence": True,
                                "source": "async",
                                "rationale": "retry enqueue failed",
                            },
                            "latency_ms": round(latency_ms, 2),
                            "attempt": next_attempt,
                            "max_retries": max_retries,
                        }
                        job_store.set_result(job_id, terminal)
                        broker.dead_letter(
                            message_id=message_id,
                            payload=payload,
                            reason="retry_enqueue_failed",
                            consumer_name=consumer_name,
                            attempt=next_attempt,
                        )
                        broker.ack(message_id)
                else:
                    terminal = {
                        "job_id": job_id,
                        "status": "dead_lettered",
                        "source": "async",
                        "timestamp_ms": round(time.time() * 1000),
                        "question": str(payload.get("question", "")),
                        "client_ip": str(payload.get("client_ip", "worker")),
                        "judge": {
                            "status": "dead_lettered",
                            "passed": False,
                            "low_confidence": True,
                            "source": "async",
                            "rationale": retry_error,
                        },
                        "latency_ms": round(latency_ms, 2),
                        "attempt": attempt,
                        "max_retries": max_retries,
                    }
                    job_store.set_result(job_id, terminal)
                    broker.dead_letter(
                        message_id=message_id,
                        payload=payload,
                        reason="max_retries_exhausted",
                        consumer_name=consumer_name,
                        attempt=attempt,
                    )
                    broker.ack(message_id)
                    logger.warning("Judge job dead-lettered job_id=%s: %s", job_id, str(error)[:120])

    logger.info("Judge worker stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
