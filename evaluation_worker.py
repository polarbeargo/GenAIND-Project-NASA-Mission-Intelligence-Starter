#!/usr/bin/env python3
"""Broker-backed evaluation worker for async RAGAS jobs.

Phase 1 externalization target:
1) API enqueues evaluation job to Redis stream
2) This worker consumes stream entries
3) Worker computes evaluation and writes result to Redis job store
"""

from __future__ import annotations

import logging
import os
import signal
import socket
import time
from typing import Any, Dict

from env_utils import load_project_env
from infra.redis_client import get_redis_client
from infra.redis_evaluation_broker import RedisEvaluationBroker
from infra.redis_job_store import RedisAsyncJobStore
from multi_agent.models import ChatWorkflowInput
from multi_agent.workers import AnalysisWorker

load_project_env(__file__)

logger = logging.getLogger("evaluation_worker")
logging.basicConfig(
    level=os.getenv("EVALUATION_WORKER_LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


class _DummySecurityViolation(Exception):
    """Placeholder security violation type for AnalysisWorker construction."""


_RUNNING = True


def _stop_worker(*_args):
    global _RUNNING
    _RUNNING = False


def _consumer_name() -> str:
    configured = os.getenv("EVALUATION_WORKER_NAME", "").strip()
    if configured:
        return configured
    return f"eval-{socket.gethostname()}-{os.getpid()}"


def _coerce_workflow_input(payload: dict) -> ChatWorkflowInput:
    return ChatWorkflowInput(
        question=str(payload.get("question", "")),
        chroma_dir=str(payload.get("chroma_dir", "./chroma_db")),
        collection_name=str(payload.get("collection_name", "nasa_space_missions_test")),
        n_results=1,
        mission_filter=payload.get("mission_filter"),
        model=str(payload.get("model", "gpt-3.5-turbo")),
        evaluate=True,
        judge_mode="off",
        conversation_history=[],
        client_ip="worker",
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
        logger.error("Redis is not available. Worker exiting.")
        return 1

    broker = RedisEvaluationBroker(
        redis_client,
        stream_name=os.getenv("EVALUATION_BROKER_STREAM", "eval:jobs"),
        consumer_group=os.getenv("EVALUATION_BROKER_GROUP", "eval-workers"),
        dead_letter_stream=os.getenv("EVALUATION_BROKER_DLQ_STREAM", "eval:jobs:dlq"),
        enabled=True,
    )
    job_store = RedisAsyncJobStore(
        redis_client,
        retention_ttl_seconds=int(os.getenv("ASYNC_JOB_RETENTION_SECONDS", "3600")),
    )
    analysis_worker = AnalysisWorker(
        logger=logger,
        security_violation=_DummySecurityViolation,
    )

    max_retries = max(0, int(os.getenv("EVALUATION_WORKER_MAX_RETRIES", "3")))
    backoff_base = max(0.0, float(os.getenv("EVALUATION_WORKER_BACKOFF_BASE_SECONDS", "0.5")))
    backoff_max = max(backoff_base, float(os.getenv("EVALUATION_WORKER_BACKOFF_MAX_SECONDS", "8.0")))
    processing_ttl = max(30, int(os.getenv("EVALUATION_WORKER_PROCESSING_TTL_SECONDS", "300")))

    consumer_name = _consumer_name()
    logger.info("Evaluation worker started as consumer=%s", consumer_name)

    while _RUNNING:
        messages = broker.consume(consumer_name=consumer_name, count=1, block_ms=3000)
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
                logger.info("Skipping already-completed evaluation job_id=%s", job_id)
                broker.ack(message_id)
                continue

            if not job_store.acquire_processing(job_id, processing_ttl_seconds=processing_ttl):
                logger.info("Skipping duplicate in-flight evaluation job_id=%s", job_id)
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

                result = analysis_worker.evaluate(workflow_input, answer, contexts)
                if isinstance(result, dict) and result.get("error"):
                    raise RuntimeError(str(result.get("error")))

                latency_ms = (time.perf_counter() - started) * 1000.0
                final_payload = dict(result) if isinstance(result, dict) else {}
                final_payload.update(
                    {
                        "job_id": job_id,
                        "status": "completed",
                        "source": "async",
                        "latency_ms": round(latency_ms, 2),
                        "finished_at_ms": round(time.time() * 1000),
                        "question": workflow_input.question,
                    }
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
                            "latency_ms": round(latency_ms, 2),
                            "finished_at_ms": round(time.time() * 1000),
                            "question": str(payload.get("question", "")),
                            "error": retry_error,
                            "attempt": next_attempt,
                            "max_retries": max_retries,
                            "next_retry_in_seconds": round(backoff, 3),
                        },
                    )
                    if backoff > 0.0:
                        time.sleep(backoff)

                    if broker.enqueue(job_id, retry_payload):
                        broker.ack(message_id)
                        job_store.release_processing(job_id)
                        logger.warning(
                            "Evaluation job retry scheduled job_id=%s attempt=%s/%s",
                            job_id,
                            next_attempt,
                            max_retries,
                        )
                    else:
                        terminal = {
                            "job_id": job_id,
                            "status": "dead_lettered",
                            "source": "async",
                            "latency_ms": round(latency_ms, 2),
                            "finished_at_ms": round(time.time() * 1000),
                            "question": str(payload.get("question", "")),
                            "error": "retry enqueue failed",
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
                        "latency_ms": round(latency_ms, 2),
                        "finished_at_ms": round(time.time() * 1000),
                        "question": str(payload.get("question", "")),
                        "error": retry_error,
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
                    logger.warning("Evaluation job dead-lettered job_id=%s: %s", job_id, str(error)[:120])

    logger.info("Evaluation worker stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
