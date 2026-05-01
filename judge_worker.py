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

from env_utils import load_project_env
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

    consumer_name = _consumer_name()
    logger.info("Judge worker started as consumer=%s", consumer_name)

    while _RUNNING:
        messages = broker.consume(consumer_name=consumer_name, count=1, block_ms=3000)
        if not messages:
            continue

        for message_id, payload in messages:
            job_id = str(payload.get("job_id", ""))
            if not job_id:
                broker.ack(message_id)
                continue

            started = time.perf_counter()
            try:
                workflow_input = _coerce_workflow_input(payload)
                answer = str(payload.get("answer", ""))
                contexts = payload.get("contexts") or []
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
            except Exception as error:
                latency_ms = (time.perf_counter() - started) * 1000.0
                final_payload = {
                    "job_id": job_id,
                    "timestamp_ms": round(time.time() * 1000),
                    "question": str(payload.get("question", "")),
                    "client_ip": str(payload.get("client_ip", "worker")),
                    "judge": {
                        "status": "error",
                        "passed": False,
                        "low_confidence": True,
                        "source": "async",
                        "rationale": str(error)[:200],
                        "overall_score": 0.0,
                        "groundedness_score": 0.0,
                        "safety_score": 0.0,
                        "task_success_score": 0.0,
                        "confidence": 0.0,
                    },
                    "latency_ms": round(latency_ms, 2),
                }
                logger.warning("Judge job failed job_id=%s: %s", job_id, str(error)[:120])

            job_store.set_result(job_id, final_payload)
            broker.ack(message_id)

    logger.info("Judge worker stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
