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

    consumer_name = _consumer_name()
    logger.info("Evaluation worker started as consumer=%s", consumer_name)

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
            except Exception as error:
                latency_ms = (time.perf_counter() - started) * 1000.0
                final_payload = {
                    "job_id": job_id,
                    "status": "error",
                    "source": "async",
                    "latency_ms": round(latency_ms, 2),
                    "finished_at_ms": round(time.time() * 1000),
                    "question": str(payload.get("question", "")),
                    "error": str(error)[:200],
                }
                logger.warning("Evaluation job failed job_id=%s: %s", job_id, str(error)[:120])

            job_store.set_result(job_id, final_payload)
            broker.ack(message_id)

    logger.info("Evaluation worker stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
