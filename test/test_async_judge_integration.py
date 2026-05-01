#!/usr/bin/env python3
"""Minimal integration tests for async judge broker and job-store paths."""

from __future__ import annotations

import logging
import multiprocessing
import threading
import time
import unittest
import uuid
from unittest.mock import MagicMock

from infra.redis_client import RedisClient
from infra.redis_job_store import RedisAsyncJobStore
from infra.redis_judge_broker import RedisJudgeBroker
from multi_agent.models import ChatWorkflowInput, RetrievalResult, SafetyPreflightResult
from multi_agent.workflow import MultiAgentChatWorkflow


class DummyViolation(Exception):
    """Placeholder security exception."""


def build_workflow(
    *,
    judge_broker_enabled: bool = False,
    judge_broker_stream: str = "judge:jobs",
    judge_broker_group: str = "judge-workers",
) -> MultiAgentChatWorkflow:
    logger = logging.getLogger("test.async.judge.integration")
    logger.setLevel(logging.CRITICAL)

    return MultiAgentChatWorkflow(
        get_collection_fn=lambda _a, _b: (None, True, None),
        logger=logger,
        jailbreak_keywords=[],
        resource_limiter=None,
        prompt_injection_detector=None,
        vector_security_validator=None,
        output_validator=None,
        sensitive_info_filter=None,
        security_violation=DummyViolation,
        security_auditor=None,
        security_level=None,
        evaluation_mode="off",
        judge_broker_enabled=judge_broker_enabled,
        judge_broker_stream=judge_broker_stream,
        judge_broker_group=judge_broker_group,
    )


def make_input() -> ChatWorkflowInput:
    return ChatWorkflowInput(
        question="What happened during Apollo 13?",
        chroma_dir="./chroma_db",
        collection_name="nasa_space_missions_test",
        n_results=3,
        mission_filter="apollo13",
        model="gpt-3.5-turbo",
        evaluate=False,
        judge_mode="async",
        conversation_history=[],
        client_ip="127.0.0.1",
    )


def _external_judge_worker_once(stream_name: str, group_name: str, consumer_name: str) -> None:
    """Consume one broker message and write a deterministic result to Redis."""
    redis_client = RedisClient(enabled=True)
    if not redis_client.is_available():
        return

    broker = RedisJudgeBroker(
        redis_client,
        stream_name=stream_name,
        consumer_group=group_name,
        enabled=True,
    )
    job_store = RedisAsyncJobStore(redis_client, retention_ttl_seconds=120)

    deadline = time.time() + 4.0
    while time.time() < deadline:
        messages = broker.consume(consumer_name=consumer_name, count=1, block_ms=250)
        if not messages:
            continue

        message_id, payload = messages[0]
        job_id = str(payload.get("job_id", ""))
        if not job_id:
            broker.ack(message_id)
            return

        result_payload = {
            "job_id": job_id,
            "timestamp_ms": round(time.time() * 1000),
            "question": str(payload.get("question", "")),
            "client_ip": str(payload.get("client_ip", "worker")),
            "judge": {
                "status": "completed",
                "source": "external-worker",
                "passed": True,
                "low_confidence": False,
                "overall_score": 0.91,
            },
            "latency_ms": 12.0,
        }
        job_store.set_result(job_id, result_payload)
        broker.ack(message_id)
        return


class TestAsyncJudgeFallback(unittest.TestCase):
    """Integration-style fallback behavior without requiring Redis."""

    def test_async_judge_falls_back_when_broker_disabled(self):
        workflow = build_workflow(judge_broker_enabled=False)

        workflow.retrieval_worker.run = MagicMock(
            return_value=RetrievalResult(
                contexts=["Apollo 13 experienced an oxygen tank explosion."],
                metadatas=[{"mission": "apollo13"}],
                context_text="Apollo 13 experienced an oxygen tank explosion.",
            )
        )
        workflow.safety_worker.preflight = MagicMock(
            return_value=SafetyPreflightResult(blocked_response=None)
        )
        workflow.analysis_worker.generate_answer = MagicMock(return_value="Apollo 13 had an oxygen tank explosion.")
        workflow.safety_worker.postflight = MagicMock(
            side_effect=lambda answer, contexts, client_ip: answer
        )

        workflow.judge_worker.judge = MagicMock(
            return_value={
                "groundedness_score": 0.8,
                "safety_score": 0.9,
                "task_success_score": 0.85,
                "overall_score": 0.85,
                "confidence": 0.9,
                "passed": True,
                "low_confidence": False,
                "rationale": "fallback path",
                "source": "llm",
            }
        )

        # Deterministic async execution in test to avoid timing flake.
        workflow._judge_executor.submit = lambda fn, *args: fn(*args)

        result = workflow.run(make_input(), openai_key="fake-key")
        self.assertEqual(result.judge.get("status"), "pending")
        job_id = result.judge.get("job_id")
        self.assertTrue(job_id)

        latest = workflow.get_last_judge_result()
        self.assertIsNotNone(latest)
        self.assertEqual(latest.get("job_id"), job_id)
        self.assertEqual(latest["judge"].get("source"), "llm")


class TestRedisJudgeIntegration(unittest.TestCase):
    """Broker/job-store integration tests that run only when Redis is available."""

    @classmethod
    def setUpClass(cls):
        cls.redis = RedisClient(enabled=True)
        if not cls.redis.is_available():
            raise unittest.SkipTest("Redis is not available; skipping Redis integration tests")

    def test_broker_enqueue_consume_ack_cycle(self):
        stream_name = f"test:judge:jobs:{uuid.uuid4()}"
        group_name = f"test-judge-workers-{uuid.uuid4().hex[:8]}"
        broker = RedisJudgeBroker(
            self.redis,
            stream_name=stream_name,
            consumer_group=group_name,
            enabled=True,
        )

        job_id = f"job-{uuid.uuid4()}"
        payload = {
            "job_id": job_id,
            "question": "What happened during Apollo 13?",
            "answer": "An oxygen tank exploded.",
            "contexts": ["Apollo 13 oxygen tank explosion context"],
        }

        self.assertTrue(broker.enqueue(job_id, payload))

        messages = broker.consume(consumer_name="test-consumer", count=1, block_ms=200)
        self.assertEqual(len(messages), 1)
        message_id, consumed_payload = messages[0]
        self.assertEqual(consumed_payload.get("job_id"), job_id)
        self.assertEqual(consumed_payload.get("question"), payload["question"])

        self.assertTrue(broker.ack(message_id))

        # Unique stream + acked message should leave no new messages.
        self.assertEqual(
            broker.consume(consumer_name="test-consumer", count=1, block_ms=50),
            [],
        )

    def test_job_store_pending_to_completed_polling(self):
        job_store = RedisAsyncJobStore(self.redis, retention_ttl_seconds=120)
        job_id = f"job-{uuid.uuid4()}"

        created = job_store.create_job(
            job_id=job_id,
            job_type="judge",
            request_id=f"req-{uuid.uuid4()}",
        )
        self.assertTrue(created)
        self.assertEqual(job_store.get_status(job_id), "pending")

        def _complete_later():
            time.sleep(0.15)
            job_store.set_result(
                job_id,
                {
                    "job_id": job_id,
                    "judge": {"status": "completed", "source": "async", "overall_score": 0.87},
                },
            )

        threading.Thread(target=_complete_later, daemon=True).start()

        deadline = time.time() + 2.0
        while time.time() < deadline:
            if job_store.get_status(job_id) == "completed":
                break
            time.sleep(0.05)

        self.assertEqual(job_store.get_status(job_id), "completed")
        result = job_store.get_result(job_id)
        self.assertIsNotNone(result)
        self.assertEqual(result.get("job_id"), job_id)
        self.assertEqual(result.get("judge", {}).get("source"), "async")

    def test_broker_enabled_async_judge_end_to_end_with_external_worker_process(self):
        stream_name = f"test:judge:e2e:{uuid.uuid4()}"
        group_name = f"test-judge-e2e-{uuid.uuid4().hex[:8]}"
        workflow = build_workflow(
            judge_broker_enabled=True,
            judge_broker_stream=stream_name,
            judge_broker_group=group_name,
        )
        # Force workflow to use Redis-enabled broker/job-store regardless of env flags.
        workflow._judge_broker = RedisJudgeBroker(
            self.redis,
            stream_name=stream_name,
            consumer_group=group_name,
            enabled=True,
        )
        workflow._redis_job_store = RedisAsyncJobStore(self.redis, retention_ttl_seconds=120)

        workflow.retrieval_worker.run = MagicMock(
            return_value=RetrievalResult(
                contexts=["Apollo 13 experienced an oxygen tank explosion."],
                metadatas=[{"mission": "apollo13"}],
                context_text="Apollo 13 experienced an oxygen tank explosion.",
            )
        )
        workflow.safety_worker.preflight = MagicMock(
            return_value=SafetyPreflightResult(blocked_response=None)
        )
        workflow.analysis_worker.generate_answer = MagicMock(
            return_value="Apollo 13 had an oxygen tank explosion."
        )
        workflow.safety_worker.postflight = MagicMock(
            side_effect=lambda answer, contexts, client_ip: answer
        )
        workflow._judge_executor.submit = MagicMock(
            side_effect=AssertionError("Fallback judge executor should not be used in broker e2e test")
        )

        worker_process = multiprocessing.Process(
            target=_external_judge_worker_once,
            args=(stream_name, group_name, "test-ext-worker"),
            daemon=True,
        )
        worker_process.start()

        result = workflow.run(make_input(), openai_key="fake-key")
        self.assertEqual(result.judge.get("status"), "pending")
        job_id = result.judge.get("job_id")
        self.assertTrue(job_id)

        deadline = time.time() + 5.0
        redis_result = None
        while time.time() < deadline:
            redis_result = workflow._redis_job_store.get_result(job_id)
            if redis_result is not None:
                break
            time.sleep(0.05)

        worker_process.join(timeout=1.0)
        if worker_process.is_alive():
            worker_process.terminate()
            worker_process.join(timeout=1.0)

        self.assertIsNotNone(redis_result)
        self.assertEqual(redis_result.get("job_id"), job_id)
        self.assertEqual(redis_result.get("judge", {}).get("source"), "external-worker")
        workflow._judge_executor.submit.assert_not_called()


if __name__ == "__main__":
    unittest.main(verbosity=2)