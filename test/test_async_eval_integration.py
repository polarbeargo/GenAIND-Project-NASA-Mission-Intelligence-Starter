#!/usr/bin/env python3
"""Integration tests for async evaluation broker and job-store paths.

Mirrors test_async_judge_integration.py with equivalent reliability
coverage for the evaluation pipeline:
  - enqueue / consume / ack cycle
  - pending-to-completed polling (threaded)
  - idempotency: processing lock + completion marker
  - dead-letter queue writes with reason/attempt/consumer metadata
  - end-to-end broker path with an external worker process
"""

from __future__ import annotations

import logging
import multiprocessing
import threading
import time
import unittest
import uuid
from unittest.mock import MagicMock

import pytest

from infra.redis_client import RedisClient
from infra.redis_evaluation_broker import RedisEvaluationBroker
from infra.redis_job_store import RedisAsyncJobStore
from multi_agent.models import ChatWorkflowInput, RetrievalResult, SafetyPreflightResult
from multi_agent.workflow import MultiAgentChatWorkflow


class DummyViolation(Exception):
    """Placeholder security exception."""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def build_workflow(
    *,
    evaluation_broker_enabled: bool = False,
    evaluation_broker_stream: str = "eval:jobs",
    evaluation_broker_group: str = "eval-workers",
    evaluation_mode: str = "async",
) -> MultiAgentChatWorkflow:
    logger = logging.getLogger("test.async.eval.integration")
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
        evaluation_mode=evaluation_mode,
        evaluation_broker_enabled=evaluation_broker_enabled,
        evaluation_broker_stream=evaluation_broker_stream,
        evaluation_broker_group=evaluation_broker_group,
    )


def make_input(*, evaluate: bool = True) -> ChatWorkflowInput:
    return ChatWorkflowInput(
        question="What caused the Apollo 13 emergency?",
        chroma_dir="./chroma_db",
        collection_name="nasa_space_missions_test",
        n_results=3,
        mission_filter="apollo13",
        model="gpt-3.5-turbo",
        evaluate=evaluate,
        judge_mode="off",
        conversation_history=[],
        client_ip="127.0.0.1",
    )


def _seed_common_mocks(workflow: MultiAgentChatWorkflow) -> None:
    """Attach deterministic mocks for retrieval / safety / generation stages."""
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


def _external_eval_worker_once(
    stream_name: str, group_name: str, consumer_name: str
) -> None:
    """Subprocess target: consume one evaluation broker message and write a result."""
    redis_client = RedisClient(enabled=True)
    if not redis_client.is_available():
        return

    broker = RedisEvaluationBroker(
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
            "status": "completed",
            "source": "external-worker",
            "faithfulness": 0.88,
            "answer_relevancy": 0.91,
            "latency_ms": 15.0,
            "finished_at_ms": round(time.time() * 1000),
            "question": str(payload.get("question", "")),
        }
        job_store.set_result(job_id, result_payload)
        broker.ack(message_id)
        return


# ---------------------------------------------------------------------------
# Fallback suite (no Redis required)
# ---------------------------------------------------------------------------

class TestAsyncEvalFallback(unittest.TestCase):
    """Integration-style fallback behaviour without requiring a live Redis."""

    def test_async_eval_falls_back_when_broker_disabled(self):
        """With broker disabled the local async executor runs the evaluation."""
        workflow = build_workflow(evaluation_broker_enabled=False, evaluation_mode="async")
        _seed_common_mocks(workflow)
        workflow.analysis_worker.evaluate = MagicMock(
            return_value={"faithfulness": 0.93, "answer_relevancy": 0.90}
        )

        # Force deterministic synchronous execution to avoid timing flake.
        workflow._eval_executor.submit = lambda fn, *args: fn(*args)
        workflow._eval_job_executor.submit = lambda fn, *args: fn(*args)

        result = workflow.run(make_input(evaluate=True), openai_key="fake-key")

        # Workflow returns pending immediately; evaluation ran synchronously in test.
        self.assertEqual(result.evaluation.get("status"), "pending")
        self.assertEqual(result.evaluation.get("source"), "async")
        job_id = result.evaluation.get("job_id")
        self.assertTrue(job_id, "evaluation job_id must be set")

        stored = workflow.get_evaluation_job(job_id)
        self.assertIsNotNone(stored)
        self.assertEqual(stored.get("status"), "completed")
        self.assertAlmostEqual(stored.get("faithfulness"), 0.93)

    def test_eval_disabled_mode_returns_disabled_payload(self):
        """evaluation_mode='off' must never touch the executor or broker."""
        workflow = build_workflow(evaluation_broker_enabled=False, evaluation_mode="off")
        _seed_common_mocks(workflow)
        workflow.analysis_worker.evaluate = MagicMock()

        result = workflow.run(make_input(evaluate=True), openai_key="fake-key")

        self.assertEqual(result.evaluation.get("status"), "disabled")
        workflow.analysis_worker.evaluate.assert_not_called()


# ---------------------------------------------------------------------------
# Redis-backed integration suite
# ---------------------------------------------------------------------------

@pytest.mark.redis
class TestRedisEvalIntegration(unittest.TestCase):
    """Broker / job-store integration tests — skipped when Redis is unavailable.

    Each test is decorated via the class-level ``@pytest.mark.redis`` marker so
    that redis-absent CI shows every skipped test individually (``s`` per test)
    rather than silently dropping the whole class via a ``setUpClass`` exception.
    Use ``--require-redis`` to turn skips into failures in CI with a Redis service.
    """

    @classmethod
    def setUpClass(cls):
        cls.redis = RedisClient(enabled=True)

    # ------------------------------------------------------------------
    # Broker enqueue / consume / ack cycle
    # ------------------------------------------------------------------

    def test_broker_enqueue_consume_ack_cycle(self):
        stream_name = f"test:eval:jobs:{uuid.uuid4()}"
        group_name = f"test-eval-workers-{uuid.uuid4().hex[:8]}"
        broker = RedisEvaluationBroker(
            self.redis,
            stream_name=stream_name,
            consumer_group=group_name,
            enabled=True,
        )

        job_id = f"job-{uuid.uuid4()}"
        payload = {
            "job_id": job_id,
            "question": "What caused the Apollo 13 emergency?",
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

        # Unique stream + acked message must leave no pending messages.
        self.assertEqual(
            broker.consume(consumer_name="test-consumer", count=1, block_ms=50),
            [],
        )

    # ------------------------------------------------------------------
    # Job store: pending → completed polling (threaded)
    # ------------------------------------------------------------------

    def test_job_store_pending_to_completed_polling(self):
        job_store = RedisAsyncJobStore(self.redis, retention_ttl_seconds=120)
        job_id = f"job-{uuid.uuid4()}"

        created = job_store.create_job(
            job_id=job_id,
            job_type="evaluation",
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
                    "status": "completed",
                    "source": "async",
                    "faithfulness": 0.88,
                    "answer_relevancy": 0.91,
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
        self.assertEqual(result.get("source"), "async")
        self.assertAlmostEqual(result.get("faithfulness"), 0.88)

    # ------------------------------------------------------------------
    # Idempotency: processing lock + completion marker
    # ------------------------------------------------------------------

    def test_job_store_processing_lock_and_completion_marker(self):
        """Second acquire_processing call must fail; set_result must mark completed."""
        job_store = RedisAsyncJobStore(self.redis, retention_ttl_seconds=120)
        job_id = f"job-{uuid.uuid4()}"

        # First worker acquires the lock.
        self.assertTrue(job_store.acquire_processing(job_id, processing_ttl_seconds=120))
        # Duplicate worker must be blocked by the lock.
        self.assertFalse(job_store.acquire_processing(job_id, processing_ttl_seconds=120))

        self.assertTrue(
            job_store.set_result(
                job_id,
                {
                    "job_id": job_id,
                    "status": "completed",
                    "source": "async",
                    "faithfulness": 0.90,
                },
            )
        )
        self.assertTrue(job_store.is_completed(job_id))

    def test_processing_lock_blocks_concurrent_threads(self):
        """Two threads racing to acquire the same job lock must not both succeed."""
        job_store = RedisAsyncJobStore(self.redis, retention_ttl_seconds=120)
        job_id = f"job-{uuid.uuid4()}"

        results: list[bool] = []
        barrier = threading.Barrier(2)

        def _try_acquire():
            barrier.wait()  # release both threads simultaneously
            results.append(bool(job_store.acquire_processing(job_id, processing_ttl_seconds=30)))

        t1 = threading.Thread(target=_try_acquire, daemon=True)
        t2 = threading.Thread(target=_try_acquire, daemon=True)
        t1.start()
        t2.start()
        t1.join(timeout=3)
        t2.join(timeout=3)

        self.assertEqual(len(results), 2)
        # Exactly one thread must win the lock.
        self.assertEqual(results.count(True), 1)
        self.assertEqual(results.count(False), 1)

    def test_processing_lock_release_requires_matching_token(self):
        job_store = RedisAsyncJobStore(self.redis, retention_ttl_seconds=120)
        job_id = f"job-{uuid.uuid4()}"

        token = job_store.acquire_processing(job_id, processing_ttl_seconds=120)
        self.assertIsInstance(token, str)
        self.assertFalse(job_store.release_processing(job_id, "wrong-token"))
        self.assertFalse(job_store.is_completed(job_id))
        self.assertFalse(job_store.acquire_processing(job_id, processing_ttl_seconds=120))
        self.assertTrue(job_store.release_processing(job_id, token))
        reacquired = job_store.acquire_processing(job_id, processing_ttl_seconds=120)
        self.assertIsInstance(reacquired, str)

    # ------------------------------------------------------------------
    # Dead-letter queue
    # ------------------------------------------------------------------

    def test_broker_dead_letter_writes_dlq_entry(self):
        stream_name = f"test:eval:jobs:{uuid.uuid4()}"
        group_name = f"test-eval-workers-{uuid.uuid4().hex[:8]}"
        dlq_stream = f"{stream_name}:dlq"
        broker = RedisEvaluationBroker(
            self.redis,
            stream_name=stream_name,
            consumer_group=group_name,
            dead_letter_stream=dlq_stream,
            enabled=True,
        )

        payload = {
            "job_id": f"job-{uuid.uuid4()}",
            "question": "malformed evaluation payload",
            "_attempt": 3,
        }
        self.assertTrue(
            broker.dead_letter(
                message_id="1-0",
                payload=payload,
                reason="max_retries_exhausted",
                consumer_name="test-eval-consumer",
                attempt=3,
            )
        )

        rows = self.redis._client.xread({dlq_stream: "0-0"}, count=1, block=100)
        self.assertTrue(rows, "DLQ stream must contain the dead-lettered entry")
        _stream, entries = rows[0]
        _message_id, fields = entries[0]
        self.assertEqual(fields.get("reason"), "max_retries_exhausted")
        self.assertEqual(fields.get("attempt"), "3")
        self.assertEqual(fields.get("consumer"), "test-eval-consumer")
        self.assertEqual(fields.get("source_stream"), stream_name)

    def test_broker_dead_letter_poison_decode_error(self):
        """Poison messages with a decode error must also land on the DLQ."""
        stream_name = f"test:eval:jobs:{uuid.uuid4()}"
        group_name = f"test-eval-workers-{uuid.uuid4().hex[:8]}"
        dlq_stream = f"{stream_name}:dlq"
        broker = RedisEvaluationBroker(
            self.redis,
            stream_name=stream_name,
            consumer_group=group_name,
            dead_letter_stream=dlq_stream,
            enabled=True,
        )

        poison_payload = {
            "_decode_error": "Expecting value: line 1 column 1 (char 0)",
            "_raw_payload": "{broken json",
        }
        self.assertTrue(
            broker.dead_letter(
                message_id="2-0",
                payload=poison_payload,
                reason="decode_error",
                consumer_name="test-eval-consumer",
                attempt=0,
            )
        )

        rows = self.redis._client.xread({dlq_stream: "0-0"}, count=1, block=100)
        self.assertTrue(rows)
        _stream, entries = rows[0]
        _message_id, fields = entries[0]
        self.assertEqual(fields.get("reason"), "decode_error")
        self.assertEqual(fields.get("attempt"), "0")

    # ------------------------------------------------------------------
    # End-to-end: broker-enabled path with an external worker process
    # ------------------------------------------------------------------

    def test_broker_enabled_async_eval_end_to_end_with_external_worker_process(self):
        stream_name = f"test:eval:e2e:{uuid.uuid4()}"
        group_name = f"test-eval-e2e-{uuid.uuid4().hex[:8]}"

        workflow = build_workflow(
            evaluation_broker_enabled=True,
            evaluation_broker_stream=stream_name,
            evaluation_broker_group=group_name,
            evaluation_mode="async",
        )
        # Override broker and job-store to use the test-scoped Redis instance.
        workflow._evaluation_broker = RedisEvaluationBroker(
            self.redis,
            stream_name=stream_name,
            consumer_group=group_name,
            enabled=True,
        )
        # Keep this test deterministic: consumer startup can race with the first
        # request; explicitly treat consumer availability as ready.
        workflow._evaluation_broker.has_active_consumers = lambda: True
        workflow._redis_job_store = RedisAsyncJobStore(self.redis, retention_ttl_seconds=120)

        _seed_common_mocks(workflow)
        # Fallback executor must NOT be used when broker is active.
        workflow._eval_executor.submit = MagicMock(
            side_effect=AssertionError(
                "Fallback eval executor must not be used when broker is enabled"
            )
        )

        # Use fork to inherit sys.path from the pytest process; spawn (macOS
        # default on Python 3.13) would start a fresh interpreter that cannot
        # import project-local modules without a PYTHONPATH setup.
        _fork_ctx = multiprocessing.get_context("fork")
        worker_process = _fork_ctx.Process(
            target=_external_eval_worker_once,
            args=(stream_name, group_name, "test-ext-eval-worker"),
            daemon=True,
        )
        worker_process.start()

        result = workflow.run(make_input(evaluate=True), openai_key="fake-key")
        self.assertEqual(result.evaluation.get("status"), "pending")
        self.assertEqual(result.evaluation.get("source"), "async")
        job_id = result.evaluation.get("job_id")
        self.assertTrue(job_id, "job_id must be present in pending evaluation payload")

        deadline = time.time() + 5.0
        redis_result = None
        while time.time() < deadline:
            candidate = workflow._redis_job_store.get_result(job_id)
            if candidate is not None and candidate.get("status") == "completed":
                redis_result = candidate
                break
            time.sleep(0.05)

        worker_process.join(timeout=1.0)
        if worker_process.is_alive():
            worker_process.terminate()
            worker_process.join(timeout=1.0)

        self.assertIsNotNone(redis_result, "external worker must write a result within deadline")
        self.assertEqual(redis_result.get("job_id"), job_id)
        self.assertEqual(redis_result.get("source"), "external-worker")
        self.assertEqual(redis_result.get("status"), "completed")
        self.assertAlmostEqual(redis_result.get("faithfulness"), 0.88)
        workflow._eval_executor.submit.assert_not_called()


if __name__ == "__main__":
    unittest.main(verbosity=2)
