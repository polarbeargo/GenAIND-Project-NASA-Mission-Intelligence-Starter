#!/usr/bin/env python3
"""Tests for async/sync evaluation modes in MultiAgentChatWorkflow."""

from __future__ import annotations

import logging
import unittest
from concurrent.futures import TimeoutError
from unittest.mock import MagicMock

from multi_agent.models import ChatWorkflowInput, RetrievalResult, SafetyPreflightResult
from multi_agent.workflow import MultiAgentChatWorkflow, StageOverloadError


class DummyViolation(Exception):
    """Placeholder security exception."""


def build_workflow(evaluation_mode: str = "async") -> MultiAgentChatWorkflow:
    logger = logging.getLogger("test.workflow.evaluation")
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
    )


def make_input(evaluate: bool = True) -> ChatWorkflowInput:
    return ChatWorkflowInput(
        question="What caused the Apollo 13 emergency?",
        chroma_dir="./chroma_db_openai",
        collection_name="nasa_space_missions_text",
        n_results=3,
        mission_filter="apollo13",
        model="gpt-3.5-turbo",
        evaluate=evaluate,
        judge_mode="off",
        conversation_history=[],
        client_ip="127.0.0.1",
    )


class TestEvaluationModes(unittest.TestCase):
    def _seed_common_mocks(self, workflow: MultiAgentChatWorkflow):
        workflow.retrieval_worker.run = MagicMock(
            return_value=RetrievalResult(
                contexts=["Apollo 13 had an oxygen tank explosion."],
                metadatas=[{"mission": "apollo13"}],
                context_text="Apollo 13 had an oxygen tank explosion.",
            )
        )
        workflow.safety_worker.preflight = MagicMock(
            return_value=SafetyPreflightResult(blocked_response=None)
        )
        workflow.analysis_worker.generate_answer = MagicMock(return_value="answer")
        workflow.safety_worker.postflight = MagicMock(
            side_effect=lambda answer, contexts, client_ip: answer
        )

    def test_async_evaluation_returns_pending_with_job_id(self):
        workflow = build_workflow(evaluation_mode="async")
        self._seed_common_mocks(workflow)
        workflow.analysis_worker.evaluate = MagicMock(return_value={"faithfulness": 0.93})

        # Force local async path and deterministic processing regardless of
        # external Redis availability in the test environment.
        workflow._evaluation_broker.enqueue = MagicMock(return_value=False)
        workflow._redis_job_store.is_completed = MagicMock(return_value=False)
        workflow._redis_job_store.acquire_processing = MagicMock(return_value=True)
        workflow._redis_job_store.release_processing = MagicMock(return_value=True)

        # Force deterministic async execution for test.
        workflow._eval_job_executor.submit = lambda fn, *args: fn(*args)
        workflow._eval_executor.submit = lambda fn, *args: fn(*args)

        result = workflow.run(make_input(evaluate=True), openai_key="fake-key")

        self.assertEqual(result.evaluation.get("status"), "pending")
        self.assertEqual(result.evaluation.get("source"), "async")
        job_id = result.evaluation.get("job_id")
        self.assertTrue(job_id)

        stored = workflow.get_evaluation_job(job_id)
        self.assertIsNotNone(stored)
        self.assertEqual(stored.get("status"), "completed")
        self.assertAlmostEqual(stored.get("faithfulness"), 0.93)

    def test_sync_evaluation_runs_inline_for_debug(self):
        workflow = build_workflow(evaluation_mode="sync")
        self._seed_common_mocks(workflow)
        workflow.analysis_worker.evaluate = MagicMock(return_value={"faithfulness": 0.88})

        result = workflow.run(make_input(evaluate=True), openai_key="fake-key")

        self.assertEqual(result.evaluation.get("source"), "sync")
        self.assertEqual(result.evaluation.get("status"), "completed")
        self.assertAlmostEqual(result.evaluation.get("faithfulness"), 0.88)
        workflow.analysis_worker.evaluate.assert_called_once()

    def test_evaluation_off_mode_returns_disabled_payload(self):
        workflow = build_workflow(evaluation_mode="off")
        self._seed_common_mocks(workflow)
        workflow.analysis_worker.evaluate = MagicMock(return_value={"faithfulness": 0.88})

        result = workflow.run(make_input(evaluate=True), openai_key="fake-key")

        self.assertEqual(result.evaluation.get("status"), "disabled")
        self.assertEqual(result.evaluation.get("source"), "disabled")
        workflow.analysis_worker.evaluate.assert_not_called()

    def test_async_evaluation_overload_returns_skipped_payload_non_fatal(self):
        workflow = build_workflow(evaluation_mode="async")
        self._seed_common_mocks(workflow)
        workflow.analysis_worker.evaluate = MagicMock(return_value={"faithfulness": 0.93})
        workflow._evaluation_broker.enqueue = MagicMock(return_value=False)
        workflow._eval_job_executor.submit = MagicMock(side_effect=StageOverloadError("eval queue full"))

        result = workflow.run(make_input(evaluate=True), openai_key="fake-key")

        self.assertEqual(result.evaluation.get("status"), "skipped")
        self.assertEqual(result.evaluation.get("source"), "overload")
        self.assertTrue(result.evaluation.get("job_id"))

        stored = workflow.get_evaluation_job(result.evaluation["job_id"])
        self.assertIsNotNone(stored)
        self.assertEqual(stored.get("status"), "skipped")

    def test_async_evaluation_timeout_records_non_fatal_error_payload(self):
        workflow = build_workflow(evaluation_mode="async")
        self._seed_common_mocks(workflow)
        workflow._evaluation_timeout_seconds = 0.05
        workflow._evaluation_broker.enqueue = MagicMock(return_value=False)
        workflow._redis_job_store.is_completed = MagicMock(return_value=False)
        workflow._redis_job_store.acquire_processing = MagicMock(return_value=True)
        workflow._redis_job_store.release_processing = MagicMock(return_value=True)

        class TimeoutFuture:
            def result(self, timeout=None):
                raise TimeoutError()

            def cancel(self):
                return True

        workflow._eval_job_executor.submit = lambda fn, *args: fn(*args)
        workflow._eval_executor.submit = lambda fn, *args: TimeoutFuture()

        result = workflow.run(make_input(evaluate=True), openai_key="fake-key")

        self.assertEqual(result.evaluation.get("status"), "pending")
        job_id = result.evaluation.get("job_id")
        self.assertTrue(job_id)

        stored = workflow.get_evaluation_job(job_id)
        self.assertIsNotNone(stored)
        self.assertEqual(stored.get("status"), "error")
        self.assertEqual(stored.get("source"), "async")
        self.assertIn("timed out", str(stored.get("error", "")).lower())

    def test_get_evaluation_job_prefers_l2_when_l1_is_stale_pending(self):
        workflow = build_workflow(evaluation_mode="async")
        job_id = "job-123"

        with workflow._evaluation_lock:
            workflow._evaluation_results[job_id] = {
                "job_id": job_id,
                "status": "pending",
                "source": "async",
            }

        workflow._redis_job_store.get_result = MagicMock(
            return_value={
                "job_id": job_id,
                "status": "completed",
                "source": "async",
                "faithfulness": 0.91,
            }
        )

        payload = workflow.get_evaluation_job(job_id)

        self.assertIsNotNone(payload)
        self.assertEqual(payload.get("status"), "completed")
        self.assertAlmostEqual(float(payload.get("faithfulness", 0.0)), 0.91)

        with workflow._evaluation_lock:
            hydrated = workflow._evaluation_results.get(job_id)
            self.assertIsNotNone(hydrated)
            self.assertEqual(hydrated.get("status"), "completed")


if __name__ == "__main__":
    unittest.main(verbosity=2)
