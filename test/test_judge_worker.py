#!/usr/bin/env python3
"""Fast tests for JudgeWorker and workflow judge modes using mocks only."""

from __future__ import annotations

import logging
import unittest
from unittest.mock import MagicMock, patch

from multi_agent.models import ChatWorkflowInput, RetrievalResult, SafetyPreflightResult
from multi_agent.workflow import MultiAgentChatWorkflow
from multi_agent.workers import JudgeWorker


class DummyViolation(Exception):
    """Local placeholder for security violation type."""


def make_workflow_input(judge_mode: str = "sync") -> ChatWorkflowInput:
    return ChatWorkflowInput(
        question="What happened during Apollo 13?",
        chroma_dir="./chroma_db",
        collection_name="nasa_space_missions_test",
        n_results=3,
        mission_filter=None,
        model="gpt-3.5-turbo",
        evaluate=False,
        judge_mode=judge_mode,
        conversation_history=[],
        client_ip="127.0.0.1",
    )


def build_workflow() -> MultiAgentChatWorkflow:
    logger = logging.getLogger("test.judge")
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
    )


class TestJudgeWorkerUnit(unittest.TestCase):
    REQUIRED_JUDGE_KEYS = {"passed", "low_confidence", "overall_score", "source", "rationale"}

    def test_judge_worker_uses_heuristic_fallback_when_llm_unavailable(self):
        output_validator = MagicMock()
        output_validator.validate_response.return_value = {
            "severity": "warning",
            "issues": [{"type": "potential_hallucination"}],
        }

        sensitive_filter = MagicMock()
        sensitive_filter.audit_sensitive_exposure.return_value = None

        worker = JudgeWorker(
            logger=logging.getLogger("test.judge.unit"),
            output_validator=output_validator,
            sensitive_info_filter=sensitive_filter,
        )

        worker._llm_judge = MagicMock(return_value=(None, False))

        result = worker.judge(
            openai_key="fake-key",
            workflow_input=make_workflow_input(judge_mode="sync"),
            answer="Apollo 13 had an oxygen tank explosion.",
            contexts=["Apollo 13 experienced an oxygen tank explosion."],
        )

        self.assertEqual(result["source"], "heuristic")
        self.assertTrue(self.REQUIRED_JUDGE_KEYS.issubset(result.keys()))
        self.assertGreaterEqual(result["overall_score"], 0.0)
        self.assertLessEqual(result["overall_score"], 1.0)

    def test_judge_worker_prefers_llm_scores_when_available(self):
        worker = JudgeWorker(
            logger=logging.getLogger("test.judge.unit.llm"),
            output_validator=None,
            sensitive_info_filter=None,
        )

        worker._llm_judge = MagicMock(
            return_value=({
                "groundedness_score": 0.91,
                "safety_score": 0.84,
                "task_success_score": 0.88,
                "confidence": 0.9,
                "rationale": "Supported by retrieval context and safe.",
            }, False)
        )

        result = worker.judge(
            openai_key="fake-key",
            workflow_input=make_workflow_input(judge_mode="sync"),
            answer="Apollo 13 suffered an oxygen tank explosion and aborted landing.",
            contexts=["Apollo 13 suffered an oxygen tank explosion leading to abort."],
        )

        self.assertEqual(result["source"], "llm")
        self.assertAlmostEqual(result["groundedness_score"], 0.91)
        self.assertAlmostEqual(result["safety_score"], 0.84)
        self.assertAlmostEqual(result["task_success_score"], 0.88)
        self.assertTrue(self.REQUIRED_JUDGE_KEYS.issubset(result.keys()))

    def test_judge_worker_timeout_falls_back_and_marks_timeout(self):
        worker = JudgeWorker(
            logger=logging.getLogger("test.judge.unit.timeout"),
            output_validator=None,
            sensitive_info_filter=None,
            judge_timeout_seconds=1.8,
        )

        worker._llm_judge = MagicMock(return_value=(None, True))

        result = worker.judge(
            openai_key="fake-key",
            workflow_input=make_workflow_input(judge_mode="sync"),
            answer="Apollo 13 had an oxygen tank explosion.",
            contexts=["Apollo 13 experienced an oxygen tank explosion."],
        )

        self.assertEqual(result["source"], "heuristic")
        self.assertIn("timeout", result["rationale"].lower())


class TestWorkflowJudgeModes(unittest.TestCase):
    REQUIRED_JUDGE_KEYS = {"passed", "low_confidence", "overall_score", "source", "rationale"}

    def setUp(self):
        self.workflow = build_workflow()

        self.workflow.retrieval_worker.run = MagicMock(
            return_value=RetrievalResult(
                contexts=["Apollo 13 experienced an oxygen tank explosion."],
                context_text="Apollo 13 experienced an oxygen tank explosion.",
            )
        )
        self.workflow.analysis_worker.generate_answer = MagicMock(
            return_value="Apollo 13 had an oxygen tank explosion."
        )
        self.workflow.safety_worker.postflight = MagicMock(
            side_effect=lambda answer, contexts, client_ip: answer
        )
        self.workflow.analysis_worker.evaluate = MagicMock(return_value={})

    def test_blocked_preflight_returns_policy_judge_payload(self):
        self.workflow.safety_worker.preflight = MagicMock(
            return_value=SafetyPreflightResult(blocked_response="Blocked by policy")
        )

        result = self.workflow.run(make_workflow_input("sync"), openai_key="fake-key")

        self.assertTrue(result.blocked)
        self.assertEqual(result.judge.get("source"), "policy")
        self.assertTrue(result.judge.get("passed"))
        self.assertTrue(self.REQUIRED_JUDGE_KEYS.issubset(result.judge.keys()))

    def test_judge_mode_off_returns_disabled_judge_payload(self):
        self.workflow.safety_worker.preflight = MagicMock(
            return_value=SafetyPreflightResult(blocked_response=None)
        )

        result = self.workflow.run(make_workflow_input("off"), openai_key="fake-key")

        self.assertFalse(result.blocked)
        self.assertEqual(result.judge.get("source"), "disabled")
        self.assertTrue(result.judge.get("low_confidence"))
        self.assertTrue(self.REQUIRED_JUDGE_KEYS.issubset(result.judge.keys()))

    def test_judge_mode_sync_runs_judge_and_returns_completed_scores(self):
        self.workflow.safety_worker.preflight = MagicMock(
            return_value=SafetyPreflightResult(blocked_response=None)
        )
        self.workflow.judge_worker.judge = MagicMock(
            return_value={
                "groundedness_score": 0.8,
                "safety_score": 0.9,
                "task_success_score": 0.85,
                "overall_score": 0.85,
                "confidence": 0.9,
                "passed": True,
                "low_confidence": False,
                "rationale": "Looks good",
                "source": "llm",
            }
        )

        result = self.workflow.run(make_workflow_input("sync"), openai_key="fake-key")

        self.workflow.judge_worker.judge.assert_called_once()
        self.assertEqual(result.judge.get("source"), "llm")
        self.assertIn("overall_score", result.judge)
        self.assertTrue(self.REQUIRED_JUDGE_KEYS.issubset(result.judge.keys()))

    def test_judge_mode_async_returns_pending_and_records_result(self):
        self.workflow.safety_worker.preflight = MagicMock(
            return_value=SafetyPreflightResult(blocked_response=None)
        )
        self.workflow.judge_worker.judge = MagicMock(
            return_value={
                "groundedness_score": 0.75,
                "safety_score": 0.88,
                "task_success_score": 0.82,
                "overall_score": 0.81,
                "confidence": 0.86,
                "passed": True,
                "low_confidence": False,
                "rationale": "Async judge done",
                "source": "llm",
            }
        )

        # Make async deterministic in tests by executing immediately.
        self.workflow._judge_executor.submit = lambda fn, *args: fn(*args)

        result = self.workflow.run(make_workflow_input("async"), openai_key="fake-key")

        self.assertEqual(result.judge.get("status"), "pending")
        self.assertTrue(self.REQUIRED_JUDGE_KEYS.issubset(result.judge.keys()))
        latest = self.workflow.get_last_judge_result()
        self.assertIsNotNone(latest)
        self.assertEqual(latest["judge"].get("source"), "llm")

    def test_judge_schema_keys_stable_across_modes(self):
        self.workflow.safety_worker.preflight = MagicMock(
            return_value=SafetyPreflightResult(blocked_response=None)
        )
        self.workflow.judge_worker.judge = MagicMock(
            return_value={
                "groundedness_score": 0.82,
                "safety_score": 0.88,
                "task_success_score": 0.86,
                "overall_score": 0.85,
                "confidence": 0.9,
                "passed": True,
                "low_confidence": False,
                "rationale": "sync complete",
                "source": "llm",
            }
        )
        self.workflow._judge_executor.submit = lambda fn, *args: fn(*args)

        sync_result = self.workflow.run(make_workflow_input("sync"), openai_key="fake-key")
        async_result = self.workflow.run(make_workflow_input("async"), openai_key="fake-key")
        off_result = self.workflow.run(make_workflow_input("off"), openai_key="fake-key")

        self.assertTrue(self.REQUIRED_JUDGE_KEYS.issubset(sync_result.judge.keys()))
        self.assertTrue(self.REQUIRED_JUDGE_KEYS.issubset(async_result.judge.keys()))
        self.assertTrue(self.REQUIRED_JUDGE_KEYS.issubset(off_result.judge.keys()))

    def test_async_judge_persists_trace_context_and_includes_latency_annotation_score(self):
        self.workflow.safety_worker.preflight = MagicMock(
            return_value=SafetyPreflightResult(blocked_response=None)
        )
        self.workflow.judge_worker.judge = MagicMock(
            return_value={
                "groundedness_score": 0.75,
                "safety_score": 0.88,
                "task_success_score": 0.82,
                "overall_score": 0.81,
                "confidence": 0.86,
                "passed": True,
                "low_confidence": False,
                "rationale": "Async judge done",
                "source": "llm",
            }
        )
        self.workflow._judge_broker.enqueue = MagicMock(return_value=False)
        self.workflow._judge_executor.submit = lambda fn, *args: fn(*args)
        self.workflow._redis_job_store.set_result = MagicMock(return_value=True)

        workflow_input = make_workflow_input("async")
        workflow_input.trace_span_id = "abc123def4567890"
        workflow_input.session_id = "session-123"

        with patch("multi_agent.workflow.post_span_annotations") as post_annotations_mock:
            result = self.workflow.run(workflow_input, openai_key="fake-key")

        self.assertEqual(result.judge.get("status"), "pending")

        self.workflow._redis_job_store.set_result.assert_called_once()
        persisted_payload = self.workflow._redis_job_store.set_result.call_args[0][1]
        self.assertEqual(persisted_payload.get("trace_span_id"), "abc123def4567890")
        self.assertEqual(persisted_payload.get("session_id"), "session-123")

        post_annotations_mock.assert_called_once()
        annotation_scores = post_annotations_mock.call_args[0][1]
        self.assertIn("latency_ms", annotation_scores)
        self.assertGreaterEqual(annotation_scores["latency_ms"], 0.0)

    def test_async_judge_broker_enqueue_payload_includes_trace_and_session(self):
        self.workflow.safety_worker.preflight = MagicMock(
            return_value=SafetyPreflightResult(blocked_response=None)
        )

        self.workflow._judge_broker.enqueue = MagicMock(return_value=True)
        self.workflow._judge_broker.has_active_consumers = MagicMock(return_value=True)
        self.workflow._judge_executor.submit = MagicMock()

        workflow_input = make_workflow_input("async")
        workflow_input.trace_span_id = "feedfacecafebeef"
        workflow_input.session_id = "session-broker-1"

        result = self.workflow.run(workflow_input, openai_key="fake-key")

        self.assertEqual(result.judge.get("status"), "pending")
        self.workflow._judge_broker.enqueue.assert_called_once()
        enqueue_payload = self.workflow._judge_broker.enqueue.call_args[0][1]
        self.assertEqual(enqueue_payload.get("trace_span_id"), "feedfacecafebeef")
        self.assertEqual(enqueue_payload.get("session_id"), "session-broker-1")
        self.workflow._judge_executor.submit.assert_not_called()


if __name__ == "__main__":
    unittest.main(verbosity=2)
