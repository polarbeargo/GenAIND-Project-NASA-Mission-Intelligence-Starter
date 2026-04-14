#!/usr/bin/env python3
"""Tests for stage timeout/circuit-breaker graceful degradation behavior."""

from __future__ import annotations

import logging
import unittest
from unittest.mock import MagicMock

from multi_agent.models import ChatWorkflowInput, RetrievalResult, SafetyPreflightResult
from multi_agent.workflow import MultiAgentChatWorkflow


class DummyViolation(Exception):
    pass


def build_workflow(evaluation_mode: str = "async") -> MultiAgentChatWorkflow:
    logger = logging.getLogger("test.stage.timeouts")
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
        retrieval_timeout_seconds=0.01,
        generation_timeout_seconds=0.05,
        evaluation_timeout_seconds=0.05,
        breaker_failure_threshold=1,
        breaker_recovery_seconds=60,
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


class TestStageTimeouts(unittest.TestCase):
    def test_retrieval_failure_returns_safe_fallback(self):
        workflow = build_workflow()

        workflow.retrieval_worker.run = MagicMock(side_effect=RuntimeError("retrieval down"))
        workflow.safety_worker.preflight = MagicMock(
            return_value=SafetyPreflightResult(blocked_response=None)
        )

        result = workflow.run(make_input(evaluate=True), openai_key="fake-key")

        self.assertFalse(result.blocked)
        self.assertEqual(result.contexts, [])
        self.assertEqual(result.evaluation, {})
        self.assertEqual(result.judge.get("source"), "degraded")
        self.assertIn("could not retrieve trusted mission sources", result.answer.lower())

    def test_evaluation_failure_returns_empty_evaluation(self):
        workflow = build_workflow(evaluation_mode="sync")

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
        workflow.analysis_worker.evaluate = MagicMock(side_effect=RuntimeError("eval failure"))

        result = workflow.run(make_input(evaluate=True), openai_key="fake-key")

        self.assertEqual(result.answer, "answer")
        self.assertEqual(result.evaluation, {})


if __name__ == "__main__":
    unittest.main(verbosity=2)
