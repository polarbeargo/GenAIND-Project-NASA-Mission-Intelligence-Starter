#!/usr/bin/env python3
"""Regression tests for multi-layer workflow caches."""

from __future__ import annotations

import logging
import unittest
from unittest.mock import MagicMock

from multi_agent.models import ChatWorkflowInput, RetrievalResult, SafetyPreflightResult
from multi_agent.retrieval_depth import HeuristicRetrievalDepthConfig, HeuristicRetrievalDepthPolicy
from multi_agent.workflow import MultiAgentChatWorkflow


class DummyViolation(Exception):
    """Placeholder security exception."""


class FixedDepthPolicy:
    """Policy used to validate workflow policy injection."""

    def __init__(self, depth: int):
        self._depth = depth

    def resolve_n_results(self, workflow_input: ChatWorkflowInput) -> int:
        return self._depth


def build_workflow() -> MultiAgentChatWorkflow:
    logger = logging.getLogger("test.workflow.cache")
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
        retrieval_cache_ttl_seconds=180,
        answer_cache_ttl_seconds=240,
    )


def build_workflow_with_policy(depth: int) -> MultiAgentChatWorkflow:
    logger = logging.getLogger("test.workflow.cache")
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
        retrieval_cache_ttl_seconds=180,
        answer_cache_ttl_seconds=240,
        retrieval_depth_policy=FixedDepthPolicy(depth),
    )


def make_input() -> ChatWorkflowInput:
    return ChatWorkflowInput(
        question="What caused the Apollo 13 emergency?",
        chroma_dir="./chroma_db_openai",
        collection_name="nasa_space_missions_text",
        n_results=3,
        mission_filter="apollo13",
        model="gpt-3.5-turbo",
        evaluate=False,
        judge_mode="off",
        conversation_history=[],
        client_ip="127.0.0.1",
    )


def make_input_with_question(question: str) -> ChatWorkflowInput:
    payload = make_input()
    payload.question = question
    return payload


class TestWorkflowCaching(unittest.TestCase):
    def test_dynamic_retrieval_depth_thresholds_are_configurable(self):
        policy = HeuristicRetrievalDepthPolicy(
            HeuristicRetrievalDepthConfig(factoid_n_results=1, broad_n_results=6)
        )
        workflow = build_workflow()
        workflow._retrieval_depth_policy = policy

        factoid_input = make_input_with_question("When did Apollo 11 launch?")
        broad_input = make_input_with_question("Summarize Apollo 13 mission timeline")

        self.assertEqual(workflow._effective_retrieval_depth(factoid_input), 1)
        self.assertEqual(workflow._effective_retrieval_depth(broad_input), 6)

    def test_dynamic_retrieval_depth_policy_is_applied_to_retrieval_input(self):
        workflow = build_workflow_with_policy(depth=5)

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
        workflow.safety_worker.postflight = MagicMock(side_effect=lambda answer, contexts, client_ip: answer)
        workflow.analysis_worker.evaluate = MagicMock(return_value={})

        workflow.run(make_input(), openai_key="fake-key")

        retrieval_input = workflow.retrieval_worker.run.call_args.args[0]
        self.assertEqual(retrieval_input.n_results, 5)

    def test_dynamic_retrieval_depth_uses_factoid_depth(self):
        workflow = build_workflow()
        workflow_input = make_input_with_question("When did Apollo 11 launch?")

        self.assertEqual(workflow._effective_retrieval_depth(workflow_input), 2)

    def test_dynamic_retrieval_depth_uses_broad_depth(self):
        workflow = build_workflow()
        workflow_input = make_input_with_question("Summarize the Apollo 13 mission timeline and lessons learned")

        self.assertEqual(workflow._effective_retrieval_depth(workflow_input), 4)

    def test_retrieval_and_answer_cache_hit_for_identical_requests(self):
        workflow = build_workflow()

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
        workflow.analysis_worker.generate_answer = MagicMock(
            return_value="Apollo 13 emergency was triggered by an oxygen tank explosion."
        )
        workflow.safety_worker.postflight = MagicMock(
            side_effect=lambda answer, contexts, client_ip: answer
        )
        workflow.analysis_worker.evaluate = MagicMock(return_value={})

        first = workflow.run(make_input(), openai_key="fake-key")
        second = workflow.run(make_input(), openai_key="fake-key")

        self.assertFalse(first.blocked)
        self.assertFalse(second.blocked)
        self.assertEqual(first.answer, second.answer)

        self.assertEqual(workflow.retrieval_worker.run.call_count, 1)
        # Mission-filtered requests bypass answer-cache short-circuit to keep
        # grounded-evidence checks active, so generation still runs each time.
        self.assertEqual(workflow.analysis_worker.generate_answer.call_count, 2)
        self.assertEqual(workflow.safety_worker.postflight.call_count, 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
