#!/usr/bin/env python3
"""Unit tests for context compression: dedup, mission priority, and token cap."""

from __future__ import annotations

import unittest

from multi_agent.context_compression import (
    CompressionConfig,
    DeduplicatingCompressor,
    _jaccard,
)


class TestJaccardSimilarity(unittest.TestCase):
    def test_identical_sets_score_one(self):
        a = frozenset(["oxygen", "tank", "explosion"])
        self.assertAlmostEqual(_jaccard(a, a), 1.0)

    def test_disjoint_sets_score_zero(self):
        a = frozenset(["apollo"])
        b = frozenset(["challenger"])
        self.assertAlmostEqual(_jaccard(a, b), 0.0)

    def test_partial_overlap(self):
        a = frozenset(["the", "oxygen", "tank"])
        b = frozenset(["the", "oxygen", "explosion"])
        # intersection=2, union=4 → 0.5
        self.assertAlmostEqual(_jaccard(a, b), 0.5)

    def test_empty_sets_score_one(self):
        self.assertAlmostEqual(_jaccard(frozenset(), frozenset()), 1.0)


class TestDeduplicatingCompressor(unittest.TestCase):
    def _compressor(self, **kwargs) -> DeduplicatingCompressor:
        return DeduplicatingCompressor(CompressionConfig(**kwargs))

    # ------------------------------------------------------------------
    # Deduplication
    # ------------------------------------------------------------------

    def test_exact_duplicate_is_removed(self):
        chunk = "The oxygen tank exploded during the Apollo 13 mission."
        ctx = [chunk, chunk]
        metas = [{"mission": "apollo13"}, {"mission": "apollo13"}]
        compressor = self._compressor(similarity_threshold=0.85)
        out_c, out_m = compressor.compress(ctx, metas, mission_filter=None)
        self.assertEqual(len(out_c), 1)
        self.assertEqual(out_c[0], chunk)

    def test_near_duplicate_above_threshold_is_removed(self):
        base = "oxygen tank explosion apollo thirteen"
        near = "oxygen tank explosion apollo thirteen extra"
        # Jaccard ~ 5/6 ≈ 0.833; at threshold=0.80 the near-dup is dropped.
        compressor = self._compressor(similarity_threshold=0.80)
        out_c, _ = compressor.compress([base, near], [{}, {}], mission_filter=None)
        self.assertEqual(len(out_c), 1)

    def test_distinct_chunks_are_both_kept(self):
        c1 = "The oxygen tank on Apollo 13 exploded."
        c2 = "The Challenger shuttle broke apart after launch."
        compressor = self._compressor(similarity_threshold=0.85)
        out_c, _ = compressor.compress([c1, c2], [{}, {}], mission_filter=None)
        self.assertEqual(len(out_c), 2)

    def test_empty_chunks_are_skipped(self):
        ctx = ["", "   ", "Real content about Apollo 11."]
        compressor = self._compressor()
        out_c, _ = compressor.compress(ctx, [{}, {}, {}], mission_filter=None)
        self.assertEqual(len(out_c), 1)
        self.assertIn("Apollo 11", out_c[0])

    # ------------------------------------------------------------------
    # Mission priority ordering
    # ------------------------------------------------------------------

    def test_mission_matching_chunk_is_sorted_first(self):
        c1 = "The Challenger disaster occurred in 1986."
        c2 = "Apollo 13 had an oxygen tank failure."
        metas = [{"mission": "challenger"}, {"mission": "apollo13"}]
        compressor = self._compressor(mission_boost=True)
        out_c, out_m = compressor.compress([c1, c2], metas, mission_filter="apollo13")
        self.assertIn("Apollo 13", out_c[0])
        self.assertEqual(out_m[0]["mission"], "apollo13")

    def test_mission_boost_disabled_preserves_original_order(self):
        c1 = "The Challenger disaster occurred in 1986."
        c2 = "Apollo 13 had an oxygen tank failure."
        metas = [{"mission": "challenger"}, {"mission": "apollo13"}]
        compressor = self._compressor(mission_boost=False)
        out_c, _ = compressor.compress([c1, c2], metas, mission_filter="apollo13")
        self.assertIn("Challenger", out_c[0])

    def test_no_mission_filter_skips_sort(self):
        c1 = "First chunk."
        c2 = "Second chunk."
        metas = [{"mission": "apollo11"}, {"mission": "apollo13"}]
        compressor = self._compressor(mission_boost=True)
        out_c, _ = compressor.compress([c1, c2], metas, mission_filter=None)
        self.assertEqual(out_c[0], c1)

    # ------------------------------------------------------------------
    # Token cap
    # ------------------------------------------------------------------

    def test_token_cap_drops_chunks_over_budget(self):
        # max_tokens=5 → max_chars=20
        c1 = "A" * 10   # 10 chars, fits
        c2 = "B" * 15   # 15 chars, would exceed 20 → dropped
        compressor = self._compressor(max_tokens=5)
        out_c, _ = compressor.compress([c1, c2], [{}, {}], mission_filter=None)
        self.assertEqual(len(out_c), 1)
        self.assertEqual(out_c[0], c1)

    def test_first_chunk_always_kept_even_if_over_budget(self):
        # Even if the very first chunk exceeds the budget, it must be kept.
        huge_chunk = "word " * 5000  # ~25000 chars > max_chars=100
        compressor = self._compressor(max_tokens=25)
        out_c, _ = compressor.compress([huge_chunk], [{}], mission_filter=None)
        self.assertEqual(len(out_c), 1)

    # ------------------------------------------------------------------
    # Empty / missing inputs
    # ------------------------------------------------------------------

    def test_empty_context_list_returns_empty(self):
        compressor = self._compressor()
        out_c, out_m = compressor.compress([], [], mission_filter="apollo11")
        self.assertEqual(out_c, [])
        self.assertEqual(out_m, [])

    def test_missing_metadatas_defaults_to_empty_dicts(self):
        ctx = ["Chunk one.", "Chunk two."]
        compressor = self._compressor()
        # Pass fewer metadatas than contexts
        out_c, out_m = compressor.compress(ctx, [], mission_filter=None)
        self.assertEqual(len(out_c), 2)
        self.assertEqual(len(out_m), 2)
        self.assertEqual(out_m[0], {})


class TestCompressionConfigDefaults(unittest.TestCase):
    def test_default_max_tokens(self):
        self.assertEqual(CompressionConfig().max_tokens, 2000)

    def test_default_similarity_threshold(self):
        self.assertAlmostEqual(CompressionConfig().similarity_threshold, 0.85)

    def test_default_mission_boost_enabled(self):
        self.assertTrue(CompressionConfig().mission_boost)


class TestCompressionIntegrationWithWorkflow(unittest.TestCase):
    """Verify the compressor is wired into MultiAgentChatWorkflow and applied."""

    def test_workflow_compressor_default_is_deduplicating(self):
        import logging

        from multi_agent.context_compression import DeduplicatingCompressor
        from multi_agent.workflow import MultiAgentChatWorkflow

        class DummyViolation(Exception):
            pass

        logger = logging.getLogger("test.compression.workflow")
        logger.setLevel(logging.CRITICAL)
        workflow = MultiAgentChatWorkflow(
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
        self.assertIsInstance(workflow._context_compressor, DeduplicatingCompressor)

    def test_workflow_accepts_custom_compressor(self):
        import logging

        from multi_agent.workflow import MultiAgentChatWorkflow

        class DummyViolation(Exception):
            pass

        class PassthroughCompressor:
            def compress(self, contexts, metadatas, mission_filter):
                return contexts, metadatas

        logger = logging.getLogger("test.compression.workflow")
        logger.setLevel(logging.CRITICAL)
        compressor = PassthroughCompressor()
        workflow = MultiAgentChatWorkflow(
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
            context_compressor=compressor,
        )
        self.assertIs(workflow._context_compressor, compressor)

    def test_compression_is_applied_before_generation(self):
        """Duplicate contexts are deduplicated before generate_answer is called."""
        import logging
        from unittest.mock import MagicMock

        from multi_agent.models import ChatWorkflowInput, RetrievalResult, SafetyPreflightResult
        from multi_agent.workflow import MultiAgentChatWorkflow

        class DummyViolation(Exception):
            pass

        logger = logging.getLogger("test.compression.workflow")
        logger.setLevel(logging.CRITICAL)

        duplicate_chunk = "The oxygen tank on Apollo 13 exploded mid-flight."
        workflow = MultiAgentChatWorkflow(
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

        workflow.retrieval_worker.run = MagicMock(
            return_value=RetrievalResult(
                contexts=[duplicate_chunk, duplicate_chunk],
                metadatas=[{"mission": "apollo13"}, {"mission": "apollo13"}],
                context_text=duplicate_chunk + "\n\n" + duplicate_chunk,
            )
        )
        workflow.safety_worker.preflight = MagicMock(
            return_value=SafetyPreflightResult(blocked_response=None)
        )
        workflow.analysis_worker.generate_answer = MagicMock(return_value="answer")
        workflow.safety_worker.postflight = MagicMock(
            side_effect=lambda answer, contexts, client_ip: answer
        )
        workflow.analysis_worker.evaluate = MagicMock(return_value={})

        workflow_input = ChatWorkflowInput(
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

        result = workflow.run(workflow_input, openai_key="fake-key")

        # Final contexts in result should be deduplicated to 1 chunk.
        self.assertEqual(len(result.contexts), 1)
        self.assertEqual(result.contexts[0], duplicate_chunk)

        # context_text passed to generate_answer should NOT contain the duplicate.
        call_kwargs = workflow.analysis_worker.generate_answer.call_args
        context_text_used = call_kwargs.kwargs.get(
            "context_text", call_kwargs.args[2] if len(call_kwargs.args) > 2 else ""
        )
        self.assertEqual(context_text_used.count(duplicate_chunk), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
