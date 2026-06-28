#!/usr/bin/env python3
"""Tests for deterministic context precision fallback scoring."""

from __future__ import annotations

import unittest

from ragas_evaluator import _calculate_context_precision_fallback


class TestContextPrecisionFallback(unittest.TestCase):
    def test_returns_one_when_all_contexts_overlap(self):
        score = _calculate_context_precision_fallback(
            question="What caused the Apollo 13 emergency?",
            answer="It was caused by an oxygen tank explosion.",
            contexts=[
                "Apollo 13 emergency happened after an oxygen tank explosion in the service module.",
                "The crew aborted landing and returned safely after the explosion event.",
            ],
        )
        self.assertGreaterEqual(score, 0.99)

    def test_returns_zero_when_contexts_are_irrelevant(self):
        score = _calculate_context_precision_fallback(
            question="What caused the Apollo 13 emergency?",
            answer="It was caused by an oxygen tank explosion.",
            contexts=[
                "Bananas grow in tropical climates and are rich in potassium.",
                "The orchestra performed a modern jazz piece downtown.",
            ],
        )
        self.assertEqual(score, 0.0)

    def test_handles_empty_contexts(self):
        score = _calculate_context_precision_fallback(
            question="What caused the Apollo 13 emergency?",
            answer="It was caused by an oxygen tank explosion.",
            contexts=[],
        )
        self.assertEqual(score, 0.0)


if __name__ == "__main__":
    unittest.main()
