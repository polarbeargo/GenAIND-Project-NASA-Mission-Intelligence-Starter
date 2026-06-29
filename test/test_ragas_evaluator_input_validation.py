#!/usr/bin/env python3
"""Input validation tests for evaluate_response_quality."""

from __future__ import annotations

import time
import unittest

from ragas_evaluator import evaluate_response_quality


class TestRagasEvaluatorInputValidation(unittest.TestCase):
    def test_returns_clear_error_for_none_question(self):
        result = evaluate_response_quality(
            question=None,
            answer="There was an oxygen tank explosion.",
            contexts=["Apollo 13 suffered an oxygen tank explosion."],
        )
        self.assertIn("error", result)
        self.assertIn("Malformed question", str(result["error"]))

    def test_returns_clear_error_for_non_string_question(self):
        result = evaluate_response_quality(
            question=123,
            answer="There was an oxygen tank explosion.",
            contexts=["Apollo 13 suffered an oxygen tank explosion."],
        )
        self.assertIn("error", result)
        self.assertIn("Malformed question", str(result["error"]))

    def test_returns_clear_error_for_empty_question(self):
        result = evaluate_response_quality(
            question="   ",
            answer="There was an oxygen tank explosion.",
            contexts=["Apollo 13 suffered an oxygen tank explosion."],
        )
        self.assertIn("error", result)
        self.assertEqual(result["error"], "Malformed question: expected non-empty string, got empty")

    def test_returns_clear_error_for_none_answer(self):
        result = evaluate_response_quality(
            question="What happened in Apollo 13?",
            answer=None,
            contexts=["Apollo 13 suffered an oxygen tank explosion."],
        )
        self.assertIn("error", result)
        self.assertIn("Malformed answer", str(result["error"]))

    def test_returns_clear_error_for_non_string_answer(self):
        result = evaluate_response_quality(
            question="What happened in Apollo 13?",
            answer=45.6,
            contexts=["Apollo 13 suffered an oxygen tank explosion."],
        )
        self.assertIn("error", result)
        self.assertIn("Malformed answer", str(result["error"]))

    def test_returns_clear_error_for_empty_answer(self):
        result = evaluate_response_quality(
            question="What happened in Apollo 13?",
            answer="  ",
            contexts=["Apollo 13 suffered an oxygen tank explosion."],
        )
        self.assertIn("error", result)
        self.assertEqual(result["error"], "Malformed answer: expected non-empty string, got empty")

    def test_returns_clear_error_for_none_contexts(self):
        result = evaluate_response_quality(
            question="What happened in Apollo 13?",
            answer="There was an oxygen tank explosion.",
            contexts=None,
        )
        self.assertIn("error", result)
        self.assertIn("Malformed contexts", str(result["error"]))

    def test_returns_clear_error_for_string_contexts(self):
        result = evaluate_response_quality(
            question="What happened in Apollo 13?",
            answer="There was an oxygen tank explosion.",
            contexts="single context string",
        )
        self.assertIn("error", result)
        self.assertIn("Malformed contexts", str(result["error"]))

    def test_returns_clear_error_for_empty_contexts(self):
        result = evaluate_response_quality(
            question="What happened in Apollo 13?",
            answer="There was an oxygen tank explosion.",
            contexts=[],
        )
        self.assertIn("error", result)
        self.assertEqual(result["error"], "No contexts available for evaluation")

    def test_malformed_input_guard_overhead_is_low(self):
        cases = [
            {"question": None, "answer": "There was an oxygen tank explosion.", "contexts": ["Apollo 13 suffered an oxygen tank explosion."]},
            {"question": "What happened in Apollo 13?", "answer": 45.6, "contexts": ["Apollo 13 suffered an oxygen tank explosion."]},
            {"question": "What happened in Apollo 13?", "answer": "There was an oxygen tank explosion.", "contexts": "single context string"},
        ]

        warmup_iterations = 100
        measured_iterations = 5000

        for payload in cases:
            for _ in range(warmup_iterations):
                evaluate_response_quality(**payload)

            start = time.perf_counter_ns()
            for _ in range(measured_iterations):
                result = evaluate_response_quality(**payload)
            elapsed_ns = time.perf_counter_ns() - start

            self.assertIn("error", result)
            average_us = elapsed_ns / measured_iterations / 1000.0
            self.assertLess(
                average_us,
                1000.0,
                msg=f"Malformed input guard too slow: {average_us:.2f} us/call",
            )


if __name__ == "__main__":
    unittest.main()
