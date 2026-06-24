#!/usr/bin/env python3
"""Unit tests for EvidentlyMonitor sink behavior and analytics contract."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from evidently_monitor import EvidentlyMonitor


class TestEvidentlyMonitor(unittest.TestCase):
    def test_file_sink_round_trip_and_curated_snapshot(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            log_path = Path(temp_dir) / "interactions.jsonl"
            monitor = EvidentlyMonitor(log_path=str(log_path), sink_type="file", mirror_sink_types=[])

            monitor.log_interaction(
                question="What happened on Apollo 13?",
                answer="An oxygen tank exploded.",
                model="gpt-4o-mini",
                backend="./chroma_db_openai:nasa_space_missions_text",
                context_count=3,
                mission="apollo_13",
                evaluation={
                    "faithfulness": 0.8,
                    "response_relevancy": 0.75,
                    "context_precision": 0.7,
                },
                error=False,
                latency_ms=123.4,
            )
            monitor.log_interaction(
                question="Did retrieval fail?",
                answer="Yes",
                model="gpt-4o-mini",
                backend="./chroma_db_openai:nasa_space_missions_text",
                context_count=0,
                mission="apollo_13",
                evaluation={"faithfulness": 0.2},
                error=True,
                latency_ms=321.0,
            )
            monitor.shutdown()

            dataset = monitor.load_dataframe()
            self.assertEqual(len(dataset), 2)

            analytics = monitor.get_analytics_summary()
            self.assertEqual(analytics["status"], "ok")
            self.assertEqual(analytics["overall"]["total_requests"], 2)
            self.assertEqual(analytics["overall"]["total_errors"], 1)

            rag_summary = monitor.get_rag_dashboard_summary()
            self.assertEqual(rag_summary["status"], "ok")
            self.assertEqual(rag_summary["overall"]["scored_requests"], 2)

            snapshot = monitor.get_prometheus_curated_snapshot()
            self.assertEqual(snapshot["sink_type"], "file")
            self.assertEqual(snapshot["requests_total"], 2.0)
            self.assertEqual(snapshot["errors_total"], 1.0)
            self.assertIn("sink_target", snapshot)
            self.assertIn("mirror_write_failures_total", snapshot)

    def test_async_evaluation_updates_merge_without_double_counting(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            log_path = Path(temp_dir) / "interactions.jsonl"
            monitor = EvidentlyMonitor(log_path=str(log_path), sink_type="file", mirror_sink_types=[])

            monitor.log_interaction(
                question="Why did Apollo 13 abort the landing?",
                answer="An oxygen tank exploded and forced mission abort.",
                model="gpt-4o-mini",
                backend="./chroma_db_openai:nasa_space_missions_text",
                context_count=3,
                mission="apollo_13",
                evaluation={},
                error=False,
                latency_ms=111.0,
                interaction_id="req-1",
            )
            monitor.log_interaction(
                question="Why did Apollo 13 abort the landing?",
                answer="An oxygen tank exploded and forced mission abort.",
                model="gpt-4o-mini",
                backend="./chroma_db_openai:nasa_space_missions_text",
                context_count=3,
                mission="apollo_13",
                evaluation={
                    "faithfulness": 0.81,
                    "response_relevancy": 0.77,
                    "context_precision": 0.74,
                },
                error=False,
                interaction_id="req-1",
                record_kind="evaluation_update",
            )
            monitor.shutdown()

            analytics = monitor.get_analytics_summary()
            self.assertEqual(analytics["status"], "ok")
            self.assertEqual(analytics["overall"]["total_requests"], 1)

            rag_summary = monitor.get_rag_dashboard_summary()
            self.assertEqual(rag_summary["status"], "ok")
            self.assertEqual(rag_summary["overall"]["scored_requests"], 1)
            self.assertAlmostEqual(rag_summary["overall"]["avg_faithfulness"], 0.81)

            snapshot = monitor.get_prometheus_curated_snapshot()
            self.assertEqual(snapshot["requests_total"], 1.0)
            self.assertEqual(snapshot["rag_scored_requests"], 1.0)
            self.assertAlmostEqual(snapshot["rag_avg_faithfulness"], 0.81)


if __name__ == "__main__":
    unittest.main()