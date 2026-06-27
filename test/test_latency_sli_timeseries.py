#!/usr/bin/env python3
"""Tests for NDJSON-backed latency SLI event logging and aggregation."""

from __future__ import annotations

import logging
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock

from monitoring.stage_sli_events import StageLatencyEventStore
from monitoring.worker_pool_events import WorkerPoolEventStore
from multi_agent.models import ChatWorkflowInput, RetrievalResult, SafetyPreflightResult
from multi_agent.workflow import MultiAgentChatWorkflow


class DummyViolation(Exception):
    pass


def make_input(evaluate: bool = False) -> ChatWorkflowInput:
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


def build_workflow(log_path: Path, generation_timeout_seconds: float = 0.05) -> MultiAgentChatWorkflow:
    logger = logging.getLogger("test.latency.timeseries")
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
        retrieval_timeout_seconds=0.05,
        generation_timeout_seconds=generation_timeout_seconds,
        evaluation_timeout_seconds=0.05,
        evaluation_mode="off",
        stage_event_store=StageLatencyEventStore(log_file=log_path),
    )


class TestLatencySLITimeseries(unittest.TestCase):
    def test_timeseries_aggregates_logged_stage_events(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = Path(tmpdir) / "stage_latency_events.jsonl"
            workflow = build_workflow(log_path)

            workflow.retrieval_worker.run = MagicMock(
                return_value=RetrievalResult(
                    contexts=["context"],
                    metadatas=[{"mission": "apollo13"}],
                    context_text="context",
                )
            )
            workflow.safety_worker.preflight = MagicMock(
                return_value=SafetyPreflightResult(blocked_response=None)
            )
            workflow.analysis_worker.generate_answer = MagicMock(return_value="answer")
            workflow.safety_worker.postflight = MagicMock(
                side_effect=lambda answer, contexts, client_ip: answer
            )

            workflow.run(make_input(evaluate=False), openai_key="fake-key")

            report = workflow.get_latency_sli_timeseries(stage="retrieval", window_minutes=60, bucket_seconds=300)

            self.assertEqual(report["stage"], "retrieval")
            self.assertEqual(report["bucket_seconds"], 300)
            self.assertGreaterEqual(len(report["series"]), 1)
            first_bucket = report["series"][-1]
            self.assertIn("bucket_start_ms", first_bucket)
            self.assertIn("p50_ms", first_bucket)
            self.assertIn("p95_ms", first_bucket)
            self.assertIn("timeout_rate", first_bucket)
            self.assertGreaterEqual(first_bucket["total_requests"], 1)

    def test_timeseries_captures_generation_timeout_rate(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = Path(tmpdir) / "stage_latency_events.jsonl"
            workflow = build_workflow(log_path, generation_timeout_seconds=0.05)

            workflow.retrieval_worker.run = MagicMock(
                return_value=RetrievalResult(
                    contexts=["context"],
                    metadatas=[{"mission": "apollo13"}],
                    context_text="context",
                )
            )
            workflow.safety_worker.preflight = MagicMock(
                return_value=SafetyPreflightResult(blocked_response=None)
            )

            def _slow_generation(_openai_key, _workflow_input, _context_text):
                time.sleep(0.7)
                return "answer"

            workflow.analysis_worker.generate_answer = MagicMock(side_effect=_slow_generation)
            workflow.safety_worker.postflight = MagicMock(
                side_effect=lambda answer, contexts, client_ip: answer
            )

            workflow.run(make_input(evaluate=False), openai_key="fake-key")

            report = workflow.get_latency_sli_timeseries(stage="generation", window_minutes=60, bucket_seconds=300)

            self.assertGreaterEqual(len(report["series"]), 1)
            latest_bucket = report["series"][-1]
            self.assertGreaterEqual(latest_bucket["timeouts"], 1)
            self.assertGreater(latest_bucket["timeout_rate"], 0.0)

    def test_timeseries_supports_mission_backend_model_filters(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = Path(tmpdir) / "stage_latency_events.jsonl"
            store = StageLatencyEventStore(log_file=log_path)

            store.record(
                stage="retrieval",
                latency_ms=120.0,
                timed_out=False,
                budget_ms=700.0,
                status="ok",
                mission="apollo13",
                backend="./chroma_db_openai:nasa_space_missions_text",
                model="gpt-3.5-turbo",
            )
            store.record(
                stage="retrieval",
                latency_ms=140.0,
                timed_out=False,
                budget_ms=700.0,
                status="ok",
                mission="apollo11",
                backend="./chroma_db_openai:nasa_space_missions_text",
                model="gpt-3.5-turbo",
            )

            filtered = store.get_timeseries(
                stage="retrieval",
                window_minutes=60,
                bucket_seconds=300,
                mission="apollo13",
                backend="./chroma_db_openai:nasa_space_missions_text",
                model="gpt-3.5-turbo",
            )

            self.assertEqual(filtered["filters"]["mission"], "apollo13")
            self.assertGreaterEqual(len(filtered["series"]), 1)
            latest_bucket = filtered["series"][-1]
            self.assertEqual(latest_bucket["total_requests"], 1)

    def test_retention_prunes_expired_lines_during_maintenance(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = Path(tmpdir) / "stage_latency_events.jsonl"
            old_timestamp_ms = round(time.time() * 1000) - (2 * 3600 * 1000)
            with log_path.open("w", encoding="utf-8") as handle:
                handle.write(
                    "{\"timestamp_ms\":%d,\"stage\":\"retrieval\",\"latency_ms\":111.0,\"timed_out\":false,\"status\":\"ok\",\"budget_ms\":700.0,\"within_budget\":true}\n"
                    % old_timestamp_ms
                )

            store = StageLatencyEventStore(
                log_file=log_path,
                retention_hours=1.0,
                maintenance_interval_seconds=1.0,
            )

            store.record(
                stage="retrieval",
                latency_ms=222.0,
                timed_out=False,
                budget_ms=700.0,
                status="ok",
                mission="apollo13",
                backend="./chroma_db_openai:nasa_space_missions_text",
                model="gpt-3.5-turbo",
            )

            report = store.get_timeseries(stage="retrieval", window_minutes=180, bucket_seconds=300)
            latest_bucket = report["series"][-1]
            self.assertEqual(latest_bucket["total_requests"], 1)

    def test_shutdown_clears_in_memory_buffers(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = Path(tmpdir) / "stage_latency_events.jsonl"
            store = StageLatencyEventStore(log_file=log_path)

            store.record(
                stage="retrieval",
                latency_ms=120.0,
                timed_out=False,
                budget_ms=700.0,
                status="ok",
                mission="apollo13",
                backend="backend-a",
                model="model-a",
            )

            self.assertGreaterEqual(len(store.get_timeseries(stage="retrieval")["series"]), 1)

            store.shutdown()

            cleared = store.get_timeseries(stage="retrieval", window_minutes=60, bucket_seconds=300)
            self.assertEqual(cleared["series"], [])


class TestWorkerPoolEventStore(unittest.TestCase):
    def test_timeseries_and_shutdown(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = Path(tmpdir) / "worker_pool_events.jsonl"
            store = WorkerPoolEventStore(log_file=log_path)

            report = {
                "generated_at_ms": round(time.time() * 1000),
                "workers": {
                    "retrieval": {
                        "max_workers": 4,
                        "queue_limit": 8,
                        "capacity": 4,
                        "inflight": 2,
                        "queued_estimate": 1,
                        "submitted": 10,
                        "completed": 8,
                        "rejected": 0,
                        "failed": 0,
                        "oldest_queue_age_seconds": 0.5,
                        "rejected_rate": 0.0,
                        "error_rate": 0.0,
                    }
                },
            }

            store.record_snapshot(report)
            timeseries = store.get_timeseries(stage="retrieval", window_minutes=60, bucket_seconds=300)

            self.assertEqual(timeseries["stage"], "retrieval")
            self.assertGreaterEqual(len(timeseries["series"]), 1)

            store.shutdown()
            cleared = store.get_timeseries(stage="retrieval", window_minutes=60, bucket_seconds=300)
            self.assertEqual(cleared["series"], [])


if __name__ == "__main__":
    unittest.main(verbosity=2)