#!/usr/bin/env python3
"""HTTP-level contract tests for /chat judge schema stability."""

from __future__ import annotations

import unittest
from contextlib import contextmanager
from typing import Any, Dict, List
from unittest.mock import patch

from fastapi.testclient import TestClient

import api_server
from multi_agent.models import ChatWorkflowResult, WorkflowError


class _NoopSpan:
    def set_attribute(self, _key: str, _value: Any) -> None:
        return None


@contextmanager
def _noop_span_context(_name: str):
    yield _NoopSpan()


class TestChatContractAPI(unittest.TestCase):
    REQUIRED_CHAT_KEYS = {"answer", "contexts", "evaluation", "judge", "latency_ms", "backend"}
    REQUIRED_JUDGE_KEYS = {"passed", "low_confidence", "overall_score", "source", "rationale"}

    def setUp(self):
        self.client = TestClient(api_server.app)
        self.base_payload: Dict[str, Any] = {
            "question": "What was the cause of the Apollo 13 emergency?",
            "chroma_dir": "./chroma_db",
            "collection_name": "nasa_space_missions_test",
            "n_results": 3,
            "evaluate": False,
        }

    def _result_for_mode(self, mode: str) -> ChatWorkflowResult:
        if mode == "sync":
            judge = {
                "groundedness_score": 0.84,
                "safety_score": 0.93,
                "task_success_score": 0.81,
                "overall_score": 0.87,
                "confidence": 0.9,
                "passed": True,
                "low_confidence": False,
                "source": "llm",
                "rationale": "Answer is grounded and safe.",
            }
        elif mode == "async":
            judge = {
                "status": "pending",
                "groundedness_score": None,
                "safety_score": None,
                "task_success_score": None,
                "overall_score": None,
                "confidence": None,
                "passed": True,
                "low_confidence": True,
                "source": "async",
                "rationale": "Judge running asynchronously.",
            }
        else:
            judge = {
                "groundedness_score": 0.0,
                "safety_score": 0.0,
                "task_success_score": 0.0,
                "overall_score": 0.0,
                "confidence": 0.0,
                "passed": True,
                "low_confidence": True,
                "source": "disabled",
                "rationale": "Judge skipped by configuration.",
            }

        return ChatWorkflowResult(
            answer="Apollo 13 emergency was caused by an oxygen tank explosion.",
            contexts=["Apollo 13 oxygen tank explosion in service module."],
            evaluation={},
            judge=judge,
            blocked=False,
        )

    def _post_chat_with_mode(self, mode: str):
        payload = dict(self.base_payload)
        payload["judge_mode"] = mode

        with (
            patch("api_server.get_openai_api_key", return_value="test-key"),
            patch("api_server.chat_workflow.run", return_value=self._result_for_mode(mode)),
            patch("api_server.monitor.log_interaction", return_value=None),
            patch("api_server.tracer.start_as_current_span", side_effect=_noop_span_context),
        ):
            return self.client.post("/chat", json=payload)

    def test_chat_contract_schema_stable_across_sync_async_off(self):
        responses = {
            "sync": self._post_chat_with_mode("sync"),
            "async": self._post_chat_with_mode("async"),
            "off": self._post_chat_with_mode("off"),
        }

        for mode, response in responses.items():
            self.assertEqual(response.status_code, 200, f"Mode {mode} should succeed")
            body = response.json()
            self.assertTrue(self.REQUIRED_CHAT_KEYS.issubset(body.keys()), f"Missing top-level keys for {mode}")
            self.assertIsInstance(body["judge"], dict)
            self.assertTrue(
                self.REQUIRED_JUDGE_KEYS.issubset(body["judge"].keys()),
                f"Missing judge contract keys for {mode}",
            )

    def test_chat_defaults_to_async_judge_mode_when_not_provided(self):
        captured_modes: List[str] = []

        def _capture_and_return(workflow_input, openai_key):
            captured_modes.append(workflow_input.judge_mode)
            return self._result_for_mode("async")

        with (
            patch("api_server.get_openai_api_key", return_value="test-key"),
            patch("api_server.chat_workflow.run", side_effect=_capture_and_return),
            patch("api_server.monitor.log_interaction", return_value=None),
            patch("api_server.tracer.start_as_current_span", side_effect=_noop_span_context),
        ):
            response = self.client.post("/chat", json=self.base_payload)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(captured_modes[-1], "async")

    def test_monitoring_client_caches_contract(self):
        response = self.client.get("/monitoring/client-caches")

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertIn("openai_client", body)
        self.assertIn("rag_client", body)
        self.assertIn("ragas_evaluator", body)

        self.assertIn("current_size", body["openai_client"])
        self.assertIn("hits", body["openai_client"])
        self.assertIn("misses", body["openai_client"])

    def test_monitoring_latency_sli_contract(self):
        response = self.client.get("/monitoring/latency-sli")

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertIn("generated_at_ms", body)
        self.assertIn("workers", body)

        workers = body["workers"]
        for name in ["preflight", "retrieval", "generation"]:
            self.assertIn(name, workers)
            self.assertIn("p50_ms", workers[name])
            self.assertIn("p95_ms", workers[name])
            self.assertIn("timeout_rate", workers[name])
            self.assertIn("budget_ms", workers[name])

    def test_monitoring_worker_pools_prometheus_contract(self):
        response = self.client.get("/monitoring/worker-pools/prometheus")

        self.assertEqual(response.status_code, 200)
        self.assertIn("text/plain", response.headers.get("content-type", ""))
        body = response.text
        self.assertIn("# TYPE nasa_worker_pool_inflight gauge", body)
        self.assertIn('nasa_worker_pool_inflight{stage="retrieval"}', body)
        self.assertIn('nasa_worker_pool_queue_depth_ratio{stage="generation"}', body)
        self.assertIn("nasa_worker_pool_generated_at_ms", body)

    def test_monitoring_latency_sli_timeseries_contract(self):
        response = self.client.get("/monitoring/latency-sli/timeseries")

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertIn("generated_at_ms", body)
        self.assertIn("window_minutes", body)
        self.assertIn("bucket_seconds", body)
        self.assertIn("workers", body)

        workers = body["workers"]
        for name in ["preflight", "retrieval", "generation", "evaluation"]:
            self.assertIn(name, workers)
            self.assertIsInstance(workers[name], list)

    def test_monitoring_latency_sli_timeseries_accepts_filters(self):
        response = self.client.get(
            "/monitoring/latency-sli/timeseries",
            params={
                "stage": "retrieval",
                "window_minutes": 60,
                "bucket_seconds": 300,
                "mission": "apollo13",
                "backend": "./chroma_db_openai:nasa_space_missions_text",
                "model": "gpt-3.5-turbo",
            },
        )

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body.get("stage"), "retrieval")
        self.assertIn("filters", body)
        self.assertEqual(body["filters"].get("mission"), "apollo13")

    def test_monitoring_latency_sli_timeseries_invalid_stage_returns_400(self):
        response = self.client.get(
            "/monitoring/latency-sli/timeseries",
            params={"stage": "invalid-stage"},
        )
        self.assertEqual(response.status_code, 400)

    def test_monitoring_security_contract(self):
        response = self.client.get("/monitoring/security")

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertIn("statistics", body)
        self.assertIn("threat_summary", body)

    def test_monitoring_security_events_contract(self):
        response = self.client.get(
            "/monitoring/security/events",
            params={"limit": 10, "severity": "high"},
        )

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertIn("count", body)
        self.assertIn("events", body)

    def test_chat_workflow_error_logs_security_event(self):
        with (
            patch("api_server.get_openai_api_key", return_value="test-key"),
            patch("api_server.chat_workflow.run", side_effect=WorkflowError(status_code=429, detail="Rate limit exceeded")),
            patch("api_server.security_dashboard.log_event", return_value=None) as log_event_mock,
            patch("api_server.tracer.start_as_current_span", side_effect=_noop_span_context),
        ):
            response = self.client.post("/chat", json=self.base_payload)

        self.assertEqual(response.status_code, 429)
        log_event_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main(verbosity=2)
