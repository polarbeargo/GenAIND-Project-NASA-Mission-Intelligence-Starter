#!/usr/bin/env python3
"""Tests for the Redis-backed HTTP rate limiter."""

from __future__ import annotations

import os
import unittest
import uuid
from contextlib import contextmanager
from typing import Any, Dict
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

import api_server
from infra.redis_client import RedisClient


class _NoopSpan:
    def set_attribute(self, _key: str, _value: Any) -> None:
        return None

    def get_span_context(self):
        return None


@contextmanager
def _noop_span_context(_name: str):
    yield _NoopSpan()


class _FakeRateLimiter:
    def __init__(self, result: Dict[str, Any] | None):
        self._result = result

    def should_limit_path(self, path: str) -> bool:
        return path == "/chat"

    def check(self, client_ip: str, path: str):
        return self._result


class TestRateLimitMiddleware(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(api_server.app)
        self.base_payload: Dict[str, Any] = {
            "question": "What was the cause of the Apollo 13 emergency?",
            "chroma_dir": "./chroma_db",
            "collection_name": "nasa_space_missions_test",
            "n_results": 3,
            "evaluate": False,
        }

    def _post_chat(self, rate_limit_result: Dict[str, Any] | None):
        with (
            patch.object(api_server, "rate_limiter", _FakeRateLimiter(rate_limit_result)),
            patch("api_server.get_openai_api_key", return_value="test-key"),
            patch("api_server.chat_workflow.run", return_value=api_server.ChatResponse(
                answer="Apollo 13 emergency was caused by an oxygen tank explosion.",
                contexts=["Apollo 13 oxygen tank explosion in service module."],
                evaluation={},
                judge={"passed": True, "low_confidence": False, "overall_score": 0.9, "source": "llm", "rationale": "ok"},
                latency_ms=12.3,
                backend="./chroma_db:nasa_space_missions_test",
                session_id="test-session",
            )),
            patch("api_server.monitor.log_interaction", return_value=None),
            patch("api_server.tracer.start_as_current_span", side_effect=_noop_span_context),
        ):
            return self.client.post("/chat", json=self.base_payload)

    def test_rate_limiter_returns_429_and_headers(self):
        response = self._post_chat(
            {
                "allowed": False,
                "limit": 5,
                "current": 5,
                "remaining": 0,
                "retry_after_seconds": 12,
                "window_seconds": 60,
                "key": "rate_limit:chat:127.0.0.1",
            }
        )

        self.assertEqual(response.status_code, 429)
        self.assertEqual(response.json()["detail"], "Rate limit exceeded")
        self.assertEqual(response.headers.get("Retry-After"), "12")
        self.assertEqual(response.headers.get("X-RateLimit-Limit"), "5")
        self.assertEqual(response.headers.get("X-RateLimit-Remaining"), "0")

    def test_rate_limiter_sets_headers_on_allowed_request(self):
        response = self._post_chat(
            {
                "allowed": True,
                "limit": 5,
                "current": 1,
                "remaining": 4,
                "retry_after_seconds": 60,
                "window_seconds": 60,
                "key": "rate_limit:chat:127.0.0.1",
            }
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers.get("X-RateLimit-Limit"), "5")
        self.assertEqual(response.headers.get("X-RateLimit-Remaining"), "4")
        self.assertEqual(response.headers.get("Retry-After"), "60")


class TestRateLimiterPathConfig(unittest.TestCase):
    def test_default_rate_limit_paths_include_expensive_and_mutating_routes(self):
        original = os.environ.get("RATE_LIMIT_PATHS")
        try:
            os.environ.pop("RATE_LIMIT_PATHS", None)
            paths = api_server._get_rate_limit_paths()
        finally:
            if original is None:
                os.environ.pop("RATE_LIMIT_PATHS", None)
            else:
                os.environ["RATE_LIMIT_PATHS"] = original

        self.assertIn("/chat", paths)
        self.assertIn("/collections/clear-cache", paths)
        self.assertIn("/collections/warm-cache", paths)
        self.assertIn("/monitoring/report", paths)
        self.assertIn("/monitoring/rag/report", paths)

    def test_should_limit_path_supports_exact_and_prefix_patterns(self):
        limiter = api_server.RedisSlidingWindowRateLimiter(
            requests_per_period=10,
            period_seconds=60,
            paths=["/chat", "/monitoring/report*"],
            enabled=True,
        )

        self.assertTrue(limiter.should_limit_path("/chat"))
        self.assertTrue(limiter.should_limit_path("/chat/"))
        self.assertTrue(limiter.should_limit_path("/monitoring/report"))
        self.assertTrue(limiter.should_limit_path("/monitoring/report/daily"))
        self.assertFalse(limiter.should_limit_path("/monitoring/analytics"))


@pytest.mark.redis
class TestRedisSlidingWindowRateLimiter(unittest.TestCase):
    def setUp(self):
        self.redis = RedisClient(enabled=True)

    def test_sliding_window_denies_repeated_requests(self):
        client_ip = f"127.0.0.1-{uuid.uuid4().hex[:8]}"
        limiter = api_server.RedisSlidingWindowRateLimiter(
            requests_per_period=1,
            period_seconds=10,
            paths=["/chat"],
            enabled=True,
        )

        with patch.object(api_server, "get_redis_client", return_value=self.redis):
            first = limiter.check(client_ip, "/chat")
            second = limiter.check(client_ip, "/chat")

        key = f"rate_limit:chat:{client_ip}"
        self.redis.delete(key)

        self.assertIsNotNone(first)
        self.assertTrue(first["allowed"])
        self.assertEqual(first["remaining"], 0)
        self.assertIsNotNone(second)
        self.assertFalse(second["allowed"])
        self.assertGreaterEqual(second["retry_after_seconds"], 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)