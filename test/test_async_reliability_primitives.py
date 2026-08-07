#!/usr/bin/env python3
"""Unit tests for async reliability primitives.

These tests exercise the concrete Redis-facing retry and lock helpers added for
worker resilience. They use mock Redis clients only; no live Redis instance is
required.
"""

from __future__ import annotations

import json
import unittest
from unittest.mock import MagicMock, patch

from infra.redis_job_store import RedisAsyncJobStore
from infra.redis_judge_broker import RedisJudgeBroker


def _redis_client() -> MagicMock:
    client = MagicMock()
    client.is_available.return_value = True
    client._client = MagicMock()
    client.eval = MagicMock(return_value=1)
    return client


class TestRedisJudgeBrokerRetryPrimitives(unittest.TestCase):
    def test_schedule_retry_writes_delayed_member_with_ready_timestamp(self):
        redis_client = _redis_client()
        broker = RedisJudgeBroker(redis_client, enabled=True)
        broker._group_initialized = True

        payload = {"job_id": "job-1", "question": "Q", "_attempt": 1}

        with patch("infra.redis_judge_broker.time.time", return_value=100.0):
            scheduled = broker.schedule_retry("job-1", payload, 2.5)

        self.assertTrue(scheduled)
        redis_client._client.zadd.assert_called_once()
        delayed_set, mapping = redis_client._client.zadd.call_args.args
        self.assertEqual(delayed_set, broker.delayed_set)
        self.assertEqual(len(mapping), 1)

        member, ready_at_ms = next(iter(mapping.items()))
        decoded = json.loads(member)
        self.assertEqual(decoded["job_id"], "job-1")
        self.assertEqual(json.loads(decoded["payload"])["_attempt"], 1)
        self.assertTrue(decoded["nonce"])
        self.assertEqual(ready_at_ms, 102500)

    def test_promote_due_calls_atomic_eval_with_due_cutoff_and_count(self):
        redis_client = _redis_client()
        redis_client.eval.return_value = 3
        broker = RedisJudgeBroker(redis_client, enabled=True)
        broker._group_initialized = True

        with patch("infra.redis_judge_broker.time.time", return_value=250.0):
            promoted = broker.promote_due(max_count=7)

        self.assertEqual(promoted, 3)
        redis_client.eval.assert_called_once()
        args = redis_client.eval.call_args.args
        self.assertEqual(args[1:3], (2, broker.delayed_set))
        self.assertEqual(args[3], broker.stream_name)
        self.assertEqual(args[4], "250000")
        self.assertEqual(args[5], "7")


class TestRedisAsyncJobStoreRenewProcessing(unittest.TestCase):
    def test_renew_processing_extends_lock_only_for_matching_token(self):
        redis_client = _redis_client()
        store = RedisAsyncJobStore(redis_client)

        renewed = store.renew_processing("job-1", "token-1", 600)

        self.assertTrue(renewed)
        redis_client.eval.assert_called_once()
        args = redis_client.eval.call_args.args
        self.assertIn("EXPIRE", args[0])
        self.assertEqual(args[1], 1)
        self.assertEqual(args[2], "job:processing:job-1")
        self.assertEqual(args[3], "token-1")
        self.assertEqual(args[4], "600")

    def test_renew_processing_returns_false_when_ownership_is_lost(self):
        redis_client = _redis_client()
        redis_client.eval.return_value = 0
        store = RedisAsyncJobStore(redis_client)

        renewed = store.renew_processing("job-2", "stale-token", 300)

        self.assertFalse(renewed)
        redis_client.eval.assert_called_once()
