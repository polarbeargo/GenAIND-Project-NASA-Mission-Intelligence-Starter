#!/usr/bin/env python3
"""Concrete reliability tests for judge_worker control flow."""

from __future__ import annotations

import time
import unittest
from unittest.mock import MagicMock, patch

import judge_worker


class TestProcessingHeartbeat(unittest.TestCase):
    def setUp(self):
        judge_worker._RUNNING = True
        judge_worker._DRAIN_EVENT.clear()

    def tearDown(self):
        judge_worker._RUNNING = True
        judge_worker._DRAIN_EVENT.clear()

    def test_heartbeat_stops_after_lock_renew_failure_and_emits_metric(self):
        job_store = MagicMock()
        job_store.renew_processing.return_value = False
        metrics = MagicMock()

        heartbeat = judge_worker._ProcessingHeartbeat(
            job_store=job_store,
            job_id="job-1",
            token="token-1",
            ttl_seconds=30,
        )
        heartbeat._interval = 0.01

        with patch("judge_worker.get_async_reliability_metrics", return_value=metrics):
            heartbeat.start()
            heartbeat._thread.join(timeout=1.0)

        self.assertFalse(heartbeat._thread.is_alive())
        job_store.renew_processing.assert_called_once_with("job-1", "token-1", 30)
        metrics.record_lock_renew_fail.assert_called_once_with(worker="judge", reason="lost")

    def test_heartbeat_stop_interrupts_wait_and_joins_thread(self):
        job_store = MagicMock()
        job_store.renew_processing.return_value = True

        heartbeat = judge_worker._ProcessingHeartbeat(
            job_store=job_store,
            job_id="job-2",
            token="token-2",
            ttl_seconds=30,
        )
        heartbeat._interval = 1.0

        heartbeat.start()
        started = time.monotonic()
        heartbeat.stop()
        elapsed = time.monotonic() - started

        self.assertFalse(heartbeat._thread.is_alive())
        self.assertLess(elapsed, 0.5)


class TestJudgeWorkerOutageBackoff(unittest.TestCase):
    def setUp(self):
        judge_worker._RUNNING = True
        judge_worker._DRAIN_EVENT.clear()

    def tearDown(self):
        judge_worker._RUNNING = True
        judge_worker._DRAIN_EVENT.clear()

    def test_outage_path_waits_with_capped_backoff_after_fast_empty_poll(self):
        redis_client = MagicMock()
        redis_client.is_available.return_value = True
        broker = MagicMock()
        broker.consume.return_value = []
        broker.reclaim_stale.return_value = []
        broker.is_available.return_value = False
        broker.promote_due.return_value = 0

        wait_calls = []

        def stop_on_wait(timeout):
            wait_calls.append(timeout)
            judge_worker._stop_worker()
            return True

        with patch("judge_worker.signal.signal"), \
             patch("judge_worker.get_redis_client", return_value=redis_client), \
             patch("judge_worker.RedisJudgeBroker", return_value=broker), \
             patch("judge_worker.RedisAsyncJobStore"), \
             patch("judge_worker.JudgeWorker"), \
             patch("judge_worker.os.path.exists", return_value=False), \
             patch("judge_worker.time.monotonic", side_effect=[0.0, 0.1]), \
             patch("judge_worker._DRAIN_EVENT.wait", side_effect=stop_on_wait), \
             patch("openai_config.get_openai_api_key", return_value="fake-key"):
            exit_code = judge_worker.run()

        self.assertEqual(exit_code, 0)
        self.assertEqual(wait_calls, [0.5])
        broker.is_available.assert_called_once_with()

    def test_healthy_idle_poll_skips_outage_ping_and_wait(self):
        redis_client = MagicMock()
        redis_client.is_available.return_value = True
        broker = MagicMock()
        broker.consume.side_effect = [[], []]
        broker.reclaim_stale.return_value = []
        broker.promote_due.return_value = 0

        def stop_on_second_consume(*args, **kwargs):
            if broker.consume.call_count >= 2:
                judge_worker._stop_worker()
            return []

        broker.consume.side_effect = stop_on_second_consume

        with patch("judge_worker.signal.signal"), \
             patch("judge_worker.get_redis_client", return_value=redis_client), \
             patch("judge_worker.RedisJudgeBroker", return_value=broker), \
             patch("judge_worker.RedisAsyncJobStore"), \
             patch("judge_worker.JudgeWorker"), \
             patch("judge_worker.os.path.exists", return_value=False), \
             patch("judge_worker.time.monotonic", side_effect=[0.0, 3.2, 4.0, 7.3]), \
             patch("judge_worker._DRAIN_EVENT.wait") as wait_mock, \
             patch("openai_config.get_openai_api_key", return_value="fake-key"):
            exit_code = judge_worker.run()

        self.assertEqual(exit_code, 0)
        broker.is_available.assert_not_called()
        wait_mock.assert_not_called()
