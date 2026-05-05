#!/usr/bin/env python3
"""Unit tests for evaluation_worker reliability logic.

All tests exercise _process_one_message() directly with mock dependencies
so no Redis instance or external process is required.

Covered paths:
1. Success: evaluate() succeeds → job_store.set_result(completed), broker.ack()
2. Poison: _decode_error present → DLQ(payload_decode_error), ack, no evaluate()
3. Poison: missing job_id → DLQ(missing_job_id), ack, no evaluate()
4. Idempotency (completed): is_completed() returns True → ack, no evaluate()
5. Idempotency (in-flight lock): acquire_processing() returns False → ack, no evaluate()
6. Retry (attempt < max): evaluate() raises → set_result(retrying), enqueue, ack,
   release_processing() — attempt counter incremented
7. Retry exhaustion (attempt == max_retries): evaluate() raises → set_result(dead_lettered),
   DLQ(max_retries_exhausted), ack
8. Retry enqueue failure: evaluate() raises, enqueue returns False → set_result(dead_lettered),
   DLQ(retry_enqueue_failed), ack
9. Backoff respected: retry schedules with non-zero sleep (mocked) when backoff_base > 0
10. Evaluate error flag: result dict with 'error' key raises RuntimeError and triggers retry
"""

from __future__ import annotations

import unittest
import uuid
from unittest.mock import MagicMock, call, patch

from evaluation_worker import _process_one_message


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_deps(
    *,
    is_completed: bool = False,
    acquire_processing: bool = True,
    evaluate_result=None,
    evaluate_side_effect=None,
    enqueue_result: bool = True,
):
    """Build mock broker, job_store, and analysis_worker for one test."""
    broker = MagicMock()
    broker.ack.return_value = True
    broker.dead_letter.return_value = True
    broker.enqueue.return_value = enqueue_result

    job_store = MagicMock()
    job_store.is_completed.return_value = is_completed
    job_store.acquire_processing.return_value = acquire_processing
    job_store.set_result.return_value = True
    job_store.release_processing.return_value = True

    analysis_worker = MagicMock()
    if evaluate_side_effect is not None:
        analysis_worker.evaluate.side_effect = evaluate_side_effect
    else:
        analysis_worker.evaluate.return_value = (
            evaluate_result if evaluate_result is not None
            else {"faithfulness": 0.90, "answer_relevancy": 0.88}
        )

    return broker, job_store, analysis_worker


def _call(
    broker,
    job_store,
    analysis_worker,
    payload,
    *,
    max_retries: int = 3,
    backoff_base: float = 0.0,   # zero by default so tests don't sleep
    backoff_max: float = 0.0,
    processing_ttl: int = 300,
    consumer_name: str = "test-consumer",
    message_id: str = "1-0",
):
    _process_one_message(
        message_id=message_id,
        payload=payload,
        broker=broker,
        job_store=job_store,
        analysis_worker=analysis_worker,
        consumer_name=consumer_name,
        max_retries=max_retries,
        backoff_base=backoff_base,
        backoff_max=backoff_max,
        processing_ttl=processing_ttl,
    )


def _good_payload(job_id: str | None = None) -> dict:
    return {
        "job_id": job_id or f"job-{uuid.uuid4()}",
        "question": "What caused the Apollo 13 emergency?",
        "answer": "An oxygen tank exploded.",
        "contexts": ["Apollo 13 oxygen tank explosion context."],
        "_attempt": 0,
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestEvaluationWorkerSuccessPath(unittest.TestCase):

    def test_successful_evaluation_stores_completed_result_and_acks(self):
        broker, job_store, analysis_worker = _make_deps(
            evaluate_result={"faithfulness": 0.92, "answer_relevancy": 0.89}
        )
        payload = _good_payload()

        _call(broker, job_store, analysis_worker, payload)

        # evaluate() must have been called once.
        analysis_worker.evaluate.assert_called_once()

        # set_result must record a completed payload on the right Redis key.
        job_store.set_result.assert_called_once()
        set_result_key = job_store.set_result.call_args[0][0]  # Gap-4: first positional arg
        stored = job_store.set_result.call_args[0][1]
        self.assertEqual(set_result_key, payload["job_id"])   # Gap-4: key must match job_id
        self.assertEqual(stored["job_id"], payload["job_id"])
        self.assertEqual(stored["status"], "completed")
        self.assertEqual(stored["source"], "async")
        self.assertAlmostEqual(stored["faithfulness"], 0.92)

        # Message must be acked exactly once.
        broker.ack.assert_called_once_with("1-0")
        broker.dead_letter.assert_not_called()

    def test_evaluate_result_error_flag_triggers_retry_not_success(self):
        """A result dict with 'error' key must be treated as a failure."""
        broker, job_store, analysis_worker = _make_deps(
            evaluate_result={"error": "ragas pipeline failure"}
        )
        payload = _good_payload()

        _call(broker, job_store, analysis_worker, payload, max_retries=3)

        stored = job_store.set_result.call_args[0][1]
        # Should be retrying, not completed.
        self.assertEqual(stored["status"], "retrying")
        broker.enqueue.assert_called_once()


class TestEvaluationWorkerPoisonMessages(unittest.TestCase):

    def test_decode_error_routes_to_dlq_and_acks_without_calling_evaluate(self):
        broker, job_store, analysis_worker = _make_deps()
        payload = {
            "_decode_error": "Expecting value: line 1 column 1",
            "_raw_payload": "{broken",
            "job_id": "",
        }

        _call(broker, job_store, analysis_worker, payload)

        broker.dead_letter.assert_called_once()
        _, kwargs = broker.dead_letter.call_args
        self.assertEqual(kwargs["reason"], "payload_decode_error")
        self.assertEqual(kwargs["consumer_name"], "test-consumer")
        broker.ack.assert_called_once_with("1-0")
        analysis_worker.evaluate.assert_not_called()
        job_store.set_result.assert_not_called()

    def test_missing_job_id_routes_to_dlq_and_acks_without_calling_evaluate(self):
        broker, job_store, analysis_worker = _make_deps()
        payload = {"question": "no job id here", "_attempt": 0}

        _call(broker, job_store, analysis_worker, payload)

        broker.dead_letter.assert_called_once()
        _, kwargs = broker.dead_letter.call_args
        self.assertEqual(kwargs["reason"], "missing_job_id")
        broker.ack.assert_called_once_with("1-0")
        analysis_worker.evaluate.assert_not_called()

    def test_empty_string_job_id_treated_as_missing(self):
        broker, job_store, analysis_worker = _make_deps()
        payload = {"job_id": "   ", "question": "Q"}  # whitespace-only stripped to ""

        _call(broker, job_store, analysis_worker, payload)

        broker.dead_letter.assert_called_once()
        _, kwargs = broker.dead_letter.call_args
        self.assertEqual(kwargs["reason"], "missing_job_id")

    def test_missing_question_in_payload_triggers_retry(self):
        """A valid job_id but absent question field must raise ValueError and retry."""
        broker, job_store, analysis_worker = _make_deps()
        payload = {"job_id": f"job-{uuid.uuid4()}", "answer": "some answer", "_attempt": 0}
        # No 'question' key → _coerce_workflow_input returns empty string → ValueError

        _call(broker, job_store, analysis_worker, payload, max_retries=3)

        stored = job_store.set_result.call_args_list[0][0][1]
        self.assertEqual(stored["status"], "retrying")
        broker.enqueue.assert_called_once()
        broker.dead_letter.assert_not_called()


class TestEvaluationWorkerIdempotency(unittest.TestCase):

    def test_already_completed_job_is_skipped_and_acked(self):
        broker, job_store, analysis_worker = _make_deps(is_completed=True)

        _call(broker, job_store, analysis_worker, _good_payload())

        broker.ack.assert_called_once()
        analysis_worker.evaluate.assert_not_called()
        broker.dead_letter.assert_not_called()
        job_store.set_result.assert_not_called()

    def test_duplicate_in_flight_lock_skips_and_acks(self):
        broker, job_store, analysis_worker = _make_deps(
            is_completed=False, acquire_processing=False
        )

        _call(broker, job_store, analysis_worker, _good_payload())

        broker.ack.assert_called_once()
        analysis_worker.evaluate.assert_not_called()
        broker.dead_letter.assert_not_called()

    def test_processing_lock_acquired_with_correct_ttl(self):
        broker, job_store, analysis_worker = _make_deps()
        payload = _good_payload()

        _call(broker, job_store, analysis_worker, payload, processing_ttl=600)

        job_store.acquire_processing.assert_called_once()
        args, kwargs = job_store.acquire_processing.call_args
        self.assertEqual(args[0], payload["job_id"])  # Gap-3: correct job_id key
        self.assertEqual(kwargs["processing_ttl_seconds"], 600)


class TestEvaluationWorkerRetryLogic(unittest.TestCase):

    def test_first_failure_schedules_retry_with_incremented_attempt(self):
        broker, job_store, analysis_worker = _make_deps(
            evaluate_side_effect=RuntimeError("transient failure")
        )
        payload = _good_payload()
        payload["_attempt"] = 0

        _call(broker, job_store, analysis_worker, payload, max_retries=3)

        # set_result called with retrying status.
        retrying_call = job_store.set_result.call_args_list[0]
        stored = retrying_call[0][1]
        self.assertEqual(stored["status"], "retrying")
        self.assertEqual(stored["attempt"], 1)
        self.assertEqual(stored["max_retries"], 3)

        # retry must be enqueued with _attempt=1.
        broker.enqueue.assert_called_once()
        enqueued_job_id, enqueued_payload = broker.enqueue.call_args[0]
        self.assertEqual(enqueued_job_id, payload["job_id"])
        self.assertEqual(enqueued_payload["_attempt"], 1)
        self.assertIn("_last_error", enqueued_payload)

        # Ack and lock release happen after successful re-enqueue.
        broker.ack.assert_called_once_with("1-0")
        job_store.release_processing.assert_called_once()
        broker.dead_letter.assert_not_called()

    def test_retry_at_max_attempts_dead_letters_the_message(self):
        broker, job_store, analysis_worker = _make_deps(
            evaluate_side_effect=RuntimeError("still failing")
        )
        payload = _good_payload()
        payload["_attempt"] = 3  # already at max

        _call(broker, job_store, analysis_worker, payload, max_retries=3)

        # set_result must record dead_lettered.
        stored = job_store.set_result.call_args[0][1]
        self.assertEqual(stored["status"], "dead_lettered")
        self.assertEqual(stored["attempt"], 3)

        # DLQ write must have correct reason.
        broker.dead_letter.assert_called_once()
        _, kwargs = broker.dead_letter.call_args
        self.assertEqual(kwargs["reason"], "max_retries_exhausted")
        self.assertEqual(kwargs["attempt"], 3)

        broker.ack.assert_called_once_with("1-0")
        broker.enqueue.assert_not_called()
        job_store.release_processing.assert_not_called()

    def test_retry_enqueue_failure_dead_letters_instead_of_scheduling(self):
        broker, job_store, analysis_worker = _make_deps(
            evaluate_side_effect=RuntimeError("failure"),
            enqueue_result=False,   # enqueue deliberately fails
        )
        payload = _good_payload()
        payload["_attempt"] = 0

        _call(broker, job_store, analysis_worker, payload, max_retries=3)

        # Most recent set_result call must be dead_lettered.
        stored = job_store.set_result.call_args[0][1]
        self.assertEqual(stored["status"], "dead_lettered")
        self.assertEqual(stored["error"], "retry enqueue failed")

        broker.dead_letter.assert_called_once()
        _, kwargs = broker.dead_letter.call_args
        self.assertEqual(kwargs["reason"], "retry_enqueue_failed")
        broker.ack.assert_called_once_with("1-0")

    def test_max_retries_zero_dead_letters_on_first_failure(self):
        """With max_retries=0 the very first error must go straight to DLQ."""
        broker, job_store, analysis_worker = _make_deps(
            evaluate_side_effect=ValueError("immediate failure")
        )
        payload = _good_payload()
        payload["_attempt"] = 0

        _call(broker, job_store, analysis_worker, payload, max_retries=0)

        stored = job_store.set_result.call_args[0][1]
        self.assertEqual(stored["status"], "dead_lettered")
        broker.dead_letter.assert_called_once()
        _, kwargs = broker.dead_letter.call_args
        self.assertEqual(kwargs["reason"], "max_retries_exhausted")
        broker.enqueue.assert_not_called()


class TestEvaluationWorkerBackoff(unittest.TestCase):

    @patch("evaluation_worker.time.sleep")
    def test_backoff_sleep_called_with_correct_duration_on_retry(self, mock_sleep):
        broker, job_store, analysis_worker = _make_deps(
            evaluate_side_effect=RuntimeError("boom")
        )
        payload = _good_payload()
        payload["_attempt"] = 0

        # base=1.0, max=8.0, attempt=0 → sleep(1.0 * 2^0) = sleep(1.0)
        _call(
            broker, job_store, analysis_worker, payload,
            max_retries=3,
            backoff_base=1.0,
            backoff_max=8.0,
        )

        mock_sleep.assert_called_once_with(1.0)

    @patch("evaluation_worker.time.sleep")
    def test_backoff_caps_at_max(self, mock_sleep):
        broker, job_store, analysis_worker = _make_deps(
            evaluate_side_effect=RuntimeError("boom")
        )
        payload = _good_payload()
        payload["_attempt"] = 10  # forces 0.5 * 2^10 = 512, capped at 4.0

        _call(
            broker, job_store, analysis_worker, payload,
            max_retries=20,
            backoff_base=0.5,
            backoff_max=4.0,
        )

        mock_sleep.assert_called_once_with(4.0)

    @patch("evaluation_worker.time.sleep")
    def test_zero_backoff_does_not_sleep(self, mock_sleep):
        broker, job_store, analysis_worker = _make_deps(
            evaluate_side_effect=RuntimeError("boom")
        )

        _call(
            broker, job_store, analysis_worker, _good_payload(),
            max_retries=3,
            backoff_base=0.0,
            backoff_max=0.0,
        )

        mock_sleep.assert_not_called()


class TestEvaluationWorkerAckOrdering(unittest.TestCase):
    """Verify ack is never called before critical side effects complete."""

    def test_ack_occurs_after_set_result_on_success(self):
        call_order: list[str] = []
        broker, job_store, analysis_worker = _make_deps()
        job_store.set_result.side_effect = lambda *a, **kw: call_order.append("set_result") or True
        broker.ack.side_effect = lambda *a, **kw: call_order.append("ack")

        _call(broker, job_store, analysis_worker, _good_payload())

        self.assertLess(call_order.index("set_result"), call_order.index("ack"))

    def test_ack_occurs_after_dead_letter_on_poisoned_message(self):
        call_order: list[str] = []
        broker, job_store, analysis_worker = _make_deps()
        broker.dead_letter.side_effect = lambda *a, **kw: call_order.append("dead_letter") or True
        broker.ack.side_effect = lambda *a, **kw: call_order.append("ack")

        payload = {"_decode_error": "bad json", "_raw_payload": "x"}
        _call(broker, job_store, analysis_worker, payload)

        self.assertLess(call_order.index("dead_letter"), call_order.index("ack"))

    def test_ack_occurs_after_enqueue_on_retry(self):
        call_order: list[str] = []
        broker, job_store, analysis_worker = _make_deps(
            evaluate_side_effect=RuntimeError("fail")
        )
        broker.enqueue.side_effect = lambda *a, **kw: call_order.append("enqueue") or True
        broker.ack.side_effect = lambda *a, **kw: call_order.append("ack")

        _call(broker, job_store, analysis_worker, _good_payload(), max_retries=3)

        self.assertLess(call_order.index("enqueue"), call_order.index("ack"))

    def test_release_processing_called_after_successful_reenqueue(self):
        call_order: list[str] = []
        broker, job_store, analysis_worker = _make_deps(
            evaluate_side_effect=RuntimeError("fail")
        )
        broker.enqueue.side_effect = lambda *a, **kw: call_order.append("enqueue") or True
        job_store.release_processing.side_effect = lambda *a, **kw: call_order.append("release") or True

        _call(broker, job_store, analysis_worker, _good_payload(), max_retries=3)

        # Release should come after enqueue (lock freed only once re-enqueue succeeded).
        self.assertIn("enqueue", call_order)
        self.assertIn("release", call_order)
        self.assertLess(call_order.index("enqueue"), call_order.index("release"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
