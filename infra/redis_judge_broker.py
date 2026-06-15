"""Redis stream broker for async judge jobs."""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Dict, List, Tuple

from infra.async_reliability_metrics import get_async_reliability_metrics
from infra.redis_client import RedisClient

logger = logging.getLogger(__name__)


class RedisJudgeBroker:
    """Queue judge jobs on Redis Streams for external worker consumption."""

    def __init__(
        self,
        redis_client: RedisClient,
        stream_name: str = "judge:jobs",
        consumer_group: str = "judge-workers",
        dead_letter_stream: str | None = None,
        enabled: bool = False,
    ):
        self.redis = redis_client
        self.stream_name = stream_name
        self.consumer_group = consumer_group
        self.dead_letter_stream = dead_letter_stream or f"{stream_name}:dlq"
        self.enabled = bool(enabled)
        self._group_initialized = False
        self._worker_label = "judge"

    def is_available(self) -> bool:
        return self.enabled and self.redis.is_available() and self.redis._client is not None

    def _ensure_group(self) -> bool:
        if self._group_initialized:
            return True
        if not self.is_available():
            return False

        try:
            # Create stream/group if missing; mkstream=True creates stream lazily.
            self.redis._client.xgroup_create(
                name=self.stream_name,
                groupname=self.consumer_group,
                id="0",
                mkstream=True,
            )
        except Exception as error:
            # BUSYGROUP is expected once group exists.
            if "BUSYGROUP" not in str(error):
                logger.warning("Failed to initialize judge consumer group: %s", error)
                return False

        self._group_initialized = True
        return True

    def enqueue(self, job_id: str, payload: Dict[str, Any]) -> bool:
        """Push one judge job message onto the broker stream."""
        if not self._ensure_group():
            return False

        try:
            message = {
                "job_id": job_id,
                "payload": json.dumps(payload),
            }
            self.redis._client.xadd(self.stream_name, message)
            return True
        except Exception as error:
            logger.warning("Failed to enqueue judge job %s: %s", job_id, error)
            return False

    def has_active_consumers(self, timeout_seconds: float = 0.0, poll_interval_seconds: float = 0.05) -> bool:
        """Return True when at least one consumer is registered on the group."""
        if not self._ensure_group():
            return False

        deadline = time.monotonic() + max(0.0, float(timeout_seconds))
        poll_interval = max(0.01, float(poll_interval_seconds))

        while True:
            try:
                consumers = self.redis._client.xinfo_consumers(self.stream_name, self.consumer_group)
                if consumers:
                    return True
            except Exception as error:
                logger.debug("Failed to inspect judge consumers: %s", error)
                return False

            if time.monotonic() >= deadline:
                return False
            time.sleep(poll_interval)

    def consume(
        self,
        consumer_name: str,
        count: int = 1,
        block_ms: int = 5000,
    ) -> List[Tuple[str, Dict[str, Any]]]:
        """Consume pending/new messages for the configured consumer group."""
        if not self._ensure_group():
            return []

        try:
            rows = self.redis._client.xreadgroup(
                groupname=self.consumer_group,
                consumername=consumer_name,
                streams={self.stream_name: ">"},
                count=max(1, int(count)),
                block=max(0, int(block_ms)),
            )
        except Exception as error:
            logger.warning("Failed to consume judge jobs: %s", error)
            return []

        messages: List[Tuple[str, Dict[str, Any]]] = []
        for _stream, entries in rows or []:
            for message_id, fields in entries:
                payload_text = fields.get("payload")
                if not payload_text:
                    continue
                try:
                    payload = json.loads(payload_text)
                except Exception as error:
                    payload = {
                        "_decode_error": str(error)[:160],
                        "_raw_payload": str(payload_text)[:500],
                    }
                if "job_id" not in payload and fields.get("job_id"):
                    payload["job_id"] = fields.get("job_id")
                messages.append((message_id, payload))
        return messages

    def dead_letter(
        self,
        *,
        message_id: str,
        payload: Dict[str, Any],
        reason: str,
        consumer_name: str,
        attempt: int,
    ) -> bool:
        """Write an unprocessable message to DLQ for later inspection."""
        if not self.is_available():
            return False
        try:
            job_id = str(payload.get("job_id", ""))
            message = {
                "job_id": job_id,
                "source_stream": self.stream_name,
                "source_message_id": message_id,
                "consumer": consumer_name,
                "reason": reason[:200],
                "attempt": str(max(0, int(attempt))),
                "timestamp_ms": str(round(time.time() * 1000)),
                "payload": json.dumps(payload),
            }
            self.redis._client.xadd(self.dead_letter_stream, message)
            get_async_reliability_metrics().record_dlq(worker=self._worker_label, reason=reason)
            return True
        except Exception as error:
            logger.warning("Failed to dead-letter judge message %s: %s", message_id, error)
            return False

    def ack(self, message_id: str) -> bool:
        """Acknowledge one stream message as processed."""
        if not self.is_available():
            return False
        try:
            self.redis._client.xack(self.stream_name, self.consumer_group, message_id)
            return True
        except Exception as error:
            logger.warning("Failed to ack judge message %s: %s", message_id, error)
            return False

    def reclaim_stale(
        self,
        consumer_name: str,
        min_idle_ms: int = 300_000,
        count: int = 10,
    ) -> List[Tuple[str, Dict[str, Any]]]:
        """Reclaim PEL entries idle longer than min_idle_ms (consumer-crash recovery)."""
        if not self._ensure_group():
            return []
        try:
            result = self.redis._client.xautoclaim(
                name=self.stream_name,
                groupname=self.consumer_group,
                consumername=consumer_name,
                min_idle_time=max(1000, int(min_idle_ms)),
                start_id="0-0",
                count=max(1, int(count)),
            )
        except Exception as error:
            logger.debug("XAUTOCLAIM not available or failed: %s", error)
            return []

        entries = result[1] if isinstance(result, (list, tuple)) and len(result) > 1 else []
        messages: List[Tuple[str, Dict[str, Any]]] = []
        for message_id, fields in entries or []:
            payload_text = fields.get("payload") if isinstance(fields, dict) else None
            if not payload_text:
                continue
            try:
                payload = json.loads(payload_text)
            except Exception as decode_error:
                payload = {
                    "_decode_error": str(decode_error)[:160],
                    "_raw_payload": str(payload_text)[:500],
                }
            if "job_id" not in payload and isinstance(fields, dict) and fields.get("job_id"):
                payload["job_id"] = fields.get("job_id")
            payload["_attempt"] = max(0, int(payload.get("_attempt", 0))) + 1
            messages.append((message_id, payload))

        if messages:
            get_async_reliability_metrics().record_reclaim(
                worker=self._worker_label,
                reclaimed_count=len(messages),
                min_idle_ms=min_idle_ms,
            )
            logger.info(
                "Reclaimed %d stale judge PEL entries (min_idle=%dms)",
                len(messages),
                min_idle_ms,
            )
        return messages
