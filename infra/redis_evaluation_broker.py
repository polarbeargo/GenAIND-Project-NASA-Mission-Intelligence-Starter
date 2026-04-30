"""Redis stream broker for async evaluation jobs."""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Tuple

from infra.redis_client import RedisClient

logger = logging.getLogger(__name__)


class RedisEvaluationBroker:
    """Queue evaluation jobs on Redis Streams for external worker consumption."""

    def __init__(
        self,
        redis_client: RedisClient,
        stream_name: str = "eval:jobs",
        consumer_group: str = "eval-workers",
        enabled: bool = False,
    ):
        self.redis = redis_client
        self.stream_name = stream_name
        self.consumer_group = consumer_group
        self.enabled = bool(enabled)
        self._group_initialized = False

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
                logger.warning("Failed to initialize evaluation consumer group: %s", error)
                return False

        self._group_initialized = True
        return True

    def enqueue(self, job_id: str, payload: Dict[str, Any]) -> bool:
        """Push one evaluation job message onto the broker stream."""
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
            logger.warning("Failed to enqueue evaluation job %s: %s", job_id, error)
            return False

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
            logger.warning("Failed to consume evaluation jobs: %s", error)
            return []

        messages: List[Tuple[str, Dict[str, Any]]] = []
        for _stream, entries in rows or []:
            for message_id, fields in entries:
                payload_text = fields.get("payload")
                if not payload_text:
                    continue
                try:
                    payload = json.loads(payload_text)
                except Exception:
                    payload = {}
                if "job_id" not in payload and fields.get("job_id"):
                    payload["job_id"] = fields.get("job_id")
                messages.append((message_id, payload))
        return messages

    def ack(self, message_id: str) -> bool:
        """Acknowledge one stream message as processed."""
        if not self.is_available():
            return False
        try:
            self.redis._client.xack(self.stream_name, self.consumer_group, message_id)
            return True
        except Exception as error:
            logger.warning("Failed to ack evaluation message %s: %s", message_id, error)
            return False
