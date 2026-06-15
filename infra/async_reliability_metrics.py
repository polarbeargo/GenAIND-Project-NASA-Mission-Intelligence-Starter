"""Redis-backed reliability counters/gauges for async worker operations."""

from __future__ import annotations

import logging
from functools import lru_cache
from typing import Dict, List, Tuple

from infra.redis_client import RedisClient, get_redis_client

logger = logging.getLogger(__name__)


def _sanitize(value: str) -> str:
    return str(value).replace("|", "_").replace("=", "_")


class AsyncReliabilityMetrics:
    """Atomic reliability metrics using Redis hash operations.

    Counters and gauges are stored in a single hash. Field format:
      <metric>|k1=v1|k2=v2
    """

    def __init__(self, redis_client: RedisClient, hash_name: str = "nasa:async:reliability:metrics"):
        self.redis = redis_client
        self.hash_name = hash_name

    @staticmethod
    def _field(metric: str, labels: Dict[str, str]) -> str:
        parts = [metric]
        for key in sorted(labels.keys()):
            parts.append(f"{_sanitize(key)}={_sanitize(labels[key])}")
        return "|".join(parts)

    def _incr(self, metric: str, labels: Dict[str, str], amount: float = 1.0) -> None:
        if not self.redis.is_available():
            return
        field = self._field(metric, labels)
        self.redis.hincrbyfloat(self.hash_name, field, amount)

    def _set(self, metric: str, labels: Dict[str, str], value: float) -> None:
        if not self.redis.is_available():
            return
        field = self._field(metric, labels)
        self.redis.hset(self.hash_name, {field: float(value)})

    def record_retry(self, worker: str, reason: str = "processing_error") -> None:
        self._incr("nasa_async_worker_retry_total", {"worker": worker, "reason": reason})

    def record_dlq(self, worker: str, reason: str) -> None:
        self._incr("nasa_async_worker_dlq_total", {"worker": worker, "reason": reason})

    def record_reclaim(self, worker: str, reclaimed_count: int, min_idle_ms: int) -> None:
        if reclaimed_count <= 0:
            return
        self._incr("nasa_async_worker_reclaim_total", {"worker": worker}, float(reclaimed_count))
        # Lower-bound idle age for reclaimed items (reclaimed entries are at least min_idle_ms old).
        self._set("nasa_async_worker_reclaim_age_lower_bound_ms", {"worker": worker}, float(min_idle_ms))

    def record_lock_acquire_fail(self, worker: str, reason: str = "contended") -> None:
        self._incr("nasa_async_worker_lock_acquire_fail_total", {"worker": worker, "reason": reason})

    def snapshot(self) -> Dict[str, List[Tuple[Dict[str, str], float]]]:
        if not self.redis.is_available():
            return {}
        raw = self.redis.hgetall(self.hash_name)
        result: Dict[str, List[Tuple[Dict[str, str], float]]] = {}
        for field, value in raw.items():
            try:
                metric, *label_parts = str(field).split("|")
                labels: Dict[str, str] = {}
                for part in label_parts:
                    if "=" not in part:
                        continue
                    key, val = part.split("=", 1)
                    labels[key] = val
                result.setdefault(metric, []).append((labels, float(value)))
            except Exception as error:
                logger.debug("Ignoring malformed reliability metric field=%s: %s", field, error)
        return result


@lru_cache(maxsize=1)
def get_async_reliability_metrics() -> AsyncReliabilityMetrics:
    return AsyncReliabilityMetrics(get_redis_client())
