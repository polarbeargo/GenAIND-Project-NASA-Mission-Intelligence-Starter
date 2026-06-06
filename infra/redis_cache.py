"""Redis L2 cache for retrieval and response caching (shared across pods)."""

import hashlib
import json
import logging
import time
from typing import Any, Dict, Optional

from infra.redis_client import RedisClient

logger = logging.getLogger(__name__)


class RedisL2Cache:
    """
    L2 cache in Redis for retrieval and response results.
    
    L1 (in-process OrderedDict) is checked first, then L2 (Redis), then L3 (ChromaDB/OpenAI).
    """

    def __init__(
        self,
        redis_client: RedisClient,
        retrieval_ttl_seconds: int = 300,
        response_ttl_seconds: int = 600,
    ):
        self.redis = redis_client
        self.retrieval_ttl = retrieval_ttl_seconds
        self.response_ttl = response_ttl_seconds
        self._hits = 0
        self._misses = 0

    def _cache_key_retrieval(
        self, query: str, mission_filter: Optional[str], collection_name: str
    ) -> str:
        """Generate cache key for retrieval results."""
        normalized = f"{query.strip().lower()}:{mission_filter or ''}:{collection_name}"
        hash_val = hashlib.md5(normalized.encode(), usedforsecurity=False).hexdigest()
        return f"cache:retrieval:{hash_val}"

    def _cache_key_response(
        self, query: str, mission_filter: Optional[str], collection_name: str, model: str, evaluate: bool
    ) -> str:
        """Generate cache key for response results."""
        normalized = (
            f"{query.strip().lower()}:{mission_filter or ''}:{collection_name}:{model}:{evaluate}"
        )
        hash_val = hashlib.md5(normalized.encode(), usedforsecurity=False).hexdigest()
        return f"cache:response:{hash_val}"

    def get_retrieval(
        self, query: str, mission_filter: Optional[str], collection_name: str
    ) -> Optional[list]:
        """Get cached retrieval result (list of doc chunks with metadata)."""
        if not self.redis.is_available():
            return None

        key = self._cache_key_retrieval(query, mission_filter, collection_name)
        result = self.redis.get(key)
        if result is not None:
            self._hits += 1
            logger.debug(f"L2 retrieval cache HIT: {key}")
            return result
        self._misses += 1
        return None

    def set_retrieval(
        self,
        query: str,
        mission_filter: Optional[str],
        collection_name: str,
        docs: list,
    ) -> None:
        """Cache retrieval result in Redis."""
        if not self.redis.is_available():
            return

        key = self._cache_key_retrieval(query, mission_filter, collection_name)
        self.redis.set(key, docs, ex=self.retrieval_ttl)
        logger.debug(f"L2 retrieval cache SET: {key}")

    def get_response(
        self, query: str, mission_filter: Optional[str], collection_name: str, model: str, evaluate: bool
    ) -> Optional[Dict[str, Any]]:
        """Get cached response (answer + metadata)."""
        if not self.redis.is_available():
            return None

        key = self._cache_key_response(query, mission_filter, collection_name, model, evaluate)
        result = self.redis.get(key)
        if result is not None:
            self._hits += 1
            logger.debug(f"L2 response cache HIT: {key}")
            return result
        self._misses += 1
        return None

    def set_response(
        self,
        query: str,
        mission_filter: Optional[str],
        collection_name: str,
        model: str,
        evaluate: bool,
        response: Dict[str, Any],
    ) -> None:
        """Cache response in Redis."""
        if not self.redis.is_available():
            return

        key = self._cache_key_response(query, mission_filter, collection_name, model, evaluate)
        self.redis.set(key, response, ex=self.response_ttl)
        logger.debug(f"L2 response cache SET: {key}")

    def clear(self) -> None:
        """Clear all cache entries (rarely needed)."""
        if not self.redis.is_available():
            return
        try:
            keys = self.redis._client.keys("cache:*") if self.redis._client else []
            if keys:
                self.redis.delete(*keys)
                logger.info(f"Cleared {len(keys)} L2 cache entries")
        except Exception as error:
            logger.warning(f"L2 cache clear failed: {error}")

    def stats(self) -> Dict[str, Any]:
        """Return cache performance statistics."""
        total = self._hits + self._misses
        hit_rate = (self._hits / total * 100) if total > 0 else 0.0
        return {
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate_percent": round(hit_rate, 2),
            "total_requests": total,
            "available": self.redis.is_available(),
        }
