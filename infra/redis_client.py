"""Redis client with connection pooling and graceful fallback."""

import json
import logging
import os
from typing import Any, Optional
from functools import lru_cache

try:
    import redis
    from redis.connection import ConnectionPool
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    redis = None
    ConnectionPool = None

logger = logging.getLogger(__name__)


class RedisClient:
    """Thread-safe Redis client with connection pooling and fallback-to-none mode."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None,
        socket_timeout: float = 5.0,
        socket_connect_timeout: float = 2.0,
        max_connections: int = 50,
        decode_responses: bool = True,
        enabled: bool = True,
    ):
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.socket_timeout = socket_timeout
        self.socket_connect_timeout = socket_connect_timeout
        self.max_connections = max_connections
        self.decode_responses = decode_responses
        self.enabled = enabled and REDIS_AVAILABLE
        self._client: Optional[Any] = None
        self._available = False

        if self.enabled:
            self._connect()
        else:
            logger.info("Redis client disabled or redis module not available.")

    def _connect(self) -> None:
        """Initialize Redis connection pool with error handling."""
        if not REDIS_AVAILABLE:
            logger.warning("Redis module not available")
            self._available = False
            return

        try:
            pool = ConnectionPool(
                host=self.host,
                port=self.port,
                db=self.db,
                password=self.password,
                decode_responses=self.decode_responses,
                socket_timeout=self.socket_timeout,
                socket_connect_timeout=self.socket_connect_timeout,
                max_connections=self.max_connections,
            )
            self._client = redis.Redis(connection_pool=pool)
            self._client.ping()
            self._available = True
            logger.info(
                f"Redis client connected to {self.host}:{self.port} db={self.db}"
            )
        except Exception as error:
            logger.warning(f"Redis connection failed: {error}. Running in no-op mode.")
            self._available = False
            self._client = None

    def is_available(self) -> bool:
        """Check if Redis is connected and available."""
        if not self._available or self._client is None:
            return False
        try:
            self._client.ping()
            return True
        except Exception:
            self._available = False
            return False

    def set(self, key: str, value: Any, ex: Optional[int] = None) -> bool:
        """Set key-value with optional expiration in seconds."""
        if not self._available or self._client is None:
            return False
        try:
            serialized = json.dumps(value) if not isinstance(value, (str, bytes)) else value
            self._client.set(key, serialized, ex=ex)
            return True
        except Exception as error:
            logger.debug(f"Redis set failed for key={key}: {error}")
            return False

    def get(self, key: str) -> Optional[Any]:
        """Get value by key, returns None if not found or error."""
        if not self._available or self._client is None:
            return None
        try:
            value = self._client.get(key)
            if value is None:
                return None
            try:
                return json.loads(value)
            except (json.JSONDecodeError, ValueError):
                return value
        except Exception as error:
            logger.debug(f"Redis get failed for key={key}: {error}")
            return None

    def delete(self, *keys: str) -> int:
        """Delete one or more keys, returns count deleted."""
        if not self._available or self._client is None:
            return 0
        try:
            return self._client.delete(*keys)
        except Exception as error:
            logger.debug(f"Redis delete failed: {error}")
            return 0

    def exists(self, key: str) -> bool:
        """Check if key exists."""
        if not self._available or self._client is None:
            return False
        try:
            return self._client.exists(key) > 0
        except Exception as error:
            logger.debug(f"Redis exists failed for key={key}: {error}")
            return False

    def hset(self, name: str, mapping: dict) -> int:
        """Set multiple hash fields."""
        if not self._available or self._client is None:
            return 0
        try:
            serialized = {
                k: json.dumps(v) if not isinstance(v, (str, bytes)) else v
                for k, v in mapping.items()
            }
            return self._client.hset(name, mapping=serialized)
        except Exception as error:
            logger.debug(f"Redis hset failed for name={name}: {error}")
            return 0

    def hget(self, name: str, key: str) -> Optional[Any]:
        """Get single hash field."""
        if not self._available or self._client is None:
            return None
        try:
            value = self._client.hget(name, key)
            if value is None:
                return None
            try:
                return json.loads(value)
            except (json.JSONDecodeError, ValueError):
                return value
        except Exception as error:
            logger.debug(f"Redis hget failed for name={name} key={key}: {error}")
            return None

    def hgetall(self, name: str) -> dict:
        """Get all hash fields."""
        if not self._available or self._client is None:
            return {}
        try:
            data = self._client.hgetall(name)
            result = {}
            for k, v in data.items():
                try:
                    result[k] = json.loads(v)
                except (json.JSONDecodeError, ValueError):
                    result[k] = v
            return result
        except Exception as error:
            logger.debug(f"Redis hgetall failed for name={name}: {error}")
            return {}

    def incr(self, key: str) -> int:
        """Increment counter by 1."""
        if not self._available or self._client is None:
            return 0
        try:
            return self._client.incr(key)
        except Exception as error:
            logger.debug(f"Redis incr failed for key={key}: {error}")
            return 0

    def eval(self, script: str, numkeys: int, *keys_and_args: Any) -> Any:
        """Execute a Lua script atomically."""
        if not self._available or self._client is None:
            return None
        try:
            return self._client.eval(script, numkeys, *keys_and_args)
        except Exception as error:
            logger.debug(f"Redis eval failed: {error}")
            return None

    def lpush(self, name: str, *values: Any) -> int:
        """Push values to list (left side)."""
        if not self._available or self._client is None:
            return 0
        try:
            serialized = [
                json.dumps(v) if not isinstance(v, (str, bytes)) else v for v in values
            ]
            return self._client.lpush(name, *serialized)
        except Exception as error:
            logger.debug(f"Redis lpush failed for name={name}: {error}")
            return 0

    def lrange(self, name: str, start: int, stop: int) -> list:
        """Get range of list items."""
        if not self._available or self._client is None:
            return []
        try:
            values = self._client.lrange(name, start, stop)
            result = []
            for v in values:
                try:
                    result.append(json.loads(v))
                except (json.JSONDecodeError, ValueError):
                    result.append(v)
            return result
        except Exception as error:
            logger.debug(f"Redis lrange failed for name={name}: {error}")
            return []

    def close(self) -> None:
        """Close Redis connection."""
        if self._client:
            try:
                self._client.close()
                self._available = False
                logger.info("Redis client closed")
            except Exception as error:
                logger.warning(f"Error closing Redis: {error}")


@lru_cache(maxsize=1)
def get_redis_client() -> RedisClient:
    """Get singleton Redis client configured from environment."""
    host = os.getenv("REDIS_HOST", "localhost")
    port = int(os.getenv("REDIS_PORT", "6379"))
    db = int(os.getenv("REDIS_DB", "0"))
    password = os.getenv("REDIS_PASSWORD")
    enabled = os.getenv("REDIS_ENABLED", "false").lower() in {"true", "1", "yes"}
    socket_timeout = float(os.getenv("REDIS_SOCKET_TIMEOUT_SECONDS", "5.0"))
    socket_connect_timeout = float(os.getenv("REDIS_SOCKET_CONNECT_TIMEOUT_SECONDS", "3.0"))
    max_connections = int(os.getenv("REDIS_MAX_CONNECTIONS", "50"))

    return RedisClient(
        host=host,
        port=port,
        db=db,
        password=password,
        socket_timeout=socket_timeout,
        socket_connect_timeout=socket_connect_timeout,
        max_connections=max_connections,
        enabled=enabled,
    )
