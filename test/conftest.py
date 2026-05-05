"""pytest configuration shared across the test suite.

Provides the ``redis`` marker and supporting fixtures so that Redis-dependent
tests skip **individually** (each shows as ``s`` in the output) rather than
silently dropping the entire class via ``unittest.SkipTest`` in ``setUpClass``.

Usage
-----
Mark any test class or method that needs a live Redis with::

    @pytest.mark.redis
    class TestMyRedisFeature(unittest.TestCase):
        ...

Without Redis available, each test shows::

    SKIPPED [redis] Redis not available — start Redis or pass --require-redis

With CI that has a Redis service container, add ``--require-redis`` to turn
the skip into an error so the suite cannot pass unnoticed::

    uv run python -m pytest --require-redis ...
"""

from __future__ import annotations

import pytest

from infra.redis_client import RedisClient


# ---------------------------------------------------------------------------
# CLI option
# ---------------------------------------------------------------------------

def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--require-redis",
        action="store_true",
        default=False,
        help=(
            "Treat Redis-dependent tests as ERRORS (not skips) when Redis is "
            "unavailable.  Use in CI environments that provision a Redis service."
        ),
    )


# ---------------------------------------------------------------------------
# Marker registration
# ---------------------------------------------------------------------------

def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "redis: marks tests that require a live Redis instance "
        "(skipped when Redis is unavailable; use --require-redis to fail instead)",
    )


# ---------------------------------------------------------------------------
# Session-scoped Redis availability check
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def _redis_status(request: pytest.FixtureRequest) -> bool:
    """Return True if Redis is reachable; False otherwise.

    If ``--require-redis`` was passed and Redis is absent, the whole session
    fails immediately rather than silently skipping.
    """
    available = RedisClient(enabled=True).is_available()
    if not available and request.config.getoption("--require-redis"):
        pytest.fail(
            "Redis is required (--require-redis) but is not available. "
            "Start Redis, or remove --require-redis to skip Redis tests instead.",
            pytrace=False,
        )
    return available


# ---------------------------------------------------------------------------
# Per-test autouse fixture — enforces the marker semantic
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _skip_if_no_redis(request: pytest.FixtureRequest, _redis_status: bool) -> None:
    """Skip individual Redis-marked tests when Redis is unavailable.

    Because this is a per-test (function-scoped) autouse fixture, each test
    appears in the output with its own ``SKIPPED`` line rather than the entire
    class being swallowed by a class-level ``unittest.SkipTest``.
    """
    if request.node.get_closest_marker("redis") and not _redis_status:
        pytest.skip(
            "Redis not available — start Redis or pass --require-redis "
            "to turn this skip into a failure"
        )
