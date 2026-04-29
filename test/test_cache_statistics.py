#!/usr/bin/env python3
"""Test cache statistics collection and visualization."""

import json
import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from multi_agent.workflow import MultiAgentChatWorkflow
from env_utils import load_project_env

load_project_env(__file__)


class DummyViolation(Exception):
    """Placeholder security exception."""


def test_cache_statistics():
    """Test that cache statistics are collected correctly."""
    print("Testing cache statistics collection...")
    print("Initializing workflow...")
    logger = logging.getLogger("test.cache_statistics")
    logger.setLevel(logging.CRITICAL)
    
    workflow = MultiAgentChatWorkflow(
        get_collection_fn=lambda _a, _b: (None, True, None),
        logger=logger,
        jailbreak_keywords=[],
        resource_limiter=None,
        prompt_injection_detector=None,
        vector_security_validator=None,
        output_validator=None,
        sensitive_info_filter=None,
        security_violation=DummyViolation,
        security_auditor=None,
        security_level=None,
        retrieval_cache_ttl_seconds=180,
        answer_cache_ttl_seconds=240,
    )

    print("\n--- Initial Cache Stats ---")
    initial_stats = workflow.get_cache_stats()
    print(json.dumps(initial_stats, indent=2, default=str))

    # Verify structure
    assert "generated_at_ms" in initial_stats
    assert "l1_retrieval" in initial_stats
    assert "l1_answer" in initial_stats
    assert "l2_redis" in initial_stats

    assert "entries" in initial_stats["l1_retrieval"]
    assert "max_entries" in initial_stats["l1_retrieval"]
    assert "entries" in initial_stats["l1_answer"]
    assert "max_entries" in initial_stats["l1_answer"]

    print("✓ Cache statistics structure is valid")

    # Verify L2 Redis structure (when available)
    if "operations" in initial_stats.get("l2_redis", {}):
        print(f"✓ L2 Redis is available: {initial_stats['l2_redis'].get('connected', False)}")
        if initial_stats["l2_redis"].get("connected"):
            print(f"  Redis operations: {initial_stats['l2_redis']['operations']}")
    else:
        print("✓ L2 Redis disabled (expected in test environment)")

    print("\n--- Cache Capacity Analysis ---")
    l1_retrieval_capacity = initial_stats["l1_retrieval"]["max_entries"]
    l1_answer_capacity = initial_stats["l1_answer"]["max_entries"]
    print(f"L1 Retrieval Cache Max Entries: {l1_retrieval_capacity}")
    print(f"L1 Answer Cache Max Entries: {l1_answer_capacity}")

    print("\n✓ Cache statistics test completed successfully")
    return True


def test_cache_statistics_endpoint():
    """Test the GET /monitoring/cache/stats endpoint via TestClient."""
    print("\nTesting cache statistics endpoint...")

    from fastapi.testclient import TestClient
    import api_server

    client = TestClient(api_server.app)
    response = client.get("/monitoring/cache/stats")

    assert response.status_code == 200, f"Expected 200, got {response.status_code}"
    data = response.json()

    assert "generated_at_ms" in data
    assert "l1_retrieval" in data
    assert "l1_answer" in data
    assert "l2_redis" in data

    print(f"✓ /monitoring/cache/stats returned 200 with valid structure")
    print(json.dumps(data, indent=2, default=str))
    return True


if __name__ == "__main__":
    try:
        success = test_cache_statistics()
        success = test_cache_statistics_endpoint() and success
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
