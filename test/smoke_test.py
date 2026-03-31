#!/usr/bin/env python3
"""Smoke test for NASA RAG integration: health + chat + monitoring + promptfoo."""

import subprocess
import sys
import time
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

import requests

from openai_config import get_openai_chat_model

API_BASE = "http://127.0.0.1:8001"
SLEEP_BEFORE_REQUESTS = 1.5


def print_header(msg: str):
    print(f"\n{'='*60}\n  {msg}\n{'='*60}")


def print_pass(msg: str):
    print(f"  ✓ {msg}")


def print_fail(msg: str):
    print(f"  ✗ {msg}")


def print_info(msg: str):
    print(f"  ℹ {msg}")


def test_health() -> bool:
    print_header("TEST 1: API Health Check")
    try:
        resp = requests.get(f"{API_BASE}/health", timeout=5)
        if resp.status_code == 200:
            print_pass(f"Health OK: {resp.json()}")
            return True
        else:
            print_fail(f"Status {resp.status_code}")
            return False
    except Exception as e:
        print_fail(f"Failed: {e}")
        return False


def test_chat() -> bool:
    print_header("TEST 2: RAG Chat with Telemetry")
    payload = {
        "question": "What was the cause of the Apollo 13 emergency?",
        "chroma_dir": "./chroma_db",
        "collection_name": "nasa_space_missions_test",
        "n_results": 3,
        "evaluate": False,
        "model": get_openai_chat_model(),
    }
    try:
        print_info(f"Query: {payload['question']}")
        resp = requests.post(f"{API_BASE}/chat", json=payload, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            print_pass(f"Chat OK (latency: {data.get('latency_ms', 0):.0f}ms)")
            print_info(f"Answer: {data['answer'][:60]}...")
            return True
        elif resp.status_code == 401:
            print_fail(f"Auth error: {resp.json().get('detail', 'Unknown')}")
            return False
        else:
            print_fail(f"HTTP {resp.status_code}")
            return False
    except requests.exceptions.Timeout:
        print_fail("Timeout")
        return False
    except Exception as e:
        print_fail(f"Failed: {e}")
        return False


def test_monitoring() -> bool:
    print_header("TEST 3: Monitoring Report")
    try:
        resp = requests.get(f"{API_BASE}/monitoring/report", params={"reference_rows": 10}, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if "error" in data:
                print_info(f"Status: {data['error']} (expected if <20 rows)")
                return True
            else:
                print_pass(f"Report ready: {data.get('rows')} rows")
                return True
        else:
            print_fail(f"HTTP {resp.status_code}")
            return False
    except Exception as e:
        print_fail(f"Failed: {e}")
        return False


def test_monitoring_analytics() -> bool:
    print_header("TEST 4: Monitoring Analytics")
    try:
        resp = requests.get(f"{API_BASE}/monitoring/analytics", timeout=10)
        if resp.status_code != 200:
            print_fail(f"HTTP {resp.status_code}")
            return False

        data = resp.json()

        # CI-friendly: allow no-data state, otherwise require core analytics keys.
        if "error" in data:
            if data["error"] in {"No monitoring data found", "Monitoring data is empty"}:
                print_info(f"Analytics status: {data['error']} (acceptable for fresh runs)")
                return True
            print_fail(f"Unexpected analytics error: {data['error']}")
            return False

        required_keys = {"status", "engine", "overall", "backend_rollups", "model_rollups"}
        if not required_keys.issubset(data.keys()):
            missing = sorted(required_keys.difference(data.keys()))
            print_fail(f"Missing analytics keys: {missing}")
            return False

        overall = data.get("overall", {})
        if "total_requests" not in overall or "error_rate_percent" not in overall:
            print_fail("Invalid overall analytics shape")
            return False

        print_pass(f"Analytics OK (engine: {data.get('engine')}, requests: {overall.get('total_requests')})")
        return True
    except Exception as e:
        print_fail(f"Failed: {e}")
        return False


def test_monitoring_rag() -> bool:
    print_header("TEST 5: RAG Monitoring Dashboard")
    try:
        resp = requests.get(f"{API_BASE}/monitoring/rag", params={"recent_failures_limit": 10}, timeout=10)
        if resp.status_code != 200:
            print_fail(f"HTTP {resp.status_code}")
            return False

        data = resp.json()

        if "error" in data:
            if data["error"] in {
                "No monitoring data found",
                "Monitoring data is empty",
                "No RAGAS-scored monitoring data found",
            }:
                print_info(f"RAG monitoring status: {data['error']} (acceptable for fresh runs)")
                return True
            print_fail(f"Unexpected RAG monitoring error: {data['error']}")
            return False

        required_keys = {
            "status",
            "overall",
            "avg_faithfulness_by_backend",
            "avg_response_relevancy_by_mission",
            "context_count_vs_score_bands",
            "low_score_recent_failures",
            "retrieval_quality_trend",
            "ranking_inc_rag",
        }
        if not required_keys.issubset(data.keys()):
            missing = sorted(required_keys.difference(data.keys()))
            print_fail(f"Missing RAG analytics keys: {missing}")
            return False

        print_pass(
            f"RAG dashboard OK (scored requests: {data.get('overall', {}).get('scored_requests', 0)})"
        )
        return True
    except Exception as e:
        print_fail(f"Failed: {e}")
        return False


def test_promptfoo() -> bool:
    print_header("TEST 6: Promptfoo Evaluation")
    try:
        print_info("Running Promptfoo...")
        result = subprocess.run(
            ["npx", "-y", "promptfoo@latest", "eval", "-c", "promptfooconfig.yaml"],
            capture_output=True,
            text=True,
            timeout=180,
        )
        output = result.stdout + result.stderr
        if "Scan stopped" in output and "0 passed" in output:
            print_fail("Scan aborted (API/auth issue)")
            return False
        else:
            print_pass("Promptfoo evaluation completed")
            return True
    except subprocess.TimeoutExpired:
        print_fail("Timeout")
        return False
    except FileNotFoundError:
        print_fail("Promptfoo not installed")
        return False
    except Exception as e:
        print_fail(f"Failed: {e}")
        return False


def main():
    print("\n" + "*" * 60)
    print("*  NASA RAG Integration Smoke Test".center(58) + "  *")
    print("*" * 60)
    
    print_info(f"API: {API_BASE}")
    print_info(f"Waiting {SLEEP_BEFORE_REQUESTS}s...")
    time.sleep(SLEEP_BEFORE_REQUESTS)
    
    tests = {
        "health": test_health(),
        "chat": test_chat(),
        "monitoring": test_monitoring(),
        "monitoring_analytics": test_monitoring_analytics(),
        "monitoring_rag": test_monitoring_rag(),
        "promptfoo": test_promptfoo(),
    }
    
    print_header("SUMMARY")
    passed = sum(1 for v in tests.values() if v)
    total = len(tests)
    print_info(f"{passed}/{total} tests passed")
    
    for name, result in tests.items():
        sym = "✓" if result else "✗"
        status = "PASS" if result else "FAIL"
        print(f"  {sym} {name.upper()}: {status}")
    
    print()
    if passed == total:
        print_pass("All tests passed!")
        return 0
    else:
        print_fail(f"{total - passed} test(s) failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
