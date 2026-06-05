"""Regression tests for Streamlit-side cache key semantics in chat.py.

These tests protect against stale cache hits when the same user question
appears in different conversation histories.
"""

from __future__ import annotations

from types import SimpleNamespace

import chat


def _setup_fake_session_state() -> None:
    """Install a minimal session_state shape used by cache helpers."""
    chat.st.session_state = SimpleNamespace(question_response_cache={})


def test_client_cache_key_changes_with_conversation_history() -> None:
    """Same question with different history must produce different keys."""
    history_a = [{"role": "user", "content": "What failed on Apollo 13?"}]
    history_b = [{"role": "user", "content": "Summarize Apollo 11 launch."}]

    key_a = chat._get_client_cache_key(
        question="What was Apollo 13?",
        backend="./chroma_db_openai:nasa_space_missions_text",
        n_docs=3,
        model="gpt-4o-mini",
        mission_filter=None,
        conversation_history=history_a,
        enable_evaluation=True,
    )
    key_b = chat._get_client_cache_key(
        question="What was Apollo 13?",
        backend="./chroma_db_openai:nasa_space_missions_text",
        n_docs=3,
        model="gpt-4o-mini",
        mission_filter=None,
        conversation_history=history_b,
        enable_evaluation=True,
    )

    assert key_a != key_b


def test_streamlit_cache_lookup_is_history_aware() -> None:
    """A cached response for one history must not leak to another history."""
    _setup_fake_session_state()

    backend = "./chroma_db_openai:nasa_space_missions_text"
    response_payload = {
        "answer": "Apollo 13 was NASA's seventh crewed Apollo mission.",
        "contexts": ["Apollo 13 launched on April 11, 1970."],
        "latency_ms": 41.2,
        "judge": {"passed": True, "source": "llm"},
        "evaluation": {"status": "completed", "faithfulness": 0.91},
        "backend": backend,
    }

    history_used_for_write = [
        {"role": "user", "content": "We were discussing oxygen tank failures."}
    ]
    different_history = [
        {"role": "user", "content": "Now we switched to Apollo 11 timeline."}
    ]

    chat._set_cached_response(
        question="What was Apollo 13?",
        backend=backend,
        n_docs=3,
        model="gpt-4o-mini",
        mission_filter=None,
        conversation_history=history_used_for_write,
        enable_evaluation=True,
        response=response_payload,
    )

    matched = chat._get_cached_response(
        question="What was Apollo 13?",
        backend=backend,
        n_docs=3,
        model="gpt-4o-mini",
        mission_filter=None,
        conversation_history=history_used_for_write,
        enable_evaluation=True,
    )
    mismatched = chat._get_cached_response(
        question="What was Apollo 13?",
        backend=backend,
        n_docs=3,
        model="gpt-4o-mini",
        mission_filter=None,
        conversation_history=different_history,
        enable_evaluation=True,
    )

    assert matched is not None
    assert matched["response"] == response_payload["answer"]
    assert matched["evaluation"]["status"] == "completed"
    assert mismatched is None


def test_streamlit_cache_separates_evaluation_mode() -> None:
    """Same question and history must not share cache entries across eval modes."""
    _setup_fake_session_state()

    backend = "./chroma_db_openai:nasa_space_missions_text"
    history = [{"role": "user", "content": "Keep the Apollo 13 context active."}]

    eval_on_payload = {
        "answer": "Apollo 13 was NASA's seventh crewed Apollo mission.",
        "contexts": ["Apollo 13 launched on April 11, 1970."],
        "latency_ms": 39.8,
        "judge": {"passed": True, "source": "llm"},
        "evaluation": {"status": "completed", "faithfulness": 0.93},
        "backend": backend,
    }
    eval_off_payload = {
        "answer": "Apollo 13 was NASA's seventh crewed Apollo mission.",
        "contexts": ["Apollo 13 launched on April 11, 1970."],
        "latency_ms": 33.4,
        "judge": {"passed": True, "source": "llm"},
        "evaluation": {},
        "backend": backend,
    }

    chat._set_cached_response(
        question="What was Apollo 13?",
        backend=backend,
        n_docs=3,
        model="gpt-4o-mini",
        mission_filter=None,
        conversation_history=history,
        enable_evaluation=True,
        response=eval_on_payload,
    )
    chat._set_cached_response(
        question="What was Apollo 13?",
        backend=backend,
        n_docs=3,
        model="gpt-4o-mini",
        mission_filter=None,
        conversation_history=history,
        enable_evaluation=False,
        response=eval_off_payload,
    )

    cached_eval_on = chat._get_cached_response(
        question="What was Apollo 13?",
        backend=backend,
        n_docs=3,
        model="gpt-4o-mini",
        mission_filter=None,
        conversation_history=history,
        enable_evaluation=True,
    )
    cached_eval_off = chat._get_cached_response(
        question="What was Apollo 13?",
        backend=backend,
        n_docs=3,
        model="gpt-4o-mini",
        mission_filter=None,
        conversation_history=history,
        enable_evaluation=False,
    )

    assert cached_eval_on is not None
    assert cached_eval_off is not None
    assert cached_eval_on is not cached_eval_off
    assert cached_eval_on["evaluation"]["status"] == "completed"
    assert cached_eval_off["evaluation"] == {}
