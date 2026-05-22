#!/usr/bin/env python3
"""
NASA RAG Chat with RAGAS Evaluation Integration

Enhanced version of the simple RAG chat that includes real-time evaluation
and feedback collection for continuous improvement.
"""

import streamlit as st
import os
import json
import time
from urllib import error, request as urllib_request

from env_utils import load_project_env
from openai_config import (
    get_openai_api_key,
    get_openai_chat_model,
    get_openai_chat_model_options,
)
import rag_client
import llm_client
import ragas_evaluator

from typing import Dict, List, Optional

load_project_env(__file__)

_EVAL_POLL_INTERVAL_SECONDS = 1.0
_EVAL_PENDING_TIMEOUT_SECONDS = 90.0

st.set_page_config(
    page_title="NASA RAG Chat with Evaluation",
    page_icon="🚀",
    layout="wide"
)

@st.cache_data(ttl=30)
def discover_chroma_backends() -> Dict[str, Dict[str, str]]:
    """Discover available ChromaDB backends in the project directory"""

    return rag_client.discover_chroma_backends()

def _normalize_query(question: str) -> str:
    """Normalize question for cache key matching."""
    return " ".join(question.lower().split())

def _get_client_cache_key(question: str, backend: str, n_docs: int, model: str) -> str:
    """Generate deterministic cache key for client-side question cache."""
    import hashlib
    normalized_q = _normalize_query(question)
    cache_str = f"{normalized_q}|{backend}|{n_docs}|{model}"
    return hashlib.md5(cache_str.encode("utf-8")).hexdigest()

def _get_cached_response(question: str, backend: str, n_docs: int, model: str) -> Dict | None:
    """Check client-side cache for identical question in same session (thread-safe via Streamlit session_state)."""
    cache_key = _get_client_cache_key(question, backend, n_docs, model)
    cached_item = st.session_state.question_response_cache.get(cache_key)
    if cached_item and isinstance(cached_item, dict):
        return cached_item
    return None

def _set_cached_response(question: str, backend: str, n_docs: int, model: str, response: Dict) -> None:
    """Store response in client-side cache (thread-safe via Streamlit session_state)."""
    cache_key = _get_client_cache_key(question, backend, n_docs, model)
    # Only cache successful responses
    if not response.get("error"):
        st.session_state.question_response_cache[cache_key] = {
            "response": response.get("answer", ""),
            "contexts": response.get("contexts", []),
            "latency_ms": response.get("latency_ms", 0.0),
            "judge": response.get("judge", {}),
            "backend": response.get("backend", ""),
            "cached": True,
        }

def call_chat_api(
    api_base_url: str,
    payload: Dict,
    timeout_seconds: float = 20.0,
    retries: int = 2,
    retry_backoff_seconds: float = 0.35,
) -> Dict:
    """Send one chat turn to FastAPI /chat with bounded retry for transient failures."""
    endpoint = f"{api_base_url.rstrip('/')}/chat"
    body = json.dumps(payload).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    for attempt in range(retries + 1):
        req = urllib_request.Request(endpoint, data=body, headers=headers, method="POST")
        try:
            with urllib_request.urlopen(req, timeout=timeout_seconds) as response:
                data = response.read().decode("utf-8")
                return json.loads(data) if data else {}
        except error.HTTPError as http_error:
            details = ""
            try:
                details = http_error.read().decode("utf-8")
            except Exception:
                details = str(http_error)
            return {
                "error": f"API error {http_error.code}: {details or str(http_error)}",
                "status_code": http_error.code,
            }
        except (error.URLError, TimeoutError) as network_error:
            if attempt >= retries:
                return {"error": f"Network error calling /chat: {network_error}"}
            time.sleep(retry_backoff_seconds * (attempt + 1))
        except json.JSONDecodeError as decode_error:
            return {"error": f"Invalid JSON from /chat: {decode_error}"}
        except Exception as request_error:
            return {"error": f"Unexpected API call failure: {request_error}"}

    return {"error": "Unknown failure calling /chat"}


def call_evaluation_job_api(
    api_base_url: str,
    job_id: str,
    timeout_seconds: float = 2.0,
) -> Dict:
    """Fetch one async evaluation job result from FastAPI."""
    endpoint = f"{api_base_url.rstrip('/')}/evaluation/{job_id}"
    headers = {
        "Accept": "application/json",
    }
    req = urllib_request.Request(endpoint, headers=headers, method="GET")
    try:
        with urllib_request.urlopen(req, timeout=timeout_seconds) as response:
            data = response.read().decode("utf-8")
            return json.loads(data) if data else {}
    except Exception:
        return {}


def run_local_chat_turn(
    prompt: str,
    selected_backend: Dict[str, str],
    n_docs: int,
    model_choice: str,
    enable_evaluation: bool,
    conversation_history: List[Dict[str, str]],
) -> Dict:
    """Legacy local path: retrieve + generate + optional sync RAGAS."""
    started = time.perf_counter()
    backend_name = f"{selected_backend['directory']}:{selected_backend['collection_name']}"

    # Keep local mode aligned with legacy behavior where CHROMA_OPENAI_API_KEY can be used.
    openai_key = get_openai_api_key(include_chroma_fallback=True)
    if not openai_key:
        return {
            "error": "OPENAI_API_KEY is not configured for local mode",
            "answer": "Error: OPENAI_API_KEY is not configured for local mode",
            "contexts": [],
            "evaluation": {},
            "judge": {},
            "latency_ms": (time.perf_counter() - started) * 1000.0,
            "backend": backend_name,
        }

    collection, success, error_message = rag_client.initialize_rag_system(
        selected_backend["directory"],
        selected_backend["collection_name"],
    )
    if not success or collection is None:
        return {
            "error": f"Local RAG init failed: {error_message}",
            "answer": f"Error: Local RAG init failed: {error_message}",
            "contexts": [],
            "evaluation": {},
            "judge": {},
            "latency_ms": (time.perf_counter() - started) * 1000.0,
            "backend": backend_name,
        }

    retrieval_results = rag_client.retrieve_documents(
        collection=collection,
        query=prompt,
        n_results=n_docs,
        mission_filter=None,
        chroma_dir=selected_backend["directory"],
    )

    contexts_list = retrieval_results["documents"][0] if retrieval_results and retrieval_results.get("documents") else []
    metadatas = retrieval_results["metadatas"][0] if retrieval_results and retrieval_results.get("metadatas") else []
    context_text = rag_client.format_context(contexts_list, metadatas) if contexts_list else ""

    answer = llm_client.generate_response(
        openai_key=openai_key,
        user_message=prompt,
        context=context_text,
        conversation_history=conversation_history,
        model=model_choice,
    )

    evaluation = {}
    if enable_evaluation:
        evaluation = ragas_evaluator.evaluate_response_quality(prompt, answer, contexts_list)

    return {
        "answer": answer,
        "contexts": contexts_list,
        "evaluation": evaluation,
        "judge": {
            "passed": True,
            "source": "local",
            "rationale": "Legacy local mode: judge disabled in Streamlit local path.",
        },
        "latency_ms": (time.perf_counter() - started) * 1000.0,
        "backend": backend_name,
    }


def _normalize_history_for_api(messages: List[Dict]) -> List[Dict[str, str]]:
    """Drop UI-only metadata before sending conversation history to /chat."""
    normalized: List[Dict[str, str]] = []
    for message in messages or []:
        if not isinstance(message, dict):
            continue
        role = message.get("role")
        content = message.get("content")
        if role in {"user", "assistant", "system"} and isinstance(content, str) and content.strip():
            normalized.append({"role": role, "content": content})
    return normalized

def display_evaluation_metrics(scores: Dict[str, float]):
    """Display evaluation metrics in the sidebar"""
    if scores.get("status") == "skipped_no_contexts":
        st.sidebar.info("Evaluation skipped: no retrieved contexts for this turn.")
        return
    if scores.get("status") == "pending_or_unavailable":
        st.sidebar.info("Evaluation is pending or unavailable for this turn.")
        return
    if str(scores.get("status", "")).lower() == "pending":
        st.sidebar.info("Evaluation is running in background. Metrics will appear shortly.")
        return

    if "error" in scores:
        if scores["error"] == "No contexts available for evaluation":
            st.sidebar.info("Evaluation skipped: no retrieved contexts for this turn.")
        else:
            st.sidebar.error(f"Evaluation Error: {scores['error']}")
        return
    
    st.sidebar.subheader("📊 Response Quality")
    
    for metric_name, score in scores.items():
        if metric_name in {"status", "source", "job_id", "question"}:
            continue
        if metric_name.endswith("_ms") or "submitted_at" in metric_name:
            continue
        if isinstance(score, (int, float)):
            normalized_score = max(0.0, min(float(score), 1.0))

            if normalized_score >= 0.8:
                color = "green"
            elif normalized_score >= 0.6:
                color = "orange"
            else:
                color = "red"

            st.sidebar.metric(
                label=metric_name.replace('_', ' ').title(),
                value=f"{score:.3f}",
                delta=None,
            )

            st.sidebar.progress(normalized_score)

def main():
    st.title("🚀 NASA Space Mission Chat with Evaluation")
    st.markdown("Chat with AI about NASA space missions with real-time quality evaluation")
    
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "current_backend" not in st.session_state:
        st.session_state.current_backend = None
    if "last_evaluation" not in st.session_state:
        st.session_state.last_evaluation = None
    if "last_contexts" not in st.session_state:
        st.session_state.last_contexts = []
    if "session_id" not in st.session_state:
        st.session_state.session_id = os.urandom(8).hex()
    if "question_response_cache" not in st.session_state:
        # Client-side cache: {cache_key: {"response": str, "contexts": list, "latency_ms": float, "cached": True}}
        st.session_state.question_response_cache = {}
    if "eval_poll_placeholder" not in st.session_state:
        st.session_state.eval_poll_placeholder = None
    if "eval_poll_text" not in st.session_state:
        st.session_state.eval_poll_text = None
    
    with st.sidebar:
        st.header("🔧 Configuration")

        st.subheader("🌐 API Settings")
        execution_mode = st.selectbox(
            "Execution Mode",
            options=["API (/chat)", "Local (legacy direct)"] ,
            index=0,
            help="Use Local mode to restore pre-change behavior (direct llm_client + ragas_evaluator).",
        )
        api_base_url = st.text_input(
            "API Base URL",
            value=os.getenv("API_BASE_URL", "http://localhost:8000"),
            help="FastAPI server base URL used by Streamlit for /chat requests",
        ).strip() or "http://localhost:8000"
        api_timeout_seconds = st.slider("API timeout (seconds)", 5, 60, 25)
        fallback_to_local = st.checkbox(
            "Fallback to Local on API error/timeout",
            value=True,
            help="If API mode fails, retry the same question through local legacy path.",
        )

        with st.spinner("Discovering ChromaDB backends..."):
            available_backends = discover_chroma_backends()
        
        if not available_backends:
            st.error("No ChromaDB backends found!")
            st.info(
                "Please run the embedding pipeline first:\n"
                "`uv run python embedding_pipeline.py --openai-key YOUR_KEY --data-path ./data_text`"
            )
            st.stop()
        
        st.subheader("📊 ChromaDB Backend")
        backend_options = {k: v["display_name"] for k, v in available_backends.items()}
        
        selected_backend_key = st.selectbox(
            "Select Document Collection",
            options=list(backend_options.keys()),
            format_func=lambda x: backend_options[x],
            help="Choose which document collection to use for retrieval"
        )
        
        selected_backend = available_backends[selected_backend_key]
        
        model_choice = st.selectbox(
            "OpenAI Model",
            options=get_openai_chat_model_options(),
            index=get_openai_chat_model_options().index(get_openai_chat_model()),
            help="Choose the OpenAI model for responses"
        )
        
        st.subheader("🔍 Retrieval Settings")
        n_docs = st.slider("Documents to retrieve", 1, 10, 3)
        
        st.subheader("📊 Evaluation Settings")
        enable_evaluation = st.checkbox("Enable RAGAS Evaluation", value=True)
        
        if (st.session_state.current_backend != selected_backend_key):
            st.session_state.current_backend = selected_backend_key

    # Efficient async evaluation polling with auto-rerun on status change
    if enable_evaluation and execution_mode == "API (/chat)":
        current_eval = st.session_state.last_evaluation
        if isinstance(current_eval, dict):
            pending_status = str(current_eval.get("status", "")).lower() == "pending"
            job_id = str(current_eval.get("job_id", "")).strip()
            if pending_status and job_id:
                submitted_at_ms = current_eval.get("submitted_at_ms")
                elapsed_seconds = 0.0
                if isinstance(submitted_at_ms, (int, float)):
                    elapsed_seconds = max(0.0, (time.time() * 1000.0 - float(submitted_at_ms)) / 1000.0)
                
                with st.empty().container():
                    if elapsed_seconds < _EVAL_PENDING_TIMEOUT_SECONDS:
                        # Poll with reasonable timeout (don't truncate it too much)
                        poll_timeout = min(2.0, max(0.5, float(api_timeout_seconds) * 0.25))
                        job_payload = call_evaluation_job_api(
                            api_base_url=api_base_url,
                            job_id=job_id,
                            timeout_seconds=poll_timeout,
                        )
                        job_result = job_payload.get("result") if isinstance(job_payload, dict) else None
                        
                        # Update state if result found, trigger rerun if status changed
                        status_changed = False
                        if isinstance(job_result, dict):
                            result_status = str(job_result.get("status", "")).lower()
                            if result_status in {"completed", "done", "success", "ok", "failed", "error", "timeout"}:
                                st.session_state.last_evaluation = job_result
                                status_changed = True
                            elif any(
                                metric_key in job_result
                                for metric_key in {"response_relevancy", "faithfulness", "bleu_score", "rouge_score"}
                            ):
                                st.session_state.last_evaluation = job_result
                                status_changed = True
                        
                        # Show progress with actual poll feedback
                        progress = min(100, int((elapsed_seconds / _EVAL_PENDING_TIMEOUT_SECONDS) * 100))
                        poll_status = "✓ polled" if job_result else "⏳ polling..."
                        st.progress(progress / 100.0, text=f"Evaluation {poll_status} ({progress}%)")
                        
                        # Trigger rerun if status changed (completion/error detected)
                        if status_changed:
                            time.sleep(0.2)  # Brief pause to let backend settle
                            st.rerun()
                        else:
                            # Show info about continued polling
                            st.info(f"Job {job_id}: Waiting for async evaluation to complete... (elapsed: {elapsed_seconds:.1f}s)")
                    else:
                        st.session_state.last_evaluation = {
                            "status": "pending_or_unavailable",
                            "error": "Evaluation is still pending after timeout window",
                        }
                        st.warning("⏱️ Evaluation timeout (90s) — marking as unavailable")
    
    if st.session_state.last_evaluation and enable_evaluation:
        display_evaluation_metrics(st.session_state.last_evaluation)
    
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
            if message.get("role") == "assistant" and isinstance(message.get("meta"), dict):
                meta = message["meta"]
                latency_ms = meta.get("latency_ms")
                backend = meta.get("backend", "unknown")
                is_cached = meta.get("cached", False)
                cache_indicator = " [📦 CACHED]" if is_cached else ""
                judge = meta.get("judge") if isinstance(meta.get("judge"), dict) else {}
                judge_source = judge.get("source", "unknown")
                judge_passed = judge.get("passed")
                latency_text = f"{float(latency_ms):.1f} ms" if isinstance(latency_ms, (int, float)) else "n/a"
                judge_text = "pass" if judge_passed is True else "review" if judge_passed is False else "n/a"
                st.caption(
                    f"Latency: {latency_text}{cache_indicator} | Backend: {backend} | Judge: {judge_text} ({judge_source})"
                )
    
    if prompt := st.chat_input("Ask about NASA space missions..."):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)
        
        with st.chat_message("assistant"):
            with st.spinner("Generating answer..."):
                conversation_history = st.session_state.messages[:-1]
                conversation_history_api = _normalize_history_for_api(conversation_history)

                if execution_mode == "Local (legacy direct)":
                    result = run_local_chat_turn(
                        prompt=prompt,
                        selected_backend=selected_backend,
                        n_docs=n_docs,
                        model_choice=model_choice,
                        enable_evaluation=bool(enable_evaluation),
                        conversation_history=conversation_history_api,
                    )
                else:
                    # Check client-side cache first (instant response for identical questions in session)
                    backend_key = f"{selected_backend['directory']}:{selected_backend['collection_name']}"
                    cached_result = _get_cached_response(prompt, backend_key, n_docs, model_choice)
                    
                    if cached_result:
                        # Instant cached response - mark cache hit for observability
                        result = {
                            "answer": cached_result["response"],
                            "contexts": cached_result["contexts"],
                            "latency_ms": cached_result.get("latency_ms", 0.0),
                            "backend": cached_result.get("backend", backend_key),
                            "judge": cached_result.get("judge", {}),
                            "cached_from_session": True,  # Flag for monitoring
                            "evaluation": {},
                        }
                    else:
                        # API call with full payload
                        payload = {
                            "question": prompt,
                            "chroma_dir": selected_backend["directory"],
                            "collection_name": selected_backend["collection_name"],
                            "n_results": n_docs,
                            "model": model_choice,
                            "evaluate": bool(enable_evaluation),
                            "conversation_history": conversation_history_api,
                            "session_id": st.session_state.session_id,
                        }
                        result = call_chat_api(
                            api_base_url=api_base_url,
                            payload=payload,
                            timeout_seconds=float(api_timeout_seconds),
                        )
                        
                        # Cache successful API responses for session reuse
                        if not result.get("error"):
                            _set_cached_response(prompt, backend_key, n_docs, model_choice, result)

                    error_text = str(result.get("error", ""))
                    status_code = result.get("status_code")
                    retryable_http_error = isinstance(status_code, int) and status_code >= 500
                    retryable_network_error = bool(error_text) and (
                        "network error" in error_text.lower() or "timed out" in error_text.lower()
                    )
                    timeout_or_retryable_error = retryable_http_error or retryable_network_error or (
                        isinstance(result.get("answer"), str)
                        and "timed out" in result.get("answer", "").lower()
                    )
                    if timeout_or_retryable_error and fallback_to_local and not result.get("cached_from_session"):
                        st.info("API path degraded. Retrying with Local legacy mode...")
                        result = run_local_chat_turn(
                            prompt=prompt,
                            selected_backend=selected_backend,
                            n_docs=n_docs,
                            model_choice=model_choice,
                            enable_evaluation=bool(enable_evaluation),
                            conversation_history=conversation_history_api,
                        )

                if result.get("error"):
                    response = f"Error: {result['error']}"
                    contexts_list = []
                    st.session_state.last_evaluation = {"error": result["error"]}
                    response_meta = {
                        "latency_ms": result.get("latency_ms"),
                        "backend": result.get("backend", "n/a"),
                        "judge": result.get("judge") if isinstance(result.get("judge"), dict) else {},
                        "cached": False,
                    }
                else:
                    response = str(result.get("answer", "I could not generate a response."))
                    contexts_list = result.get("contexts") or []
                    st.session_state.last_contexts = contexts_list
                    is_session_cached = result.get("cached_from_session", False)
                    response_meta = {
                        "latency_ms": result.get("latency_ms"),
                        "backend": result.get("backend", "unknown"),
                        "judge": result.get("judge") if isinstance(result.get("judge"), dict) else {},
                        "cached": is_session_cached,
                    }

                    evaluation_scores = result.get("evaluation", {})
                    if enable_evaluation:
                        if evaluation_scores:
                            st.session_state.last_evaluation = evaluation_scores
                        elif contexts_list:
                            st.session_state.last_evaluation = {"status": "pending_or_unavailable"}
                        else:
                            st.session_state.last_evaluation = {"status": "skipped_no_contexts"}
                    else:
                        st.session_state.last_evaluation = None

                    judge_result = response_meta.get("judge")
                    is_cached = response_meta.get("cached", False)
                    cache_indicator = " [📦 CACHED]" if is_cached else ""
                    
                    if isinstance(judge_result, dict) and judge_result:
                        source = judge_result.get("source", "unknown")
                        passed = judge_result.get("passed", False)
                        confidence = "pass" if passed else "review"
                        latency_val = response_meta.get("latency_ms")
                        latency_text = (
                            f"{float(latency_val):.1f} ms" if isinstance(latency_val, (int, float)) else "n/a"
                        )
                        backend_value = response_meta.get("backend", "unknown")
                        st.caption(
                            f"Latency: {latency_text}{cache_indicator} | Backend: {backend_value} | Judge: {confidence} ({source})"
                        )

                st.markdown(response)
        
        st.session_state.messages.append({"role": "assistant", "content": response, "meta": response_meta})
        st.rerun()


if __name__ == "__main__":
    main()
