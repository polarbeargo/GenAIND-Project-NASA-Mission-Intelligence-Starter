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

st.set_page_config(
    page_title="NASA RAG Chat with Evaluation",
    page_icon="🚀",
    layout="wide"
)

@st.cache_data(ttl=30)
def discover_chroma_backends() -> Dict[str, Dict[str, str]]:
    """Discover available ChromaDB backends in the project directory"""

    return rag_client.discover_chroma_backends()

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

    if "error" in scores:
        if scores["error"] == "No contexts available for evaluation":
            st.sidebar.info("Evaluation skipped: no retrieved contexts for this turn.")
        else:
            st.sidebar.error(f"Evaluation Error: {scores['error']}")
        return
    
    st.sidebar.subheader("📊 Response Quality")
    
    for metric_name, score in scores.items():
        if isinstance(score, (int, float)):
            if score >= 0.8:
                color = "green"
            elif score >= 0.6:
                color = "orange"
            else:
                color = "red"
            
            st.sidebar.metric(
                label=metric_name.replace('_', ' ').title(),
                value=f"{score:.3f}",
                delta=None
            )

            st.sidebar.progress(score)

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
    
    if st.session_state.last_evaluation and enable_evaluation:
        display_evaluation_metrics(st.session_state.last_evaluation)
    
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
            if message.get("role") == "assistant" and isinstance(message.get("meta"), dict):
                meta = message["meta"]
                latency_ms = meta.get("latency_ms")
                backend = meta.get("backend", "unknown")
                judge = meta.get("judge") if isinstance(meta.get("judge"), dict) else {}
                judge_source = judge.get("source", "unknown")
                judge_passed = judge.get("passed")
                latency_text = f"{float(latency_ms):.1f} ms" if isinstance(latency_ms, (int, float)) else "n/a"
                judge_text = "pass" if judge_passed is True else "review" if judge_passed is False else "n/a"
                st.caption(
                    f"Latency: {latency_text} | Backend: {backend} | Judge: {judge_text} ({judge_source})"
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
                    if timeout_or_retryable_error and fallback_to_local:
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
                    }
                else:
                    response = str(result.get("answer", "I could not generate a response."))
                    contexts_list = result.get("contexts") or []
                    st.session_state.last_contexts = contexts_list
                    response_meta = {
                        "latency_ms": result.get("latency_ms"),
                        "backend": result.get("backend", "unknown"),
                        "judge": result.get("judge") if isinstance(result.get("judge"), dict) else {},
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
                            f"Latency: {latency_text} | Backend: {backend_value} | Judge: {confidence} ({source})"
                        )

                st.markdown(response)
        
        st.session_state.messages.append({"role": "assistant", "content": response, "meta": response_meta})
        st.rerun()


if __name__ == "__main__":
    main()
