#!/usr/bin/env python3
"""FastAPI server for NASA RAG + telemetry + monitoring."""

from __future__ import annotations

import logging
import os
import time
from contextlib import asynccontextmanager
from functools import lru_cache
from typing import Dict, List, Optional, Any

from env_utils import load_project_env
from fastapi import FastAPI, HTTPException, status, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

import rag_client
from openai_config import get_openai_api_key, get_openai_chat_model
from evidently_monitor import EvidentlyMonitor
from observability import init_telemetry, telemetry_status
from multi_agent import ChatWorkflowInput, MultiAgentChatWorkflow, WorkflowError

try:
    from security import (
        PromptInjectionDetector,
        SensitiveInfoFilter,
        OutputValidator,
        ResourceLimitEnforcer,
        VectorSecurityValidator,
        SecurityLevel,
        SecurityViolation,
        SecurityAuditor,
    )
except ImportError:
    PromptInjectionDetector = None
    SensitiveInfoFilter = None
    OutputValidator = None
    ResourceLimitEnforcer = None
    VectorSecurityValidator = None
    SecurityLevel = None
    SecurityViolation = Exception
    SecurityAuditor = None

load_project_env(__file__)

logger = logging.getLogger(__name__)

class CacheStats:
    """Track cache performance metrics for monitoring."""

    def __init__(self):
        self.hits = 0
        self.misses = 0
        self.init_times: List[float] = []

    def record_hit(self):
        self.hits += 1

    def record_miss(self, duration_ms: float):
        self.misses += 1
        self.init_times.append(duration_ms)
        if len(self.init_times) > 100:
            self.init_times = self.init_times[-100:]

    @property
    def hit_rate(self) -> float:
        total = self.hits + self.misses
        return (self.hits / total * 100) if total > 0 else 0.0

    @property
    def avg_init_ms(self) -> float:
        return sum(self.init_times) / len(self.init_times) if self.init_times else 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate_percent": round(self.hit_rate, 2),
            "avg_init_ms": round(self.avg_init_ms, 2),
            "total_requests": self.hits + self.misses,
        }


cache_stats = CacheStats()


@lru_cache(maxsize=16)  # Increased from 8 for multi-backend scenarios
def _cached_rag_init(chroma_dir: str, collection_name: str):
    """Cache RAG collection initialization with performance tracking."""
    init_start = time.perf_counter()
    result = rag_client.initialize_rag_system(chroma_dir, collection_name)
    duration_ms = (time.perf_counter() - init_start) * 1000
    cache_stats.record_miss(duration_ms)
    return result


def _get_cached_rag_init(chroma_dir: str, collection_name: str):
    """Wrapper to track cache hits/misses separately."""
    cache_info_before = _cached_rag_init.cache_info()
    collection, success, error = _cached_rag_init(chroma_dir, collection_name)
    cache_info_after = _cached_rag_init.cache_info()
    if cache_info_after.hits > cache_info_before.hits:
        cache_stats.record_hit()
    return collection, success, error


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle: startup (pre-warm cache) and shutdown."""
    logger.info("Pre-warming RAG collection cache...")
    backends_to_warm = [
        ("./chroma_db", "nasa_space_missions_test"),
        ("./chroma_db_openai", "nasa_space_missions_text"),
    ]
    for chroma_dir, collection_name in backends_to_warm:
        try:
            _cached_rag_init(chroma_dir, collection_name)
            logger.info(f"  ✓ Warmed: {chroma_dir}/{collection_name}")
        except Exception as e:
            logger.warning(f"  - Skip (optional): {chroma_dir} - {str(e)[:50]}")
    logger.info(f"Cache ready: {cache_stats.to_dict()}")
    yield
    logger.info("Shutting down NASA RAG API")


app = FastAPI(title="NASA Mission Intelligence API", version="1.0.0", lifespan=lifespan)
tracer = init_telemetry(app, service_name="nasa-mission-intelligence-api")
monitor = EvidentlyMonitor()

# Initialize security controls (LLM10: Resource limiting)
resource_limiter = ResourceLimitEnforcer(
    max_input_tokens=2000,
    max_output_tokens=1000,
    max_queries_per_minute=10,
    max_embedding_batch=100,
) if ResourceLimitEnforcer else None

# Jailbreak keywords (LLM07: System prompt protection)
JAILBREAK_KEYWORDS = [
    "system prompt", "system message", "original instructions",
    "developer mode", "admin mode", "bypass", "jailbreak",
    "ignore previous", "disregard", "forget", "override",
]

chat_workflow = MultiAgentChatWorkflow(
    get_collection_fn=_get_cached_rag_init,
    logger=logger,
    jailbreak_keywords=JAILBREAK_KEYWORDS,
    resource_limiter=resource_limiter,
    prompt_injection_detector=PromptInjectionDetector,
    vector_security_validator=VectorSecurityValidator,
    output_validator=OutputValidator,
    sensitive_info_filter=SensitiveInfoFilter,
    security_violation=SecurityViolation,
    security_auditor=SecurityAuditor,
    security_level=SecurityLevel,
)

@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    response.headers["Content-Security-Policy"] = "default-src 'self'"
    return response

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("ALLOWED_ORIGINS", "localhost:3000,localhost:8000").split(","),
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type"],
)


class ChatRequest(BaseModel):
    question: str = Field(..., min_length=1)
    chroma_dir: str = "./chroma_db_openai"
    collection_name: str = "nasa_space_missions_text"
    n_results: int = Field(default=3, ge=1, le=10)
    mission_filter: Optional[str] = None
    model: str = Field(default_factory=get_openai_chat_model)
    evaluate: bool = True
    conversation_history: List[Dict[str, str]] = Field(default_factory=list)


class ChatResponse(BaseModel):
    answer: str
    contexts: List[str]
    evaluation: Dict[str, float | str]
    latency_ms: float
    backend: str


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/tracing/status")
def tracing_status() -> Dict[str, Any]:
    """Return unified tracing configuration and availability status."""
    return telemetry_status()


@app.post("/chat", response_model=ChatResponse)
def chat(request: ChatRequest, http_request: Request) -> ChatResponse:
    """RAG chat endpoint with comprehensive OWASP LLM security controls.
    
    Implements:
    - LLM01: Prompt Injection Detection
    - LLM02: Sensitive Information Filtering
    - LLM05: Output Validation
    - LLM07: System Prompt Protection
    - LLM08: Vector Security Validation
    - LLM10: Rate Limiting & Resource Enforcement
    """
    openai_key = get_openai_api_key(include_chroma_fallback=False)
    if not openai_key:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="OPENAI_API_KEY is not configured",
        )

    backend_name = f"{request.chroma_dir}:{request.collection_name}"
    started = time.perf_counter()
    error_msg = None
    client_ip = http_request.client.host if http_request.client else "unknown"

    with tracer.start_as_current_span("nasa.rag.chat") as span:
        span.set_attribute("model", request.model)
        span.set_attribute("n_results", request.n_results)
        span.set_attribute("backend", backend_name)

        workflow_input = ChatWorkflowInput(
            question=request.question,
            chroma_dir=request.chroma_dir,
            collection_name=request.collection_name,
            n_results=request.n_results,
            mission_filter=request.mission_filter,
            model=request.model,
            evaluate=request.evaluate,
            conversation_history=request.conversation_history,
            client_ip=client_ip,
        )

        try:
            workflow_result = chat_workflow.run(
                workflow_input=workflow_input,
                openai_key=openai_key,
            )

            latency_ms = (time.perf_counter() - started) * 1000.0
            span.set_attribute("latency_ms", latency_ms)
            span.set_attribute("context_count", len(workflow_result.contexts))
            span.set_attribute("error", False)

            monitor.log_interaction(
                question=request.question,
                answer=workflow_result.answer,
                model=request.model,
                backend=backend_name,
                context_count=len(workflow_result.contexts),
                mission=request.mission_filter,
                evaluation=workflow_result.evaluation if isinstance(workflow_result.evaluation, dict) else None,
                error=False,
                latency_ms=latency_ms,
            )

            return ChatResponse(
                answer=workflow_result.answer,
                contexts=workflow_result.contexts,
                evaluation=workflow_result.evaluation,
                latency_ms=latency_ms,
                backend=backend_name,
            )

        except WorkflowError as error:
            raise HTTPException(status_code=error.status_code, detail=error.detail)
        except HTTPException:
            raise
        except Exception as error:
            error_msg = str(error)
            logger.error(f"Unexpected error in /chat: {error_msg}")
            latency_ms = (time.perf_counter() - started) * 1000.0
            span.set_attribute("error", True)
            span.set_attribute("error_message", error_msg[:100])
            monitor.log_interaction(
                question=request.question,
                answer="[ERROR] Request failed",
                model=request.model,
                backend=backend_name,
                context_count=0,
                mission=request.mission_filter,
                evaluation={"error": error_msg[:200]},
                error=True,
                latency_ms=latency_ms,
            )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Internal server error: {error_msg[:100]}",
            )


@app.get("/monitoring/report")
def monitoring_report(reference_rows: int = 100) -> Dict[str, str]:
    """Generate Evidently drift report from interaction logs."""
    return monitor.build_drift_report(reference_rows=reference_rows)


@app.get("/monitoring/analytics")
def monitoring_analytics() -> Dict[str, Any]:
    """Return latency/error rollups from monitoring logs."""
    return monitor.get_analytics_summary()


@app.get("/monitoring/rag")
def monitoring_rag(recent_failures_limit: int = 20) -> Dict[str, Any]:
    """Return RAG-specific rollups built from RAGAS scores and retrieval metadata."""
    return monitor.get_rag_dashboard_summary(recent_failures_limit=recent_failures_limit)


@app.get("/monitoring/rag/report")
def monitoring_rag_report(reference_rows: int = 100) -> Dict[str, str]:
    """Generate an Evidently HTML report for RAG-specific score trends."""
    return monitor.build_rag_report(reference_rows=reference_rows)


@app.get("/collections/clear-cache")
def clear_cache_endpoint() -> Dict[str, str]:
    """Clear the LRU cache for RAG collection initialization."""
    _cached_rag_init.cache_clear()
    logger.info("Cache cleared by request")
    return {"status": "cache cleared"}


@app.get("/cache/stats")
def cache_stats_endpoint() -> Dict[str, Any]:
    """Get cache performance statistics and LRU info."""
    stats = cache_stats.to_dict()
    lru_info = _cached_rag_init.cache_info()
    stats["lru_info"] = {
        "hits": lru_info.hits,
        "misses": lru_info.misses,
        "maxsize": lru_info.maxsize,
        "currsize": lru_info.currsize,
    }
    return stats


@app.post("/collections/warm-cache")
def warm_cache_endpoint(backends: Optional[List[Dict[str, str]]] = None) -> Dict[str, Any]:
    """Pre-warm cache for backends (bulk initialization)."""
    if backends is None:
        backends = [
            {"chroma_dir": "./chroma_db", "collection_name": "nasa_space_missions_test"},
            {"chroma_dir": "./chroma_db_openai", "collection_name": "nasa_space_missions_text"},
        ]
    
    results = {}
    for backend in backends:
        chroma_dir = backend.get("chroma_dir")
        collection_name = backend.get("collection_name")
        if not chroma_dir or not collection_name:
            continue
        try:
            _cached_rag_init(chroma_dir, collection_name)
            results[f"{chroma_dir}:{collection_name}"] = "warmed"
        except Exception as e:
            results[f"{chroma_dir}:{collection_name}"] = f"error: {str(e)[:50]}"
    
    return {
        "status": "warmup complete",
        "backends_warmed": results,
        "cache_stats": cache_stats.to_dict(),
    }
