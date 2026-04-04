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

import llm_client
import rag_client
import ragas_evaluator
from openai_config import get_openai_api_key, get_openai_chat_model
from evidently_monitor import EvidentlyMonitor
from observability import init_telemetry, telemetry_status

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

        try:
            # ====================
            # SECURITY: Jailbreak Detection (LLM07)
            # ====================
            if any(kw in request.question.lower() for kw in JAILBREAK_KEYWORDS):
                logger.warning(f"Jailbreak attempt from {client_ip}: {request.question[:50]}")
                if SecurityAuditor:
                    SecurityAuditor.log_security_event(
                        event_type="jailbreak_attempt",
                        severity=SecurityLevel.HIGH,
                        user_id=client_ip,
                        details={"question_sample": request.question[:100]}
                    )
                return ChatResponse(
                    answer="I'm designed to answer questions about NASA missions. Please ask about Apollo, Challenger, or Shuttle missions.",
                    contexts=[],
                    evaluation={},
                    latency_ms=(time.perf_counter() - started) * 1000,
                    backend=backend_name,
                )
            
            # ====================
            # SECURITY: Token & Rate Limits (LLM10)
            # ====================
            if resource_limiter:
                try:
                    resource_limiter.check_input_tokens(request.question)
                    resource_limiter.check_query_rate(client_ip)
                except SecurityViolation as e:
                    logger.warning(f"Resource limit exceeded: {e}")
                    if SecurityAuditor:
                        SecurityAuditor.log_security_event(
                            event_type="rate_limit_exceeded",
                            severity=SecurityLevel.MEDIUM,
                            user_id=client_ip,
                            details={"error": str(e)}
                        )
                    raise HTTPException(
                        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                        detail="Rate limit exceeded",
                    )
            
            # ====================
            # SECURITY: Prompt Injection Detection (LLM01)
            # ====================
            if PromptInjectionDetector:
                injection_check = PromptInjectionDetector.detect_injection(request.question)
                if injection_check:
                    logger.warning(f"Injection attempt from {client_ip}")
                    if SecurityAuditor:
                        SecurityAuditor.log_security_event(
                            event_type="injection_attempt",
                            severity=SecurityLevel.HIGH,
                            user_id=client_ip,
                        )
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="Invalid input detected",
                    )
            
            # ====================
            # SECURITY: Vector Security Validation (LLM08)
            # ====================
            if VectorSecurityValidator:
                try:
                    VectorSecurityValidator.validate_embedding_source(
                        request.collection_name,
                        request.chroma_dir
                    )
                except SecurityViolation as e:
                    logger.error(f"Vector validation failed: {e}")
                    raise HTTPException(
                        status_code=status.HTTP_403_FORBIDDEN,
                        detail="Invalid collection",
                    )
            
            # Use cached collection initialization with hit/miss tracking
            collection, success, error = _get_cached_rag_init(
                request.chroma_dir,
                request.collection_name,
            )
            if not success or collection is None:
                error_msg = f"Failed to initialize RAG: {error}"
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=error_msg,
                )

            docs_result = rag_client.retrieve_documents(
                collection,
                request.question,
                request.n_results,
                request.mission_filter,
                request.chroma_dir,
            )

            contexts: List[str] = []
            context_text = ""
            if docs_result and docs_result.get("documents"):
                contexts = docs_result["documents"][0]
                
                # SECURITY: Check vector results for poisoning (LLM08)
                if VectorSecurityValidator:
                    poisoning_check = VectorSecurityValidator.detect_poisoned_results(
                        docs_result["documents"][0],
                        docs_result.get("metadatas", [{}])[0] if docs_result.get("metadatas") else {},
                    )
                    if poisoning_check:
                        logger.warning(f"Potentially poisoned results detected")
                        if SecurityAuditor:
                            SecurityAuditor.log_security_event(
                                event_type="poisoned_results",
                                severity=SecurityLevel.MEDIUM,
                                details={"count": len(contexts)}
                            )
                
                context_text = rag_client.format_context(
                    docs_result["documents"][0],
                    docs_result["metadatas"][0],
                )

            # Call LLM (security checks done in llm_client.py)
            try:
                answer = llm_client.generate_response(
                    openai_key=openai_key,
                    user_message=request.question,
                    context=context_text,
                    conversation_history=request.conversation_history,
                    model=request.model,
                )
            except SecurityViolation as se:
                logger.error(f"Security violation in LLM call: {se}")
                if SecurityAuditor:
                    SecurityAuditor.log_security_event(
                        event_type="security_violation",
                        severity=SecurityLevel.HIGH,
                        user_id=client_ip,
                        details={"error": str(se)}
                    )
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Security validation failed",
                )
            except Exception as e:
                error_str = str(e)
                logger.error(f"LLM generation failed: {error_str}")
                
                if "401" in error_str or "invalid_api_key" in error_str.lower():
                    raise HTTPException(
                        status_code=status.HTTP_401_UNAUTHORIZED,
                        detail="Invalid OpenAI API key. Check OPENAI_API_KEY configuration.",
                    )
                elif "429" in error_str or "rate_limit" in error_str.lower():
                    raise HTTPException(
                        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                        detail="OpenAI rate limit exceeded. Please retry after a moment.",
                    )
                elif "503" in error_str or "unavailable" in error_str.lower():
                    raise HTTPException(
                        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                        detail="OpenAI service temporarily unavailable.",
                    )
                else:
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail=f"LLM generation error: {error_str[:100]}",
                    )

            # ====================
            # SECURITY: Output Validation (LLM05)
            # ====================
            if OutputValidator:
                validation = OutputValidator.validate_response(answer, contexts)
                
                if validation["severity"] == "critical":
                    logger.error(f"Critical output validation failure: {validation}")
                    if SecurityAuditor:
                        SecurityAuditor.log_security_event(
                            event_type="output_validation_critical",
                            severity=SecurityLevel.CRITICAL,
                            user_id=client_ip,
                        )
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail="Response validation failed",
                    )
                
                if validation["severity"] == "warning":
                    logger.warning(f"Output validation warnings: {validation['issues']}")
            
            # ====================
            # SECURITY: Sensitive Information Filtering (LLM02, LLM07)
            # ====================
            if SensitiveInfoFilter:
                answer = SensitiveInfoFilter.filter_response(answer, strict=True)
            
            evaluation: Dict[str, float | str] = {}
            if request.evaluate and contexts:
                try:
                    evaluation = ragas_evaluator.evaluate_response_quality(
                        question=request.question,
                        answer=answer,
                        contexts=contexts,
                    )
                except Exception as e:
                    logger.warning(f"Evaluation failed (non-fatal): {e}")
                    evaluation = {"error": "Evaluation unavailable"}

            latency_ms = (time.perf_counter() - started) * 1000.0
            span.set_attribute("latency_ms", latency_ms)
            span.set_attribute("context_count", len(contexts))
            span.set_attribute("error", False)

            monitor.log_interaction(
                question=request.question,
                answer=answer,
                model=request.model,
                backend=backend_name,
                context_count=len(contexts),
                mission=request.mission_filter,
                evaluation=evaluation if isinstance(evaluation, dict) else None,
                error=False,
                latency_ms=latency_ms,
            )

            return ChatResponse(
                answer=answer,
                contexts=contexts,
                evaluation=evaluation,
                latency_ms=latency_ms,
                backend=backend_name,
            )

        except HTTPException:
            raise
        except Exception as e:
            error_msg = str(e)
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
