"""Unified OpenTelemetry setup for FastAPI, requests, OpenAI, and Phoenix/OTLP."""

from __future__ import annotations

import logging
import os
from typing import Any, Dict

from fastapi import FastAPI
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased

try:
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor  # type: ignore[import-not-found]
except Exception:
    FastAPIInstrumentor = None

try:
    from opentelemetry.instrumentation.requests import RequestsInstrumentor  # type: ignore[import-not-found]
except Exception:
    RequestsInstrumentor = None

try:
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
except Exception:  # pragma: no cover - optional at runtime
    OTLPSpanExporter = None

try:
    from openinference.instrumentation.openai import OpenAIInstrumentor
except Exception:
    OpenAIInstrumentor = None

try:
    from openinference.instrumentation import TraceConfig
except Exception:
    TraceConfig = None

# phoenix.otel is imported lazily inside init_telemetry() only when PHOENIX_ENDPOINT
# is configured.  A module-level import triggers Phoenix's SQLite/Alembic setup as a
# side effect even when no Phoenix endpoint is in use (e.g. during tests).
import importlib.util as _importlib_util
_PHOENIX_AVAILABLE = _importlib_util.find_spec("phoenix") is not None


logger = logging.getLogger(__name__)

_TELEMETRY_STATE: Dict[str, Any] = {
    "initialized": False,
    "service_name": None,
    "exporter": "none",
    "endpoint": None,
    "project": None,
    "openinference_openai_available": OpenAIInstrumentor is not None,
    "phoenix_available": _PHOENIX_AVAILABLE,
    "requests_instrumented": False,
    "fastapi_instrumented": False,
    "openai_instrumented": False,
    "openai_hide_embedding_vectors": True,
}


def _as_bool(value: str, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _float_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        val = float(raw)
    except ValueError:
        return default
    return max(0.0, min(1.0, val))


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def init_telemetry(app: FastAPI, service_name: str = "nasa-rag-api"):
    """Configure one deterministic and efficient tracing pipeline.

    Exporter precedence:
    1) PHOENIX_ENDPOINT (Phoenix OTLP)
    2) OTEL_EXPORTER_OTLP_ENDPOINT (generic OTLP)
    3) Console exporter when TELEMETRY_CONSOLE_FALLBACK=true
    4) No exporter (lowest overhead)
    """
    if _TELEMETRY_STATE["initialized"]:
        return trace.get_tracer(_TELEMETRY_STATE["service_name"] or service_name)

    sample_ratio = _float_env("OTEL_TRACES_SAMPLE_RATE", 1.0)
    resource = Resource.create({"service.name": service_name})

    phoenix_endpoint = os.getenv("PHOENIX_ENDPOINT", "").strip()
    otlp_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "").strip()
    project_name = os.getenv("PHOENIX_PROJECT_NAME", "nasa-mission-intelligence").strip()
    chosen_endpoint = phoenix_endpoint or otlp_endpoint
    console_fallback = _as_bool(os.getenv("TELEMETRY_CONSOLE_FALLBACK", "false"), default=False)

    if phoenix_endpoint and _PHOENIX_AVAILABLE:
        try:
            from phoenix.otel import register as _phoenix_register
        except Exception:
            _phoenix_register = None
    else:
        _phoenix_register = None

    if _phoenix_register is not None:
        provider = _phoenix_register(
            endpoint=phoenix_endpoint,
            project_name=project_name,
            batch=True,
            set_global_tracer_provider=True,
            verbose=False,
            auto_instrument=False,
            resource=resource,
            sampler=ParentBased(TraceIdRatioBased(sample_ratio)),
        )
        _TELEMETRY_STATE["exporter"] = "phoenix"
        _TELEMETRY_STATE["endpoint"] = phoenix_endpoint
    elif chosen_endpoint and OTLPSpanExporter is not None:
        provider = TracerProvider(
            resource=resource,
            sampler=ParentBased(TraceIdRatioBased(sample_ratio)),
        )
        exporter = OTLPSpanExporter(endpoint=chosen_endpoint)
        _TELEMETRY_STATE["exporter"] = "otlp"
        _TELEMETRY_STATE["endpoint"] = chosen_endpoint
    elif console_fallback:
        provider = TracerProvider(
            resource=resource,
            sampler=ParentBased(TraceIdRatioBased(sample_ratio)),
        )
        exporter = ConsoleSpanExporter()
        _TELEMETRY_STATE["exporter"] = "console"
        _TELEMETRY_STATE["endpoint"] = None
    else:
        provider = TracerProvider(
            resource=resource,
            sampler=ParentBased(TraceIdRatioBased(sample_ratio)),
        )
        exporter = None
        _TELEMETRY_STATE["exporter"] = "none"
        _TELEMETRY_STATE["endpoint"] = None

    if _TELEMETRY_STATE["exporter"] in {"otlp", "console"} and exporter is not None:
        provider.add_span_processor(
            BatchSpanProcessor(
                exporter,
                max_queue_size=_int_env("OTEL_BSP_MAX_QUEUE_SIZE", 1024),
                max_export_batch_size=_int_env("OTEL_BSP_MAX_EXPORT_BATCH_SIZE", 256),
                schedule_delay_millis=_int_env("OTEL_BSP_SCHEDULE_DELAY_MS", 500),
                export_timeout_millis=_int_env("OTEL_BSP_EXPORT_TIMEOUT_MS", 30000),
            )
        )

    if _TELEMETRY_STATE["exporter"] != "phoenix":
        trace.set_tracer_provider(provider)

    if RequestsInstrumentor is not None:
        try:
            RequestsInstrumentor().instrument()
        except Exception:
            logger.debug("Requests instrumentation already active")
        _TELEMETRY_STATE["requests_instrumented"] = True

    if FastAPIInstrumentor is not None:
        try:
            FastAPIInstrumentor.instrument_app(app, tracer_provider=provider)
        except Exception:
            logger.debug("FastAPI instrumentation already active")
        _TELEMETRY_STATE["fastapi_instrumented"] = True

    if OpenAIInstrumentor is not None:
        try:
            hide_embedding_vectors = _as_bool(
                os.getenv("OTEL_OPENAI_HIDE_EMBEDDING_VECTORS", "true"),
                default=True,
            )
            if TraceConfig is not None:
                OpenAIInstrumentor().instrument(
                    config=TraceConfig(
                        hide_embedding_vectors=hide_embedding_vectors,
                        hide_embeddings_vectors=hide_embedding_vectors,
                    )
                )
            else:
                OpenAIInstrumentor().instrument()
            _TELEMETRY_STATE["openai_hide_embedding_vectors"] = hide_embedding_vectors
        except Exception:
            logger.debug("OpenAI instrumentation already active")
        _TELEMETRY_STATE["openai_instrumented"] = True

    _TELEMETRY_STATE["initialized"] = True
    _TELEMETRY_STATE["service_name"] = service_name
    _TELEMETRY_STATE["project"] = project_name

    return trace.get_tracer(service_name)


def telemetry_status() -> Dict[str, Any]:
    """Return effective telemetry configuration and feature availability."""
    return {
        "initialized": _TELEMETRY_STATE["initialized"],
        "service_name": _TELEMETRY_STATE["service_name"],
        "exporter": _TELEMETRY_STATE["exporter"],
        "endpoint": _TELEMETRY_STATE["endpoint"],
        "phoenix_project": _TELEMETRY_STATE["project"],
        "phoenix_available": _TELEMETRY_STATE["phoenix_available"],
        "openinference_openai_available": _TELEMETRY_STATE["openinference_openai_available"],
        "requests_instrumented": _TELEMETRY_STATE["requests_instrumented"],
        "fastapi_instrumented": _TELEMETRY_STATE["fastapi_instrumented"],
        "openai_instrumented": _TELEMETRY_STATE["openai_instrumented"],
        "openai_hide_embedding_vectors": _TELEMETRY_STATE["openai_hide_embedding_vectors"],
        "sample_rate": _float_env("OTEL_TRACES_SAMPLE_RATE", 1.0),
        "otlp_exporter_available": OTLPSpanExporter is not None,
    }
