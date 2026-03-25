"""Phoenix (Arize) observability setup for LLM/RAG tracing.
Phoenix traces are viewable at http://localhost:6006/projects after running:
    uv run python -m phoenix.server.main serve
"""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

try:
    from phoenix.otel import register as _phoenix_register
    PHOENIX_AVAILABLE = True
except ImportError:
    _phoenix_register = None
    PHOENIX_AVAILABLE = False

try:
    from openinference.instrumentation.openai import OpenAIInstrumentor
    OPENINFERENCE_OPENAI_AVAILABLE = True
except ImportError:
    OpenAIInstrumentor = None
    OPENINFERENCE_OPENAI_AVAILABLE = False


def configure_phoenix_tracing(
    project_name: str | None = None,
    endpoint: str | None = None,
) -> bool:
    """Register Phoenix as an OpenTelemetry trace exporter.

    Args:
        project_name: Phoenix project name (defaults to PHOENIX_PROJECT_NAME env
                      var, then "nasa-mission-intelligence").
        endpoint: Phoenix OTLP endpoint (defaults to PHOENIX_ENDPOINT env var,
                  then "http://localhost:6006/v1/traces").

    Returns:
        True if Phoenix tracing was successfully configured, False otherwise.
    """
    if not PHOENIX_AVAILABLE:
        logger.warning(
            "arize-phoenix-otel not installed — Phoenix tracing disabled. "
            "Install with: uv add arize-phoenix-otel"
        )
        return False

    resolved_endpoint = (
        endpoint
        or os.getenv("PHOENIX_ENDPOINT", "http://localhost:6006/v1/traces")
    )
    resolved_project = (
        project_name
        or os.getenv("PHOENIX_PROJECT_NAME", "nasa-mission-intelligence")
    )

    try:
        _phoenix_register(
            project_name=resolved_project,
            endpoint=resolved_endpoint,
        )
        logger.info(
            f"Phoenix tracing configured: project='{resolved_project}' "
            f"endpoint='{resolved_endpoint}'"
        )
    except Exception as exc:
        logger.warning(f"Phoenix registration failed (tracing disabled): {exc}")
        return False

    # Auto-instrument OpenAI calls with LLM-specific spans (prompt, completion,
    # token counts, model name) using OpenInference semantic conventions.
    if OPENINFERENCE_OPENAI_AVAILABLE:
        try:
            OpenAIInstrumentor().instrument()
            logger.info("OpenAI auto-instrumentation enabled via OpenInference")
        except Exception as exc:
            logger.warning(f"OpenAI instrumentation failed (non-fatal): {exc}")
    else:
        logger.warning(
            "openinference-instrumentation-openai not installed — "
            "OpenAI spans will lack LLM-specific attributes. "
            "Install with: uv add openinference-instrumentation-openai"
        )

    return True


def phoenix_status() -> dict:
    """Return the current Phoenix tracing configuration status."""
    return {
        "phoenix_available": PHOENIX_AVAILABLE,
        "openinference_openai_available": OPENINFERENCE_OPENAI_AVAILABLE,
        "phoenix_endpoint": os.getenv("PHOENIX_ENDPOINT", "http://localhost:6006/v1/traces"),
        "phoenix_project": os.getenv("PHOENIX_PROJECT_NAME", "nasa-mission-intelligence"),
    }
