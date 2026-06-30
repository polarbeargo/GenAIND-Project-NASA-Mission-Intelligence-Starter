"""Shared helpers for posting Phoenix span annotations safely.

These helpers are intentionally dependency-light and fail-open so telemetry
never interrupts request processing or async worker completion paths.
"""

from __future__ import annotations

import math
import os
from functools import lru_cache
from numbers import Number
from typing import Any, Dict, Iterable, Optional

from opentelemetry.instrumentation.utils import suppress_instrumentation

try:
    from phoenix.client import Client as PhoenixClient
except Exception:  # pragma: no cover - optional runtime dependency
    PhoenixClient = None


def phoenix_base_url() -> str:
    configured = (os.getenv("PHOENIX_BASE_URL") or "").strip()
    if configured:
        return configured.rstrip("/")

    endpoint = (os.getenv("PHOENIX_ENDPOINT") or "http://localhost:6006/v1/traces").strip()
    if endpoint.endswith("/v1/traces"):
        return endpoint[: -len("/v1/traces")].rstrip("/")
    return endpoint.rstrip("/")


@lru_cache(maxsize=8)
def _get_phoenix_client(base_url: str):
    if PhoenixClient is None:
        return None
    return PhoenixClient(base_url=base_url)


def collect_annotation_scores(
    *payloads: Any,
    passthrough_keys: Optional[Iterable[str]] = None,
) -> Dict[str, float]:
    """Collect numeric annotation values from dict payloads.

    - By default, only values in [0.0, 1.0] are retained (score-like metrics).
    - Keys in ``passthrough_keys`` are retained for any finite float value
      (for example ``latency_ms``).
    """
    scores: Dict[str, float] = {}
    passthrough = {str(item) for item in (passthrough_keys or ())}

    for payload in payloads:
        if not isinstance(payload, dict):
            continue
        for key, value in payload.items():
            if isinstance(value, bool) or not isinstance(value, Number):
                continue
            score = float(value)
            if not math.isfinite(score):
                continue
            key_str = str(key)
            if key_str in passthrough or (0.0 <= score <= 1.0):
                scores[key_str] = score
    return scores


def post_span_annotations(
    span_id: str,
    scores: Dict[str, float],
    *,
    base_url: Optional[str] = None,
    logger=None,
) -> None:
    """Best-effort annotation post; never raises to caller."""
    if not span_id or not scores or PhoenixClient is None:
        return

    try:
        client = _get_phoenix_client((base_url or phoenix_base_url()).rstrip("/"))
        if client is None:
            return
        # Avoid tracing the Phoenix annotation transport itself; otherwise
        # requests auto-instrumentation can create extra transport spans that
        # show up in Phoenix as "unknown kind" alongside the real chat span.
        with suppress_instrumentation():
            for name, score in scores.items():
                client.spans.add_span_annotation(
                    span_id=span_id,
                    annotation_name=name,
                    annotator_kind="CODE",
                    score=float(score),
                    sync=False,
                )
    except Exception as error:  # pragma: no cover - telemetry must never break serving
        if logger is not None:
            logger.warning("Failed to post Phoenix annotations for span %s: %s", span_id, error)
