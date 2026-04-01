"""Compatibility wrappers for legacy tracing imports.

Telemetry initialization now lives in observability.py to guarantee a single
deterministic provider pipeline.
"""

from __future__ import annotations

import os

from observability import telemetry_status


def configure_phoenix_tracing(
    project_name: str | None = None,
    endpoint: str | None = None,
) -> bool:
    """Store Phoenix env preferences used by unified observability init.

    Returns True if the provided configuration values are valid strings.
    """
    if endpoint is not None:
        os.environ["PHOENIX_ENDPOINT"] = endpoint
    if project_name is not None:
        os.environ["PHOENIX_PROJECT_NAME"] = project_name
    return True


def phoenix_status() -> dict:
    """Return tracing status from the unified observability pipeline."""
    status = telemetry_status()
    status.update(
        {
            "phoenix_endpoint": os.getenv("PHOENIX_ENDPOINT", ""),
            "phoenix_project": os.getenv("PHOENIX_PROJECT_NAME", "nasa-mission-intelligence"),
        }
    )
    return status
