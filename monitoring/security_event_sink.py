"""Security event sink adapters for operational monitoring surfaces."""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from security.llm_security import SecurityEvent, SecurityEventSink

logger = logging.getLogger(__name__)


class DashboardSecurityEventSink(SecurityEventSink):
    """Route workflow security events into the in-process dashboard."""

    def __init__(self, dashboard):
        self._dashboard = dashboard

    def emit(self, event: SecurityEvent) -> None:
        try:
            self._dashboard.log_event(
                event_type=event.event_type,
                severity=event.severity,
                user_id=event.user_id,
                ip_address=event.user_id,
                details=event.details,
            )
        except Exception as error:
            logger.warning("Security dashboard sink failed: %s", error)

    def log_security_event(
        self,
        event_type: str,
        severity,
        user_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.emit(
            SecurityEvent(
                event_type=event_type,
                severity=getattr(severity, "value", str(severity)).strip().lower() or "medium",
                user_id=user_id,
                details=details or {},
            )
        )