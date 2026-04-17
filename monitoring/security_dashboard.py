"""
Real-time Security Monitoring Dashboard for NASA Mission Intelligence API.

Tracks OWASP LLM security events and provides monitoring endpoints:
- Real-time event stream
- Aggregated statistics by severity/type
- Threshold-based alerting
- Historical trends
"""

import json
import logging
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Any

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class Severity(str, Enum):
    """Security event severity levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

class EventType(str, Enum):
    """Security event types used for detection, validation, and monitoring."""

    INJECTION_ATTEMPT = "injection_attempt"
    INFO_LEAK_DETECTED = "info_leak_detected"
    POISONED_RESULTS = "poisoned_results"
    OUTPUT_VALIDATION_CRITICAL = "output_validation_critical"
    OUTPUT_VALIDATION_WARNING = "output_validation_warning"
    JAILBREAK_ATTEMPT = "jailbreak_attempt"
    SYSTEM_PROMPT_LEAK = "system_prompt_leak"
    VECTOR_VALIDATION_FAILED = "vector_validation_failed"
    RATE_LIMIT_EXCEEDED = "rate_limit_exceeded"
    TOKEN_LIMIT_EXCEEDED = "token_limit_exceeded"
    COST_THRESHOLD_EXCEEDED = "cost_threshold_exceeded"
    SECURITY_VIOLATION = "security_violation"
    API_ERROR = "api_error"

@dataclass
class SecurityEvent:
    """Represents a security event."""
    timestamp: datetime
    event_type: EventType
    severity: Severity
    user_id: Optional[str] = None
    ip_address: Optional[str] = None
    details: Optional[Dict[str, Any]] = None
    resolved: bool = False
    resolution_time: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        data = asdict(self)
        data["timestamp"] = self.timestamp.isoformat()
        data["event_type"] = self.event_type.value
        data["severity"] = self.severity.value
        if self.resolution_time:
            data["resolution_time"] = self.resolution_time.isoformat()
        return data


class SecurityDashboard:
    """Real-time security monitoring dashboard."""
    
    ALERT_THRESHOLDS = {
        "injections_per_minute": 5,
        "rate_limit_per_hour": 100,
        "critical_events_per_hour": 3,
        "info_leaks_per_day": 2,
    }
    
    def __init__(self, max_events: int = 10000, log_file: Optional[Path] = None):
        """Initialize security dashboard.
        
        Args:
            max_events: Maximum events to keep in memory
            log_file: Optional file path for persistent logging
        """
        self.max_events = max_events
        self.events: deque = deque(maxlen=max_events)
        self.log_file = log_file or Path(__file__).parent / "security_events.log"
        
        self.event_counts: Dict[str, int] = defaultdict(int)
        self.severity_counts: Dict[Severity, int] = defaultdict(int)
        self.user_attack_map: Dict[str, int] = defaultdict(int)
        self.ip_attack_map: Dict[str, int] = defaultdict(int)
        
        self.active_threats: List[SecurityEvent] = []
        self.threat_lock = threading.Lock()
        
        self.last_alert_time: Dict[str, datetime] = {}
        self.recent_alerts: deque = deque(maxlen=100)
        
        self._ensure_log_file()
        
        logger.info(f"SecurityDashboard initialized (max_events={max_events})")
    
    def _ensure_log_file(self) -> None:
        """Ensure log file and parent directory exist."""
        self.log_file.parent.mkdir(parents=True, exist_ok=True)
        if not self.log_file.exists():
            self.log_file.touch()
            logger.info(f"Created security events log: {self.log_file}")
    
    def log_event(
        self,
        event_type: str,
        severity: str,
        user_id: Optional[str] = None,
        ip_address: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> SecurityEvent:
        """Log a security event.
        
        Args:
            event_type: Type of event (see EventType enum)
            severity: Event severity (low, medium, high, critical)
            user_id: User identifier if applicable
            ip_address: IP address of requester if applicable
            details: Additional event details
            
        Returns:
            SecurityEvent object
        """
        try:
            event_type_enum = EventType(event_type)
        except ValueError:
            event_type_enum = EventType.SECURITY_VIOLATION
        
        try:
            severity_enum = Severity(severity)
        except ValueError:
            severity_enum = Severity.MEDIUM
        
        event = SecurityEvent(
            timestamp=datetime.now(),
            event_type=event_type_enum,
            severity=severity_enum,
            user_id=user_id,
            ip_address=ip_address,
            details=details or {},
        )
        
        self.events.append(event)
        
        self.event_counts[event_type] += 1
        self.severity_counts[severity_enum] += 1
        if user_id:
            self.user_attack_map[user_id] += 1
        if ip_address:
            self.ip_attack_map[ip_address] += 1
        
        with self.threat_lock:
            if severity_enum in [Severity.HIGH, Severity.CRITICAL]:
                self.active_threats.append(event)
        
        self._write_to_log(event)
        self._check_alert_thresholds(event)
        
        logger.warning(
            f"[{severity_enum.value.upper()}] {event_type}: user={user_id}, ip={ip_address}"
        )
        
        return event
    
    def _write_to_log(self, event: SecurityEvent) -> None:
        """Write event to log file."""
        try:
            with open(self.log_file, "a") as f:
                f.write(json.dumps(event.to_dict()) + "\n")
        except Exception as e:
            logger.error(f"Failed to write to security log: {e}")
    
    def _check_alert_thresholds(self, event: SecurityEvent) -> None:
        """Check if event triggers alerting thresholds."""
        now = datetime.now()
        one_minute_ago = now - timedelta(minutes=1)
        one_hour_ago = now - timedelta(hours=1)
        
        if event.event_type == EventType.INJECTION_ATTEMPT:
            minute_injections = sum(
                1 for e in self.events
                if e.event_type == EventType.INJECTION_ATTEMPT
                and e.timestamp > one_minute_ago
            )
            if minute_injections >= self.ALERT_THRESHOLDS["injections_per_minute"]:
                self._raise_alert(
                    "INJECTION_SPIKE",
                    f"High injection attempt rate: {minute_injections} in last minute"
                )
        
        if event.severity == Severity.CRITICAL:
            hour_criticals = sum(
                1 for e in self.events
                if e.severity == Severity.CRITICAL
                and e.timestamp > one_hour_ago
            )
            if hour_criticals >= self.ALERT_THRESHOLDS["critical_events_per_hour"]:
                self._raise_alert(
                    "CRITICAL_THRESHOLD",
                    f"High critical events: {hour_criticals} in last hour"
                )
    
    def _raise_alert(self, alert_type: str, message: str) -> None:
        """Raise a security alert."""
        alert = {
            "timestamp": datetime.now().isoformat(),
            "type": alert_type,
            "message": message,
            "severity": "high",
        }
        self.recent_alerts.append(alert)
        logger.critical(f"🚨 ALERT: {alert_type} - {message}")
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get aggregated security statistics.
        
        Returns:
            Dictionary with event statistics
        """
        now = datetime.now()
        one_hour_ago = now - timedelta(hours=1)
        one_day_ago = now - timedelta(days=1)
        
        hour_events = [e for e in self.events if e.timestamp > one_hour_ago]
        day_events = [e for e in self.events if e.timestamp > one_day_ago]
        
        return {
            "total_events": len(self.events),
            "events_last_hour": len(hour_events),
            "events_last_day": len(day_events),
            "event_types": dict(self.event_counts),
            "severity_distribution": {
                s.value: count for s, count in self.severity_counts.items()
            },
            "critical_events_last_hour": sum(
                1 for e in hour_events if e.severity == Severity.CRITICAL
            ),
            "high_severity_events_last_hour": sum(
                1 for e in hour_events if e.severity == Severity.HIGH
            ),
            "top_attackers": dict(
                sorted(self.ip_attack_map.items(), key=lambda x: x[1], reverse=True)[:10]
            ),
            "active_threats": len(self.active_threats),
            "dashboard_timestamp": now.isoformat(),
        }
    
    def get_alerts(self) -> List[Dict[str, Any]]:
        """Get recent security alerts.
        
        Returns:
            List of recent alerts
        """
        return list(self.recent_alerts)
    
    def get_events(self, limit: int = 50, severity: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get recent security events.
        
        Args:
            limit: Maximum number of events to return
            severity: Filter by severity level
            
        Returns:
            List of recent events
        """
        events_list = list(self.events)
        events_list.reverse()
        
        if severity:
            events_list = [e for e in events_list if e.severity.value == severity]
        
        return [e.to_dict() for e in events_list[:limit]]
    
    def get_threat_summary(self) -> Dict[str, Any]:
        """Get summary of active threats.
        
        Returns:
            Dictionary with threat information
        """
        with self.threat_lock:
            critical_threats = [t for t in self.active_threats if t.severity == Severity.CRITICAL]
            high_threats = [t for t in self.active_threats if t.severity == Severity.HIGH]
        
        return {
            "active_threat_count": len(self.active_threats),
            "critical_threats": len(critical_threats),
            "high_severity_threats": len(high_threats),
            "threat_types": list(set(t.event_type.value for t in self.active_threats)),
            "most_recent_threat": (
                self.active_threats[-1].to_dict() if self.active_threats else None
            ),
        }
    
    def get_vulnerability_coverage(self) -> Dict[str, Any]:
        """Get coverage of OWASP LLM Top 10 protections.
        
        Returns:
            Dictionary showing which vulnerabilities have been detected
        """
        coverage_map = {
            "LLM01_PromptInjection": any(e.event_type == EventType.INJECTION_ATTEMPT for e in self.events),
            "LLM02_SensitiveInfoDisclosure": any(e.event_type == EventType.INFO_LEAK_DETECTED for e in self.events),
            "LLM04_DataPoisoning": any(e.event_type == EventType.POISONED_RESULTS for e in self.events),
            "LLM05_ImproperOutput": any(
                e.event_type in [EventType.OUTPUT_VALIDATION_CRITICAL, EventType.OUTPUT_VALIDATION_WARNING]
                for e in self.events
            ),
            "LLM07_SystemPromptLeakage": any(
                e.event_type in [EventType.JAILBREAK_ATTEMPT, EventType.SYSTEM_PROMPT_LEAK]
                for e in self.events
            ),
            "LLM08_VectorWeakness": any(e.event_type == EventType.VECTOR_VALIDATION_FAILED for e in self.events),
            "LLM10_UnboundedConsumption": any(
                e.event_type in [
                    EventType.RATE_LIMIT_EXCEEDED,
                    EventType.TOKEN_LIMIT_EXCEEDED,
                    EventType.COST_THRESHOLD_EXCEEDED,
                ]
                for e in self.events
            ),
        }
        
        return coverage_map
    
    def export_events(self, file_path: Path) -> int:
        """Export events to CSV file.
        
        Args:
            file_path: Path to export file
            
        Returns:
            Number of events exported
        """
        try:
            import csv
            
            with open(file_path, "w", newline="") as f:
                if not self.events:
                    return 0
                
                writer = csv.DictWriter(f, fieldnames=self.events[0].to_dict().keys())
                writer.writeheader()
                
                for event in self.events:
                    writer.writerow(event.to_dict())
            
            logger.info(f"Exported {len(self.events)} events to {file_path}")
            return len(self.events)
        except Exception as e:
            logger.error(f"Failed to export events: {e}")
            return 0
    
    def reset(self) -> None:
        """Reset all statistics and event history."""
        self.events.clear()
        self.event_counts.clear()
        self.severity_counts.clear()
        self.user_attack_map.clear()
        self.ip_attack_map.clear()
        
        with self.threat_lock:
            self.active_threats.clear()
        
        logger.info("SecurityDashboard reset")

_dashboard_instance: Optional[SecurityDashboard] = None


def get_dashboard() -> SecurityDashboard:
    """Get or create global dashboard instance."""
    global _dashboard_instance
    if _dashboard_instance is None:
        _dashboard_instance = SecurityDashboard()
    return _dashboard_instance
