"""Data models used by the multi-agent chat workflow."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


class WorkflowError(Exception):
    """Workflow-level exception with HTTP-friendly status/details."""

    def __init__(self, status_code: int, detail: str):
        self.status_code = status_code
        self.detail = detail
        super().__init__(detail)


@dataclass
class ChatWorkflowInput:
    """Normalized input passed into the multi-agent workflow."""

    question: str
    chroma_dir: str
    collection_name: str
    n_results: int
    mission_filter: Optional[str]
    model: str
    evaluate: bool
    judge_mode: str
    conversation_history: List[Dict[str, str]]
    client_ip: str
    trace_span_id: Optional[str] = None
    session_id: Optional[str] = None


@dataclass
class RetrievalResult:
    """Output of the retrieval worker."""

    contexts: List[str] = field(default_factory=list)
    metadatas: List[Dict] = field(default_factory=list)
    context_text: str = ""


@dataclass
class SafetyPreflightResult:
    """Output of the safety preflight worker."""

    blocked_response: Optional[str] = None


@dataclass
class ChatWorkflowResult:
    """Workflow result consumed by API layer and monitoring."""

    answer: str
    contexts: List[str]
    evaluation: Dict[str, Any]
    judge: Dict[str, Any] = field(default_factory=dict)
    blocked: bool = False
