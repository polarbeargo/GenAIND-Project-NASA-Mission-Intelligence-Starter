"""Retrieval depth policy interfaces and heuristic implementations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, Sequence

from multi_agent.models import ChatWorkflowInput


class RetrievalDepthPolicy(Protocol):
    """Interface for selecting retrieval depth per request."""

    def resolve_n_results(self, workflow_input: ChatWorkflowInput) -> int:
        """Return effective retrieval depth for this request."""


@dataclass(frozen=True)
class HeuristicRetrievalDepthConfig:
    """Configuration for keyword-based retrieval depth selection."""

    factoid_n_results: int = 2
    broad_n_results: int = 4
    factoid_starts: Sequence[str] = (
        "what",
        "when",
        "where",
        "who",
        "which",
        "is",
        "was",
        "were",
        "did",
        "does",
        "how many",
        "how long",
    )
    broad_markers: Sequence[str] = (
        "summarize",
        "summary",
        "overview",
        "compare",
        "comparison",
        "timeline",
        "lessons",
        "analyze",
        "analysis",
        "synthesize",
        "impact",
        "causes",
        "how did",
        "why did",
    )


class HeuristicRetrievalDepthPolicy:
    """Classifies question intent to choose a retrieval depth."""

    def __init__(self, config: HeuristicRetrievalDepthConfig | None = None):
        self._config = config or HeuristicRetrievalDepthConfig()

    @staticmethod
    def _normalize(text: str) -> str:
        return " ".join((text or "").strip().lower().split())

    def resolve_n_results(self, workflow_input: ChatWorkflowInput) -> int:
        normalized = self._normalize(workflow_input.question)

        if any(marker in normalized for marker in self._config.broad_markers):
            return max(1, int(self._config.broad_n_results))
        if normalized.startswith(tuple(self._config.factoid_starts)):
            return max(1, int(self._config.factoid_n_results))

        # Keep caller-provided depth when the heuristic is uncertain.
        return max(1, int(workflow_input.n_results))
