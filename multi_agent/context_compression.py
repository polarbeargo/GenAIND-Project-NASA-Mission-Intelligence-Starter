"""Context compression: dedup near-duplicate chunks and cap total context tokens.

Pipeline applied after retrieval but before generation:
  1. Remove near-duplicate chunks (Jaccard word-set similarity).
  2. Promote chunks whose metadata mission matches the active mission filter.
  3. Cap total character budget (~4 chars per token) to control prompt size.

Expected impact:
  - Fewer redundant sentences fed to the LLM → lower token cost.
  - Mission-relevant chunks surfaced first → improved factual precision.
  - Deterministic and library-free (no scipy / sklearn dependency).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Protocol, Tuple


class ContextCompressor(Protocol):
    """Interface for compressing retrieved chunks before generation."""

    def compress(
        self,
        contexts: List[str],
        metadatas: List[Dict],
        mission_filter: str | None,
    ) -> Tuple[List[str], List[Dict]]:
        """Return deduplicated, prioritised, and token-capped (contexts, metadatas)."""


@dataclass(frozen=True)
class CompressionConfig:
    """Tunable parameters for :class:`DeduplicatingCompressor`."""

    max_tokens: int = 2000
    """Rough token budget.  Every 4 characters ≈ 1 token (English prose estimate)."""

    similarity_threshold: float = 0.85
    """Jaccard word-set similarity at or above which a chunk is a near-duplicate."""

    mission_boost: bool = True
    """When ``True``, chunks whose metadata ``mission`` matches the active filter
    are sorted to the front before the token cap is applied."""


class DeduplicatingCompressor:
    """Removes near-duplicates, applies mission-priority ordering, and caps tokens."""

    def __init__(self, config: CompressionConfig | None = None):
        self._config = config or CompressionConfig()

    def compress(
        self,
        contexts: List[str],
        metadatas: List[Dict],
        mission_filter: str | None,
    ) -> Tuple[List[str], List[Dict]]:
        if not contexts:
            return contexts, list(metadatas)

        # Ensure the metadata list is parallel to contexts.
        metas: List[Dict] = list(metadatas) if metadatas else []
        if len(metas) < len(contexts):
            metas.extend([{}] * (len(contexts) - len(metas)))

        deduped_c, deduped_m = self._deduplicate(contexts, metas)

        if self._config.mission_boost and mission_filter:
            deduped_c, deduped_m = self._sort_by_mission(deduped_c, deduped_m, mission_filter)

        return self._apply_token_cap(deduped_c, deduped_m)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _deduplicate(
        self, contexts: List[str], metadatas: List[Dict]
    ) -> Tuple[List[str], List[Dict]]:
        threshold = self._config.similarity_threshold
        kept_c: List[str] = []
        kept_m: List[Dict] = []
        kept_sets: List[frozenset] = []

        for ctx, meta in zip(contexts, metadatas):
            words: frozenset = frozenset((ctx or "").lower().split())
            if not words:
                continue
            if any(_jaccard(words, existing) >= threshold for existing in kept_sets):
                continue
            kept_c.append(ctx)
            kept_m.append(meta)
            kept_sets.append(words)

        return kept_c, kept_m

    def _sort_by_mission(
        self,
        contexts: List[str],
        metadatas: List[Dict],
        mission_filter: str,
    ) -> Tuple[List[str], List[Dict]]:
        mission_key = mission_filter.strip().lower().replace(" ", "_")
        paired = list(zip(contexts, metadatas))
        # Stable sort keeps original order within each tier.
        paired.sort(
            key=lambda pair: 0 if str(pair[1].get("mission", "")).lower() == mission_key else 1
        )
        if not paired:
            return contexts, metadatas
        out_c, out_m = zip(*paired)
        return list(out_c), list(out_m)

    def _apply_token_cap(
        self, contexts: List[str], metadatas: List[Dict]
    ) -> Tuple[List[str], List[Dict]]:
        max_chars = self._config.max_tokens * 4  # ~4 chars per token
        total = 0
        kept_c: List[str] = []
        kept_m: List[Dict] = []

        for ctx, meta in zip(contexts, metadatas):
            chunk_chars = len(ctx or "")
            # Always keep the first chunk such that the context is never empty.
            if kept_c and total + chunk_chars > max_chars:
                break
            kept_c.append(ctx)
            kept_m.append(meta)
            total += chunk_chars

        return kept_c, kept_m


def _jaccard(a: frozenset, b: frozenset) -> float:
    """Jaccard similarity between two word-sets."""
    union = len(a | b)
    return len(a & b) / union if union else 1.0
