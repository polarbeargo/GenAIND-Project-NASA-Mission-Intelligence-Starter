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

import math
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

    use_optimized_dedup: bool = False
    """When ``True``, use the blocked/cached/short-circuit dedup path.
    Defaults to ``False`` so runtime behavior stays on the simpler baseline path
    unless explicitly enabled."""


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
        if not self._config.use_optimized_dedup:
            return self._deduplicate_naive(contexts, metadatas)

        threshold = self._config.similarity_threshold
        kept_c: List[str] = []
        kept_m: List[Dict] = []
        kept_sets: List[frozenset] = []
        # Block comparisons by token-set size to avoid full O(n^2) scans.
        kept_by_size: Dict[int, List[int]] = {}
        # Per-call cache to avoid recomputing token sets for repeated chunks.
        token_cache: Dict[str, frozenset] = {}

        for ctx, meta in zip(contexts, metadatas):
            key = (ctx or "").lower()
            words = token_cache.get(key)
            if words is None:
                words = frozenset(key.split())
                token_cache[key] = words
            if not words:
                continue

            min_size, max_size = _candidate_size_bounds(len(words), threshold)
            is_duplicate = False
            for candidate_size in range(min_size, max_size + 1):
                for idx in kept_by_size.get(candidate_size, []):
                    if _jaccard_meets_threshold(words, kept_sets[idx], threshold):
                        is_duplicate = True
                        break
                if is_duplicate:
                    break

            if is_duplicate:
                continue

            kept_c.append(ctx)
            kept_m.append(meta)
            kept_sets.append(words)
            kept_by_size.setdefault(len(words), []).append(len(kept_sets) - 1)

        return kept_c, kept_m

    def _deduplicate_naive(
        self, contexts: List[str], metadatas: List[Dict]
    ) -> Tuple[List[str], List[Dict]]:
        threshold = self._config.similarity_threshold
        kept_c: List[str] = []
        kept_m: List[Dict] = []
        kept_sets: List[frozenset] = []

        for ctx, meta in zip(contexts, metadatas):
            words = frozenset((ctx or "").lower().split())
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
    inter = len(a & b)
    union = len(a) + len(b) - inter
    return inter / union if union else 1.0


def _candidate_size_bounds(size: int, threshold: float) -> Tuple[int, int]:
    """Return feasible candidate set-size bounds for Jaccard >= threshold."""
    if size <= 0:
        return 0, 0
    if threshold <= 0.0:
        return 1, size * 2
    if threshold >= 1.0:
        return size, size
    lower = max(1, math.ceil(size * threshold))
    upper = max(lower, math.floor(size / threshold))
    return lower, upper


def _jaccard_meets_threshold(a: frozenset, b: frozenset, threshold: float) -> bool:
    """Fast check for Jaccard(a, b) >= threshold using size bounds + early exits."""
    if threshold <= 0.0:
        return True
    if threshold >= 1.0:
        return a == b

    len_a = len(a)
    len_b = len(b)
    if not len_a and not len_b:
        return True
    if not len_a or not len_b:
        return False

    min_len = min(len_a, len_b)
    max_len = max(len_a, len_b)
    # Hard upper bound: Jaccard <= min_len / max_len.
    if (min_len / max_len) < threshold:
        return False

    # Rearranged from: inter / (len_a + len_b - inter) >= threshold
    required_inter = math.ceil(threshold * (len_a + len_b) / (1.0 + threshold))
    if required_inter > min_len:
        return False

    smaller, larger = (a, b) if len_a <= len_b else (b, a)
    inter = 0
    processed = 0
    small_len = len(smaller)
    for token in smaller:
        processed += 1
        if token in larger:
            inter += 1
            if inter >= required_inter:
                return True
        # Even if all remaining tokens matched, cannot reach required_inter.
        if inter + (small_len - processed) < required_inter:
            return False

    return False
