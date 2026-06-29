import os
from threading import Lock
import re

import chromadb
import logging
from chromadb.config import Settings
from chromadb.utils.embedding_functions import OpenAIEmbeddingFunction
from typing import Any, Dict, List, Optional
from pathlib import Path

from openai_config import get_openai_api_key, get_openai_base_url, get_openai_embedding_model

try:
    from security import VectorSecurityValidator, SecurityViolation
except ImportError:
    VectorSecurityValidator = None  # Graceful degradation
    SecurityViolation = Exception

logger = logging.getLogger(__name__)

_CHROMA_CLIENTS: Dict[str, chromadb.PersistentClient] = {}
_CHROMA_CLIENTS_LOCK = Lock()
_EMBEDDING_FUNCTIONS: Dict[tuple[str, str, str], OpenAIEmbeddingFunction] = {}
_EMBEDDING_FUNCTIONS_LOCK = Lock()
_CHROMA_CLIENT_CACHE_HITS = 0
_CHROMA_CLIENT_CACHE_MISSES = 0
_EMBEDDING_FN_CACHE_HITS = 0
_EMBEDDING_FN_CACHE_MISSES = 0
_TOKEN_PATTERN = re.compile(r"[a-z0-9]+")
_STOP_WORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "did",
    "do",
    "for",
    "from",
    "had",
    "has",
    "have",
    "how",
    "in",
    "is",
    "it",
    "mission",
    "of",
    "on",
    "or",
    "the",
    "to",
    "was",
    "were",
    "what",
    "when",
    "where",
    "who",
    "why",
}

_MISSION_ALIASES = {
    "apollo11": "apollo_11",
    "apollo_11": "apollo_11",
    "apollo 11": "apollo_11",
    "apollo-11": "apollo_11",
    "apollo13": "apollo_13",
    "apollo_13": "apollo_13",
    "apollo 13": "apollo_13",
    "apollo-13": "apollo_13",
    "challenger": "challenger",
    "sts-51l": "challenger",
    "sts_51l": "challenger",
    "sts 51l": "challenger",
}

_QUERY_ALIAS_EXPANSIONS = {
    "sts-51l": ["challenger", "space shuttle challenger", "mission 51l"],
    "sts 51l": ["challenger", "space shuttle challenger", "mission 51l"],
    "sts_51l": ["challenger", "space shuttle challenger", "mission 51l"],
    "apollo13": ["apollo 13", "apollo thirteen"],
    "apollo-13": ["apollo 13", "apollo thirteen"],
    "apollo11": ["apollo 11", "apollo eleven"],
    "apollo-11": ["apollo 11", "apollo eleven"],
    "cryo stir": ["cryogenic stir", "oxygen tank stir"],
    "oxygen tank": ["cryogenic oxygen tank", "o2 tank"],
    "o2 tank": ["oxygen tank", "cryogenic oxygen tank"],
}

_MISSION_CONTEXT_TERMS = {
    "apollo_11": ["apollo 11", "saturn v", "lunar landing", "command module"],
    "apollo_13": ["apollo 13", "oxygen tank", "command module", "lunar module"],
    "challenger": ["challenger", "sts-51l", "solid rocket booster", "launch decision"],
}


def _normalize_mission_filter(mission_filter: str) -> str:
    """Normalize user mission aliases to collection metadata keys."""
    raw = (mission_filter or "").strip().lower()
    if not raw:
        return ""
    if raw in _MISSION_ALIASES:
        return _MISSION_ALIASES[raw]
    # Generic fallback for known underscore style used in metadata.
    return raw.replace(" ", "_").replace("-", "_")


def _get_first_pass_multiplier() -> int:
    """Return candidate expansion factor for first-pass retrieval."""
    try:
        value = int(os.getenv("RETRIEVAL_FIRST_PASS_MULTIPLIER", "4"))
    except ValueError:
        value = 4
    return max(1, min(value, 8))


def _get_first_pass_max_candidates() -> int:
    """Return hard cap for first-pass candidate set size."""
    try:
        value = int(os.getenv("RETRIEVAL_FIRST_PASS_MAX_CANDIDATES", "24"))
    except ValueError:
        value = 24
    return max(1, min(value, 100))


def _hybrid_enabled() -> bool:
    return os.getenv("RETRIEVAL_HYBRID_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}


def _get_keyword_term_limit() -> int:
    try:
        value = int(os.getenv("RETRIEVAL_KEYWORD_TERM_LIMIT", "3"))
    except ValueError:
        value = 3
    return max(1, min(value, 8))


def _get_keyword_candidates_per_term() -> int:
    try:
        value = int(os.getenv("RETRIEVAL_KEYWORD_CANDIDATES_PER_TERM", "4"))
    except ValueError:
        value = 4
    return max(1, min(value, 16))


def _query_rewrite_enabled() -> bool:
    return os.getenv("RETRIEVAL_QUERY_REWRITE_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}


def _get_query_rewrite_max_expansion_terms() -> int:
    try:
        value = int(os.getenv("RETRIEVAL_QUERY_REWRITE_MAX_EXPANSION_TERMS", "10"))
    except ValueError:
        value = 10
    return max(2, min(value, 24))


def _tokenize_for_rerank(text: str) -> set[str]:
    """Tokenize text into lowercase alphanumeric terms for overlap scoring."""
    return set(_TOKEN_PATTERN.findall((text or "").lower()))


def _extract_keyword_terms(query: str) -> List[str]:
    terms: List[str] = []
    seen = set()
    for token in _TOKEN_PATTERN.findall((query or "").lower()):
        if len(token) < 3 or token in _STOP_WORDS:
            continue
        if token in seen:
            continue
        seen.add(token)
        terms.append(token)
    return terms


def _infer_mission_from_query(query: str) -> str:
    raw = (query or "").strip().lower()
    if not raw:
        return ""

    for alias, normalized in _MISSION_ALIASES.items():
        if alias and alias in raw:
            return normalized

    return ""


def _rewrite_query_for_retrieval(query: str, mission_filter: Optional[str]) -> str:
    """Rewrite query with bounded alias + mission-context expansion for recall.

    This function is deterministic and side-effect free so it is safe under
    concurrent request load.
    """
    base_query = (query or "").strip()
    if not base_query or not _query_rewrite_enabled():
        return base_query

    lower_query = base_query.lower()
    max_terms = _get_query_rewrite_max_expansion_terms()

    expansions: List[str] = []
    seen = set()

    def _append_terms(terms: List[str]) -> None:
        for term in terms:
            candidate = (term or "").strip().lower()
            if not candidate or candidate in seen:
                continue
            seen.add(candidate)
            expansions.append(term.strip())
            if len(expansions) >= max_terms:
                return

    for alias, alias_expansions in _QUERY_ALIAS_EXPANSIONS.items():
        if alias in lower_query:
            _append_terms(alias_expansions)
            if len(expansions) >= max_terms:
                break

    normalized_filter = _normalize_mission_filter(mission_filter or "") if mission_filter else ""
    inferred_mission = _infer_mission_from_query(lower_query)
    effective_mission = normalized_filter or inferred_mission
    if effective_mission in _MISSION_CONTEXT_TERMS and len(expansions) < max_terms:
        _append_terms(_MISSION_CONTEXT_TERMS[effective_mission])

    if not expansions:
        return base_query

    return f"{base_query} {' '.join(expansions[:max_terms])}".strip()


def _flatten_results(results: Dict[str, Any]) -> tuple[list[str], list[Dict[str, Any]], list[Any], list[Any]]:
    documents = list(((results.get("documents") or [[]])[0] or []))
    metadatas = list(((results.get("metadatas") or [[]])[0] or []))
    distances = list(((results.get("distances") or [[]])[0] or []))
    ids = list(((results.get("ids") or [[]])[0] or []))
    return documents, metadatas, distances, ids


def _merge_candidate_results(
    semantic_results: Dict[str, Any],
    keyword_results: List[Dict[str, Any]],
    max_candidates: int,
) -> Dict[str, Any]:
    documents, metadatas, distances, ids = _flatten_results(semantic_results)

    merged_docs: List[str] = []
    merged_metas: List[Dict[str, Any]] = []
    merged_distances: List[Any] = []
    merged_ids: List[Any] = []
    seen_keys = set()

    def _append(docs: list[str], metas: list[Dict[str, Any]], dists: list[Any], row_ids: list[Any]) -> None:
        for idx, doc in enumerate(docs):
            row_id = row_ids[idx] if idx < len(row_ids) else None
            key = ("id", str(row_id)) if row_id is not None else ("doc", doc)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            merged_docs.append(doc)
            merged_metas.append(metas[idx] if idx < len(metas) else {})
            merged_distances.append(dists[idx] if idx < len(dists) else None)
            merged_ids.append(row_id)
            if len(merged_docs) >= max_candidates:
                return

    _append(documents, metadatas, distances, ids)
    if len(merged_docs) < max_candidates:
        for keyword_result in keyword_results:
            k_docs, k_metas, k_dists, k_ids = _flatten_results(keyword_result)
            _append(k_docs, k_metas, k_dists, k_ids)
            if len(merged_docs) >= max_candidates:
                break

    merged = {
        "documents": [merged_docs],
        "metadatas": [merged_metas],
    }
    if any(distance is not None for distance in merged_distances):
        merged["distances"] = [merged_distances]
    if any(row_id is not None for row_id in merged_ids):
        merged["ids"] = [merged_ids]
    return merged


def _query_collection(
    collection,
    query: str,
    n_results: int,
    where_filter: Optional[Dict[str, Any]],
    where_document: Optional[Dict[str, Any]] = None,
    query_embedding: Optional[List[float]] = None,
) -> Dict[str, Any]:
    query_kwargs: Dict[str, Any] = {
        "n_results": n_results,
        "where": where_filter,
    }
    if where_document is not None:
        query_kwargs["where_document"] = where_document

    if query_embedding is not None:
        query_kwargs["query_embeddings"] = [query_embedding]
    else:
        query_kwargs["query_texts"] = [query]

    return collection.query(**query_kwargs)


def _run_hybrid_first_pass(
    collection,
    query: str,
    first_pass_n: int,
    where_filter: Optional[Dict[str, Any]],
    chroma_dir: Optional[str] = None,
) -> Dict[str, Any]:
    query_embedding = None
    # Only precompute query embeddings for OpenAI-backed collections.
    # Non-OpenAI collections can have a different vector dimension (e.g., 384),
    # so forcing OpenAI embeddings (e.g., 1536) causes hard query failures.
    effective_chroma_dir = chroma_dir or getattr(collection, "_rag_chroma_dir", None)
    if effective_chroma_dir and _is_openai_chroma_dir(str(effective_chroma_dir)):
        embedding_function = _build_embedding_function()
        if embedding_function is not None:
            try:
                embedded_query = embedding_function([query])[0]
                query_embedding = embedded_query.tolist() if hasattr(embedded_query, "tolist") else embedded_query
            except Exception as error:
                logger.debug("Query embedding precompute failed; falling back to query_texts: %s", error)

    semantic_results = _query_collection(
        collection=collection,
        query=query,
        n_results=first_pass_n,
        where_filter=where_filter,
        query_embedding=query_embedding,
    )

    if not _hybrid_enabled():
        return semantic_results

    keyword_terms = _extract_keyword_terms(query)[: _get_keyword_term_limit()]
    if not keyword_terms:
        return semantic_results

    keyword_results: List[Dict[str, Any]] = []
    keyword_n = min(first_pass_n, _get_keyword_candidates_per_term())

    for term in keyword_terms:
        try:
            keyword_results.append(
                _query_collection(
                    collection=collection,
                    query=query,
                    n_results=keyword_n,
                    where_filter=where_filter,
                    where_document={"$contains": term},
                    query_embedding=query_embedding,
                )
            )
        except TypeError:
            # Some test doubles or older collection wrappers may not expose where_document.
            logger.debug("Keyword probe skipped: collection.query has no where_document support")
            break
        except Exception as error:
            logger.debug("Keyword probe failed for term '%s': %s", term, error)

    if not keyword_results:
        return semantic_results

    return _merge_candidate_results(
        semantic_results=semantic_results,
        keyword_results=keyword_results,
        max_candidates=first_pass_n,
    )


def _rerank_documents(query: str, results: Dict[str, Any], keep_n: int) -> Dict[str, Any]:
    """Rerank first-pass candidates using lexical overlap + vector distance signal."""
    documents = (results.get("documents") or [[]])[0]
    metadatas = (results.get("metadatas") or [[]])[0]
    distances = (results.get("distances") or [[]])[0]
    ids = (results.get("ids") or [[]])[0]

    if not documents or keep_n <= 0 or len(documents) <= keep_n:
        return results

    query_tokens = _tokenize_for_rerank(query)
    safe_query_size = max(1, len(query_tokens))

    numeric_distances = [float(distance) for distance in distances[: len(documents)]] if distances else []
    if numeric_distances:
        min_distance = min(numeric_distances)
        max_distance = max(numeric_distances)
        denom = max(max_distance - min_distance, 1e-12)
    else:
        min_distance = 0.0
        max_distance = 0.0
        denom = 1.0

    scored: List[tuple[float, int]] = []
    for idx, document in enumerate(documents):
        doc_tokens = _tokenize_for_rerank(document)
        lexical_overlap = len(query_tokens & doc_tokens) / safe_query_size

        phrase_bonus = 0.1 if (query and query.lower() in (document or "").lower()) else 0.0
        lexical_score = min(1.0, lexical_overlap + phrase_bonus)

        if numeric_distances and idx < len(numeric_distances):
            # Lower vector distance means better semantic match.
            distance_similarity = (max_distance - numeric_distances[idx]) / denom
        else:
            distance_similarity = 0.5

        # Favor lexical precision slightly to improve groundedness on mission fact queries.
        final_score = (0.65 * lexical_score) + (0.35 * distance_similarity)
        scored.append((final_score, idx))

    # Stable deterministic ordering: score desc, original index asc.
    ranked_indices = [idx for _, idx in sorted(scored, key=lambda item: (-item[0], item[1]))[:keep_n]]

    reranked_documents = [documents[idx] for idx in ranked_indices]
    reranked_metadatas = [metadatas[idx] if idx < len(metadatas) else {} for idx in ranked_indices]
    reranked_distances = [distances[idx] for idx in ranked_indices] if distances else []
    reranked_ids = [ids[idx] for idx in ranked_indices] if ids else []

    updated = dict(results)
    updated["documents"] = [reranked_documents]
    updated["metadatas"] = [reranked_metadatas]
    if distances:
        updated["distances"] = [reranked_distances]
    if ids:
        updated["ids"] = [reranked_ids]
    return updated


def _get_persistent_client(chroma_dir: str) -> chromadb.PersistentClient:
    normalized = os.path.normpath(chroma_dir)
    global _CHROMA_CLIENT_CACHE_HITS
    global _CHROMA_CLIENT_CACHE_MISSES
    with _CHROMA_CLIENTS_LOCK:
        client = _CHROMA_CLIENTS.get(normalized)
        if client is not None:
            _CHROMA_CLIENT_CACHE_HITS += 1
            return client

        _CHROMA_CLIENT_CACHE_MISSES += 1
        client = chromadb.PersistentClient(
            path=chroma_dir,
            settings=Settings(anonymized_telemetry=False),
        )
        _CHROMA_CLIENTS[normalized] = client
        return client


def _get_embedding_function(api_key: str, api_base: str, model_name: str) -> OpenAIEmbeddingFunction:
    cache_key = (api_key, api_base, model_name)
    global _EMBEDDING_FN_CACHE_HITS
    global _EMBEDDING_FN_CACHE_MISSES
    with _EMBEDDING_FUNCTIONS_LOCK:
        embedding_fn = _EMBEDDING_FUNCTIONS.get(cache_key)
        if embedding_fn is not None:
            _EMBEDDING_FN_CACHE_HITS += 1
            return embedding_fn

        _EMBEDDING_FN_CACHE_MISSES += 1
        embedding_fn = OpenAIEmbeddingFunction(
            api_key=api_key,
            model_name=model_name,
            api_base=api_base,
        )
        _EMBEDDING_FUNCTIONS[cache_key] = embedding_fn
        return embedding_fn


def get_client_cache_metrics() -> Dict[str, Dict[str, int]]:
    """Return lightweight cache metrics for Chroma/OpenAI embedding resources."""
    with _CHROMA_CLIENTS_LOCK:
        chroma_metrics = {
            "current_size": len(_CHROMA_CLIENTS),
            "hits": _CHROMA_CLIENT_CACHE_HITS,
            "misses": _CHROMA_CLIENT_CACHE_MISSES,
        }
    with _EMBEDDING_FUNCTIONS_LOCK:
        embedding_metrics = {
            "current_size": len(_EMBEDDING_FUNCTIONS),
            "hits": _EMBEDDING_FN_CACHE_HITS,
            "misses": _EMBEDDING_FN_CACHE_MISSES,
        }
    return {
        "chroma_persistent_client": chroma_metrics,
        "openai_embedding_function": embedding_metrics,
    }


def _is_openai_chroma_dir(chroma_dir: str) -> bool:
    """Return True when the selected backend points to chroma_db_openai."""
    normalized = os.path.normpath(chroma_dir or "")
    return os.path.basename(normalized) == "chroma_db_openai"


def _build_embedding_function():
    api_key = get_openai_api_key()
    if not api_key:
        return None

    return _get_embedding_function(
        api_key=api_key,
        api_base=get_openai_base_url(),
        model_name=get_openai_embedding_model(),
    )

def discover_chroma_backends() -> Dict[str, Dict[str, str]]:
    """Discover available ChromaDB backends in the project directory"""
    backends = {}
    current_dir = Path(".")
    
    chroma_dirs = sorted(
        directory for directory in current_dir.iterdir()
        if directory.is_dir() and ("chroma" in directory.name.lower() or directory.name.lower().endswith("_db"))
    )

    for directory in chroma_dirs:
        try:
            client = _get_persistent_client(str(directory))
            collections = client.list_collections()

            for collection in collections:
                collection_name = collection.name if hasattr(collection, "name") else str(collection)
                key = f"{directory.name}:{collection_name}"

                try:
                    document_count = client.get_collection(collection_name).count()
                except Exception:
                    document_count = "unknown"

                backends[key] = {
                    "directory": str(directory),
                    "collection_name": collection_name,
                    "display_name": f"{directory.name}/{collection_name} ({document_count} docs)",
                    "document_count": str(document_count)
                }
        except Exception as error:
            error_text = str(error)
            if len(error_text) > 80:
                error_text = f"{error_text[:77]}..."

            key = f"{directory.name}:unavailable"
            backends[key] = {
                "directory": str(directory),
                "collection_name": "",
                "display_name": f"{directory.name} (unavailable: {error_text})",
                "document_count": "unknown"
            }

    return backends

def initialize_rag_system(chroma_dir: str, collection_name: str):
    """Initialize the RAG system with specified backend (cached for performance)"""

    try:
        client = _get_persistent_client(chroma_dir)
        embedding_function = _build_embedding_function()
        collection = None

        if embedding_function is not None and _is_openai_chroma_dir(chroma_dir):
            collection = client.get_collection(
                name=collection_name,
                embedding_function=embedding_function,
            )

        if collection is None:
            collection = client.get_collection(name=collection_name)

        try:
            setattr(collection, "_rag_chroma_dir", chroma_dir)
        except Exception:
            pass

        return collection, True, None
    except Exception as error:
        return None, False, str(error)

def retrieve_documents(
    collection,
    query: str,
    n_results: int = 3,
    mission_filter: Optional[str] = None,
    chroma_dir: Optional[str] = None,
) -> Optional[Dict]:
    """Retrieve documents with two-stage retrieval + local reranking (LLM08-aware)."""
    


    if VectorSecurityValidator:
        try:
            effective_chroma_dir = chroma_dir or getattr(collection, "_rag_chroma_dir", "./chroma_db_openai")
            VectorSecurityValidator.validate_embedding_source(
                collection.name if hasattr(collection, 'name') else 'unknown',
                effective_chroma_dir,
            )
        except SecurityViolation as e:
            logger.error(f"Vector validation failed: {e}")
            raise
    
    where_filter = None

    if mission_filter and mission_filter.strip().lower() not in {"all", "any", "*", "none"}:
        normalized_mission = _normalize_mission_filter(mission_filter)
        where_filter = {"mission": normalized_mission}

    requested_n = max(1, int(n_results))
    first_pass_n = max(
        requested_n,
        min(requested_n * _get_first_pass_multiplier(), _get_first_pass_max_candidates()),
    )

    retrieval_query = _rewrite_query_for_retrieval(
        query=query,
        mission_filter=mission_filter,
    )

    results = _run_hybrid_first_pass(
        collection=collection,
        query=retrieval_query,
        first_pass_n=first_pass_n,
        where_filter=where_filter,
        chroma_dir=chroma_dir,
    )

    results = _rerank_documents(query=query, results=results, keep_n=requested_n)
    
    if VectorSecurityValidator and results and results.get("documents"):
        poisoning_check = VectorSecurityValidator.detect_poisoned_results(
            results["documents"][0] if results.get("documents") else [],
            results["metadatas"][0] if results.get("metadatas") else [{} for _ in range(requested_n)],
        )
        if poisoning_check:
            logger.warning(f"Potentially poisoned results detected: {poisoning_check}")

    return results

def warm_collection_index(collection) -> Dict[str, Any]:
    """Prime ChromaDB index metadata for *collection* during server startup.

    Calls `count()` and `peek(limit=1)` so the underlying HNSW index and
    SQLite metadata tables are loaded into the process cache before the first
    real request arrives.  Both calls are fast (no embedding round-trip) and
    idempotent, which is safe to run on every startup.

    Returns:
    A dict containing:
    - `count`: document count when available, otherwise -1
    - `index_primed`: whether minimal index data was loaded
    - `error`: optional truncated error message on failure
    """

    try:
        doc_count = collection.count()
        peek = collection.peek(limit=1)
        index_primed = len(peek.get("ids", [])) > 0
        return {"count": doc_count, "index_primed": index_primed}
    except Exception as exc:
        return {"count": -1, "index_primed": False, "error": str(exc)[:80]}


def format_context(documents: List[str], metadatas: List[Dict]) -> str:
    """Format retrieved documents into context"""
    if not documents:
        return ""
    
    context_parts = ["Use these retrieved sources when answering:"]

    for index, (document, metadata) in enumerate(zip(documents, metadatas), start=1):
        metadata = metadata or {}
        mission = str(metadata.get("mission", "unknown")).replace("_", " ").title()
        source = str(metadata.get("source", "unknown"))
        category = str(metadata.get("document_category", "general")).replace("_", " ").title()

        context_parts.append(
            f"Source {index} | Mission: {mission} | Category: {category} | File: {source}"
        )

        cleaned_document = (document or "").strip()
        if len(cleaned_document) > 1500:
            cleaned_document = f"{cleaned_document[:1500]}..."
        context_parts.append(cleaned_document)

    return "\n\n".join(context_parts)