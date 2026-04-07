import os

import chromadb
import logging
from chromadb.config import Settings
from chromadb.utils.embedding_functions import OpenAIEmbeddingFunction
from typing import Dict, List, Optional
from pathlib import Path

from openai_config import get_openai_api_key, get_openai_base_url, get_openai_embedding_model

try:
    from security import VectorSecurityValidator, SecurityViolation
except ImportError:
    VectorSecurityValidator = None  # Graceful degradation
    SecurityViolation = Exception

logger = logging.getLogger(__name__)


def _is_openai_chroma_dir(chroma_dir: str) -> bool:
    """Return True when the selected backend points to chroma_db_openai."""
    normalized = os.path.normpath(chroma_dir or "")
    return os.path.basename(normalized) == "chroma_db_openai"


def _build_embedding_function():
    api_key = get_openai_api_key()
    if not api_key:
        return None

    return OpenAIEmbeddingFunction(
        api_key=api_key,
        model_name=get_openai_embedding_model(),
        api_base=get_openai_base_url(),
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
            client = chromadb.PersistentClient(
                path=str(directory),
                settings=Settings(anonymized_telemetry=False)
            )
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
        client = chromadb.PersistentClient(
            path=chroma_dir,
            settings=Settings(anonymized_telemetry=False)
        )
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
    """Retrieve relevant documents from ChromaDB with security validation (LLM08)."""
    


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
        normalized_mission = mission_filter.strip().lower().replace(" ", "_")
        where_filter = {"mission": normalized_mission}

    results = collection.query(
        query_texts=[query],
        n_results=n_results,
        where=where_filter
    )
    
    if VectorSecurityValidator and results and results.get("documents"):
        poisoning_check = VectorSecurityValidator.detect_poisoned_results(
            results["documents"][0] if results.get("documents") else [],
            results["metadatas"][0] if results.get("metadatas") else [{} for _ in range(n_results)],
        )
        if poisoning_check:
            logger.warning(f"Potentially poisoned results detected: {poisoning_check}")

    return results

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