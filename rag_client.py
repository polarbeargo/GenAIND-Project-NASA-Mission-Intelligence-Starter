import chromadb
from chromadb.config import Settings
from typing import Dict, List, Optional
from pathlib import Path

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
        collection = client.get_collection(name=collection_name)
        return collection, True, None
    except Exception as error:
        return None, False, str(error)

def retrieve_documents(collection, query: str, n_results: int = 3, 
                      mission_filter: Optional[str] = None) -> Optional[Dict]:
    """Retrieve relevant documents from ChromaDB with optional filtering"""
    where_filter = None

    if mission_filter and mission_filter.strip().lower() not in {"all", "any", "*", "none"}:
        normalized_mission = mission_filter.strip().lower().replace(" ", "_")
        where_filter = {"mission": normalized_mission}

    results = collection.query(
        query_texts=[query],
        n_results=n_results,
        where=where_filter
    )

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