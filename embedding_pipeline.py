#!/usr/bin/env python3
"""
ChromaDB Embedding Pipeline for NASA Space Mission Data - Text Files Only

This script reads parsed text data from various NASA space mission folders and creates
a permanent ChromaDB collection with OpenAI embeddings for RAG applications.
Optimized to process only text files to avoid duplication with JSON versions.

Supported data sources:
- Apollo 11 extracted data (text files only)
- Apollo 13 extracted data (text files only)
- Apollo 11 Textract extracted data (text files only)
- Challenger transcribed audio data (text files only)
"""

import os
import json
import logging
from collections import Counter
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple, Set
import chromadb
from chromadb.config import Settings
import openai
from openai import OpenAI
import hashlib
import time
from datetime import datetime
import argparse
from chromadb.utils.embedding_functions import OpenAIEmbeddingFunction

from env_utils import load_project_env
from openai_config import (
    get_openai_api_key,
    get_openai_base_url,
    set_chroma_openai_api_key,
)

try:
    import polars as pl
    POLARS_AVAILABLE = True
except Exception:
    pl = None
    POLARS_AVAILABLE = False

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except Exception:
    pd = None
    PANDAS_AVAILABLE = False

load_project_env(__file__)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('chroma_embedding_text_only.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ChromaEmbeddingPipelineTextOnly:
    """Pipeline for creating ChromaDB collections with OpenAI embeddings - Text files only"""
    
    def __init__(self, 
                 openai_api_key: str,
                 chroma_persist_directory: str = "./chroma_db",
                 collection_name: str = "nasa_space_missions_text",
                 embedding_model: str = "text-embedding-3-small",
                 chunk_size: int = 1000,
                 chunk_overlap: int = 200):
        """
        Initialize the embedding pipeline
        
        Args:
            openai_api_key: OpenAI API key
            chroma_persist_directory: Directory to persist ChromaDB
            collection_name: Name of the ChromaDB collection
            embedding_model: OpenAI embedding model to use
            chunk_size: Maximum size of text chunks
            chunk_overlap: Overlap between chunks
        """
        _openai_base = get_openai_base_url()
        self.openai_client = OpenAI(api_key=openai_api_key, base_url=_openai_base)
        self.openai_api_key = openai_api_key
        self.chroma_persist_directory = chroma_persist_directory
        self.collection_name = collection_name
        self.embedding_model = embedding_model
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap

        set_chroma_openai_api_key(openai_api_key)
        Path(chroma_persist_directory).mkdir(parents=True, exist_ok=True)

        self.chroma_client = chromadb.PersistentClient(
            path=chroma_persist_directory,
            settings=Settings(anonymized_telemetry=False)
        )
        self.embedding_function = OpenAIEmbeddingFunction(
            api_key=openai_api_key,
            model_name=embedding_model,
            api_base=_openai_base,
        )
        self.collection = self.chroma_client.get_or_create_collection(
            name=collection_name,
            embedding_function=self.embedding_function,
            metadata={
                "pipeline": "text_only",
                "embedding_model": embedding_model,
            }
        )
        self.manifest_path = Path(chroma_persist_directory) / f"{collection_name}_content_manifest.json"
        self.manifest = self._load_manifest()

    def _load_manifest(self) -> Dict[str, Any]:
        """Load persistent content hash manifest used for incremental embedding updates."""
        if not self.manifest_path.exists():
            return {
                "version": 1,
                "collection": self.collection_name,
                "chunk_size": self.chunk_size,
                "chunk_overlap": self.chunk_overlap,
                "embedding_model": self.embedding_model,
                "files": {},
            }

        try:
            with self.manifest_path.open("r", encoding="utf-8") as handle:
                data = json.load(handle)
            files = data.get("files")
            if not isinstance(files, dict):
                data["files"] = {}
            return data
        except Exception as exc:
            logger.warning("Unable to load manifest %s (%s). Starting fresh.", self.manifest_path, exc)
            return {
                "version": 1,
                "collection": self.collection_name,
                "chunk_size": self.chunk_size,
                "chunk_overlap": self.chunk_overlap,
                "embedding_model": self.embedding_model,
                "files": {},
            }

    def _save_manifest(self) -> None:
        """Persist manifest atomically to avoid partial writes during failures."""
        self.manifest["collection"] = self.collection_name
        self.manifest["chunk_size"] = self.chunk_size
        self.manifest["chunk_overlap"] = self.chunk_overlap
        self.manifest["embedding_model"] = self.embedding_model
        self.manifest["updated_at"] = datetime.utcnow().isoformat() + "Z"

        temp_path = self.manifest_path.with_suffix(self.manifest_path.suffix + ".tmp")
        with temp_path.open("w", encoding="utf-8") as handle:
            json.dump(self.manifest, handle, ensure_ascii=True, indent=2, sort_keys=True)
        temp_path.replace(self.manifest_path)

    @staticmethod
    def _hash_text(value: str) -> str:
        """Return deterministic SHA256 hash for UTF-8 text."""
        return hashlib.sha256(value.encode("utf-8")).hexdigest()

    @staticmethod
    def _manifest_file_key(file_path: Path) -> str:
        """Return deterministic manifest key for a file path."""
        return str(file_path.resolve()).replace("\\", "/")

    def _read_text_with_hash(self, file_path: Path) -> Tuple[str, str]:
        """Read text file and compute content hash in one pass."""
        with file_path.open("r", encoding="utf-8") as handle:
            content = handle.read()
        return content, self._hash_text(content)

    def _documents_from_content(self, file_path: Path, content: str) -> List[Tuple[str, Dict[str, Any]]]:
        """Build chunked documents from already-loaded content."""
        if not content.strip():
            return []

        metadata = {
            'source': file_path.stem,
            'file_path': str(file_path),
            'file_type': 'text',
            'content_type': 'full_text',
            'mission': self.extract_mission_from_path(file_path),
            'data_type': self.extract_data_type_from_path(file_path),
            'document_category': self.extract_document_category_from_filename(file_path.name),
            'file_size': len(content),
            'processed_timestamp': datetime.now().isoformat(),
        }
        return self.chunk_text(content, metadata)

    def get_embeddings_batch(self, texts: List[str]) -> List[List[float]]:
        """Get embeddings for a list of text inputs in one API call for throughput efficiency."""
        if not texts:
            return []
        response = self.openai_client.embeddings.create(
            model=self.embedding_model,
            input=texts,
        )
        if not response.data:
            raise ValueError("No embeddings returned from OpenAI")
        return [item.embedding for item in response.data]
    
    def chunk_text(self, text: str, metadata: Dict[str, Any]) -> List[Tuple[str, Dict[str, Any]]]:
        """
        Split text into chunks with metadata
        
        Args:
            text: Text to chunk
            metadata: Base metadata for the text
            
        Returns:
            List of (chunk_text, chunk_metadata) tuples
        """
        cleaned_text = text.strip()
        if not cleaned_text:
            return []

        if len(cleaned_text) <= self.chunk_size:
            chunk_metadata = dict(metadata)
            chunk_metadata.update({
                'chunk_index': 0,
                'chunk_count': 1,
                'chunk_start': 0,
                'chunk_end': len(cleaned_text),
            })
            return [(cleaned_text, chunk_metadata)]

        chunks: List[Tuple[str, Dict[str, Any]]] = []
        text_length = len(cleaned_text)
        start = 0
        chunk_index = 0
        effective_overlap = min(self.chunk_overlap, max(self.chunk_size - 1, 0))
        min_break_position = int(self.chunk_size * 0.6)

        while start < text_length:
            end = min(start + self.chunk_size, text_length)

            if end < text_length:
                sentence_breaks = [
                    cleaned_text.rfind('. ', start, end),
                    cleaned_text.rfind('? ', start, end),
                    cleaned_text.rfind('! ', start, end),
                    cleaned_text.rfind('\n', start, end),
                ]
                best_break = max(sentence_breaks)
                if best_break > start + min_break_position:
                    end = best_break + 1

            chunk_content = cleaned_text[start:end].strip()
            if chunk_content:
                chunk_metadata = dict(metadata)
                chunk_metadata.update({
                    'chunk_index': chunk_index,
                    'chunk_start': start,
                    'chunk_end': end,
                })
                chunks.append((chunk_content, chunk_metadata))
                chunk_index += 1

            if end >= text_length:
                break

            next_start = end - effective_overlap
            if next_start <= start:
                next_start = start + 1
            start = next_start

        chunk_count = len(chunks)
        for _, chunk_metadata in chunks:
            chunk_metadata['chunk_count'] = chunk_count

        return chunks
    
    def check_document_exists(self, doc_id: str) -> bool:
        """
        Check if a document with the given ID already exists in the collection
        
        Args:
            doc_id: Document ID to check
            
        Returns:
            True if document exists, False otherwise
        """
        try:
            result = self.collection.get(ids=[doc_id])
            return bool(result.get('ids'))
        except Exception:
            return False
    
    def update_document(self, doc_id: str, text: str, metadata: Dict[str, Any]) -> bool:
        """
        Update an existing document in the collection
        
        Args:
            doc_id: Document ID to update
            text: New text content
            metadata: New metadata
            
        Returns:
            True if successful, False otherwise
        """
        try:
            embedding = self.get_embedding(text)
            self.collection.update(
                ids=[doc_id],
                documents=[text],
                metadatas=[metadata],
                embeddings=[embedding]
            )
            logger.debug(f"Updated document: {doc_id}")
            return True
        except Exception as e:
            logger.error(f"Error updating document {doc_id}: {e}")
            return False
    
    def delete_documents_by_source(self, source_pattern: str) -> int:
        """
        Delete all documents from a specific source (useful for re-processing files)
        
        Args:
            source_pattern: Pattern to match source names
            
        Returns:
            Number of documents deleted
        """
        try:
            all_docs = self.collection.get()
            
            ids_to_delete = []
            for i, metadata in enumerate(all_docs['metadatas']):
                if source_pattern in metadata.get('source', ''):
                    ids_to_delete.append(all_docs['ids'][i])
            
            if ids_to_delete:
                self.collection.delete(ids=ids_to_delete)
                logger.info(f"Deleted {len(ids_to_delete)} documents matching source pattern: {source_pattern}")
                return len(ids_to_delete)
            else:
                logger.info(f"No documents found matching source pattern: {source_pattern}")
                return 0
                
        except Exception as e:
            logger.error(f"Error deleting documents by source: {e}")
            return 0
    
    def get_file_documents(self, file_path: Path) -> List[str]:
        """
        Get all document IDs for a specific file
        
        Args:
            file_path: Path to the file
            
        Returns:
            List of document IDs for the file
        """
        try:
            source = file_path.stem
            mission = self.extract_mission_from_path(file_path)
            
            all_docs = self.collection.get()
            
            file_doc_ids = []
            for i, metadata in enumerate(all_docs['metadatas']):
                if (metadata.get('source') == source and 
                    metadata.get('mission') == mission):
                    file_doc_ids.append(all_docs['ids'][i])
            
            return file_doc_ids
            
        except Exception as e:
            logger.error(f"Error getting file documents: {e}")
            return []
    
    def get_embedding(self, text: str) -> List[float]:
        """
        Get OpenAI embedding for text
        
        Args:
            text: Text to embed
            
        Returns:
            Embedding vector
        """
        try:
            response = self.openai_client.embeddings.create(
                model=self.embedding_model,
                input=text
            )
            if not response.data:
                raise ValueError("No embedding returned from OpenAI")
            return response.data[0].embedding
        except Exception as e:
            logger.error(f"Error getting embedding: {e}")
            raise

    def generate_document_id(self, file_path: Path, metadata: Dict[str, Any]) -> str:
        """
        Generate stable document ID based on file path and chunk position
        This allows for document updates without changing IDs
        """
        mission = str(metadata.get('mission', 'unknown')).lower().replace(' ', '_')
        source = str(metadata.get('source', file_path.stem)).lower().replace(' ', '_')
        chunk_index = int(metadata.get('chunk_index', 0))

        mission = ''.join(char if (char.isalnum() or char in {'_', '-'}) else '_' for char in mission)
        source = ''.join(char if (char.isalnum() or char in {'_', '-'}) else '_' for char in source)

        return f"{mission}_{source}_chunk_{chunk_index:04d}"
    
    def process_text_file(self, file_path: Path) -> List[Tuple[str, Dict[str, Any]]]:
        """
        Process plain text files with enhanced metadata extraction
        
        Args:
            file_path: Path to text file
            
        Returns:
            List of (text, metadata) tuples
        """
        try:
            content, _ = self._read_text_with_hash(file_path)
            return self._documents_from_content(file_path, content)
            
        except Exception as e:
            logger.error(f"Error processing text file {file_path}: {e}")
            return []

    def _process_file_incremental(
        self,
        file_path: Path,
        batch_size: int,
    ) -> Dict[str, Any]:
        """Incrementally process one file based on manifest content and chunk hashes."""
        stats = {
            'chunks': 0,
            'added': 0,
            'updated': 0,
            'skipped': 0,
            'deleted': 0,
            'status': 'pending',
            'error': '',
        }

        content, file_hash = self._read_text_with_hash(file_path)
        file_key = self._manifest_file_key(file_path)
        old_entry = self.manifest.get('files', {}).get(file_key, {})

        old_file_hash = str(old_entry.get('file_hash', ''))
        old_chunk_hashes = old_entry.get('chunk_hashes', {}) or {}
        old_doc_ids = set(old_entry.get('doc_ids', []) or [])

        if old_file_hash and old_file_hash == file_hash:
            stats['status'] = 'unchanged_file'
            stats['chunks'] = int(old_entry.get('chunk_count', len(old_chunk_hashes)))
            stats['skipped'] = stats['chunks']
            return stats

        documents = self._documents_from_content(file_path, content)
        if not documents:
            if old_doc_ids:
                self.collection.delete(ids=list(old_doc_ids))
                stats['deleted'] = len(old_doc_ids)
            self.manifest.setdefault('files', {})[file_key] = {
                'file_hash': file_hash,
                'chunk_count': 0,
                'chunk_hashes': {},
                'doc_ids': [],
                'updated_at': datetime.utcnow().isoformat() + 'Z',
            }
            stats['status'] = 'empty'
            return stats

        upsert_payload: List[Tuple[str, str, Dict[str, Any], str]] = []
        new_chunk_hashes: Dict[str, str] = {}
        new_doc_ids: Set[str] = set()

        for text, metadata in documents:
            doc_id = self.generate_document_id(file_path, metadata)
            chunk_hash = self._hash_text(text)
            new_chunk_hashes[doc_id] = chunk_hash
            new_doc_ids.add(doc_id)

            if old_chunk_hashes.get(doc_id) == chunk_hash:
                stats['skipped'] += 1
                continue
            upsert_payload.append((doc_id, text, metadata, chunk_hash))

        stale_ids = list(old_doc_ids - new_doc_ids)
        if stale_ids:
            self.collection.delete(ids=stale_ids)
            stats['deleted'] += len(stale_ids)

        for batch_start in range(0, len(upsert_payload), batch_size):
            batch = upsert_payload[batch_start:batch_start + batch_size]
            ids = [item[0] for item in batch]
            texts = [item[1] for item in batch]
            metadatas = [item[2] for item in batch]
            embeddings = self.get_embeddings_batch(texts)

            self.collection.upsert(
                ids=ids,
                documents=texts,
                metadatas=metadatas,
                embeddings=embeddings,
            )

            for doc_id in ids:
                if doc_id in old_doc_ids:
                    stats['updated'] += 1
                else:
                    stats['added'] += 1

        self.manifest.setdefault('files', {})[file_key] = {
            'file_hash': file_hash,
            'chunk_count': len(documents),
            'chunk_hashes': new_chunk_hashes,
            'doc_ids': sorted(new_doc_ids),
            'updated_at': datetime.utcnow().isoformat() + 'Z',
        }

        stats['chunks'] = len(documents)
        stats['status'] = 'processed'
        return stats
    
    def extract_mission_from_path(self, file_path: Path) -> str:
        """Extract mission name from file path"""
        path_str = str(file_path).lower()
        if 'apollo11' in path_str or 'apollo_11' in path_str:
            return 'apollo_11'
        elif 'apollo13' in path_str or 'apollo_13' in path_str:
            return 'apollo_13'
        elif 'challenger' in path_str:
            return 'challenger'
        else:
            return 'unknown'
    
    def extract_data_type_from_path(self, file_path: Path) -> str:
        """Extract data type from file path"""
        path_str = str(file_path).lower()
        if 'transcript' in path_str:
            return 'transcript'
        elif 'textract' in path_str:
            return 'textract_extracted'
        elif 'audio' in path_str:
            return 'audio_transcript'
        elif 'flight_plan' in path_str:
            return 'flight_plan'
        else:
            return 'document'
    
    def extract_document_category_from_filename(self, filename: str) -> str:
        """Extract document category from filename for better organization"""
        filename_lower = filename.lower()
        
        if 'pao' in filename_lower:
            return 'public_affairs_officer'
        elif 'cm' in filename_lower:
            return 'command_module'
        elif 'tec' in filename_lower:
            return 'technical'
        elif 'flight_plan' in filename_lower:
            return 'flight_plan'
        
        elif 'mission_audio' in filename_lower:
            return 'mission_audio'
        
        elif 'ntrs' in filename_lower:
            return 'nasa_archive'
        elif '19900066485' in filename_lower:
            return 'technical_report'
        elif '19710015566' in filename_lower:
            return 'mission_report'
        
        elif 'full_text' in filename_lower:
            return 'complete_document'
        else:
            return 'general_document'
    
    def scan_text_files_only(self, base_path: str) -> List[Path]:
        """
        Scan data directories for text files only (avoiding JSON duplicates)
        
        Args:
            base_path: Base directory path
            
        Returns:
            List of text file paths to process
        """
        base_path = Path(base_path)
        files_to_process = []
        
        data_dirs = [
            'apollo11',
            'apollo13',
            'challenger'
        ]
        
        for data_dir in data_dirs:
            dir_path = base_path / data_dir
            if dir_path.exists():
                logger.info(f"Scanning directory: {dir_path}")
                
                text_files = list(dir_path.glob('**/*.txt'))
                files_to_process.extend(text_files)
                logger.info(f"Found {len(text_files)} text files in {data_dir}")
        
        filtered_files = []
        for file_path in files_to_process:
            if (file_path.name.startswith('.') or 
                'summary' in file_path.name.lower() or
                file_path.suffix.lower() != '.txt'):
                continue
            filtered_files.append(file_path)
        
        logger.info(f"Total text files to process: {len(filtered_files)}")
        
        mission_counts = {}
        for file_path in filtered_files:
            mission = self.extract_mission_from_path(file_path)
            mission_counts[mission] = mission_counts.get(mission, 0) + 1
        
        logger.info("Files by mission:")
        for mission, count in mission_counts.items():
            logger.info(f"  {mission}: {count} files")
        
        return filtered_files
    
    def add_documents_to_collection(self, documents: List[Tuple[str, Dict[str, Any]]], 
                                   file_path: Path, batch_size: int = 50, 
                                   update_mode: str = 'skip') -> Dict[str, int]:
        """
        Add documents to ChromaDB collection in batches with update handling
        
        Args:
            documents: List of (text, metadata) tuples
            file_path: Path to the source file
            batch_size: Number of documents to process in each batch
            update_mode: How to handle existing documents:
                        'skip' - skip existing documents
                        'update' - update existing documents
                        'replace' - delete all existing documents from file and re-add
            
        Returns:
            Dictionary with counts of added, updated, and skipped documents
        """
        if not documents:
            return {'added': 0, 'updated': 0, 'skipped': 0}
        
        stats = {'added': 0, 'updated': 0, 'skipped': 0}

        if update_mode not in {'skip', 'update', 'replace'}:
            raise ValueError("update_mode must be one of: skip, update, replace")

        if update_mode == 'replace':
            existing_file_ids = self.get_file_documents(file_path)
            if existing_file_ids:
                self.collection.delete(ids=existing_file_ids)

        for batch_start in range(0, len(documents), batch_size):
            batch = documents[batch_start:batch_start + batch_size]

            for text, metadata in batch:
                doc_id = self.generate_document_id(file_path, metadata)
                doc_exists = self.check_document_exists(doc_id)

                if doc_exists and update_mode == 'skip':
                    stats['skipped'] += 1
                    continue

                try:
                    if doc_exists and update_mode == 'update':
                        if self.update_document(doc_id, text, metadata):
                            stats['updated'] += 1
                        else:
                            stats['skipped'] += 1
                    else:
                        embedding = self.get_embedding(text)
                        self.collection.add(
                            ids=[doc_id],
                            documents=[text],
                            metadatas=[metadata],
                            embeddings=[embedding]
                        )
                        stats['added'] += 1
                except Exception as e:
                    logger.error(f"Error processing document {doc_id}: {e}")
                    stats['skipped'] += 1

        return stats
    
    def process_all_text_data(
        self,
        base_path: str,
        update_mode: str = 'skip',
        batch_size: int = 50,
    ) -> Dict[str, int]:
        """
        Process all text files and add to ChromaDB
        
        Args:
            base_path: Base directory containing data folders
            update_mode: How to handle existing documents:
                        'skip' - skip existing documents (default)
                        'update' - update existing documents
                        'replace' - delete all existing documents from file and re-add
            
        Returns:
            Statistics about processed files
        """
        stats = {
            'files_processed': 0,
            'documents_added': 0,
            'documents_updated': 0,
            'documents_skipped': 0,
            'errors': 0,
            'total_chunks': 0,
            'missions': {}
        }
        run_rows: List[Dict[str, Any]] = []

        files_to_process = self.scan_text_files_only(base_path)
        use_incremental = update_mode == 'incremental'

        if use_incremental:
            manifest_files = self.manifest.setdefault('files', {})
            existing_keys = set(manifest_files.keys())
            seen_keys = {self._manifest_file_key(path) for path in files_to_process}
            missing_keys = sorted(existing_keys - seen_keys)
            for missing_key in missing_keys:
                entry = manifest_files.get(missing_key, {})
                old_ids = entry.get('doc_ids', []) or []
                if old_ids:
                    try:
                        self.collection.delete(ids=list(old_ids))
                    except Exception as exc:
                        logger.warning("Failed deleting stale file docs from %s: %s", missing_key, exc)
                manifest_files.pop(missing_key, None)

        for file_path in files_to_process:
            mission = self.extract_mission_from_path(file_path)
            if mission not in stats['missions']:
                stats['missions'][mission] = {
                    'files': 0,
                    'chunks': 0,
                    'added': 0,
                    'updated': 0,
                    'skipped': 0,
                }

            run_row: Dict[str, Any] = {
                'file_path': str(file_path),
                'source': file_path.stem,
                'mission': mission,
                'update_mode': update_mode,
                'chunks': 0,
                'added': 0,
                'updated': 0,
                'skipped': 0,
                'status': 'pending',
                'error': '',
                'deleted': 0,
            }

            try:
                stats['files_processed'] += 1
                stats['missions'][mission]['files'] += 1

                if use_incremental:
                    file_stats = self._process_file_incremental(
                        file_path=file_path,
                        batch_size=batch_size,
                    )
                    chunk_count = int(file_stats['chunks'])
                else:
                    documents = self.process_text_file(file_path)
                    if not documents:
                        run_row['status'] = 'empty'
                        continue
                    file_stats = self.add_documents_to_collection(
                        documents=documents,
                        file_path=file_path,
                        batch_size=batch_size,
                        update_mode=update_mode,
                    )
                    chunk_count = len(documents)

                stats['total_chunks'] += chunk_count
                stats['documents_added'] += file_stats['added']
                stats['documents_updated'] += file_stats['updated']
                stats['documents_skipped'] += file_stats['skipped']

                stats['missions'][mission]['chunks'] += chunk_count
                stats['missions'][mission]['added'] += file_stats['added']
                stats['missions'][mission]['updated'] += file_stats['updated']
                stats['missions'][mission]['skipped'] += file_stats['skipped']
                run_row['chunks'] = chunk_count
                run_row['added'] = file_stats['added']
                run_row['updated'] = file_stats['updated']
                run_row['skipped'] = file_stats['skipped']
                run_row['deleted'] = int(file_stats.get('deleted', 0))
                run_row['status'] = str(file_stats.get('status', 'processed'))
            except Exception as e:
                stats['errors'] += 1
                logger.error(f"Error processing file {file_path}: {e}")
                run_row['status'] = 'error'
                run_row['error'] = str(e)[:500]
            finally:
                run_rows.append(run_row)

        if use_incremental:
            self._save_manifest()

        stats['run_summary_artifacts'] = self._persist_run_summary_artifacts(
            run_rows=run_rows,
            base_path=base_path,
            update_mode=update_mode,
        )
        
        return stats

    def _persist_run_summary_artifacts(
        self,
        run_rows: List[Dict[str, Any]],
        base_path: str,
        update_mode: str,
    ) -> Dict[str, str]:
        """Persist run-level artifacts with a Polars-first approach."""
        if not run_rows:
            return {}

        output_dir = Path("monitoring") / "embedding_runs"
        output_dir.mkdir(parents=True, exist_ok=True)

        run_id = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        prefix = f"embedding_run_{run_id}"
        artifact_paths: Dict[str, str] = {}

        if POLARS_AVAILABLE and pl is not None:
            dataset = pl.DataFrame(run_rows)
            dataset = dataset.with_columns([
                pl.lit(run_id).alias("run_id"),
                pl.lit(str(base_path)).alias("base_path"),
                pl.lit(update_mode).alias("update_mode"),
                pl.lit(datetime.utcnow().isoformat() + "Z").alias("generated_at"),
            ])

            detail_parquet = output_dir / f"{prefix}_detail.parquet"
            detail_csv = output_dir / f"{prefix}_detail.csv"
            rollup_csv = output_dir / f"{prefix}_rollup.csv"

            dataset.write_parquet(str(detail_parquet))
            dataset.write_csv(str(detail_csv))

            rollup = dataset.group_by(["mission", "status"]).agg([
                pl.len().alias("files"),
                pl.col("chunks").sum().alias("chunks"),
                pl.col("added").sum().alias("added"),
                pl.col("updated").sum().alias("updated"),
                pl.col("skipped").sum().alias("skipped"),
            ]).sort(["mission", "status"])
            rollup.write_csv(str(rollup_csv))

            artifact_paths = {
                "run_id": run_id,
                "detail_parquet": str(detail_parquet),
                "detail_csv": str(detail_csv),
                "rollup_csv": str(rollup_csv),
            }
            return artifact_paths

        if PANDAS_AVAILABLE and pd is not None:
            dataset = pd.DataFrame(run_rows)
            dataset["run_id"] = run_id
            dataset["base_path"] = str(base_path)
            dataset["update_mode"] = update_mode
            dataset["generated_at"] = datetime.utcnow().isoformat() + "Z"

            detail_csv = output_dir / f"{prefix}_detail.csv"
            rollup_csv = output_dir / f"{prefix}_rollup.csv"
            dataset.to_csv(detail_csv, index=False)

            rollup = dataset.groupby(["mission", "status"], dropna=False).agg(
                files=("file_path", "count"),
                chunks=("chunks", "sum"),
                added=("added", "sum"),
                updated=("updated", "sum"),
                skipped=("skipped", "sum"),
            ).reset_index()
            rollup.to_csv(rollup_csv, index=False)

            return {
                "run_id": run_id,
                "detail_csv": str(detail_csv),
                "rollup_csv": str(rollup_csv),
            }

        detail_json = output_dir / f"{prefix}_detail.json"
        with detail_json.open("w", encoding="utf-8") as handle:
            json.dump(
                {
                    "run_id": run_id,
                    "base_path": str(base_path),
                    "update_mode": update_mode,
                    "rows": run_rows,
                },
                handle,
                ensure_ascii=True,
                indent=2,
            )
        return {
            "run_id": run_id,
            "detail_json": str(detail_json),
        }
    
    def get_collection_info(self) -> Dict[str, Any]:
        """Get information about the ChromaDB collection"""
        try:
            return {
                'collection_name': self.collection.name,
                'document_count': self.collection.count(),
                'metadata': self.collection.metadata or {},
                'persist_directory': self.chroma_persist_directory,
                'embedding_model': self.embedding_model,
            }
        except Exception as e:
            logger.error(f"Error getting collection info: {e}")
            return {'error': str(e)}
    
    def query_collection(self, query_text: str, n_results: int = 5) -> Dict[str, Any]:
        """
        Query the collection for testing
        
        Args:
            query_text: Query text
            n_results: Number of results to return
            
        Returns:
            Query results
        """
        try:
            return self.collection.query(
                query_texts=[query_text],
                n_results=n_results,
            )
        except Exception as e:
            logger.error(f"Error querying collection: {e}")
            return {'error': str(e)}
    
    def get_collection_stats(self) -> Dict[str, Any]:
        """Get detailed statistics about the collection"""
        try:
            all_docs = self.collection.get()
            
            if not all_docs['metadatas']:
                return {'error': 'No documents in collection'}

            rows = [
                {
                    'mission': str((metadata or {}).get('mission', 'unknown') or 'unknown'),
                    'data_type': str((metadata or {}).get('data_type', 'unknown') or 'unknown'),
                    'document_category': str((metadata or {}).get('document_category', 'unknown') or 'unknown'),
                    'file_type': str((metadata or {}).get('file_type', 'unknown') or 'unknown'),
                }
                for metadata in all_docs['metadatas']
            ]

            if POLARS_AVAILABLE and pl is not None:
                dataset = pl.DataFrame(rows)

                def _counts_polars(column: str) -> Dict[str, int]:
                    grouped = dataset.group_by(column).len()
                    return {
                        str(value): int(count)
                        for value, count in grouped.iter_rows()
                    }

                return {
                    'total_documents': len(rows),
                    'missions': _counts_polars('mission'),
                    'data_types': _counts_polars('data_type'),
                    'document_categories': _counts_polars('document_category'),
                    'file_types': _counts_polars('file_type'),
                }

            if PANDAS_AVAILABLE and pd is not None:
                dataset = pd.DataFrame(rows)

                def _counts_pandas(column: str) -> Dict[str, int]:
                    value_counts = dataset[column].fillna('unknown').astype(str).value_counts(dropna=False)
                    return {
                        str(key): int(value)
                        for key, value in value_counts.to_dict().items()
                    }

                return {
                    'total_documents': len(rows),
                    'missions': _counts_pandas('mission'),
                    'data_types': _counts_pandas('data_type'),
                    'document_categories': _counts_pandas('document_category'),
                    'file_types': _counts_pandas('file_type'),
                }

            missions: Counter = Counter()
            data_types: Counter = Counter()
            doc_categories: Counter = Counter()
            file_types: Counter = Counter()

            for row in rows:
                missions[row['mission']] += 1
                data_types[row['data_type']] += 1
                doc_categories[row['document_category']] += 1
                file_types[row['file_type']] += 1

            return {
                'total_documents': len(rows),
                'missions': dict(missions),
                'data_types': dict(data_types),
                'document_categories': dict(doc_categories),
                'file_types': dict(file_types),
            }
            
        except Exception as e:
            logger.error(f"Error getting collection stats: {e}")
            return {'error': str(e)}

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='ChromaDB Embedding Pipeline for NASA Data')
    parser.add_argument('--data-path', default='.', help='Path to data directories')
    parser.add_argument('--openai-key', default=None, help='OpenAI API key (or set OPENAI_API_KEY in .env)')
    parser.add_argument('--chroma-dir', default='./chroma_db_openai', help='ChromaDB persist directory')
    parser.add_argument('--collection-name', default='nasa_space_missions_text', help='Collection name')
    parser.add_argument('--embedding-model', default='text-embedding-3-small', help='OpenAI embedding model')
    parser.add_argument('--chunk-size', type=int, default=500, help='Text chunk size')
    parser.add_argument('--chunk-overlap', type=int, default=100, help='Chunk overlap size')
    parser.add_argument('--batch-size', type=int, default=50, help='Batch size for processing')
    parser.add_argument('--update-mode', choices=['skip', 'update', 'replace', 'incremental'], default='incremental',
                       help='How to handle existing documents: skip, update, replace, or incremental (manifest-based)')
    parser.add_argument('--test-query', help='Test query after processing')
    parser.add_argument('--stats-only', action='store_true', help='Only show collection statistics')
    parser.add_argument('--delete-source', help='Delete all documents from a specific source pattern')
    
    args = parser.parse_args()
    
    openai_key = args.openai_key or get_openai_api_key(include_chroma_fallback=False)
    if not openai_key:
        logger.error("OpenAI API key not found. Provide --openai-key or set OPENAI_API_KEY in .env")
        return
 
    logger.info("Initializing ChromaDB Embedding Pipeline...")
    pipeline = ChromaEmbeddingPipelineTextOnly(
        openai_api_key=openai_key,
        chroma_persist_directory=args.chroma_dir,
        collection_name=args.collection_name,
        embedding_model=args.embedding_model,
        chunk_size=args.chunk_size,
        chunk_overlap=args.chunk_overlap
    )
    
    if args.delete_source:
        deleted_count = pipeline.delete_documents_by_source(args.delete_source)
        logger.info(f"Deleted {deleted_count} documents matching source pattern: {args.delete_source}")
        return
    
    if args.stats_only:
        logger.info("Collection Statistics:")
        stats = pipeline.get_collection_stats()
        for key, value in stats.items():
            logger.info(f"{key}: {value}")
        return
    
    logger.info(f"Starting text data processing with update mode: {args.update_mode}")
    start_time = time.time()
    
    stats = pipeline.process_all_text_data(
        args.data_path,
        update_mode=args.update_mode,
        batch_size=args.batch_size,
    )
    
    end_time = time.time()
    processing_time = end_time - start_time
    
    logger.info("=" * 60)
    logger.info("PROCESSING COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Files processed: {stats['files_processed']}")
    logger.info(f"Total chunks created: {stats['total_chunks']}")
    logger.info(f"Documents added to collection: {stats['documents_added']}")
    logger.info(f"Documents updated in collection: {stats['documents_updated']}")
    logger.info(f"Documents skipped (already exist): {stats['documents_skipped']}")
    logger.info(f"Errors: {stats['errors']}")
    logger.info(f"Processing time: {processing_time:.2f} seconds")
    logger.info("\nMission breakdown:")
    for mission, mission_stats in stats['missions'].items():
        logger.info(f"  {mission}: {mission_stats['files']} files, {mission_stats['chunks']} chunks")
        logger.info(f"    Added: {mission_stats['added']}, Updated: {mission_stats['updated']}, Skipped: {mission_stats['skipped']}")
    
    collection_info = pipeline.get_collection_info()
    logger.info(f"\nCollection: {collection_info.get('collection_name', 'N/A')}")
    logger.info(f"Total documents in collection: {collection_info.get('document_count', 'N/A')}")
    
    if args.test_query:
        logger.info(f"\nTesting query: '{args.test_query}'")
        results = pipeline.query_collection(args.test_query)
        if results and 'documents' in results:
            logger.info(f"Found {len(results['documents'][0])} results:")
            for i, doc in enumerate(results['documents'][0][:3]):  # Show top 3
                logger.info(f"Result {i+1}: {doc[:200]}...")
    
    logger.info("Pipeline completed successfully!")

if __name__ == "__main__":
    main()
