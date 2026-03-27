#!/usr/bin/env python3
"""
Process a single sample file to populate chroma_db_openai with real NASA data
"""
import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))
from env_utils import load_project_env
from openai_config import get_openai_api_key
from embedding_pipeline import ChromaEmbeddingPipelineTextOnly

load_project_env(__file__)

api_key = get_openai_api_key(include_chroma_fallback=False)
if not api_key:
    print("ERROR: OPENAI_API_KEY not set")
    exit(1)

print("Initializing pipeline...")
pipeline = ChromaEmbeddingPipelineTextOnly(
    openai_api_key=api_key,
    chroma_persist_directory='./chroma_db_openai',
    collection_name='nasa_space_missions_text'
)

data_path = Path('./data_text')
text_files = list(data_path.glob('*/*.txt'))

if not text_files:
    print("ERROR: No text files found")
    exit(1)

print(f"Processing {len(text_files)} files...")
print("This may take a minute or two for embeddings...\n")

stats = pipeline.process_all_text_data('./data_text', update_mode='skip')

print("\n=== PROCESSING COMPLETE ===")
print(f"Files processed: {stats['files_processed']}")
print(f"Total chunks: {stats['total_chunks']}")
print(f"Documents added: {stats['documents_added']}")
print(f"Total documents in collection: {pipeline.get_collection_info().get('document_count', 'N/A')}")
