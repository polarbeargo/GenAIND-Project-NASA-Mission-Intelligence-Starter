#!/usr/bin/env python3
"""
Quick population of ChromaDB with NASA mission data
Processes documents and creates embeddings using your OPENAI_API_KEY from .env
"""
import os
import sys
from pathlib import Path

from env_utils import load_project_env
from openai_config import get_openai_api_key

load_project_env(__file__)
api_key = get_openai_api_key(include_chroma_fallback=False)

if not api_key:
    print("❌ ERROR: OPENAI_API_KEY not found in .env")
    print("   Please add: OPENAI_API_KEY=your-key-here")
    sys.exit(1)

print("✓ API key loaded from .env")
print("\n⏳ Starting document processing...")
print("   This will generate embeddings for all NASA mission files.")
print("   Processing time depends on file count and size.\n")

from embedding_pipeline import ChromaEmbeddingPipelineTextOnly

try:
    pipeline = ChromaEmbeddingPipelineTextOnly(
        openai_api_key=api_key,
        chroma_persist_directory='./chroma_db_openai',
        collection_name='nasa_space_missions_text',
        chunk_size=500,
        chunk_overlap=100
    )
    
    stats = pipeline.process_all_text_data('./data_text', update_mode='skip')
    
    print("\n" + "="*60)
    print("✓ PROCESSING COMPLETE")
    print("="*60)
    print(f"Files processed: {stats['files_processed']}")
    print(f"Total chunks created: {stats['total_chunks']}")
    print(f"Documents added: {stats['documents_added']}")
    
    info = pipeline.get_collection_info()
    print(f"Total documents in collection: {info.get('document_count', 'N/A')}")
    
    print("\n✓ Ready to chat! Run:")
    print("  uv run streamlit run chat.py")
    
except Exception as e:
    print(f"\n❌ Error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
