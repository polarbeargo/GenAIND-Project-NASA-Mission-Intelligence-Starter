#!/usr/bin/env python3
"""
Create a mock ChromaDB collection for testing without OpenAI API key
"""

import sys

import chromadb
from chromadb.config import Settings
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from env_utils import load_project_env

load_project_env(__file__)

def create_mock_chroma():
    """Create a mock ChromaDB with sample data for testing"""
    
    chroma_dir = Path("./chroma_db")
    chroma_dir.mkdir(exist_ok=True)
    
    client = chromadb.PersistentClient(
        path=str(chroma_dir),
        settings=Settings(anonymized_telemetry=False)
    )
    
    collection = client.get_or_create_collection(
        name="nasa_space_missions_test",
        metadata={"description": "Test collection for NASA space missions"}
    )
    
    sample_docs = [
        "Apollo 11 was the mission that landed humans on the moon in 1969. Neil Armstrong was the first person to set foot on the lunar surface.",
        "Apollo 13 experienced an oxygen tank explosion but the crew safely returned to Earth thanks to quick thinking and teamwork.",
        "The Space Shuttle Challenger disaster occurred in 1986 when the shuttle broke apart during launch due to O-ring failure.",
        "NASA's Artemis program aims to return humans to the Moon and establish a sustainable presence there.",
        "The International Space Station is a collaborative project between multiple space agencies orbiting Earth.",
    ]
    
    sample_metadatas = [
        {"mission": "apollo_11", "source": "apollo11_facts", "document_category": "mission_overview"},
        {"mission": "apollo_13", "source": "apollo13_facts", "document_category": "mission_overview"},
        {"mission": "challenger", "source": "challenger_facts", "document_category": "mission_overview"},
        {"mission": "artemis", "source": "artemis_facts", "document_category": "mission_overview"},
        {"mission": "iss", "source": "iss_facts", "document_category": "mission_overview"},
    ]
    
    for i, (doc, metadata) in enumerate(zip(sample_docs, sample_metadatas)):
        collection.add(
            ids=[f"doc_{i}"],
            documents=[doc],
            metadatas=[metadata]
        )
    
    print(f"✓ Mock ChromaDB created at: {chroma_dir}")
    print(f"✓ Collection 'nasa_space_missions_test' created with {len(sample_docs)} sample documents")
    print("\nYou can now run the chat app:")
    print("  uv run streamlit run chat.py")
    print("\nTo replace with real data later, run:")
    print("  uv run python embedding_pipeline.py --openai-key YOUR_KEY --data-path ./data_text")

if __name__ == "__main__":
    create_mock_chroma()
