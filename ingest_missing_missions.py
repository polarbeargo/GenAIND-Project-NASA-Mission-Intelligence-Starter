#!/usr/bin/env python3
"""Targeted mission-only ingestion helper for faster and cheaper backfills."""

import argparse
import json
import logging
import time
from typing import List, Optional

from embedding_pipeline import ChromaEmbeddingPipelineTextOnly
from openai_config import get_openai_api_key


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def parse_missions(values: List[str]) -> List[str]:
    """Parse missions from CLI values, allowing comma-separated entries."""
    missions: List[str] = []
    for value in values:
        missions.extend([item.strip() for item in value.split(",") if item.strip()])
    if not missions:
        raise ValueError("At least one mission is required")
    return missions


def main() -> None:
    parser = argparse.ArgumentParser(description="Targeted mission-only embedding ingestion")
    parser.add_argument(
        "--missions",
        nargs="+",
        required=True,
        help="Mission filter(s), e.g. challenger or apollo_13,apollo11",
    )
    parser.add_argument("--data-path", default="./data_text", help="Path to mission data folders")
    parser.add_argument("--chroma-dir", default="./chroma_db_openai", help="ChromaDB persist directory")
    parser.add_argument("--collection-name", default="nasa_space_missions_text", help="Collection name")
    parser.add_argument("--embedding-model", default="text-embedding-3-small", help="Embedding model")
    parser.add_argument("--batch-size", type=int, default=50, help="Batch size")
    parser.add_argument(
        "--update-mode",
        choices=["skip", "update", "replace", "incremental"],
        default="incremental",
        help="Update mode (incremental recommended for resume)",
    )
    parser.add_argument(
        "--checkpoint-manifest-each-file",
        action="store_true",
        help="Save incremental manifest after each processed file",
    )
    parser.add_argument(
        "--fast-upsert",
        action="store_true",
        help="Use batched upsert fast path in non-incremental modes",
    )
    args = parser.parse_args()

    openai_key = get_openai_api_key(include_chroma_fallback=False)
    if not openai_key:
        raise RuntimeError("OpenAI API key not found. Set OPENAI_API_KEY in .env")

    mission_filters = parse_missions(args.missions)

    pipeline = ChromaEmbeddingPipelineTextOnly(
        openai_api_key=openai_key,
        chroma_persist_directory=args.chroma_dir,
        collection_name=args.collection_name,
        embedding_model=args.embedding_model,
    )

    logger.info("Starting targeted ingestion for missions: %s", ", ".join(mission_filters))
    start = time.time()

    stats = pipeline.process_all_text_data(
        base_path=args.data_path,
        update_mode=args.update_mode,
        batch_size=args.batch_size,
        missions=mission_filters,
        checkpoint_manifest_each_file=args.checkpoint_manifest_each_file,
        fast_upsert=args.fast_upsert,
    )

    elapsed = time.time() - start
    logger.info("Targeted ingestion finished in %.2fs", elapsed)
    logger.info("Summary: %s", json.dumps(stats, ensure_ascii=True))


if __name__ == "__main__":
    main()
