import tempfile
import unittest
from pathlib import Path

from embedding_pipeline import ChromaEmbeddingPipelineTextOnly


class FakeCollection:
    def __init__(self):
        self.upsert_calls = []
        self.delete_calls = []

    def upsert(self, ids, documents, metadatas, embeddings):
        self.upsert_calls.append(
            {
                "ids": list(ids),
                "documents": list(documents),
                "metadatas": list(metadatas),
                "embeddings": list(embeddings),
            }
        )

    def delete(self, ids):
        self.delete_calls.append(list(ids))


class TestEmbeddingPipelineIncremental(unittest.TestCase):
    def _make_pipeline_stub(self, workdir: Path) -> ChromaEmbeddingPipelineTextOnly:
        pipeline = ChromaEmbeddingPipelineTextOnly.__new__(ChromaEmbeddingPipelineTextOnly)
        pipeline.collection = FakeCollection()
        pipeline.collection_name = "nasa_space_missions_text"
        pipeline.chunk_size = 500
        pipeline.chunk_overlap = 100
        pipeline.embedding_model = "text-embedding-3-small"
        pipeline.manifest_path = workdir / "manifest.json"
        pipeline.manifest = {"files": {}}
        pipeline.get_embeddings_batch = lambda texts: [[float(i)] for i, _ in enumerate(texts)]
        return pipeline

    def test_incremental_skips_unchanged_file_by_file_hash(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            workdir = Path(tmpdir)
            pipeline = self._make_pipeline_stub(workdir)
            file_path = workdir / "apollo11" / "sample.txt"
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_text("unchanged content", encoding="utf-8")

            file_key = pipeline._manifest_file_key(file_path)
            file_hash = pipeline._hash_text("unchanged content")
            pipeline.manifest["files"][file_key] = {
                "file_hash": file_hash,
                "chunk_count": 3,
                "chunk_hashes": {"id1": "h1", "id2": "h2", "id3": "h3"},
                "doc_ids": ["id1", "id2", "id3"],
            }

            stats = pipeline._process_file_incremental(file_path=file_path, batch_size=8)

            self.assertEqual("unchanged_file", stats["status"])
            self.assertEqual(3, stats["chunks"])
            self.assertEqual(3, stats["skipped"])
            self.assertEqual([], pipeline.collection.upsert_calls)
            self.assertEqual([], pipeline.collection.delete_calls)

    def test_incremental_upserts_only_changed_chunks_and_deletes_stale(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            workdir = Path(tmpdir)
            pipeline = self._make_pipeline_stub(workdir)
            file_path = workdir / "apollo13" / "log.txt"
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_text("new file content", encoding="utf-8")

            doc0 = "apollo_13_log_chunk_0000"
            doc1 = "apollo_13_log_chunk_0001"
            stale_doc = "apollo_13_log_chunk_0002"
            unchanged_chunk = "stable chunk text"
            changed_chunk_old = "old changed chunk"
            changed_chunk_new = "new changed chunk"

            file_key = pipeline._manifest_file_key(file_path)
            pipeline.manifest["files"][file_key] = {
                "file_hash": "old-file-hash",
                "chunk_count": 3,
                "chunk_hashes": {
                    doc0: pipeline._hash_text(unchanged_chunk),
                    doc1: pipeline._hash_text(changed_chunk_old),
                    stale_doc: pipeline._hash_text("stale"),
                },
                "doc_ids": [doc0, doc1, stale_doc],
            }

            pipeline._documents_from_content = lambda _path, _content: [
                (
                    unchanged_chunk,
                    {"mission": "apollo_13", "source": "log", "chunk_index": 0},
                ),
                (
                    changed_chunk_new,
                    {"mission": "apollo_13", "source": "log", "chunk_index": 1},
                ),
            ]

            stats = pipeline._process_file_incremental(file_path=file_path, batch_size=10)

            self.assertEqual("processed", stats["status"])
            self.assertEqual(2, stats["chunks"])
            self.assertEqual(0, stats["added"])
            self.assertEqual(1, stats["updated"])
            self.assertEqual(1, stats["skipped"])
            self.assertEqual(1, stats["deleted"])

            self.assertEqual(1, len(pipeline.collection.upsert_calls))
            self.assertEqual([doc1], pipeline.collection.upsert_calls[0]["ids"])
            self.assertEqual([[stale_doc]], pipeline.collection.delete_calls)

            new_entry = pipeline.manifest["files"][file_key]
            self.assertEqual(2, new_entry["chunk_count"])
            self.assertEqual(sorted([doc0, doc1]), sorted(new_entry["doc_ids"]))
            self.assertEqual(
                pipeline._hash_text(changed_chunk_new),
                new_entry["chunk_hashes"][doc1],
            )

    def test_incremental_empty_file_removes_existing_docs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            workdir = Path(tmpdir)
            pipeline = self._make_pipeline_stub(workdir)
            file_path = workdir / "challenger" / "empty.txt"
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_text("\n\n", encoding="utf-8")

            file_key = pipeline._manifest_file_key(file_path)
            pipeline.manifest["files"][file_key] = {
                "file_hash": "old-hash",
                "chunk_count": 2,
                "chunk_hashes": {"d1": "h1", "d2": "h2"},
                "doc_ids": ["d1", "d2"],
            }

            stats = pipeline._process_file_incremental(file_path=file_path, batch_size=4)

            self.assertEqual("empty", stats["status"])
            self.assertEqual(2, stats["deleted"])
            self.assertEqual(1, len(pipeline.collection.delete_calls))
            self.assertEqual(set(["d1", "d2"]), set(pipeline.collection.delete_calls[0]))

            updated_entry = pipeline.manifest["files"][file_key]
            self.assertEqual(0, updated_entry["chunk_count"])
            self.assertEqual({}, updated_entry["chunk_hashes"])
            self.assertEqual([], updated_entry["doc_ids"])

    def test_process_all_text_data_incremental_prunes_missing_manifest_entries(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            workdir = Path(tmpdir)
            pipeline = self._make_pipeline_stub(workdir)

            existing_file = workdir / "apollo11" / "keep.txt"
            existing_file.parent.mkdir(parents=True, exist_ok=True)
            existing_file.write_text("keep", encoding="utf-8")

            removed_file = workdir / "apollo11" / "removed.txt"
            removed_key = pipeline._manifest_file_key(removed_file)
            pipeline.manifest["files"][removed_key] = {
                "file_hash": "old",
                "doc_ids": ["old_doc_1", "old_doc_2"],
            }

            pipeline.scan_text_files_only = lambda _base: [existing_file]
            pipeline._process_file_incremental = lambda file_path, batch_size: {
                "chunks": 2,
                "added": 1,
                "updated": 0,
                "skipped": 1,
                "deleted": 0,
                "status": "processed",
                "error": "",
            }
            pipeline._persist_run_summary_artifacts = lambda run_rows, base_path, update_mode: {}

            saved = {"called": False}
            pipeline._save_manifest = lambda: saved.__setitem__("called", True)

            stats = pipeline.process_all_text_data(
                base_path=str(workdir),
                update_mode="incremental",
                batch_size=16,
            )

            self.assertTrue(saved["called"])
            self.assertEqual(1, stats["files_processed"])
            self.assertEqual(2, stats["total_chunks"])
            self.assertEqual(1, stats["documents_added"])
            self.assertEqual(1, stats["documents_skipped"])
            self.assertNotIn(removed_key, pipeline.manifest["files"])
            self.assertEqual(1, len(pipeline.collection.delete_calls))
            self.assertEqual(set(["old_doc_1", "old_doc_2"]), set(pipeline.collection.delete_calls[0]))


if __name__ == "__main__":
    unittest.main()
