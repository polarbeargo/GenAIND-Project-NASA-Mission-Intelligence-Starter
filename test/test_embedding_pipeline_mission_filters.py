#!/usr/bin/env python3
"""Regression tests for mission alias parsing and mission-scoped scanning."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from embedding_pipeline import ChromaEmbeddingPipelineTextOnly
from ingest_missing_missions import parse_missions


class TestMissionAliasParsing(unittest.TestCase):
    def test_normalize_mission_aliases(self):
        normalize = ChromaEmbeddingPipelineTextOnly.normalize_mission_name

        self.assertEqual(normalize("apollo11"), "apollo_11")
        self.assertEqual(normalize("Apollo 11"), "apollo_11")
        self.assertEqual(normalize("apollo-13"), "apollo_13")
        self.assertEqual(normalize("apollo_13"), "apollo_13")
        self.assertEqual(normalize("challenger"), "challenger")

    def test_normalize_mission_aliases_invalid(self):
        normalize = ChromaEmbeddingPipelineTextOnly.normalize_mission_name
        self.assertIsNone(normalize("voyager"))

    def test_helper_parse_missions_supports_commas_and_spaces(self):
        missions = parse_missions(["challenger,apollo13", "apollo_11"])
        self.assertEqual(missions, ["challenger", "apollo13", "apollo_11"])


class TestMissionFilterScan(unittest.TestCase):
    def setUp(self):
        self._temp_dir = tempfile.TemporaryDirectory()
        self.base_path = Path(self._temp_dir.name)
        self.pipeline = ChromaEmbeddingPipelineTextOnly.__new__(ChromaEmbeddingPipelineTextOnly)

        self._write("apollo11", "a11_1.txt", "Apollo 11 content")
        self._write("apollo11", "summary_notes.txt", "Should be ignored")
        self._write("apollo11", ".hidden.txt", "Should be ignored")

        self._write("apollo13", "a13_1.txt", "Apollo 13 content")
        self._write("challenger", "ch_1.txt", "Challenger content")

    def tearDown(self):
        self._temp_dir.cleanup()

    def _write(self, folder: str, name: str, content: str) -> None:
        path = self.base_path / folder
        path.mkdir(parents=True, exist_ok=True)
        (path / name).write_text(content, encoding="utf-8")

    def test_scan_without_filter_returns_all_supported_missions(self):
        files = self.pipeline.scan_text_files_only(str(self.base_path))
        self.assertEqual(len(files), 3)

        missions = {self.pipeline.extract_mission_from_path(path) for path in files}
        self.assertEqual(missions, {"apollo_11", "apollo_13", "challenger"})

    def test_scan_with_alias_filter_returns_only_selected_missions(self):
        files = self.pipeline.scan_text_files_only(
            str(self.base_path),
            missions=["Apollo 13", "challenger"],
        )

        self.assertEqual(len(files), 2)
        missions = {self.pipeline.extract_mission_from_path(path) for path in files}
        self.assertEqual(missions, {"apollo_13", "challenger"})

    def test_scan_with_unknown_mission_raises(self):
        with self.assertRaises(ValueError):
            self.pipeline.scan_text_files_only(str(self.base_path), missions=["voyager"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
