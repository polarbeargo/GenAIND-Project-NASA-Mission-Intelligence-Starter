#!/usr/bin/env python3
"""Unit tests for two-stage retrieval and local reranking."""

from __future__ import annotations

import unittest
from unittest.mock import patch

import rag_client


class FakeCollection:
    """Simple fake Chroma collection for deterministic retrieval tests."""

    def __init__(self, result_payload):
        self._result_payload = result_payload
        self.calls = []
        self.name = "nasa_space_missions_text"

    def query(self, query_texts, n_results, where=None, where_document=None):
        self.calls.append(
            {
                "query_texts": query_texts,
                "n_results": n_results,
                "where": where,
                "where_document": where_document,
            }
        )
        return self._result_payload


class FakeHybridCollection:
    """Fake collection that returns distinct payloads for semantic vs keyword probes."""

    def __init__(self):
        self.name = "nasa_space_missions_text"
        self.calls = []

    def query(self, query_texts, n_results, where=None, where_document=None):
        self.calls.append(
            {
                "query_texts": query_texts,
                "n_results": n_results,
                "where": where,
                "where_document": where_document,
            }
        )
        if where_document is None:
            return {
                "documents": [["generic flight summary", "telemetry note"]],
                "metadatas": [[{"source": "a"}, {"source": "b"}]],
                "distances": [[0.05, 0.08]],
                "ids": [["a", "b"]],
            }

        token = where_document.get("$contains")
        if token == "oxygen":
            return {
                "documents": [["Apollo 13 oxygen tank explosion details"]],
                "metadatas": [[{"source": "c"}]],
                "distances": [[0.4]],
                "ids": [["c"]],
            }
        return {
            "documents": [[]],
            "metadatas": [[]],
            "distances": [[]],
            "ids": [[]],
        }


class TestTwoStageRetrieval(unittest.TestCase):
    def test_first_pass_expands_candidate_set(self):
        fake = FakeCollection(
            {
                "documents": [["doc a", "doc b", "doc c", "doc d"]],
                "metadatas": [[{}, {}, {}, {}]],
                "distances": [[0.2, 0.3, 0.4, 0.5]],
                "ids": [["a", "b", "c", "d"]],
            }
        )

        with patch.dict(
            "os.environ",
            {
                "RETRIEVAL_FIRST_PASS_MULTIPLIER": "4",
                "RETRIEVAL_FIRST_PASS_MAX_CANDIDATES": "24",
                "RETRIEVAL_HYBRID_ENABLED": "false",
            },
            clear=False,
        ), patch.object(rag_client, "VectorSecurityValidator", None):
            rag_client.retrieve_documents(fake, query="apollo 13", n_results=3)

        self.assertEqual(len(fake.calls), 1)
        self.assertEqual(fake.calls[0]["n_results"], 12)

    def test_second_pass_reranks_and_trims_to_top_n(self):
        fake = FakeCollection(
            {
                "documents": [[
                    "random unrelated text",
                    "Apollo 13 oxygen tank explosion details",
                    "weather notes",
                    "crew and mission timeline",
                ]],
                "metadatas": [[
                    {"source": "a"},
                    {"source": "b"},
                    {"source": "c"},
                    {"source": "d"},
                ]],
                "distances": [[0.01, 0.45, 0.2, 0.3]],
                "ids": [["a", "b", "c", "d"]],
            }
        )

        with patch.dict(
            "os.environ",
            {
                "RETRIEVAL_FIRST_PASS_MULTIPLIER": "4",
                "RETRIEVAL_FIRST_PASS_MAX_CANDIDATES": "24",
                "RETRIEVAL_HYBRID_ENABLED": "false",
            },
            clear=False,
        ), patch.object(rag_client, "VectorSecurityValidator", None):
            result = rag_client.retrieve_documents(
                fake,
                query="apollo 13 oxygen tank",
                n_results=2,
            )

        self.assertEqual(len(result["documents"][0]), 2)
        self.assertEqual(result["documents"][0][0], "Apollo 13 oxygen tank explosion details")
        self.assertEqual(result["ids"][0][0], "b")

    def test_mission_filter_still_applies_to_first_pass_query(self):
        fake = FakeCollection(
            {
                "documents": [["doc"]],
                "metadatas": [[{"mission": "apollo_13"}]],
                "distances": [[0.1]],
                "ids": [["x"]],
            }
        )

        with patch.object(rag_client, "VectorSecurityValidator", None):
            rag_client.retrieve_documents(
                fake,
                query="apollo 13",
                n_results=1,
                mission_filter="Apollo 13",
            )

        self.assertEqual(fake.calls[0]["where"], {"mission": "apollo_13"})

    def test_hybrid_keyword_probe_expands_candidates_and_preserves_determinism(self):
        fake = FakeHybridCollection()

        with patch.dict(
            "os.environ",
            {
                "RETRIEVAL_HYBRID_ENABLED": "true",
                "RETRIEVAL_FIRST_PASS_MULTIPLIER": "2",
                "RETRIEVAL_FIRST_PASS_MAX_CANDIDATES": "8",
                "RETRIEVAL_KEYWORD_TERM_LIMIT": "3",
                "RETRIEVAL_KEYWORD_CANDIDATES_PER_TERM": "2",
            },
            clear=False,
        ), patch.object(rag_client, "VectorSecurityValidator", None):
            result = rag_client.retrieve_documents(
                fake,
                query="apollo 13 oxygen failure",
                n_results=2,
            )

        # Semantic call + at least one keyword probe should execute.
        self.assertGreaterEqual(len(fake.calls), 2)
        self.assertIsNone(fake.calls[0]["where_document"])
        self.assertTrue(any(call["where_document"] for call in fake.calls[1:]))

        # Keyword-exact candidate should survive merge and deterministic rerank.
        self.assertEqual(result["documents"][0][0], "Apollo 13 oxygen tank explosion details")
        self.assertEqual(result["ids"][0][0], "c")

    def test_hybrid_keyword_probe_failure_falls_back_to_semantic_results(self):
        fake = FakeCollection(
            {
                "documents": [["apollo 13 incident summary"]],
                "metadatas": [[{"source": "x"}]],
                "distances": [[0.2]],
                "ids": [["x"]],
            }
        )

        def _raise_for_where_document(*args, **kwargs):
            if kwargs.get("where_document"):
                raise RuntimeError("where_document not available")
            return {
                "documents": [["apollo 13 incident summary"]],
                "metadatas": [[{"source": "x"}]],
                "distances": [[0.2]],
                "ids": [["x"]],
            }

        with patch.dict(
            "os.environ",
            {
                "RETRIEVAL_HYBRID_ENABLED": "true",
                "RETRIEVAL_KEYWORD_TERM_LIMIT": "2",
            },
            clear=False,
        ), patch.object(rag_client, "VectorSecurityValidator", None):
            with patch.object(fake, "query", side_effect=_raise_for_where_document):
                result = rag_client.retrieve_documents(
                    fake,
                    query="apollo 13 oxygen",
                    n_results=1,
                )

        self.assertEqual(result["documents"][0], ["apollo 13 incident summary"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
