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

    def query(self, query_texts, n_results, where=None):
        self.calls.append(
            {
                "query_texts": query_texts,
                "n_results": n_results,
                "where": where,
            }
        )
        return self._result_payload


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


if __name__ == "__main__":
    unittest.main(verbosity=2)
