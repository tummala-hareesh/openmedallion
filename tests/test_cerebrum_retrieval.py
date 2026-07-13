"""tests/test_cerebrum_retrieval.py — RAG roadmap Phase 2, build order step 9:
dynamic few-shot retrieval (openmedallion.cerebrum.retrieval).

Locked design decisions:
- Embeddings are rebuilt lazily, cached per CerebrumPipeline instance (not
  persisted to disk) — see tests/test_cerebrum.py for the pipeline-level
  caching behavior; this file covers the pure retrieval logic in isolation.
- ChromaDB is only used as an embedding function provider — ranking itself
  is plain cosine similarity, so these tests never need chromadb installed
  (an injectable _embed_fn stands in for it, matching the _client= pattern
  used throughout cerebrum/).
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from openmedallion.cerebrum.retrieval import (
    CONFIDENCE_THRESHOLD,
    embed_examples,
    embed_payloads,
    load_verified_examples,
    rank_examples,
    rank_payloads,
    rank_payloads_scored,
)


def _fake_embed_fn(vectors: dict[str, list[float]]):
    """Return a deterministic embed_fn: text -> pre-assigned vector."""
    def _embed(texts: list[str]) -> list[list[float]]:
        return [vectors[t] for t in texts]
    return _embed


class TestLoadVerifiedExamples:

    def _write(self, tmp_path: Path, lines: list[dict]) -> Path:
        examples_dir = tmp_path / "examples"
        examples_dir.mkdir()
        with open(examples_dir / "synthetic.jsonl", "w") as f:
            for line in lines:
                f.write(json.dumps(line) + "\n")
        return examples_dir

    def test_returns_only_verified(self, tmp_path):
        examples_dir = self._write(tmp_path, [
            {"question": "a", "sql": "SELECT 1", "verified": True},
            {"question": "b", "sql": "SELECT 2", "verified": False},
        ])
        result = load_verified_examples(examples_dir)
        assert [e["question"] for e in result] == ["a"]

    def test_missing_file_returns_empty(self, tmp_path):
        assert load_verified_examples(tmp_path / "examples") == []

    def test_empty_file_returns_empty(self, tmp_path):
        examples_dir = self._write(tmp_path, [])
        assert load_verified_examples(examples_dir) == []


class TestEmbedAndRank:

    def test_ranks_by_cosine_similarity(self):
        examples = [
            {"question": "revenue by region", "sql": "SELECT region, SUM(amount) FROM orders GROUP BY region"},
            {"question": "how many customers", "sql": "SELECT COUNT(*) FROM customers"},
        ]
        vectors = {
            "revenue by region":  [1.0, 0.0],
            "how many customers": [0.0, 1.0],
            "total revenue per region": [0.9, 0.1],  # closer to "revenue by region"
        }
        embed_fn = _fake_embed_fn(vectors)
        embedded = embed_examples(examples, embed_fn)
        ranked = rank_examples("total revenue per region", embedded, embed_fn, top_k=1)

        assert len(ranked) == 1
        assert ranked[0]["question"] == "revenue by region"

    def test_top_k_limits_results(self):
        examples = [{"question": f"q{i}", "sql": f"SELECT {i}"} for i in range(5)]
        vectors = {f"q{i}": [float(i), 0.0] for i in range(5)}
        vectors["query"] = [2.0, 0.0]
        embed_fn = _fake_embed_fn(vectors)
        embedded = embed_examples(examples, embed_fn)
        ranked = rank_examples("query", embedded, embed_fn, top_k=3)
        assert len(ranked) == 3

    def test_fewer_examples_than_top_k_returns_all(self):
        examples = [{"question": "only one", "sql": "SELECT 1"}]
        vectors = {"only one": [1.0, 0.0], "query": [1.0, 0.0]}
        embed_fn = _fake_embed_fn(vectors)
        embedded = embed_examples(examples, embed_fn)
        ranked = rank_examples("query", embedded, embed_fn, top_k=3)
        assert len(ranked) == 1

    def test_empty_examples_returns_empty(self):
        embed_fn = _fake_embed_fn({"query": [1.0, 0.0]})
        assert embed_examples([], embed_fn) == []
        assert rank_examples("query", [], embed_fn, top_k=3) == []

    def test_embed_examples_only_embeds_questions_once(self):
        # embed_fn should be called with all question texts in a single batch,
        # not once per example — matters for real embedding-API efficiency.
        calls: list[list[str]] = []
        def _embed(texts: list[str]) -> list[list[float]]:
            calls.append(texts)
            return [[1.0, 0.0] for _ in texts]

        examples = [{"question": "a", "sql": "SELECT 1"}, {"question": "b", "sql": "SELECT 2"}]
        embed_examples(examples, _embed)
        assert len(calls) == 1
        assert calls[0] == ["a", "b"]


class TestGenericEmbedAndRankPayloads:
    """embed_payloads/rank_payloads are the generic form embed_examples/
    rank_examples now wrap — schema.py's table-relevance ranking (build order
    step 10) is the second real call site that justified generalizing."""

    def test_embed_payloads_arbitrary_type(self):
        # Payloads need not be dicts — any type text_fn can extract text from.
        payloads = [("orders", "Order line items"), ("products", "Product catalog")]
        calls: list[list[str]] = []
        def _embed(texts: list[str]) -> list[list[float]]:
            calls.append(texts)
            return [[1.0, 0.0] for _ in texts]

        embedded = embed_payloads(payloads, lambda p: p[1], _embed)
        assert len(embedded) == 2
        assert calls == [["Order line items", "Product catalog"]]

    def test_rank_payloads_by_similarity(self):
        payloads = [("orders", "revenue by region"), ("customers", "how many customers")]
        vectors = {
            "revenue by region":  [1.0, 0.0],
            "how many customers": [0.0, 1.0],
            "total revenue per region": [0.9, 0.1],
        }
        embed_fn = _fake_embed_fn(vectors)
        embedded = embed_payloads(payloads, lambda p: p[1], embed_fn)
        ranked = rank_payloads("total revenue per region", embedded, embed_fn, top_k=1)
        assert ranked == [("orders", "revenue by region")]

    def test_embed_examples_is_a_thin_wrapper(self):
        # Sanity check that the specialization didn't diverge from the generic path.
        examples = [{"question": "q1", "sql": "SELECT 1"}]
        embed_fn = _fake_embed_fn({"q1": [1.0, 0.0]})
        via_generic  = embed_payloads(examples, lambda e: e["question"], embed_fn)
        via_specific = embed_examples(examples, embed_fn)
        assert via_generic == via_specific


class TestRankPayloadsScored:
    """rank_payloads_scored backs the confidence-gated fallback (schema.py's
    rank_relevant_tables_scored + CerebrumPipeline._get_relevant_tables) —
    callers need the raw top score, not just the ranked order."""

    def test_returns_payload_and_score_pairs_sorted_descending(self):
        payloads = [("orders", "revenue by region"), ("customers", "how many customers")]
        vectors = {
            "revenue by region":  [1.0, 0.0],
            "how many customers": [0.0, 1.0],
            "total revenue per region": [0.9, 0.1],
        }
        embed_fn = _fake_embed_fn(vectors)
        embedded = embed_payloads(payloads, lambda p: p[1], embed_fn)
        ranked = rank_payloads_scored("total revenue per region", embedded, embed_fn, top_k=2)
        assert [p for p, _ in ranked] == payloads  # orders first, higher similarity
        assert ranked[0][1] > ranked[1][1]
        assert ranked[0][1] == pytest.approx(1.0, abs=0.01)

    def test_rank_payloads_is_consistent_with_scored_version(self):
        # rank_payloads is now a thin wrapper over rank_payloads_scored —
        # confirm it didn't diverge.
        payloads = [("orders", "revenue by region"), ("customers", "how many customers")]
        vectors = {
            "revenue by region":  [1.0, 0.0],
            "how many customers": [0.0, 1.0],
            "total revenue per region": [0.9, 0.1],
        }
        embed_fn = _fake_embed_fn(vectors)
        embedded = embed_payloads(payloads, lambda p: p[1], embed_fn)
        via_plain  = rank_payloads("total revenue per region", embedded, embed_fn, top_k=1)
        via_scored = [p for p, _ in rank_payloads_scored("total revenue per region", embedded, embed_fn, top_k=1)]
        assert via_plain == via_scored

    def test_empty_embedded_payloads_returns_empty(self):
        embed_fn = _fake_embed_fn({})
        assert rank_payloads_scored("q", [], embed_fn) == []

    def test_confidence_threshold_is_the_locked_value(self):
        assert CONFIDENCE_THRESHOLD == 0.7
