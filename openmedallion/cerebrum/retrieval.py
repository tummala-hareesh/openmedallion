"""cerebrum/retrieval.py — embedding-based ranking via ChromaDB (RAG roadmap
Phase 2, build order steps 9-10: dynamic few-shot retrieval + schema pruning).

ChromaDB is used purely as an embedding function provider here — with a
typical corpus of a few dozen short texts (example questions, or table/column
descriptions), ranking itself is plain cosine similarity over the embedded
vectors, not a full vector-DB query. This keeps the ranking logic
(:func:`embed_payloads`/:func:`rank_payloads`) pure and testable without
chromadb installed at all — an injectable ``embed_fn`` stands in for it in
tests, matching the ``_client=`` injection pattern already used by
``cerebrum/llm.py``/``cerebrum/pipeline.py``.

``embed_payloads``/``rank_payloads`` are generic over any payload type (a
``text_fn`` extracts the string to embed) — ``embed_examples``/
``rank_examples`` (step 9, ranking ``{"question", "sql"}`` dicts) and
``cerebrum/schema.py``'s table-relevance ranking (step 10, ranking
``(table_name, TableMeta)`` pairs) are both thin specializations over the
same generic core, generalized once there were two real call sites.

Locked design decisions (see CLAUDE.md "Roadmap: RAG Accuracy Improvement"):
- Embeddings are rebuilt lazily and cached **per `CerebrumPipeline` instance**
  — no persisted index file on disk, no cache-invalidation logic needed.
  ``embed_*()`` (called once per instance, on first ``ask()``) is separate
  from ``rank_*()`` (called once per question) precisely so the pipeline can
  cache the former and always recompute the latter.
- Only ``verified: true`` examples from ``synthetic.jsonl`` are eligible —
  unreviewed drafts have no accuracy guarantee. Similarly, only
  ``status: approved`` silver-layer tables are eligible for schema pruning
  (see ``cerebrum/schema.py:rank_relevant_tables()``).
- **Confidence-gated fallback (build order step "confidence signal"):** a
  single score — the top-1 cosine similarity from schema-table ranking only
  (not blended with few-shot example ranking, matching the locked "no
  multi-dimensional confidence scoring" decision) — gates whether schema
  pruning trusts the curated ``metadata.yaml`` corpus or falls back to a
  ChromaDB-embedded search over every silver table's raw DDL, regardless of
  metadata/approval status (see ``cerebrum/schema.py:rank_relevant_tables_scored``/
  ``describe_all_tables``, and ``CerebrumPipeline._get_relevant_tables``).
  ``CONFIDENCE_THRESHOLD`` below is that gate.
"""
from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import TypeVar

EmbedFn = Callable[[list[str]], list[list[float]]]

T = TypeVar("T")

#: Below this top-1 cosine similarity, structured (metadata.yaml-backed)
#: schema retrieval is considered unreliable and callers should fall back to
#: a broader raw-schema search. Locked value from CLAUDE.md's RAG roadmap.
CONFIDENCE_THRESHOLD = 0.7


def get_embed_fn() -> EmbedFn:
    """Return a ChromaDB-backed embedding function.

    Lazily imports ``chromadb`` — requires the ``openmedallion[cerebrum]``
    optional extra. Not called unless dynamic retrieval is actually used.
    """
    from chromadb.utils import embedding_functions

    ef = embedding_functions.DefaultEmbeddingFunction()

    def _embed(texts: list[str]) -> list[list[float]]:
        return list(ef(texts))

    return _embed


def load_verified_examples(examples_dir: str | Path) -> list[dict[str, str]]:
    """Read ``verified: true`` examples from ``<examples_dir>/synthetic.jsonl``.

    Returns an empty list if the file doesn't exist or has no verified entries.
    """
    path = Path(examples_dir) / "synthetic.jsonl"
    if not path.exists():
        return []

    examples: list[dict[str, str]] = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            if obj.get("verified"):
                examples.append({"question": obj["question"], "sql": obj["sql"]})
    return examples


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    dot     = sum(x * y for x, y in zip(a, b))
    norm_a  = sum(x * x for x in a) ** 0.5
    norm_b  = sum(y * y for y in b) ** 0.5
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def embed_payloads(
    payloads: list[T], text_fn: Callable[[T], str], embed_fn: EmbedFn
) -> list[tuple[T, list[float]]]:
    """Embed ``text_fn(payload)`` for every payload in a single batch call.

    Returns ``[(payload, vector), ...]`` — pair this with :func:`rank_payloads`
    to rank against a specific query without re-embedding the corpus.
    """
    if not payloads:
        return []
    vectors = embed_fn([text_fn(p) for p in payloads])
    return list(zip(payloads, vectors))


def rank_payloads_scored(
    query: str,
    embedded_payloads: list[tuple[T, list[float]]],
    embed_fn: EmbedFn,
    top_k: int = 3,
) -> list[tuple[T, float]]:
    """Return up to *top_k* ``(payload, similarity)`` pairs, most similar first.

    Same ranking as :func:`rank_payloads` but keeps the cosine similarity
    score alongside each payload — needed by callers that gate behavior on
    the top score (see ``CONFIDENCE_THRESHOLD``), not just the ranked order.

    Returns ``[]`` if ``embedded_payloads`` is empty.
    """
    if not embedded_payloads:
        return []
    query_vector = embed_fn([query])[0]
    ranked = sorted(
        (
            (payload, _cosine_similarity(vector, query_vector))
            for payload, vector in embedded_payloads
        ),
        key=lambda pair: pair[1],
        reverse=True,
    )
    return ranked[:top_k]


def rank_payloads(
    query: str,
    embedded_payloads: list[tuple[T, list[float]]],
    embed_fn: EmbedFn,
    top_k: int = 3,
) -> list[T]:
    """Return up to *top_k* payloads most similar to *query*.

    Args:
        query: The text to rank against (a question, for either use case).
        embedded_payloads: Output of :func:`embed_payloads` — pre-computed
            once per pipeline instance, reused across queries.
        embed_fn: Same embedding function used to build ``embedded_payloads``
            (embeddings from different functions aren't comparable).
        top_k: Maximum number of payloads to return.

    Returns:
        list[T]: The original payloads, ranked by cosine similarity, most
        similar first. Empty if ``embedded_payloads`` is empty.
    """
    return [payload for payload, _score in rank_payloads_scored(query, embedded_payloads, embed_fn, top_k)]


def embed_examples(
    examples: list[dict[str, str]], embed_fn: EmbedFn
) -> list[tuple[dict[str, str], list[float]]]:
    """Embed every example's ``question`` — a thin specialization of
    :func:`embed_payloads` for ``{"question", "sql"}`` dicts."""
    return embed_payloads(examples, lambda e: e["question"], embed_fn)


def rank_examples(
    question: str,
    embedded_examples: list[tuple[dict[str, str], list[float]]],
    embed_fn: EmbedFn,
    top_k: int = 3,
) -> list[dict[str, str]]:
    """Return up to *top_k* examples most similar to *question* — a thin
    specialization of :func:`rank_payloads`."""
    return rank_payloads(question, embedded_examples, embed_fn, top_k)
