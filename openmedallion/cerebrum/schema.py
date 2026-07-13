"""cerebrum/schema.py — Build LLM schema context from silver Parquet files,
with optional relevance-based pruning (RAG roadmap Phase 2, build order step 10)
and a confidence-gated raw-schema fallback (the "confidence-signal gap"
flagged in CLAUDE.md, now wired).

Reads every *.parquet file in the silver directory, uses DuckDB DESCRIBE
to extract column names and types, and returns a DDL-style context string
for injection into the LLM prompt. When a project has curated table/column
descriptions in metadata.yaml, :func:`rank_relevant_tables` can narrow the
DDL shown to just the top-k tables relevant to the current question, keeping
the prompt tight on large schemas — same embedding-based ranking technique
as ``cerebrum/retrieval.py``'s dynamic few-shot retrieval (step 9), applied
to table/column descriptions instead of example questions.

:func:`rank_relevant_tables_scored` and :func:`describe_all_tables` back the
confidence gate: when the top similarity from the curated (approved-only)
corpus falls below ``retrieval.CONFIDENCE_THRESHOLD``, ``CerebrumPipeline``
falls back to ranking against every silver table's *raw* DDL text instead —
no metadata.yaml or approval status required, since this is the path meant
to work even when curated knowledge is absent or unreliable.
"""
from __future__ import annotations

from pathlib import Path

import duckdb

from openmedallion.cerebrum import retrieval as _retrieval
from openmedallion.metadata.schema import MetadataConfig, TableMeta


def _describe_parquets(parquets: list[Path]) -> list[tuple[str, list[tuple[str, str]]]]:
    """Run DuckDB DESCRIBE over each parquet file — shared by
    :func:`build_schema_context` and :func:`describe_all_tables` so the
    view-registration/DESCRIBE loop isn't duplicated."""
    if not parquets:
        return []

    con = duckdb.connect()
    described: list[tuple[str, list[tuple[str, str]]]] = []
    try:
        for path in parquets:
            table_name = path.stem
            con.execute(
                f"CREATE OR REPLACE VIEW {table_name} AS "
                f"SELECT * FROM read_parquet('{path}')"
            )
            rows = con.execute(f"DESCRIBE {table_name}").fetchall()
            described.append((table_name, [(col, dtype) for col, dtype, *_ in rows]))
    finally:
        con.close()

    return described


def describe_all_tables(silver_dir: str | Path) -> list[tuple[str, list[tuple[str, str]]]]:
    """Return ``(table_name, [(column, dtype), ...])`` for every ``*.parquet``
    in *silver_dir*, unfiltered by metadata.yaml or approval status.

    This is the raw corpus used by the confidence-gated fallback — it works
    for any project, including ones with no metadata.yaml at all.
    """
    silver_dir = Path(silver_dir)
    return _describe_parquets(sorted(silver_dir.glob("*.parquet")))


def _raw_table_text(name: str, columns: list[tuple[str, str]]) -> str:
    """Build the text embedded for one table in the raw-schema fallback
    corpus — table name + column names (dtypes carry little semantic
    meaning for matching a natural-language question, so they're omitted)."""
    return " ".join([name, *(col for col, _dtype in columns)])


def build_schema_context(silver_dir: str | Path, *, tables: list[str] | None = None) -> str:
    """Return a DDL-style schema string covering silver tables.

    Example output::

        TABLE orders (
          order_id             BIGINT,
          customer_id          BIGINT,
          amount               DOUBLE,
          status               VARCHAR
        )

        TABLE products (
          product_id           BIGINT,
          name                 VARCHAR,
          price                DOUBLE
        )

    Parameters
    ----------
    silver_dir:
        Path to the silver layer directory. Every ``*.parquet`` file becomes
        a DuckDB view and is described.
    tables:
        Optional table-name allowlist (e.g. from :func:`rank_relevant_tables`)
        — when given, only these tables' DDL is included (names not present
        in ``silver_dir`` are silently ignored). ``None`` (default) includes
        every table, unchanged from prior behavior.

    Returns
    -------
    str
        Multi-table DDL context string, or empty string if no parquets found.
    """
    silver_dir = Path(silver_dir)
    parquets = sorted(silver_dir.glob("*.parquet"))
    if tables is not None:
        allowed = set(tables)
        parquets = [p for p in parquets if p.stem in allowed]

    blocks = [
        f"TABLE {table_name} (\n"
        + ",\n".join(f"  {col:<28} {dtype}" for col, dtype in columns)
        + "\n)"
        for table_name, columns in _describe_parquets(parquets)
    ]
    return "\n\n".join(blocks)


def _table_text(name: str, table: TableMeta) -> str:
    """Build the text embedded for one table — description + synonyms +
    every column's description, so relevance ranking sees business
    vocabulary, not just raw column names."""
    parts = [name, table.description or ""]
    if table.synonyms:
        parts.append(" ".join(table.synonyms))
    for col_name, col in table.columns.items():
        parts.append(f"{col_name} {col.description or ''}")
    return " ".join(p for p in parts if p)


def rank_relevant_tables(
    question: str,
    metadata: MetadataConfig,
    embed_fn: _retrieval.EmbedFn,
    top_k: int = 5,
) -> list[str]:
    """Return up to *top_k* approved silver-table names most relevant to *question*.

    Only ``status: approved`` **silver-layer** tables are eligible — matches
    the same locked scope as synthetic example generation (build order step
    7): cerebrum's runtime doesn't query gold Parquet, and unreviewed
    (draft/stale) table descriptions have no accuracy guarantee.

    Args:
        question: The user's natural-language question.
        metadata: A project's loaded metadata (``load_metadata()``).
        embed_fn: Embedding function — see ``cerebrum/retrieval.py:get_embed_fn()``.
        top_k: Maximum number of table names to return.

    Returns:
        list[str]: Table names, most relevant first. Empty if there are no
        approved silver-layer tables — callers should fall back to showing
        every table (``build_schema_context(silver_dir)`` with no ``tables=``
        filter) in that case.
    """
    names, _confidence = rank_relevant_tables_scored(question, metadata, embed_fn, top_k=top_k)
    return names


def rank_relevant_tables_scored(
    question: str,
    metadata: MetadataConfig,
    embed_fn: _retrieval.EmbedFn,
    top_k: int = 5,
) -> tuple[list[str], float]:
    """Same as :func:`rank_relevant_tables`, but also returns the top-1 cosine
    similarity as a confidence score for the confidence-gated fallback (see
    ``CerebrumPipeline._get_relevant_tables`` and
    ``retrieval.CONFIDENCE_THRESHOLD``).

    Returns:
        tuple[list[str], float]: ``(table_names, confidence)``. Confidence is
        ``0.0`` (always below any real threshold) when there are no approved
        silver-layer tables to rank against.
    """
    approved = [
        (name, table) for name, table in metadata.tables.items()
        if table.status == "approved" and table.layer == "silver"
    ]
    if not approved:
        return [], 0.0

    embedded = _retrieval.embed_payloads(approved, lambda p: _table_text(*p), embed_fn)
    ranked   = _retrieval.rank_payloads_scored(question, embedded, embed_fn, top_k=top_k)
    confidence = ranked[0][1] if ranked else 0.0
    return [name for (name, _table), _score in ranked], confidence
