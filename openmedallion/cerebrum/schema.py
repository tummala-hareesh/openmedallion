"""cerebrum/schema.py — Build LLM schema context from silver Parquet files,
with optional relevance-based pruning (RAG roadmap Phase 2, build order step 10).

Reads every *.parquet file in the silver directory, uses DuckDB DESCRIBE
to extract column names and types, and returns a DDL-style context string
for injection into the LLM prompt. When a project has curated table/column
descriptions in metadata.yaml, :func:`rank_relevant_tables` can narrow the
DDL shown to just the top-k tables relevant to the current question, keeping
the prompt tight on large schemas — same embedding-based ranking technique
as ``cerebrum/retrieval.py``'s dynamic few-shot retrieval (step 9), applied
to table/column descriptions instead of example questions.
"""
from __future__ import annotations

from pathlib import Path

import duckdb

from openmedallion.cerebrum import retrieval as _retrieval
from openmedallion.metadata.schema import MetadataConfig, TableMeta


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
    if not parquets:
        return ""

    con = duckdb.connect()
    blocks: list[str] = []
    try:
        for path in parquets:
            table_name = path.stem
            con.execute(
                f"CREATE OR REPLACE VIEW {table_name} AS "
                f"SELECT * FROM read_parquet('{path}')"
            )
            rows = con.execute(f"DESCRIBE {table_name}").fetchall()
            lines = [f"  {col:<28} {dtype}" for col, dtype, *_ in rows]
            blocks.append(f"TABLE {table_name} (\n" + ",\n".join(lines) + "\n)")
    finally:
        con.close()

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
    approved = [
        (name, table) for name, table in metadata.tables.items()
        if table.status == "approved" and table.layer == "silver"
    ]
    if not approved:
        return []

    embedded = _retrieval.embed_payloads(approved, lambda p: _table_text(*p), embed_fn)
    ranked   = _retrieval.rank_payloads(question, embedded, embed_fn, top_k=top_k)
    return [name for name, _ in ranked]
