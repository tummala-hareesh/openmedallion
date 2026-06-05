"""cerebrum/schema.py — Build LLM schema context from silver Parquet files.

Reads every *.parquet file in the silver directory, uses DuckDB DESCRIBE
to extract column names and types, and returns a DDL-style context string
for injection into the LLM prompt.
"""
from __future__ import annotations

from pathlib import Path

import duckdb


def build_schema_context(silver_dir: str | Path) -> str:
    """Return a DDL-style schema string covering all silver tables.

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

    Returns
    -------
    str
        Multi-table DDL context string, or empty string if no parquets found.
    """
    silver_dir = Path(silver_dir)
    parquets = sorted(silver_dir.glob("*.parquet"))
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
