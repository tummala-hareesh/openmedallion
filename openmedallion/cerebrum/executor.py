"""cerebrum/executor.py — DuckDB SQL executor returning a Polars DataFrame.

Registers all silver Parquet files as named DuckDB views, executes the
validated SQL, and returns the result as a ``polars.DataFrame``.

View names are the file stems — e.g. ``orders.parquet`` → view ``orders``.
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import polars as pl


def execute(sql: str, silver_dir: str | Path) -> pl.DataFrame:
    """Execute *sql* against the silver layer and return a Polars DataFrame.

    Parameters
    ----------
    sql:
        A validated SELECT (or WITH … SELECT) query.
    silver_dir:
        Directory containing silver Parquet files.  Every ``*.parquet``
        file is registered as a DuckDB view before execution.

    Returns
    -------
    polars.DataFrame

    Raises
    ------
    duckdb.Error
        On any DuckDB execution error (syntax, missing column, etc.).
    """
    silver_dir = Path(silver_dir)
    con = duckdb.connect()
    try:
        for path in sorted(silver_dir.glob("*.parquet")):
            con.execute(
                f"CREATE OR REPLACE VIEW {path.stem} AS "
                f"SELECT * FROM read_parquet('{path}')"
            )
        return con.execute(sql).pl()
    finally:
        con.close()
