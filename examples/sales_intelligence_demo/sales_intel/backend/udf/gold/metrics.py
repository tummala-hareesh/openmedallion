"""Gold pre-aggregation UDF: pass the quarter column through for temporal grouping."""
from __future__ import annotations

from pathlib import Path

import polars as pl


def add_quarter(df: pl.DataFrame, silver_dir: str | Path) -> pl.DataFrame:
    """Ensure the quarter column is present for group_by aggregation.

    The quarter is already derived in the silver enrich UDF.  This pre-agg UDF
    is a no-op pass-through that satisfies the gold contract and acts as a hook
    for any future pre-aggregation logic (e.g. filtering out test deals).

    Args:
        df: rep_performance DataFrame from silver.
        silver_dir: Silver directory path (unused; required by contract).

    Returns:
        DataFrame unchanged.
    """
    return df
