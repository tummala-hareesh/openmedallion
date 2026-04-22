"""Gold pre-aggregation UDF: add derived columns used across multiple aggregations."""
import polars as pl
from pathlib import Path


def add_metrics(df: pl.DataFrame, silver_dir: str | Path) -> pl.DataFrame:
    """Derive order_month (YYYY-MM) from order_date for temporal grouping.

    Args:
        df: Enriched order lines DataFrame.
        silver_dir: Silver directory path (unused here; required by contract).

    Returns:
        DataFrame with order_month column added.
    """
    return df.with_columns(
        pl.col("order_date").str.slice(0, 7).alias("order_month")
    )
