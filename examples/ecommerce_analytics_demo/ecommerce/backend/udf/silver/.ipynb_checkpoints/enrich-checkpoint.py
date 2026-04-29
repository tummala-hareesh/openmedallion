"""Silver derived-table UDF: join orders + products + customers into one enriched table."""
import polars as pl
from pathlib import Path

from openmedallion.storage import read_parquet, join


def build_order_lines_enriched(silver_dir: str | Path) -> pl.DataFrame:
    """Join the three silver tables and compute line revenue and margin.

    Args:
        silver_dir: Path to the silver directory (local or s3://).

    Returns:
        Enriched DataFrame with line_revenue, line_cost, and margin_amount columns.
    """
    orders    = read_parquet(join(silver_dir, "orders.parquet"))
    products  = read_parquet(join(silver_dir, "products.parquet"))
    customers = read_parquet(join(silver_dir, "customers.parquet"))

    return (
        orders
        .join(products,  on="product_id",  how="left")
        .join(customers, on="customer_id", how="left")
        .with_columns([
            (pl.col("qty").cast(pl.Float64) * pl.col("unit_price")).alias("line_revenue"),
            (pl.col("qty").cast(pl.Float64) * pl.col("unit_cost")).alias("line_cost"),
        ])
        .with_columns(
            (pl.col("line_revenue") - pl.col("line_cost")).alias("margin_amount")
        )
    )
