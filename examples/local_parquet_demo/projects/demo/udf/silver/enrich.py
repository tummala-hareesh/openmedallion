import polars as pl


def flag_large_orders(df: pl.DataFrame, threshold: float = 100.0) -> pl.DataFrame:
    """Add is_large_order flag: True when amount >= threshold."""
    return df.with_columns(
        (pl.col("amount") >= threshold).alias("is_large_order")
    )
