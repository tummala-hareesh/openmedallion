"""Gold pre-aggregation UDF: compute salary as % of job-band maximum."""
import polars as pl
from pathlib import Path


def add_salary_metrics(df: pl.DataFrame, silver_dir: str | Path) -> pl.DataFrame:
    return df.with_columns(
        ((pl.col("salary") - pl.col("min_salary")) /
         (pl.col("max_salary") - pl.col("min_salary")) * 100)
        .clip(0, 100)
        .alias("salary_pct_of_max")
    )
