"""Silver derived-table UDF: join transactions + reps + targets into rep_performance."""
from __future__ import annotations

from pathlib import Path

import polars as pl

from openmedallion.storage import join, read_parquet


def build_rep_performance(silver_dir: str | Path) -> pl.DataFrame:
    """Join transactions with rep info and regional targets.

    Produces one row per transaction enriched with:
    - rep name, region, team
    - the quarterly target for that rep's region
    - attainment_pct  (amount / target_amount × 100, approximate per-deal share)

    Args:
        silver_dir: Path to the silver directory.

    Returns:
        Enriched DataFrame ready for gold aggregations.
    """
    txn     = read_parquet(join(silver_dir, "transactions.parquet"))
    reps    = read_parquet(join(silver_dir, "reps.parquet"))
    targets = read_parquet(join(silver_dir, "targets.parquet"))

    # Derive quarter from txn_date (YYYY-Qn) so we can join to targets
    txn = txn.with_columns(
        (
            pl.col("txn_date").str.slice(0, 4)
            + "-Q"
            + ((pl.col("txn_date").str.slice(5, 2).cast(pl.Int32) - 1) // 3 + 1)
            .cast(pl.Utf8)
        ).alias("quarter")
    )

    return (
        txn
        .join(reps,    on="rep_id",              how="left")
        .join(targets, on=["region", "quarter"],  how="left")
        .with_columns(
            (pl.col("amount") / pl.col("target_amount") * 100.0)
            .round(1)
            .alias("attainment_pct")
        )
    )
