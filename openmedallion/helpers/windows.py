"""helpers/windows.py — Reusable window function helpers for use inside UDF functions.

Usage in a UDF:
    from openmedallion.helpers.windows import rank_within, running_total, lag_column
"""
import polars as pl


def rank_within(
    df: pl.DataFrame,
    partition_by: str | list[str],
    order_by: str | list[str],
    alias: str = "rank",
    descending: bool = False,
    method: str = "dense",
) -> pl.DataFrame:
    """Add a rank column within each partition, ordered by one or more columns."""
    partition = partition_by if isinstance(partition_by, list) else [partition_by]
    order     = order_by     if isinstance(order_by,     list) else [order_by]

    return df.with_columns(
        pl.col(order[0])
          .rank(method=method, descending=descending)
          .over(partition)
          .alias(alias)
    )


def running_total(
    df: pl.DataFrame,
    col: str,
    partition_by: str | list[str],
    order_by: str | list[str],
    alias: str | None = None,
) -> pl.DataFrame:
    """Add a cumulative sum column within each partition."""
    partition = partition_by if isinstance(partition_by, list) else [partition_by]
    out_name  = alias or f"running_{col}"

    return df.with_columns(
        pl.col(col).cum_sum().over(partition).alias(out_name)
    )


def lag_column(
    df: pl.DataFrame,
    col: str,
    partition_by: str | list[str],
    order_by: str | list[str],
    n: int = 1,
    alias: str | None = None,
    fill_value=None,
) -> pl.DataFrame:
    """Add a lagged (previous row) value of a column within each partition."""
    partition = partition_by if isinstance(partition_by, list) else [partition_by]
    out_name  = alias or f"prev_{col}"
    expr      = pl.col(col).shift(n).over(partition)

    if fill_value is not None:
        expr = expr.fill_null(pl.lit(fill_value))

    return df.with_columns(expr.alias(out_name))


def pct_of_total(
    df: pl.DataFrame,
    col: str,
    partition_by: str | list[str],
    alias: str | None = None,
) -> pl.DataFrame:
    """Add a column showing each row's value as a percentage of its partition total."""
    partition = partition_by if isinstance(partition_by, list) else [partition_by]
    out_name  = alias or f"{col}_pct"

    return df.with_columns(
        (pl.col(col) / pl.col(col).sum().over(partition) * 100.0)
        .round(2)
        .alias(out_name)
    )


def row_number(
    df: pl.DataFrame,
    partition_by: str | list[str],
    order_by: str | list[str],
    alias: str = "row_num",
    descending: bool = False,
) -> pl.DataFrame:
    """Add a sequential row number within each partition (1-based)."""
    return rank_within(
        df,
        partition_by=partition_by,
        order_by=order_by,
        alias=alias,
        descending=descending,
        method="ordinal",
    )


def lead_column(
    df: pl.DataFrame,
    col: str,
    partition_by: str | list[str],
    order_by: str | list[str],
    n: int = 1,
    alias: str | None = None,
    fill_value=None,
) -> pl.DataFrame:
    """Add a lead (next row) value of a column within each partition."""
    partition = partition_by if isinstance(partition_by, list) else [partition_by]
    out_name  = alias or f"next_{col}"
    expr      = pl.col(col).shift(-n).over(partition)
    if fill_value is not None:
        expr = expr.fill_null(pl.lit(fill_value))
    return df.with_columns(expr.alias(out_name))


def rolling_avg(
    df: pl.DataFrame,
    col: str,
    partition_by: str | list[str],
    order_by: str,
    window: int,
    alias: str | None = None,
    min_periods: int | None = None,
) -> pl.DataFrame:
    """Add a rolling (moving) average within each partition."""
    partition = partition_by if isinstance(partition_by, list) else [partition_by]
    out_name  = alias or f"rolling_avg_{col}"
    mp        = min_periods or window

    return df.with_columns(
        pl.col(col)
          .rolling_mean(window_size=window, min_periods=mp)
          .over(partition)
          .alias(out_name)
    )


def first_last_within(
    df: pl.DataFrame,
    col: str,
    partition_by: str | list[str],
    order_by: str | list[str],
    descending: bool = False,
) -> pl.DataFrame:
    """Add first_<col> and last_<col> within each partition."""
    partition = partition_by if isinstance(partition_by, list) else [partition_by]

    return df.with_columns([
        pl.col(col).first().over(partition).alias(f"first_{col}"),
        pl.col(col).last().over(partition).alias(f"last_{col}"),
    ])
