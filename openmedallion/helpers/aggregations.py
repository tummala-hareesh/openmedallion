"""helpers/aggregations.py — Reusable aggregation helpers for use inside UDF functions.

Import in any UDF file:
    from openmedallion.helpers.aggregations import attach_group_stats, pivot_to_columns
"""
import polars as pl


def attach_group_stats(
    df: pl.DataFrame,
    group_by: str | list[str],
    col: str,
    stats: list[str],
    prefix: str | None = None,
) -> pl.DataFrame:
    """Compute group-level aggregations and attach them back as new columns."""
    groups  = group_by if isinstance(group_by, list) else [group_by]
    pfx     = prefix or f"{col}_group_"

    stat_map = {
        "sum":   pl.col(col).sum(),
        "mean":  pl.col(col).mean(),
        "min":   pl.col(col).min(),
        "max":   pl.col(col).max(),
        "count": pl.col(col).count(),
        "std":   pl.col(col).std(),
    }

    window_exprs = [
        stat_map[s].over(groups).alias(f"{pfx}{col}_{s}")
        for s in stats
        if s in stat_map
    ]

    return df.with_columns(window_exprs)


def top_n_within(
    df: pl.DataFrame,
    partition_by: str | list[str],
    order_by: str | list[str],
    n: int = 1,
    descending: bool = True,
    alias: str = "is_top_n",
) -> pl.DataFrame:
    """Add a boolean column flagging the top N rows within each partition."""
    partition = partition_by if isinstance(partition_by, list) else [partition_by]
    order     = order_by     if isinstance(order_by,     list) else [order_by]

    ranked = df.with_columns(
        pl.col(order[0])
          .rank(method="ordinal", descending=descending)
          .over(partition)
          .alias("__rank__")
    )

    return (
        ranked
        .with_columns((pl.col("__rank__") <= n).alias(alias))
        .drop("__rank__")
    )


def pivot_to_columns(
    df: pl.DataFrame,
    index: str | list[str],
    pivot_col: str,
    value_col: str,
    agg: str = "sum",
    prefix: str = "",
) -> pl.DataFrame:
    """Pivot distinct values of pivot_col into separate columns."""
    index_cols = index if isinstance(index, list) else [index]

    pivoted = df.pivot(
        values=value_col,
        index=index_cols,
        on=pivot_col,
        aggregate_function=agg,
    )

    if prefix:
        rename = {
            c: f"{prefix}{c}"
            for c in pivoted.columns
            if c not in index_cols
        }
        pivoted = pivoted.rename(rename)

    return pivoted


def unpivot_columns(
    df: pl.DataFrame,
    index: str | list[str],
    cols: list[str],
    variable_name: str = "variable",
    value_name: str = "value",
) -> pl.DataFrame:
    """Melt wide columns into long format (inverse of pivot_to_columns)."""
    index_cols = index if isinstance(index, list) else [index]
    return df.unpivot(
        on=cols,
        index=index_cols,
        variable_name=variable_name,
        value_name=value_name,
    )


def flag_outliers(
    df: pl.DataFrame,
    col: str,
    partition_by: str | list[str] | None = None,
    method: str = "iqr",
    multiplier: float = 1.5,
    alias: str | None = None,
) -> pl.DataFrame:
    """Add a boolean column flagging statistical outliers."""
    out_name  = alias or f"is_outlier_{col}"
    partition = (partition_by if isinstance(partition_by, list) else [partition_by]) \
                if partition_by else None

    if method == "iqr":
        q1   = pl.col(col).quantile(0.25)
        q3   = pl.col(col).quantile(0.75)
        iqr  = q3 - q1
        lo   = q1 - multiplier * iqr
        hi   = q3 + multiplier * iqr
        if partition:
            lo  = lo.over(partition)
            hi  = hi.over(partition)
        flag = (pl.col(col) < lo) | (pl.col(col) > hi)

    elif method == "zscore":
        mean = pl.col(col).mean()
        std  = pl.col(col).std()
        if partition:
            mean = mean.over(partition)
            std  = std.over(partition)
        flag = ((pl.col(col) - mean) / std).abs() > multiplier

    else:
        raise ValueError(f"Unknown outlier method '{method}'. Use 'iqr' or 'zscore'.")

    return df.with_columns(flag.alias(out_name))
