"""helpers/joins.py — Reusable join helpers for use inside UDF functions.

Usage in a UDF:
    from openmedallion.helpers.joins import join_tables, lookup_join, safe_join
"""
import polars as pl


def join_tables(
    left: pl.DataFrame,
    right: pl.DataFrame,
    on: str | list[str] | None = None,
    left_on: str | list[str] | None = None,
    right_on: str | list[str] | None = None,
    how: str = "inner",
    suffix: str = "_right",
) -> pl.DataFrame:
    """General-purpose join between two DataFrames."""
    if on is not None:
        left_on = right_on = on if isinstance(on, list) else [on]
    else:
        left_on  = left_on  if isinstance(left_on,  list) else [left_on]
        right_on = right_on if isinstance(right_on, list) else [right_on]

    return left.join(right, left_on=left_on, right_on=right_on,
                     how=how, suffix=suffix)


def lookup_join(
    df: pl.DataFrame,
    lookup: pl.DataFrame,
    key: str | list[str],
    cols: list[str],
    how: str = "left",
) -> pl.DataFrame:
    """Attach a specific subset of columns from a lookup table."""
    keys = key if isinstance(key, list) else [key]
    right = lookup.select(list(dict.fromkeys(keys + cols)))
    return df.join(right, on=keys, how=how)


def safe_join(
    left: pl.DataFrame,
    right: pl.DataFrame,
    on: str | list[str],
    how: str = "left",
    fill: dict | None = None,
) -> pl.DataFrame:
    """Join that fills nulls introduced by the join with explicit defaults."""
    keys   = on if isinstance(on, list) else [on]
    result = left.join(right, on=keys, how=how)

    if fill:
        result = result.with_columns([
            pl.col(col).fill_null(val) for col, val in fill.items()
            if col in result.columns
        ])

    return result


def multi_join(
    base: pl.DataFrame,
    joins: list[dict],
) -> pl.DataFrame:
    """Apply a sequence of joins to a base DataFrame."""
    df = base
    for spec in joins:
        right = spec.pop("right")
        df    = join_tables(df, right, **spec)
        spec["right"] = right
    return df


def asof_join(
    left: pl.DataFrame,
    right: pl.DataFrame,
    on: str,
    by: str | list[str] | None = None,
    strategy: str = "backward",
    suffix: str = "_right",
) -> pl.DataFrame:
    """Time-based / nearest-value join."""
    return left.join_asof(right, on=on, by=by, strategy=strategy, suffix=suffix)


def cross_join_filtered(
    left: pl.DataFrame,
    right: pl.DataFrame,
    predicate: pl.Expr,
    suffix: str = "_right",
) -> pl.DataFrame:
    """Cross join followed immediately by a filter predicate."""
    return left.join(right, how="cross", suffix=suffix).filter(predicate)
