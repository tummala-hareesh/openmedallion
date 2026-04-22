"""helpers/dates.py — Reusable date/time helpers for use inside UDF functions.

Usage in a UDF:
    from openmedallion.helpers.dates import date_trunc, days_between, classify_recency
"""
import polars as pl
from datetime import date


def date_trunc(
    df: pl.DataFrame,
    col: str,
    unit: str,
    alias: str | None = None,
) -> pl.DataFrame:
    """Truncate a date/datetime column to a given time unit."""
    out_name = alias or f"{col}_{unit}"

    trunc_map = {
        "day":     "1d",
        "week":    "1w",
        "month":   "1mo",
        "quarter": "1q",
        "year":    "1y",
    }

    if unit not in trunc_map:
        raise ValueError(
            f"Unknown unit '{unit}'. Choose from: {list(trunc_map.keys())}"
        )

    return df.with_columns(
        pl.col(col).dt.truncate(trunc_map[unit]).alias(out_name)
    )


def days_between(
    df: pl.DataFrame,
    start: str,
    end: str,
    alias: str | None = None,
    abs_value: bool = False,
) -> pl.DataFrame:
    """Add a column with the number of days between two date columns."""
    out_name = alias or f"days_{start}_to_{end}"
    expr     = (pl.col(end) - pl.col(start)).dt.total_days()
    if abs_value:
        expr = expr.abs()
    return df.with_columns(expr.alias(out_name))


def classify_recency(
    df: pl.DataFrame,
    col: str,
    alias: str = "recency_band",
    as_of: date | None = None,
    bands: list[tuple[int, str]] | None = None,
) -> pl.DataFrame:
    """Add a recency band label based on how many days ago a date column was."""
    ref   = pl.lit(as_of or date.today())
    age   = (ref - pl.col(col)).dt.total_days()

    default_bands = [(7, "active"), (30, "recent"), (90, "lapsing"), (9999, "inactive")]
    thresholds    = bands or default_bands

    expr = pl.when(age <= thresholds[0][0]).then(pl.lit(thresholds[0][1]))
    for days, label in thresholds[1:]:
        expr = expr.when(age <= days).then(pl.lit(label))
    expr = expr.otherwise(pl.lit(thresholds[-1][1]))

    return df.with_columns(expr.alias(alias))


def add_calendar_columns(
    df: pl.DataFrame,
    col: str,
    parts: list[str] | None = None,
) -> pl.DataFrame:
    """Extract calendar parts from a date/datetime column as separate columns."""
    all_parts = ["year", "quarter", "month", "week", "day", "day_of_week", "is_weekend"]
    selected  = parts or all_parts

    expr_map = {
        "year":        pl.col(col).dt.year().alias(f"{col}_year"),
        "quarter":     pl.col(col).dt.quarter().alias(f"{col}_quarter"),
        "month":       pl.col(col).dt.month().alias(f"{col}_month"),
        "week":        pl.col(col).dt.week().alias(f"{col}_week"),
        "day":         pl.col(col).dt.day().alias(f"{col}_day"),
        "day_of_week": pl.col(col).dt.weekday().alias(f"{col}_day_of_week"),
        "is_weekend":  (pl.col(col).dt.weekday() >= 5).alias(f"{col}_is_weekend"),
    }

    invalid = set(selected) - set(expr_map)
    if invalid:
        raise ValueError(f"Unknown calendar parts: {invalid}. Choose from: {all_parts}")

    return df.with_columns([expr_map[p] for p in selected if p in expr_map])
