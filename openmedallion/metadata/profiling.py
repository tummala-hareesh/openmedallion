"""metadata/profiling.py — opt-in ydata-profiling enrichment for metadata.yaml.

Locked design decisions (confirmed via AskUserQuestion, not assumed):
- Opt-in only, via `metadata generate --profile` / `metadata refresh --profile`.
  A full ``ProfileReport`` (correlations, interactions) is somewhat expensive,
  so this is never run implicitly — the default path stays fast and
  dependency-free (plain DuckDB DESCRIBE + 8-sample DISTINCT, see
  ``metadata/generator.py``).
- Adds three optional ``ColumnMeta`` fields: ``dtype`` (ydata-profiling's
  inferred type — Numeric/Categorical/Boolean/DateTime/Text/...), ``stats``
  (``null_pct``, ``distinct_count``, plus ``min``/``max``/``mean`` for numeric
  columns), and ``accepted_values`` (the full sorted unique-value list, but
  only for low-cardinality categorical/boolean columns — <= 20 uniques — to
  avoid dumping a huge list for high-cardinality columns).

``_summarize_column()`` is pure and testable without ydata-profiling installed
— it operates on the same per-column "variable description" dict shape that
``ProfileReport.get_description().variables[col]`` returns. ``profile_columns()``
accepts a ``_report_fn=`` injection point (matches the ``_client=`` pattern in
``cerebrum/llm.py`` and ``_embed_fn=`` in ``cerebrum/retrieval.py``) so tests
never need the real ``ydata_profiling``/``polars`` call either.
"""
from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

ACCEPTED_VALUES_MAX_DISTINCT = 20

# ydata-profiling column "type" values that represent a bounded set of
# categories rather than a continuous/free-text/high-cardinality field.
_CATEGORICAL_TYPES = {"Categorical", "Boolean"}


def _summarize_column(raw: dict[str, Any]) -> dict[str, Any]:
    """Reduce one ydata-profiling per-column variable description to our shape.

    ``raw`` is expected to look like an entry of
    ``ProfileReport.get_description().variables`` — a dict with at least
    ``type``, ``n_missing``, ``p_missing``, ``n_distinct``, and (for
    categorical/boolean columns) ``value_counts_without_nan`` (a pandas
    Series indexed by value). Numeric columns additionally carry ``mean``,
    ``min``, ``max``.
    """
    dtype = raw.get("type")

    stats: dict[str, Any] = {
        "null_pct": raw.get("p_missing", 0.0),
        "distinct_count": raw.get("n_distinct", 0),
    }
    if dtype == "Numeric":
        for key in ("min", "max", "mean"):
            if key in raw:
                stats[key] = raw[key]

    accepted_values: list[Any] | None = None
    if dtype in _CATEGORICAL_TYPES:
        n_distinct = raw.get("n_distinct", 0)
        if 0 < n_distinct <= ACCEPTED_VALUES_MAX_DISTINCT:
            counts = raw.get("value_counts_without_nan")
            if counts is not None:
                try:
                    accepted_values = sorted(counts.index.tolist(), key=str)
                except TypeError:
                    accepted_values = list(counts.index)

    return {"dtype": dtype, "stats": stats, "accepted_values": accepted_values}


def _run_profile_report(path: Path) -> dict[str, dict[str, Any]]:
    """Run ydata-profiling (minimal=True — no correlations/interactions) and
    return the raw per-column variable descriptions.

    Raises ``ModuleNotFoundError`` (name="ydata_profiling") if the optional
    ``openmedallion[profile]`` extra isn't installed — callers should catch
    this and print install guidance rather than a raw traceback.
    """
    from ydata_profiling import ProfileReport
    import polars as pl

    df = pl.read_parquet(path).to_pandas()
    report = ProfileReport(df, minimal=True)
    description = report.get_description()
    return dict(description.variables)


def profile_columns(
    path: Path,
    *,
    _report_fn: Callable[[Path], dict[str, dict[str, Any]]] | None = None,
) -> dict[str, dict[str, Any]]:
    """Return ``{column_name: {dtype, stats, accepted_values}}`` for a Parquet file.

    Args:
        path: Silver/gold Parquet file to profile.
        _report_fn: Inject a fake raw-variables provider, bypassing the real
            ydata-profiling call. Intended for testing only.
    """
    report_fn = _report_fn or _run_profile_report
    raw_variables = report_fn(path)
    return {name: _summarize_column(raw) for name, raw in raw_variables.items()}
