"""tests/test_metadata_profiling.py — ydata-profiling enrichment for metadata.yaml.

Locked design decisions (confirmed via AskUserQuestion, not assumed):
- Opt-in only: `metadata generate --profile` / `metadata refresh --profile`.
  Without the flag, behavior is unchanged (DuckDB DESCRIBE + 8-sample DISTINCT).
- Three new optional ColumnMeta fields: `dtype`, `stats` (null_pct, distinct_count,
  min/max/mean where numeric), `accepted_values` (full sorted unique-value list,
  populated only for low-cardinality categorical/boolean columns — <= 20 uniques —
  to avoid dumping huge lists for high-cardinality columns).

`_summarize_column()` is a pure function over ydata-profiling's per-column
"variable description" dict shape (the same shape `ProfileReport.get_description()
.variables[col]` returns) — testable with hand-built fixture dicts, no
ydata-profiling install required. `profile_columns()` accepts a `_report_fn=`
injection point (same pattern as cerebrum/llm.py's `_client=` and
cerebrum/retrieval.py's `_embed_fn=`) so the real ydata-profiling/polars call is
never exercised in this test file either — matches the project's convention of
keeping new logic testable without the optional heavy dependency installed.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from openmedallion.metadata.profiling import (
    ACCEPTED_VALUES_MAX_DISTINCT,
    _summarize_column,
    profile_columns,
)


def _raw_var(**kwargs) -> dict:
    """Build a minimal fake ydata-profiling per-column variable description."""
    base = {
        "type": "Numeric",
        "n_missing": 0,
        "p_missing": 0.0,
        "n_distinct": 3,
        "value_counts_without_nan": pd.Series({}),
    }
    base.update(kwargs)
    return base


class TestSummarizeColumn:

    def test_numeric_column_gets_stats_min_max_mean(self):
        raw = _raw_var(type="Numeric", mean=20.0, min=10.0, max=30.0, n_distinct=3)
        out = _summarize_column(raw)
        assert out["dtype"] == "Numeric"
        assert out["stats"]["min"] == 10.0
        assert out["stats"]["max"] == 30.0
        assert out["stats"]["mean"] == 20.0

    def test_stats_always_include_null_pct_and_distinct_count(self):
        raw = _raw_var(n_missing=2, p_missing=0.25, n_distinct=5)
        out = _summarize_column(raw)
        assert out["stats"]["null_pct"] == 0.25
        assert out["stats"]["distinct_count"] == 5

    def test_non_numeric_column_has_no_min_max_mean_keys(self):
        raw = _raw_var(type="Categorical", n_distinct=2)
        out = _summarize_column(raw)
        assert "min" not in out["stats"]
        assert "max" not in out["stats"]
        assert "mean" not in out["stats"]

    def test_low_cardinality_categorical_gets_accepted_values(self):
        raw = _raw_var(
            type="Categorical",
            n_distinct=3,
            value_counts_without_nan=pd.Series({"active": 5, "pending": 2, "closed": 1}),
        )
        out = _summarize_column(raw)
        assert out["accepted_values"] == ["active", "closed", "pending"]  # sorted

    def test_high_cardinality_column_has_no_accepted_values(self):
        raw = _raw_var(
            type="Categorical",
            n_distinct=ACCEPTED_VALUES_MAX_DISTINCT + 1,
            value_counts_without_nan=pd.Series({f"v{i}": 1 for i in range(ACCEPTED_VALUES_MAX_DISTINCT + 1)}),
        )
        out = _summarize_column(raw)
        assert out["accepted_values"] is None

    def test_boundary_cardinality_exactly_at_threshold_gets_accepted_values(self):
        raw = _raw_var(
            type="Categorical",
            n_distinct=ACCEPTED_VALUES_MAX_DISTINCT,
            value_counts_without_nan=pd.Series({f"v{i}": 1 for i in range(ACCEPTED_VALUES_MAX_DISTINCT)}),
        )
        out = _summarize_column(raw)
        assert out["accepted_values"] is not None
        assert len(out["accepted_values"]) == ACCEPTED_VALUES_MAX_DISTINCT

    def test_numeric_column_never_gets_accepted_values_even_if_low_cardinality(self):
        """accepted_values is for categorical/boolean columns, not numeric ranges."""
        raw = _raw_var(
            type="Numeric", n_distinct=2, mean=1.5, min=1, max=2,
            value_counts_without_nan=pd.Series({1: 5, 2: 5}),
        )
        out = _summarize_column(raw)
        assert out["accepted_values"] is None

    def test_boolean_column_gets_accepted_values(self):
        raw = _raw_var(
            type="Boolean",
            n_distinct=2,
            value_counts_without_nan=pd.Series({True: 5, False: 3}),
        )
        out = _summarize_column(raw)
        assert set(out["accepted_values"]) == {True, False}


class TestProfileColumns:

    def test_injected_report_fn_bypasses_real_ydata_profiling(self, tmp_path: Path):
        fake_report = {
            "status": _raw_var(type="Categorical", n_distinct=2,
                                value_counts_without_nan=pd.Series({"open": 3, "closed": 2})),
            "amount": _raw_var(type="Numeric", mean=15.0, min=10.0, max=20.0, n_distinct=5),
        }
        result = profile_columns(tmp_path / "orders.parquet", _report_fn=lambda p: fake_report)

        assert result["status"]["dtype"] == "Categorical"
        assert result["status"]["accepted_values"] == ["closed", "open"]
        assert result["amount"]["stats"]["mean"] == 15.0

    def test_missing_dependency_raises_clear_module_not_found(self, tmp_path: Path, monkeypatch):
        import builtins
        real_import = builtins.__import__

        def _blocked_import(name, *args, **kwargs):
            if name == "ydata_profiling":
                raise ModuleNotFoundError(name="ydata_profiling")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", _blocked_import)
        with pytest.raises(ModuleNotFoundError):
            profile_columns(tmp_path / "orders.parquet")
