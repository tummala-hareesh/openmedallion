"""tests/test_udfs.py — tests for the UDF loading framework (load_udf, check_return)."""
import pytest
import polars as pl
from pathlib import Path

from openmedallion.contracts.udf import load_udf, check_return


def _write_udf(tmp_path, code: str, name: str = "udf.py") -> Path:
    p = tmp_path / name
    p.write_text(code)
    return p


class TestLoadUdf:

    def test_loads_function_from_file(self, tmp_path):
        f = _write_udf(tmp_path, "def my_fn(df): return df")
        fn, kwargs = load_udf({"file": str(f), "function": "my_fn"}, cache={}, layer="silver")
        assert callable(fn)
        assert kwargs == {}

    def test_returns_args_from_step(self, tmp_path):
        f = _write_udf(tmp_path, "def fn(df, x=0): return df")
        _, kwargs = load_udf(
            {"file": str(f), "function": "fn", "args": {"x": 42}},
            cache={}, layer="silver"
        )
        assert kwargs == {"x": 42}

    def test_caches_module_by_path(self, tmp_path):
        f = _write_udf(tmp_path, "def fn(df): return df")
        cache: dict = {}
        load_udf({"file": str(f), "function": "fn"}, cache=cache, layer="silver")
        load_udf({"file": str(f), "function": "fn"}, cache=cache, layer="silver")
        assert len(cache) == 1

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_udf({"file": "nonexistent.py", "function": "fn"}, cache={}, layer="silver")

    def test_missing_function_raises(self, tmp_path):
        f = _write_udf(tmp_path, "def real_fn(df): return df")
        with pytest.raises(AttributeError, match="ghost_fn"):
            load_udf({"file": str(f), "function": "ghost_fn"}, cache={}, layer="silver")

    def test_args_defaults_to_empty_dict_when_absent(self, tmp_path):
        f = _write_udf(tmp_path, "def fn(df): return df")
        _, kwargs = load_udf({"file": str(f), "function": "fn"}, cache={}, layer="gold")
        assert kwargs == {}

    def test_args_none_becomes_empty_dict(self, tmp_path):
        f = _write_udf(tmp_path, "def fn(df): return df")
        _, kwargs = load_udf({"file": str(f), "function": "fn", "args": None},
                             cache={}, layer="silver")
        assert kwargs == {}


class TestCheckReturn:

    def test_dataframe_passes(self):
        check_return(pl.DataFrame({"x": [1]}), "fn", "f.py")

    def test_non_dataframe_raises_type_error(self):
        with pytest.raises(TypeError, match="must return pl.DataFrame"):
            check_return("oops", "my_fn", "f.py", layer="silver")

    def test_error_includes_function_name(self):
        with pytest.raises(TypeError, match="my_function"):
            check_return(42, "my_function", "f.py")

    def test_layer_prefix_in_message(self):
        with pytest.raises(TypeError, match="\\[gold\\]"):
            check_return(None, "fn", "f.py", layer="gold")
