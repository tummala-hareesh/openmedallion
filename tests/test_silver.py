"""tests/test_silver.py — tests for SilverTransformer."""
import pytest
import polars as pl
from pathlib import Path

from openmedallion.pipeline.silver import SilverTransformer


def make_cfg(tmp_path, tables=None, derived=None):
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    bronze.mkdir(exist_ok=True)
    silver.mkdir(exist_ok=True)
    return {
        "paths": {"bronze": str(bronze), "silver": str(silver)},
        "bronze_to_silver": {
            "tables":        tables  or [],
            "derived_tables": derived or [],
        },
    }


def write_bronze(tmp_path, filename, df):
    path = tmp_path / "bronze" / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)
    return path


class TestStructuralTransforms:

    def test_rename(self, tmp_path):
        df = pl.DataFrame({"OLD_ID": [1, 2], "VALUE": [10, 20]})
        write_bronze(tmp_path, "t.parquet", df)
        cfg = make_cfg(tmp_path, tables=[{
            "source_file": "t.parquet", "output_file": "out.parquet",
            "transforms": [{"type": "rename", "columns": {"OLD_ID": "id", "VALUE": "val"}}],
        }])
        SilverTransformer(cfg).transform()
        result = pl.read_parquet(tmp_path / "silver" / "out.parquet")
        assert "id" in result.columns
        assert "val" in result.columns
        assert "OLD_ID" not in result.columns

    def test_cast(self, tmp_path):
        df = pl.DataFrame({"id": ["1", "2", "3"]})
        write_bronze(tmp_path, "t.parquet", df)
        cfg = make_cfg(tmp_path, tables=[{
            "source_file": "t.parquet", "output_file": "out.parquet",
            "transforms": [{"type": "cast", "columns": {"id": "Int64"}}],
        }])
        SilverTransformer(cfg).transform()
        result = pl.read_parquet(tmp_path / "silver" / "out.parquet")
        assert result["id"].dtype == pl.Int64

    def test_drop(self, tmp_path):
        df = pl.DataFrame({"id": [1, 2], "keep": ["a", "b"], "drop_me": [9, 9]})
        write_bronze(tmp_path, "t.parquet", df)
        cfg = make_cfg(tmp_path, tables=[{
            "source_file": "t.parquet", "output_file": "out.parquet",
            "transforms": [{"type": "drop", "columns": ["drop_me"]}],
        }])
        SilverTransformer(cfg).transform()
        result = pl.read_parquet(tmp_path / "silver" / "out.parquet")
        assert "drop_me" not in result.columns
        assert "keep" in result.columns

    def test_drop_empty_columns_list_is_noop(self, tmp_path):
        df = pl.DataFrame({"id": [1, 2]})
        write_bronze(tmp_path, "t.parquet", df)
        cfg = make_cfg(tmp_path, tables=[{
            "source_file": "t.parquet", "output_file": "out.parquet",
            "transforms": [{"type": "drop", "columns": []}],
        }])
        SilverTransformer(cfg).transform()
        result = pl.read_parquet(tmp_path / "silver" / "out.parquet")
        assert result.columns == ["id"]

    def test_unknown_transform_type_raises(self, tmp_path):
        df = pl.DataFrame({"id": [1]})
        write_bronze(tmp_path, "t.parquet", df)
        cfg = make_cfg(tmp_path, tables=[{
            "source_file": "t.parquet", "output_file": "out.parquet",
            "transforms": [{"type": "nonexistent"}],
        }])
        with pytest.raises(ValueError, match="Unknown transform type"):
            SilverTransformer(cfg).transform()

    def test_multiple_transforms_applied_in_order(self, tmp_path):
        df = pl.DataFrame({"OLD": ["1", "2"]})
        write_bronze(tmp_path, "t.parquet", df)
        cfg = make_cfg(tmp_path, tables=[{
            "source_file": "t.parquet", "output_file": "out.parquet",
            "transforms": [
                {"type": "rename", "columns": {"OLD": "id"}},
                {"type": "cast",   "columns": {"id": "Int64"}},
            ],
        }])
        SilverTransformer(cfg).transform()
        result = pl.read_parquet(tmp_path / "silver" / "out.parquet")
        assert result["id"].dtype == pl.Int64

    def test_missing_bronze_file_skips_gracefully(self, tmp_path, capsys):
        cfg = make_cfg(tmp_path, tables=[{
            "source_file": "missing.parquet", "output_file": "out.parquet",
            "transforms": [],
        }])
        SilverTransformer(cfg).transform()
        assert not (tmp_path / "silver" / "out.parquet").exists()
        assert "⚠️" in capsys.readouterr().out


class TestUDFTransforms:

    def _write_udf(self, tmp_path, code: str) -> Path:
        p = tmp_path / "my_udf.py"
        p.write_text(code)
        return p

    def test_udf_adds_column(self, tmp_path):
        df = pl.DataFrame({"id": [1, 2, 3]})
        write_bronze(tmp_path, "t.parquet", df)
        udf_file = self._write_udf(tmp_path, """
import polars as pl
def add_flag(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(pl.lit(True).alias("flagged"))
""")
        cfg = make_cfg(tmp_path, tables=[{
            "source_file": "t.parquet", "output_file": "out.parquet",
            "transforms": [{"type": "udf", "file": str(udf_file), "function": "add_flag"}],
        }])
        SilverTransformer(cfg).transform()
        result = pl.read_parquet(tmp_path / "silver" / "out.parquet")
        assert "flagged" in result.columns
        assert result["flagged"].all()

    def test_udf_receives_kwargs(self, tmp_path):
        df = pl.DataFrame({"val": [1, 2, 3]})
        write_bronze(tmp_path, "t.parquet", df)
        udf_file = self._write_udf(tmp_path, """
import polars as pl
def threshold_flag(df: pl.DataFrame, threshold: int = 0) -> pl.DataFrame:
    return df.with_columns((pl.col("val") > threshold).alias("above"))
""")
        cfg = make_cfg(tmp_path, tables=[{
            "source_file": "t.parquet", "output_file": "out.parquet",
            "transforms": [{"type": "udf", "file": str(udf_file),
                            "function": "threshold_flag", "args": {"threshold": 2}}],
        }])
        SilverTransformer(cfg).transform()
        result = pl.read_parquet(tmp_path / "silver" / "out.parquet")
        assert result["above"].to_list() == [False, False, True]

    def test_udf_filters_rows(self, tmp_path):
        df = pl.DataFrame({"id": [1, 2, 3, 4]})
        write_bronze(tmp_path, "t.parquet", df)
        udf_file = self._write_udf(tmp_path, """
import polars as pl
def keep_even(df: pl.DataFrame) -> pl.DataFrame:
    return df.filter(pl.col("id") % 2 == 0)
""")
        cfg = make_cfg(tmp_path, tables=[{
            "source_file": "t.parquet", "output_file": "out.parquet",
            "transforms": [{"type": "udf", "file": str(udf_file), "function": "keep_even"}],
        }])
        SilverTransformer(cfg).transform()
        result = pl.read_parquet(tmp_path / "silver" / "out.parquet")
        assert result["id"].to_list() == [2, 4]

    def test_udf_wrong_return_type_raises(self, tmp_path):
        df = pl.DataFrame({"id": [1]})
        write_bronze(tmp_path, "t.parquet", df)
        udf_file = self._write_udf(tmp_path, """
def bad_udf(df): return "not a dataframe"
""")
        cfg = make_cfg(tmp_path, tables=[{
            "source_file": "t.parquet", "output_file": "out.parquet",
            "transforms": [{"type": "udf", "file": str(udf_file), "function": "bad_udf"}],
        }])
        with pytest.raises(TypeError, match="must return pl.DataFrame"):
            SilverTransformer(cfg).transform()

    def test_udf_missing_function_raises(self, tmp_path):
        df = pl.DataFrame({"id": [1]})
        write_bronze(tmp_path, "t.parquet", df)
        udf_file = self._write_udf(tmp_path, "def existing(df): return df")
        cfg = make_cfg(tmp_path, tables=[{
            "source_file": "t.parquet", "output_file": "out.parquet",
            "transforms": [{"type": "udf", "file": str(udf_file), "function": "nonexistent"}],
        }])
        with pytest.raises(AttributeError, match="nonexistent"):
            SilverTransformer(cfg).transform()

    def test_udf_missing_file_raises(self, tmp_path):
        df = pl.DataFrame({"id": [1]})
        write_bronze(tmp_path, "t.parquet", df)
        cfg = make_cfg(tmp_path, tables=[{
            "source_file": "t.parquet", "output_file": "out.parquet",
            "transforms": [{"type": "udf", "file": "does_not_exist.py", "function": "f"}],
        }])
        with pytest.raises(FileNotFoundError):
            SilverTransformer(cfg).transform()

    def test_udf_module_cached_across_tables(self, tmp_path):
        df = pl.DataFrame({"id": [1]})
        write_bronze(tmp_path, "a.parquet", df)
        write_bronze(tmp_path, "b.parquet", df)
        udf_file = self._write_udf(tmp_path, """
import polars as pl
def identity(df): return df
""")
        cfg = make_cfg(tmp_path, tables=[
            {"source_file": "a.parquet", "output_file": "a_out.parquet",
             "transforms": [{"type": "udf", "file": str(udf_file), "function": "identity"}]},
            {"source_file": "b.parquet", "output_file": "b_out.parquet",
             "transforms": [{"type": "udf", "file": str(udf_file), "function": "identity"}]},
        ])
        t = SilverTransformer(cfg)
        t.transform()
        assert len(t._udf_cache) == 1


class TestDerivedTables:

    def test_derived_udf_receives_silver_dir(self, tmp_path):
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir()
        pl.DataFrame({"id": [1, 2]}).write_parquet(silver_dir / "base.parquet")

        udf_file = tmp_path / "derived_udf.py"
        udf_file.write_text("""
import polars as pl
from pathlib import Path
def build_derived(silver_dir) -> pl.DataFrame:
    df = pl.read_parquet(Path(silver_dir) / "base.parquet")
    return df.with_columns(pl.lit("derived").alias("source"))
""")
        cfg = {
            "paths": {"bronze": str(tmp_path / "bronze"), "silver": str(silver_dir)},
            "bronze_to_silver": {
                "tables": [],
                "derived_tables": [{
                    "output_file": "derived.parquet",
                    "udf": {"file": str(udf_file), "function": "build_derived"},
                }],
            },
        }
        (tmp_path / "bronze").mkdir(exist_ok=True)
        SilverTransformer(cfg).transform()
        result = pl.read_parquet(silver_dir / "derived.parquet")
        assert "source" in result.columns
        assert result["source"][0] == "derived"

    def test_derived_select_keeps_only_named_columns(self, tmp_path):
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir()
        pl.DataFrame({"id": [1], "a": [2], "b": [3]}).write_parquet(silver_dir / "t.parquet")

        udf_file = tmp_path / "u.py"
        udf_file.write_text("""
import polars as pl
from pathlib import Path
def fn(silver_dir) -> pl.DataFrame:
    return pl.read_parquet(Path(silver_dir) / "t.parquet")
""")
        cfg = {
            "paths": {"bronze": str(tmp_path / "bronze"), "silver": str(silver_dir)},
            "bronze_to_silver": {
                "tables": [],
                "derived_tables": [{
                    "output_file": "out.parquet",
                    "udf": {"file": str(udf_file), "function": "fn"},
                    "select": ["id", "a"],
                }],
            },
        }
        (tmp_path / "bronze").mkdir(exist_ok=True)
        SilverTransformer(cfg).transform()
        result = pl.read_parquet(silver_dir / "out.parquet")
        assert result.columns == ["id", "a"]
        assert "b" not in result.columns
