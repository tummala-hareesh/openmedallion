"""tests/test_layers.py — tests for layer constants and path discovery helpers."""
import polars as pl
from pathlib import Path

from openmedallion.cli.main import LAYERS, DEFAULT_LAYER, _discover_bronze_paths, _discover_silver_paths
from openmedallion.pipeline.bronze import BronzeLoader


class TestLayerConstants:

    def test_all_layers_defined(self):
        assert set(LAYERS.keys()) == {"bronze", "silver", "gold", "export", "explore"}

    def test_default_layer_is_gold(self):
        assert DEFAULT_LAYER == "gold"

    def test_bronze_final_var(self):
        assert LAYERS["bronze"][0] == ["bronze"]

    def test_silver_final_var(self):
        assert LAYERS["silver"][0] == ["silver"]

    def test_gold_final_var(self):
        assert LAYERS["gold"][0] == ["gold"]

    def test_export_final_var(self):
        assert LAYERS["export"][0] == ["bi_export"]

    def test_explore_final_var(self):
        assert LAYERS["explore"][0] == ["explore"]

    def test_all_layers_have_label(self):
        for layer, (_, label) in LAYERS.items():
            assert isinstance(label, str) and len(label) > 0


class TestDiscoverBronzePaths:

    def test_finds_parquet_files(self, tmp_path):
        bronze = tmp_path / "bronze"
        bronze.mkdir()
        pl.DataFrame({"id": [1]}).write_parquet(bronze / "ORDERS.parquet")
        pl.DataFrame({"id": [2]}).write_parquet(bronze / "CUSTOMERS.parquet")
        cfg = {"paths": {"bronze": str(bronze)}}
        result = _discover_bronze_paths(cfg)
        assert "ORDERS"    in result
        assert "CUSTOMERS" in result

    def test_paths_are_path_objects(self, tmp_path):
        bronze = tmp_path / "bronze"
        bronze.mkdir()
        pl.DataFrame({"id": [1]}).write_parquet(bronze / "T.parquet")
        cfg = {"paths": {"bronze": str(bronze)}}
        result = _discover_bronze_paths(cfg)
        assert isinstance(result["T"], Path)

    def test_missing_bronze_dir_returns_empty(self, tmp_path, capsys):
        cfg = {"paths": {"bronze": str(tmp_path / "nonexistent")}}
        result = _discover_bronze_paths(cfg)
        assert result == {}
        assert "⚠️" in capsys.readouterr().out

    def test_non_parquet_files_excluded(self, tmp_path):
        bronze = tmp_path / "bronze"
        bronze.mkdir()
        (bronze / "README.txt").write_text("ignore me")
        pl.DataFrame({"id": [1]}).write_parquet(bronze / "ORDERS.parquet")
        cfg = {"paths": {"bronze": str(bronze)}}
        result = _discover_bronze_paths(cfg)
        assert list(result.keys()) == ["ORDERS"]


class TestDiscoverSilverPaths:

    def test_finds_parquet_files(self, tmp_path):
        silver = tmp_path / "silver"
        silver.mkdir()
        pl.DataFrame({"id": [1]}).write_parquet(silver / "orders.parquet")
        pl.DataFrame({"id": [2]}).write_parquet(silver / "customers.parquet")
        cfg = {"paths": {"silver": str(silver)}}
        result = _discover_silver_paths(cfg)
        assert "orders.parquet"    in result
        assert "customers.parquet" in result

    def test_keys_include_extension(self, tmp_path):
        silver = tmp_path / "silver"
        silver.mkdir()
        pl.DataFrame({"id": [1]}).write_parquet(silver / "orders.parquet")
        cfg = {"paths": {"silver": str(silver)}}
        result = _discover_silver_paths(cfg)
        assert "orders.parquet" in result
        assert "orders" not in result

    def test_missing_silver_dir_returns_empty(self, tmp_path, capsys):
        cfg = {"paths": {"silver": str(tmp_path / "nonexistent")}}
        result = _discover_silver_paths(cfg)
        assert result == {}
        assert "⚠️" in capsys.readouterr().out


class TestBronzeLoaderSelect:

    def _cfg(self, tmp_path, tables):
        return {
            "pipeline": {"name": "test"},
            "paths": {"bronze": str(tmp_path / "bronze"), "silver": "", "gold": "", "export": ""},
            "source": {"type": "local_files", "tables": tables},
        }

    def test_select_keeps_only_named_columns(self, tmp_path):
        src = tmp_path / "data.csv"
        src.write_text("id,name,email,amount\n1,Alice,a@b.com,100\n2,Bob,b@b.com,200\n")
        cfg = self._cfg(tmp_path, [{"name": "t", "path": str(src), "select": ["id", "amount"]}])
        result = BronzeLoader(cfg).load()
        df = pl.read_parquet(result["t"])
        assert df.columns == ["id", "amount"]
        assert len(df) == 2

    def test_select_absent_keeps_all_columns(self, tmp_path):
        src = tmp_path / "data.csv"
        src.write_text("id,name,email\n1,Alice,a@b.com\n")
        cfg = self._cfg(tmp_path, [{"name": "t", "path": str(src)}])
        result = BronzeLoader(cfg).load()
        df = pl.read_parquet(result["t"])
        assert set(df.columns) == {"id", "name", "email"}

    def test_select_invalid_column_raises(self, tmp_path):
        src = tmp_path / "data.csv"
        src.write_text("id,name\n1,Alice\n")
        cfg = self._cfg(tmp_path, [{"name": "t", "path": str(src), "select": ["id", "nonexistent"]}])
        import pytest
        with pytest.raises(Exception):
            BronzeLoader(cfg).load()
