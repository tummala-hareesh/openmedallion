"""tests/test_layers.py — tests for layer constants and path discovery helpers."""
import polars as pl
from pathlib import Path

from openmedallion.cli.main import LAYERS, DEFAULT_LAYER, _discover_bronze_paths, _discover_silver_paths


class TestLayerConstants:

    def test_all_four_layers_defined(self):
        assert set(LAYERS.keys()) == {"bronze", "silver", "gold", "export"}

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
