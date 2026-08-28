"""tests/test_bronze_multisource.py — multi-source bronze ingestion."""
import polars as pl
from pathlib import Path
from unittest.mock import patch

from openmedallion.pipeline.bronze import BronzeLoader


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    import csv
    import io
    buf = io.StringIO()
    if rows:
        w = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    path.write_text(buf.getvalue())


def _cfg(sources: list[dict]) -> dict:
    return {
        "pipeline": {"name": "multi_test"},
        "paths": {"bronze": "b", "silver": "s", "gold": "g", "export": "e"},
        "sources": sources,
    }


def _local_source(tables: list[dict]) -> dict:
    return {"type": "local_files", "tables": tables}


# ---------------------------------------------------------------------------
# Two local_files sources — tables from both appear in results
# ---------------------------------------------------------------------------

class TestTwoLocalFilesSources:

    def test_tables_from_both_sources_in_results(self, tmp_path):
        orders_csv = tmp_path / "orders.csv"
        products_csv = tmp_path / "products.csv"
        _write_csv(orders_csv, [{"order_id": 1, "amount": 100}])
        _write_csv(products_csv, [{"product_id": 1, "name": "Widget"}])

        bronze_dir = tmp_path / "bronze"
        cfg = {
            "pipeline": {"name": "multi_test"},
            "paths": {"bronze": str(bronze_dir), "silver": "s", "gold": "g", "export": "e"},
            "sources": [
                {"type": "local_files", "tables": [{"name": "orders", "path": str(orders_csv)}]},
                {"type": "local_files", "tables": [{"name": "products", "path": str(products_csv)}]},
            ],
        }
        loader = BronzeLoader(cfg)
        results = loader.load()

        assert "orders" in results
        assert "products" in results

    def test_parquet_files_written_for_both_sources(self, tmp_path):
        csv1 = tmp_path / "a.csv"
        csv2 = tmp_path / "b.csv"
        _write_csv(csv1, [{"x": 1}])
        _write_csv(csv2, [{"y": 2}])
        bronze_dir = tmp_path / "bronze"

        cfg = {
            "pipeline": {"name": "multi_test"},
            "paths": {"bronze": str(bronze_dir), "silver": "s", "gold": "g", "export": "e"},
            "sources": [
                {"type": "local_files", "tables": [{"name": "tableA", "path": str(csv1)}]},
                {"type": "local_files", "tables": [{"name": "tableB", "path": str(csv2)}]},
            ],
        }
        loader = BronzeLoader(cfg)
        loader.load()

        assert (bronze_dir / "tableA.parquet").exists()
        assert (bronze_dir / "tableB.parquet").exists()

    def test_data_content_correct_for_each_source(self, tmp_path):
        csv1 = tmp_path / "orders.csv"
        csv2 = tmp_path / "customers.csv"
        _write_csv(csv1, [{"order_id": 10, "amount": 50}])
        _write_csv(csv2, [{"customer_id": 99, "name": "Alice"}])
        bronze_dir = tmp_path / "bronze"

        cfg = {
            "pipeline": {"name": "multi_test"},
            "paths": {"bronze": str(bronze_dir), "silver": "s", "gold": "g", "export": "e"},
            "sources": [
                {"type": "local_files", "tables": [{"name": "orders", "path": str(csv1)}]},
                {"type": "local_files", "tables": [{"name": "customers", "path": str(csv2)}]},
            ],
        }
        BronzeLoader(cfg).load()

        orders = pl.read_parquet(str(bronze_dir / "orders.parquet"))
        customers = pl.read_parquet(str(bronze_dir / "customers.parquet"))
        assert orders["order_id"][0] == 10
        assert customers["name"][0] == "Alice"


# ---------------------------------------------------------------------------
# Backward compat — legacy source: + destination: still works
# ---------------------------------------------------------------------------

class TestLegacySourceNormalisation:

    def test_legacy_source_dict_normalised_to_sources_list(self, tmp_path):
        csv = tmp_path / "items.csv"
        _write_csv(csv, [{"id": 1}])
        bronze_dir = tmp_path / "bronze"

        cfg = {
            "pipeline": {"name": "legacy_test"},
            "paths": {"bronze": str(bronze_dir), "silver": "s", "gold": "g", "export": "e"},
            "source": {"type": "local_files", "tables": [{"name": "items", "path": str(csv)}]},
        }
        loader = BronzeLoader(cfg)
        assert len(loader.sources) == 1
        assert loader.sources[0]["type"] == "local_files"

    def test_legacy_source_produces_results(self, tmp_path):
        csv = tmp_path / "items.csv"
        _write_csv(csv, [{"id": 1}])
        bronze_dir = tmp_path / "bronze"

        cfg = {
            "pipeline": {"name": "legacy_test"},
            "paths": {"bronze": str(bronze_dir), "silver": "s", "gold": "g", "export": "e"},
            "source": {"type": "local_files", "tables": [{"name": "items", "path": str(csv)}]},
        }
        results = BronzeLoader(cfg).load()
        assert "items" in results

    def test_legacy_destination_carried_into_source(self, tmp_path):
        csv = tmp_path / "x.csv"
        _write_csv(csv, [{"v": 1}])
        bronze_dir = tmp_path / "bronze"

        cfg = {
            "pipeline": {"name": "t"},
            "paths": {"bronze": str(bronze_dir), "silver": "s", "gold": "g", "export": "e"},
            "source": {"type": "local_files", "tables": [{"name": "x", "path": str(csv)}]},
            "destination": {"type": "filesystem", "bucket_url": "data"},
        }
        loader = BronzeLoader(cfg)
        assert loader.sources[0].get("destination") == {"type": "filesystem", "bucket_url": "data"}

    def test_top_level_destination_shared_across_sources_list(self, tmp_path):
        cfg = {
            "pipeline": {"name": "t"},
            "paths": {"bronze": "b", "silver": "s", "gold": "g", "export": "e"},
            "sources": [
                {"type": "local_files", "tables": []},
                {"type": "local_files", "tables": []},
            ],
            "destination": {"type": "filesystem", "bucket_url": "shared"},
        }
        loader = BronzeLoader(cfg)
        assert loader.sources[0]["destination"] == {"type": "filesystem", "bucket_url": "shared"}
        assert loader.sources[1]["destination"] == {"type": "filesystem", "bucket_url": "shared"}

    def test_top_level_destination_does_not_override_per_source_destination(self, tmp_path):
        cfg = {
            "pipeline": {"name": "t"},
            "paths": {"bronze": "b", "silver": "s", "gold": "g", "export": "e"},
            "sources": [
                {
                    "type": "local_files",
                    "tables": [],
                    "destination": {"type": "duckdb", "db_path": "own.duckdb"},
                },
            ],
            "destination": {"type": "filesystem", "bucket_url": "shared"},
        }
        loader = BronzeLoader(cfg)
        assert loader.sources[0]["destination"] == {"type": "duckdb", "db_path": "own.duckdb"}


# ---------------------------------------------------------------------------
# src / dst pre-populated from first source at construction time
# ---------------------------------------------------------------------------

class TestSrcDstInitialisation:

    def test_src_populated_after_construction(self):
        cfg = {
            "pipeline": {"name": "t"},
            "paths": {"bronze": "b", "silver": "s", "gold": "g", "export": "e"},
            "sources": [{"type": "local_files", "tables": []}],
        }
        loader = BronzeLoader(cfg)
        assert loader.src["type"] == "local_files"

    def test_src_empty_when_no_sources(self):
        cfg = {
            "pipeline": {"name": "t"},
            "paths": {"bronze": "b", "silver": "s", "gold": "g", "export": "e"},
        }
        loader = BronzeLoader(cfg)
        assert loader.src == {}
        assert loader.dst == {}

    def test_dst_populated_from_first_source_destination(self):
        cfg = {
            "pipeline": {"name": "t"},
            "paths": {"bronze": "b", "silver": "s", "gold": "g", "export": "e"},
            "sources": [
                {
                    "type": "sql_database",
                    "connection_string": "sqlite:///:memory:",
                    "destination": {"type": "filesystem", "bucket_url": "data"},
                    "tables": [],
                }
            ],
        }
        loader = BronzeLoader(cfg)
        assert loader.dst == {"type": "filesystem", "bucket_url": "data"}


# ---------------------------------------------------------------------------
# Explore dispatched per-source
# ---------------------------------------------------------------------------

class TestMultiSourceExplore:

    def test_explore_dispatched_for_each_source(self, tmp_path):
        csv1 = tmp_path / "orders.csv"
        csv2 = tmp_path / "products.csv"
        _write_csv(csv1, [{"id": 1}])
        _write_csv(csv2, [{"id": 2}])
        bronze_dir = tmp_path / "bronze"

        cfg = {
            "pipeline": {"name": "t"},
            "paths": {"bronze": str(bronze_dir), "silver": "s", "gold": "g", "export": "e"},
            "sources": [
                {
                    "type": "local_files",
                    "tables": [{
                        "name": "orders",
                        "path": str(csv1),
                        "explore": [{"report_type": "profile", "output_file": "orders.html"}],
                    }],
                },
                {
                    "type": "local_files",
                    "tables": [{
                        "name": "products",
                        "path": str(csv2),
                        "explore": [{"report_type": "profile", "output_file": "products.html"}],
                    }],
                },
            ],
        }
        with patch("openmedallion.pipeline.explore._dispatch_reports") as mock_dr:
            BronzeLoader(cfg).load()
            assert mock_dr.call_count == 2
