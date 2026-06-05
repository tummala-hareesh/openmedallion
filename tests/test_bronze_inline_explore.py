"""tests/test_bronze_inline_explore.py — inline explore: specs on bronze tables.

Tests cover the local_files source path (_local_files_load).
The dlt/sql path (_collect_parquets) uses the same _dispatch_reports call
and is exercised via the per_table_explore map; tested via direct unit call.
"""
from pathlib import Path
from unittest.mock import patch

from openmedallion.pipeline.bronze import BronzeLoader


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _cfg(tmp_path: Path, tables: list) -> dict:
    bronze = tmp_path / "bronze"
    bronze.mkdir(exist_ok=True)
    return {
        "pipeline": {"name": "test"},
        "paths": {
            "bronze": str(bronze),
            "silver": str(tmp_path / "silver"),
            "gold":   str(tmp_path / "gold"),
            "export": str(tmp_path / "export"),
        },
        "source": {"type": "local_files", "tables": tables},
    }


def _write_csv(tmp_path: Path, name: str) -> Path:
    p = tmp_path / name
    p.write_text("id,val\n1,10\n2,20\n")
    return p


# ---------------------------------------------------------------------------
# local_files path
# ---------------------------------------------------------------------------

class TestBronzeLocalFilesExplore:

    def test_profile_dispatched(self, tmp_path):
        src = _write_csv(tmp_path, "orders.csv")
        cfg = _cfg(tmp_path, tables=[{
            "name": "orders", "path": str(src),
            "explore": [{"report_type": "profile"}],
        }])
        with patch("openmedallion.explore.profile.generate_profile") as mock_gp:
            BronzeLoader(cfg).load()
        mock_gp.assert_called_once()

    def test_walker_dispatched(self, tmp_path):
        src = _write_csv(tmp_path, "orders.csv")
        cfg = _cfg(tmp_path, tables=[{
            "name": "orders", "path": str(src),
            "explore": [{"report_type": "walker"}],
        }])
        with patch("openmedallion.explore.walker.generate_walker") as mock_gw:
            BronzeLoader(cfg).load()
        mock_gw.assert_called_once()

    def test_no_explore_key_no_dispatch(self, tmp_path):
        src = _write_csv(tmp_path, "orders.csv")
        cfg = _cfg(tmp_path, tables=[{"name": "orders", "path": str(src)}])
        with patch("openmedallion.explore.profile.generate_profile") as mock_gp, \
             patch("openmedallion.explore.walker.generate_walker") as mock_gw:
            BronzeLoader(cfg).load()
        mock_gp.assert_not_called()
        mock_gw.assert_not_called()

    def test_output_in_bronze_addons(self, tmp_path):
        src = _write_csv(tmp_path, "orders.csv")
        cfg = _cfg(tmp_path, tables=[{
            "name": "orders", "path": str(src),
            "explore": [{"report_type": "profile", "output_file": "orders_profile.html"}],
        }])
        with patch("openmedallion.explore.profile.generate_profile") as mock_gp:
            BronzeLoader(cfg).load()
        args, _ = mock_gp.call_args
        assert args[1].parent == tmp_path / "bronze" / "add-ons"

    def test_default_output_filename(self, tmp_path):
        src = _write_csv(tmp_path, "customers.csv")
        cfg = _cfg(tmp_path, tables=[{
            "name": "customers", "path": str(src),
            "explore": [{"report_type": "profile"}],
        }])
        with patch("openmedallion.explore.profile.generate_profile") as mock_gp:
            BronzeLoader(cfg).load()
        args, _ = mock_gp.call_args
        assert args[1].name == "customers_profile.html"

    def test_default_title_from_stem(self, tmp_path):
        src = _write_csv(tmp_path, "raw_orders.csv")
        cfg = _cfg(tmp_path, tables=[{
            "name": "raw_orders", "path": str(src),
            "explore": [{"report_type": "profile"}],
        }])
        with patch("openmedallion.explore.profile.generate_profile") as mock_gp:
            BronzeLoader(cfg).load()
        _, kwargs = mock_gp.call_args
        assert kwargs.get("title") == "Raw Orders"

    def test_explicit_title_forwarded(self, tmp_path):
        src = _write_csv(tmp_path, "orders.csv")
        cfg = _cfg(tmp_path, tables=[{
            "name": "orders", "path": str(src),
            "explore": [{"report_type": "profile", "title": "Raw Orders Audit"}],
        }])
        with patch("openmedallion.explore.profile.generate_profile") as mock_gp:
            BronzeLoader(cfg).load()
        _, kwargs = mock_gp.call_args
        assert kwargs.get("title") == "Raw Orders Audit"

    def test_minimal_forwarded(self, tmp_path):
        src = _write_csv(tmp_path, "orders.csv")
        cfg = _cfg(tmp_path, tables=[{
            "name": "orders", "path": str(src),
            "explore": [{"report_type": "profile", "minimal": True}],
        }])
        with patch("openmedallion.explore.profile.generate_profile") as mock_gp:
            BronzeLoader(cfg).load()
        _, kwargs = mock_gp.call_args
        assert kwargs.get("minimal") is True

    def test_multiple_specs_same_table(self, tmp_path):
        src = _write_csv(tmp_path, "orders.csv")
        cfg = _cfg(tmp_path, tables=[{
            "name": "orders", "path": str(src),
            "explore": [
                {"report_type": "profile", "output_file": "orders_profile.html"},
                {"report_type": "walker",  "output_file": "orders_walker.html"},
            ],
        }])
        with patch("openmedallion.explore.profile.generate_profile") as mock_gp, \
             patch("openmedallion.explore.walker.generate_walker") as mock_gw:
            BronzeLoader(cfg).load()
        mock_gp.assert_called_once()
        mock_gw.assert_called_once()

    def test_addons_dir_created(self, tmp_path):
        src = _write_csv(tmp_path, "orders.csv")
        cfg = _cfg(tmp_path, tables=[{
            "name": "orders", "path": str(src),
            "explore": [{"report_type": "profile"}],
        }])
        with patch("openmedallion.explore.profile.generate_profile"):
            BronzeLoader(cfg).load()
        assert (tmp_path / "bronze" / "add-ons").exists()

    def test_unknown_report_type_skips_no_raise(self, tmp_path, capsys):
        src = _write_csv(tmp_path, "orders.csv")
        cfg = _cfg(tmp_path, tables=[{
            "name": "orders", "path": str(src),
            "explore": [{"report_type": "unknown_type"}],
        }])
        BronzeLoader(cfg).load()
        assert "unknown" in capsys.readouterr().out.lower()


# ---------------------------------------------------------------------------
# --no-explore flag (_explore: False in cfg)
# ---------------------------------------------------------------------------

class TestNoExploreFlag:

    def test_dispatch_suppressed_when_explore_disabled(self, tmp_path):
        src = _write_csv(tmp_path, "orders.csv")
        cfg = _cfg(tmp_path, tables=[{
            "name": "orders", "path": str(src),
            "explore": [{"report_type": "profile", "output_file": "orders.html"}],
        }])
        cfg["_explore"] = False
        with patch("openmedallion.explore.profile.generate_profile") as mock_gp:
            BronzeLoader(cfg).load()
        mock_gp.assert_not_called()

    def test_parquet_still_written_when_explore_disabled(self, tmp_path):
        src = _write_csv(tmp_path, "orders.csv")
        cfg = _cfg(tmp_path, tables=[{
            "name": "orders", "path": str(src),
            "explore": [{"report_type": "profile"}],
        }])
        cfg["_explore"] = False
        with patch("openmedallion.explore.profile.generate_profile"):
            BronzeLoader(cfg).load()
        assert (tmp_path / "bronze" / "orders.parquet").exists()

    def test_addons_dir_not_created_when_explore_disabled(self, tmp_path):
        src = _write_csv(tmp_path, "orders.csv")
        cfg = _cfg(tmp_path, tables=[{
            "name": "orders", "path": str(src),
            "explore": [{"report_type": "profile"}],
        }])
        cfg["_explore"] = False
        BronzeLoader(cfg).load()
        assert not (tmp_path / "bronze" / "add-ons").exists()
