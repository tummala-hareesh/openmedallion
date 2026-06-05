"""tests/test_silver_inline_explore.py — inline explore: specs on silver tables.

All tests mock at the source module (openmedallion.explore.profile / .walker)
so optional deps (ydata-profiling, pygwalker) are not required in CI.
"""
from pathlib import Path
from unittest.mock import patch

import polars as pl

from openmedallion.pipeline.silver import SilverTransformer


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _cfg(tmp_path: Path, tables=None, derived=None) -> dict:
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    bronze.mkdir(exist_ok=True)
    silver.mkdir(exist_ok=True)
    return {
        "paths": {"bronze": str(bronze), "silver": str(silver)},
        "bronze_to_silver": {
            "tables":         tables  or [],
            "derived_tables": derived or [],
        },
    }


def _write_bronze(tmp_path: Path, name: str) -> Path:
    p = tmp_path / "bronze" / name
    p.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"id": [1, 2], "val": [10, 20]}).write_parquet(p)
    return p


# ---------------------------------------------------------------------------
# Base table — profile dispatch
# ---------------------------------------------------------------------------

class TestBaseTableExplore:

    def test_profile_dispatched(self, tmp_path):
        _write_bronze(tmp_path, "sales.parquet")
        cfg = _cfg(tmp_path, tables=[{
            "source_file": "sales.parquet",
            "output_file": "sales.parquet",
            "explore": [{"report_type": "profile"}],
        }])
        with patch("openmedallion.explore.profile.generate_profile") as mock_gp:
            SilverTransformer(cfg).transform()
        mock_gp.assert_called_once()

    def test_walker_dispatched(self, tmp_path):
        _write_bronze(tmp_path, "sales.parquet")
        cfg = _cfg(tmp_path, tables=[{
            "source_file": "sales.parquet",
            "output_file": "sales.parquet",
            "explore": [{"report_type": "walker"}],
        }])
        with patch("openmedallion.explore.walker.generate_walker") as mock_gw:
            SilverTransformer(cfg).transform()
        mock_gw.assert_called_once()

    def test_no_explore_key_no_dispatch(self, tmp_path):
        _write_bronze(tmp_path, "sales.parquet")
        cfg = _cfg(tmp_path, tables=[{
            "source_file": "sales.parquet",
            "output_file": "sales.parquet",
        }])
        with patch("openmedallion.explore.profile.generate_profile") as mock_gp, \
             patch("openmedallion.explore.walker.generate_walker") as mock_gw:
            SilverTransformer(cfg).transform()
        mock_gp.assert_not_called()
        mock_gw.assert_not_called()

    def test_output_lands_in_silver_subdir(self, tmp_path):
        _write_bronze(tmp_path, "sales.parquet")
        cfg = _cfg(tmp_path, tables=[{
            "source_file": "sales.parquet",
            "output_file": "sales.parquet",
            "explore": [{"report_type": "profile", "output_file": "sales_report.html"}],
        }])
        with patch("openmedallion.explore.profile.generate_profile") as mock_gp:
            SilverTransformer(cfg).transform()
        args, _ = mock_gp.call_args
        assert args[1].parent == tmp_path / "silver" / "add-ons"

    def test_default_output_filename(self, tmp_path):
        _write_bronze(tmp_path, "employees.parquet")
        cfg = _cfg(tmp_path, tables=[{
            "source_file": "employees.parquet",
            "output_file": "employees.parquet",
            "explore": [{"report_type": "profile"}],
        }])
        with patch("openmedallion.explore.profile.generate_profile") as mock_gp:
            SilverTransformer(cfg).transform()
        args, _ = mock_gp.call_args
        assert args[1].name == "employees_profile.html"

    def test_default_title_from_stem(self, tmp_path):
        _write_bronze(tmp_path, "salary_by_job.parquet")
        cfg = _cfg(tmp_path, tables=[{
            "source_file": "salary_by_job.parquet",
            "output_file": "salary_by_job.parquet",
            "explore": [{"report_type": "profile"}],
        }])
        with patch("openmedallion.explore.profile.generate_profile") as mock_gp:
            SilverTransformer(cfg).transform()
        _, kwargs = mock_gp.call_args
        assert kwargs.get("title") == "Salary By Job"

    def test_explicit_title_forwarded(self, tmp_path):
        _write_bronze(tmp_path, "sales.parquet")
        cfg = _cfg(tmp_path, tables=[{
            "source_file": "sales.parquet",
            "output_file": "sales.parquet",
            "explore": [{"report_type": "profile", "title": "My Custom Title"}],
        }])
        with patch("openmedallion.explore.profile.generate_profile") as mock_gp:
            SilverTransformer(cfg).transform()
        _, kwargs = mock_gp.call_args
        assert kwargs.get("title") == "My Custom Title"

    def test_minimal_flag_forwarded(self, tmp_path):
        _write_bronze(tmp_path, "sales.parquet")
        cfg = _cfg(tmp_path, tables=[{
            "source_file": "sales.parquet",
            "output_file": "sales.parquet",
            "explore": [{"report_type": "profile", "minimal": True}],
        }])
        with patch("openmedallion.explore.profile.generate_profile") as mock_gp:
            SilverTransformer(cfg).transform()
        _, kwargs = mock_gp.call_args
        assert kwargs.get("minimal") is True

    def test_minimal_defaults_false(self, tmp_path):
        _write_bronze(tmp_path, "sales.parquet")
        cfg = _cfg(tmp_path, tables=[{
            "source_file": "sales.parquet",
            "output_file": "sales.parquet",
            "explore": [{"report_type": "profile"}],
        }])
        with patch("openmedallion.explore.profile.generate_profile") as mock_gp:
            SilverTransformer(cfg).transform()
        _, kwargs = mock_gp.call_args
        assert kwargs.get("minimal") is False

    def test_multiple_specs_same_table(self, tmp_path):
        _write_bronze(tmp_path, "sales.parquet")
        cfg = _cfg(tmp_path, tables=[{
            "source_file": "sales.parquet",
            "output_file": "sales.parquet",
            "explore": [
                {"report_type": "profile", "output_file": "sales_profile.html"},
                {"report_type": "walker",  "output_file": "sales_walker.html"},
            ],
        }])
        with patch("openmedallion.explore.profile.generate_profile") as mock_gp, \
             patch("openmedallion.explore.walker.generate_walker") as mock_gw:
            SilverTransformer(cfg).transform()
        mock_gp.assert_called_once()
        mock_gw.assert_called_once()

    def test_explore_dir_created(self, tmp_path):
        _write_bronze(tmp_path, "sales.parquet")
        cfg = _cfg(tmp_path, tables=[{
            "source_file": "sales.parquet",
            "output_file": "sales.parquet",
            "explore": [{"report_type": "profile"}],
        }])
        with patch("openmedallion.explore.profile.generate_profile"):
            SilverTransformer(cfg).transform()
        assert (tmp_path / "silver" / "add-ons").exists()

    def test_unknown_report_type_skips_no_raise(self, tmp_path, capsys):
        _write_bronze(tmp_path, "sales.parquet")
        cfg = _cfg(tmp_path, tables=[{
            "source_file": "sales.parquet",
            "output_file": "sales.parquet",
            "explore": [{"report_type": "unknown_type"}],
        }])
        SilverTransformer(cfg).transform()
        assert "unknown" in capsys.readouterr().out.lower()

    def test_explore_output_next_to_silver_data(self, tmp_path):
        """Reports land in silver_path/add-ons/, co-located with silver data."""
        _write_bronze(tmp_path, "sales.parquet")
        cfg = _cfg(tmp_path, tables=[{
            "source_file": "sales.parquet",
            "output_file": "sales.parquet",
            "explore": [{"report_type": "profile"}],
        }])
        with patch("openmedallion.explore.profile.generate_profile") as mock_gp:
            SilverTransformer(cfg).transform()
        args, _ = mock_gp.call_args
        assert args[1].parent == tmp_path / "silver" / "add-ons"


# ---------------------------------------------------------------------------
# Derived table explore
# ---------------------------------------------------------------------------

def _write_derived_udf(tmp_path: Path, name: str = "derived_udf.py") -> Path:
    """Write a minimal derived-table UDF file and return its path."""
    p = tmp_path / name
    p.write_text(
        "import polars as pl\n"
        "def build(silver_path, **_):\n"
        "    return pl.DataFrame({'x': [1]})\n"
    )
    return p


class TestDerivedTableExplore:

    def test_derived_table_profile_dispatched(self, tmp_path):
        udf_file = _write_derived_udf(tmp_path)
        cfg = _cfg(tmp_path, derived=[{
            "output_file": "derived.parquet",
            "udf": {"file": str(udf_file), "function": "build"},
            "explore": [{"report_type": "profile"}],
        }])
        with patch("openmedallion.explore.profile.generate_profile") as mock_gp:
            SilverTransformer(cfg).transform()
        mock_gp.assert_called_once()

    def test_derived_table_output_in_silver_subdir(self, tmp_path):
        udf_file = _write_derived_udf(tmp_path)
        cfg = _cfg(tmp_path, derived=[{
            "output_file": "derived.parquet",
            "udf": {"file": str(udf_file), "function": "build"},
            "explore": [{"report_type": "profile", "output_file": "derived_report.html"}],
        }])
        with patch("openmedallion.explore.profile.generate_profile") as mock_gp:
            SilverTransformer(cfg).transform()
        args, _ = mock_gp.call_args
        assert args[1].parent == tmp_path / "silver" / "add-ons"


# ---------------------------------------------------------------------------
# --no-explore flag (_explore: False in cfg)
# ---------------------------------------------------------------------------

class TestNoExploreFlag:

    def test_dispatch_suppressed_for_base_table(self, tmp_path):
        _write_bronze(tmp_path, "employees.parquet")
        cfg = _cfg(tmp_path, tables=[{
            "source_file": "employees.parquet",
            "output_file": "employees.parquet",
            "explore": [{"report_type": "profile", "output_file": "emp.html"}],
        }])
        cfg["_explore"] = False
        with patch("openmedallion.explore.profile.generate_profile") as mock_gp:
            SilverTransformer(cfg).transform()
        mock_gp.assert_not_called()

    def test_dispatch_suppressed_for_derived_table(self, tmp_path):
        udf_file = _write_derived_udf(tmp_path)
        cfg = _cfg(tmp_path, derived=[{
            "output_file": "derived.parquet",
            "udf": {"file": str(udf_file), "function": "build"},
            "explore": [{"report_type": "profile", "output_file": "derived.html"}],
        }])
        cfg["_explore"] = False
        with patch("openmedallion.explore.profile.generate_profile") as mock_gp:
            SilverTransformer(cfg).transform()
        mock_gp.assert_not_called()

    def test_parquet_still_written_when_explore_disabled(self, tmp_path):
        _write_bronze(tmp_path, "employees.parquet")
        cfg = _cfg(tmp_path, tables=[{
            "source_file": "employees.parquet",
            "output_file": "employees.parquet",
            "explore": [{"report_type": "profile"}],
        }])
        cfg["_explore"] = False
        SilverTransformer(cfg).transform()
        assert (tmp_path / "silver" / "employees.parquet").exists()
