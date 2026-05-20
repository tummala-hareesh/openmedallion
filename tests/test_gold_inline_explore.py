"""tests/test_gold_inline_explore.py — inline explore: specs on gold aggregations."""
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from openmedallion.pipeline.gold import GoldAggregator


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _cfg(tmp_path: Path, aggregations: list, project_name: str = "proj") -> dict:
    silver = tmp_path / "silver"
    gold   = tmp_path / "gold"
    silver.mkdir(exist_ok=True)
    gold.mkdir(exist_ok=True)
    return {
        "paths": {
            "silver": str(silver),
            "gold":   str(gold),
        },
        "silver_to_gold": {
            "projects": [{"name": project_name, "aggregations": aggregations}]
        },
    }


def _write_silver(tmp_path: Path, name: str) -> Path:
    p = tmp_path / "silver" / name
    p.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"dept": ["A", "B"], "salary": [100.0, 200.0]}).write_parquet(p)
    return p


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestGoldInlineExplore:

    def test_profile_dispatched(self, tmp_path):
        _write_silver(tmp_path, "employees.parquet")
        cfg = _cfg(tmp_path, aggregations=[{
            "source_file": "employees.parquet",
            "output_file": "headcount.parquet",
            "group_by": ["dept"],
            "metrics": [{"agg": "count", "alias": "n"}],
            "explore": [{"report_type": "profile"}],
        }])
        with patch("openmedallion.explore.profile.generate_profile") as mock_gp:
            GoldAggregator(cfg).aggregate()
        mock_gp.assert_called_once()

    def test_walker_dispatched(self, tmp_path):
        _write_silver(tmp_path, "employees.parquet")
        cfg = _cfg(tmp_path, aggregations=[{
            "source_file": "employees.parquet",
            "output_file": "headcount.parquet",
            "group_by": ["dept"],
            "metrics": [{"agg": "count", "alias": "n"}],
            "explore": [{"report_type": "walker"}],
        }])
        with patch("openmedallion.explore.walker.generate_walker") as mock_gw:
            GoldAggregator(cfg).aggregate()
        mock_gw.assert_called_once()

    def test_no_explore_key_no_dispatch(self, tmp_path):
        _write_silver(tmp_path, "employees.parquet")
        cfg = _cfg(tmp_path, aggregations=[{
            "source_file": "employees.parquet",
            "output_file": "headcount.parquet",
            "group_by": ["dept"],
            "metrics": [{"agg": "count", "alias": "n"}],
        }])
        with patch("openmedallion.explore.profile.generate_profile") as mock_gp, \
             patch("openmedallion.explore.walker.generate_walker") as mock_gw:
            GoldAggregator(cfg).aggregate()
        mock_gp.assert_not_called()
        mock_gw.assert_not_called()

    def test_output_in_gold_addons_project(self, tmp_path):
        _write_silver(tmp_path, "employees.parquet")
        cfg = _cfg(tmp_path, aggregations=[{
            "source_file": "employees.parquet",
            "output_file": "headcount.parquet",
            "group_by": ["dept"],
            "metrics": [{"agg": "count", "alias": "n"}],
            "explore": [{"report_type": "profile", "output_file": "headcount_profile.html"}],
        }], project_name="oracle_hr")
        with patch("openmedallion.explore.profile.generate_profile") as mock_gp:
            GoldAggregator(cfg).aggregate()
        args, _ = mock_gp.call_args
        assert args[1].parent == tmp_path / "gold" / "add-ons" / "oracle_hr"

    def test_default_output_filename(self, tmp_path):
        _write_silver(tmp_path, "employees.parquet")
        cfg = _cfg(tmp_path, aggregations=[{
            "source_file": "employees.parquet",
            "output_file": "headcount_by_dept.parquet",
            "group_by": ["dept"],
            "metrics": [{"agg": "count", "alias": "n"}],
            "explore": [{"report_type": "profile"}],
        }])
        with patch("openmedallion.explore.profile.generate_profile") as mock_gp:
            GoldAggregator(cfg).aggregate()
        args, _ = mock_gp.call_args
        assert args[1].name == "headcount_by_dept_profile.html"

    def test_default_title_from_stem(self, tmp_path):
        _write_silver(tmp_path, "employees.parquet")
        cfg = _cfg(tmp_path, aggregations=[{
            "source_file": "employees.parquet",
            "output_file": "salary_by_dept.parquet",
            "group_by": ["dept"],
            "metrics": [{"agg": "mean", "column": "salary", "alias": "avg_salary"}],
            "explore": [{"report_type": "profile"}],
        }])
        with patch("openmedallion.explore.profile.generate_profile") as mock_gp:
            GoldAggregator(cfg).aggregate()
        _, kwargs = mock_gp.call_args
        assert kwargs.get("title") == "Salary By Dept"

    def test_addons_dir_created(self, tmp_path):
        _write_silver(tmp_path, "employees.parquet")
        cfg = _cfg(tmp_path, aggregations=[{
            "source_file": "employees.parquet",
            "output_file": "headcount.parquet",
            "group_by": ["dept"],
            "metrics": [{"agg": "count", "alias": "n"}],
            "explore": [{"report_type": "profile"}],
        }], project_name="myproject")
        with patch("openmedallion.explore.profile.generate_profile"):
            GoldAggregator(cfg).aggregate()
        assert (tmp_path / "gold" / "add-ons" / "myproject").exists()

    def test_multiple_specs_same_aggregation(self, tmp_path):
        _write_silver(tmp_path, "employees.parquet")
        cfg = _cfg(tmp_path, aggregations=[{
            "source_file": "employees.parquet",
            "output_file": "headcount.parquet",
            "group_by": ["dept"],
            "metrics": [{"agg": "count", "alias": "n"}],
            "explore": [
                {"report_type": "profile", "output_file": "headcount_profile.html"},
                {"report_type": "walker",  "output_file": "headcount_walker.html"},
            ],
        }])
        with patch("openmedallion.explore.profile.generate_profile") as mock_gp, \
             patch("openmedallion.explore.walker.generate_walker") as mock_gw:
            GoldAggregator(cfg).aggregate()
        mock_gp.assert_called_once()
        mock_gw.assert_called_once()

    def test_unknown_report_type_skips_no_raise(self, tmp_path, capsys):
        _write_silver(tmp_path, "employees.parquet")
        cfg = _cfg(tmp_path, aggregations=[{
            "source_file": "employees.parquet",
            "output_file": "headcount.parquet",
            "group_by": ["dept"],
            "metrics": [{"agg": "count", "alias": "n"}],
            "explore": [{"report_type": "unknown_type"}],
        }])
        GoldAggregator(cfg).aggregate()
        assert "unknown" in capsys.readouterr().out.lower()


# ---------------------------------------------------------------------------
# --no-explore flag (_explore: False in cfg)
# ---------------------------------------------------------------------------

class TestNoExploreFlag:

    def test_dispatch_suppressed_when_explore_disabled(self, tmp_path):
        _write_silver(tmp_path, "employees.parquet")
        cfg = _cfg(tmp_path, aggregations=[{
            "source_file": "employees.parquet",
            "output_file": "headcount.parquet",
            "group_by": ["dept"],
            "metrics": [{"agg": "count", "alias": "n"}],
            "explore": [{"report_type": "profile", "output_file": "headcount.html"}],
        }])
        cfg["_explore"] = False
        with patch("openmedallion.explore.profile.generate_profile") as mock_gp:
            GoldAggregator(cfg).aggregate()
        mock_gp.assert_not_called()

    def test_parquet_still_written_when_explore_disabled(self, tmp_path):
        _write_silver(tmp_path, "employees.parquet")
        cfg = _cfg(tmp_path, aggregations=[{
            "source_file": "employees.parquet",
            "output_file": "headcount.parquet",
            "group_by": ["dept"],
            "metrics": [{"agg": "count", "alias": "n"}],
            "explore": [{"report_type": "profile"}],
        }])
        cfg["_explore"] = False
        GoldAggregator(cfg).aggregate()
        assert (tmp_path / "gold" / "proj" / "headcount.parquet").exists()
