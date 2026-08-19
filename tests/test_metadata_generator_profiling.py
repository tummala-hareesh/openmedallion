"""tests/test_metadata_generator_profiling.py — `metadata generate/refresh --profile`
wiring: opt-in ydata-profiling enrichment of ColumnMeta (dtype/stats/accepted_values).

Same fixture/mocking pattern as tests/test_metadata_generator.py and
tests/test_metadata_refresh.py. `_profile_fn=` injection bypasses real
ydata-profiling (matches `_client=` for LLM calls) — no optional dependency
needed to run these tests.
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest
import yaml

from openmedallion.metadata.generator import generate_metadata, refresh_metadata


def _draft_json(description: str, columns: dict[str, dict]) -> str:
    return json.dumps({"description": description, "synonyms": ["thing"], "columns": columns})


@pytest.fixture()
def project(tmp_path: Path) -> tuple[Path, str]:
    proj_dir = tmp_path / "proj"
    silver_dir = proj_dir / "data" / "silver"
    gold_dir   = proj_dir / "data" / "gold" / "proj"
    silver_dir.mkdir(parents=True)
    gold_dir.mkdir(parents=True)

    pl.DataFrame({
        "order_id": [1, 2, 3],
        "amount":   [10.0, 20.0, 30.0],
        "region":   ["North", "South", "East"],
    }).write_parquet(silver_dir / "orders.parquet")

    pl.DataFrame({
        "region": ["North", "South"],
        "total":  [10.0, 20.0],
    }).write_parquet(gold_dir / "region_summary.parquet")

    main = {
        "pipeline": {"name": "proj"},
        "includes": {"bronze": "bronze.yaml", "silver": "silver.yaml", "gold": "gold.yaml"},
        "paths": {
            "bronze": str(proj_dir / "data" / "bronze"),
            "silver": str(silver_dir),
            "gold":   str(proj_dir / "data" / "gold"),
            "export": str(proj_dir / "data" / "export"),
        },
    }
    bronze = {"source": {"type": "filesystem"}}
    silver_cfg = {"bronze_to_silver": {"tables": []}}
    gold_cfg = {"silver_to_gold": {"projects": []}}
    for name, data in [("main", main), ("bronze", bronze), ("silver", silver_cfg), ("gold", gold_cfg)]:
        with open(proj_dir / f"{name}.yaml", "w") as f:
            yaml.dump(data, f)

    return tmp_path, "proj"


def _fake_profile_fn(path: Path) -> dict:
    """A stand-in for profile_columns() — keyed by column name, already-summarized shape."""
    if path.stem == "orders":
        return {
            "order_id": {"dtype": "Numeric", "stats": {"null_pct": 0.0, "distinct_count": 3, "min": 1, "max": 3, "mean": 2.0}, "accepted_values": None},
            "amount":   {"dtype": "Numeric", "stats": {"null_pct": 0.0, "distinct_count": 3, "min": 10.0, "max": 30.0, "mean": 20.0}, "accepted_values": None},
            "region":   {"dtype": "Categorical", "stats": {"null_pct": 0.0, "distinct_count": 3}, "accepted_values": ["East", "North", "South"]},
        }
    return {
        "region": {"dtype": "Categorical", "stats": {"null_pct": 0.0, "distinct_count": 2}, "accepted_values": ["North", "South"]},
        "total":  {"dtype": "Numeric", "stats": {"null_pct": 0.0, "distinct_count": 2, "min": 10.0, "max": 20.0, "mean": 15.0}, "accepted_values": None},
    }


class TestGenerateMetadataWithProfiling:

    def test_profile_false_by_default_columns_have_no_profiling_fields(self, project):
        projects_root, name = project
        mock_llm = MagicMock(side_effect=[
            _draft_json("d1", {"order_id": {"description": "d"}, "amount": {"description": "d"}, "region": {"description": "d"}}),
            _draft_json("d2", {"region": {"description": "d"}, "total": {"description": "d"}}),
        ])
        result = generate_metadata(name, projects_root, _client=mock_llm)
        assert result.tables["orders"].columns["amount"].dtype is None
        assert result.tables["orders"].columns["amount"].stats is None

    def test_profile_true_enriches_columns_with_dtype_stats_accepted_values(self, project):
        projects_root, name = project
        mock_llm = MagicMock(side_effect=[
            _draft_json("d1", {"order_id": {"description": "d"}, "amount": {"description": "d"}, "region": {"description": "d"}}),
            _draft_json("d2", {"region": {"description": "d"}, "total": {"description": "d"}}),
        ])
        result = generate_metadata(name, projects_root, _client=mock_llm, use_profiling=True, _profile_fn=_fake_profile_fn)

        amount = result.tables["orders"].columns["amount"]
        assert amount.dtype == "Numeric"
        assert amount.stats["mean"] == 20.0

        region = result.tables["orders"].columns["region"]
        assert region.accepted_values == ["East", "North", "South"]

    def test_profile_true_does_not_change_llm_call_count(self, project):
        projects_root, name = project
        mock_llm = MagicMock(side_effect=[
            _draft_json("d1", {"order_id": {"description": "d"}, "amount": {"description": "d"}, "region": {"description": "d"}}),
            _draft_json("d2", {"region": {"description": "d"}, "total": {"description": "d"}}),
        ])
        generate_metadata(name, projects_root, _client=mock_llm, use_profiling=True, _profile_fn=_fake_profile_fn)
        assert mock_llm.call_count == 2  # profiling is deterministic, adds zero LLM calls

    def test_approved_table_is_not_profiled(self, project):
        projects_root, name = project
        existing = {
            "tables": {
                "orders": {
                    "layer": "silver", "status": "approved", "description": "Human-reviewed.",
                    "columns": {"order_id": {}, "amount": {}, "region": {}},
                },
            },
        }
        with open(projects_root / name / "metadata.yaml", "w") as f:
            yaml.dump(existing, f)

        profile_fn = MagicMock(side_effect=_fake_profile_fn)
        mock_llm = MagicMock(side_effect=[
            _draft_json("d2", {"region": {"description": "d"}, "total": {"description": "d"}}),
        ])
        result = generate_metadata(name, projects_root, _client=mock_llm, use_profiling=True, _profile_fn=profile_fn)

        assert result.tables["orders"].columns["amount"].dtype is None  # untouched
        profile_fn.assert_called_once()  # only for region_summary, not the approved orders table


class TestRefreshMetadataWithProfiling:

    def test_redrafted_table_gets_profiling_when_enabled(self, project):
        projects_root, name = project
        with open(projects_root / name / "metadata.yaml", "w") as f:
            yaml.dump({
                "tables": {
                    "orders": {"layer": "silver", "status": "draft", "columns": {}},
                    "region_summary": {"layer": "gold", "status": "approved", "schema_hash": "keep", "columns": {}},
                },
            }, f)

        mock_llm = MagicMock(side_effect=[
            _draft_json("Fresh.", {"order_id": {"description": "d"}, "amount": {"description": "d"}, "region": {"description": "d"}}),
        ])
        result = refresh_metadata(name, projects_root, _client=mock_llm, use_profiling=True, _profile_fn=_fake_profile_fn)

        assert result.tables["orders"].columns["amount"].dtype == "Numeric"
