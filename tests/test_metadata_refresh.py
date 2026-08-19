"""tests/test_metadata_refresh.py — RAG roadmap follow-on: `medallion metadata refresh`.

Closes the flagged gap: metadata.yaml's `status: stale` has existed in the schema
since build order step 1, but nothing ever set a table to `stale` or re-generated
only stale entries. This adds:

- `openmedallion.metadata.drift._schema_fingerprint()` — pure, order-independent
  hash of (column_name, dtype) pairs.
- `openmedallion.metadata.drift.detect_drift()` — compares each table's stored
  `schema_hash` against a fresh DuckDB DESCRIBE of the live silver/gold Parquet.
- `openmedallion.metadata.generator.refresh_metadata()` — locked behavior:
  approved + drifted → flipped to `stale` only, NO LLM call this run (a human
  must explicitly re-run refresh/generate to get new content — "approved is a
  trust boundary" stays strict). draft/stale (any) → re-drafted via the existing
  `_draft_table()`, same LLM-call pattern as `generate_metadata()`. approved +
  unchanged → untouched, no LLM call. Table dropped from Parquet → left in YAML,
  flagged via `on_step`, never deleted.

LLM calls are mocked (MagicMock side_effect), matching tests/test_metadata_generator.py.
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest
import yaml

from openmedallion.metadata.drift import _schema_fingerprint, detect_drift
from openmedallion.metadata.generator import refresh_metadata
from openmedallion.metadata.loader import load_metadata


def _draft_json(description: str, columns: dict[str, dict]) -> str:
    return json.dumps({"description": description, "synonyms": ["thing"], "columns": columns})


@pytest.fixture()
def project(tmp_path: Path) -> tuple[Path, str]:
    """A minimal real project: main.yaml + one silver table + one gold table."""
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


def _write_metadata(projects_root: Path, name: str, tables: dict) -> None:
    with open(projects_root / name / "metadata.yaml", "w") as f:
        yaml.dump({"tables": tables}, f)


def _orders_hash(project) -> str:
    """Compute the real fingerprint for the fixture's `orders` table schema."""
    projects_root, name = project
    path = projects_root / name / "data" / "silver" / "orders.parquet"
    con = __import__("duckdb").connect()
    col_info = [(r[0], r[1]) for r in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{path}')"
    ).fetchall()]
    con.close()
    return _schema_fingerprint(col_info)


class TestSchemaFingerprint:

    def test_same_columns_same_dtypes_same_hash(self):
        a = _schema_fingerprint([("id", "BIGINT"), ("name", "VARCHAR")])
        b = _schema_fingerprint([("id", "BIGINT"), ("name", "VARCHAR")])
        assert a == b

    def test_order_independent(self):
        a = _schema_fingerprint([("id", "BIGINT"), ("name", "VARCHAR")])
        b = _schema_fingerprint([("name", "VARCHAR"), ("id", "BIGINT")])
        assert a == b

    def test_added_column_changes_hash(self):
        a = _schema_fingerprint([("id", "BIGINT")])
        b = _schema_fingerprint([("id", "BIGINT"), ("name", "VARCHAR")])
        assert a != b

    def test_removed_column_changes_hash(self):
        a = _schema_fingerprint([("id", "BIGINT"), ("name", "VARCHAR")])
        b = _schema_fingerprint([("id", "BIGINT")])
        assert a != b

    def test_retyped_column_changes_hash(self):
        a = _schema_fingerprint([("id", "BIGINT")])
        b = _schema_fingerprint([("id", "VARCHAR")])
        assert a != b

    def test_empty_schema_is_stable(self):
        assert _schema_fingerprint([]) == _schema_fingerprint([])


class TestDetectDrift:

    def test_no_metadata_yaml_reports_no_drift(self, project):
        """First-run case: nothing to compare against yet — that's `generate`'s job, not drift."""
        projects_root, name = project
        drift, dropped = detect_drift(name, projects_root)
        assert drift == {}
        assert dropped == []

    def test_unchanged_schema_reports_no_drift(self, project):
        projects_root, name = project
        _write_metadata(projects_root, name, {
            "orders": {"layer": "silver", "status": "approved", "schema_hash": _orders_hash(project), "columns": {}},
            "region_summary": {"layer": "gold", "status": "approved", "columns": {}},
        })
        drift, dropped = detect_drift(name, projects_root)
        assert "orders" not in drift
        assert dropped == []

    def test_missing_schema_hash_is_not_treated_as_drift(self, project):
        """A table drafted before schema_hash existed shouldn't be flagged until refreshed once."""
        projects_root, name = project
        _write_metadata(projects_root, name, {
            "orders": {"layer": "silver", "status": "approved", "columns": {}},
        })
        drift, dropped = detect_drift(name, projects_root)
        assert "orders" not in drift

    def test_changed_schema_is_reported_as_drift(self, project):
        projects_root, name = project
        _write_metadata(projects_root, name, {
            "orders": {"layer": "silver", "status": "approved", "schema_hash": "deadbeef", "columns": {}},
        })
        drift, dropped = detect_drift(name, projects_root)
        assert "orders" in drift
        assert isinstance(drift["orders"], str)

    def test_dropped_table_reported_separately_not_as_drift(self, project):
        projects_root, name = project
        _write_metadata(projects_root, name, {
            "orders": {"layer": "silver", "status": "approved", "schema_hash": _orders_hash(project), "columns": {}},
            "ghost_table": {"layer": "silver", "status": "approved", "schema_hash": "whatever", "columns": {}},
        })
        drift, dropped = detect_drift(name, projects_root)
        assert "ghost_table" not in drift
        assert "ghost_table" in dropped


class TestRefreshMetadata:

    def test_approved_unchanged_table_untouched_no_llm_call(self, project):
        projects_root, name = project
        _write_metadata(projects_root, name, {
            "orders": {
                "layer": "silver", "status": "approved", "description": "Human-reviewed.",
                "schema_hash": _orders_hash(project), "columns": {},
            },
        })
        mock_llm = MagicMock()
        result = refresh_metadata(name, projects_root, _client=mock_llm)

        assert mock_llm.call_count == 0  # refresh only touches tables already in metadata.yaml
        assert result.tables["orders"].status == "approved"
        assert result.tables["orders"].description == "Human-reviewed."

    def test_approved_drifted_table_flips_to_stale_without_llm_call(self, project):
        projects_root, name = project
        _write_metadata(projects_root, name, {
            "orders": {
                "layer": "silver", "status": "approved", "description": "Human-reviewed.",
                "schema_hash": "deadbeef", "columns": {},
            },
            "region_summary": {
                "layer": "gold", "status": "approved", "description": "Approved gold.",
                "schema_hash": _schema_fingerprint([("region", "VARCHAR"), ("total", "DOUBLE")]), "columns": {},
            },
        })
        mock_llm = MagicMock()
        result = refresh_metadata(name, projects_root, _client=mock_llm)

        assert mock_llm.call_count == 0
        assert result.tables["orders"].status == "stale"
        assert result.tables["orders"].description == "Human-reviewed."  # content NOT overwritten
        assert result.tables["orders"].schema_hash == _orders_hash(project)  # hash updated so it isn't re-flagged forever
        assert result.tables["region_summary"].status == "approved"  # unaffected

    def test_draft_table_is_redrafted_and_hash_updated(self, project):
        projects_root, name = project
        _write_metadata(projects_root, name, {
            "orders": {"layer": "silver", "status": "draft", "columns": {}},
            "region_summary": {
                "layer": "gold", "status": "approved",
                "schema_hash": _schema_fingerprint([("region", "VARCHAR"), ("total", "DOUBLE")]), "columns": {},
            },
        })
        mock_llm = MagicMock(side_effect=[
            _draft_json("Fresh.", {
                "order_id": {"description": "d"}, "amount": {"description": "d"}, "region": {"description": "d"},
            }),
        ])
        result = refresh_metadata(name, projects_root, _client=mock_llm)

        assert mock_llm.call_count == 1
        assert result.tables["orders"].status == "draft"
        assert result.tables["orders"].description == "Fresh."
        assert result.tables["orders"].schema_hash == _orders_hash(project)

    def test_stale_table_is_redrafted_regardless_of_drift(self, project):
        projects_root, name = project
        _write_metadata(projects_root, name, {
            "orders": {"layer": "silver", "status": "stale", "schema_hash": _orders_hash(project), "columns": {}},
            "region_summary": {
                "layer": "gold", "status": "approved",
                "schema_hash": _schema_fingerprint([("region", "VARCHAR"), ("total", "DOUBLE")]), "columns": {},
            },
        })
        mock_llm = MagicMock(side_effect=[
            _draft_json("Refreshed.", {
                "order_id": {"description": "d"}, "amount": {"description": "d"}, "region": {"description": "d"},
            }),
        ])
        result = refresh_metadata(name, projects_root, _client=mock_llm)

        assert mock_llm.call_count == 1
        assert result.tables["orders"].description == "Refreshed."
        assert result.tables["orders"].status == "draft"  # redraft resets to draft, same as generate_metadata

    def test_dropped_table_left_in_yaml_and_flagged_via_on_step(self, project):
        projects_root, name = project
        _write_metadata(projects_root, name, {
            "orders": {"layer": "silver", "status": "approved", "schema_hash": _orders_hash(project), "columns": {}},
            "region_summary": {
                "layer": "gold", "status": "approved",
                "schema_hash": _schema_fingerprint([("region", "VARCHAR"), ("total", "DOUBLE")]), "columns": {},
            },
            "ghost_table": {"layer": "silver", "status": "approved", "schema_hash": "whatever", "columns": {}},
        })
        mock_llm = MagicMock()
        steps: list[str] = []
        result = refresh_metadata(name, projects_root, _client=mock_llm, on_step=steps.append)

        assert "ghost_table" in result.tables  # never deleted
        assert any("ghost_table" in s for s in steps)

    def test_writes_metadata_yaml_to_disk(self, project):
        projects_root, name = project
        _write_metadata(projects_root, name, {
            "orders": {"layer": "silver", "status": "approved", "schema_hash": "deadbeef", "columns": {}},
            "region_summary": {
                "layer": "gold", "status": "approved",
                "schema_hash": _schema_fingerprint([("region", "VARCHAR"), ("total", "DOUBLE")]), "columns": {},
            },
        })
        mock_llm = MagicMock()
        refresh_metadata(name, projects_root, _client=mock_llm)

        reloaded = load_metadata(name, projects_root)
        assert reloaded.tables["orders"].status == "stale"

    def test_on_step_callback_invoked(self, project):
        projects_root, name = project
        _write_metadata(projects_root, name, {
            "orders": {"layer": "silver", "status": "draft", "columns": {}},
            "region_summary": {
                "layer": "gold", "status": "approved",
                "schema_hash": _schema_fingerprint([("region", "VARCHAR"), ("total", "DOUBLE")]), "columns": {},
            },
        })
        mock_llm = MagicMock(side_effect=[
            _draft_json("d", {
                "order_id": {"description": "d"}, "amount": {"description": "d"}, "region": {"description": "d"},
            }),
        ])
        steps: list[str] = []
        refresh_metadata(name, projects_root, _client=mock_llm, on_step=steps.append)
        assert len(steps) >= 1


class TestCmdMetadataRefreshCheck:
    """CLI-level `--check` behavior: drift detection only, no LLM, exit code reflects result."""

    def _args(self, project, name, check=True):
        import argparse
        projects_root, _ = project
        return argparse.Namespace(
            project=name, projects=projects_root, check=check, model=None, provider=None,
        )

    def test_check_exits_zero_when_no_drift(self, project, capsys):
        from openmedallion.cli.main import cmd_metadata_refresh

        projects_root, name = project
        _write_metadata(projects_root, name, {
            "orders": {"layer": "silver", "status": "approved", "schema_hash": _orders_hash(project), "columns": {}},
        })
        # should NOT raise SystemExit
        cmd_metadata_refresh(self._args(project, name))
        out = capsys.readouterr().out
        assert "No schema drift" in out

    def test_check_exits_one_when_drift_detected(self, project):
        from openmedallion.cli.main import cmd_metadata_refresh

        projects_root, name = project
        _write_metadata(projects_root, name, {
            "orders": {"layer": "silver", "status": "approved", "schema_hash": "deadbeef", "columns": {}},
        })
        with pytest.raises(SystemExit) as exc_info:
            cmd_metadata_refresh(self._args(project, name))
        assert exc_info.value.code == 1

    def test_check_mode_never_writes_metadata_yaml(self, project):
        from openmedallion.cli.main import cmd_metadata_refresh

        projects_root, name = project
        _write_metadata(projects_root, name, {
            "orders": {"layer": "silver", "status": "approved", "schema_hash": "deadbeef", "columns": {}},
        })
        before = (projects_root / name / "metadata.yaml").read_text()
        with pytest.raises(SystemExit):
            cmd_metadata_refresh(self._args(project, name))
        after = (projects_root / name / "metadata.yaml").read_text()
        assert before == after  # untouched — no schema_hash update, no status change
