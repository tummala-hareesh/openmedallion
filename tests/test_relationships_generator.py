"""tests/test_relationships_generator.py — RAG roadmap Phase 1, build order
step 5: `medallion relationships generate` (openmedallion.relationships.generator).

No LLM involved — detection is deterministic, so these are plain
integration tests against real Parquet files (mirrors the `project` fixture
pattern in tests/test_metadata_generator.py).
"""
from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
import yaml

from openmedallion.relationships.generator import generate_relationships
from openmedallion.relationships.loader import load_relationships


@pytest.fixture()
def project(tmp_path: Path) -> tuple[Path, str]:
    """A minimal real project: employees/departments (fk_naming) + a gold rollup (grain)."""
    proj_dir   = tmp_path / "proj"
    silver_dir = proj_dir / "data" / "silver"
    gold_dir   = proj_dir / "data" / "gold" / "proj"
    silver_dir.mkdir(parents=True)
    gold_dir.mkdir(parents=True)

    pl.DataFrame({
        "employee_id":   [1, 2, 3],
        "department_id": [10, 20, 10],
    }).write_parquet(silver_dir / "employees.parquet")

    pl.DataFrame({
        "department_id":   [10, 20],
        "department_name": ["Sales", "IT"],
    }).write_parquet(silver_dir / "departments.parquet")

    pl.DataFrame({
        "department_name": ["Sales", "IT"],
        "headcount":       [2, 1],
    }).write_parquet(gold_dir / "headcount_by_department.parquet")

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


def _write_relationships(projects_root: Path, name: str, data: dict) -> None:
    with open(projects_root / name / "relationships.yaml", "w") as f:
        yaml.dump(data, f)


class TestGenerateRelationships:

    def test_detects_across_real_silver_and_gold_files(self, project):
        projects_root, name = project
        result = generate_relationships(name, projects_root)

        pairs = {(r.from_table, r.to_table, tuple(r.join_on)) for r in result.relationships}
        assert ("employees", "departments", ("department_id",)) in pairs
        # Tie on column count (2 vs 2) — alphabetical tie-break: "departments" < "headcount_by_department".
        assert ("departments", "headcount_by_department", ("department_name",)) in pairs

    def test_writes_relationships_yaml_to_disk(self, project):
        projects_root, name = project
        generate_relationships(name, projects_root)

        assert (projects_root / name / "relationships.yaml").exists()
        reloaded = load_relationships(name, projects_root)
        assert len(reloaded.relationships) > 0

    def test_approved_entry_preserved_byte_for_byte(self, project):
        projects_root, name = project
        _write_relationships(projects_root, name, {
            "relationships": [{
                "from_table": "employees", "to_table": "departments",
                "join_on": ["department_id"], "confidence": "high",
                "status": "approved", "method": "fk_naming",
            }],
        })
        result = generate_relationships(name, projects_root)

        matches = [r for r in result.relationships
                   if r.from_table == "employees" and r.to_table == "departments"]
        assert len(matches) == 1
        assert matches[0].status == "approved"

    def test_draft_auto_detected_entry_is_replaced_not_duplicated(self, project):
        projects_root, name = project
        _write_relationships(projects_root, name, {
            "relationships": [{
                "from_table": "employees", "to_table": "departments",
                "join_on": ["department_id"], "confidence": "low",
                "status": "draft", "method": "fk_naming",
            }],
        })
        result = generate_relationships(name, projects_root)

        matches = [r for r in result.relationships
                   if r.from_table == "employees" and r.to_table == "departments"
                   and r.join_on == ["department_id"]]
        assert len(matches) == 1
        # Replaced by the fresh detection (confidence high), not left at the stale low value.
        assert matches[0].confidence == "high"

    def test_hand_added_entry_always_preserved_regardless_of_status(self, project):
        projects_root, name = project
        _write_relationships(projects_root, name, {
            "relationships": [{
                "from_table": "employees", "to_table": "employees",
                "join_on": ["manager_id"], "confidence": "medium",
                "status": "draft",
                # no "method" — this is a hand-added relationship
            }],
        })
        result = generate_relationships(name, projects_root)

        matches = [r for r in result.relationships if r.join_on == ["manager_id"]]
        assert len(matches) == 1
        assert matches[0].method is None
        assert matches[0].status == "draft"

    def test_returns_relationships_config(self, project):
        projects_root, name = project
        result = generate_relationships(name, projects_root)
        from openmedallion.relationships.schema import RelationshipsConfig
        assert isinstance(result, RelationshipsConfig)

    def test_on_step_callback_invoked(self, project):
        projects_root, name = project
        steps: list[str] = []
        generate_relationships(name, projects_root, on_step=steps.append)
        assert len(steps) >= 2

    def test_no_existing_file_is_ok(self, project):
        projects_root, name = project
        result = generate_relationships(name, projects_root)
        assert len(result.relationships) > 0
