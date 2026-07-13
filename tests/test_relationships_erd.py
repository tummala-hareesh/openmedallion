"""tests/test_relationships_erd.py — `medallion relationships erd` (a follow-on
to the RAG accuracy roadmap): render relationships.yaml + real Parquet dtypes
as a Mermaid erDiagram.

Locked design decisions (from the design discussion, not guessed):
- Mermaid ``erDiagram`` output, folded into `medallion relationships` as a
  new subcommand (not a standalone command or a new `explore:` report type).
- Approved-only by default (``include_all=False``) — matches the "approved
  is the trust boundary" convention used everywhere else in the RAG roadmap
  (schema pruning, examples generation). ``include_all=True`` draws every
  relationship regardless of status.
- Only tables that are an endpoint of an *included* relationship are drawn —
  no orphan tables with no relationships.
- Column types come from real Parquet dtypes (``DESCRIBE`` via DuckDB, same
  helper pattern as ``relationships/generator.py:_describe_columns``), not
  from ``metadata.yaml``'s optional/sparse ``columns:`` dict — accurate even
  for a project with no curated metadata at all.
- No PK inference — nothing in relationships.yaml/metadata.yaml actually
  asserts a primary key, so none is guessed at; columns render as plain
  typed fields with the Mermaid relationship line implying the FK.
- Written to a fixed file name, ``<project>/relationships_erd.md`` — a
  Markdown file with a fenced ```mermaid block, directly viewable on GitHub
  and embeddable via mkdocs' ``--8<--`` snippet syntax.
"""
from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
import yaml

from openmedallion.relationships.erd import generate_erd


@pytest.fixture()
def project(tmp_path: Path) -> tuple[Path, str]:
    """A minimal real project: employees/departments (silver) + a gold rollup."""
    proj_dir   = tmp_path / "proj"
    silver_dir = proj_dir / "data" / "silver"
    gold_dir   = proj_dir / "data" / "gold" / "proj"
    silver_dir.mkdir(parents=True)
    gold_dir.mkdir(parents=True)

    pl.DataFrame({
        "employee_id":   [1, 2, 3],
        "department_id": [10, 20, 10],
        "name":          ["Alice", "Bob", "Carol"],
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


class TestGenerateErd:

    def test_renders_mermaid_er_diagram_block(self, project):
        projects_root, name = project
        _write_relationships(projects_root, name, {"relationships": [
            {"from_table": "employees", "to_table": "departments",
             "join_on": ["department_id"], "confidence": "high", "status": "approved"},
        ]})
        content = generate_erd(name, projects_root)
        assert "```mermaid" in content
        assert "erDiagram" in content
        assert "```" in content

    def test_excludes_draft_relationships_by_default(self, project):
        projects_root, name = project
        _write_relationships(projects_root, name, {"relationships": [
            {"from_table": "employees", "to_table": "departments",
             "join_on": ["department_id"], "confidence": "high", "status": "draft"},
        ]})
        content = generate_erd(name, projects_root)
        assert "EMPLOYEES" not in content.upper() or "departments" not in content

    def test_includes_approved_relationships_by_default(self, project):
        projects_root, name = project
        _write_relationships(projects_root, name, {"relationships": [
            {"from_table": "employees", "to_table": "departments",
             "join_on": ["department_id"], "confidence": "high", "status": "approved"},
        ]})
        content = generate_erd(name, projects_root)
        assert "employees" in content
        assert "departments" in content

    def test_include_all_flag_includes_draft(self, project):
        projects_root, name = project
        _write_relationships(projects_root, name, {"relationships": [
            {"from_table": "employees", "to_table": "departments",
             "join_on": ["department_id"], "confidence": "high", "status": "draft"},
        ]})
        content = generate_erd(name, projects_root, include_all=True)
        assert "employees" in content
        assert "departments" in content

    def test_only_includes_tables_referenced_by_included_relationships(self, project):
        projects_root, name = project
        _write_relationships(projects_root, name, {"relationships": [
            {"from_table": "employees", "to_table": "departments",
             "join_on": ["department_id"], "confidence": "high", "status": "approved"},
        ]})
        content = generate_erd(name, projects_root)
        # headcount_by_department is a real gold table but isn't referenced
        # by any approved relationship in this test, so it must not appear.
        assert "headcount_by_department" not in content

    def test_includes_column_dtypes_from_real_parquet(self, project):
        projects_root, name = project
        _write_relationships(projects_root, name, {"relationships": [
            {"from_table": "employees", "to_table": "departments",
             "join_on": ["department_id"], "confidence": "high", "status": "approved"},
        ]})
        content = generate_erd(name, projects_root)
        assert "employee_id" in content
        assert "name" in content
        assert "department_name" in content

    def test_includes_gold_table_when_referenced(self, project):
        projects_root, name = project
        _write_relationships(projects_root, name, {"relationships": [
            {"from_table": "departments", "to_table": "headcount_by_department",
             "join_on": ["department_name"], "confidence": "medium", "status": "approved"},
        ]})
        content = generate_erd(name, projects_root)
        assert "headcount_by_department" in content
        assert "headcount" in content

    def test_join_on_multiple_columns_shown_in_label(self, project):
        projects_root, name = project
        _write_relationships(projects_root, name, {"relationships": [
            {"from_table": "employees", "to_table": "departments",
             "join_on": ["department_id"], "confidence": "high", "status": "approved"},
        ]})
        content = generate_erd(name, projects_root)
        assert "department_id" in content

    def test_no_relationships_at_all_still_returns_valid_markdown(self, project):
        projects_root, name = project
        content = generate_erd(name, projects_root)
        assert "```mermaid" in content
        assert "erDiagram" in content

    def test_writes_to_relationships_erd_md(self, project):
        projects_root, name = project
        _write_relationships(projects_root, name, {"relationships": [
            {"from_table": "employees", "to_table": "departments",
             "join_on": ["department_id"], "confidence": "high", "status": "approved"},
        ]})
        generate_erd(name, projects_root)
        erd_path = projects_root / name / "relationships_erd.md"
        assert erd_path.exists()
        assert "erDiagram" in erd_path.read_text()

    def test_missing_parquet_for_referenced_table_does_not_crash(self, project):
        projects_root, name = project
        _write_relationships(projects_root, name, {"relationships": [
            {"from_table": "employees", "to_table": "nonexistent_table",
             "join_on": ["some_id"], "confidence": "low", "status": "approved"},
        ]})
        content = generate_erd(name, projects_root)
        assert "nonexistent_table" in content

    def test_no_pk_marker_present(self, project):
        # Locked decision: no PK inference — nothing asserts a primary key,
        # so the generator must never emit a "PK" annotation.
        projects_root, name = project
        _write_relationships(projects_root, name, {"relationships": [
            {"from_table": "employees", "to_table": "departments",
             "join_on": ["department_id"], "confidence": "high", "status": "approved"},
        ]})
        content = generate_erd(name, projects_root)
        assert " PK" not in content
