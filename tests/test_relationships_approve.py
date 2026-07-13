"""tests/test_relationships_approve.py — RAG roadmap Phase 1, build order step 6:
`medallion relationships approve` (openmedallion.relationships.approve).

Near-direct port of tests/test_metadata_approve.py's structure, adapted for
relationships being a list (keyed by (from_table, to_table, join_on) identity)
rather than a dict-of-tables (keyed by table name).
"""
from pathlib import Path

import pytest
import yaml

from openmedallion.relationships.approve import (
    apply_approvals,
    approve_relationships,
    list_reviewable_relationships,
)
from openmedallion.relationships.loader import load_relationships


def _raw() -> dict:
    return {
        "relationships": [
            {
                "from_table": "employees", "to_table": "departments",
                "join_on": ["department_id"], "confidence": "high",
                "status": "draft", "method": "fk_naming",
            },
            {
                "from_table": "employees", "to_table": "jobs",
                "join_on": ["job_id"], "confidence": "high",
                "status": "stale", "method": "fk_naming",
            },
            {
                "from_table": "employees_enriched", "to_table": "departments",
                "join_on": ["department_id"], "confidence": "high",
                "status": "approved", "method": "fk_naming",
            },
        ],
    }


class TestApplyApprovals:

    def test_approve_flips_status(self):
        raw = _raw()
        key = ("employees", "departments", ("department_id",))
        result = apply_approvals(raw, {key: {"status": "approved"}})
        matches = [r for r in result["relationships"]
                   if r["from_table"] == "employees" and r["to_table"] == "departments"]
        assert matches[0]["status"] == "approved"

    def test_unrelated_entries_untouched(self):
        raw = _raw()
        key = ("employees", "departments", ("department_id",))
        result = apply_approvals(raw, {key: {"status": "approved"}})
        jobs_entry = next(r for r in result["relationships"] if r["to_table"] == "jobs")
        approved_entry = next(r for r in result["relationships"]
                               if r["from_table"] == "employees_enriched")
        assert jobs_entry["status"] == "stale"
        assert approved_entry["status"] == "approved"
        assert approved_entry["confidence"] == "high"

    def test_does_not_mutate_input(self):
        raw = _raw()
        key = ("employees", "departments", ("department_id",))
        apply_approvals(raw, {key: {"status": "approved"}})
        matches = [r for r in raw["relationships"]
                   if r["from_table"] == "employees" and r["to_table"] == "departments"]
        assert matches[0]["status"] == "draft"

    def test_edits_are_deep_merged_before_approving(self):
        raw = _raw()
        key = ("employees", "departments", ("department_id",))
        result = apply_approvals(raw, {key: {"confidence": "medium", "status": "approved"}})
        matches = [r for r in result["relationships"]
                   if r["from_table"] == "employees" and r["to_table"] == "departments"]
        assert matches[0]["confidence"] == "medium"
        assert matches[0]["status"] == "approved"
        assert matches[0]["method"] == "fk_naming"

    def test_skip_decision_leaves_status_untouched(self):
        raw = _raw()
        key = ("employees", "departments", ("department_id",))
        result = apply_approvals(raw, {key: {}})
        matches = [r for r in result["relationships"]
                   if r["from_table"] == "employees" and r["to_table"] == "departments"]
        assert matches[0]["status"] == "draft"

    def test_unknown_identity_raises(self):
        raw = _raw()
        key = ("nonexistent", "table", ("col",))
        with pytest.raises(ValueError, match="nonexistent"):
            apply_approvals(raw, {key: {"status": "approved"}})

    def test_empty_decisions_is_noop(self):
        raw = _raw()
        result = apply_approvals(raw, {})
        assert result == raw

    def test_approving_already_approved_is_idempotent(self):
        raw = _raw()
        key = ("employees_enriched", "departments", ("department_id",))
        result = apply_approvals(raw, {key: {"status": "approved"}})
        matches = [r for r in result["relationships"] if r["from_table"] == "employees_enriched"]
        assert matches[0]["status"] == "approved"

    def test_identity_ignores_join_on_order(self):
        # join_on order in the decision key shouldn't matter — identity is
        # sorted before comparison, matching detector._identity()'s convention.
        raw = _raw()
        raw["relationships"][0]["join_on"] = ["a", "b"]
        key = ("employees", "departments", ("b", "a"))
        result = apply_approvals(raw, {key: {"status": "approved"}})
        matches = [r for r in result["relationships"]
                   if r["from_table"] == "employees" and r["to_table"] == "departments"]
        assert matches[0]["status"] == "approved"


class TestListReviewableRelationships:

    def test_returns_draft_and_stale_only(self, tmp_path):
        _write(tmp_path, "proj", _raw())
        reviewable = list_reviewable_relationships("proj", tmp_path)
        pairs = {(r.from_table, r.to_table) for r in reviewable}
        assert pairs == {("employees", "departments"), ("employees", "jobs")}

    def test_sorted_deterministically(self, tmp_path):
        _write(tmp_path, "proj", _raw())
        reviewable = list_reviewable_relationships("proj", tmp_path)
        keys = [(r.from_table, r.to_table, tuple(r.join_on)) for r in reviewable]
        assert keys == sorted(keys)

    def test_empty_when_all_approved(self, tmp_path):
        raw = _raw()
        for r in raw["relationships"]:
            r["status"] = "approved"
        _write(tmp_path, "proj", raw)
        assert list_reviewable_relationships("proj", tmp_path) == []

    def test_missing_file_returns_empty(self, tmp_path):
        (tmp_path / "proj").mkdir(parents=True)
        assert list_reviewable_relationships("proj", tmp_path) == []


class TestApproveRelationships:

    def test_writes_approval_back_to_disk(self, tmp_path):
        _write(tmp_path, "proj", _raw())
        key = ("employees", "departments", ("department_id",))
        approve_relationships("proj", tmp_path, {key: {"status": "approved"}})

        with open(tmp_path / "proj" / "relationships.yaml") as f:
            on_disk = yaml.safe_load(f)
        matches = [r for r in on_disk["relationships"]
                   if r["from_table"] == "employees" and r["to_table"] == "departments"]
        assert matches[0]["status"] == "approved"
        jobs_entry = next(r for r in on_disk["relationships"] if r["to_table"] == "jobs")
        assert jobs_entry["status"] == "stale"

    def test_returns_relationships_config(self, tmp_path):
        _write(tmp_path, "proj", _raw())
        key = ("employees", "departments", ("department_id",))
        result = approve_relationships("proj", tmp_path, {key: {"status": "approved"}})
        matches = [r for r in result.relationships
                   if r.from_table == "employees" and r.to_table == "departments"]
        assert matches[0].status == "approved"

    def test_missing_relationships_yaml_raises_clear_error(self, tmp_path):
        (tmp_path / "proj").mkdir(parents=True)
        with pytest.raises(FileNotFoundError, match="relationships generate"):
            approve_relationships("proj", tmp_path, {("a", "b", ("c",)): {"status": "approved"}})

    def test_invalid_edit_raises_value_error(self, tmp_path):
        _write(tmp_path, "proj", _raw())
        key = ("employees", "departments", ("department_id",))
        with pytest.raises(ValueError, match="status"):
            approve_relationships("proj", tmp_path, {key: {"status": "reviewed"}})

    def test_merged_load_reflects_approval(self, tmp_path):
        _write(tmp_path, "proj", _raw())
        key = ("employees", "departments", ("department_id",))
        approve_relationships("proj", tmp_path, {key: {"status": "approved"}})

        reloaded = load_relationships("proj", tmp_path)
        matches = [r for r in reloaded.relationships
                   if r.from_table == "employees" and r.to_table == "departments"]
        assert matches[0].status == "approved"


def _write(root: Path, project: str, data: dict) -> Path:
    project_dir = root / project
    project_dir.mkdir(parents=True, exist_ok=True)
    with open(project_dir / "relationships.yaml", "w") as f:
        yaml.dump(data, f)
    return project_dir
