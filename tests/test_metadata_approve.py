"""tests/test_metadata_approve.py — RAG roadmap Phase 1, build order step 3:
`medallion metadata approve` (openmedallion.metadata.approve).

The interactive input() loop in cli/main.py is intentionally thin and
untested here — these tests exercise the pure, testable core: apply_approvals
(status transitions + optional edits, deep-merged into the raw dict),
approve_metadata (validate + write back to disk), and list_reviewable_tables
(what the interactive loop iterates over).
"""
from pathlib import Path

import pytest
import yaml

from openmedallion.metadata.approve import apply_approvals, approve_metadata, list_reviewable_tables
from openmedallion.metadata.loader import load_metadata


def _raw() -> dict:
    return {
        "tables": {
            "orders": {
                "layer": "silver",
                "description": "Order line items.",
                "status": "draft",
                "columns": {
                    "order_id": {"description": "Unique order id."},
                    "amount":   {"description": "Order amount."},
                },
            },
            "customers": {
                "layer": "silver",
                "description": "Customer records.",
                "status": "stale",
                "columns": {"customer_id": {"description": "Unique customer id."}},
            },
            "region_summary": {
                "layer": "gold",
                "description": "Already reviewed.",
                "status": "approved",
                "columns": {"region": {"description": "Sales region."}},
            },
        },
        "glossary": {"term": "definition"},
    }


class TestApplyApprovals:

    def test_approve_flips_status(self):
        raw = _raw()
        result = apply_approvals(raw, {"orders": {"status": "approved"}})
        assert result["tables"]["orders"]["status"] == "approved"

    def test_unrelated_tables_untouched(self):
        raw = _raw()
        result = apply_approvals(raw, {"orders": {"status": "approved"}})
        assert result["tables"]["customers"]["status"] == "stale"
        assert result["tables"]["region_summary"]["status"] == "approved"
        assert result["tables"]["region_summary"]["description"] == "Already reviewed."

    def test_does_not_mutate_input(self):
        raw = _raw()
        apply_approvals(raw, {"orders": {"status": "approved"}})
        assert raw["tables"]["orders"]["status"] == "draft"

    def test_glossary_untouched(self):
        raw = _raw()
        result = apply_approvals(raw, {"orders": {"status": "approved"}})
        assert result["glossary"] == {"term": "definition"}

    def test_edits_are_deep_merged_before_approving(self):
        raw = _raw()
        result = apply_approvals(raw, {
            "orders": {"description": "Human-refined description.", "status": "approved"},
        })
        assert result["tables"]["orders"]["description"] == "Human-refined description."
        assert result["tables"]["orders"]["status"] == "approved"
        # Untouched sibling fields survive the merge.
        assert result["tables"]["orders"]["columns"]["amount"]["description"] == "Order amount."

    def test_column_level_edit_is_deep_merged(self):
        raw = _raw()
        result = apply_approvals(raw, {
            "orders": {"columns": {"amount": {"synonyms": ["total"]}}, "status": "approved"},
        })
        assert result["tables"]["orders"]["columns"]["amount"]["synonyms"] == ["total"]
        assert result["tables"]["orders"]["columns"]["amount"]["description"] == "Order amount."
        assert result["tables"]["orders"]["columns"]["order_id"]["description"] == "Unique order id."

    def test_skip_decision_leaves_status_untouched(self):
        raw = _raw()
        # No "status" key in the decision — table stays draft (a "skip" from the CLI).
        result = apply_approvals(raw, {"orders": {}})
        assert result["tables"]["orders"]["status"] == "draft"

    def test_unknown_table_raises(self):
        raw = _raw()
        with pytest.raises(ValueError, match="nonexistent"):
            apply_approvals(raw, {"nonexistent": {"status": "approved"}})

    def test_empty_decisions_is_noop(self):
        raw = _raw()
        result = apply_approvals(raw, {})
        assert result == raw

    def test_approving_already_approved_table_is_idempotent(self):
        raw = _raw()
        result = apply_approvals(raw, {"region_summary": {"status": "approved"}})
        assert result["tables"]["region_summary"]["status"] == "approved"
        assert result["tables"]["region_summary"]["description"] == "Already reviewed."


class TestListReviewableTables:

    def test_returns_draft_and_stale_only(self, tmp_path):
        _write(tmp_path, "proj", _raw())
        reviewable = list_reviewable_tables("proj", tmp_path)
        names = {name for name, _ in reviewable}
        assert names == {"orders", "customers"}

    def test_sorted_by_name(self, tmp_path):
        _write(tmp_path, "proj", _raw())
        reviewable = list_reviewable_tables("proj", tmp_path)
        assert [name for name, _ in reviewable] == sorted(name for name, _ in reviewable)

    def test_empty_when_all_approved(self, tmp_path):
        raw = _raw()
        raw["tables"]["orders"]["status"] = "approved"
        raw["tables"]["customers"]["status"] = "approved"
        _write(tmp_path, "proj", raw)
        assert list_reviewable_tables("proj", tmp_path) == []

    def test_missing_metadata_yaml_returns_empty(self, tmp_path):
        (tmp_path / "proj").mkdir(parents=True)
        assert list_reviewable_tables("proj", tmp_path) == []


class TestApproveMetadata:

    def test_writes_approval_back_to_disk(self, tmp_path):
        _write(tmp_path, "proj", _raw())
        approve_metadata("proj", tmp_path, {"orders": {"status": "approved"}})

        with open(tmp_path / "proj" / "metadata.yaml") as f:
            on_disk = yaml.safe_load(f)
        assert on_disk["tables"]["orders"]["status"] == "approved"
        assert on_disk["tables"]["customers"]["status"] == "stale"

    def test_returns_metadata_config(self, tmp_path):
        _write(tmp_path, "proj", _raw())
        result = approve_metadata("proj", tmp_path, {"orders": {"status": "approved"}})
        assert result.tables["orders"].status == "approved"

    def test_does_not_touch_enhancements_file(self, tmp_path):
        _write(tmp_path, "proj", _raw(), enhancements={"glossary": {"extra": "value"}})
        approve_metadata("proj", tmp_path, {"orders": {"status": "approved"}})

        with open(tmp_path / "proj" / "metadata_enhancements.yaml") as f:
            enh = yaml.safe_load(f)
        assert enh == {"glossary": {"extra": "value"}}

    def test_merged_load_still_reflects_enhancements_after_approval(self, tmp_path):
        _write(tmp_path, "proj", _raw(), enhancements={"tables": {"orders": {"synonyms": ["po"]}}})
        approve_metadata("proj", tmp_path, {"orders": {"status": "approved"}})

        merged = load_metadata("proj", tmp_path)
        assert merged.tables["orders"].status == "approved"
        assert merged.tables["orders"].synonyms == ["po"]

    def test_missing_metadata_yaml_raises_clear_error(self, tmp_path):
        (tmp_path / "proj").mkdir(parents=True)
        with pytest.raises(FileNotFoundError, match="metadata generate"):
            approve_metadata("proj", tmp_path, {"orders": {"status": "approved"}})

    def test_invalid_edit_raises_value_error(self, tmp_path):
        _write(tmp_path, "proj", _raw())
        with pytest.raises(ValueError, match="status"):
            approve_metadata("proj", tmp_path, {"orders": {"status": "reviewed"}})


def _write(root: Path, project: str, metadata: dict, enhancements: dict | None = None) -> Path:
    project_dir = root / project
    project_dir.mkdir(parents=True, exist_ok=True)
    with open(project_dir / "metadata.yaml", "w") as f:
        yaml.dump(metadata, f)
    if enhancements is not None:
        with open(project_dir / "metadata_enhancements.yaml", "w") as f:
            yaml.dump(enhancements, f)
    return project_dir
