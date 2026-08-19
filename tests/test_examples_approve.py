"""tests/test_examples_approve.py — RAG roadmap Phase 1/2 boundary, build order
step 8: `medallion examples approve` (openmedallion.examples.approve).

Locked design decision: an example's identity for approval purposes is a
content hash of (question, sql) — derived on the fly, no schema change to
the already-shipped SyntheticExample/generate_examples(). Editing the
question/sql text between generate and approve is correctly treated as a
different example (new hash), matching how relationships identity
((from_table, to_table, join_on)) already works.
"""
from pathlib import Path

import pytest

from openmedallion.examples.approve import (
    apply_approvals,
    approve_examples,
    content_hash,
    list_reviewable_examples,
    list_templatable_examples,
)


def _raw() -> list[dict]:
    return [
        {"question": "How many orders?", "sql": "SELECT COUNT(*) AS n FROM orders", "verified": False},
        {"question": "Total revenue?", "sql": "SELECT SUM(amount) FROM orders", "verified": False},
        {"question": "Already checked", "sql": "SELECT 1", "verified": True},
    ]


def _raw_with_templating() -> list[dict]:
    return [
        {"question": "How many orders?", "sql": "SELECT COUNT(*) AS n FROM orders", "verified": False},
        {"question": "Already checked", "sql": "SELECT 1", "verified": True},
        {
            "question": "Revenue by region for {date_range}?",
            "sql": "SELECT region, SUM(amount) FROM orders WHERE order_date BETWEEN {date_range} GROUP BY region",
            "verified": True,
        },
        {
            "question": "Already a template",
            "sql": "SELECT * FROM orders WHERE region = {region}",
            "verified": True,
            "templated": True,
            "params": {"region": "one of: US, EU, APAC"},
        },
    ]


class TestContentHash:

    def test_deterministic(self):
        assert content_hash("q", "SELECT 1") == content_hash("q", "SELECT 1")

    def test_differs_on_question(self):
        assert content_hash("q1", "SELECT 1") != content_hash("q2", "SELECT 1")

    def test_differs_on_sql(self):
        assert content_hash("q", "SELECT 1") != content_hash("q", "SELECT 2")


class TestApplyApprovals:

    def test_approve_flips_verified(self):
        raw = _raw()
        key = content_hash("How many orders?", "SELECT COUNT(*) AS n FROM orders")
        result = apply_approvals(raw, {key: {"verified": True}})
        matches = [r for r in result if r["question"] == "How many orders?"]
        assert matches[0]["verified"] is True

    def test_unrelated_entries_untouched(self):
        raw = _raw()
        key = content_hash("How many orders?", "SELECT COUNT(*) AS n FROM orders")
        result = apply_approvals(raw, {key: {"verified": True}})
        revenue = next(r for r in result if r["question"] == "Total revenue?")
        checked = next(r for r in result if r["question"] == "Already checked")
        assert revenue["verified"] is False
        assert checked["verified"] is True

    def test_does_not_mutate_input(self):
        raw = _raw()
        key = content_hash("How many orders?", "SELECT COUNT(*) AS n FROM orders")
        apply_approvals(raw, {key: {"verified": True}})
        assert raw[0]["verified"] is False

    def test_edit_before_approving_is_deep_merged(self):
        raw = _raw()
        key = content_hash("How many orders?", "SELECT COUNT(*) AS n FROM orders")
        result = apply_approvals(raw, {key: {"sql": "SELECT COUNT(*) AS total FROM orders", "verified": True}})
        matches = [r for r in result if r["question"] == "How many orders?"]
        assert matches[0]["sql"] == "SELECT COUNT(*) AS total FROM orders"
        assert matches[0]["verified"] is True

    def test_skip_decision_leaves_verified_untouched(self):
        raw = _raw()
        key = content_hash("How many orders?", "SELECT COUNT(*) AS n FROM orders")
        result = apply_approvals(raw, {key: {}})
        matches = [r for r in result if r["question"] == "How many orders?"]
        assert matches[0]["verified"] is False

    def test_unknown_hash_raises(self):
        raw = _raw()
        with pytest.raises(ValueError, match="nonexistent"):
            apply_approvals(raw, {"nonexistent": {"verified": True}})

    def test_empty_decisions_is_noop(self):
        raw = _raw()
        result = apply_approvals(raw, {})
        assert result == raw

    def test_editing_content_changes_identity(self):
        # Editing question/sql produces a different hash — correctly treated
        # as "not the same example" for any decision keyed by the OLD hash.
        raw = _raw()
        old_key = content_hash("How many orders?", "SELECT COUNT(*) AS n FROM orders")
        result = apply_approvals(raw, {old_key: {"question": "How many total orders?"}})
        edited = result[0]
        new_key = content_hash(edited["question"], edited["sql"])
        assert new_key != old_key
        with pytest.raises(ValueError):
            apply_approvals(result, {old_key: {"verified": True}})


class TestListReviewableExamples:

    def _write(self, root: Path, project: str, lines: list[dict]) -> Path:
        project_dir = root / project / "examples"
        project_dir.mkdir(parents=True, exist_ok=True)
        with open(project_dir / "synthetic.jsonl", "w") as f:
            for line in lines:
                import json
                f.write(json.dumps(line) + "\n")
        return project_dir

    def test_returns_unverified_only(self, tmp_path):
        self._write(tmp_path, "proj", _raw())
        reviewable = list_reviewable_examples("proj", tmp_path)
        questions = {e.question for e in reviewable}
        assert questions == {"How many orders?", "Total revenue?"}

    def test_empty_when_all_verified(self, tmp_path):
        raw = _raw()
        for r in raw:
            r["verified"] = True
        self._write(tmp_path, "proj", raw)
        assert list_reviewable_examples("proj", tmp_path) == []

    def test_missing_file_returns_empty(self, tmp_path):
        (tmp_path / "proj").mkdir(parents=True)
        assert list_reviewable_examples("proj", tmp_path) == []


class TestApproveExamples:

    def _write(self, root: Path, project: str, lines: list[dict]) -> Path:
        project_dir = root / project / "examples"
        project_dir.mkdir(parents=True, exist_ok=True)
        with open(project_dir / "synthetic.jsonl", "w") as f:
            for line in lines:
                import json
                f.write(json.dumps(line) + "\n")
        return project_dir

    def test_writes_approval_back_to_disk(self, tmp_path):
        self._write(tmp_path, "proj", _raw())
        key = content_hash("How many orders?", "SELECT COUNT(*) AS n FROM orders")
        approve_examples("proj", tmp_path, {key: {"verified": True}})

        path = tmp_path / "proj" / "examples" / "synthetic.jsonl"
        import json
        lines = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
        matches = [line for line in lines if line["question"] == "How many orders?"]
        assert matches[0]["verified"] is True
        untouched = [line for line in lines if line["question"] == "Total revenue?"]
        assert untouched[0]["verified"] is False

    def test_returns_synthetic_examples(self, tmp_path):
        self._write(tmp_path, "proj", _raw())
        key = content_hash("How many orders?", "SELECT COUNT(*) AS n FROM orders")
        result = approve_examples("proj", tmp_path, {key: {"verified": True}})
        matches = [e for e in result if e.question == "How many orders?"]
        assert matches[0].verified is True

    def test_missing_synthetic_jsonl_raises_clear_error(self, tmp_path):
        (tmp_path / "proj").mkdir(parents=True)
        with pytest.raises(FileNotFoundError, match="examples generate"):
            approve_examples("proj", tmp_path, {"x": {"verified": True}})

    def test_preserves_order(self, tmp_path):
        raw = _raw()
        self._write(tmp_path, "proj", raw)
        key = content_hash("Total revenue?", "SELECT SUM(amount) FROM orders")
        result = approve_examples("proj", tmp_path, {key: {"verified": True}})
        assert [e.question for e in result] == [r["question"] for r in raw]


class TestListTemplatableExamples:
    """Template-Routed Query Layer roadmap (see CLAUDE.md), build order step 2:
    `medallion examples approve --template` reviews only already-`verified:
    true` examples not yet promoted to `templated: true` — templating is a
    stricter, later, separate review pass, never implied by verification.
    """

    def _write(self, root: Path, project: str, lines: list[dict]) -> Path:
        project_dir = root / project / "examples"
        project_dir.mkdir(parents=True, exist_ok=True)
        with open(project_dir / "synthetic.jsonl", "w") as f:
            for line in lines:
                import json
                f.write(json.dumps(line) + "\n")
        return project_dir

    def test_returns_verified_untemplated_only(self, tmp_path):
        self._write(tmp_path, "proj", _raw_with_templating())
        templatable = list_templatable_examples("proj", tmp_path)
        questions = {e.question for e in templatable}
        assert questions == {"Already checked", "Revenue by region for {date_range}?"}

    def test_excludes_unverified_entries(self, tmp_path):
        self._write(tmp_path, "proj", _raw_with_templating())
        templatable = list_templatable_examples("proj", tmp_path)
        assert "How many orders?" not in {e.question for e in templatable}

    def test_excludes_already_templated_entries(self, tmp_path):
        self._write(tmp_path, "proj", _raw_with_templating())
        templatable = list_templatable_examples("proj", tmp_path)
        assert "Already a template" not in {e.question for e in templatable}

    def test_preserves_file_order(self, tmp_path):
        raw = _raw_with_templating()
        self._write(tmp_path, "proj", raw)
        templatable = list_templatable_examples("proj", tmp_path)
        expected_order = [r["question"] for r in raw if r["verified"] and not r.get("templated")]
        assert [e.question for e in templatable] == expected_order

    def test_empty_when_nothing_verified(self, tmp_path):
        self._write(tmp_path, "proj", _raw())
        # _raw() has one verified entry ("Already checked") with no params.
        templatable = list_templatable_examples("proj", tmp_path)
        assert [e.question for e in templatable] == ["Already checked"]

    def test_missing_file_returns_empty(self, tmp_path):
        (tmp_path / "proj").mkdir(parents=True)
        assert list_templatable_examples("proj", tmp_path) == []


class TestTemplatingDecisions:
    """Promotion itself is a generic decisions-dict merge (apply_approvals
    already supports arbitrary field edits) — these tests lock the specific
    templated/params promotion path end-to-end through approve_examples,
    including the schema's `templated` requires `verified` guard.
    """

    def _write(self, root: Path, project: str, lines: list[dict]) -> Path:
        project_dir = root / project / "examples"
        project_dir.mkdir(parents=True, exist_ok=True)
        with open(project_dir / "synthetic.jsonl", "w") as f:
            for line in lines:
                import json
                f.write(json.dumps(line) + "\n")
        return project_dir

    def test_promotes_verified_example_to_template(self, tmp_path):
        raw = _raw_with_templating()
        self._write(tmp_path, "proj", raw)
        key = content_hash(
            "Revenue by region for {date_range}?",
            "SELECT region, SUM(amount) FROM orders WHERE order_date BETWEEN {date_range} GROUP BY region",
        )
        decisions = {key: {"templated": True, "params": {"date_range": "ISO date range, e.g. 2026-Q1"}}}
        result = approve_examples("proj", tmp_path, decisions)
        promoted = next(e for e in result if e.question == "Revenue by region for {date_range}?")
        assert promoted.templated is True
        assert promoted.params == {"date_range": "ISO date range, e.g. 2026-Q1"}

    def test_promotion_persists_to_disk(self, tmp_path):
        raw = _raw_with_templating()
        self._write(tmp_path, "proj", raw)
        key = content_hash(
            "Revenue by region for {date_range}?",
            "SELECT region, SUM(amount) FROM orders WHERE order_date BETWEEN {date_range} GROUP BY region",
        )
        decisions = {key: {"templated": True, "params": {"date_range": "ISO date range, e.g. 2026-Q1"}}}
        approve_examples("proj", tmp_path, decisions)

        path = tmp_path / "proj" / "examples" / "synthetic.jsonl"
        import json
        lines = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
        promoted = next(line for line in lines if line["question"] == "Revenue by region for {date_range}?")
        assert promoted["templated"] is True
        assert promoted["params"] == {"date_range": "ISO date range, e.g. 2026-Q1"}

    def test_templating_an_unverified_example_raises(self, tmp_path):
        raw = _raw_with_templating()
        self._write(tmp_path, "proj", raw)
        key = content_hash("How many orders?", "SELECT COUNT(*) AS n FROM orders")
        decisions = {key: {"templated": True, "params": {}}}
        with pytest.raises(ValueError, match="templated"):
            approve_examples("proj", tmp_path, decisions)

    def test_templating_does_not_disturb_other_entries(self, tmp_path):
        raw = _raw_with_templating()
        self._write(tmp_path, "proj", raw)
        key = content_hash(
            "Revenue by region for {date_range}?",
            "SELECT region, SUM(amount) FROM orders WHERE order_date BETWEEN {date_range} GROUP BY region",
        )
        decisions = {key: {"templated": True, "params": {"date_range": "ISO date range, e.g. 2026-Q1"}}}
        result = approve_examples("proj", tmp_path, decisions)
        untouched = next(e for e in result if e.question == "Already checked")
        assert untouched.templated is False
        assert untouched.params is None
