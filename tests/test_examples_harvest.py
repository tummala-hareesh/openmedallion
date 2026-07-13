"""tests/test_examples_harvest.py — RAG roadmap Phase 3, build order step 14
(final step): `medallion examples harvest` / `review` (openmedallion.examples.harvest).

Locked design decisions:
- harvest_candidates() promotes harvested.jsonl "status: candidate" entries
  into synthetic.jsonl as verified: false — still needs a human pass via the
  existing `medallion examples approve`, since a thumbs-up means "this
  looked right to a user", not "the SQL is definitely correct".
- Promoted candidates are marked status: "harvested" (not deleted) so
  re-running harvest never duplicates them, and duplicates against existing
  synthetic.jsonl entries (by content_hash) are skipped.
- list_failures() is a plain, honest listing of failures.jsonl — no
  fabricated "error type" grouping, since failures.jsonl (already shipped)
  has no reason/error-type field to group by.
"""
from __future__ import annotations

import json
from pathlib import Path

from openmedallion.examples.harvest import harvest_candidates, list_failures
from openmedallion.examples.schema import SyntheticExample


def _write_jsonl(path: Path, lines: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        for line in lines:
            f.write(json.dumps(line) + "\n")


def _read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


class TestHarvestCandidates:

    def test_promotes_candidates_as_unverified(self, tmp_path):
        _write_jsonl(tmp_path / "proj" / "examples" / "harvested.jsonl", [
            {"question": "How many orders?", "sql": "SELECT COUNT(*) AS n FROM orders",
             "result_shape": {"rows": 1, "columns": ["n"]}, "status": "candidate"},
        ])
        result = harvest_candidates("proj", tmp_path)
        assert len(result) == 1
        assert isinstance(result[0], SyntheticExample)
        assert result[0].question == "How many orders?"
        assert result[0].verified is False

    def test_writes_to_synthetic_jsonl(self, tmp_path):
        _write_jsonl(tmp_path / "proj" / "examples" / "harvested.jsonl", [
            {"question": "q", "sql": "SELECT 1", "result_shape": {"rows": 1, "columns": []}, "status": "candidate"},
        ])
        harvest_candidates("proj", tmp_path)
        entries = _read_jsonl(tmp_path / "proj" / "examples" / "synthetic.jsonl")
        assert len(entries) == 1
        assert entries[0]["verified"] is False

    def test_preserves_existing_synthetic_entries(self, tmp_path):
        _write_jsonl(tmp_path / "proj" / "examples" / "synthetic.jsonl", [
            {"question": "existing verified", "sql": "SELECT 1", "verified": True},
        ])
        _write_jsonl(tmp_path / "proj" / "examples" / "harvested.jsonl", [
            {"question": "new candidate", "sql": "SELECT 2", "result_shape": {"rows": 1, "columns": []}, "status": "candidate"},
        ])
        harvest_candidates("proj", tmp_path)
        entries = _read_jsonl(tmp_path / "proj" / "examples" / "synthetic.jsonl")
        questions = {e["question"] for e in entries}
        assert questions == {"existing verified", "new candidate"}
        existing = next(e for e in entries if e["question"] == "existing verified")
        assert existing["verified"] is True

    def test_marks_promoted_candidates_as_harvested(self, tmp_path):
        _write_jsonl(tmp_path / "proj" / "examples" / "harvested.jsonl", [
            {"question": "q", "sql": "SELECT 1", "result_shape": {"rows": 1, "columns": []}, "status": "candidate"},
        ])
        harvest_candidates("proj", tmp_path)
        entries = _read_jsonl(tmp_path / "proj" / "examples" / "harvested.jsonl")
        assert entries[0]["status"] == "harvested"

    def test_already_harvested_entries_are_not_reharvested(self, tmp_path):
        _write_jsonl(tmp_path / "proj" / "examples" / "harvested.jsonl", [
            {"question": "old", "sql": "SELECT 1", "result_shape": {"rows": 1, "columns": []}, "status": "harvested"},
        ])
        result = harvest_candidates("proj", tmp_path)
        assert result == []
        entries = _read_jsonl(tmp_path / "proj" / "examples" / "synthetic.jsonl")
        assert entries == []

    def test_running_harvest_twice_does_not_duplicate(self, tmp_path):
        _write_jsonl(tmp_path / "proj" / "examples" / "harvested.jsonl", [
            {"question": "q", "sql": "SELECT 1", "result_shape": {"rows": 1, "columns": []}, "status": "candidate"},
        ])
        harvest_candidates("proj", tmp_path)
        second_result = harvest_candidates("proj", tmp_path)
        assert second_result == []
        entries = _read_jsonl(tmp_path / "proj" / "examples" / "synthetic.jsonl")
        assert len(entries) == 1

    def test_skips_candidate_already_present_in_synthetic_by_content(self, tmp_path):
        _write_jsonl(tmp_path / "proj" / "examples" / "synthetic.jsonl", [
            {"question": "q", "sql": "SELECT 1", "verified": False},
        ])
        _write_jsonl(tmp_path / "proj" / "examples" / "harvested.jsonl", [
            {"question": "q", "sql": "SELECT 1", "result_shape": {"rows": 1, "columns": []}, "status": "candidate"},
        ])
        result = harvest_candidates("proj", tmp_path)
        assert result == []
        entries = _read_jsonl(tmp_path / "proj" / "examples" / "synthetic.jsonl")
        assert len(entries) == 1  # not duplicated
        # still marked harvested even though it was a duplicate — processed either way
        harvested_entries = _read_jsonl(tmp_path / "proj" / "examples" / "harvested.jsonl")
        assert harvested_entries[0]["status"] == "harvested"

    def test_missing_harvested_jsonl_returns_empty(self, tmp_path):
        (tmp_path / "proj").mkdir(parents=True)
        assert harvest_candidates("proj", tmp_path) == []

    def test_multiple_candidates_mixed_with_already_harvested(self, tmp_path):
        _write_jsonl(tmp_path / "proj" / "examples" / "harvested.jsonl", [
            {"question": "a", "sql": "SELECT 1", "result_shape": {"rows": 1, "columns": []}, "status": "harvested"},
            {"question": "b", "sql": "SELECT 2", "result_shape": {"rows": 1, "columns": []}, "status": "candidate"},
            {"question": "c", "sql": "SELECT 3", "result_shape": {"rows": 1, "columns": []}, "status": "candidate"},
        ])
        result = harvest_candidates("proj", tmp_path)
        assert {e.question for e in result} == {"b", "c"}


class TestListFailures:

    def test_returns_all_failures_in_order(self, tmp_path):
        _write_jsonl(tmp_path / "proj" / "examples" / "failures.jsonl", [
            {"question": "q1", "sql": "SELECT 1", "status": "failed"},
            {"question": "q2", "sql": "SELECT 2", "status": "failed"},
        ])
        result = list_failures("proj", tmp_path)
        assert [f.question for f in result] == ["q1", "q2"]

    def test_missing_file_returns_empty(self, tmp_path):
        (tmp_path / "proj").mkdir(parents=True)
        assert list_failures("proj", tmp_path) == []

    def test_does_not_modify_failures_jsonl(self, tmp_path):
        path = tmp_path / "proj" / "examples" / "failures.jsonl"
        _write_jsonl(path, [{"question": "q", "sql": "SELECT 1", "status": "failed"}])
        original = path.read_text()
        list_failures("proj", tmp_path)
        assert path.read_text() == original
