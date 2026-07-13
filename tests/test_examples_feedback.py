"""tests/test_examples_feedback.py — RAG roadmap Phase 3, build order step 13:
cortex thumbs up/down feedback (openmedallion.examples.feedback).

Locked design decision: cortex never writes these files directly (it never
imports cerebrum/pipeline internals, only talks to neuron over HTTP) — this
module is called from a new neuron POST /feedback endpoint, not from cortex.

Per the original Phase 3 plan: thumbs-up writes (question, sql, result_shape)
to harvested.jsonl as status: candidate; thumbs-down logs to failures.jsonl.
Both are simple append-only logs (unlike synthetic.jsonl's generate/approve
in-place-rewrite pattern) — harvesting/reviewing them into synthetic.jsonl
is separate, not-yet-built future work (medallion examples harvest/review).
"""
from __future__ import annotations

import json
from pathlib import Path

from openmedallion.examples.feedback import record_feedback


def _read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


class TestRecordFeedback:

    def test_thumbs_up_appends_to_harvested_jsonl(self, tmp_path):
        record_feedback(
            "proj", tmp_path,
            question="How many orders?", sql="SELECT COUNT(*) AS n FROM orders",
            columns=["n"], row_count=1, thumbs_up=True,
        )
        entries = _read_jsonl(tmp_path / "proj" / "examples" / "harvested.jsonl")
        assert len(entries) == 1
        assert entries[0]["question"] == "How many orders?"
        assert entries[0]["sql"] == "SELECT COUNT(*) AS n FROM orders"
        assert entries[0]["result_shape"] == {"rows": 1, "columns": ["n"]}
        assert entries[0]["status"] == "candidate"

    def test_thumbs_down_appends_to_failures_jsonl(self, tmp_path):
        record_feedback(
            "proj", tmp_path,
            question="How many orders?", sql="SELECT COUNT(*) AS n FROM orders",
            columns=["n"], row_count=1, thumbs_up=False,
        )
        entries = _read_jsonl(tmp_path / "proj" / "examples" / "failures.jsonl")
        assert len(entries) == 1
        assert entries[0]["question"] == "How many orders?"
        assert entries[0]["status"] == "failed"

    def test_thumbs_up_does_not_touch_failures_jsonl(self, tmp_path):
        record_feedback(
            "proj", tmp_path,
            question="q", sql="SELECT 1", columns=[], row_count=1, thumbs_up=True,
        )
        assert not (tmp_path / "proj" / "examples" / "failures.jsonl").exists()

    def test_thumbs_down_does_not_touch_harvested_jsonl(self, tmp_path):
        record_feedback(
            "proj", tmp_path,
            question="q", sql="SELECT 1", columns=[], row_count=1, thumbs_up=False,
        )
        assert not (tmp_path / "proj" / "examples" / "harvested.jsonl").exists()

    def test_appends_without_overwriting_existing_entries(self, tmp_path):
        record_feedback("proj", tmp_path, question="q1", sql="SELECT 1", columns=[], row_count=1, thumbs_up=True)
        record_feedback("proj", tmp_path, question="q2", sql="SELECT 2", columns=[], row_count=1, thumbs_up=True)
        entries = _read_jsonl(tmp_path / "proj" / "examples" / "harvested.jsonl")
        assert [e["question"] for e in entries] == ["q1", "q2"]

    def test_creates_examples_directory_if_missing(self, tmp_path):
        assert not (tmp_path / "proj" / "examples").exists()
        record_feedback("proj", tmp_path, question="q", sql="SELECT 1", columns=[], row_count=0, thumbs_up=True)
        assert (tmp_path / "proj" / "examples").exists()

    def test_default_columns_and_row_count(self, tmp_path):
        record_feedback("proj", tmp_path, question="q", sql="SELECT 1", thumbs_up=True)
        entries = _read_jsonl(tmp_path / "proj" / "examples" / "harvested.jsonl")
        assert entries[0]["result_shape"] == {"rows": 0, "columns": []}
