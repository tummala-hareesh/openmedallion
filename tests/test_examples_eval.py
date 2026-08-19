"""tests/test_examples_eval.py — `medallion examples eval`.

Maps onto the roadmap's flagged-but-unbuilt "RAG eval set" idea, reframed as
an admin-triggered regression check (per user request): for every
`verified: true` example in synthetic.jsonl, re-run the question through the
current pipeline and compare the freshly-generated SQL's *result* against
the stored golden SQL's result — executed via DuckDB, not compared as SQL
text (semantically-identical SQL can differ syntactically).

No real model retraining exists in this project — "eval" checks whether a
curation change (new metadata, new examples, new relationships) improved or
regressed answers, not whether a model's weights changed.

LLM calls are mocked (matches tests/test_metadata_generator.py's pattern) —
no Ollama needed.
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest

from openmedallion.examples.eval import run_eval


@pytest.fixture()
def project(tmp_path: Path) -> Path:
    """A minimal silver dir + synthetic.jsonl with two verified examples."""
    silver_dir = tmp_path / "silver"
    silver_dir.mkdir()
    pl.DataFrame({
        "order_id": [1, 2, 3],
        "amount":   [10.0, 20.0, 30.0],
        "region":   ["North", "South", "East"],
    }).write_parquet(silver_dir / "orders.parquet")

    examples_dir = tmp_path / "examples"
    examples_dir.mkdir()
    lines = [
        json.dumps({"question": "How many orders?", "sql": "SELECT COUNT(*) AS n FROM orders", "verified": True}),
        json.dumps({"question": "Total amount?", "sql": "SELECT SUM(amount) AS total FROM orders", "verified": True}),
        json.dumps({"question": "Draft, not verified", "sql": "SELECT 1", "verified": False}),
    ]
    (examples_dir / "synthetic.jsonl").write_text("\n".join(lines) + "\n")

    return tmp_path


class TestRunEval:

    def test_only_verified_examples_are_evaluated(self, project):
        mock_client = MagicMock(side_effect=[
            "SELECT COUNT(*) AS n FROM orders",
            "SELECT SUM(amount) AS total FROM orders",
        ])
        results = run_eval(project / "silver", project / "examples", _client=mock_client)
        assert len(results) == 2  # not 3 — the unverified draft is skipped
        assert mock_client.call_count == 2

    def test_matching_sql_produces_a_match(self, project):
        mock_client = MagicMock(side_effect=[
            "SELECT COUNT(*) AS n FROM orders",       # same as golden
            "SELECT SUM(amount) AS total FROM orders", # same as golden
        ])
        results = run_eval(project / "silver", project / "examples", _client=mock_client)
        assert all(r.match for r in results)

    def test_semantically_equivalent_but_syntactically_different_sql_still_matches(self, project):
        """Compares executed results, not raw SQL text."""
        mock_client = MagicMock(side_effect=[
            "SELECT count(*)   AS n FROM   orders",  # different whitespace/case, same result
            "SELECT SUM(amount) AS total FROM orders",
        ])
        results = run_eval(project / "silver", project / "examples", _client=mock_client)
        assert results[0].match is True

    def test_different_result_produces_a_mismatch(self, project):
        mock_client = MagicMock(side_effect=[
            "SELECT COUNT(*) AS n FROM orders WHERE region = 'North'",  # different result: 1, not 3
            "SELECT SUM(amount) AS total FROM orders",
        ])
        results = run_eval(project / "silver", project / "examples", _client=mock_client)
        assert results[0].match is False
        assert results[1].match is True

    def test_result_carries_question_and_both_sql_strings(self, project):
        mock_client = MagicMock(side_effect=[
            "SELECT COUNT(*) AS n FROM orders",
            "SELECT SUM(amount) AS total FROM orders",
        ])
        results = run_eval(project / "silver", project / "examples", _client=mock_client)
        assert results[0].question == "How many orders?"
        assert results[0].golden_sql == "SELECT COUNT(*) AS n FROM orders"
        assert results[0].new_sql == "SELECT COUNT(*) AS n FROM orders"

    def test_invalid_new_sql_is_reported_as_a_mismatch_not_raised(self, project):
        mock_client = MagicMock(side_effect=[
            "SELECT this is not valid SQL at all",
            "SELECT SUM(amount) AS total FROM orders",
        ])
        results = run_eval(project / "silver", project / "examples", _client=mock_client)
        assert results[0].match is False
        assert results[0].error  # non-empty error message

    def test_no_synthetic_file_returns_empty_list(self, tmp_path):
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir()
        results = run_eval(silver_dir, tmp_path / "examples", _client=MagicMock())
        assert results == []

    def test_on_step_callback_invoked(self, project):
        mock_client = MagicMock(side_effect=[
            "SELECT COUNT(*) AS n FROM orders",
            "SELECT SUM(amount) AS total FROM orders",
        ])
        steps: list[str] = []
        run_eval(project / "silver", project / "examples", _client=mock_client, on_step=steps.append)
        assert len(steps) >= 2

    def test_use_templates_defaults_false_leaves_new_fields_empty(self, project):
        # Zero-cost-by-default: use_templates omitted -> no pipeline is ever
        # constructed, existing fields/behavior are completely unchanged.
        mock_client = MagicMock(side_effect=[
            "SELECT COUNT(*) AS n FROM orders",
            "SELECT SUM(amount) AS total FROM orders",
        ])
        results = run_eval(project / "silver", project / "examples", _client=mock_client)
        assert all(r.templated_sql == "" for r in results)
        assert all(r.templated_match is None for r in results)
        assert mock_client.call_count == 2  # no extra calls from a template-routed re-run


@pytest.fixture()
def templated_project(tmp_path: Path) -> Path:
    """A single verified+templated example, no params -- kept minimal so the
    mocked LLM's call sequence through CerebrumPipeline stays predictable."""
    silver_dir = tmp_path / "silver"
    silver_dir.mkdir()
    pl.DataFrame({
        "order_id": [1, 2, 3],
        "amount":   [10.0, 20.0, 30.0],
        "region":   ["North", "South", "East"],
    }).write_parquet(silver_dir / "orders.parquet")

    examples_dir = tmp_path / "examples"
    examples_dir.mkdir()
    (examples_dir / "synthetic.jsonl").write_text(
        json.dumps({
            "question": "How many orders?",
            "sql": "SELECT COUNT(*) AS n FROM orders",
            "verified": True, "templated": True, "params": {},
        }) + "\n"
    )
    return tmp_path


class TestRunEvalUseTemplates:
    """Template-Routed Query Layer roadmap (see CLAUDE.md), build order step 7:
    run_eval(use_templates=True) additionally routes each verified question
    through CerebrumPipeline(use_templates=True), reporting whether
    template-routing changed the outcome vs. the stored golden SQL."""

    def _fake_embed_fn(self, vectors: dict[str, list[float]]):
        def _embed(texts: list[str]) -> list[list[float]]:
            return [vectors[t] for t in texts]
        return _embed

    def test_matching_template_reports_a_match(self, templated_project):
        embed_fn = self._fake_embed_fn({"How many orders?": [1.0, 0.0]})
        # sequence: normal-path generation -> template fill -> recommend
        mock_client = MagicMock(side_effect=[
            "SELECT COUNT(*) AS n FROM orders",
            "{}",
            "recommended prompt text",
        ])
        results = run_eval(
            templated_project / "silver", templated_project / "examples",
            _client=mock_client, use_templates=True, _embed_fn=embed_fn,
        )
        assert results[0].templated_sql == "SELECT COUNT(*) AS n FROM orders"
        assert results[0].templated_match is True
        assert results[0].templated_error == ""

    def test_false_positive_template_match_reports_a_mismatch(self, tmp_path):
        # A single templated example can never diverge from its own golden
        # SQL (they're the same stored entry) -- a real "template-routing
        # changed the result" mismatch instead comes from a *false-positive*
        # match: a second, unrelated question the embedding wrongly treats
        # as similar enough to route through a template whose logic answers
        # something different.
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir()
        pl.DataFrame({
            "order_id": [1, 2, 3],
            "region":   ["North", "South", "East"],
        }).write_parquet(silver_dir / "orders.parquet")
        examples_dir = tmp_path / "examples"
        examples_dir.mkdir()
        lines = [
            json.dumps({
                "question": "How many orders?",
                "sql": "SELECT COUNT(*) AS n FROM orders",
                "verified": True,
            }),
            json.dumps({
                "question": "How many orders exactly?",
                "sql": "SELECT COUNT(*) AS n FROM orders WHERE region = 'North'",
                "verified": True, "templated": True, "params": {},
            }),
        ]
        (examples_dir / "synthetic.jsonl").write_text("\n".join(lines) + "\n")

        embed_fn = self._fake_embed_fn({
            "How many orders?": [1.0, 0.0],
            "How many orders exactly?": [1.0, 0.0],  # same vector -> false-positive match
        })
        mock_client = MagicMock(side_effect=[
            "SELECT COUNT(*) AS n FROM orders",                          # ex1 normal path
            "{}",                                                        # ex1 template fill
            "recommended 1",                                             # ex1 template recommend
            "SELECT COUNT(*) AS n FROM orders WHERE region = 'North'",   # ex2 normal path
            "{}",                                                        # ex2 template fill
            "recommended 2",                                             # ex2 template recommend
        ])
        results = run_eval(
            silver_dir, examples_dir,
            _client=mock_client, use_templates=True, _embed_fn=embed_fn,
        )
        first = next(r for r in results if r.question == "How many orders?")
        assert first.templated_sql == "SELECT COUNT(*) AS n FROM orders WHERE region = 'North'"
        assert first.templated_match is False  # 1 row, not the golden 3

    def test_pipeline_exception_is_reported_not_raised(self, templated_project):
        embed_fn = self._fake_embed_fn({"How many orders?": [1.0, 0.0]})
        # fill call returns malformed JSON -> falls back to normal generation
        # inside the pipeline, which then also returns invalid SQL -> the
        # pipeline's own validator exhausts retries and raises ValueError.
        mock_client = MagicMock(side_effect=[
            "SELECT COUNT(*) AS n FROM orders",  # normal path (outside pipeline)
            "not json",                          # fill attempt fails to parse
            "DROP TABLE orders",                 # fallback SQL-gen: rejected by allowlist
            "DROP TABLE orders",                 # retry 1
            "DROP TABLE orders",                 # retry 2 -> exhausted, raises
        ])
        results = run_eval(
            templated_project / "silver", templated_project / "examples",
            _client=mock_client, use_templates=True, _embed_fn=embed_fn,
        )
        assert results[0].templated_match is False
        assert results[0].templated_error  # non-empty
