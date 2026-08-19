"""tests/test_cli_query_chat_history.py — `medallion query` writes per-person
chat history (openmedallion.neuron.chat_history), same as neuron's /query.

CerebrumPipeline is mocked (matches tests/test_neuron.py's pattern) — no
Ollama or real silver Parquet needed. `getpass.getuser()` is not exercised
here — `--user` is always passed explicitly.
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest
import yaml

from openmedallion.cerebrum.pipeline import QueryResult
from openmedallion.cli.main import cmd_query


@pytest.fixture()
def project(tmp_path: Path) -> tuple[Path, str]:
    proj_dir = tmp_path / "proj"
    silver_dir = proj_dir / "data" / "silver"
    silver_dir.mkdir(parents=True)
    pl.DataFrame({"order_id": [1, 2]}).write_parquet(silver_dir / "orders.parquet")

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
    for name, data in [
        ("main", main),
        ("bronze", {"source": {"type": "filesystem"}}),
        ("silver", {"bronze_to_silver": {"tables": []}}),
        ("gold", {"silver_to_gold": {"projects": []}}),
    ]:
        with open(proj_dir / f"{name}.yaml", "w") as f:
            yaml.dump(data, f)

    return tmp_path, "proj"


def _args(project, name, question="How many orders?", user="alice"):
    import argparse
    projects_root, _ = project
    return argparse.Namespace(
        project=name, question=question, projects=str(projects_root),
        model=None, provider=None, nlg_model=None, nlg_provider=None,
        detect_ambiguity=False, decompose=False, use_templates=False,
        user=user,
    )


class TestCmdQueryChatHistory:

    def test_successful_query_writes_a_turn_for_the_named_user(self, project):
        projects_root, name = project
        qr = QueryResult(
            question="How many orders?", sql="SELECT COUNT(*) AS n FROM orders",
            result=pl.DataFrame({"n": [2]}), recommended_prompt="Count orders",
        )
        with patch("openmedallion.cerebrum.pipeline.CerebrumPipeline") as mock_cls:
            mock_cls.return_value.ask.return_value = qr
            cmd_query(_args(project, name))

        path = projects_root / name / "chat_history" / "alice.jsonl"
        assert path.exists()
        entry = json.loads(path.read_text().splitlines()[0])
        assert entry["question"] == "How many orders?"
        assert entry["row_count"] == 1

    def test_no_user_flag_falls_back_to_os_username(self, project, monkeypatch):
        projects_root, name = project
        monkeypatch.setattr("getpass.getuser", lambda: "os_user")
        qr = QueryResult(
            question="q", sql="SELECT 1", result=pl.DataFrame({"n": [1]}),
            recommended_prompt="r",
        )
        with patch("openmedallion.cerebrum.pipeline.CerebrumPipeline") as mock_cls:
            mock_cls.return_value.ask.return_value = qr
            cmd_query(_args(project, name, user=None))

        assert (projects_root / name / "chat_history" / "os_user.jsonl").exists()

    def test_successful_query_records_a_session_id(self, project):
        projects_root, name = project
        qr = QueryResult(
            question="q", sql="SELECT 1", result=pl.DataFrame({"n": [1]}),
            recommended_prompt="r",
        )
        with patch("openmedallion.cerebrum.pipeline.CerebrumPipeline") as mock_cls:
            mock_cls.return_value.ask.return_value = qr
            cmd_query(_args(project, name))

        entry = json.loads(
            (projects_root / name / "chat_history" / "alice.jsonl").read_text().splitlines()[0]
        )
        assert entry["session_id"]  # non-empty — one uuid per CLI invocation

    def test_ambiguous_question_writes_a_failed_turn(self, project):
        from openmedallion.cerebrum.pipeline import AmbiguousQuestionError

        projects_root, name = project
        with patch("openmedallion.cerebrum.pipeline.CerebrumPipeline") as mock_cls:
            mock_cls.return_value.ask.side_effect = AmbiguousQuestionError("q", "which one?")
            with pytest.raises(SystemExit):
                cmd_query(_args(project, name))

        path = projects_root / name / "chat_history" / "alice.jsonl"
        assert path.exists()
        entry = json.loads(path.read_text().splitlines()[0])
        assert entry["response_generated"] is False

    def test_unexpected_error_writes_a_failed_turn(self, project):
        projects_root, name = project
        with patch("openmedallion.cerebrum.pipeline.CerebrumPipeline") as mock_cls:
            mock_cls.return_value.ask.side_effect = RuntimeError("boom")
            with pytest.raises(SystemExit):
                cmd_query(_args(project, name))

        path = projects_root / name / "chat_history" / "alice.jsonl"
        assert path.exists()
        entry = json.loads(path.read_text().splitlines()[0])
        assert entry["response_generated"] is False


class TestCmdQueryUseTemplatesFlag:
    """Template-Routed Query Layer roadmap (see CLAUDE.md), build order step 6:
    --use-templates on `medallion query` passes straight through to
    CerebrumPipeline's opt-in constructor flag."""

    def test_use_templates_defaults_false(self, project):
        projects_root, name = project
        qr = QueryResult(
            question="q", sql="SELECT 1", result=pl.DataFrame({"n": [1]}),
            recommended_prompt="r",
        )
        with patch("openmedallion.cerebrum.pipeline.CerebrumPipeline") as mock_cls:
            mock_cls.return_value.ask.return_value = qr
            cmd_query(_args(project, name))
        assert mock_cls.call_args.kwargs["use_templates"] is False

    def test_use_templates_flag_forwarded_true(self, project):
        projects_root, name = project
        qr = QueryResult(
            question="q", sql="SELECT 1", result=pl.DataFrame({"n": [1]}),
            recommended_prompt="r",
        )
        args = _args(project, name)
        args.use_templates = True
        with patch("openmedallion.cerebrum.pipeline.CerebrumPipeline") as mock_cls:
            mock_cls.return_value.ask.return_value = qr
            cmd_query(args)
        assert mock_cls.call_args.kwargs["use_templates"] is True
