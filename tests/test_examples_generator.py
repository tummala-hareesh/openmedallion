"""tests/test_examples_generator.py — RAG roadmap Phase 1, build order step 7:
`medallion examples generate` (openmedallion.examples.generator).

Locked design decisions (see CLAUDE.md "Roadmap: RAG Accuracy Improvement"):
- Silver-layer approved tables only for v1 — cerebrum's runtime doesn't
  register gold Parquet as DuckDB views yet.
- One LLM call for the whole batch (JSON array), retried on invalid JSON —
  same pattern as metadata/generator.py.
- An example whose SQL never validates (even after cerebrum/validator.py's
  retry) is dropped, not written with an error flag.
- On regeneration, verified: true entries are kept untouched; verified:
  false entries are replaced by a fresh batch.
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest
import yaml

from openmedallion.examples.generator import generate_examples


def _batch_json(pairs: list[dict]) -> str:
    return json.dumps(pairs)


@pytest.fixture()
def project(tmp_path: Path) -> tuple[Path, str]:
    """A minimal real project: one approved silver table, one draft silver
    table, one approved gold table (to prove gold is excluded), and an
    approved relationship."""
    proj_dir   = tmp_path / "proj"
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
        "customer_id": [1, 2],
        "name":        ["Alice", "Bob"],
    }).write_parquet(silver_dir / "customers.parquet")

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
    for name, data in [
        ("main", main),
        ("bronze", {"source": {"type": "filesystem"}}),
        ("silver", {"bronze_to_silver": {"tables": []}}),
        ("gold", {"silver_to_gold": {"projects": []}}),
    ]:
        with open(proj_dir / f"{name}.yaml", "w") as f:
            yaml.dump(data, f)

    metadata = {
        "tables": {
            "orders": {
                "layer": "silver", "status": "approved",
                "description": "Order line items.",
                "columns": {
                    "order_id": {"description": "id"},
                    "amount":   {"description": "amount"},
                    "region":   {"description": "region"},
                },
            },
            "customers": {
                "layer": "silver", "status": "draft",  # not approved — must be excluded
                "description": "Customer records.",
                "columns": {"customer_id": {"description": "id"}, "name": {"description": "name"}},
            },
            "region_summary": {
                "layer": "gold", "status": "approved",  # gold — must be excluded (locked decision)
                "description": "Revenue by region.",
                "columns": {"region": {"description": "region"}, "total": {"description": "total"}},
            },
        },
    }
    with open(proj_dir / "metadata.yaml", "w") as f:
        yaml.dump(metadata, f)

    relationships = {
        "relationships": [
            {
                "from_table": "orders", "to_table": "customers",
                "join_on": ["order_id"], "status": "approved", "method": "fk_naming",
            },
        ],
    }
    with open(proj_dir / "relationships.yaml", "w") as f:
        yaml.dump(relationships, f)

    return tmp_path, "proj"


def _synthetic_path(projects_root: Path, name: str) -> Path:
    return projects_root / name / "examples" / "synthetic.jsonl"


class TestGenerateExamples:

    def test_generates_and_validates_examples(self, project):
        projects_root, name = project
        mock_llm = MagicMock(return_value=_batch_json([
            {"question": "How many orders are there?", "sql": "SELECT COUNT(*) AS n FROM orders"},
            {"question": "Total revenue by region?", "sql": "SELECT region, SUM(amount) AS total FROM orders GROUP BY region"},
        ]))
        result = generate_examples(name, projects_root, count=2, _client=mock_llm)

        assert len(result) == 2
        assert all(e.verified is False for e in result)
        assert {e.question for e in result} == {"How many orders are there?", "Total revenue by region?"}

    def test_writes_synthetic_jsonl(self, project):
        projects_root, name = project
        mock_llm = MagicMock(return_value=_batch_json([
            {"question": "How many orders?", "sql": "SELECT COUNT(*) AS n FROM orders"},
        ]))
        generate_examples(name, projects_root, count=1, _client=mock_llm)

        path = _synthetic_path(projects_root, name)
        assert path.exists()
        lines = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
        assert lines[0]["question"] == "How many orders?"
        assert lines[0]["verified"] is False

    def test_prompt_excludes_draft_and_gold_tables(self, project):
        projects_root, name = project
        mock_llm = MagicMock(return_value=_batch_json([
            {"question": "q", "sql": "SELECT COUNT(*) AS n FROM orders"},
        ]))
        generate_examples(name, projects_root, count=1, _client=mock_llm)

        prompt = mock_llm.call_args_list[0].args[0]
        assert "orders" in prompt
        assert "customers" not in prompt        # draft — excluded
        assert "region_summary" not in prompt    # gold — excluded

    def test_invalid_sql_dropped_after_exhausting_retries(self, project):
        projects_root, name = project
        mock_llm = MagicMock(side_effect=[
            _batch_json([{"question": "bad one", "sql": "SELECT * FROM nonexistent_table"}]),
            "SELECT * FROM nonexistent_table",  # retry 1 — still bad
            "SELECT * FROM nonexistent_table",  # retry 2 — still bad
        ])
        result = generate_examples(name, projects_root, count=1, _client=mock_llm)
        assert result == []

    def test_invalid_sql_fixed_by_retry_is_kept(self, project):
        projects_root, name = project
        mock_llm = MagicMock(side_effect=[
            _batch_json([{"question": "fixable", "sql": "SELEC COUNT(*) FROM orders"}]),  # typo'd keyword
            "SELECT COUNT(*) AS n FROM orders",  # retry — fixed
        ])
        result = generate_examples(name, projects_root, count=1, _client=mock_llm)
        assert len(result) == 1
        assert result[0].sql == "SELECT COUNT(*) AS n FROM orders"

    def test_no_approved_silver_tables_raises(self, project):
        projects_root, name = project
        meta_path = projects_root / name / "metadata.yaml"
        with open(meta_path) as f:
            meta = yaml.safe_load(f)
        for t in meta["tables"].values():
            t["status"] = "draft"
        with open(meta_path, "w") as f:
            yaml.dump(meta, f)

        with pytest.raises(ValueError, match="approved"):
            generate_examples(name, projects_root, count=1, _client=MagicMock())

    def test_regeneration_keeps_verified_replaces_unverified(self, project):
        projects_root, name = project
        path = _synthetic_path(projects_root, name)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            f.write(json.dumps({"question": "old verified", "sql": "SELECT 1", "verified": True}) + "\n")
            f.write(json.dumps({"question": "old unverified", "sql": "SELECT 2", "verified": False}) + "\n")

        mock_llm = MagicMock(return_value=_batch_json([
            {"question": "new one", "sql": "SELECT COUNT(*) AS n FROM orders"},
        ]))
        result = generate_examples(name, projects_root, count=1, _client=mock_llm)

        questions = {e.question for e in result}
        assert "old verified" in questions
        assert "old unverified" not in questions
        assert "new one" in questions

    def test_on_step_callback_invoked(self, project):
        projects_root, name = project
        mock_llm = MagicMock(return_value=_batch_json([
            {"question": "q", "sql": "SELECT COUNT(*) AS n FROM orders"},
        ]))
        steps: list[str] = []
        generate_examples(name, projects_root, count=1, _client=mock_llm, on_step=steps.append)
        assert len(steps) >= 2

    def test_retries_once_on_invalid_batch_json(self, project):
        projects_root, name = project
        mock_llm = MagicMock(side_effect=[
            "not a json array at all",
            _batch_json([{"question": "q", "sql": "SELECT COUNT(*) AS n FROM orders"}]),
        ])
        result = generate_examples(name, projects_root, count=1, _client=mock_llm)
        assert len(result) == 1

    def test_raises_after_exhausting_batch_json_retries(self, project):
        projects_root, name = project
        mock_llm = MagicMock(return_value="still not json")
        with pytest.raises(ValueError, match="valid JSON"):
            generate_examples(name, projects_root, count=1, _client=mock_llm)
