"""tests/test_metadata_generator.py — RAG roadmap Phase 1, build order step 2:
`medallion metadata generate` (openmedallion.metadata.generator).

LLM calls are replaced with mock callables (MagicMock side_effect), matching
the pattern in tests/test_cerebrum.py — no running LLM server required.
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest
import yaml

from openmedallion.metadata.generator import generate_metadata
from openmedallion.metadata.loader import load_metadata


def _draft_json(description: str, columns: dict[str, dict]) -> str:
    return json.dumps({"description": description, "synonyms": ["thing"], "columns": columns})


@pytest.fixture()
def project(tmp_path: Path) -> tuple[Path, str]:
    """A minimal real project: main.yaml + one silver table + one gold table."""
    proj_dir = tmp_path / "proj"
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
    bronze = {"source": {"type": "filesystem"}}
    silver_cfg = {"bronze_to_silver": {"tables": []}}
    gold_cfg = {"silver_to_gold": {"projects": []}}
    for name, data in [("main", main), ("bronze", bronze), ("silver", silver_cfg), ("gold", gold_cfg)]:
        with open(proj_dir / f"{name}.yaml", "w") as f:
            yaml.dump(data, f)

    return tmp_path, "proj"


class TestGenerateMetadata:

    def test_drafts_every_table_with_one_llm_call_each(self, project):
        projects_root, name = project
        mock_llm = MagicMock(side_effect=[
            _draft_json("Order line items.", {
                "order_id": {"description": "Unique order id.", "synonyms": []},
                "amount":   {"description": "Order amount in USD.", "synonyms": ["total"]},
                "region":   {"description": "Sales region.", "synonyms": []},
            }),
            _draft_json("Revenue by region.", {
                "region": {"description": "Sales region.", "synonyms": []},
                "total":  {"description": "Total revenue.", "synonyms": ["sum"]},
            }),
        ])
        result = generate_metadata(name, projects_root, _client=mock_llm)

        assert mock_llm.call_count == 2
        assert result.tables["orders"].layer == "silver"
        assert result.tables["orders"].status == "draft"
        assert result.tables["orders"].description == "Order line items."
        assert result.tables["orders"].columns["amount"].description == "Order amount in USD."
        assert result.tables["region_summary"].layer == "gold"
        assert result.tables["region_summary"].description == "Revenue by region."

    def test_value_examples_are_sampled_from_real_data_not_llm(self, project):
        projects_root, name = project
        mock_llm = MagicMock(side_effect=[
            _draft_json("Order line items.", {
                "order_id": {"description": "d", "synonyms": []},
                "amount":   {"description": "d", "synonyms": []},
                "region":   {"description": "d", "synonyms": []},
            }),
            _draft_json("Revenue by region.", {
                "region": {"description": "d", "synonyms": []},
                "total":  {"description": "d", "synonyms": []},
            }),
        ])
        result = generate_metadata(name, projects_root, _client=mock_llm)

        assert set(result.tables["orders"].columns["region"].value_examples) == {"North", "South", "East"}
        assert set(result.tables["orders"].columns["amount"].value_examples) == {10.0, 20.0, 30.0}

    def test_writes_metadata_yaml_to_disk(self, project):
        projects_root, name = project
        mock_llm = MagicMock(side_effect=[
            _draft_json("Order line items.", {
                "order_id": {"description": "d", "synonyms": []},
                "amount":   {"description": "d", "synonyms": []},
                "region":   {"description": "d", "synonyms": []},
            }),
            _draft_json("Revenue by region.", {
                "region": {"description": "d", "synonyms": []},
                "total":  {"description": "d", "synonyms": []},
            }),
        ])
        generate_metadata(name, projects_root, _client=mock_llm)

        reloaded = load_metadata(name, projects_root)
        assert reloaded.tables["orders"].status == "draft"
        assert (projects_root / name / "metadata.yaml").exists()

    def test_approved_table_is_skipped_and_preserved_unchanged(self, project):
        projects_root, name = project
        existing = {
            "tables": {
                "orders": {
                    "layer": "silver",
                    "description": "Human-reviewed description.",
                    "status": "approved",
                    "columns": {
                        "order_id": {"description": "reviewed"},
                        "amount":   {"description": "reviewed"},
                        "region":   {"description": "reviewed"},
                    },
                },
            },
        }
        with open(projects_root / name / "metadata.yaml", "w") as f:
            yaml.dump(existing, f)

        mock_llm = MagicMock(side_effect=[
            _draft_json("Revenue by region.", {
                "region": {"description": "d", "synonyms": []},
                "total":  {"description": "d", "synonyms": []},
            }),
        ])
        result = generate_metadata(name, projects_root, _client=mock_llm)

        assert mock_llm.call_count == 1  # only region_summary drafted, orders skipped
        assert result.tables["orders"].description == "Human-reviewed description."
        assert result.tables["orders"].status == "approved"
        assert result.tables["orders"].columns["order_id"].description == "reviewed"

    def test_draft_status_table_is_regenerated(self, project):
        projects_root, name = project
        existing = {
            "tables": {
                "orders": {"layer": "silver", "description": "Stale draft.", "status": "draft", "columns": {}},
            },
        }
        with open(projects_root / name / "metadata.yaml", "w") as f:
            yaml.dump(existing, f)

        mock_llm = MagicMock(side_effect=[
            _draft_json("Fresh description.", {
                "order_id": {"description": "d", "synonyms": []},
                "amount":   {"description": "d", "synonyms": []},
                "region":   {"description": "d", "synonyms": []},
            }),
            _draft_json("Revenue by region.", {
                "region": {"description": "d", "synonyms": []},
                "total":  {"description": "d", "synonyms": []},
            }),
        ])
        result = generate_metadata(name, projects_root, _client=mock_llm)

        assert mock_llm.call_count == 2
        assert result.tables["orders"].description == "Fresh description."

    def test_stale_status_table_is_regenerated(self, project):
        projects_root, name = project
        existing = {"tables": {"orders": {"layer": "silver", "status": "stale", "columns": {}}}}
        with open(projects_root / name / "metadata.yaml", "w") as f:
            yaml.dump(existing, f)

        mock_llm = MagicMock(side_effect=[
            _draft_json("Refreshed.", {
                "order_id": {"description": "d", "synonyms": []},
                "amount":   {"description": "d", "synonyms": []},
                "region":   {"description": "d", "synonyms": []},
            }),
            _draft_json("Revenue by region.", {
                "region": {"description": "d", "synonyms": []},
                "total":  {"description": "d", "synonyms": []},
            }),
        ])
        result = generate_metadata(name, projects_root, _client=mock_llm)

        assert result.tables["orders"].status == "draft"
        assert result.tables["orders"].description == "Refreshed."

    def test_existing_glossary_is_preserved_untouched(self, project):
        projects_root, name = project
        existing = {"tables": {}, "glossary": {"term": "definition"}}
        with open(projects_root / name / "metadata.yaml", "w") as f:
            yaml.dump(existing, f)

        mock_llm = MagicMock(side_effect=[
            _draft_json("d1", {"order_id": {"description": "d"}, "amount": {"description": "d"}, "region": {"description": "d"}}),
            _draft_json("d2", {"region": {"description": "d"}, "total": {"description": "d"}}),
        ])
        result = generate_metadata(name, projects_root, _client=mock_llm)

        assert result.glossary == {"term": "definition"}

    def test_markdown_fenced_json_response_is_parsed(self, project):
        projects_root, name = project
        fenced = "```json\n" + _draft_json("Order line items.", {
            "order_id": {"description": "d"}, "amount": {"description": "d"}, "region": {"description": "d"},
        }) + "\n```"
        mock_llm = MagicMock(side_effect=[
            fenced,
            _draft_json("Revenue by region.", {"region": {"description": "d"}, "total": {"description": "d"}}),
        ])
        result = generate_metadata(name, projects_root, _client=mock_llm)
        assert result.tables["orders"].description == "Order line items."

    def test_retries_once_on_invalid_json_then_succeeds(self, project):
        projects_root, name = project
        mock_llm = MagicMock(side_effect=[
            "not json at all",
            _draft_json("Order line items.", {
                "order_id": {"description": "d"}, "amount": {"description": "d"}, "region": {"description": "d"},
            }),
            _draft_json("Revenue by region.", {"region": {"description": "d"}, "total": {"description": "d"}}),
        ])
        result = generate_metadata(name, projects_root, _client=mock_llm)
        assert mock_llm.call_count == 3
        assert result.tables["orders"].description == "Order line items."

    def test_raises_after_exhausting_retries_on_invalid_json(self, project):
        projects_root, name = project
        mock_llm = MagicMock(return_value="still not json")
        with pytest.raises(ValueError, match="valid JSON"):
            generate_metadata(name, projects_root, _client=mock_llm)

    def test_on_step_callback_invoked(self, project):
        projects_root, name = project
        mock_llm = MagicMock(side_effect=[
            _draft_json("d1", {"order_id": {"description": "d"}, "amount": {"description": "d"}, "region": {"description": "d"}}),
            _draft_json("d2", {"region": {"description": "d"}, "total": {"description": "d"}}),
        ])
        steps = []
        generate_metadata(name, projects_root, _client=mock_llm, on_step=steps.append)
        assert len(steps) >= 2
