"""tests/test_relationships.py — RAG roadmap Phase 1, build order step 4:
relationships.yaml schema (openmedallion.relationships).

Locked design decisions (see CLAUDE.md "Roadmap: RAG Accuracy Improvement"):
- Single relationships.yaml + status: draft/approved/stale field per entry —
  mirrors metadata.yaml's pattern, no separate relationships_recommended.yaml.
- No relationships_enhancements.yaml overlay — detection is deterministic
  pattern matching, not LLM-drafted, so there's no regeneration-clobber risk
  to guard against. Hand-edit relationships.yaml directly.
- join_on: list[str] always, even for a single-column join.
- method: fk_naming | lineage | grain records which detection rule found it;
  optional, since a human can hand-add a relationship with no detection rule.
"""
from pathlib import Path

import pytest
import yaml

from openmedallion.relationships.schema import RelationshipsConfig
from openmedallion.relationships.loader import load_relationships


def _valid_relationships() -> dict:
    return {
        "relationships": [
            {
                "from_table": "employees_enriched",
                "to_table": "departments",
                "join_on": ["department_id"],
                "confidence": "high",
                "status": "draft",
                "method": "fk_naming",
            },
        ],
    }


class TestRelationshipsSchema:

    def test_valid_relationships_builds_model(self):
        model = RelationshipsConfig(**_valid_relationships())
        rel = model.relationships[0]
        assert rel.from_table == "employees_enriched"
        assert rel.to_table == "departments"
        assert rel.join_on == ["department_id"]
        assert rel.confidence == "high"
        assert rel.status == "draft"
        assert rel.method == "fk_naming"

    def test_empty_config_is_valid(self):
        model = RelationshipsConfig()
        assert model.relationships == []

    def test_status_defaults_to_draft(self):
        cfg = _valid_relationships()
        del cfg["relationships"][0]["status"]
        model = RelationshipsConfig(**cfg)
        assert model.relationships[0].status == "draft"

    def test_confidence_defaults_to_high(self):
        cfg = _valid_relationships()
        del cfg["relationships"][0]["confidence"]
        model = RelationshipsConfig(**cfg)
        assert model.relationships[0].confidence == "high"

    def test_method_is_optional(self):
        cfg = _valid_relationships()
        del cfg["relationships"][0]["method"]
        model = RelationshipsConfig(**cfg)
        assert model.relationships[0].method is None

    def test_composite_join_on(self):
        cfg = _valid_relationships()
        cfg["relationships"][0]["join_on"] = ["folderrsn", "processrsn"]
        model = RelationshipsConfig(**cfg)
        assert model.relationships[0].join_on == ["folderrsn", "processrsn"]

    def test_join_on_empty_list_raises(self):
        cfg = _valid_relationships()
        cfg["relationships"][0]["join_on"] = []
        with pytest.raises(ValueError, match="join_on"):
            RelationshipsConfig(**cfg)

    def test_invalid_status_raises(self):
        cfg = _valid_relationships()
        cfg["relationships"][0]["status"] = "reviewed"
        with pytest.raises(ValueError, match="status"):
            RelationshipsConfig(**cfg)

    def test_invalid_confidence_raises(self):
        cfg = _valid_relationships()
        cfg["relationships"][0]["confidence"] = "certain"
        with pytest.raises(ValueError, match="confidence"):
            RelationshipsConfig(**cfg)

    def test_invalid_method_raises(self):
        cfg = _valid_relationships()
        cfg["relationships"][0]["method"] = "guess"
        with pytest.raises(ValueError, match="method"):
            RelationshipsConfig(**cfg)

    def test_missing_from_table_raises(self):
        cfg = _valid_relationships()
        del cfg["relationships"][0]["from_table"]
        with pytest.raises(ValueError, match="from_table"):
            RelationshipsConfig(**cfg)

    def test_missing_to_table_raises(self):
        cfg = _valid_relationships()
        del cfg["relationships"][0]["to_table"]
        with pytest.raises(ValueError, match="to_table"):
            RelationshipsConfig(**cfg)

    def test_missing_join_on_raises(self):
        cfg = _valid_relationships()
        del cfg["relationships"][0]["join_on"]
        with pytest.raises(ValueError, match="join_on"):
            RelationshipsConfig(**cfg)

    def test_unknown_relationship_key_raises(self):
        cfg = _valid_relationships()
        cfg["relationships"][0]["confidance"] = "high"
        with pytest.raises(ValueError, match="confidance"):
            RelationshipsConfig(**cfg)

    def test_unknown_top_level_key_raises(self):
        cfg = {"relationshps": []}
        with pytest.raises(ValueError, match="relationshps"):
            RelationshipsConfig(**cfg)

    def test_stale_status_accepted(self):
        cfg = _valid_relationships()
        cfg["relationships"][0]["status"] = "stale"
        model = RelationshipsConfig(**cfg)
        assert model.relationships[0].status == "stale"

    def test_multiple_relationships_between_same_tables_allowed(self):
        cfg = _valid_relationships()
        cfg["relationships"].append({
            "from_table": "employees_enriched",
            "to_table": "departments",
            "join_on": ["manager_id"],
            "method": "grain",
        })
        model = RelationshipsConfig(**cfg)
        assert len(model.relationships) == 2


class TestLoadRelationships:

    def _write(self, root: Path, project: str, data: dict) -> Path:
        project_dir = root / project
        project_dir.mkdir(parents=True, exist_ok=True)
        with open(project_dir / "relationships.yaml", "w") as f:
            yaml.dump(data, f)
        return project_dir

    def test_loads_relationships_yaml(self, tmp_path):
        self._write(tmp_path, "hr", _valid_relationships())
        model = load_relationships("hr", tmp_path)
        assert model.relationships[0].to_table == "departments"

    def test_missing_file_returns_empty_config(self, tmp_path):
        (tmp_path / "hr").mkdir(parents=True)
        model = load_relationships("hr", tmp_path)
        assert model.relationships == []

    def test_invalid_file_raises_value_error(self, tmp_path):
        self._write(tmp_path, "hr", {"relationships": [{"from_table": "a"}]})
        with pytest.raises(ValueError, match="\\[relationships\\]"):
            load_relationships("hr", tmp_path)
