"""tests/test_metadata.py — RAG roadmap Phase 1: metadata.yaml schema + loader.

Locked design decisions (see CLAUDE.md "Roadmap: RAG Accuracy Improvement"):
- status: draft/approved/stale is table-level only, no per-column status.
- metadata_enhancements.yaml deep-merges over metadata.yaml at load time
  (enhancements win), reusing config/loader.py's _deep_merge semantics.
- glossary: is a top-level dict[str, str] of business terms.
- Schema is a typed Pydantic model (MetadataConfig), extra="forbid", matching
  the config/schema.py pattern from T-TODO-4.
"""
import pytest
import yaml
from pathlib import Path

from openmedallion.metadata.schema import MetadataConfig
from openmedallion.metadata.loader import load_metadata, _validate_metadata


def _write(root: Path, project: str, metadata: dict | None = None, enhancements: dict | None = None) -> Path:
    project_dir = root / project
    project_dir.mkdir(parents=True, exist_ok=True)
    if metadata is not None:
        with open(project_dir / "metadata.yaml", "w") as f:
            yaml.dump(metadata, f)
    if enhancements is not None:
        with open(project_dir / "metadata_enhancements.yaml", "w") as f:
            yaml.dump(enhancements, f)
    return project_dir


def _valid_metadata() -> dict:
    return {
        "tables": {
            "employees_enriched": {
                "layer": "silver",
                "description": "One row per employee with department and job title joined in.",
                "status": "draft",
                "synonyms": ["staff", "personnel"],
                "columns": {
                    "employee_id": {
                        "description": "Unique identifier for an employee.",
                        "value_examples": [100, 101, 102],
                        "synonyms": ["emp_id"],
                    },
                    "salary": {
                        "description": "Employee's annual base salary in USD.",
                        "value_examples": [50000, 62000, 95000],
                    },
                },
            },
        },
        "glossary": {
            "active_employee": "an employee currently assigned to one of the target departments",
        },
    }


class TestMetadataSchema:

    def test_valid_metadata_builds_model(self):
        model = MetadataConfig(**_valid_metadata())
        assert model.tables["employees_enriched"].layer == "silver"
        assert model.tables["employees_enriched"].status == "draft"
        assert model.tables["employees_enriched"].columns["salary"].value_examples == [50000, 62000, 95000]
        assert model.glossary["active_employee"].startswith("an employee")

    def test_empty_config_is_valid(self):
        model = MetadataConfig()
        assert model.tables == {}
        assert model.glossary is None

    def test_status_defaults_to_draft(self):
        cfg = _valid_metadata()
        del cfg["tables"]["employees_enriched"]["status"]
        model = MetadataConfig(**cfg)
        assert model.tables["employees_enriched"].status == "draft"

    def test_invalid_status_raises(self):
        cfg = _valid_metadata()
        cfg["tables"]["employees_enriched"]["status"] = "reviewed"
        with pytest.raises(ValueError, match="status"):
            MetadataConfig(**cfg)

    def test_invalid_layer_raises(self):
        cfg = _valid_metadata()
        cfg["tables"]["employees_enriched"]["layer"] = "bronze"
        with pytest.raises(ValueError, match="layer"):
            MetadataConfig(**cfg)

    def test_columns_default_to_empty_dict(self):
        cfg = {"tables": {"jobs": {"layer": "silver"}}}
        model = MetadataConfig(**cfg)
        assert model.tables["jobs"].columns == {}

    def test_no_per_column_status_field(self):
        # Locked decision: status is table-level only.
        cfg = _valid_metadata()
        cfg["tables"]["employees_enriched"]["columns"]["salary"]["status"] = "approved"
        with pytest.raises(ValueError, match="status"):
            MetadataConfig(**cfg)

    def test_unknown_table_key_raises(self):
        cfg = _valid_metadata()
        cfg["tables"]["employees_enriched"]["descriptoin"] = "typo"
        with pytest.raises(ValueError, match="descriptoin"):
            MetadataConfig(**cfg)

    def test_unknown_top_level_key_raises(self):
        cfg = _valid_metadata()
        cfg["glosary"] = cfg.pop("glossary")
        with pytest.raises(ValueError, match="glosary"):
            MetadataConfig(**cfg)

    def test_glossary_optional(self):
        cfg = _valid_metadata()
        del cfg["glossary"]
        model = MetadataConfig(**cfg)
        assert model.glossary is None


class TestLoadMetadata:

    def test_loads_metadata_yaml(self, tmp_path):
        _write(tmp_path, "hr", metadata=_valid_metadata())
        model = load_metadata("hr", tmp_path)
        assert model.tables["employees_enriched"].description.startswith("One row")

    def test_missing_metadata_yaml_returns_empty_config(self, tmp_path):
        (tmp_path / "hr").mkdir(parents=True)
        model = load_metadata("hr", tmp_path)
        assert model.tables == {}

    def test_enhancements_override_description(self, tmp_path):
        _write(
            tmp_path, "hr",
            metadata=_valid_metadata(),
            enhancements={"tables": {"employees_enriched": {"description": "Human-refined description."}}},
        )
        model = load_metadata("hr", tmp_path)
        assert model.tables["employees_enriched"].description == "Human-refined description."
        # Unrelated fields survive the merge untouched.
        assert model.tables["employees_enriched"].status == "draft"
        assert model.tables["employees_enriched"].columns["salary"].description.startswith("Employee's annual")

    def test_enhancements_add_new_synonym_without_losing_others(self, tmp_path):
        _write(
            tmp_path, "hr",
            metadata=_valid_metadata(),
            enhancements={"tables": {"employees_enriched": {"columns": {"salary": {"synonyms": ["pay", "comp"]}}}}},
        )
        model = load_metadata("hr", tmp_path)
        assert model.tables["employees_enriched"].columns["salary"].synonyms == ["pay", "comp"]
        assert model.tables["employees_enriched"].columns["employee_id"].synonyms == ["emp_id"]

    def test_enhancements_can_add_glossary_entries(self, tmp_path):
        _write(
            tmp_path, "hr",
            metadata=_valid_metadata(),
            enhancements={"glossary": {"fiscal_year": "runs Feb 1 - Jan 31, not calendar year"}},
        )
        model = load_metadata("hr", tmp_path)
        assert "active_employee" in model.glossary
        assert model.glossary["fiscal_year"].startswith("runs Feb 1")

    def test_enhancements_without_metadata_yaml_still_loads(self, tmp_path):
        _write(tmp_path, "hr", enhancements={"glossary": {"term": "definition"}})
        model = load_metadata("hr", tmp_path)
        assert model.glossary == {"term": "definition"}

    def test_invalid_merged_config_raises_value_error(self, tmp_path):
        _write(tmp_path, "hr", metadata={"tables": {"jobs": {"layer": "oracle"}}})
        with pytest.raises(ValueError, match="layer"):
            load_metadata("hr", tmp_path)

    def test_validate_metadata_matches_load_metadata_errors(self, tmp_path):
        with pytest.raises(ValueError, match="\\[metadata\\]"):
            _validate_metadata({"tables": {"jobs": {"layer": "oracle"}}})
