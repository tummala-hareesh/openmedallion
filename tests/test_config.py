"""tests/test_config.py — tests for config loader + validator."""
import pytest
import yaml
from pathlib import Path

from openmedallion.config import load_project, _deep_merge, _validate_config


def write_project(root: Path, project: str, main: dict, bronze: dict,
                  silver: dict, gold: dict) -> Path:
    project_dir = root / project
    project_dir.mkdir(parents=True)
    for name, data in [("main", main), ("bronze", bronze),
                       ("silver", silver), ("gold", gold)]:
        with open(project_dir / f"{name}.yaml", "w") as f:
            yaml.dump(data, f)
    return project_dir


def minimal_project(project: str = "test") -> tuple[dict, dict, dict, dict]:
    return (
        {"pipeline": {"name": project},
         "includes": {"bronze": "bronze.yaml", "silver": "silver.yaml", "gold": "gold.yaml"},
         "paths": {"bronze": "./b", "silver": "./s", "gold": "./g", "export": "./e"}},
        {"source": {"type": "filesystem"}},
        {"bronze_to_silver": {"tables": []}},
        {"silver_to_gold": {"projects": []}},
    )


class TestLoadProject:

    def test_loads_and_merges_all_four_files(self, tmp_path):
        write_project(tmp_path, "sales", *minimal_project("sales"))
        cfg = load_project("sales", tmp_path)
        assert cfg["pipeline"]["name"] == "sales"
        assert "source" in cfg
        assert "bronze_to_silver" in cfg
        assert "silver_to_gold" in cfg

    def test_includes_key_is_consumed(self, tmp_path):
        write_project(tmp_path, "p", *minimal_project())
        cfg = load_project("p", tmp_path)
        assert "includes" not in cfg

    def test_paths_preserved(self, tmp_path):
        write_project(tmp_path, "p", *minimal_project())
        cfg = load_project("p", tmp_path)
        assert cfg["paths"]["bronze"] == "./b"
        assert cfg["paths"]["silver"] == "./s"

    def test_missing_project_raises_file_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="No config found"):
            load_project("nonexistent", tmp_path)

    def test_missing_includes_raises_value_error(self, tmp_path):
        main = {"pipeline": {"name": "p"}, "paths": {}}
        project_dir = tmp_path / "p"
        project_dir.mkdir()
        with open(project_dir / "main.yaml", "w") as f:
            yaml.dump(main, f)
        with pytest.raises(ValueError, match="'includes' block missing"):
            load_project("p", tmp_path)

    def test_missing_layer_file_raises_file_not_found(self, tmp_path):
        main, bronze, silver, gold = minimal_project()
        project_dir = tmp_path / "p"
        project_dir.mkdir()
        with open(project_dir / "main.yaml", "w") as f:
            yaml.dump(main, f)
        with open(project_dir / "silver.yaml", "w") as f:
            yaml.dump(silver, f)
        with open(project_dir / "gold.yaml", "w") as f:
            yaml.dump(gold, f)
        with pytest.raises(FileNotFoundError, match="bronze"):
            load_project("p", tmp_path)

    def test_missing_layer_key_raises_value_error(self, tmp_path):
        main = {"pipeline": {"name": "p"},
                "includes": {"bronze": "bronze.yaml", "silver": "silver.yaml"},
                "paths": {}}
        project_dir = tmp_path / "p"
        project_dir.mkdir()
        with open(project_dir / "main.yaml", "w") as f:
            yaml.dump(main, f)
        with pytest.raises(ValueError, match="includes.gold"):
            load_project("p", tmp_path)

    def test_custom_projects_root(self, tmp_path):
        custom_root = tmp_path / "custom_projects"
        custom_root.mkdir()
        write_project(custom_root, "my_proj", *minimal_project("my_proj"))
        cfg = load_project("my_proj", custom_root)
        assert cfg["pipeline"]["name"] == "my_proj"


class TestDeepMerge:

    def test_top_level_keys_merged(self):
        base = {"a": 1}
        _deep_merge(base, {"b": 2})
        assert base == {"a": 1, "b": 2}

    def test_nested_dicts_merged_recursively(self):
        base = {"paths": {"bronze": "b", "silver": "s"}}
        _deep_merge(base, {"paths": {"gold": "g"}})
        assert base["paths"] == {"bronze": "b", "silver": "s", "gold": "g"}

    def test_scalar_override_replaces(self):
        base = {"name": "old"}
        _deep_merge(base, {"name": "new"})
        assert base["name"] == "new"

    def test_list_override_replaces_not_extends(self):
        base = {"tables": [1, 2]}
        _deep_merge(base, {"tables": [3, 4, 5]})
        assert base["tables"] == [3, 4, 5]

    def test_empty_override_leaves_base_unchanged(self):
        base = {"a": 1, "b": {"c": 2}}
        _deep_merge(base, {})
        assert base == {"a": 1, "b": {"c": 2}}


def _valid_cfg(**overrides) -> dict:
    cfg = {
        "pipeline": {"name": "test"},
        "paths": {"bronze": "./b", "silver": "./s", "gold": "./g", "export": "./e"},
        "source": {"type": "filesystem"},
        "bronze_to_silver": {"tables": []},
        "silver_to_gold": {"projects": []},
    }
    for dotted_key, value in overrides.items():
        parts = dotted_key.split(".")
        node = cfg
        for part in parts[:-1]:
            node = node.setdefault(part, {})
        if value is None:
            node.pop(parts[-1], None)
        else:
            node[parts[-1]] = value
    return cfg


class TestValidateConfig:

    def test_valid_config_passes(self):
        _validate_config(_valid_cfg())

    def test_missing_pipeline_name_raises(self):
        with pytest.raises(ValueError, match="pipeline.name"):
            _validate_config(_valid_cfg(**{"pipeline.name": ""}))

    def test_pipeline_name_none_raises(self):
        cfg = _valid_cfg()
        cfg["pipeline"].pop("name")
        with pytest.raises(ValueError, match="pipeline.name"):
            _validate_config(cfg)

    def test_missing_path_key_raises(self):
        cfg = _valid_cfg()
        del cfg["paths"]["export"]
        with pytest.raises(ValueError, match="paths.export"):
            _validate_config(cfg)

    def test_invalid_source_type_raises(self):
        with pytest.raises(ValueError, match="source.type"):
            _validate_config(_valid_cfg(**{"source.type": "oracle"}))

    def test_valid_source_types_accepted(self):
        for t in ("sql_database", "rest_api", "filesystem"):
            _validate_config(_valid_cfg(**{"source.type": t}))

    def test_source_block_absent_is_ok(self):
        cfg = _valid_cfg()
        del cfg["source"]
        _validate_config(cfg)

    def test_b2s_tables_not_list_raises(self):
        with pytest.raises(ValueError, match="bronze_to_silver.tables"):
            _validate_config(_valid_cfg(**{"bronze_to_silver.tables": "bad"}))

    def test_b2s_table_missing_source_file_raises(self):
        cfg = _valid_cfg()
        cfg["bronze_to_silver"]["tables"] = [{"output_file": "out.parquet"}]
        with pytest.raises(ValueError, match="source_file"):
            _validate_config(cfg)

    def test_b2s_table_missing_output_file_raises(self):
        cfg = _valid_cfg()
        cfg["bronze_to_silver"]["tables"] = [{"source_file": "src.parquet"}]
        with pytest.raises(ValueError, match="output_file"):
            _validate_config(cfg)

    def test_invalid_transform_type_raises(self):
        cfg = _valid_cfg()
        cfg["bronze_to_silver"]["tables"] = [{
            "source_file": "a.parquet", "output_file": "b.parquet",
            "transforms": [{"type": "explode"}],
        }]
        with pytest.raises(ValueError, match="transforms\\[0\\].type"):
            _validate_config(cfg)

    def test_udf_transform_missing_file_raises(self):
        cfg = _valid_cfg()
        cfg["bronze_to_silver"]["tables"] = [{
            "source_file": "a.parquet", "output_file": "b.parquet",
            "transforms": [{"type": "udf", "function": "fn"}],
        }]
        with pytest.raises(ValueError, match="'file'"):
            _validate_config(cfg)

    def test_udf_transform_missing_function_raises(self):
        cfg = _valid_cfg()
        cfg["bronze_to_silver"]["tables"] = [{
            "source_file": "a.parquet", "output_file": "b.parquet",
            "transforms": [{"type": "udf", "file": "f.py"}],
        }]
        with pytest.raises(ValueError, match="'function'"):
            _validate_config(cfg)

    def test_s2g_projects_not_list_raises(self):
        with pytest.raises(ValueError, match="silver_to_gold.projects"):
            _validate_config(_valid_cfg(**{"silver_to_gold.projects": "bad"}))

    def test_s2g_project_missing_name_raises(self):
        cfg = _valid_cfg()
        cfg["silver_to_gold"]["projects"] = [{"aggregations": []}]
        with pytest.raises(ValueError, match="projects\\[0\\].name"):
            _validate_config(cfg)

    def test_pre_agg_udf_missing_file_raises(self):
        cfg = _valid_cfg()
        cfg["silver_to_gold"]["projects"] = [{
            "name": "proj",
            "aggregations": [{"pre_agg_udf": {"function": "fn"}}],
        }]
        with pytest.raises(ValueError, match="pre_agg_udf.*'file'"):
            _validate_config(cfg)

    def test_pre_agg_udf_missing_function_raises(self):
        cfg = _valid_cfg()
        cfg["silver_to_gold"]["projects"] = [{
            "name": "proj",
            "aggregations": [{"pre_agg_udf": {"file": "f.py"}}],
        }]
        with pytest.raises(ValueError, match="pre_agg_udf.*'function'"):
            _validate_config(cfg)

    def test_pre_agg_udf_absent_is_ok(self):
        cfg = _valid_cfg()
        cfg["silver_to_gold"]["projects"] = [{"name": "proj", "aggregations": [{}]}]
        _validate_config(cfg)
