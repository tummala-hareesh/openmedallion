"""config/validator.py — structural validation for a merged project config dict."""

_VALID_SOURCE_TYPES    = {"sql_database", "rest_api", "filesystem", "local_files"}
_VALID_TRANSFORM_TYPES = {"rename", "cast", "drop", "udf"}


def _validate_config(cfg: dict) -> None:
    """Validate a fully-merged project config dict.

    Checks that required keys are present and that enumerated values are
    recognised.  Raises ``ValueError`` with a human-readable message that
    identifies the exact failing key path so callers can fix the YAML
    without reading source code.

    Called automatically by :func:`~openmedallion.config.loader.load_project`
    after env-var expansion.

    Args:
        cfg: The merged config dict returned by loading all four YAML files.

    Raises:
        ValueError: On any structural or value problem found in the config.
    """
    def require(cond: bool, msg: str) -> None:
        if not cond:
            raise ValueError(f"[config] {msg}")

    def require_str(val, path: str) -> None:
        require(isinstance(val, str) and val.strip(), f"'{path}' must be a non-empty string")

    def require_list(val, path: str) -> None:
        require(isinstance(val, list), f"'{path}' must be a list")

    # pipeline.name
    require_str(cfg.get("pipeline", {}).get("name"), "pipeline.name")

    # paths.*
    paths = cfg.get("paths", {})
    for key in ("bronze", "silver", "gold", "export"):
        require(key in paths, f"paths.{key} is required")

    # source.type (optional block — only validate if present)
    source = cfg.get("source")
    if source is not None:
        src_type = source.get("type")
        require(
            src_type in _VALID_SOURCE_TYPES,
            f"source.type must be one of {sorted(_VALID_SOURCE_TYPES)}, got '{src_type}'"
        )

    # bronze_to_silver (optional block)
    b2s = cfg.get("bronze_to_silver")
    if b2s is not None:
        tables = b2s.get("tables", [])
        require_list(tables, "bronze_to_silver.tables")
        for i, tbl in enumerate(tables):
            path = f"bronze_to_silver.tables[{i}]"
            require("source_file" in tbl, f"{path}.source_file is required")
            require("output_file" in tbl, f"{path}.output_file is required")
            for j, tx in enumerate(tbl.get("transforms", [])):
                tx_path = f"{path}.transforms[{j}]"
                tx_type = tx.get("type")
                require(
                    tx_type in _VALID_TRANSFORM_TYPES,
                    f"{tx_path}.type must be one of {sorted(_VALID_TRANSFORM_TYPES)}, "
                    f"got '{tx_type}'"
                )
                if tx_type == "udf":
                    require("file" in tx, f"{tx_path} (udf): 'file' is required")
                    require("function" in tx, f"{tx_path} (udf): 'function' is required")

    # silver_to_gold (optional block)
    s2g = cfg.get("silver_to_gold")
    if s2g is not None:
        projects = s2g.get("projects", [])
        require_list(projects, "silver_to_gold.projects")
        for i, proj in enumerate(projects):
            path = f"silver_to_gold.projects[{i}]"
            require("name" in proj, f"{path}.name is required")
            aggs = proj.get("aggregations", [])
            require_list(aggs, f"{path}.aggregations")
            for j, agg in enumerate(aggs):
                udf_block = agg.get("pre_agg_udf")
                if udf_block is not None:
                    udf_path = f"{path}.aggregations[{j}].pre_agg_udf"
                    require("file" in udf_block, f"{udf_path}: 'file' is required")
                    require("function" in udf_block, f"{udf_path}: 'function' is required")
