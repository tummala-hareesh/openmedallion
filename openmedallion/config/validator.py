"""config/validator.py — structural validation for a merged project config dict."""

_VALID_SOURCE_TYPES    = {"sql_database", "rest_api", "filesystem", "local_files"}
_VALID_TRANSFORM_TYPES = {"rename", "cast", "drop", "udf", "fillna", "clip", "normalize", "deduplicate", "filter_rows", "map_values", "allowed_values", "coerce_bool"}
_VALID_DIALECTS        = {"oracle", "postgres", "mysql", "mssql", "sqlite"}
_VALID_NORMALIZE_OPS   = {"upper", "lower", "strip", "strip_lower"}
_VALID_AGG_TYPES       = {"count", "sum", "mean", "min", "max", "median", "std", "var", "first", "last", "count_distinct"}
_VALID_REPORT_TYPES    = {"profile", "walker"}
_VALID_DUCKDB_MODES    = {"views", "tables"}


def _validate_explore_specs(specs: list, path: str, require, require_str) -> None:
    """Validate a list of inline explore report specs at *path*."""
    for k, spec in enumerate(specs):
        exp_path = f"{path}[{k}]"
        rt = spec.get("report_type")
        require(
            rt in _VALID_REPORT_TYPES,
            f"{exp_path}.report_type must be one of {sorted(_VALID_REPORT_TYPES)}, got '{rt}'"
        )
        if "output_file" in spec:
            require_str(spec["output_file"], f"{exp_path}.output_file")
        if "title" in spec:
            require_str(spec["title"], f"{exp_path}.title")
        if "minimal" in spec:
            require(isinstance(spec["minimal"], bool), f"{exp_path}.minimal must be a bool")


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

    def _require_str_list(val, path: str) -> None:
        require(isinstance(val, list) and len(val) > 0
                and all(isinstance(c, str) and c.strip() for c in val),
                f"'{path}' must be a non-empty list of column name strings")

    def _validate_source(source: dict, prefix: str) -> None:
        """Validate one source block (used for both source: and sources: entries)."""
        src_type = source.get("type")
        require(
            src_type in _VALID_SOURCE_TYPES,
            f"{prefix}.type must be one of {sorted(_VALID_SOURCE_TYPES)}, got '{src_type}'"
        )
        if src_type == "sql_database":
            if "credentials_file" in source:
                require_str(source["credentials_file"], f"{prefix}.credentials_file")
                dialect = source.get("dialect")
                require(
                    dialect in _VALID_DIALECTS,
                    f"{prefix}.dialect is required when using credentials_file. "
                    f"Must be one of {sorted(_VALID_DIALECTS)}, got '{dialect}'"
                )
            if "dialect" in source and "credentials_file" not in source:
                require(
                    source["dialect"] in _VALID_DIALECTS,
                    f"{prefix}.dialect must be one of {sorted(_VALID_DIALECTS)}, "
                    f"got '{source['dialect']}'"
                )
        if src_type in ("sql_database", "local_files"):
            for i, tbl in enumerate(source.get("tables", [])):
                if "alias" in tbl:
                    require_str(tbl["alias"], f"{prefix}.tables[{i}].alias")
                if "select" in tbl:
                    _require_str_list(tbl["select"], f"{prefix}.tables[{i}].select")
                if "explore" in tbl:
                    _validate_explore_specs(
                        tbl["explore"], f"{prefix}.tables[{i}].explore", require, require_str
                    )
        else:
            if "alias" in source:
                require_str(source["alias"], f"{prefix}.alias")
            if "select" in source:
                _require_str_list(source["select"], f"{prefix}.select")

    # source / sources (both optional; raise if both present)
    has_source  = cfg.get("source")  is not None
    has_sources = cfg.get("sources") is not None
    require(
        not (has_source and has_sources),
        "use either 'sources:' (list) or 'source:' (dict), not both"
    )
    if has_sources:
        sources_list = cfg["sources"]
        require(isinstance(sources_list, list) and len(sources_list) > 0,
                "'sources' must be a non-empty list")
        for i, src in enumerate(sources_list):
            _validate_source(src, f"sources[{i}]")
    elif has_source:
        _validate_source(cfg["source"], "source")

    def _validate_duckdb(block: dict, prefix: str) -> None:
        if "enabled" in block:
            require(isinstance(block["enabled"], bool), f"{prefix}.enabled must be a bool")
        if "path" in block:
            require_str(block["path"], f"{prefix}.path")
        mode = block.get("mode", "views")
        require(
            mode in _VALID_DUCKDB_MODES,
            f"{prefix}.mode must be one of {sorted(_VALID_DUCKDB_MODES)}, got '{mode}'"
        )

    # bronze_to_silver (optional block)
    b2s = cfg.get("bronze_to_silver")
    if b2s is not None:
        if duck := b2s.get("duckdb"):
            _validate_duckdb(duck, "bronze_to_silver.duckdb")
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
                if tx_type == "fillna":
                    require(isinstance(tx.get("columns"), dict), f"{tx_path} (fillna): 'columns' must be a dict")
                if tx_type == "clip":
                    require(isinstance(tx.get("columns"), dict), f"{tx_path} (clip): 'columns' must be a dict")
                if tx_type == "normalize":
                    cols = tx.get("columns")
                    require(isinstance(cols, dict), f"{tx_path} (normalize): 'columns' must be a dict")
                    if isinstance(cols, dict):
                        for c, op in cols.items():
                            require(op in _VALID_NORMALIZE_OPS,
                                    f"{tx_path} (normalize): op '{op}' for column '{c}' must be one of {sorted(_VALID_NORMALIZE_OPS)}")
                if tx_type == "filter_rows":
                    require(isinstance(tx.get("expr"), str) and tx.get("expr", "").strip(),
                            f"{tx_path} (filter_rows): 'expr' must be a non-empty string")
                if tx_type == "map_values":
                    require(isinstance(tx.get("column"), str) and tx.get("column", "").strip(),
                            f"{tx_path} (map_values): 'column' must be a non-empty string")
                    require(isinstance(tx.get("mapping"), dict), f"{tx_path} (map_values): 'mapping' must be a dict")
                if tx_type == "allowed_values":
                    require(isinstance(tx.get("columns"), dict), f"{tx_path} (allowed_values): 'columns' must be a dict mapping column names to lists")
                    if isinstance(tx.get("columns"), dict):
                        for c, vals in tx["columns"].items():
                            require(isinstance(vals, list) and len(vals) > 0,
                                    f"{tx_path} (allowed_values): values for column '{c}' must be a non-empty list")
                if tx_type == "coerce_bool":
                    require(isinstance(tx.get("columns"), list) and len(tx.get("columns", [])) > 0,
                            f"{tx_path} (coerce_bool): 'columns' must be a non-empty list of column names")
            if "explore" in tbl:
                _validate_explore_specs(tbl["explore"], f"{path}.explore", require, require_str)

        for i, dtbl in enumerate(b2s.get("derived_tables", [])):
            path = f"bronze_to_silver.derived_tables[{i}]"
            if "explore" in dtbl:
                _validate_explore_specs(dtbl["explore"], f"{path}.explore", require, require_str)

    # silver_to_gold (optional block)
    s2g = cfg.get("silver_to_gold")
    if s2g is not None:
        if duck := s2g.get("duckdb"):
            _validate_duckdb(duck, "silver_to_gold.duckdb")
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
                agg_path = f"{path}.aggregations[{j}]"
                for mi, m in enumerate(agg.get("metrics", [])):
                    agg_type = m.get("agg")
                    require(agg_type in _VALID_AGG_TYPES,
                            f"{agg_path}.metrics[{mi}].agg must be one of {sorted(_VALID_AGG_TYPES)}, got '{agg_type}'")
                if "having" in agg:
                    require(isinstance(agg["having"], str) and agg["having"].strip(),
                            f"{agg_path}.having must be a non-empty string")
                if "sort" in agg:
                    sort_block = agg["sort"]
                    require(isinstance(sort_block.get("columns"), list) and sort_block["columns"],
                            f"{agg_path}.sort.columns must be a non-empty list")
                if "limit" in agg:
                    require(isinstance(agg["limit"], int) and agg["limit"] > 0,
                            f"{agg_path}.limit must be a positive integer")
                if "explore" in agg:
                    _validate_explore_specs(
                        agg["explore"], f"{path}.aggregations[{j}].explore", require, require_str
                    )
