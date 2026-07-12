"""config/validator.py — structural validation for a merged project config dict.

Backed by the Pydantic models in :mod:`openmedallion.config.schema`.  This
module's job is just to translate ``pydantic.ValidationError`` into a plain
``ValueError`` whose message identifies the failing key path, matching the
format callers (and the existing test suite) already depend on.  Message
translation itself lives in :mod:`openmedallion.config.errors` so other
Pydantic-backed loaders (e.g. ``metadata/loader.py``) can reuse it.
"""
from pydantic import ValidationError

from openmedallion.config.errors import format_validation_error
from openmedallion.config.schema import ProjectConfig

# Kept for any external code importing these directly (e.g. CLI help text).
_VALID_SOURCE_TYPES    = {"sql_database", "rest_api", "filesystem", "local_files"}
_VALID_TRANSFORM_TYPES = {"rename", "cast", "drop", "udf", "fillna", "clip", "normalize", "deduplicate", "filter_rows", "map_values", "allowed_values", "coerce_bool"}
_VALID_DIALECTS        = {"oracle", "postgres", "mysql", "mssql", "sqlite"}
_VALID_NORMALIZE_OPS   = {"upper", "lower", "strip", "strip_lower"}
_VALID_AGG_TYPES       = {"count", "sum", "mean", "min", "max", "median", "std", "var", "first", "last", "count_distinct"}
_VALID_REPORT_TYPES    = {"profile", "walker"}
_VALID_DUCKDB_MODES    = {"views", "tables"}


def _validate_config(cfg: dict) -> None:
    """Validate a fully-merged project config dict.

    Builds a :class:`~openmedallion.config.schema.ProjectConfig` from *cfg*
    and raises ``ValueError`` with a human-readable message identifying the
    exact failing key path on any structural or value problem, so callers can
    fix the YAML without reading source code.

    Called automatically by :func:`~openmedallion.config.loader.load_project`
    after env-var expansion.

    Args:
        cfg: The merged config dict returned by loading all four YAML files.

    Raises:
        ValueError: On any structural or value problem found in the config.
    """
    try:
        ProjectConfig(**cfg)
    except ValidationError as e:
        raise ValueError(format_validation_error(e, "config")) from None
