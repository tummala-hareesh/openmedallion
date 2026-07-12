"""config/validator.py — structural validation for a merged project config dict.

Backed by the Pydantic models in :mod:`openmedallion.config.schema`.  This
module's job is just to translate ``pydantic.ValidationError`` into a plain
``ValueError`` whose message identifies the failing key path, matching the
format callers (and the existing test suite) already depend on.
"""
from pydantic import ValidationError

from openmedallion.config.schema import ProjectConfig

# Kept for any external code importing these directly (e.g. CLI help text).
_VALID_SOURCE_TYPES    = {"sql_database", "rest_api", "filesystem", "local_files"}
_VALID_TRANSFORM_TYPES = {"rename", "cast", "drop", "udf", "fillna", "clip", "normalize", "deduplicate", "filter_rows", "map_values", "allowed_values", "coerce_bool"}
_VALID_DIALECTS        = {"oracle", "postgres", "mysql", "mssql", "sqlite"}
_VALID_NORMALIZE_OPS   = {"upper", "lower", "strip", "strip_lower"}
_VALID_AGG_TYPES       = {"count", "sum", "mean", "min", "max", "median", "std", "var", "first", "last", "count_distinct"}
_VALID_REPORT_TYPES    = {"profile", "walker"}
_VALID_DUCKDB_MODES    = {"views", "tables"}


def _format_loc(loc: tuple) -> str:
    """Render a pydantic error ``loc`` tuple as ``a.b[0].c``."""
    out = ""
    for tok in loc:
        if isinstance(tok, int):
            out += f"[{tok}]"
        else:
            out += f".{tok}" if out else str(tok)
    return out


def _format_error(err: dict) -> str:
    loc   = err["loc"]
    etype = err["type"]
    msg   = err["msg"]

    if msg.startswith("Value error, "):
        msg = msg[len("Value error, "):]
        prefix = _format_loc(loc)
        return f"{prefix}: {msg}" if prefix else msg

    if etype == "missing":
        field = loc[-1]
        parent = _format_loc(loc[:-1])
        if field in ("file", "function"):
            return f"{parent}: '{field}' is required" if parent else f"'{field}' is required"
        full = _format_loc(loc)
        return f"{full} is required"

    return f"{_format_loc(loc)}: {msg}"


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
        messages = [_format_error(err) for err in e.errors()]
        raise ValueError(f"[config] {'; '.join(messages)}") from None
