"""contracts/udf.py — Shared UDF loader and canonical contract definitions.

Three UDF contracts are recognised by this framework.  All UDFs must return
``pl.DataFrame`` and may accept arbitrary keyword arguments from the YAML
``args:`` block.

Silver base-table UDF
    Signature: ``(df: pl.DataFrame, **kwargs) -> pl.DataFrame``
    Called once per base table, after structural transforms (rename/cast/drop).

Silver derived-table UDF
    Signature: ``(silver_dir: Path, **kwargs) -> pl.DataFrame``
    Called after all base tables are written.  Must load its own inputs from
    ``silver_dir`` and return the derived DataFrame.

Gold pre-aggregation UDF
    Signature: ``(df: pl.DataFrame, silver_dir: Path, **kwargs) -> pl.DataFrame``
    Called before the ``group_by`` aggregation.  Receives the source DataFrame
    plus the silver directory so it can join additional tables for enrichment.

Usage
-----
    from openmedallion.contracts.udf import load_udf, check_return

    fn, kwargs = load_udf(step, cache=self._udf_cache, layer="silver")
    result = fn(df, **kwargs)
    check_return(result, step["function"], step["file"])
"""
import importlib.util
from pathlib import Path
from typing import Any, Callable


def load_udf(step: dict, *, cache: dict, layer: str) -> tuple[Callable, dict]:
    """Resolve a UDF config block to a callable and its keyword arguments.

    Loads the UDF module from ``step["file"]``, retrieves ``step["function"]``,
    and returns them together with any ``step["args"]`` as a kwargs dict.
    Modules are cached in ``cache`` (keyed by resolved file path) so repeated
    calls for the same file within one pipeline run do not re-import.

    Args:
        step:   UDF config dict with ``file``, ``function``, and optional ``args``.
        cache:  Per-instance mutable dict used as a module cache.
        layer:  ``"silver"`` or ``"gold"`` — used only in error messages.

    Returns:
        tuple[Callable, dict]: ``(fn, kwargs)`` ready to call.

    Raises:
        FileNotFoundError: If ``step["file"]`` does not exist.
        AttributeError:   If ``step["function"]`` is not in the loaded module.
    """
    file_path = Path(step["file"])
    func_name = step["function"]
    kwargs: dict[str, Any] = step.get("args") or {}

    if not file_path.exists():
        raise FileNotFoundError(
            f"[{layer}] UDF file not found: {file_path}\n"
            f"          (path is relative to the project root)"
        )

    key = str(file_path.resolve())
    if key not in cache:
        spec   = importlib.util.spec_from_file_location(file_path.stem, file_path)
        module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
        spec.loader.exec_module(module)                  # type: ignore[union-attr]
        cache[key] = module

    module = cache[key]
    if not hasattr(module, func_name):
        available = [n for n in dir(module) if not n.startswith("_")]
        raise AttributeError(
            f"[{layer}] '{func_name}' not found in {file_path}. "
            f"Available: {available}"
        )

    return getattr(module, func_name), kwargs


def check_return(result: Any, func_name: str, file_path: Any, layer: str = "") -> None:
    """Assert that a UDF returned a ``pl.DataFrame``.

    Args:
        result:    Value returned by the UDF.
        func_name: Name of the UDF function (for error messages).
        file_path: Path to the UDF file (for error messages).
        layer:     ``"silver"`` or ``"gold"`` prefix for the error message.

    Raises:
        TypeError: If ``result`` is not a ``pl.DataFrame``.
    """
    import polars as pl
    if not isinstance(result, pl.DataFrame):
        prefix = f"[{layer}] " if layer else ""
        raise TypeError(
            f"{prefix}UDF '{func_name}' in {file_path} must return pl.DataFrame, "
            f"got {type(result).__name__}"
        )
