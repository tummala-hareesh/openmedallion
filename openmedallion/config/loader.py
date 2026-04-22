"""config/loader.py — load and merge the 4 project YAML files into one config dict.

Folder layout (created by ``medallion init <project>``):

.. code-block:: text

    projects/
      <project>/
        main.yaml      ← pipeline name, paths, bi_export, includes
        bronze.yaml    ← source connection, destination, tables, incremental
        silver.yaml    ← bronze_to_silver transforms + derived table UDFs
        gold.yaml      ← silver_to_gold aggregations + pre-agg UDFs

How it works
------------
1. Read ``projects/<project>/main.yaml`` — the entry point.
2. Extract ``includes`` — maps each layer to a filename in the same folder.
3. Load bronze → silver → gold in order, deep-merging into one dict.
4. Expand ``${VAR}`` / ``${VAR:-default}`` placeholders in all string values.
5. Validate structural integrity.
6. Return the merged dict to the Hamilton DAG.
"""
import os
import re
import yaml
from pathlib import Path

from openmedallion.config.validator import _validate_config


_ENV_PATTERN = re.compile(r"\$\{(\w+)(?::-(.*?))?\}")


def expand_env_str(s: str) -> str:
    """Expand ``${VAR}`` and ``${VAR:-default}`` placeholders in a single string.

    This is the canonical env-var expander for the whole framework.
    Use this wherever a single string value needs substitution (e.g. connection
    strings).  For expanding an entire config dict, use :func:`_expand_env_vars`.

    Args:
        s: String potentially containing ``${VAR}`` or ``${VAR:-default}``.

    Returns:
        str: String with all placeholders replaced.

    Raises:
        EnvironmentError: If a referenced env var is not set and has no default.
    """
    def _replacer(m: re.Match) -> str:
        var, default = m.group(1), m.group(2)
        val = os.environ.get(var)
        if val is not None:
            return val
        if default is not None:
            return default
        raise EnvironmentError(
            f"Required env var '{var}' is not set (referenced in project config)."
        )
    return _ENV_PATTERN.sub(_replacer, s)


def _expand_env_vars(obj: object) -> None:
    """Recursively expand ``${VAR}`` / ``${VAR:-default}`` in all string values.

    Modifies dicts and lists in-place.  Delegates per-string expansion to
    :func:`expand_env_str`.

    Args:
        obj: The config object to expand in-place (dict, list, or scalar).
    """
    def _walk(node: object) -> None:
        if isinstance(node, dict):
            for key, val in node.items():
                if isinstance(val, str):
                    node[key] = expand_env_str(val)
                else:
                    _walk(val)
        elif isinstance(node, list):
            for i, item in enumerate(node):
                if isinstance(item, str):
                    node[i] = expand_env_str(item)
                else:
                    _walk(item)

    _walk(obj)


def _deep_merge(base: dict, override: dict) -> None:
    """Merge *override* into *base* in-place, recursing into nested dicts.

    Args:
        base: Target dict modified in-place.
        override: Source dict whose values overwrite ``base``.
    """
    for key, val in override.items():
        if key in base and isinstance(base[key], dict) and isinstance(val, dict):
            _deep_merge(base[key], val)
        else:
            base[key] = val


def load_project(project: str, projects_root: str | Path = "projects") -> dict:
    """Read all YAML files for a project and return one merged config dict.

    Args:
        project: Project name — folder name under ``projects_root``.
        projects_root: Parent directory containing project folders.
            Defaults to ``"projects"``.

    Returns:
        dict: Fully merged configuration ready for the pipeline engine classes.

    Raises:
        FileNotFoundError: If ``main.yaml`` or any included layer file is missing.
        ValueError: If the ``includes`` block is absent or a layer key is missing.
    """
    root      = Path(projects_root) / project
    main_path = root / "main.yaml"

    if not main_path.exists():
        raise FileNotFoundError(
            f"No config found for project '{project}' at {main_path}\n"
            f"Run:  medallion init {project}"
        )

    with open(main_path) as f:
        cfg = yaml.safe_load(f) or {}

    includes = cfg.pop("includes", {})
    if not includes:
        raise ValueError(f"'includes' block missing in {main_path}")

    for layer in ("bronze", "silver", "gold"):
        if not includes.get(layer):
            raise ValueError(f"'includes.{layer}' missing in {main_path}")

    for layer in ("bronze", "silver", "gold"):
        layer_path = root / includes[layer]
        if not layer_path.exists():
            raise FileNotFoundError(f"Included {layer} config not found: {layer_path}")

        with open(layer_path) as f:
            _deep_merge(cfg, yaml.safe_load(f) or {})

        print(f"📋  [config] loaded {layer:6s}: {layer_path}")

    _expand_env_vars(cfg)
    _validate_config(cfg)
    return cfg
