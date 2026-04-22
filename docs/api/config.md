# Config API

Handles loading, merging, environment-variable expansion, and validation of the four project YAML files.

```python
from openmedallion.config.loader    import load_project, expand_env_str
from openmedallion.config.validator import _validate_config
```

---

## `load_project`

```python
def load_project(
    project: str,
    projects_root: str | Path = "projects",
) -> dict
```

Read all four YAML files for a project and return one merged config dict, ready for the pipeline engine classes.

**Parameters:**

| Name | Type | Default | Description |
| --- | --- | --- | --- |
| `project` | `str` | — | Project name — folder name under `projects_root`. |
| `projects_root` | `str \| Path` | `"projects"` | Parent directory containing project folders. |

**Returns:** `dict` — Fully merged configuration with env vars expanded and structure validated.

**Raises:**

| Exception | When |
| --- | --- |
| `FileNotFoundError` | `main.yaml` or any included layer file does not exist. |
| `ValueError` | `includes` block missing, a layer key missing, or structural validation failure. |
| `EnvironmentError` | A `${VAR}` placeholder references an env var that is not set and has no default. |

**How it works:**

1. Reads `projects/<project>/main.yaml`
2. Pops `includes` — maps each layer (`bronze`, `silver`, `gold`) to a filename
3. Loads each layer file and deep-merges into the main config
4. Calls `_expand_env_vars()` to resolve all `${VAR}` placeholders
5. Calls `_validate_config()` to check structural integrity

**Example:**

```python
from openmedallion.config.loader import load_project

cfg = load_project("sales_project")
# cfg == {
#     "pipeline": {"name": "sales_project"},
#     "paths": {"bronze": "./data/bronze", ...},
#     "source": {"type": "sql_database", ...},
#     "bronze_to_silver": {...},
#     "silver_to_gold": {...},
# }
```

```python
# Custom projects directory
cfg = load_project("sales_project", projects_root="/var/pipelines/projects")
```

---

## `expand_env_str`

```python
def expand_env_str(s: str) -> str
```

Expand `${VAR}` and `${VAR:-default}` placeholders in a single string.

**Parameters:**

| Name | Type | Description |
| --- | --- | --- |
| `s` | `str` | String potentially containing `${VAR}` or `${VAR:-default}` patterns. |

**Returns:** `str` — String with all placeholders replaced by their values.

**Raises:** `EnvironmentError` — If a referenced env var is not set and has no default value.

**Examples:**

```python
import os
from openmedallion.config.loader import expand_env_str

os.environ["DB_HOST"] = "prod-db.example.com"

expand_env_str("oracle+oracledb://user:pass@${DB_HOST}/xe")
# "oracle+oracledb://user:pass@prod-db.example.com/xe"

expand_env_str("s3://${BUCKET:-my-default-bucket}/data")
# "s3://my-default-bucket/data"  (when BUCKET is not set)

expand_env_str("${MISSING_VAR}")
# EnvironmentError: Required env var 'MISSING_VAR' is not set
```

!!! tip
    Use `expand_env_str` in UDFs that build connection strings at runtime rather than reading from config, to keep credentials out of YAML files.

---

## `_deep_merge`

```python
def _deep_merge(base: dict, override: dict) -> None
```

Merge `override` into `base` in-place, recursing into nested dicts. Non-dict values are overwritten; dict values are merged recursively.

**Parameters:**

| Name | Type | Description |
| --- | --- | --- |
| `base` | `dict` | Target dict — modified in-place. |
| `override` | `dict` | Source dict — values from here overwrite `base`. |

**Returns:** `None` — modifies `base` in-place.

**Example:**

```python
from openmedallion.config.loader import _deep_merge

base     = {"paths": {"bronze": "./local"}, "pipeline": {"name": "x"}}
override = {"paths": {"silver": "./silver"}, "pipeline": {"name": "y"}}

_deep_merge(base, override)
# base == {"paths": {"bronze": "./local", "silver": "./silver"}, "pipeline": {"name": "y"}}
```

---

## `_validate_config`

```python
def _validate_config(cfg: dict) -> None
```

Validate a fully-merged project config dict. Called automatically by `load_project()` — you rarely need to call this directly.

**Parameters:**

| Name | Type | Description |
| --- | --- | --- |
| `cfg` | `dict` | Merged config dict from all four YAML files. |

**Raises:** `ValueError` — with a human-readable message identifying the failing key path.

**What is validated:**

- `pipeline.name` — non-empty string
- All four `paths.*` keys present
- `source.type` (if present) — one of `sql_database`, `rest_api`, `filesystem`
- Each `bronze_to_silver.tables[i]` — has `source_file` and `output_file`
- Each `transforms[j].type` — one of `rename`, `cast`, `drop`, `udf`
- UDF transform blocks — have `file` and `function`
- Each `silver_to_gold.projects[i]` — has `name`
- Each `pre_agg_udf` block — has `file` and `function`

**Example error message:**

```
ValueError: [config] bronze_to_silver.tables[0].source_file is required
```
