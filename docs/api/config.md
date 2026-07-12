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

Internally builds a [`ProjectConfig`](#projectconfig-pydantic-schema) Pydantic model from `cfg` and translates any `pydantic.ValidationError` into a plain `ValueError` whose message identifies the failing key path — same contract as before, just backed by typed models instead of hand-written `if` checks.

**Parameters:**

| Name | Type | Description |
| --- | --- | --- |
| `cfg` | `dict` | Merged config dict from all four YAML files. |

**Raises:** `ValueError` — with a human-readable message identifying the failing key path.

**What is validated:**

- `pipeline.name` — non-empty string
- All four `paths.*` keys present
- `source.type` / each `sources[i].type` — one of `sql_database`, `rest_api`, `filesystem`, `local_files`
- Each `bronze_to_silver.tables[i]` — has `source_file` and `output_file`
- Each `transforms[j].type` — one of the declarative transform types, with type-specific required keys (e.g. `udf` needs `file` + `function`, `map_values` needs `column` + `mapping`)
- Each `silver_to_gold.projects[i]` — has `name`
- Each `pre_agg_udf` block — has `file` and `function`
- **Unknown keys anywhere in the schema** — every block uses `extra="forbid"`, so a typo like `filter_propogate` or `pipline` is rejected at load time instead of being silently ignored
- **`filter_propagate` cross-reference** — the referenced table must exist in the same source (matched by alias, then name) and must itself have a `filter` set

**Example error message:**

```
ValueError: [config] bronze_to_silver.tables[0].source_file is required
```

---

## `ProjectConfig` (Pydantic schema)

```python
from openmedallion.config.schema import ProjectConfig
```

`openmedallion/config/schema.py` defines the full set of Pydantic models backing `_validate_config` — `ProjectConfig`, `PipelineBlock`, `PathsBlock`, `SourceBlock`, `SourceTable`, `IncrementalBlock`, `TransformSpec`, `SilverTable`, `DerivedTable`, `DuckdbBlock`, `BronzeToSilver`, `MetricSpec`, `SortSpec`, `Aggregation`, `GoldProject`, `SilverToGold`, `BiExport`, `ExploreSpec`.

Every model sets `model_config = ConfigDict(extra="forbid")` (the one exception is `DestinationBlock`, which stays permissive since dlt destinations like `bigquery`/`snowflake` take backend-specific kwargs not worth enumerating). Cross-field rules (credentials_file → dialect, filter_propagate → valid + filtered reference, duckdb enabled → path required) are expressed as `@model_validator(mode="after")` methods rather than imperative checks.

You can build a `ProjectConfig` directly for type-checked access to a merged config:

```python
from openmedallion.config.schema import ProjectConfig

model = ProjectConfig(**cfg)
model.pipeline.name       # str
model.source.type         # Literal["sql_database", "rest_api", "filesystem", "local_files"]
```
