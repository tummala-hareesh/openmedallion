# Contracts API

UDF loading and return-type enforcement. These are the functions the pipeline uses internally to resolve, import, and validate every UDF call.

```python
from openmedallion.contracts.udf import load_udf, check_return
```

---

## UDF Contracts

OpenMedallion enforces three distinct UDF contracts. Each has a different signature depending on where in the pipeline it runs.

### Silver base-table UDF

```python
def my_udf(df: pl.DataFrame, **kwargs) -> pl.DataFrame
```

- Called once per base table, after structural transforms (`rename`, `cast`, `drop`)
- Receives the already-transformed DataFrame
- `kwargs` come from the `args:` block in `silver.yaml`
- Must operate only on `df` — do not read files from the filesystem

### Silver derived-table UDF

```python
def my_udf(silver_dir: str | Path, **kwargs) -> pl.DataFrame
```

- Called after all base tables are written to `silver_dir`
- Must use `openmedallion.storage.read_parquet` and `openmedallion.storage.join` to read inputs — **not** `pl.read_parquet` or `Path /` operations
- Returns a brand-new DataFrame (not a transformed version of an input)

### Gold pre-aggregation UDF

```python
def my_udf(df: pl.DataFrame, silver_dir: str | Path, **kwargs) -> pl.DataFrame
```

- Called before the `group_by` aggregation in a gold aggregation block
- Receives the source DataFrame **and** `silver_dir` for optional joins to other silver tables
- Any column added here can be used in `group_by` or `metrics`

---

## `load_udf`

```python
def load_udf(
    step: dict,
    *,
    cache: dict,
    layer: str,
) -> tuple[Callable, dict]
```

Resolve a UDF config block to a callable and its keyword arguments.

**Parameters:**

| Name | Type | Description |
| --- | --- | --- |
| `step` | `dict` | UDF config dict. Must have `"file"` and `"function"` keys. May have `"args"`. |
| `cache` | `dict` | Mutable dict used as a module cache. Pass `self._udf_cache` from the caller. |
| `layer` | `str` | `"silver"` or `"gold"` — used in error messages only. |

**Returns:** `tuple[Callable, dict]` — `(fn, kwargs)` ready to call as `fn(df, **kwargs)` or `fn(silver_dir, **kwargs)`.

**Raises:**

| Exception | When |
| --- | --- |
| `FileNotFoundError` | `step["file"]` does not exist. Path is relative to the project root (where `medallion` is run). |
| `AttributeError` | `step["function"]` is not defined in the loaded module. Error message lists available names. |

**Module caching:** The module is imported once per pipeline run and stored in `cache` keyed by the resolved absolute path. Subsequent calls for the same file return the cached module without re-importing.

**The `step` dict:**

```python
# From silver.yaml (udf transform type):
step = {
    "file":     "projects/my_project/udf/silver/base.py",
    "function": "enrich_orders",
    "args":     {"threshold": 500.0},
}

# From gold.yaml (pre_agg_udf block):
step = {
    "file":     "projects/my_project/udf/gold/transforms.py",
    "function": "prepare_orders",
    # "args" is optional — defaults to {}
}
```

**Usage in a pipeline class:**

```python
fn, kwargs = load_udf(step, cache=self._udf_cache, layer="silver")
result = fn(df, **kwargs)                      # base UDF
# or:
result = fn(self.silver_path, **kwargs)        # derived UDF
# or:
result = fn(df, self.silver_path, **kwargs)    # gold pre-agg UDF
check_return(result, step["function"], step["file"], layer="silver")
```

---

## `check_return`

```python
def check_return(
    result: Any,
    func_name: str,
    file_path: Any,
    layer: str = "",
) -> None
```

Assert that a UDF returned a `pl.DataFrame`. Called automatically by the pipeline after every UDF invocation.

**Parameters:**

| Name | Type | Description |
| --- | --- | --- |
| `result` | `Any` | The value returned by the UDF. |
| `func_name` | `str` | Name of the UDF function (for the error message). |
| `file_path` | `Any` | Path to the UDF file (for the error message). |
| `layer` | `str` | `"silver"` or `"gold"` prefix for the error message. Optional. |

**Raises:** `TypeError` — if `result` is not a `pl.DataFrame`.

**Error message format:**
```
TypeError: [silver] UDF 'enrich_orders' in projects/my_project/udf/silver/base.py
           must return pl.DataFrame, got DataFrame
```
(where `DataFrame` in the last position would be e.g. `NoneType` or `pandas.core.frame.DataFrame`)

**Usage:**

```python
from openmedallion.contracts.udf import load_udf, check_return

fn, kwargs = load_udf(step, cache=cache, layer="gold")
result = fn(df, silver_dir, **kwargs)
check_return(result, step["function"], step["file"], layer="gold")
```

---

## Writing a Custom Pipeline Stage

If you extend OpenMedallion with a custom stage, use `load_udf` and `check_return` the same way the built-in stages do:

```python
from openmedallion.contracts.udf import load_udf, check_return

class MyCustomStage:
    def __init__(self, cfg: dict):
        self._udf_cache: dict = {}

    def run_udf(self, df, step: dict, layer="custom") -> pl.DataFrame:
        fn, kwargs = load_udf(step, cache=self._udf_cache, layer=layer)
        result = fn(df, **kwargs)
        check_return(result, step["function"], step["file"], layer=layer)
        return result
```
