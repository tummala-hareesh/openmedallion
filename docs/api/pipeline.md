# Pipeline API

The four pipeline engine classes. In normal use you do not instantiate these directly — the Hamilton DAG nodes in `pipeline/nodes.py` do it for you. They are documented here for advanced use cases and testing.

```python
from openmedallion.pipeline.bronze import BronzeLoader
from openmedallion.pipeline.silver import SilverTransformer
from openmedallion.pipeline.gold   import GoldAggregator
from openmedallion.pipeline.export import BIExporter
```

---

## `BronzeLoader`

Ingests source data via dlt into raw Parquet files in the bronze layer.

```python
class BronzeLoader:
    def __init__(self, cfg: dict) -> None
    def load(self) -> dict[str, str]
```

**Constructor parameters:**

| Name | Type | Description |
| --- | --- | --- |
| `cfg` | `dict` | Merged project config. Must contain `source`, `destination`, `pipeline.name`, and `paths.bronze`. |

### `load()`

Run the dlt pipeline and return a mapping of table name → Parquet path for every loaded table.

**Returns:** `dict[str, str]` — `{table_name: "/path/to/TABLE.parquet", ...}`

**How it works:**

1. Builds a dlt `Pipeline` object from `destination` config
2. Builds dlt source resources from `source` config (SQL tables, REST endpoint, or filesystem)
3. Runs `pipeline.run(sources)`
4. Calls `_collect_parquets()` — merges dlt shards into one Parquet per table

**Supported sources (`source.type`):**

| Value | Driver | Notes |
| --- | --- | --- |
| `sql_database` | `dlt.sources.sql_database.sql_table` | Oracle, Postgres, MySQL, MSSQL, SQLite |
| `rest_api` | `dlt.sources.rest_api.rest_api_source` | Pagination, auth, cursors built in |
| `filesystem` | `dlt.sources.filesystem` | Local or cloud Parquet/CSV files |

**Supported destinations (`destination.type`):**

| Value | Notes |
| --- | --- |
| `filesystem` | Local path or `s3://` — writes Parquet shards |
| `duckdb` | Local `.duckdb` file |
| `bigquery` | Google BigQuery |
| `snowflake` | Snowflake |

**Incremental modes:**

| Mode | Config key | Behaviour |
| --- | --- | --- |
| `replace` | (default) | Full overwrite each run |
| `append` | `cursor_column`, `initial_value` | Adds only rows newer than the cursor |
| `merge` | `primary_key` | Full upsert — new rows insert, existing rows update |

**Example:**

```python
cfg = load_project("sales_project")
loader = BronzeLoader(cfg)
paths = loader.load()
# paths == {"ORDERS": "./data/bronze/ORDERS.parquet", ...}
```

---

## `SilverTransformer`

Applies structural transforms and UDFs to bronze Parquet, writing silver Parquet.

```python
class SilverTransformer:
    def __init__(self, cfg: dict) -> None
    def transform(self) -> dict[str, str]
```

**Constructor parameters:**

| Name | Type | Description |
| --- | --- | --- |
| `cfg` | `dict` | Merged project config. Reads `paths.bronze`, `paths.silver`, and `bronze_to_silver`. |

### `transform()`

Run both silver phases and return paths for all written files.

**Returns:** `dict[str, str]` — `{output_filename: "/path/to/file.parquet", ...}`

**Phase 1 — base tables:**

For each entry in `bronze_to_silver.tables`:

1. Read `source_file` from `paths.bronze`
2. Apply each transform in order: `rename` → `cast` → `drop` → `udf`
3. Write `output_file` to `paths.silver`

Missing bronze files produce a warning and are skipped — they do not cause a failure.

**Phase 2 — derived tables:**

For each entry in `bronze_to_silver.derived_tables`:

1. Call the UDF with `silver_dir` — the UDF loads its own inputs and returns a new DataFrame
2. Optionally apply a `select` projection
3. Write `output_file` to `paths.silver`

**Transform types** (applied in declaration order):

| Type | Operation |
| --- | --- |
| `rename` | `df.rename(columns_dict)` |
| `cast` | `df.with_columns([pl.col(c).cast(pl.DType) ...])` |
| `drop` | `df.drop(columns_list)` |
| `udf` | Calls `fn(df, **kwargs)` — see [Silver base UDF](../concepts/udf-guide.md#1-silver-base-table-udf) |

**Example:**

```python
cfg = load_project("sales_project")
transformer = SilverTransformer(cfg)
paths = transformer.transform()
# paths == {"orders.parquet": "./data/silver/orders.parquet", ...}
```

---

## `GoldAggregator`

Aggregates silver Parquet into per-BI-project gold Parquet.

```python
class GoldAggregator:
    def __init__(self, cfg: dict) -> None
    def aggregate(self) -> dict[str, list[str]]
```

**Constructor parameters:**

| Name | Type | Description |
| --- | --- | --- |
| `cfg` | `dict` | Merged project config. Reads `paths.silver`, `paths.gold`, and `silver_to_gold`. |

### `aggregate()`

Run all gold aggregations and return the paths written.

**Returns:** `dict[str, list[str]]` — `{project_name: ["/path/to/output.parquet", ...], ...}`

**For each aggregation block:**

1. Read `source_file` from `paths.silver`
2. Optionally run `pre_agg_udf` — see [Gold pre-agg UDF](../concepts/udf-guide.md#3-gold-pre-aggregation-udf)
3. Apply `group_by` + `metrics` aggregation (or `select` for pass-through)
4. Write result to `paths.gold/<project_name>/<output_file>`

**Supported aggregation functions:**

| `agg` value | Polars expression |
| --- | --- |
| `count` | `pl.len()` |
| `sum` | `pl.col(c).sum()` |
| `mean` | `pl.col(c).mean()` |
| `min` | `pl.col(c).min()` |
| `max` | `pl.col(c).max()` |

**Grand-total aggregation** — omit `group_by` (or pass an empty list) to aggregate all rows into a single row:

```yaml
- source_file: orders.parquet
  group_by: []
  metrics:
    - {column: order_id, agg: count, alias: total_orders}
    - {column: amount,   agg: sum,   alias: grand_total}
  output_file: totals.parquet
```

**Example:**

```python
cfg = load_project("sales_project")
agg = GoldAggregator(cfg)
results = agg.aggregate()
# results == {"analytics": ["./data/gold/analytics/customer_summary.parquet", ...]}
```

---

## `BIExporter`

Copies gold Parquet files to the export directory and writes CSV versions for BI tools.

```python
class BIExporter:
    def __init__(self, cfg: dict) -> None
    def export(self) -> None
```

**Constructor parameters:**

| Name | Type | Description |
| --- | --- | --- |
| `cfg` | `dict` | Merged project config. Reads `paths.gold`, `paths.export`, and `bi_export`. |

### `export()`

Copy all configured tables from gold to export, writing both `.parquet` and `.csv` versions.

**Returns:** `None`

**Behaviour:**

- If `bi_export.enabled` is `false`, the method returns immediately without doing anything
- For each table in each BI project: copies the Parquet and writes a CSV at the same path with `.csv` extension
- Missing source files produce a warning and are skipped

**Example:**

```python
cfg = load_project("sales_project")
exporter = BIExporter(cfg)
exporter.export()
# 📤  [export/default] parquet → ./data/export/default/summary.parquet
# 📄  [export/default] csv    → ./data/export/default/summary.csv
```

---

## Hamilton Nodes

The Hamilton DAG nodes in `pipeline/nodes.py` wire the four classes together:

```python
def config(cfg: dict) -> dict:
    """Pass the raw merged config dict into the DAG."""
    return cfg

def bronze(config: dict) -> dict[str, Path]:
    """Ingest source tables via dlt → raw Parquet in the bronze layer."""
    return BronzeLoader(config).load()

def silver(config: dict, bronze: dict[str, Path]) -> dict[str, Path]:
    """Apply structural transforms and UDFs → silver Parquet."""
    return SilverTransformer(config).transform()

def gold(config: dict, silver: dict[str, Path]) -> dict[str, list[Path]]:
    """Aggregate silver tables → per-BI-project gold Parquet."""
    return GoldAggregator(config).aggregate()

def bi_export(config: dict, gold: dict[str, list[Path]]) -> None:
    """Copy gold Parquet to the export directory; write CSV fallbacks."""
    BIExporter(config).export()
```

Hamilton infers execution order from argument names — `silver(config, bronze)` automatically depends on both the `config` and `bronze` nodes.
