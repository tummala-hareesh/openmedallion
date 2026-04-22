# YAML Schema Reference

A project is defined by four YAML files. `load_project()` deep-merges them in order (bronze → silver → gold into main) and validates the result before the pipeline runs.

---

## File Layout

```text
projects/<project>/
├── main.yaml      ← pipeline name, paths, bi_export, includes
├── bronze.yaml    ← source connection, destination, incremental
├── silver.yaml    ← bronze_to_silver transforms + derived UDFs
└── gold.yaml      ← silver_to_gold aggregations + pre-agg UDFs
```

---

## main.yaml

```yaml
pipeline:
  name: my_project          # (required) pipeline name — used in dlt and gold output dirs

includes:
  bronze: bronze.yaml       # (required) filename of the bronze config in this folder
  silver: silver.yaml       # (required) filename of the silver config
  gold:   gold.yaml         # (required) filename of the gold config

paths:
  bronze: ./data/bronze     # (required) bronze output directory — local or s3://
  silver: ./data/silver     # (required) silver output directory
  gold:   ./data/gold       # (required) gold output directory
  export: ./data/export     # (required) export output directory

bi_export:                  # (optional) BI export config
  enabled: true
  projects:
    - name: default
      tables:
        - summary.parquet
```

### `pipeline`

| Key | Type | Required | Description |
| --- | --- | --- | --- |
| `name` | string | ✅ | Project/pipeline name. Used as `dataset_name` in dlt and as the subfolder name under `gold/`. |

### `includes`

| Key | Type | Required | Description |
| --- | --- | --- | --- |
| `bronze` | string | ✅ | Filename of the bronze YAML (relative to the project folder). |
| `silver` | string | ✅ | Filename of the silver YAML. |
| `gold` | string | ✅ | Filename of the gold YAML. |

### `paths`

All paths support local directory strings and `s3://bucket/prefix` URIs.

| Key | Type | Required | Description |
| --- | --- | --- | --- |
| `bronze` | string | ✅ | Where `BronzeLoader` writes raw Parquet. |
| `silver` | string | ✅ | Where `SilverTransformer` reads/writes. |
| `gold` | string | ✅ | Root of gold output; actual files go into `gold/<project_name>/`. |
| `export` | string | ✅ | Where `BIExporter` copies files for BI consumption. |

### `bi_export`

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `enabled` | bool | `true` | Skip the export step entirely when `false`. |
| `projects` | list | `[]` | List of BI project specs. |
| `projects[].name` | string | — | Subfolder name under `gold/` to export from. |
| `projects[].tables` | list[string] | — | Filenames to copy + convert to CSV. |

### Environment variable expansion

Any string value in any YAML file may contain `${VAR}` or `${VAR:-default}` placeholders. Expansion happens after all four files are merged.

```yaml
# Raises EnvironmentError if ORACLE_USER is not set
connection_string: "oracle+oracledb://${ORACLE_USER}:${ORACLE_PASSWORD}@${ORACLE_DSN}"

# Falls back to "us-east-1" if AWS_DEFAULT_REGION is not set
bucket_url: "s3://${BUCKET:-my-default-bucket}/data"
```

---

## bronze.yaml

```yaml
source:
  type: sql_database         # sql_database | rest_api | filesystem
  dialect: oracle            # oracle | postgres | mysql | mssql | sqlite
  connection_string: "oracle+oracledb://${USER}:${PASS}@${DSN}"
  schema: MY_SCHEMA          # (optional) database schema
  tables:
    - name: ORDERS
      incremental:
        mode: append         # replace | append | merge
        cursor_column: UPDATED_AT
        initial_value: "2024-01-01T00:00:00"
    - name: CUSTOMERS
      incremental:
        mode: merge
        primary_key: CUSTOMER_ID

destination:
  type: filesystem           # filesystem | duckdb | bigquery | snowflake
  bucket_url: ./data/bronze  # local path or s3:// URI
```

### `source`

| Key | Type | Required | Description |
| --- | --- | --- | --- |
| `type` | enum | ✅ | `sql_database`, `rest_api`, or `filesystem`. |
| `dialect` | string | sql only | SQLAlchemy dialect: `oracle`, `postgres`, `mysql`, `mssql`, `sqlite`. |
| `connection_string` | string | sql only | Full SQLAlchemy connection string. Supports `${VAR}` expansion. |
| `schema` | string | — | Database schema to scope table discovery. |
| `tables` | list | sql/filesystem | Tables to ingest. |
| `base_url` | string | rest only | REST API base URL. |
| `resource` | string | rest only | Resource/endpoint name. |
| `bucket_url` | string | filesystem | Source bucket or directory. |
| `file_glob` | string | filesystem | Glob pattern, e.g. `**/*.parquet`. |
| `format` | string | filesystem | `parquet` (default) or `csv`. |

### `source.tables[]`

| Key | Type | Required | Description |
| --- | --- | --- | --- |
| `name` | string | ✅ | Table name in the source database. |
| `incremental` | object | — | Omit for full-replace each run. |
| `incremental.mode` | enum | — | `replace` (default), `append`, or `merge`. |
| `incremental.cursor_column` | string | append | Column used to track the high-watermark. |
| `incremental.initial_value` | string | append | Value to use on the very first run. |
| `incremental.primary_key` | string | merge | Primary key column for upsert. |

### `destination`

| Key | Type | Required | Description |
| --- | --- | --- | --- |
| `type` | enum | ✅ | `filesystem`, `duckdb`, `bigquery`, or `snowflake`. |
| `bucket_url` | string | filesystem | Local path or `s3://` URI. Should equal `paths.bronze`. |
| `db_path` | string | duckdb | Path to the `.duckdb` file (default: `bronze.duckdb`). |

---

## silver.yaml

```yaml
bronze_to_silver:
  tables:
    - source_file: ORDERS.parquet     # filename in bronze directory
      output_file: orders.parquet     # filename to write in silver directory
      transforms:
        - type: rename
          columns:
            ORDER_ID:    order_id
            UPDATED_AT:  updated_at
        - type: cast
          columns:
            order_id: Int64
            amount:   Float64
        - type: drop
          columns: [INTERNAL_COL, DEBUG_COL]
        - type: udf
          file: projects/my_project/udf/silver/base.py
          function: enrich_orders
          args:
            threshold: 500.0

  derived_tables:
    - output_file: order_lines_enriched.parquet
      udf:
        file: projects/my_project/udf/silver/derived.py
        function: build_enriched
      select: [order_id, product_id, line_revenue]   # optional column projection
```

### `bronze_to_silver.tables[]`

| Key | Type | Required | Description |
| --- | --- | --- | --- |
| `source_file` | string | ✅ | Parquet filename to read from `paths.bronze`. |
| `output_file` | string | ✅ | Parquet filename to write to `paths.silver`. |
| `transforms` | list | — | Ordered list of transform steps. |

### `transforms[]`

Each entry must have a `type` field. Valid types: `rename`, `cast`, `drop`, `udf`.

=== "rename"
    ```yaml
    - type: rename
      columns:
        OLD_NAME: new_name
        ANOTHER:  another_name
    ```
    Renames columns using Polars `.rename()`. Keys are original names, values are new names.

=== "cast"
    ```yaml
    - type: cast
      columns:
        order_id:  Int64
        amount:    Float64
        is_active: Boolean
    ```
    Casts columns to Polars dtypes. Use Polars dtype names: `Int64`, `Float64`, `Utf8`, `Boolean`, `Date`, `Datetime`, etc.

=== "drop"
    ```yaml
    - type: drop
      columns: [INTERNAL_COL, DEBUG_COL]
    ```
    Drops named columns. No-op if the list is empty.

=== "udf"
    ```yaml
    - type: udf
      file: projects/my_project/udf/silver/base.py
      function: enrich_orders
      args:
        threshold: 500.0
    ```
    Calls a Python function. See [Silver base-table UDF](../concepts/udf-guide.md#1-silver-base-table-udf).

    | Key | Required | Description |
    | --- | --- | --- |
    | `file` | ✅ | Path to the Python file, relative to the project root (where `medallion` is run). |
    | `function` | ✅ | Name of the function to call. |
    | `args` | — | Dict of keyword arguments passed to the function. |

### `bronze_to_silver.derived_tables[]`

| Key | Type | Required | Description |
| --- | --- | --- | --- |
| `output_file` | string | ✅ | Filename to write to `paths.silver`. |
| `udf` | object | ✅ | UDF spec: `file`, `function`, optional `args`. |
| `select` | list[string] | — | If present, only these columns are kept from the UDF result. |

---

## gold.yaml

```yaml
silver_to_gold:
  projects:
    - name: analytics                     # subfolder under paths.gold
      aggregations:
        - source_file: orders.parquet     # silver filename to read
          pre_agg_udf:                    # (optional) runs before group_by
            file: projects/my_project/udf/gold/transforms.py
            function: prepare_orders
            args:
              include_region: true
          group_by: [customer_id, region] # columns to group on
          metrics:
            - {column: order_id, agg: count, alias: total_orders}
            - {column: amount,   agg: sum,   alias: total_revenue}
            - {column: amount,   agg: mean,  alias: avg_order_value}
          output_file: customer_summary.parquet

        - source_file: orders.parquet     # pass-through (no group_by)
          select: [order_id, amount, status]
          output_file: orders_flat.parquet
```

### `silver_to_gold.projects[]`

| Key | Type | Required | Description |
| --- | --- | --- | --- |
| `name` | string | ✅ | Subfolder name under `paths.gold`. Also used for BI export. |
| `aggregations` | list | ✅ | List of aggregation blocks. |

### `aggregations[]`

| Key | Type | Required | Description |
| --- | --- | --- | --- |
| `source_file` | string | ✅ | Parquet filename to read from `paths.silver`. |
| `pre_agg_udf` | object | — | UDF to run before `group_by`. See [Gold pre-agg UDF](../concepts/udf-guide.md#3-gold-pre-aggregation-udf). |
| `group_by` | list[string] | — | Column names to group by. Omit for grand-total aggregation. |
| `metrics` | list | — | Metric specs. Required unless `select` is used. |
| `select` | list[string] | — | Pass-through mode: select columns without aggregating. |
| `output_file` | string | ✅ | Parquet filename to write under `paths.gold/<project>/`. |

### `metrics[]`

| Key | Type | Required | Description |
| --- | --- | --- | --- |
| `column` | string | ✅ (except `count`) | Source column name. For `count`, any column works. |
| `agg` | enum | ✅ | `count`, `sum`, `mean`, `min`, or `max`. |
| `alias` | string | ✅ | Output column name in the result. |

### `pre_agg_udf`

| Key | Type | Required | Description |
| --- | --- | --- | --- |
| `file` | string | ✅ | Path to the Python file, relative to project root. |
| `function` | string | ✅ | Function name. |
| `args` | dict | — | Keyword arguments forwarded to the function. |

---

## Validation Rules

`_validate_config()` checks the following automatically after loading:

| Rule | Error |
| --- | --- |
| `pipeline.name` must be a non-empty string | `pipeline.name must be a non-empty string` |
| All four `paths.*` keys must be present | `paths.bronze is required` |
| `source.type` (if present) must be in `{sql_database, rest_api, filesystem}` | `source.type must be one of ...` |
| `bronze_to_silver.tables[i].source_file` and `.output_file` required | `bronze_to_silver.tables[0].source_file is required` |
| Each `transforms[j].type` must be `rename/cast/drop/udf` | `transforms[0].type must be one of ...` |
| UDF transform blocks must have `file` and `function` | `transforms[0] (udf): 'file' is required` |
| `silver_to_gold.projects[i].name` required | `silver_to_gold.projects[0].name is required` |
| `pre_agg_udf` blocks must have `file` and `function` | `pre_agg_udf: 'file' is required` |
