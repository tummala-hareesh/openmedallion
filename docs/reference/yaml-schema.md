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
| `dialect` | enum | creds file | Required when `credentials_file` is used. One of `oracle`, `postgres`, `mysql`, `mssql`, `sqlite`. |
| `credentials_file` | string | — | Path to a credentials YAML (see `secrets.yaml.example`). Preferred over `connection_string`. Supports `${VAR}` expansion. |
| `connection_string` | string | — | Raw SQLAlchemy URL. Supports `${VAR}` expansion. Used only when `credentials_file` is absent. |
| `schema` | string | — | Database schema to scope table discovery and `SELECT` statements. |
| `tables` | list | sql/filesystem | Tables to ingest. |
| `base_url` | string | rest only | REST API base URL. |
| `resource` | string | rest only | Resource/endpoint name. |
| `bucket_url` | string | filesystem | Source bucket or directory. |
| `file_glob` | string | filesystem | Glob pattern, e.g. `**/*.parquet`. |
| `format` | string | filesystem | `parquet` (default) or `csv`. |

#### Credential file format (`credentials_file`)

openmedallion reads the YAML file, looks up the `dialect` key, and builds the
SQLAlchemy connection string internally.  Connection is tested and available
tables are printed before ingestion starts.

```yaml
# /workspace/secrets.yaml  (keep outside repo — never commit real credentials)

oracle:
  host:     db.corp.com
  port:     1521          # default
  service:  XE
  username: hr
  password: secret

postgres:
  host:     pg.corp.com
  port:     5432          # default
  database: analytics
  username: etl
  password: secret

sqlite:
  path: data/mydb.db      # relative to CWD where medallion runs
```

Set the path and dialect via env vars (supports `${VAR:-default}` expansion):

```yaml
# bronze.yaml
source:
  type:             sql_database
  dialect:          "${SECRETS_DIALECT:-sqlite}"
  credentials_file: "${SECRETS_PATH:-dev_credentials.yaml}"
  schema:           HR
```

### `source.tables[]`

| Key | Type | Required | Description |
| --- | --- | --- | --- |
| `name` | string | ✅ | Table name in the source database. |
| `select` | list[string] | — | Column names to ingest. Omit to ingest all columns. For SQL sources the projection is pushed to the database; for local_files it is applied after reading. Always include the `cursor_column` when using `append` mode. |
| `incremental` | object | — | Omit for full-replace each run. |
| `incremental.mode` | enum | — | `replace` (default), `append`, or `merge`. |
| `incremental.cursor_column` | string | append | Column used to track the high-watermark. Always include this column in `select`. |
| `incremental.initial_value` | string | append | Value to use on the very first run. |
| `incremental.primary_key` | string or list | merge | Column(s) that uniquely identify a row in the Parquet file (used to build the file index). |
| `incremental.merge_key` | string or list | merge | Column(s) used to match incoming rows against existing rows. Usually identical to `primary_key`. |
| `explore` | list | — | List of report specs to generate after this table is written. See [Inline explore](#inline-explore). |

#### Incremental modes

| Mode | When to use | Required keys |
| --- | --- | --- |
| `replace` | Small lookup/reference tables. Full re-pull and overwrite every run. Safe when source rows can be deleted. | — |
| `append` | Immutable event or log tables. Fetches only rows newer than the cursor and appends to the existing Parquet. No deduplication. | `cursor_column`, `initial_value` |
| `merge` | Tables where rows can be updated after their first write (e.g. a permit whose status changes over time). Upserts by `merge_key` — updates matching rows, inserts new ones. | `primary_key`, `merge_key` |

`merge` and `cursor_column` can be combined: the cursor limits how many rows are fetched from the source database, while `merge_key` controls how they land in the Parquet file. This is the right pattern for large tables where only recent rows are added but existing rows may also change.

```yaml
# Large table — limit source pull by cursor, upsert by primary key
- name: folder
  incremental:
    mode: merge
    primary_key: folderrsn
    merge_key: folderrsn
    cursor_column: indate
    initial_value: "2025-01-01 00:00:00"

# Lookup table — full re-pull every run, upsert to avoid duplicates across runs
- name: validstatus
  incremental:
    mode: merge
    primary_key: statuscode
    merge_key: statuscode

# Composite key — child records keyed on two columns
- name: folderprocess
  incremental:
    mode: merge
    primary_key: [folderrsn, processrsn]
    merge_key: [folderrsn, processrsn]

# Append-only event log
- name: audit_log
  incremental:
    mode: append
    cursor_column: created_at
    initial_value: "2024-01-01T00:00:00"
```

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

  # Optional: register all silver Parquet files as DuckDB views or tables
  # after the silver run completes.
  duckdb:
    enabled: true                        # false or omit to skip (default: false)
    path: data/silver/silver.duckdb      # where to write the .duckdb file
    mode: views                          # views (default) | tables

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

### `bronze_to_silver.duckdb`

Optional block. When `enabled: true`, all `*.parquet` files in `paths.silver` are registered in a DuckDB file after the silver run completes.

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `enabled` | bool | `false` | Set to `true` to activate DuckDB registration. |
| `path` | string | — | Path to write the `.duckdb` file. Created automatically if absent. |
| `mode` | enum | `views` | `views` — lightweight pointer, data stays in Parquet, **not** standalone-shareable. `tables` — data copied into DuckDB, file is self-contained and shareable. |

**When to use each mode:**

| Mode | File size | Shareable alone | Best for |
| --- | --- | --- | --- |
| `views` | Tiny | ❌ (needs Parquet files) | Local use, `cerebrum` queries, notebooks |
| `tables` | ≈ Parquet total | ✅ | Sharing with colleagues, attaching to reports |

### `bronze_to_silver.tables[]`

| Key | Type | Required | Description |
| --- | --- | --- | --- |
| `source_file` | string | ✅ | Parquet filename to read from `paths.bronze`. |
| `output_file` | string | ✅ | Parquet filename to write to `paths.silver`. |
| `transforms` | list | — | Ordered list of transform steps. |
| `explore` | list | — | List of report specs to generate after this table is written. See [Inline explore](#inline-explore). |

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

  # Optional: register all gold Parquet files as DuckDB views or tables
  # after the gold run completes. All projects are registered into one file.
  duckdb:
    enabled: true                      # false or omit to skip (default: false)
    path: data/gold/gold.duckdb        # where to write the .duckdb file
    mode: tables                       # tables recommended for gold (shareable)

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

### `silver_to_gold.duckdb`

Optional block. Same semantics as [`bronze_to_silver.duckdb`](#bronze_to_silverduckdb). When `enabled: true`, Parquet files from **all** gold project subdirectories are registered into a single `.duckdb` file after the gold run completes.

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `enabled` | bool | `false` | Set to `true` to activate DuckDB registration. |
| `path` | string | — | Path to write the `.duckdb` file. |
| `mode` | enum | `views` | `views` or `tables`. Recommend `tables` for gold (shareable). |

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
| `explore` | list | — | List of report specs to generate after this aggregation is written. See [Inline explore](#inline-explore). |

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

## Inline Explore

Add an `explore:` list directly to any table entry in `bronze.yaml`, `silver.yaml`, or `gold.yaml`. Reports are generated immediately after the Parquet file for that table is written — no separate `explore.yaml` or `paths.explore` key needed.

```yaml
# bronze.yaml — on a source table
- name: employees
  explore:
    - report_type: profile
      output_file:  employees_profile.html
      title:        "Employees Bronze Quality"

# silver.yaml — on a transformed table
- source_file: employees.parquet
  explore:
    - report_type: walker
      output_file:  employees_explorer.html
      title:        "Employees Explorer"

# gold.yaml — on an aggregation
aggregations:
  - output_file: headcount.parquet
    explore:
      - report_type: profile
        output_file:  headcount_profile.html
        title:        "Headcount by Department"
```

### `explore[]`

| Key | Type | Required | Description |
| --- | --- | --- | --- |
| `report_type` | enum | ✅ | `profile` (ydata-profiling HTML report) or `walker` (pygwalker interactive explorer). |
| `output_file` | string | ✅ | HTML filename to write. |
| `title` | string | — | Optional title shown in the report. |

**Report output paths** — reports are co-located with their layer's data under an `add-ons/` subdirectory:

| Layer | Output path |
| --- | --- |
| Bronze | `paths.bronze/add-ons/<output_file>` |
| Silver | `paths.silver/add-ons/<output_file>` |
| Gold | `paths.gold/add-ons/<project>/<output_file>` |

**Optional dependencies** — install the extras for the report type you want:

```bash
pip install "openmedallion[profile]"   # ydata-profiling >= 4.0
pip install "openmedallion[explore]"   # pygwalker >= 0.4
```

If the optional dependency is not installed, the report is silently skipped with a notice. An unknown `report_type` also prints a warning and skips without aborting the run.

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
| `bronze_to_silver.duckdb.enabled` must be a bool | `bronze_to_silver.duckdb.enabled must be a bool` |
| `bronze_to_silver.duckdb.mode` must be `views` or `tables` | `bronze_to_silver.duckdb.mode must be one of ...` |
| `silver_to_gold.duckdb.enabled` must be a bool | `silver_to_gold.duckdb.enabled must be a bool` |
| `silver_to_gold.duckdb.mode` must be `views` or `tables` | `silver_to_gold.duckdb.mode must be one of ...` |
