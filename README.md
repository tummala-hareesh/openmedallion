# openmedallion

Declarative medallion pipelines in pure open-source Python — local first, cloud portable, fast by default.

## Install

```bash
pip install openmedallion
```

## Quickstart

```bash
# 1. Create a new project
medallion init my_project

# 2. Run the full pipeline
medallion run my_project

# 3. Inspect the Hamilton DAG
medallion dag

# 4. Launch the live tracker UI
medallion serve
```

## Architecture

| Layer | Tool | Role |
|-------|------|------|
| Ingestion (Bronze) | dlt | Schema-inferred raw load from any source |
| Transform (Silver) | Polars | Typed, composable Python UDFs |
| Aggregate (Gold) | Polars | YAML-declared group-by metrics |
| Export | Polars | Parquet + CSV export to BI tools |
| Orchestration | Hamilton | DAG wiring with live web tracker |

## When to use openmedallion

Use openmedallion when you want a **convention-over-configuration** data pipeline that:
- Runs locally without cloud credentials for development
- Scales to S3 with a one-line config change
- Keeps business logic in plain Python UDFs, not a proprietary DSL
- Provides a live DAG visualiser out of the box

## Links

- [examples/local_parquet_demo](examples/local_parquet_demo/) — zero-credential quickstart
- [examples/incremental_sql_demo](examples/incremental_sql_demo/) — incremental append + merge with SQLite
