"""scaffold/templates.py — scaffold a new project folder with config, UDF stubs, and docs.

Creates the full project structure under <project>/ in the current directory:

    <project>/
    ├── main.yaml                  ← pipeline entry point (required by load_project)
    ├── backend/
    │   ├── bronze.yaml            ← source connection, destination, tables, incremental
    │   ├── silver.yaml            ← bronze_to_silver transforms + derived table UDFs
    │   ├── gold.yaml              ← silver_to_gold aggregations + pre-agg UDFs
    │   └── udf/
    │       ├── silver/
    │       │   ├── base.py        ← per-table silver UDF stub
    │       │   └── derived.py     ← derived silver table UDF stub
    │       └── gold/
    │           └── transforms.py  ← gold pre-aggregation UDF stub
    ├── frontend/                  ← BI/dashboard artefacts (Tableau, Power BI, etc.)
    ├── data/                      ← gitignored pipeline outputs (bronze/silver/gold/export)
    ├── summary/
    │   └── summary.md             ← analysis summary + dashboard narrative
    ├── kestra_flow.yml            ← Kestra orchestration flow (mount via docker-compose.yml)
    └── README.md                  ← project documentation template
"""
import os
import yaml
from pathlib import Path


def _main_template(project: str) -> dict:
    return {
        "pipeline": {"name": project},
        "includes": {
            "bronze": "backend/bronze.yaml",
            "silver": "backend/silver.yaml",
            "gold":   "backend/gold.yaml",
        },
        "paths": {
            "bronze": f"{project}/data/bronze",
            "silver": f"{project}/data/silver",
            "gold":   f"{project}/data/gold",
            "export": f"{project}/data/export",
        },
        "bi_export": {
            "enabled": True,
            "projects": [{"name": "default", "tables": ["summary.parquet"]}],
        },
    }


def _bronze_template() -> dict:
    return {
        "source": {
            "type": "sql_database", "dialect": "oracle",
            "connection_string": "oracle+oracledb://${ORACLE_USER}:${ORACLE_PASSWORD}@${ORACLE_DSN}",
            "schema": "MY_SCHEMA",
            "tables": [{"name": "MY_TABLE", "incremental": {
                "mode": "append", "cursor_column": "UPDATED_AT",
                "initial_value": "2024-01-01T00:00:00"}}],
        },
        "destination": {"type": "filesystem", "bucket_url": "./data/bronze"},
    }


def _silver_template(project: str) -> dict:
    return {
        "bronze_to_silver": {
            "tables": [{"source_file": "MY_TABLE.parquet", "output_file": "my_table.parquet",
                "transforms": [
                    {"type": "rename", "columns": {"MY_ID": "id", "UPDATED_AT": "updated_at"}},
                    {"type": "cast",   "columns": {"id": "Int64"}},
                    {"type": "udf",
                     "file": f"{project}/backend/udf/silver/base.py",
                     "function": "enrich_my_table"},
                ]}],
            "derived_tables": [{"output_file": "my_derived_table.parquet",
                "udf": {"file": f"{project}/backend/udf/silver/derived.py",
                        "function": "build_my_derived_table"}}],
        },
    }


def _gold_template(project: str) -> dict:
    return {
        "silver_to_gold": {"projects": [{"name": "default", "aggregations": [{
            "source_file": "my_table.parquet",
            "pre_agg_udf": {"file": f"{project}/backend/udf/gold/transforms.py",
                            "function": "prepare_my_table"},
            "group_by": ["id"],
            "metrics": [{"column": "id", "agg": "count", "alias": "total_records"}],
            "output_file": "summary.parquet",
        }]}]},
    }


def _udf_base(project: str) -> str:
    return f'''"""{project}/backend/udf/silver/base.py
Silver UDF — add columns, filter rows, reshape a single table.
Signature: (df: pl.DataFrame, **kwargs) -> pl.DataFrame
"""
import polars as pl
from datetime import date


def enrich_my_table(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(pl.lit(str(date.today())).alias("ingested_at"))
'''


def _udf_derived(project: str) -> str:
    return f'''"""{project}/backend/udf/silver/derived.py
Silver derived UDF — build a new table by joining silver tables.
Contract: (silver_dir: str | Path, **kwargs) -> pl.DataFrame

Use openmedallion.storage helpers (read_parquet, join) — not pl.read_parquet or Path /
— so the UDF works with both local paths and s3:// URIs.
"""
import polars as pl
from openmedallion.helpers.joins import lookup_join
from openmedallion.storage       import read_parquet, join


def build_my_derived_table(silver_dir) -> pl.DataFrame:
    left  = read_parquet(join(silver_dir, "my_table.parquet"))
    right = read_parquet(join(silver_dir, "other_table.parquet"))
    return lookup_join(left, right, key="id", cols=["name"], how="left")
'''


def _udf_gold(project: str) -> str:
    return f'''"""{project}/backend/udf/gold/transforms.py
Gold pre-aggregation UDF — joins, filtering before group_by.
Contract: (df: pl.DataFrame, silver_dir: str | Path, **kwargs) -> pl.DataFrame

Use openmedallion.storage helpers (read_parquet, join, exists) — not pl.read_parquet or
Path / — so the UDF works with both local paths and s3:// URIs.
"""
import polars as pl
from openmedallion.helpers.joins   import safe_join
from openmedallion.helpers.windows import rank_within
from openmedallion.storage         import exists, read_parquet, join


def prepare_my_table(df: pl.DataFrame, silver_dir) -> pl.DataFrame:
    lookup_path = join(silver_dir, "lookup_table.parquet")
    if exists(lookup_path):
        lookup = read_parquet(lookup_path)
        df = safe_join(df, lookup.select(["id", "category"]),
                       on="id", how="left", fill={{"category": "unknown"}})
    return rank_within(df, partition_by="category", order_by="id", alias="rank")
'''


def _readme_template(project: str) -> str:
    return f"""# {project}

## Overview

<!-- 1-2 sentences describing what this project analyses and its business purpose. -->

**Data Source:** <!-- e.g. Oracle ERP, REST API, CSV exports -->
**Refresh Frequency:** <!-- e.g. Daily at 06:00 UTC -->
**Primary Stakeholders:** <!-- see Stakeholders section -->

---

## Environment Setup

### Prerequisites

- Python 3.11+
- [`uv`](https://github.com/astral-sh/uv) package manager
- Access to source database / API (see `.env.example`)

### Steps

```bash
# 1. Clone and enter the repo
git clone <repo-url>
cd <repo-root>

# 2. Install dependencies
uv sync

# 3. Copy and fill in credentials
cp .env.example .env
# Edit .env — add ORACLE_USER, ORACLE_PASSWORD, ORACLE_DSN (or equivalent)

# 4. Verify setup
medallion --help
```

---

## Folder Structure

```
{project}/
├── main.yaml          ← pipeline entry point
├── backend/
│   ├── bronze.yaml    ← source config (connection, tables, incremental mode)
│   ├── silver.yaml    ← transform config (renames, casts, UDFs)
│   ├── gold.yaml      ← aggregation config (group_by, metrics)
│   └── udf/           ← custom Python transform functions
│       ├── silver/
│       └── gold/
├── frontend/          ← dashboard files (.pbix, .twb, etc.)
├── data/              ← pipeline outputs — gitignored
├── summary/
│   └── summary.md     ← analysis narrative
└── README.md          ← this file
```

---

## Design Layout — ETL Pipeline

```
[Source DB / API]
       │
       ▼  bronze  (dlt)
[Raw Parquet — {project}/data/bronze/]
       │
       ▼  silver  (Polars + UDFs)
[Cleaned Parquet — {project}/data/silver/]
       │
       ▼  gold  (Polars group_by + UDFs)
[Aggregated Parquet — {project}/data/gold/]
       │
       ▼  export
[CSV + Parquet — {project}/data/export/]  →  [BI Tool]
```

---

## How to Refresh Backend Data

Run the full pipeline from the repo root:

```bash
medallion run {project}
```

Run individual layers:

```bash
medallion run {project} --layer bronze   # ingest only
medallion run {project} --layer silver   # transform only
medallion run {project} --layer gold     # aggregate only
```

Inspect the Hamilton DAG before running:

```bash
medallion dag {project}
```

---

## How to Update the Frontend

<!-- Choose whichever applies and delete the rest. -->

**Power BI:** Open `frontend/{project}.pbix` → Home → Refresh → Publish to service.

**Tableau:** Open `frontend/{project}.twb` → Data → Refresh All Extracts → Publish to Tableau Server.

**Automated refresh:** <!-- If the BI tool is connected live, no manual step is needed. Describe the scheduled refresh here. -->

---

## Snapshot of Dashboard Views

<!-- Insert screenshots here.
Example:
![Overview Page](summary/screenshots/overview.png)
![Trend Page](summary/screenshots/trends.png)
-->

*Screenshots pending first dashboard publish.*

---

## Additional References

| Resource | Link |
|---|---|
| Source system docs | <!-- URL --> |
| Data warehouse schema | <!-- URL --> |
| openmedallion docs | https://github.com/tummala-hareesh/openmedallion |
| Project ticket / epic | <!-- URL --> |

---

## Developers

| Name | Role | Contact |
|---|---|---|
| <!-- Name --> | <!-- Lead / Contributor --> | <!-- email --> |

---

## Stakeholders

| Name | Department | Notes |
|---|---|---|
| <!-- Name --> | <!-- Dept --> | <!-- e.g. approves requirements --> |
"""


def _kestra_flow_template(project: str) -> str:
    return f"""\
id: {project}
namespace: openmedallion.projects
description: "Bronze → Silver → Gold pipeline for {project}."

tasks:

  - id: run_bronze
    type: io.kestra.plugin.scripts.python.Commands
    warningOnStdErr: false
    commands:
      - medallion run {project} --layer bronze
    workingDirectory: /app/{project}

  - id: run_silver
    type: io.kestra.plugin.scripts.python.Commands
    warningOnStdErr: false
    commands:
      - medallion run {project} --layer silver
    workingDirectory: /app/{project}
    dependsOn:
      - run_bronze

  - id: run_gold
    type: io.kestra.plugin.scripts.python.Commands
    warningOnStdErr: false
    commands:
      - medallion run {project} --layer gold
    workingDirectory: /app/{project}
    dependsOn:
      - run_silver

# Uncomment to enable a scheduled refresh:
# triggers:
#   - id: daily_refresh
#     type: io.kestra.plugin.core.trigger.Schedule
#     cron: "0 6 * * *"   # every day at 06:00 UTC
"""


def _summary_template(project: str) -> str:
    return f"""# {project} — Analysis Summary

## Data Summary

<!-- 1-2 paragraphs describing the dataset: what it contains, its time range,
     row counts, key dimensions, and any data quality notes. -->

The `{project}` dataset covers ... [time range] ... containing approximately
[N] records across [M] tables sourced from [system name].

Key dimensions include: ...

Notable data quality observations: ...

---

## Dashboard Summary

<!-- 1-2 paragraphs describing the dashboard: what questions it answers,
     which metrics are highlighted, and how stakeholders use it. -->

The dashboard provides ... [business teams] ... with visibility into ...
The primary metrics tracked are ...

Stakeholders use this dashboard to ...

---

## Key Findings

<!-- Bullet list of the top 3-5 insights from the latest analysis run. -->

- ...
- ...
- ...

---

## Open Questions / Next Steps

<!-- Items requiring follow-up or future analysis. -->

- [ ] ...
"""


def init_project(
    project: str,
    projects_root: str | Path = ".",
) -> None:
    """Scaffold a new project directory with full structure.

    Creates <project>/ under ``projects_root`` (defaults to the current directory):

      <project>/main.yaml
      <project>/backend/{bronze,silver,gold}.yaml + udf stubs
      <project>/frontend/
      <project>/data/          (gitignored)
      <project>/summary/       (summary.md)
      <project>/kestra_flow.yml
      <project>/README.md

    Args:
        project: Name of the new project — used as the folder name and in
            generated config templates.
        projects_root: Parent directory where the project folder is created.
            Defaults to ``"."`` (current working directory).
    """
    project_dir = Path(projects_root) / project
    backend_dir = project_dir / "backend"
    udf_silver  = backend_dir / "udf" / "silver"
    udf_gold    = backend_dir / "udf" / "gold"

    if project_dir.exists():
        print(f"❌  [init] '{project}' already exists — aborting.")
        print(f"    delete {project_dir}/ to reinitialise.")
        return

    # --- main.yaml at project root (required by load_project) ---
    project_dir.mkdir(parents=True)
    main_path = project_dir / "main.yaml"
    with open(main_path, "w") as f:
        yaml.dump(_main_template(project), f, default_flow_style=False, sort_keys=False)
    print(f"🏗️   [init] created {main_path}")

    # --- backend/ layer YAMLs ---
    backend_dir.mkdir(parents=True)
    for path, data in [
        (backend_dir / "bronze.yaml", _bronze_template()),
        (backend_dir / "silver.yaml", _silver_template(project)),
        (backend_dir / "gold.yaml",   _gold_template(project)),
    ]:
        with open(path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)
        print(f"🏗️   [init] created {path}")

    # --- backend/ UDF stubs ---
    udf_silver.mkdir(parents=True)
    udf_gold.mkdir(parents=True)
    for pkg in [backend_dir, backend_dir / "udf", udf_silver, udf_gold]:
        (pkg / "__init__.py").touch()

    for path, content in [
        (udf_silver / "base.py",       _udf_base(project)),
        (udf_silver / "derived.py",    _udf_derived(project)),
        (udf_gold   / "transforms.py", _udf_gold(project)),
    ]:
        with open(path, "w") as f:
            f.write(content)
        print(f"🏗️   [init] created {path}")

    # --- frontend/ ---
    fe_dir = project_dir / "frontend"
    fe_dir.mkdir()
    (fe_dir / ".gitkeep").touch()
    print(f"🏗️   [init] created {fe_dir}/")

    # --- data/ (gitignored) ---
    data_dir = project_dir / "data"
    data_dir.mkdir()
    with open(data_dir / ".gitignore", "w") as f:
        f.write("# Pipeline outputs — never commit data\n*\n!.gitignore\n")
    print(f"🏗️   [init] created {data_dir}/ (gitignored)")

    # --- summary/ ---
    sum_dir = project_dir / "summary"
    sum_dir.mkdir()
    with open(sum_dir / "summary.md", "w") as f:
        f.write(_summary_template(project))
    print(f"🏗️   [init] created {sum_dir}/")

    # --- README.md ---
    readme_path = project_dir / "README.md"
    with open(readme_path, "w") as f:
        f.write(_readme_template(project))
    print(f"🏗️   [init] created {readme_path}")

    # --- kestra_flow.yml ---
    kestra_path = project_dir / "kestra_flow.yml"
    with open(kestra_path, "w") as f:
        f.write(_kestra_flow_template(project))
    print(f"🏗️   [init] created {kestra_path}")

    print(f"\n✅  Project '{project}' initialised.")
    print(f"   Backend   : {project}/backend/")
    print(f"   Frontend  : {project}/frontend/")
    print(f"   Summary   : {project}/summary/")
    print(f"   Kestra    : add {project}/kestra_flow.yml to docker-compose.yml volumes to activate")
    print(f"   Run       : medallion run {project}")
