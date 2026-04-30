"""scaffold/templates.py — scaffold a new project folder with config, UDF stubs, and docs.

Creates the full project structure under <path_project>/<project>/:

    <path_project>/<project>/
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
    ├── frontend/
    │   ├── tableau/               ← Tableau workbook files (.twb, .twbx)
    │   ├── powerbi/               ← Power BI files (.pbix)
    │   └── streamlit/             ← Streamlit dashboard (app.py + dashboard.yaml)
    ├── ipynb/
    │   └── walkthrough.ipynb      ← step-by-step notebook for the project
    ├── kestra_flow.yaml           ← Kestra orchestration flow
    └── README.md                  ← project documentation template

Data outputs go to <path_data>/ (outside the project folder, gitignored separately).
"""
import json
import os
import yaml
from pathlib import Path


def _main_template(project: str, path_data: str = "data") -> dict:
    return {
        "pipeline": {"name": project},
        "includes": {
            "bronze": "backend/bronze.yaml",
            "silver": "backend/silver.yaml",
            "gold":   "backend/gold.yaml",
        },
        "paths": {
            "bronze": f"{path_data}/bronze",
            "silver": f"{path_data}/silver",
            "gold":   f"{path_data}/gold",
            "export": f"{path_data}/export",
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
        "destination": {"type": "filesystem", "bucket_url": "data/bronze"},
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


def _readme_template(project: str, path_data: str = "data") -> str:
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
├── frontend/
│   ├── tableau/       ← Tableau workbook files
│   └── powerbi/       ← Power BI files
├── ipynb/
│   └── walkthrough.ipynb  ← end-to-end pipeline walkthrough
└── kestra_flow.yaml   ← Kestra orchestration (mount in docker-compose.yml)

{path_data}/           ← pipeline outputs (outside project folder, gitignored)
├── bronze/
├── silver/
├── gold/
└── export/
```

---

## Design Layout — ETL Pipeline

```
[Source DB / API]
       │
       ▼  bronze  (dlt)
[Raw Parquet — {path_data}/bronze/]
       │
       ▼  silver  (Polars + UDFs)
[Cleaned Parquet — {path_data}/silver/]
       │
       ▼  gold  (Polars group_by + UDFs)
[Aggregated Parquet — {path_data}/gold/]
       │
       ▼  export
[CSV + Parquet — {path_data}/export/]  →  [BI Tool]
```

---

## How to Refresh Backend Data

Run the full pipeline from the example root:

```bash
medallion run {project}
```

Run individual layers:

```bash
medallion run {project} --layer bronze   # ingest only
medallion run {project} --layer silver   # transform only
medallion run {project} --layer gold     # aggregate only
```

Or open `ipynb/walkthrough.ipynb` in Jupyter for a step-by-step guided run.

---

## How to Update the Frontend

<!-- Choose whichever applies and delete the rest. -->

**Power BI:** Open `frontend/powerbi/{project}.pbix` → Home → Refresh → Publish to service.

**Tableau:** Open `frontend/tableau/{project}.twb` → Data → Refresh All Extracts → Publish to Tableau Server.

---

## Snapshot of Dashboard Views

<!-- Insert screenshots here.
Example:
![Overview Page](frontend/screenshots/overview.png)
-->

*Screenshots pending first dashboard publish.*

---

## Additional References

| Resource | Link |
|---|---|
| Source system docs | <!-- URL --> |
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


def _kestra_flow_template(project: str, working_dir: str = "/app") -> str:
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
    workingDirectory: {working_dir}

  - id: run_silver
    type: io.kestra.plugin.scripts.python.Commands
    warningOnStdErr: false
    commands:
      - medallion run {project} --layer silver
    workingDirectory: {working_dir}
    dependsOn:
      - run_bronze

  - id: run_gold
    type: io.kestra.plugin.scripts.python.Commands
    warningOnStdErr: false
    commands:
      - medallion run {project} --layer gold
    workingDirectory: {working_dir}
    dependsOn:
      - run_silver

# Uncomment to enable a scheduled refresh:
# triggers:
#   - id: daily_refresh
#     type: io.kestra.plugin.core.trigger.Schedule
#     cron: "0 6 * * *"   # every day at 06:00 UTC
"""


def _walkthrough_notebook(project: str, path_data: str = "data") -> str:
    """Return a generic Jupyter notebook JSON string for ipynb/walkthrough.ipynb."""

    def code(source_lines: list[str]) -> dict:
        return {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": source_lines,
        }

    def md(source_lines: list[str]) -> dict:
        return {
            "cell_type": "markdown",
            "metadata": {},
            "source": source_lines,
        }

    cells = [
        md([
            f"# {project} — Pipeline Walkthrough\n",
            "\n",
            "Run cells top to bottom to execute the full **Bronze → Silver → Gold** pipeline.\n",
            "\n",
            "| Layer | What happens |\n",
            "|-------|--------------|\n",
            "| Bronze | Raw data is ingested and written as Parquet |\n",
            "| Silver | Renames, casts, UDFs produce clean Parquet files |\n",
            "| Gold | Group-by aggregations produce BI-ready summaries |\n",
        ]),
        md(["## Setup\n", "\n", "Navigate to the example root (parent of this project folder).\n"]),
        code([
            "import os, sys\n",
            "from pathlib import Path\n",
            "\n",
            "# Two levels up: ipynb/ → project/ → example_root/\n",
            "example_root = Path(os.getcwd()).parent.parent\n",
            "os.chdir(example_root)\n",
            "print(f'Working directory: {os.getcwd()}')",
        ]),
        md([
            "## Step 1 — Bronze\n",
            "\n",
            "Seed the bronze layer from your source data.  \n",
            "Uncomment the option that matches your source:\n",
        ]),
        code([
            "# Option A: filesystem demo (runs seed.py in the example root)\n",
            "# exec(open('seed.py').read())\n",
            "#\n",
            "# Option B: SQL demo (creates the database)\n",
            "# exec(open('setup_db.py').read())\n",
            "#\n",
            "# Option C: dlt-managed bronze source\n",
            f"# !medallion run {project} --layer bronze",
        ]),
        md([
            "## Step 2 — Silver\n",
            "\n",
            "Apply rename, cast, and UDF transforms declared in `backend/silver.yaml`.\n",
        ]),
        code([f"!medallion run {project} --layer silver"]),
        code([
            "import polars as pl\n",
            "\n",
            f"silver_dir = Path('{path_data}/silver')\n",
            "for f in sorted(silver_dir.glob('*.parquet')):\n",
            "    print(f'\\n── {{f.name}} ──')\n",
            "    print(pl.read_parquet(f).head(5))",
        ]),
        md([
            "## Step 3 — Gold\n",
            "\n",
            "Run group-by aggregations declared in `backend/gold.yaml`.\n",
        ]),
        code([f"!medallion run {project} --layer gold"]),
        code([
            f"gold_dir = Path('{path_data}/gold/{project}')\n",
            "for f in sorted(gold_dir.glob('*.parquet')):\n",
            "    print(f'\\n── {{f.name}} ──')\n",
            "    print(pl.read_parquet(f))",
        ]),
    ]

    nb = {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {
            "kernelspec": {
                "display_name": "py312_oma",
                "language": "python",
                "name": "venv",
            },
            "language_info": {
                "name": "python",
                "version": "3.12.0",
            },
        },
        "cells": cells,
    }

    return json.dumps(nb, indent=1, ensure_ascii=False)


def init_project(
    project: str,
    path_project: str | Path = ".",
    path_data: str | None = None,
) -> None:
    """Scaffold a new project directory with full structure.

    Args:
        project: Name of the new project — used as the folder name and in
            generated config templates.
        path_project: Parent directory where ``<project>/`` folder is created.
            Defaults to the current working directory.
        path_data: Base path (relative to ``path_project``) where pipeline
            outputs (bronze/silver/gold/export) are written.  Defaults to
            ``"data"``, placing outputs at ``<path_project>/data/``.

    Creates::

        <path_project>/<project>/
            main.yaml
            README.md
            kestra_flow.yaml
            backend/{bronze,silver,gold}.yaml + udf stubs
            frontend/tableau/
            frontend/powerbi/
            ipynb/walkthrough.ipynb
    """
    if path_data is None:
        path_data = "data"

    project_dir = Path(path_project) / project
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
        yaml.dump(_main_template(project, path_data), f, default_flow_style=False, sort_keys=False)
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
    for bi_dir in [project_dir / "frontend" / "tableau",
                   project_dir / "frontend" / "powerbi"]:
        bi_dir.mkdir(parents=True)
        (bi_dir / ".gitkeep").touch()
    print(f"🏗️   [init] created {project_dir / 'frontend'}/")

    # --- ipynb/ walkthrough notebook ---
    ipynb_dir = project_dir / "ipynb"
    ipynb_dir.mkdir()
    nb_path = ipynb_dir / "walkthrough.ipynb"
    with open(nb_path, "w", encoding="utf-8") as f:
        f.write(_walkthrough_notebook(project, path_data))
    print(f"🏗️   [init] created {nb_path}")

    # --- README.md ---
    readme_path = project_dir / "README.md"
    with open(readme_path, "w") as f:
        f.write(_readme_template(project, path_data))
    print(f"🏗️   [init] created {readme_path}")

    # --- kestra_flow.yaml ---
    kestra_path = project_dir / "kestra_flow.yaml"
    with open(kestra_path, "w") as f:
        f.write(_kestra_flow_template(project))
    print(f"🏗️   [init] created {kestra_path}")

    print(f"\n✅  Project '{project}' initialised at {project_dir}/")
    print(f"   Backend   : {project}/backend/")
    print(f"   Frontend  : {project}/frontend/tableau|powerbi/")
    print(f"   Notebook  : {project}/ipynb/walkthrough.ipynb")
    print(f"   Data      : {path_data}/  (bronze · silver · gold · export)")
    print(f"   Kestra    : add {project}/kestra_flow.yaml to your Kestra flows volume")
    print(f"   Run       : medallion run {project}")
