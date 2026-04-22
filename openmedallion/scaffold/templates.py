"""scaffold/templates.py — scaffold a new project folder with config and UDF stubs.

Creates in the user's project root:
  projects/<project>/main.yaml
  projects/<project>/bronze.yaml
  projects/<project>/silver.yaml
  projects/<project>/gold.yaml
  projects/<project>/udf/silver/base.py
  projects/<project>/udf/silver/derived.py
  projects/<project>/udf/gold/transforms.py

Also creates a matching project folder in tb-frontend (if it exists).
The frontend root defaults to the ``FRONTEND_ROOT`` environment variable,
or the ``tb-frontend/`` sibling of the current working directory.
"""
import os
import yaml
from pathlib import Path

# When running as an installed package, look for tb-frontend next to cwd.
# Developers using an editable install from TB_Gitlab/ will have it as a sibling.
_DEFAULT_FRONTEND_ROOT = Path(os.environ.get("FRONTEND_ROOT", "")) or (
    Path.cwd().parent / "tb-frontend"
)


def _main_template(project: str) -> dict:
    return {
        "pipeline": {"name": project},
        "includes": {"bronze": "bronze.yaml", "silver": "silver.yaml", "gold": "gold.yaml"},
        "paths": {
            "bronze": f"./data/{project}/bronze",
            "silver": f"./data/{project}/silver",
            "gold":   f"./data/{project}/gold",
            "export": f"./data/{project}/export",
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
                     "file": f"projects/{project}/udf/silver/base.py",
                     "function": "enrich_my_table"},
                ]}],
            "derived_tables": [{"output_file": "my_derived_table.parquet",
                "udf": {"file": f"projects/{project}/udf/silver/derived.py",
                        "function": "build_my_derived_table"}}],
        },
    }


def _gold_template(project: str) -> dict:
    return {
        "silver_to_gold": {"projects": [{"name": "default", "aggregations": [{
            "source_file": "my_table.parquet",
            "pre_agg_udf": {"file": f"projects/{project}/udf/gold/transforms.py",
                            "function": "prepare_my_table"},
            "group_by": ["id"],
            "metrics": [{"column": "id", "agg": "count", "alias": "total_records"}],
            "output_file": "summary.parquet",
        }]}]},
    }


def _udf_base(project: str) -> str:
    return f'''"""projects/{project}/udf/silver/base.py
Silver UDF — add columns, filter rows, reshape a single table.
Signature: (df: pl.DataFrame, **kwargs) -> pl.DataFrame
"""
import polars as pl
from datetime import date


def enrich_my_table(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(pl.lit(str(date.today())).alias("ingested_at"))
'''


def _udf_derived(project: str) -> str:
    return f'''"""projects/{project}/udf/silver/derived.py
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
    return f'''"""projects/{project}/udf/gold/transforms.py
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


def init_project(
    project: str,
    projects_root: str | Path = "projects",
    frontend_root: str | Path | None = None,
) -> None:
    """Scaffold a new project directory with YAML configs and UDF stubs.

    Args:
        project: Name of the new project — used as the folder name and in
            generated config templates.
        projects_root: Parent directory where the backend project folder is created.
            Defaults to ``"projects"``.
        frontend_root: Root of the tb-frontend repo. Defaults to the
            ``FRONTEND_ROOT`` env var, falling back to the ``tb-frontend/``
            sibling of the current directory.
    """
    project_dir = Path(projects_root) / project
    udf_silver  = project_dir / "udf" / "silver"
    udf_gold    = project_dir / "udf" / "gold"

    if project_dir.exists():
        print(f"❌  [init] '{project}' already exists — aborting.")
        print(f"    delete {project_dir}/ to reinitialise.")
        return

    # config YAMLs
    project_dir.mkdir(parents=True)
    for path, data in [
        (project_dir / "main.yaml",   _main_template(project)),
        (project_dir / "bronze.yaml", _bronze_template()),
        (project_dir / "silver.yaml", _silver_template(project)),
        (project_dir / "gold.yaml",   _gold_template(project)),
    ]:
        with open(path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)
        print(f"🏗️   [init] created {path}")

    # UDF stubs
    udf_silver.mkdir(parents=True)
    udf_gold.mkdir(parents=True)
    for pkg in [project_dir, project_dir / "udf", udf_silver, udf_gold]:
        (pkg / "__init__.py").touch()

    for path, content in [
        (udf_silver / "base.py",       _udf_base(project)),
        (udf_silver / "derived.py",    _udf_derived(project)),
        (udf_gold   / "transforms.py", _udf_gold(project)),
    ]:
        with open(path, "w") as f:
            f.write(content)
        print(f"🏗️   [init] created {path}")

    # frontend project folder (optional)
    fe_root = Path(
        frontend_root
        if frontend_root is not None
        else os.environ.get("FRONTEND_ROOT", str(_DEFAULT_FRONTEND_ROOT))
    )
    fe_project_dir = fe_root / "projects" / project
    if fe_project_dir.exists():
        print(f"⏭️   [init] frontend '{fe_project_dir}' already exists — skipped.")
    elif not fe_root.exists():
        print(f"⚠️   [init] frontend root not found: {fe_root}")
        print(f"    Set FRONTEND_ROOT env var to create the frontend project folder.")
    else:
        fe_project_dir.mkdir(parents=True)
        print(f"🏗️   [init] created {fe_project_dir}")

    print(f"\n✅  Project '{project}' initialised.")
    print(f"   Backend   : projects/{project}/")
    print(f"   Frontend  : {fe_project_dir}")
    print(f"   Run       : medallion run {project}")
