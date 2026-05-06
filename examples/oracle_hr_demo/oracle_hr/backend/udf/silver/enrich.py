"""Silver derived-table UDF: join employees + departments + jobs."""
import polars as pl
from pathlib import Path

from openmedallion.storage import read_parquet, join


def build_employees_enriched(silver_dir: str | Path) -> pl.DataFrame:
    employees   = read_parquet(join(silver_dir, "employees.parquet"))
    departments = read_parquet(join(silver_dir, "departments.parquet"))
    jobs        = read_parquet(join(silver_dir, "jobs.parquet"))

    return (
        employees
        .join(departments, on="department_id", how="left", suffix="_dept")
        .join(jobs,        on="job_id",         how="left", suffix="_job")
    )
