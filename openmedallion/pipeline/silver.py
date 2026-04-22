"""pipeline/silver.py — Polars transform engine: bronze Parquet → silver Parquet.

Two-phase execution
-------------------
Phase 1 — base tables
    Read each bronze Parquet file, apply structural transforms (rename / cast / drop)
    followed by a UDF, and write to the silver directory.

Phase 2 — derived tables
    After all base tables are written, call a UDF per derived table.
    The UDF receives the silver directory path and builds a new DataFrame
    by loading and joining any tables it needs.

Transform types: ``rename``, ``cast``, ``drop``, ``udf``.
"""
import polars as pl

from openmedallion import storage
from openmedallion.contracts.udf import load_udf, check_return


class SilverTransformer:
    """Apply structural transforms and UDFs to bronze Parquet, writing silver Parquet.

    Args:
        cfg: Merged project config dict. Reads ``paths.bronze``, ``paths.silver``,
            and ``bronze_to_silver``.
    """

    def __init__(self, cfg: dict):
        self.bronze_path = cfg["paths"]["bronze"]
        self.silver_path = cfg["paths"]["silver"]
        self.tables      = cfg["bronze_to_silver"].get("tables", [])
        self.derived     = cfg["bronze_to_silver"].get("derived_tables", [])
        self._udf_cache: dict[str, object] = {}

    def transform(self) -> dict[str, str]:
        """Run both silver phases and return paths for all written files."""
        storage.mkdir(self.silver_path)
        results = {}

        # phase 1: base tables
        for tbl in self.tables:
            src = storage.join(self.bronze_path, tbl["source_file"])
            if not storage.exists(src):
                print(f"⚠️   [silver] missing bronze file: {src}")
                continue
            df = storage.read_parquet(src)
            for step in tbl.get("transforms", []):
                df = self._apply(df, step)
            out = storage.join(self.silver_path, tbl["output_file"])
            storage.write_parquet(df, out)
            print(f"🔧  [silver] base    {tbl['source_file']} → {tbl['output_file']}  ({len(df)} rows)")
            results[tbl["output_file"]] = out

        # phase 2: derived tables via UDF
        for dtbl in self.derived:
            udf_step = dtbl["udf"]
            df = self._call_derived_udf(udf_step)
            if cols := dtbl.get("select"):
                df = df.select(cols)
            out = storage.join(self.silver_path, dtbl["output_file"])
            storage.write_parquet(df, out)
            print(f"🔧  [silver] derived {dtbl['output_file']}  ({len(df)} rows)")
            results[dtbl["output_file"]] = out

        return results

    def _apply(self, df: pl.DataFrame, step: dict) -> pl.DataFrame:
        t = step["type"]
        if t == "rename":
            return df.rename(step["columns"])
        if t == "cast":
            return df.with_columns(
                [pl.col(c).cast(getattr(pl, dtype))
                 for c, dtype in step["columns"].items()]
            )
        if t == "drop":
            cols = step.get("columns") or []
            return df.drop(cols) if cols else df
        if t == "udf":
            return self._call_udf(df, step)
        raise ValueError(
            f"[silver] Unknown transform type: '{t}'. "
            f"Allowed: rename, cast, drop, udf"
        )

    def _call_udf(self, df: pl.DataFrame, step: dict) -> pl.DataFrame:
        fn, kwargs = load_udf(step, cache=self._udf_cache, layer="silver")
        result = fn(df, **kwargs)
        check_return(result, step["function"], step["file"], layer="silver")
        print(f"⚙️   [silver] udf  {step['function']}()  {len(df)} → {len(result)} rows")
        return result

    def _call_derived_udf(self, step: dict) -> pl.DataFrame:
        fn, kwargs = load_udf(step, cache=self._udf_cache, layer="silver")
        result = fn(self.silver_path, **kwargs)
        check_return(result, step["function"], step["file"], layer="silver")
        return result
