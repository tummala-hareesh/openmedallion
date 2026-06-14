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
        self.bronze_path     = cfg["paths"]["bronze"]
        self.silver_path     = cfg["paths"]["silver"]
        self.tables          = cfg["bronze_to_silver"].get("tables", [])
        self.derived         = cfg["bronze_to_silver"].get("derived_tables", [])
        self.duckdb_cfg      = cfg["bronze_to_silver"].get("duckdb")
        self._udf_cache:     dict[str, object] = {}
        self.explore_enabled = cfg.get("_explore", True)

    def transform(self) -> dict[str, str]:
        """Run both silver phases and return paths for all written files."""
        storage.mkdir(self.silver_path)
        print(f"\n── Silver {'─' * 49}")
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
            if self.explore_enabled and (explore_specs := tbl.get("explore")):
                from pathlib import Path as _Path
                from openmedallion.pipeline.explore import _dispatch_reports
                _dispatch_reports(
                    src     = _Path(out),
                    out_dir = _Path(self.silver_path) / "add-ons",
                    specs   = explore_specs,
                    context = "explore/silver",
                )

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
            if self.explore_enabled and (explore_specs := dtbl.get("explore")):
                from pathlib import Path as _Path
                from openmedallion.pipeline.explore import _dispatch_reports
                _dispatch_reports(
                    src     = _Path(out),
                    out_dir = _Path(self.silver_path) / "add-ons",
                    specs   = explore_specs,
                    context = "explore/silver",
                )

        if self.duckdb_cfg and self.duckdb_cfg.get("enabled", False):
            from openmedallion.pipeline.duckdb_views import register
            register(
                parquet_dir = self.silver_path,
                db_path     = self.duckdb_cfg["path"],
                mode        = self.duckdb_cfg.get("mode", "views"),
            )

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
        if t == "fillna":
            return df.with_columns(
                [pl.col(c).fill_null(v) for c, v in step["columns"].items()]
            )
        if t == "clip":
            exprs = []
            for col, bounds in step["columns"].items():
                dtype = df[col].dtype
                if dtype in (pl.Date, pl.Datetime):
                    # parse string bounds to the column's native type
                    def _lit_date(val: str, dt=dtype):
                        if dt == pl.Date:
                            import datetime as _dt
                            return pl.lit(_dt.date.fromisoformat(val))
                        return pl.lit(val).str.to_datetime()
                    expr = pl.col(col)
                    if "min" in bounds:
                        expr = pl.when(expr < _lit_date(bounds["min"])).then(_lit_date(bounds["min"])).otherwise(expr)
                    if "max" in bounds:
                        expr = pl.when(expr > _lit_date(bounds["max"])).then(_lit_date(bounds["max"])).otherwise(expr)
                    exprs.append(expr.alias(col))
                else:
                    expr = pl.col(col)
                    if "min" in bounds:
                        expr = expr.clip(lower_bound=bounds["min"])
                    if "max" in bounds:
                        expr = expr.clip(upper_bound=bounds["max"])
                    exprs.append(expr)
            return df.with_columns(exprs)
        if t == "normalize":
            _OPS = {
                "upper":       lambda e: e.str.to_uppercase(),
                "lower":       lambda e: e.str.to_lowercase(),
                "strip":       lambda e: e.str.strip_chars(),
                "strip_lower": lambda e: e.str.strip_chars().str.to_lowercase(),
            }
            exprs = []
            for col, op in step["columns"].items():
                if op not in _OPS:
                    raise ValueError(f"[silver] normalize: unknown op '{op}'. Allowed: {sorted(_OPS)}")
                exprs.append(_OPS[op](pl.col(col)))
            return df.with_columns(exprs)
        if t == "deduplicate":
            subset = step.get("subset") or None
            keep   = step.get("keep", "first")
            return df.unique(subset=subset, keep=keep, maintain_order=True)
        if t == "filter_rows":
            return df.lazy().filter(pl.sql_expr(step["expr"])).collect()
        if t == "allowed_values":
            exprs = []
            for col, allowed in step["columns"].items():
                exprs.append(
                    pl.when(pl.col(col).is_in(allowed)).then(pl.col(col)).otherwise(pl.lit(None)).alias(col)
                )
            return df.with_columns(exprs)
        if t == "coerce_bool":
            _TRUTHY  = {"true", "yes", "1", "on"}
            _FALSY   = {"false", "no", "0", "off"}
            exprs = []
            for col in step["columns"]:
                if df[col].dtype == pl.Boolean:
                    exprs.append(pl.col(col))
                else:
                    lower = pl.col(col).cast(pl.Utf8).str.to_lowercase()
                    exprs.append(
                        pl.when(lower.is_in(list(_TRUTHY))).then(pl.lit(True))
                          .when(lower.is_in(list(_FALSY))).then(pl.lit(False))
                          .otherwise(pl.lit(None))
                          .alias(col)
                    )
            return df.with_columns(exprs)
        if t == "map_values":
            col     = step["column"]
            mapping = step["mapping"]
            default = step.get("default")
            when_chain = pl.when(pl.col(col) == list(mapping.keys())[0]).then(pl.lit(list(mapping.values())[0]))
            for k, v in list(mapping.items())[1:]:
                when_chain = when_chain.when(pl.col(col) == k).then(pl.lit(v))
            fallback = pl.lit(default) if default is not None else pl.col(col)
            return df.with_columns(when_chain.otherwise(fallback).alias(col))
        raise ValueError(
            f"[silver] Unknown transform type: '{t}'. "
            f"Allowed: rename, cast, drop, udf, fillna, clip, normalize, deduplicate, filter_rows, map_values, allowed_values, coerce_bool"
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
