"""pipeline/gold.py — Polars aggregation engine: silver Parquet → gold Parquet.

For each aggregation block defined under a BI project in ``silver_to_gold``:

1. **Load** the source Parquet from the silver directory.
2. **Pre-aggregation UDF** (optional) — runs before the group_by step.
3. **Aggregate** using ``group_by`` + ``metrics``, or pass through flat.
4. **Write** the result to ``gold/<project_name>/<output_file>``.

Pre-aggregation UDF contract: ``(df, silver_dir, **kwargs) -> pl.DataFrame``

Metric aggregations: ``count``, ``sum``, ``mean``, ``min``, ``max``.
"""
import polars as pl

from openmedallion import storage
from openmedallion.contracts.udf import load_udf, check_return


AGG_MAP = {
    "count": lambda _: pl.len(),
    "sum":   lambda c: pl.col(c).sum(),
    "mean":  lambda c: pl.col(c).mean(),
    "min":   lambda c: pl.col(c).min(),
    "max":   lambda c: pl.col(c).max(),
}


class GoldAggregator:
    """Aggregate silver Parquet into per-BI-project gold Parquet.

    Args:
        cfg: Merged project config dict. Reads ``paths.silver``, ``paths.gold``,
            and ``silver_to_gold``.
    """

    def __init__(self, cfg: dict):
        self.silver_path = cfg["paths"]["silver"]
        self.gold_root   = cfg["paths"]["gold"]
        self.projects    = cfg["silver_to_gold"]["projects"]
        self._udf_cache: dict[str, object] = {}

    def aggregate(self) -> dict[str, list[str]]:
        """Run all gold aggregations and return the paths written."""
        results = {}
        print(f"\n── Gold {'─' * 51}")
        for project in self.projects:
            name    = project["name"]
            out_dir = storage.join(self.gold_root, name)
            storage.mkdir(out_dir)
            paths   = []

            for agg in project["aggregations"]:
                df = storage.read_parquet(storage.join(self.silver_path, agg["source_file"]))

                if udf_step := agg.get("pre_agg_udf"):
                    df = self._call_udf(df, udf_step)

                df = self._apply_agg(df, agg)

                out = storage.join(out_dir, agg["output_file"])
                storage.write_parquet(df, out)
                print(f"📊  [gold/{name}] {agg['output_file']}  ({len(df)} rows)")
                paths.append(out)

            results[name] = paths
        return results

    def _apply_agg(self, df: pl.DataFrame, agg: dict) -> pl.DataFrame:
        metrics = agg.get("metrics", [])
        groups  = agg.get("group_by") or []
        if not metrics:
            cols = agg.get("select")
            return df.select(cols) if cols else df
        exprs = [
            AGG_MAP[m["agg"]](m.get("column")).alias(m["alias"])
            for m in metrics
        ]
        return df.group_by(groups).agg(exprs) if groups else df.select(exprs)

    def _call_udf(self, df: pl.DataFrame, step: dict) -> pl.DataFrame:
        fn, kwargs = load_udf(step, cache=self._udf_cache, layer="gold")
        result = fn(df, self.silver_path, **kwargs)
        check_return(result, step["function"], step["file"], layer="gold")
        print(f"⚙️   [gold]  udf {step['function']}()  {len(df)} → {len(result)} rows")
        return result
