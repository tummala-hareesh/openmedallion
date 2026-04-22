"""tests/test_gold.py — tests for GoldAggregator."""
import pytest
import polars as pl
from pathlib import Path

from openmedallion.pipeline.gold import GoldAggregator


def make_cfg(tmp_path, projects):
    silver = tmp_path / "silver"
    gold   = tmp_path / "gold"
    silver.mkdir(exist_ok=True)
    gold.mkdir(exist_ok=True)
    return {
        "paths": {"silver": str(silver), "gold": str(gold)},
        "silver_to_gold": {"projects": projects},
    }


def write_silver(tmp_path, filename, df):
    (tmp_path / "silver" / filename).parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(tmp_path / "silver" / filename)


class TestAggregations:

    def test_count_aggregation(self, tmp_path):
        df = pl.DataFrame({"customer_id": [1, 1, 2, 2, 2], "order_id": [10, 11, 12, 13, 14]})
        write_silver(tmp_path, "orders.parquet", df)
        cfg = make_cfg(tmp_path, projects=[{"name": "bi", "aggregations": [{
            "source_file": "orders.parquet",
            "group_by": ["customer_id"],
            "metrics": [{"column": "order_id", "agg": "count", "alias": "n_orders"}],
            "output_file": "result.parquet",
        }]}])
        GoldAggregator(cfg).aggregate()
        result = pl.read_parquet(tmp_path / "gold" / "bi" / "result.parquet")
        by_customer = {r["customer_id"]: r["n_orders"]
                       for r in result.sort("customer_id").to_dicts()}
        assert by_customer[1] == 2
        assert by_customer[2] == 3

    def test_sum_aggregation(self, tmp_path):
        df = pl.DataFrame({"product_id": [1, 1, 2], "qty": [3, 5, 7]})
        write_silver(tmp_path, "lines.parquet", df)
        cfg = make_cfg(tmp_path, projects=[{"name": "bi", "aggregations": [{
            "source_file": "lines.parquet",
            "group_by": ["product_id"],
            "metrics": [{"column": "qty", "agg": "sum", "alias": "total_qty"}],
            "output_file": "result.parquet",
        }]}])
        GoldAggregator(cfg).aggregate()
        result = pl.read_parquet(tmp_path / "gold" / "bi" / "result.parquet")
        totals = {r["product_id"]: r["total_qty"] for r in result.to_dicts()}
        assert totals[1] == 8
        assert totals[2] == 7

    def test_mean_min_max_aggregations(self, tmp_path):
        df = pl.DataFrame({"grp": ["a", "a", "a"], "val": [2.0, 4.0, 6.0]})
        write_silver(tmp_path, "t.parquet", df)
        cfg = make_cfg(tmp_path, projects=[{"name": "bi", "aggregations": [{
            "source_file": "t.parquet",
            "group_by": ["grp"],
            "metrics": [
                {"column": "val", "agg": "mean", "alias": "avg"},
                {"column": "val", "agg": "min",  "alias": "lo"},
                {"column": "val", "agg": "max",  "alias": "hi"},
            ],
            "output_file": "result.parquet",
        }]}])
        GoldAggregator(cfg).aggregate()
        result = pl.read_parquet(tmp_path / "gold" / "bi" / "result.parquet")
        row = result.to_dicts()[0]
        assert row["avg"] == pytest.approx(4.0)
        assert row["lo"]  == pytest.approx(2.0)
        assert row["hi"]  == pytest.approx(6.0)

    def test_no_group_by_returns_grand_total(self, tmp_path):
        df = pl.DataFrame({"id": [1, 2, 3]})
        write_silver(tmp_path, "t.parquet", df)
        cfg = make_cfg(tmp_path, projects=[{"name": "bi", "aggregations": [{
            "source_file": "t.parquet",
            "group_by": [],
            "metrics": [{"column": "id", "agg": "count", "alias": "total"}],
            "output_file": "result.parquet",
        }]}])
        GoldAggregator(cfg).aggregate()
        result = pl.read_parquet(tmp_path / "gold" / "bi" / "result.parquet")
        assert result["total"][0] == 3

    def test_flat_select_passthrough(self, tmp_path):
        df = pl.DataFrame({"a": [1, 2], "b": [3, 4], "c": [5, 6]})
        write_silver(tmp_path, "t.parquet", df)
        cfg = make_cfg(tmp_path, projects=[{"name": "bi", "aggregations": [{
            "source_file": "t.parquet",
            "metrics": [],
            "select": ["a", "b"],
            "output_file": "result.parquet",
        }]}])
        GoldAggregator(cfg).aggregate()
        result = pl.read_parquet(tmp_path / "gold" / "bi" / "result.parquet")
        assert result.columns == ["a", "b"]

    def test_multiple_projects_independent(self, tmp_path):
        df = pl.DataFrame({"id": [1, 2, 3]})
        write_silver(tmp_path, "t.parquet", df)
        cfg = make_cfg(tmp_path, projects=[
            {"name": "proj_a", "aggregations": [{
                "source_file": "t.parquet", "group_by": [],
                "metrics": [{"column": "id", "agg": "count", "alias": "n"}],
                "output_file": "out.parquet",
            }]},
            {"name": "proj_b", "aggregations": [{
                "source_file": "t.parquet", "group_by": [],
                "metrics": [{"column": "id", "agg": "sum", "alias": "s"}],
                "output_file": "out.parquet",
            }]},
        ])
        GoldAggregator(cfg).aggregate()
        assert (tmp_path / "gold" / "proj_a" / "out.parquet").exists()
        assert (tmp_path / "gold" / "proj_b" / "out.parquet").exists()

    def test_returns_correct_path_dict(self, tmp_path):
        df = pl.DataFrame({"id": [1]})
        write_silver(tmp_path, "t.parquet", df)
        cfg = make_cfg(tmp_path, projects=[{"name": "bi", "aggregations": [{
            "source_file": "t.parquet", "group_by": [],
            "metrics": [{"column": "id", "agg": "count", "alias": "n"}],
            "output_file": "result.parquet",
        }]}])
        results = GoldAggregator(cfg).aggregate()
        assert "bi" in results
        assert len(results["bi"]) == 1
        assert Path(results["bi"][0]).name == "result.parquet"


class TestPreAggUDF:

    def _write_udf(self, tmp_path, code):
        p = tmp_path / "gold_udf.py"
        p.write_text(code)
        return p

    def test_pre_agg_udf_filters_before_aggregation(self, tmp_path):
        df = pl.DataFrame({"grp": ["a", "a", "b"], "val": [1, -1, 2]})
        write_silver(tmp_path, "t.parquet", df)
        udf_file = self._write_udf(tmp_path, """
import polars as pl
from pathlib import Path
def filter_positive(df: pl.DataFrame, silver_dir: Path) -> pl.DataFrame:
    return df.filter(pl.col("val") > 0)
""")
        cfg = make_cfg(tmp_path, projects=[{"name": "bi", "aggregations": [{
            "source_file": "t.parquet",
            "pre_agg_udf": {"file": str(udf_file), "function": "filter_positive"},
            "group_by": ["grp"],
            "metrics": [{"column": "val", "agg": "sum", "alias": "total"}],
            "output_file": "result.parquet",
        }]}])
        GoldAggregator(cfg).aggregate()
        result = pl.read_parquet(tmp_path / "gold" / "bi" / "result.parquet")
        totals = {r["grp"]: r["total"] for r in result.to_dicts()}
        assert totals["a"] == 1
        assert totals["b"] == 2

    def test_pre_agg_udf_receives_silver_dir(self, tmp_path):
        df = pl.DataFrame({"id": [1, 2], "val": [10, 20]})
        write_silver(tmp_path, "t.parquet", df)
        lookup = pl.DataFrame({"id": [1, 2], "cat": ["x", "y"]})
        write_silver(tmp_path, "lookup.parquet", lookup)

        udf_file = self._write_udf(tmp_path, """
import polars as pl
from pathlib import Path
def join_cat(df: pl.DataFrame, silver_dir) -> pl.DataFrame:
    lkp = pl.read_parquet(Path(silver_dir) / "lookup.parquet")
    return df.join(lkp, on="id", how="left")
""")
        cfg = make_cfg(tmp_path, projects=[{"name": "bi", "aggregations": [{
            "source_file": "t.parquet",
            "pre_agg_udf": {"file": str(udf_file), "function": "join_cat"},
            "group_by": ["cat"],
            "metrics": [{"column": "val", "agg": "sum", "alias": "total"}],
            "output_file": "result.parquet",
        }]}])
        GoldAggregator(cfg).aggregate()
        result = pl.read_parquet(tmp_path / "gold" / "bi" / "result.parquet")
        cats = {r["cat"]: r["total"] for r in result.to_dicts()}
        assert cats["x"] == 10
        assert cats["y"] == 20

    def test_pre_agg_udf_wrong_return_raises(self, tmp_path):
        df = pl.DataFrame({"id": [1]})
        write_silver(tmp_path, "t.parquet", df)
        udf_file = self._write_udf(tmp_path, """
from pathlib import Path
def bad(df, silver_dir: Path): return 42
""")
        cfg = make_cfg(tmp_path, projects=[{"name": "bi", "aggregations": [{
            "source_file": "t.parquet",
            "pre_agg_udf": {"file": str(udf_file), "function": "bad"},
            "group_by": [], "metrics": [{"column": "id", "agg": "count", "alias": "n"}],
            "output_file": "r.parquet",
        }]}])
        with pytest.raises(TypeError, match="must return pl.DataFrame"):
            GoldAggregator(cfg).aggregate()
