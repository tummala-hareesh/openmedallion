"""tests/test_helpers.py — tests for openmedallion helpers (joins, windows, aggregations, dates)."""
import pytest
import polars as pl
from datetime import date

from openmedallion.helpers.joins        import join_tables, lookup_join, safe_join, multi_join
from openmedallion.helpers.windows      import (rank_within, running_total, lag_column,
                                                lead_column, pct_of_total, row_number,
                                                first_last_within)
from openmedallion.helpers.aggregations import attach_group_stats, top_n_within, pivot_to_columns, flag_outliers
from openmedallion.helpers.dates        import date_trunc, days_between, classify_recency, add_calendar_columns


class TestJoinTables:

    def test_inner_join_on_shared_key(self):
        left  = pl.DataFrame({"id": [1, 2, 3], "a": ["x", "y", "z"]})
        right = pl.DataFrame({"id": [1, 2, 4], "b": [10, 20, 40]})
        result = join_tables(left, right, on="id", how="inner")
        assert sorted(result["id"].to_list()) == [1, 2]

    def test_left_join_preserves_all_left_rows(self):
        left  = pl.DataFrame({"id": [1, 2, 3]})
        right = pl.DataFrame({"id": [1, 2], "val": [10, 20]})
        result = join_tables(left, right, on="id", how="left")
        assert len(result) == 3
        assert result.filter(pl.col("id") == 3)["val"][0] is None

    def test_explicit_left_on_right_on(self):
        left  = pl.DataFrame({"order_id": [1, 2], "x": ["a", "b"]})
        right = pl.DataFrame({"oid": [1, 2], "y": [10, 20]})
        result = join_tables(left, right, left_on="order_id", right_on="oid")
        assert "x" in result.columns
        assert "y" in result.columns

    def test_suffix_applied_to_duplicate_columns(self):
        left  = pl.DataFrame({"id": [1], "name": ["alice"]})
        right = pl.DataFrame({"id": [1], "name": ["bob"]})
        result = join_tables(left, right, on="id", suffix="_r")
        assert "name_r" in result.columns


class TestLookupJoin:

    def test_only_specified_cols_brought_over(self):
        df     = pl.DataFrame({"id": [1, 2], "val": [10, 20]})
        lookup = pl.DataFrame({"id": [1, 2], "name": ["a", "b"], "extra": [9, 9]})
        result = lookup_join(df, lookup, key="id", cols=["name"])
        assert "name" in result.columns
        assert "extra" not in result.columns

    def test_key_not_duplicated_in_output(self):
        df     = pl.DataFrame({"id": [1, 2]})
        lookup = pl.DataFrame({"id": [1, 2], "label": ["x", "y"]})
        result = lookup_join(df, lookup, key="id", cols=["label"])
        assert result.columns.count("id") == 1

    def test_unmatched_rows_get_null(self):
        df     = pl.DataFrame({"id": [1, 99]})
        lookup = pl.DataFrame({"id": [1], "label": ["found"]})
        result = lookup_join(df, lookup, key="id", cols=["label"])
        assert result.filter(pl.col("id") == 99)["label"][0] is None


class TestSafeJoin:

    def test_null_filled_with_defaults(self):
        left  = pl.DataFrame({"id": [1, 2]})
        right = pl.DataFrame({"id": [1], "score": [99]})
        result = safe_join(left, right, on="id", fill={"score": 0})
        assert result.filter(pl.col("id") == 2)["score"][0] == 0

    def test_matched_rows_not_overwritten(self):
        left  = pl.DataFrame({"id": [1, 2]})
        right = pl.DataFrame({"id": [1, 2], "score": [10, 20]})
        result = safe_join(left, right, on="id", fill={"score": -1})
        assert result.sort("id")["score"].to_list() == [10, 20]


class TestMultiJoin:

    def test_joins_applied_sequentially(self):
        base   = pl.DataFrame({"id": [1, 2]})
        right1 = pl.DataFrame({"id": [1, 2], "a": ["x", "y"]})
        right2 = pl.DataFrame({"id": [1, 2], "b": [10, 20]})
        result = multi_join(base, [
            {"right": right1, "on": "id", "how": "left"},
            {"right": right2, "on": "id", "how": "left"},
        ])
        assert "a" in result.columns
        assert "b" in result.columns


class TestRankWithin:

    def test_ranks_within_partition(self):
        df = pl.DataFrame({"grp": ["a","a","a"], "val": [3, 1, 2]})
        result = rank_within(df, partition_by="grp", order_by="val")
        assert sorted(result["rank"].to_list()) == [1, 2, 3]

    def test_descending_rank(self):
        df = pl.DataFrame({"grp": ["a","a","a"], "val": [10, 20, 30]})
        result = rank_within(df, partition_by="grp", order_by="val", descending=True)
        assert result.filter(pl.col("val") == 30)["rank"][0] == 1

    def test_separate_ranks_per_partition(self):
        df = pl.DataFrame({"grp": ["a","a","b","b"], "val": [1, 2, 1, 2]})
        result = rank_within(df, partition_by="grp", order_by="val")
        assert result["rank"].max() == 2
        assert result["rank"].min() == 1


class TestRunningTotal:

    def test_cumulative_sum_within_partition(self):
        df = pl.DataFrame({
            "order_id": [1, 1, 1],
            "line_id":  [1, 2, 3],
            "val":      [10.0, 20.0, 30.0],
        })
        result = running_total(df, col="val", partition_by="order_id", order_by="line_id")
        assert "running_val" in result.columns

    def test_custom_alias(self):
        df = pl.DataFrame({"grp": ["a","a"], "v": [1.0, 2.0]})
        result = running_total(df, col="v", partition_by="grp", order_by="v", alias="cumsum")
        assert "cumsum" in result.columns


class TestLagColumn:

    def test_lag_shifts_values(self):
        df = pl.DataFrame({"grp": ["a","a","a"], "ord": [1,2,3], "val": [10,20,30]})
        result = lag_column(df, col="val", partition_by="grp", order_by="ord")
        assert "prev_val" in result.columns

    def test_fill_value_applied_to_first_row(self):
        df = pl.DataFrame({"grp": ["a","a"], "ord": [1,2], "val": [10,20]})
        result = lag_column(df, col="val", partition_by="grp", order_by="ord", fill_value=0)
        assert 0 in result["prev_val"].to_list()


class TestLeadColumn:

    def test_lead_shifts_forward(self):
        df = pl.DataFrame({"grp": ["a","a","a"], "ord": [1,2,3], "val": [10,20,30]})
        result = lead_column(df, col="val", partition_by="grp", order_by="ord")
        assert "next_val" in result.columns

    def test_custom_alias(self):
        df = pl.DataFrame({"g": ["a","a"], "o": [1,2], "v": [1,2]})
        result = lead_column(df, col="v", partition_by="g", order_by="o", alias="future_v")
        assert "future_v" in result.columns


class TestPctOfTotal:

    def test_percentages_sum_to_100_per_partition(self):
        df = pl.DataFrame({"grp": ["a","a","a"], "val": [10.0, 20.0, 70.0]})
        result = pct_of_total(df, col="val", partition_by="grp")
        total_pct = result.group_by("grp").agg(pl.col("val_pct").sum())
        assert total_pct["val_pct"][0] == pytest.approx(100.0, abs=0.01)


class TestRowNumber:

    def test_row_numbers_are_sequential(self):
        df = pl.DataFrame({"grp": ["a","a","a"], "ord": [3, 1, 2]})
        result = row_number(df, partition_by="grp", order_by="ord")
        assert sorted(result["row_num"].to_list()) == [1, 2, 3]


class TestFirstLastWithin:

    def test_first_and_last_columns_added(self):
        df = pl.DataFrame({"grp": ["a","a","a"], "ord": [1,2,3], "val": [10,20,30]})
        result = first_last_within(df, col="val", partition_by="grp", order_by="ord")
        assert "first_val" in result.columns
        assert "last_val" in result.columns


class TestAttachGroupStats:

    def test_group_stats_attached_as_new_columns(self):
        df = pl.DataFrame({"grp": ["a","a","b"], "val": [10.0, 20.0, 30.0]})
        result = attach_group_stats(df, group_by="grp", col="val",
                                    stats=["sum", "mean"], prefix="g_")
        assert "g_val_sum"  in result.columns
        assert "g_val_mean" in result.columns
        assert len(result) == 3

    def test_group_sum_correct(self):
        df = pl.DataFrame({"grp": ["a","a"], "val": [5.0, 15.0]})
        result = attach_group_stats(df, group_by="grp", col="val", stats=["sum"])
        assert result["val_group_val_sum"].unique()[0] == pytest.approx(20.0)


class TestTopNWithin:

    def test_flags_top_n_rows(self):
        df = pl.DataFrame({"grp": ["a","a","a","a"], "val": [1, 4, 3, 2]})
        result = top_n_within(df, partition_by="grp", order_by="val", n=2)
        top2 = result.filter(pl.col("is_top_n"))["val"].sort(descending=True).to_list()
        assert top2 == [4, 3]

    def test_custom_alias(self):
        df = pl.DataFrame({"grp": ["a","a"], "val": [1, 2]})
        result = top_n_within(df, partition_by="grp", order_by="val", alias="winner")
        assert "winner" in result.columns


class TestFlagOutliers:

    def test_iqr_flags_extreme_value(self):
        df = pl.DataFrame({"val": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 1000.0]})
        result = flag_outliers(df, col="val")
        assert result.filter(pl.col("val") == 1000.0)["is_outlier_val"][0] is True

    def test_zscore_flags_extreme_value(self):
        df = pl.DataFrame({"val": [1.0, 1.0, 1.0, 1.0, 1.0, 100.0]})
        result = flag_outliers(df, col="val", method="zscore", multiplier=2.0)
        assert result.filter(pl.col("val") == 100.0)["is_outlier_val"][0] is True

    def test_normal_values_not_flagged(self):
        df = pl.DataFrame({"val": [10.0, 11.0, 10.5, 10.2, 10.8]})
        result = flag_outliers(df, col="val")
        assert not result["is_outlier_val"].any()


class TestPivotToColumns:

    def test_wide_format_produced(self):
        df = pl.DataFrame({
            "product_id": [1, 1, 2, 2],
            "month":      ["jan", "feb", "jan", "feb"],
            "revenue":    [100, 200, 300, 400],
        })
        result = pivot_to_columns(df, index="product_id", pivot_col="month",
                                  value_col="revenue", agg="sum")
        assert "jan" in result.columns or "feb" in result.columns
        assert len(result) == 2


class TestDateTrunc:

    def test_truncate_to_month(self):
        df = pl.DataFrame({"dt": pl.Series(["2024-03-15", "2024-03-28"]).str.to_date()})
        result = date_trunc(df, col="dt", unit="month", alias="month")
        months = result["month"].to_list()
        assert all(d.day == 1 for d in months)
        assert all(d.month == 3 for d in months)

    def test_truncate_to_year(self):
        df = pl.DataFrame({"dt": pl.Series(["2024-06-15"]).str.to_date()})
        result = date_trunc(df, col="dt", unit="year", alias="yr")
        assert result["yr"][0].month == 1
        assert result["yr"][0].day   == 1

    def test_unknown_unit_raises(self):
        df = pl.DataFrame({"dt": pl.Series(["2024-01-01"]).str.to_date()})
        with pytest.raises(ValueError, match="Unknown unit"):
            date_trunc(df, col="dt", unit="fortnight")

    def test_default_alias(self):
        df = pl.DataFrame({"dt": pl.Series(["2024-01-15"]).str.to_date()})
        result = date_trunc(df, col="dt", unit="month")
        assert "dt_month" in result.columns


class TestDaysBetween:

    def test_positive_diff(self):
        df = pl.DataFrame({
            "start": pl.Series(["2024-01-01"]).str.to_date(),
            "end":   pl.Series(["2024-01-11"]).str.to_date(),
        })
        result = days_between(df, start="start", end="end", alias="diff")
        assert result["diff"][0] == 10

    def test_negative_diff_without_abs(self):
        df = pl.DataFrame({
            "start": pl.Series(["2024-01-11"]).str.to_date(),
            "end":   pl.Series(["2024-01-01"]).str.to_date(),
        })
        result = days_between(df, start="start", end="end")
        assert result["days_start_to_end"][0] == -10

    def test_abs_value(self):
        df = pl.DataFrame({
            "start": pl.Series(["2024-01-11"]).str.to_date(),
            "end":   pl.Series(["2024-01-01"]).str.to_date(),
        })
        result = days_between(df, start="start", end="end", abs_value=True)
        assert result["days_start_to_end"][0] == 10


class TestClassifyRecency:

    def test_recent_date_labelled_active(self):
        df = pl.DataFrame({"last_dt": pl.Series([date.today()]).cast(pl.Date)})
        result = classify_recency(df, col="last_dt", as_of=date.today())
        assert result["recency_band"][0] == "active"

    def test_old_date_labelled_inactive(self):
        from datetime import timedelta
        old_date = date.today() - timedelta(days=400)
        df = pl.DataFrame({"last_dt": pl.Series([old_date]).cast(pl.Date)})
        result = classify_recency(df, col="last_dt", as_of=date.today())
        assert result["recency_band"][0] == "inactive"

    def test_custom_bands(self):
        from datetime import timedelta
        dt = date.today() - timedelta(days=3)
        df = pl.DataFrame({"dt": pl.Series([dt]).cast(pl.Date)})
        result = classify_recency(df, col="dt", as_of=date.today(),
                                  bands=[(5, "fresh"), (999, "stale")])
        assert result["recency_band"][0] == "fresh"


class TestAddCalendarColumns:

    def test_requested_parts_added(self):
        df = pl.DataFrame({"dt": pl.Series(["2024-06-15"]).str.to_date()})
        result = add_calendar_columns(df, col="dt", parts=["year", "month"])
        assert "dt_year"  in result.columns
        assert "dt_month" in result.columns
        assert result["dt_year"][0]  == 2024
        assert result["dt_month"][0] == 6

    def test_is_weekend_flag(self):
        df = pl.DataFrame({"dt": pl.Series(["2024-06-15"]).str.to_date()})
        result = add_calendar_columns(df, col="dt", parts=["is_weekend"])
        assert result["dt_is_weekend"][0] is True

    def test_unknown_part_raises(self):
        df = pl.DataFrame({"dt": pl.Series(["2024-01-01"]).str.to_date()})
        with pytest.raises(ValueError, match="Unknown calendar parts"):
            add_calendar_columns(df, col="dt", parts=["fortnight"])
