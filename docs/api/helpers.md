# Helpers API

Reusable Polars helper functions for use inside UDFs. Import what you need:

```python
from openmedallion.helpers.joins        import join_tables, lookup_join, safe_join, multi_join, asof_join, cross_join_filtered
from openmedallion.helpers.windows      import rank_within, running_total, lag_column, lead_column, pct_of_total, row_number, rolling_avg, first_last_within
from openmedallion.helpers.aggregations import attach_group_stats, top_n_within, pivot_to_columns, unpivot_columns, flag_outliers
from openmedallion.helpers.dates        import date_trunc, days_between, classify_recency, add_calendar_columns
```

---

## Joins

### `join_tables`

```python
def join_tables(
    left: pl.DataFrame,
    right: pl.DataFrame,
    on: str | list[str] | None = None,
    left_on: str | list[str] | None = None,
    right_on: str | list[str] | None = None,
    how: str = "inner",
    suffix: str = "_right",
) -> pl.DataFrame
```

General-purpose join between two DataFrames. Use `on` when both sides share the same key column name; use `left_on` + `right_on` for differently named keys.

```python
# Same key name on both sides
result = join_tables(orders, customers, on="customer_id", how="left")

# Different key names
result = join_tables(orders, lookup, left_on="prod_id", right_on="id")
```

---

### `lookup_join`

```python
def lookup_join(
    df: pl.DataFrame,
    lookup: pl.DataFrame,
    key: str | list[str],
    cols: list[str],
    how: str = "left",
) -> pl.DataFrame
```

Attach a specific subset of columns from a lookup table. Only the `key` column(s) and the listed `cols` are selected from `lookup` before joining — avoids column conflicts from wide lookup tables.

```python
# Attach product_name and category from products lookup
df = lookup_join(orders, products, key="product_id", cols=["product_name", "category"])
```

---

### `safe_join`

```python
def safe_join(
    left: pl.DataFrame,
    right: pl.DataFrame,
    on: str | list[str],
    how: str = "left",
    fill: dict | None = None,
) -> pl.DataFrame
```

Join that fills nulls introduced by the join with explicit defaults. Useful for optional enrichment where missing keys should produce a known default rather than null.

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `on` | `str \| list[str]` | — | Join key(s). |
| `how` | `str` | `"left"` | Join type. |
| `fill` | `dict \| None` | `None` | `{column_name: fill_value}` applied after the join. |

```python
df = safe_join(
    orders,
    campaign_attr.select(["order_id", "campaign_id", "channel"]),
    on="order_id",
    how="left",
    fill={"campaign_id": "unknown", "channel": "unknown"},
)
```

---

### `multi_join`

```python
def multi_join(
    base: pl.DataFrame,
    joins: list[dict],
) -> pl.DataFrame
```

Apply a sequence of joins to a base DataFrame. Each dict in `joins` is passed to `join_tables` as keyword arguments with a `"right"` key pointing to the right DataFrame.

```python
result = multi_join(orders, [
    {"right": products,  "on": "product_id",  "how": "left"},
    {"right": customers, "on": "customer_id", "how": "left"},
])
```

---

### `asof_join`

```python
def asof_join(
    left: pl.DataFrame,
    right: pl.DataFrame,
    on: str,
    by: str | list[str] | None = None,
    strategy: str = "backward",
    suffix: str = "_right",
) -> pl.DataFrame
```

Time-based / nearest-value join. Matches each row in `left` to the nearest row in `right` by `on` (typically a datetime column).

| `strategy` | Description |
| --- | --- |
| `"backward"` | Matches the last `right` value ≤ the `left` value |
| `"forward"` | Matches the first `right` value ≥ the `left` value |
| `"nearest"` | Matches the closest value in either direction |

```python
# Match each transaction to the most recent exchange rate
df = asof_join(transactions, rates, on="timestamp", by="currency", strategy="backward")
```

---

### `cross_join_filtered`

```python
def cross_join_filtered(
    left: pl.DataFrame,
    right: pl.DataFrame,
    predicate: pl.Expr,
    suffix: str = "_right",
) -> pl.DataFrame
```

Cross join followed immediately by a filter predicate. Equivalent to `SELECT ... FROM left, right WHERE predicate`.

```python
# All (customer, product) pairs where the customer's region matches the product's target_region
result = cross_join_filtered(
    customers, products,
    predicate=pl.col("region") == pl.col("target_region_right"),
)
```

---

## Window Functions

All window functions add a new column to the DataFrame without changing its row count.

### `rank_within`

```python
def rank_within(
    df: pl.DataFrame,
    partition_by: str | list[str],
    order_by: str | list[str],
    alias: str = "rank",
    descending: bool = False,
    method: str = "dense",
) -> pl.DataFrame
```

Add a rank column within each partition, ordered by one or more columns.

| `method` | Description |
| --- | --- |
| `"dense"` | Tied rows share a rank; next rank is consecutive (1, 2, 2, 3) |
| `"ordinal"` | Each row gets a unique rank (1, 2, 3, 4) |
| `"min"` | Tied rows share the lowest rank (1, 2, 2, 4) |

```python
# Rank each customer's orders newest-first (1 = most recent)
df = rank_within(df, partition_by="customer_id", order_by="order_date",
                 alias="order_recency_rank", descending=True)
```

---

### `row_number`

```python
def row_number(
    df: pl.DataFrame,
    partition_by: str | list[str],
    order_by: str | list[str],
    alias: str = "row_num",
    descending: bool = False,
) -> pl.DataFrame
```

Add a sequential 1-based row number within each partition. Equivalent to `rank_within(..., method="ordinal")`.

```python
df = row_number(df, partition_by="order_id", order_by="line_id", alias="line_num")
```

---

### `running_total`

```python
def running_total(
    df: pl.DataFrame,
    col: str,
    partition_by: str | list[str],
    order_by: str | list[str],
    alias: str | None = None,
) -> pl.DataFrame
```

Add a cumulative sum column within each partition. Default alias: `running_<col>`.

```python
# Cumulative spend per customer, ordered by order date
df = running_total(df, col="amount", partition_by="customer_id", order_by="order_date",
                   alias="cumulative_spend")
```

---

### `lag_column`

```python
def lag_column(
    df: pl.DataFrame,
    col: str,
    partition_by: str | list[str],
    order_by: str | list[str],
    n: int = 1,
    alias: str | None = None,
    fill_value=None,
) -> pl.DataFrame
```

Add a lagged (previous row's) value of a column within each partition. Default alias: `prev_<col>`.

```python
# Previous order amount per customer
df = lag_column(df, col="amount", partition_by="customer_id", order_by="order_date",
                fill_value=0.0, alias="prev_order_amount")
```

---

### `lead_column`

```python
def lead_column(
    df: pl.DataFrame,
    col: str,
    partition_by: str | list[str],
    order_by: str | list[str],
    n: int = 1,
    alias: str | None = None,
    fill_value=None,
) -> pl.DataFrame
```

Add a lead (next row's) value of a column within each partition. Default alias: `next_<col>`.

```python
df = lead_column(df, col="amount", partition_by="customer_id", order_by="order_date",
                 alias="next_order_amount")
```

---

### `pct_of_total`

```python
def pct_of_total(
    df: pl.DataFrame,
    col: str,
    partition_by: str | list[str],
    alias: str | None = None,
) -> pl.DataFrame
```

Add a column showing each row's value as a percentage of its partition total. Result is rounded to 2 decimal places. Default alias: `<col>_pct`.

```python
# What % of each product's total revenue is this order line?
df = pct_of_total(df, col="line_value", partition_by="product_id",
                  alias="line_pct_of_product")
```

---

### `rolling_avg`

```python
def rolling_avg(
    df: pl.DataFrame,
    col: str,
    partition_by: str | list[str],
    order_by: str,
    window: int,
    alias: str | None = None,
    min_periods: int | None = None,
) -> pl.DataFrame
```

Add a rolling (moving) average within each partition. Default alias: `rolling_avg_<col>`. `min_periods` defaults to `window` (requires a full window before producing a value).

```python
# 7-day moving average of daily revenue per product
df = rolling_avg(df, col="daily_revenue", partition_by="product_id",
                 order_by="date", window=7, alias="revenue_7d_avg")
```

---

### `first_last_within`

```python
def first_last_within(
    df: pl.DataFrame,
    col: str,
    partition_by: str | list[str],
    order_by: str | list[str],
    descending: bool = False,
) -> pl.DataFrame
```

Add `first_<col>` and `last_<col>` columns showing the first and last value within each partition.

```python
# First and last order amount per customer
df = first_last_within(df, col="amount", partition_by="customer_id", order_by="order_date")
# Adds: first_amount, last_amount
```

---

## Aggregation Helpers

### `attach_group_stats`

```python
def attach_group_stats(
    df: pl.DataFrame,
    group_by: str | list[str],
    col: str,
    stats: list[str],
    prefix: str | None = None,
) -> pl.DataFrame
```

Compute group-level aggregations and attach them back as new columns (window-style — row count unchanged). Supported `stats`: `"sum"`, `"mean"`, `"min"`, `"max"`, `"count"`, `"std"`.

Default column prefix: `<col>_group_`.

```python
# Attach group sum and mean of amount per customer
df = attach_group_stats(df, group_by="customer_id", col="amount",
                        stats=["sum", "mean"], prefix="cust_amount_")
# Adds: cust_amount_amount_sum, cust_amount_amount_mean
```

---

### `top_n_within`

```python
def top_n_within(
    df: pl.DataFrame,
    partition_by: str | list[str],
    order_by: str | list[str],
    n: int = 1,
    descending: bool = True,
    alias: str = "is_top_n",
) -> pl.DataFrame
```

Add a boolean column flagging the top N rows within each partition. Row count unchanged.

```python
# Flag the top 3 orders (by amount) per customer
df = top_n_within(df, partition_by="customer_id", order_by="amount", n=3, alias="is_top_3")
```

---

### `pivot_to_columns`

```python
def pivot_to_columns(
    df: pl.DataFrame,
    index: str | list[str],
    pivot_col: str,
    value_col: str,
    agg: str = "sum",
    prefix: str = "",
) -> pl.DataFrame
```

Pivot distinct values of `pivot_col` into separate columns (wide format). Equivalent to SQL `PIVOT`.

```python
# Pivot monthly revenue into one column per month
df = pivot_to_columns(df, index="product_id", pivot_col="month",
                      value_col="revenue", agg="sum", prefix="rev_")
```

---

### `unpivot_columns`

```python
def unpivot_columns(
    df: pl.DataFrame,
    index: str | list[str],
    cols: list[str],
    variable_name: str = "variable",
    value_name: str = "value",
) -> pl.DataFrame
```

Melt wide columns into long format. Inverse of `pivot_to_columns`.

```python
# Unpivot monthly revenue columns back to long format
df = unpivot_columns(df, index="product_id",
                     cols=["rev_2024-01", "rev_2024-02", "rev_2024-03"],
                     variable_name="month", value_name="revenue")
```

---

### `flag_outliers`

```python
def flag_outliers(
    df: pl.DataFrame,
    col: str,
    partition_by: str | list[str] | None = None,
    method: str = "iqr",
    multiplier: float = 1.5,
    alias: str | None = None,
) -> pl.DataFrame
```

Add a boolean column flagging statistical outliers. Default alias: `is_outlier_<col>`.

| `method` | Algorithm | Default `multiplier` |
| --- | --- | --- |
| `"iqr"` | Outside `[Q1 - k·IQR, Q3 + k·IQR]` | `1.5` |
| `"zscore"` | `abs(z-score) > k` | `1.5` (use `3.0` for stricter) |

When `partition_by` is set, statistics are computed within each group.

```python
# Flag order amounts more than 3 standard deviations from the customer mean
df = flag_outliers(df, col="amount", partition_by="customer_id",
                   method="zscore", multiplier=3.0)
```

---

## Date Helpers

### `date_trunc`

```python
def date_trunc(
    df: pl.DataFrame,
    col: str,
    unit: str,
    alias: str | None = None,
) -> pl.DataFrame
```

Truncate a date/datetime column to a given time unit. Default alias: `<col>_<unit>`.

Valid `unit` values: `"day"`, `"week"`, `"month"`, `"quarter"`, `"year"`.

```python
df = date_trunc(df, col="order_date", unit="month", alias="order_month")
# 2024-01-15 → 2024-01-01
```

---

### `days_between`

```python
def days_between(
    df: pl.DataFrame,
    start: str,
    end: str,
    alias: str | None = None,
    abs_value: bool = False,
) -> pl.DataFrame
```

Add a column with the number of days between two date columns. Default alias: `days_<start>_to_<end>`.

```python
# Days from order creation to delivery
df = days_between(df, start="created_at", end="delivered_at",
                  alias="delivery_days", abs_value=True)
```

---

### `classify_recency`

```python
def classify_recency(
    df: pl.DataFrame,
    col: str,
    alias: str = "recency_band",
    as_of: date | None = None,
    bands: list[tuple[int, str]] | None = None,
) -> pl.DataFrame
```

Add a recency band label based on how many days ago a date column was. Defaults to today as the reference date.

**Default bands:**

| Days since `col` | Label |
| --- | --- |
| ≤ 7 | `"active"` |
| ≤ 30 | `"recent"` |
| ≤ 90 | `"lapsing"` |
| > 90 | `"inactive"` |

```python
df = classify_recency(df, col="last_order_date", alias="customer_recency")

# Custom bands
df = classify_recency(
    df, col="last_login",
    bands=[(1, "daily"), (7, "weekly"), (30, "monthly"), (9999, "churned")]
)
```

---

### `add_calendar_columns`

```python
def add_calendar_columns(
    df: pl.DataFrame,
    col: str,
    parts: list[str] | None = None,
) -> pl.DataFrame
```

Extract calendar parts from a date/datetime column as separate columns. When `parts` is `None`, all parts are added.

**Available parts:**

| Part | Output column | Type |
| --- | --- | --- |
| `"year"` | `<col>_year` | `Int32` |
| `"quarter"` | `<col>_quarter` | `Int8` |
| `"month"` | `<col>_month` | `Int8` |
| `"week"` | `<col>_week` | `Int8` |
| `"day"` | `<col>_day` | `Int8` |
| `"day_of_week"` | `<col>_day_of_week` | `Int8` (0=Mon, 6=Sun) |
| `"is_weekend"` | `<col>_is_weekend` | `Boolean` |

```python
# All calendar parts
df = add_calendar_columns(df, col="order_date")

# Only year and month
df = add_calendar_columns(df, col="order_date", parts=["year", "month"])
```
