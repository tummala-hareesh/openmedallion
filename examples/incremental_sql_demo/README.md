# incremental_sql_demo

Shows append + merge incremental loading from a local SQLite database.
No credentials required — SQLite ships with Python.

Re-run the pipeline after adding delta data to observe that only changed rows are
loaded on subsequent bronze runs.

## Incremental modes demonstrated

| Table | Mode | Behaviour |
|-------|------|-----------|
| `orders` | `append` (cursor: `created_at`) | Only rows newer than the last run are loaded |
| `customers` | `merge` (key: `customer_id`) | Rows are upserted — updates overwrite, inserts add |

## Prerequisites

```bash
pip install openmedallion
```

## First run

```bash
# From this directory (examples/incremental_sql_demo/)

# Step 1 — create the SQLite database with seed data
python setup_db.py

# Step 2 — ingest bronze (dlt loads both tables)
medallion run retail --layer bronze

# Step 3 — silver + gold (default layer)
medallion run retail
```

After step 3, inspect:
```
data/retail/gold/retail/
├── customer_summary.parquet    # total_orders, total_spent per customer_id
└── pipeline_totals.parquet     # grand-total order count and revenue
```

## Delta load — observe incremental behaviour

```bash
# Add 2 new orders and update one customer's tier
python add_delta.py

# Re-run bronze — only the 2 new orders are appended; Charlie's tier is merged
medallion run retail --layer bronze

# Re-run silver + gold to reflect the new data
medallion run retail
```

Read the updated gold output:
```python
import polars as pl

summary = pl.read_parquet("data/retail/gold/retail/customer_summary.parquet")
print(summary.sort("customer_id"))
# Charlie (103) now shows 2 orders; totals reflect the new rows

totals = pl.read_parquet("data/retail/gold/retail/pipeline_totals.parquet")
print(totals)
# total_orders: 7, total_revenue updated
```

## Project layout

```
projects/retail/
├── main.yaml      # pipeline name + paths
├── bronze.yaml    # SQLite source with append + merge incremental modes
├── silver.yaml    # type casts for both tables
└── gold.yaml      # customer summary + grand-total aggregations
```

## How incremental state is tracked

dlt stores a cursor state file alongside the bronze Parquet shards:
```
data/retail/bronze/bronze/orders/_dlt_loads/
```
Delete this directory to force a full reload on the next bronze run.
