# 🔄 incremental_sql_demo

**Append and merge incremental loads from a local SQLite database — no credentials required.**

This example shows how OpenMedallion handles growing datasets. Run the pipeline twice: the second run loads **only the new and changed rows**, not the full table.

---

## 🔄 Incremental Modes

```mermaid
flowchart TD
    subgraph first["First run (full load)"]
        direction LR
        DB1["SQLite\n5 orders · 3 customers"] -->|"SELECT * WHERE created_at >= '2024-01-01'"| dlt1["dlt"]
        dlt1 -->|"5 rows"| B1["orders.parquet\n5 rows"]
        dlt1 -->|"cursor saved: 2024-03-15"| S1["dlt state"]
        DB1 -->|"SELECT * (full scan)"| dlt2["dlt"]
        dlt2 -->|"3 rows"| B2["customers.parquet\n3 rows"]
    end

    subgraph delta["Delta run (after add_delta.py)"]
        direction LR
        DB2["SQLite\n7 orders · 3 customers\nCharlie: bronze→platinum"] -->|"SELECT * WHERE created_at > 2024-03-15"| dlt3["dlt"]
        dlt3 -->|"2 new rows only"| B3["orders.parquet\n+2 rows appended"]
        DB2 -->|"SELECT * (merge on customer_id)"| dlt4["dlt"]
        dlt4 -->|"Charlie row updated"| B4["customers.parquet\nupserted"]
    end
```

| Table | Mode | Key | Behaviour |
| --- | --- | --- | --- |
| `orders` | `append` | `cursor_column: created_at` | Only rows newer than the last run are loaded |
| `customers` | `merge` | `primary_key: customer_id` | Full upsert — updates overwrite, new rows insert |

---

## 📊 What the Data Looks Like

**Initial seed (`setup_db.py`):**

| order_id | customer_id | amount | created_at |
| --- | --- | --- | --- |
| 1 | 101 | 120.00 | 2024-01-10 |
| 2 | 102 | 340.00 | 2024-02-05 |
| 3 | 103 | 89.50 | 2024-02-20 |
| 4 | 101 | 210.00 | 2024-03-01 |
| 5 | 102 | 55.00 | 2024-03-15 |

**After delta (`add_delta.py`):**

| Δ | order_id | customer_id | amount | created_at |
| --- | --- | --- | --- | --- |
| ➕ new | 6 | 103 | 175.00 | 2024-04-02 |
| ➕ new | 7 | 101 | 430.00 | 2024-04-10 |

| Δ | customer_id | name | tier |
| --- | --- | --- | --- |
| ✏️ updated | 103 | Charlie | bronze → **platinum** |

On the second bronze run:

- `orders`: dlt reads cursor `2024-03-15` and fetches only the 2 new rows
- `customers`: dlt merges on `customer_id` — Charlie's tier is updated, others unchanged

---

## 🚀 Run the Demo

```bash
# From this directory (examples/incremental_sql_demo/)

# Step 1 — create SQLite database with seed data
python setup_db.py

# Step 2 — full bronze load (5 orders, 3 customers)
medallion run retail --layer bronze

# Step 3 — silver + gold
medallion run retail

# Inspect first-run gold output
python -c "
import polars as pl
print(pl.read_parquet('data/retail/gold/retail/customer_summary.parquet').sort('customer_id'))
print(pl.read_parquet('data/retail/gold/retail/pipeline_totals.parquet'))
"
```

---

## 🔁 Delta Load — Observe Incremental Behaviour

```bash
# Add 2 new orders + update Charlie's tier
python add_delta.py

# Bronze: only picks up 2 new orders + merges Charlie's customer row
medallion run retail --layer bronze

# Re-run silver + gold
medallion run retail

# Verify: Charlie now shows 2 orders; totals reflect the 2 new rows
python -c "
import polars as pl
print(pl.read_parquet('data/retail/gold/retail/customer_summary.parquet').sort('customer_id'))
print(pl.read_parquet('data/retail/gold/retail/pipeline_totals.parquet'))
"
```

---

## 📊 Expected Output After Full Pipeline

### `customer_summary.parquet`

| customer_id | total_orders | total_spent |
| --- | --- | --- |
| 101 | 2 | 330.0 |
| 102 | 2 | 395.0 |
| 103 | 1 | 89.5 |

### `pipeline_totals.parquet`

| total_orders | total_revenue |
| --- | --- |
| 5 | 814.5 |

After the delta run: `total_orders = 7`, `total_revenue = 1419.5`, Charlie shows `2` orders.

---

## 🗂️ Project Layout

```text
projects/retail/
├── main.yaml       # pipeline name + paths
├── bronze.yaml     # SQLite source + append/merge incremental config
├── silver.yaml     # type casts for orders and customers
└── gold.yaml       # customer summary + grand-total aggregations
```

---

## 🔑 How Incremental State Is Tracked

dlt writes a cursor state file alongside the bronze Parquet shards:

```text
data/retail/bronze/bronze/orders/_dlt_loads/
```

Delete this directory to force a full reload on the next bronze run:

```bash
rm -rf data/retail/bronze/bronze/orders/_dlt_loads/
```

---

## 🔍 Things to Try

- Add a third order in `add_delta.py` and observe that only it is picked up
- Change `initial_value` in `bronze.yaml` and delete `_dlt_loads/` to reload from a different date
- Add a `max` metric for `amount` to `gold.yaml` and re-run gold only
