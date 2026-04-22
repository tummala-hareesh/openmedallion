# local_parquet_demo

Zero-credential quickstart: full silver → gold pipeline reading from a bundled CSV file.
No cloud account, no database driver, no environment variables required.

## What this demo covers

| Step | Command | Result |
|------|---------|--------|
| 1. Seed bronze | `python seed.py` | Converts `data/source/orders.csv` → `data/demo/bronze/ORDERS.parquet` |
| 2. Run silver | `medallion run demo --layer silver` | Renames columns, casts types, adds `is_large_order` flag |
| 3. Run gold | `medallion run demo --layer gold` | Two aggregations: by customer and by status |

## Prerequisites

```bash
pip install openmedallion
```

## Run the demo

```bash
# From this directory (examples/local_parquet_demo/)

# Step 1 — seed the bronze layer from the bundled CSV
python seed.py

# Step 2 — silver transform (reads bronze, applies UDF)
medallion run demo --layer silver

# Step 3 — gold aggregation (reads silver)
medallion run demo --layer gold
```

## Expected output files

```
data/demo/
├── bronze/
│   └── ORDERS.parquet           # seeded by seed.py (15 rows, raw column names)
├── silver/
│   └── orders.parquet           # renamed, cast, + is_large_order flag
└── gold/
    └── demo/
        ├── orders_by_customer.parquet   # total_orders, total_spent per customer
        └── orders_by_status.parquet     # num_orders, total_amount per status
```

## Project layout

```
projects/demo/
├── main.yaml                    # pipeline name + paths
├── silver.yaml                  # column renames, casts, UDF reference
├── gold.yaml                    # two aggregation specs
└── udf/silver/enrich.py         # flags orders where amount >= threshold
```

## Read the results

```python
import polars as pl

customers = pl.read_parquet("data/demo/gold/demo/orders_by_customer.parquet")
print(customers)

status = pl.read_parquet("data/demo/gold/demo/orders_by_status.parquet")
print(status)
```

## Explore further

- Edit `gold.yaml` to add a new aggregation metric and re-run step 3.
- Edit `udf/silver/enrich.py` to change the `threshold` logic and re-run step 2.
- Try `medallion dag` to see the full Hamilton DAG printed as a text tree.
