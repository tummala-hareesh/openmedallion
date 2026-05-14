"""seed.py — generate source CSVs and convert them into bronze Parquet files.

Run this once before the pipeline:
    cd examples/ecommerce_analytics_demo
    python seed.py

Two steps:
  1. Write data/source/{orders,products,customers}.csv  (raw source data)
  2. Copy to data/bronze/{orders,products,customers}.parquet  (bronze layer)

Column names stay lowercase — this demo uses local_files, not a SQL source,
so dlt's uppercase normalisation does not apply here.
"""
import sys
#sys.stdout.reconfigure(encoding="utf-8")

import polars as pl
from pathlib import Path

# ---------------------------------------------------------------------------
# Source data
# ---------------------------------------------------------------------------

PRODUCTS = pl.DataFrame({
    "product_id":   [1, 2, 3, 4, 5, 6, 7, 8],
    "product_name": [
        "Laptop Pro 15", "Wireless Headphones", "Webcam HD",
        "Winter Jacket", "Cotton T-Shirt", "Running Sneakers",
        "Python Mastery", "Data Science Handbook",
    ],
    "category":   ["Electronics", "Electronics", "Electronics",
                   "Clothing",    "Clothing",    "Clothing",
                   "Books",       "Books"],
    "unit_price": [1200.0, 150.0, 80.0, 120.0, 30.0, 90.0, 45.0, 55.0],
    "unit_cost":  [ 900.0,  80.0, 40.0,  60.0, 12.0, 45.0, 20.0, 25.0],
})

CUSTOMERS = pl.DataFrame({
    "customer_id": [1, 2, 3, 4, 5],
    "name":        ["Alice Chen", "Bob Smith", "Carol Lee", "David Kim", "Eve Brown"],
    "region":      ["US-West", "US-East", "Europe", "US-West", "Asia"],
    "tier":        ["gold", "silver", "gold", "bronze", "silver"],
})

ORDERS = pl.DataFrame({
    "order_id":    list(range(1, 21)),
    "product_id":  [1, 2, 4, 7,  1, 5, 6, 8, 3,  2, 4, 7, 6, 8, 5, 3,  2, 5, 7, 6],
    "customer_id": [1, 1, 3, 2,  3, 4, 5, 2, 1,  3, 4, 5, 1, 2, 3, 5,  4, 1, 2, 5],
    "qty":         [1, 2, 2, 3,  1, 5, 2, 1, 3,  2, 1, 2, 1, 2, 3, 1,  1, 5, 1, 1],
    "order_date":  [
        "2024-01-05", "2024-01-12", "2024-01-18", "2024-01-25",
        "2024-02-03", "2024-02-08", "2024-02-14", "2024-02-20", "2024-02-27",
        "2024-03-05", "2024-03-08", "2024-03-12", "2024-03-15",
        "2024-03-20", "2024-03-22", "2024-03-28",
        "2024-04-02", "2024-04-10", "2024-04-15", "2024-04-22",
    ],
    "status": (["completed"] * 19) + ["completed"],
})

# ---------------------------------------------------------------------------
# Write source CSVs
# ---------------------------------------------------------------------------

src = Path("data/source")
src.mkdir(parents=True, exist_ok=True)

ORDERS.write_csv(src / "orders.csv")
PRODUCTS.write_csv(src / "products.csv")
CUSTOMERS.write_csv(src / "customers.csv")

print("📄  Source CSVs written to data/source/")
for name in ("orders", "products", "customers"):
    df = pl.read_csv(src / f"{name}.csv")
    print(f"    {name}.csv  — {len(df)} rows × {len(df.columns)} cols")

# ---------------------------------------------------------------------------
# Convert to bronze Parquet
# ---------------------------------------------------------------------------

bronze = Path("data/bronze")
bronze.mkdir(parents=True, exist_ok=True)

print()
for name, df in [("orders", ORDERS), ("products", PRODUCTS), ("customers", CUSTOMERS)]:
    out = bronze / f"{name}.parquet"
    df.write_parquet(out)
    print(f"📥  {len(df):>2} rows → {out}")

print()
print("✅  Bronze ready.")
print()
print("Next steps:")
print("  medallion run ecommerce --layer silver")
print("  medallion run ecommerce --layer gold")
print("  (or run the full pipeline: medallion run ecommerce)")
