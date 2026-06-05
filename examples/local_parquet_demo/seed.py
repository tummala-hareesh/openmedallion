"""seed.py — generate source CSV and convert it into bronze Parquet.

Run this once before the pipeline:
    cd examples/local_parquet_demo
    python seed.py

Two steps:
  1. Write data/source/orders.csv  (raw source data)
  2. Copy to data/bronze/ORDERS.parquet  (bronze layer)

Column names stay uppercase — silver.yaml renames them to lowercase.
"""
import polars as pl
from pathlib import Path

# ---------------------------------------------------------------------------
# Source data — 15 orders, 3 customers, 3 statuses
# ---------------------------------------------------------------------------

ORDERS = pl.DataFrame({
    "ORDER_ID":     list(range(1, 16)),
    "CUSTOMER_ID":  [101,102,103, 101,102,103, 101,102,103, 101,102,103, 101,102,103],
    "CUSTOMER_NAME":["Alice","Bob","Charlie","Alice","Bob","Charlie",
                     "Alice","Bob","Charlie","Alice","Bob","Charlie",
                     "Alice","Bob","Charlie"],
    "AMOUNT":       [250.0, 45.5, 130.0,
                      80.0,180.0, 200.0,
                     120.0, 75.0,  85.0,
                     150.0, 25.0,  60.0,
                      95.0,120.0, 110.0],
    "STATUS":       ["completed","completed","completed",
                     "pending",  "completed","completed",
                     "completed","pending",  "completed",
                     "completed","cancelled","pending",
                     "completed","completed","completed"],
})

# ---------------------------------------------------------------------------
# Write source CSV
# ---------------------------------------------------------------------------

src = Path("data/source")
src.mkdir(parents=True, exist_ok=True)

ORDERS.write_csv(src / "orders.csv")
print(f"📄  Source CSV written → data/source/orders.csv  ({len(ORDERS)} rows)")
print()
print(ORDERS.head(5))

# ---------------------------------------------------------------------------
# Convert to bronze Parquet
# ---------------------------------------------------------------------------

bronze = Path("data/bronze")
bronze.mkdir(parents=True, exist_ok=True)

ORDERS.write_parquet(bronze / "ORDERS.parquet")
print(f"\n📥  {len(ORDERS)} rows → data/bronze/ORDERS.parquet")
print()
print("✅  Bronze ready.")
print()
print("Next steps:")
print("  medallion run demo --layer silver")
print("  medallion run demo --layer gold")
