"""seed.py — converts source CSVs into bronze Parquet files.

Column names are uppercased to match what a real dlt sql_database source would
produce (e.g. Oracle, SQL Server). The silver.yaml rename transforms then
normalise them to lowercase for the rest of the pipeline.
"""
import sys
sys.stdout.reconfigure(encoding="utf-8")

import polars as pl
from pathlib import Path

src  = Path("data/source")
dest = Path("data/bronze")
dest.mkdir(parents=True, exist_ok=True)

for name in ("orders", "products", "customers"):
    df  = pl.read_csv(src / f"{name}.csv")
    # Uppercase column names — mirrors dlt sql_database normalisation
    df  = df.rename({col: col.upper() for col in df.columns})
    out = dest / f"{name.upper()}.parquet"
    df.write_parquet(out)
    print(f"✅  {len(df):>3} rows → {out}")

print()
print("Bronze ready. Next steps:")
print("  medallion run ecommerce --layer silver")
print("  medallion run ecommerce --layer gold")
