"""seed.py — converts the bundled source CSV to a bronze Parquet file.

Run this once before using `medallion run demo --layer silver`.
"""
import sys
sys.stdout.reconfigure(encoding="utf-8")

import polars as pl
from pathlib import Path

src  = Path("data/source/orders.csv")
dest = Path("data/demo/bronze")
dest.mkdir(parents=True, exist_ok=True)

df = pl.read_csv(src)
df.write_parquet(dest / "ORDERS.parquet")
print(f"✅  Seeded {len(df)} rows → {dest / 'ORDERS.parquet'}")
print()
print("Next steps:")
print("  medallion run demo --layer silver   # transform bronze → silver")
print("  medallion run demo --layer gold     # aggregate silver → gold")
