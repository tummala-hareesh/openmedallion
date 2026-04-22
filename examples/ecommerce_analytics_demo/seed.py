"""seed.py — converts source CSVs into bronze Parquet files."""
import sys
sys.stdout.reconfigure(encoding="utf-8")

import polars as pl
from pathlib import Path

src  = Path("data/source")
dest = Path("ecommerce/data/bronze")
dest.mkdir(parents=True, exist_ok=True)

for name in ("orders", "products", "customers"):
    df  = pl.read_csv(src / f"{name}.csv")
    out = dest / f"{name.upper()}.parquet"
    df.write_parquet(out)
    print(f"✅  {len(df):>3} rows → {out}")

print()
print("Bronze ready. Next steps:")
print("  medallion run ecommerce --layer silver")
print("  medallion run ecommerce --layer gold")
print("  python inspect.py")
