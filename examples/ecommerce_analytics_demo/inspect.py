"""inspect.py — print gold outputs with totals. Run after the pipeline."""
import sys
sys.stdout.reconfigure(encoding="utf-8")

import polars as pl
from pathlib import Path


def banner(title: str) -> None:
    print()
    print("━" * 62)
    print(f"  {title}")
    print("━" * 62)


gold = Path("data/ecommerce/gold/ecommerce")

if not gold.exists():
    print("Gold directory not found. Run the pipeline first:")
    print("  python seed.py")
    print("  medallion run ecommerce --layer silver")
    print("  medallion run ecommerce --layer gold")
    sys.exit(1)

banner("📊  REVENUE BY CATEGORY")
cat = pl.read_parquet(gold / "revenue_by_category.parquet").sort("total_revenue", descending=True)
cat = cat.with_columns(
    (pl.col("total_margin") / pl.col("total_revenue") * 100).round(1).alias("margin_pct")
)
print(cat)

banner("👤  TOP CUSTOMERS  (ranked by spend)")
cust = pl.read_parquet(gold / "top_customers.parquet").sort("total_spent", descending=True)
print(cust)

banner("📅  MONTHLY REVENUE")
monthly = pl.read_parquet(gold / "monthly_summary.parquet").sort("order_month")
print(monthly)

banner("💰  TOTALS")
total_rev    = cat["total_revenue"].sum()
total_margin = cat["total_margin"].sum()
total_orders = cat["num_orders"].sum()
print(f"  Orders  : {total_orders}")
print(f"  Revenue : ${total_rev:,.2f}")
print(f"  Margin  : ${total_margin:,.2f}  ({total_margin/total_rev*100:.1f}%)")
print()
