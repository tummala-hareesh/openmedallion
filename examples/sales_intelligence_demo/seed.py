"""seed.py — generate source CSVs and write them as bronze Parquet.

Run once before the pipeline:
    cd examples/sales_intelligence_demo
    python seed.py
"""
import polars as pl
from pathlib import Path

# ── Source data ───────────────────────────────────────────────────────────────

REPS = pl.DataFrame({
    "rep_id":  [1, 2, 3, 4, 5, 6],
    "name":    ["Alice Chen", "Bob Smith", "Carol Lee", "David Kim", "Eve Brown", "Frank Wu"],
    "region":  ["North", "South", "East", "West", "North", "East"],
    "team":    ["Enterprise", "SMB", "Enterprise", "SMB", "SMB", "Enterprise"],
})

TARGETS = pl.DataFrame({
    "region":        ["North", "South", "East", "West"] * 4,
    "quarter":       (["2024-Q1"] * 4 + ["2024-Q2"] * 4
                      + ["2024-Q3"] * 4 + ["2024-Q4"] * 4),
    "target_amount": [
        80_000, 60_000, 90_000, 70_000,   # Q1
        85_000, 65_000, 95_000, 75_000,   # Q2
        90_000, 70_000,100_000, 80_000,   # Q3
        95_000, 75_000,105_000, 85_000,   # Q4
    ],
})

TRANSACTIONS = pl.DataFrame({
    "txn_id":     list(range(1, 61)),
    "rep_id":     [
        1, 2, 3, 4, 5, 6, 1, 2, 3, 4,   # Jan
        5, 6, 1, 2, 3, 4, 5, 6, 1, 2,   # Feb
        3, 4, 5, 6, 1, 2, 3, 4, 5, 6,   # Mar
        1, 2, 3, 4, 5, 6, 1, 2, 3, 4,   # Apr
        5, 6, 1, 2, 3, 4, 5, 6, 1, 2,   # May
        3, 4, 5, 6, 1, 2, 3, 4, 5, 6,   # Jun
    ],
    "product": [
        "SaaS Basic", "SaaS Pro", "SaaS Enterprise", "SaaS Pro", "SaaS Basic", "SaaS Enterprise",
        "SaaS Enterprise", "SaaS Basic", "SaaS Pro", "SaaS Enterprise",
        "SaaS Pro", "SaaS Basic", "SaaS Pro", "SaaS Enterprise", "SaaS Basic", "SaaS Pro",
        "SaaS Enterprise", "SaaS Pro", "SaaS Basic", "SaaS Enterprise",
        "SaaS Basic", "SaaS Pro", "SaaS Enterprise", "SaaS Basic", "SaaS Pro", "SaaS Basic",
        "SaaS Enterprise", "SaaS Pro", "SaaS Basic", "SaaS Enterprise",
        "SaaS Pro", "SaaS Enterprise", "SaaS Basic", "SaaS Enterprise", "SaaS Pro", "SaaS Basic",
        "SaaS Enterprise", "SaaS Basic", "SaaS Pro", "SaaS Enterprise",
        "SaaS Basic", "SaaS Pro", "SaaS Enterprise", "SaaS Pro", "SaaS Basic", "SaaS Enterprise",
        "SaaS Pro", "SaaS Enterprise", "SaaS Basic", "SaaS Pro",
        "SaaS Enterprise", "SaaS Basic", "SaaS Pro", "SaaS Enterprise", "SaaS Basic", "SaaS Pro",
        "SaaS Enterprise", "SaaS Basic", "SaaS Pro", "SaaS Enterprise",
    ],
    "amount": [
        12_000, 28_000, 85_000, 22_000,  9_500, 75_000,
        92_000, 11_000, 31_000, 88_000,
        26_000,  8_000, 33_000, 95_000, 14_000, 29_000,
        78_000, 35_000, 10_500, 91_000,
        15_000, 24_000, 82_000, 13_000, 37_000, 16_000,
        89_000, 27_000, 11_500, 76_000,
        32_000, 87_000, 18_000, 93_000, 21_000, 14_500,
        84_000, 19_000, 25_000, 96_000,
        17_000, 22_000, 79_000, 30_000, 12_500, 88_000,
        28_000, 98_000, 16_500, 34_000,
        83_000, 20_000, 31_000, 91_000, 15_000, 38_000,
        86_000, 23_000, 29_000, 102_000,
    ],
    "units": [
        1, 1, 1, 1, 2, 1,  1, 2, 1, 1,
        1, 3, 1, 1, 2, 1,  1, 1, 2, 1,
        2, 1, 1, 2, 1, 3,  1, 1, 2, 1,
        1, 1, 2, 1, 1, 3,  1, 2, 1, 1,
        2, 1, 1, 1, 2, 1,  1, 1, 3, 1,
        1, 2, 1, 1, 2, 1,  1, 2, 1, 1,
    ],
    "txn_date": [
        "2024-01-08", "2024-01-11", "2024-01-15", "2024-01-19", "2024-01-22", "2024-01-26",
        "2024-01-29", "2024-02-02", "2024-02-06", "2024-02-09",
        "2024-02-13", "2024-02-17", "2024-02-20", "2024-02-24", "2024-02-27", "2024-03-02",
        "2024-03-06", "2024-03-09", "2024-03-13", "2024-03-17",
        "2024-03-20", "2024-03-23", "2024-03-27", "2024-03-30", "2024-04-03", "2024-04-07",
        "2024-04-10", "2024-04-14", "2024-04-17", "2024-04-21",
        "2024-04-24", "2024-04-28", "2024-05-01", "2024-05-05", "2024-05-08", "2024-05-12",
        "2024-05-15", "2024-05-19", "2024-05-22", "2024-05-26",
        "2024-05-29", "2024-06-02", "2024-06-05", "2024-06-09", "2024-06-12", "2024-06-16",
        "2024-06-19", "2024-06-23", "2024-06-26", "2024-06-30",
        "2024-07-03", "2024-07-07", "2024-07-10", "2024-07-14", "2024-07-17", "2024-07-21",
        "2024-07-24", "2024-07-28", "2024-07-31", "2024-08-04",
    ],
    "stage": (["closed_won"] * 55) + (["closed_won"] * 4) + ["closed_won"],
})

# ── Write source CSVs ─────────────────────────────────────────────────────────

src = Path("data/source")
src.mkdir(parents=True, exist_ok=True)

REPS.write_csv(src / "reps.csv")
TARGETS.write_csv(src / "targets.csv")
TRANSACTIONS.write_csv(src / "transactions.csv")

print("📄  Source CSVs written to data/source/")
for name, df in [("reps", REPS), ("targets", TARGETS), ("transactions", TRANSACTIONS)]:
    print(f"    {name}.csv  — {len(df)} rows × {len(df.columns)} cols")

# ── Write bronze Parquet ──────────────────────────────────────────────────────

bronze = Path("data/bronze")
bronze.mkdir(parents=True, exist_ok=True)

print()
for name, df in [("reps", REPS), ("targets", TARGETS), ("transactions", TRANSACTIONS)]:
    out = bronze / f"{name}.parquet"
    df.write_parquet(out)
    print(f"📥  {len(df):>2} rows → {out}")

print()
print("✅  Bronze ready.")
print()
print("Next steps:")
print("  medallion run sales_intel --layer silver")
print("  medallion run sales_intel --layer gold")
print("  python show_cerebrum.py")
