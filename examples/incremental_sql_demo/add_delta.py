"""add_delta.py — simulates a delta load: new orders + updated customer tier.

Run this after the first full pipeline run, then re-run bronze to observe:
  - orders: only the 2 new rows appended (cursor-based incremental)
  - customers: customer 103 updated in-place (primary-key merge)
"""
import sys
sys.stdout.reconfigure(encoding="utf-8")

import sqlite3

con = sqlite3.connect("data/retail.db")
cur = con.cursor()

cur.executemany("INSERT OR REPLACE INTO orders VALUES (?,?,?,?)", [
    (6, 101,  90.00, "2024-02-01"),
    (7, 103, 450.00, "2024-02-02"),
])

cur.execute("UPDATE customers SET tier=? WHERE customer_id=?", ("platinum", 103))

con.commit()
con.close()

print("✅  Added 2 new orders (IDs 6, 7) and promoted Charlie to 'platinum' tier.")
print()
print("Re-run bronze to pick up only the delta:")
print("  medallion run retail --layer bronze")
print("  medallion run retail")
