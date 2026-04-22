"""setup_db.py — creates a local SQLite database with seed data.

SQLite ships with Python — no driver installation required.
Run this once before your first `medallion run retail --layer bronze`.
"""
import sys
sys.stdout.reconfigure(encoding="utf-8")

import sqlite3
from pathlib import Path

DB = Path("retail/data/retail.db")
DB.parent.mkdir(parents=True, exist_ok=True)

con = sqlite3.connect(DB)
cur = con.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        order_id    INTEGER PRIMARY KEY,
        customer_id INTEGER NOT NULL,
        amount      REAL    NOT NULL,
        created_at  TEXT    NOT NULL
    )
""")

cur.execute("""
    CREATE TABLE IF NOT EXISTS customers (
        customer_id INTEGER PRIMARY KEY,
        name        TEXT NOT NULL,
        email       TEXT NOT NULL,
        tier        TEXT NOT NULL
    )
""")

cur.executemany("INSERT OR REPLACE INTO orders VALUES (?,?,?,?)", [
    (1, 101, 150.00, "2024-01-05"),
    (2, 102,  45.50, "2024-01-06"),
    (3, 101, 300.00, "2024-01-07"),
    (4, 103,  80.00, "2024-01-08"),
    (5, 102, 200.00, "2024-01-09"),
])

cur.executemany("INSERT OR REPLACE INTO customers VALUES (?,?,?,?)", [
    (101, "Alice",   "alice@example.com",   "gold"),
    (102, "Bob",     "bob@example.com",     "silver"),
    (103, "Charlie", "charlie@example.com", "bronze"),
])

con.commit()
con.close()

print(f"✅  Database seeded at {DB}  (5 orders, 3 customers)")
print()
print("Next steps:")
print("  medallion run retail --layer bronze   # ingest into bronze")
print("  medallion run retail                  # silver + gold (default)")
print()
print("Then simulate a delta load:")
print("  python add_delta.py")
print("  medallion run retail --layer bronze   # only new rows loaded")
