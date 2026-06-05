"""setup_db.py — creates the SQLite HR database for the demo.

SQLite ships with Python — no Oracle install required.
Run this once before your first `medallion run oracle_hr --layer bronze`.

The bronze filter keeps all 12 employees in target departments (10, 20, 60, 80, 90),
both ACTIVE and INACTIVE.  The 3 in dept 30 / 50 are excluded at source.

For real Oracle/Postgres:
  1. Copy ../secrets.yaml.example → ../secrets.yaml and fill in your credentials.
  2. In oracle_hr/backend/bronze.yaml, comment out the connection_string lines
     and uncomment the dialect + credentials_file lines.
"""
import sys
#sys.stdout.reconfigure(encoding="utf-8")

import sqlite3
from pathlib import Path

DB = Path("oracle_hr/data/source/oracle_hr.db")
DB.parent.mkdir(parents=True, exist_ok=True)

con = sqlite3.connect(DB)
cur = con.cursor()

# ── Schema ──────────────────────────────────────────────────────────────────

cur.executescript("""
    CREATE TABLE IF NOT EXISTS departments (
        department_id   INTEGER PRIMARY KEY,
        department_name TEXT    NOT NULL,
        manager_id      INTEGER,
        location_id     INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS jobs (
        job_id      TEXT    PRIMARY KEY,
        job_title   TEXT    NOT NULL,
        min_salary  REAL    NOT NULL,
        max_salary  REAL    NOT NULL
    );

    CREATE TABLE IF NOT EXISTS employees (
        employee_id   INTEGER PRIMARY KEY,
        first_name    TEXT    NOT NULL,
        last_name     TEXT    NOT NULL,
        email         TEXT    NOT NULL,
        hire_date     TEXT    NOT NULL,
        job_id        TEXT    NOT NULL,
        salary        REAL    NOT NULL,
        manager_id    INTEGER,
        department_id INTEGER NOT NULL,
        status        TEXT    NOT NULL DEFAULT 'ACTIVE'
    );
""")

# ── Departments (7 rows; pipeline filter keeps only 5) ──────────────────────

cur.executemany("INSERT OR REPLACE INTO departments VALUES (?,?,?,?)", [
    (10, "Administration", 200, 1700),
    (20, "Marketing",      201, 1800),
    (30, "Purchasing",     114, 1700),   # excluded by department filter
    (50, "Shipping",       124, 1500),   # excluded by department filter
    (60, "IT",             103, 1400),
    (80, "Sales",          145, 2500),
    (90, "Executive",      100, 1700),
])

# ── Jobs (8 rows) ───────────────────────────────────────────────────────────

cur.executemany("INSERT OR REPLACE INTO jobs VALUES (?,?,?,?)", [
    ("AD_PRES", "President",                         20000, 40000),
    ("AD_VP",   "Administration Vice President",     15000, 30000),
    ("AD_ASST", "Administration Assistant",           3000,  6000),
    ("MK_MAN",  "Marketing Manager",                  9000, 15000),
    ("MK_REP",  "Marketing Representative",           4000,  9000),
    ("IT_PROG", "Programmer",                         4000, 10000),
    ("SA_MAN",  "Sales Manager",                     10000, 20000),
    ("SA_REP",  "Sales Representative",               6000, 12000),
])

# ── Employees (15 rows) ─────────────────────────────────────────────────────
# 12 ACTIVE in target depts (10,20,60,80,90)  → pass the bronze filter
#  1 ACTIVE in dept 30 (Purchasing)            → excluded by department filter
#  2 INACTIVE in target depts                  → excluded by status filter

cur.executemany(
    "INSERT OR REPLACE INTO employees "
    "(employee_id,first_name,last_name,email,hire_date,job_id,salary,manager_id,department_id,status) "
    "VALUES (?,?,?,?,?,?,?,?,?,?)",
    [
        # dept 90 – Executive
        (100, "Steven",    "King",     "SKING",    "2003-06-17", "AD_PRES", 24000, None, 90, "ACTIVE"),
        (101, "Neena",     "Kochhar",  "NKOCHHAR", "2005-09-21", "AD_VP",   17000, 100,  90, "ACTIVE"),
        # dept 90 – INACTIVE (excluded by status filter)
        (102, "Lex",       "De Haan",  "LDEHAAN",  "2001-01-13", "AD_VP",   17000, 100,  90, "INACTIVE"),
        # dept 60 – IT
        (103, "Alexander", "Hunold",   "AHUNOLD",  "2006-01-03", "IT_PROG",  9000, 102,  60, "ACTIVE"),
        (104, "Bruce",     "Ernst",    "BERNST",   "2007-05-21", "IT_PROG",  6000, 103,  60, "ACTIVE"),
        # dept 80 – Sales
        (145, "John",      "Russell",  "JRUSSEL",  "2004-10-01", "SA_MAN",  14000, 100,  80, "ACTIVE"),
        (146, "Karen",     "Partners", "KPARTNER", "2005-01-05", "SA_REP",  13500, 145,  80, "ACTIVE"),
        (147, "Alberto",   "Errazuriz","AERRAZUR", "2005-03-10", "SA_REP",  12000, 145,  80, "ACTIVE"),
        # dept 80 – INACTIVE (excluded by status filter)
        (148, "Gerald",    "Cambrault","GCAMBRAU", "2007-10-15", "SA_MAN",  11000, 100,  80, "INACTIVE"),
        # dept 20 – Marketing
        (201, "Michael",   "Hartstein","MHARTSTE", "2004-02-17", "MK_MAN",  13000, 100,  20, "ACTIVE"),
        (202, "Pat",       "Fay",      "PFAY",     "2005-08-17", "MK_REP",   6000, 201,  20, "ACTIVE"),
        # dept 10 – Administration
        (200, "Jennifer",  "Whalen",   "JWHALEN",  "2003-09-17", "AD_ASST",  4400, 101,  10, "ACTIVE"),
        # dept 30 – Purchasing (excluded by department filter)
        (114, "Den",       "Raphaely", "DRAPHEAL", "2002-12-07", "AD_ASST",  11000, 100, 30, "ACTIVE"),
        # dept 50 – Shipping (excluded by department filter)
        (124, "Kevin",     "Mourgos",  "KMOURGOS", "2007-11-16", "SA_REP",   5800, 100, 50, "ACTIVE"),
        (125, "Julia",     "Nayer",    "JNAYER",   "2005-07-16", "SA_REP",   3200, 124, 50, "ACTIVE"),
    ]
)

con.commit()
con.close()

print(f"✅  Database seeded at {DB}")
print()
print("   15 employees inserted:")
print("    • 12 in target departments  (10, 20, 60, 80, 90)  → ingested by bronze")
print("    •  3 in dept 30 / 50        → excluded by department filter")
print()
print("   Bronze filter: department only.  All 12 employees (ACTIVE and INACTIVE) are ingested.")
print()
print("Next steps:")
print("  medallion run oracle_hr --projects . --layer bronze")
print("  medallion run oracle_hr --projects . --layer silver")
print("  medallion run oracle_hr --projects .")
print()
print("To use a real Oracle/Postgres database:")
print("  1. cp ../secrets.yaml.example ../secrets.yaml  and fill in your credentials")
print("  2. Edit oracle_hr/backend/bronze.yaml — swap connection_string for credentials_file")
