# oracle_hr_demo

Incremental Bronze → Silver → Gold pipeline against an Oracle-style HR schema.
Ships with a SQLite copy of the schema — no Oracle install required for the demo.

---

## Quick start (SQLite demo — zero credentials)

```bash
# From this directory (examples/oracle_hr_demo/)

# Step 1 — seed the SQLite DB and create secrets.yaml
python setup_db.py

# Step 2 — full pipeline
medallion run oracle_hr --projects .

# Or layer-by-layer
medallion run oracle_hr --projects . --layer bronze
medallion run oracle_hr --projects . --layer silver
medallion run oracle_hr --projects . --layer gold
```

---

## Connecting to a real Oracle / Postgres database

1. Edit `secrets.yaml` (created by `setup_db.py`) — fill in your `oracle:` or `postgres:` block.
2. Change `dialect:` in `oracle_hr/backend/bronze.yaml` to `oracle` (or `postgres`, `mysql`, `mssql`).
3. Install the driver:

```bash
pip install "openmedallion[oracle]"   # for Oracle
```

4. Re-run bronze — the `filter:` clause passes straight through to the real DB query.

---

## What the pipeline demonstrates

| Feature | Where |
|---------|-------|
| `filter:` — SQL WHERE pushed to source at ingestion | `bronze.yaml` employees table |
| `select:` — column pruning (drops PII email + status) | `bronze.yaml` employees table |
| Incremental `append` on `hire_date` cursor | `bronze.yaml` employees table |
| Incremental `merge` on primary key | `bronze.yaml` departments + jobs |
| Silver derived UDF — 3-table join → `employees_enriched` | `udf/silver/enrich.py` |
| Gold pre-agg UDF — `salary_pct_of_max` | `udf/gold/metrics.py` |
| `credentials_file:` — structured credential YAML | `bronze.yaml` + `secrets.yaml` |

---

## Walkthrough notebook

Open `oracle_hr/ipynb/walkthrough.ipynb` for a guided run with inline output.
