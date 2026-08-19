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

## RAG accuracy — the worked `metadata.yaml` / `relationships.yaml` example

`oracle_hr/metadata.yaml` and `oracle_hr/relationships.yaml` are hand-written worked
examples referenced from the main project's RAG accuracy roadmap — including a
self-referential relationship (`employees.manager_id → employees.employee_id`,
employees managing employees). No LLM call is required to inspect or render them:

```bash
medallion relationships erd oracle_hr --projects .           # approved relationships only
medallion relationships erd oracle_hr --projects . --all     # include draft/stale too
```

Renders a Mermaid `erDiagram` to `oracle_hr/relationships_erd.md`. To review/approve
metadata or relationships interactively (no LLM needed either):

```bash
medallion metadata approve      oracle_hr --projects .
medallion relationships approve oracle_hr --projects .
```

Regenerating drafts (`metadata generate`, `examples generate`) requires
`pip install "openmedallion[cerebrum]"` and a running LLM — see
[docs/guides/rag-accuracy.md](https://github.com/tummala-hareesh/openmedallion/blob/main/docs/guides/rag-accuracy.md)
for the full design, or
[examples/sales_intelligence_demo/](https://github.com/tummala-hareesh/openmedallion/tree/main/examples/sales_intelligence_demo)
for a runnable, offline RAG walkthrough.

---

## Walkthrough notebook

Open `oracle_hr/ipynb/walkthrough.ipynb` for a guided run with inline output.
