# local_parquet_demo — `demo`

**Zero-credential quickstart — CSV in, gold Parquet out, no cloud account needed.**

One table, one UDF, two gold aggregations. The simplest possible OpenMedallion pipeline.

---

## Pipeline Flow

```mermaid
flowchart LR
    CSV["📄 data/source/orders.csv\n15 rows"]

    subgraph bronze["🟤 Bronze"]
        B["data/bronze/ORDERS.parquet\nraw column names"]
    end

    subgraph silver["⚪ Silver"]
        S["data/silver/orders.parquet\nrenamed · cast\nis_large_order added"]
    end

    subgraph gold["🟡 Gold"]
        G1["data/gold/demo/orders_by_customer.parquet\ntotal_orders · total_spent"]
        G2["data/gold/demo/orders_by_status.parquet\nnum_orders · total_amount"]
    end

    CSV -->|seed.py| B
    B   -->|medallion run demo --layer silver| S
    S   -->|medallion run demo --layer gold| G1
    S   -->|medallion run demo --layer gold| G2
```

---

## What Happens at Each Step

### Step 1 — seed.py

Reads the bundled CSV and writes a bronze Parquet file:

| Before (CSV) | After (ORDERS.parquet) |
| --- | --- |
| Raw text rows | Columnar Parquet, same column names |
| `ORDER_ID,CUSTOMER_NAME,...` | `ORDER_ID,CUSTOMER_NAME,...` |

### Step 2 — Silver transform

`backend/silver.yaml` applies three transforms in order:

| Transform | What it does |
| --- | --- |
| `rename` | `ORDER_ID` → `order_id`, `CUSTOMER_ID` → `customer_id`, etc. |
| `cast` | `order_id` → `Int64`, `customer_id` → `Int64`, `amount` → `Float64` |
| `udf` | Calls `flag_large_orders()` — adds `is_large_order` boolean column |

Sample silver output:

| order_id | customer_id | customer_name | amount | status | is_large_order |
| --- | --- | --- | --- | --- | --- |
| 1 | 101 | Alice | 250.0 | completed | true |
| 6 | 102 | Bob | 75.0 | pending | false |
| 11 | 103 | Charlie | 200.0 | completed | true |

### Step 3 — Gold aggregation

**`orders_by_customer.parquet`** — group by `customer_id, customer_name`:

| customer_id | customer_name | total_orders | total_spent |
| --- | --- | --- | --- |
| 101 | Alice | 4 | 600.0 |
| 102 | Bob | 3 | 300.5 |
| 103 | Charlie | 3 | 415.0 |

**`orders_by_status.parquet`** — group by `status`:

| status | num_orders | total_amount |
| --- | --- | --- |
| completed | 11 | 2070.5 |
| pending | 3 | 230.0 |
| cancelled | 1 | 25.0 |

---

## Run the Demo

```bash
# From examples/local_parquet_demo/

# Step 1 — seed bronze from the bundled CSV
python seed.py

# Step 2 — silver: rename, cast, flag large orders
medallion run demo --layer silver

# Step 3 — gold: aggregate by customer and by status
medallion run demo --layer gold
```

Or open `ipynb/walkthrough.ipynb` in Jupyter — it runs everything cell by cell.

---

## Folder Structure

```text
demo/                          ← project root (run medallion from example root)
├── main.yaml                  ← pipeline name, layer includes, data paths
├── README.md                  ← this file
├── kestra_flow.yaml           ← Kestra orchestration
├── ipynb/
│   └── walkthrough.ipynb      ← guided end-to-end run
├── backend/
│   ├── bronze.yaml            ← placeholder (seed.py handles bronze for this demo)
│   ├── silver.yaml            ← rename, cast, udf transforms
│   ├── gold.yaml              ← two group_by aggregations
│   └── udf/silver/
│       └── enrich.py          ← flag_large_orders(df, threshold) → df
└── frontend/
    ├── tableau/               ← Tableau workbook files
    └── powerbi/               ← Power BI files

data/                          ← pipeline outputs (gitignored, outside project folder)
├── source/orders.csv          ← 15-row seed data
├── bronze/ORDERS.parquet
├── silver/orders.parquet
└── gold/demo/
    ├── orders_by_customer.parquet
    └── orders_by_status.parquet
```

---

## Things to Try

- Change the `threshold` arg in `backend/silver.yaml` (currently `100.0`) and re-run silver
- Add a `mean` metric to `backend/gold.yaml` and re-run gold only
- Run `medallion dag` to print the Hamilton DAG for the full pipeline
