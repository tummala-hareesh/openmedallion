# sales_intelligence_demo

**Natural-language queries, interactive explore reports, and a full bronze → silver → gold pipeline — one self-contained example.**

This demo is the reference for all features added after 2026.5.4:

| Feature | Where |
|---|---|
| `cerebrum` — LLM → DuckDB SQL → Polars result | `show_cerebrum.py` |
| `explore: walker` (interactive pygwalker report) | `silver.yaml`, `gold.yaml` |
| Silver derived UDF (three-table join + attainment %) | `udf/silver/enrich.py` |
| Gold pre-agg UDF (pass-through / hook) | `udf/gold/metrics.py` |
| `explore: profile` on bronze, silver, gold | `bronze.yaml`, `silver.yaml`, `gold.yaml` |

---

## Data Model

```mermaid
erDiagram
    TRANSACTIONS {
        int    txn_id    PK
        int    rep_id    FK
        string product
        float  amount
        int    units
        string txn_date
        string stage
    }
    REPS {
        int    rep_id   PK
        string name
        string region
        string team
    }
    TARGETS {
        string region   PK
        string quarter  PK
        float  target_amount
    }
    TRANSACTIONS }o--|| REPS    : "rep_id"
    TRANSACTIONS }o--|| TARGETS : "region + quarter"
```

---

## Pipeline Flow

```mermaid
flowchart LR
    subgraph src["📂 Source CSVs"]
        T["transactions.csv\n60 rows"]
        R["reps.csv\n6 rows"]
        G["targets.csv\n16 rows"]
    end

    subgraph bronze["🟤 Bronze"]
        BT["transactions.parquet\n+ profile report"]
        BR["reps.parquet\n+ profile report"]
        BG["targets.parquet\n+ profile report"]
    end

    subgraph silver["⚪ Silver"]
        ST["transactions.parquet\ncast · profile report"]
        SR["reps.parquet\ncast · walker report"]
        SG["targets.parquet\ncast"]
        RP["rep_performance.parquet\njoined · attainment_pct\nwalker + profile reports"]
    end

    subgraph gold["🟡 Gold"]
        G1["regional_summary.parquet\nprofile + walker reports"]
        G2["rep_leaderboard.parquet\nprofile report"]
        G3["quarterly_trend.parquet\nprofile report"]
    end

    T -->|seed.py| BT
    R -->|seed.py| BR
    G -->|seed.py| BG

    BT --> ST
    BR --> SR
    BG --> SG

    ST & SR & SG -->|udf: build_rep_performance| RP

    RP --> G1
    RP --> G2
    RP -->|pre_agg_udf: add_quarter| G3
```

---

## Source Data

**reps.csv** — 6 sales reps across 2 teams and 4 regions:

| rep_id | name | region | team |
|---|---|---|---|
| 1 | Alice Chen | North | Enterprise |
| 2 | Bob Smith | South | SMB |
| 3 | Carol Lee | East | Enterprise |
| 4 | David Kim | West | SMB |
| 5 | Eve Brown | North | SMB |
| 6 | Frank Wu | East | Enterprise |

**targets.csv** — 16 rows: regional quarterly targets for Q1–Q4 2024.

**transactions.csv** — 60 closed-won deals across Q1–Q3 2024.

---

## Explore Reports

This demo uses both `profile` and `walker` report types:

| Layer | Table | Report type | What you learn |
|---|---|---|---|
| Bronze | transactions | `profile` | Data quality, null rates, distributions |
| Bronze | reps | `profile` | Team/region breakdown |
| Silver | reps | `walker` | Drag-and-drop exploration of rep attributes |
| Silver | rep_performance | `walker` | Interactive join exploration (reps × targets) |
| Silver | rep_performance | `profile` | Quality check on the derived join |
| Gold | regional_summary | `walker` | Interactive regional comparison |
| Gold | rep_leaderboard | `profile` | Top-rep distribution |

Run the pipeline with explore reports (requires `openmedallion[profile]` + `openmedallion[explore]`):

```bash
medallion run sales_intel   # no --no-explore flag
```

Reports are written to `data/<layer>/add-ons/`.

---

## cerebrum — Natural Language Queries

`show_cerebrum.py` demonstrates the cerebrum components without Ollama:

```bash
python show_cerebrum.py
```

It shows:
1. **Schema context** — the DDL string the LLM receives for all 4 silver tables
2. **Prompt preview** — how the full prompt looks with schema + few-shot examples
3. **Three live DuckDB queries** — executed directly against silver Parquet files
4. **Recommender output** — canonical question string (mock LLM)

### Sample questions you can ask with a real LLM

```
Which rep had the highest revenue in Q2?
Show me the quarterly trend for the North region
Which team — Enterprise or SMB — closed more deals?
Who is closest to hitting their quarterly target?
Compare attainment across regions in Q1 vs Q2
```

---

## Run the Demo

```bash
# From this directory (examples/sales_intelligence_demo/)

# Step 1 — seed bronze
python seed.py

# Step 2 — run full pipeline
medallion run sales_intel

# Step 3 — show cerebrum demo (no Ollama needed)
python show_cerebrum.py

# Step 4 — go live with Ollama (optional)
# Terminal 1:
medallion ask sales_intel

# Terminal 2:
medallion cortex sales_intel
# → open http://localhost:8050
```

---

## Expected Results

### `regional_summary.parquet`

| region | total_revenue | num_deals |
|---|---|---|
| East | 993,000 | 20 |
| North | 744,000 | 20 |
| West | 510,000 | 10 |
| South | 449,000 | 10 |

East leads — two Enterprise reps (Carol Lee + Frank Wu) are assigned there.

### `rep_leaderboard.parquet`

| name | region | team | total_revenue | num_deals |
|---|---|---|---|---|
| Frank Wu | East | Enterprise | 534,500 | 10 |
| David Kim | West | SMB | 510,000 | 10 |
| Carol Lee | East | Enterprise | 458,500 | 10 |
| Bob Smith | South | SMB | 449,000 | 10 |
| Alice Chen | North | Enterprise | 411,000 | 10 |

### `quarterly_trend.parquet`

| quarter | quarterly_revenue | num_deals |
|---|---|---|
| 2024-Q1 | 1,007,000 | 24 |
| 2024-Q2 | 1,171,000 | 26 |
| 2024-Q3 | 518,000 | 10 |

Q2 is the peak quarter. Q3 data is partial (only July–August in the seed).

---

## Things to Try

- Add a `filter` in `bronze.yaml` to ingest only `closed_won` deals
- Extend `build_rep_performance()` to compute a rolling 3-month revenue column
- Ask cerebrum: *"Which rep has the highest attainment_pct in Q2?"*
- Add a new gold aggregation: revenue by `product` and `team`
- Switch `explore: walker` to `explore: profile` on `rep_performance` to compare report types
