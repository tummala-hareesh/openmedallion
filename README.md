# OpenMedallion

**Declarative medallion pipelines in pure open-source Python — local first, cloud portable, fast by default.**

[![PyPI version](https://img.shields.io/pypi/v/openmedallion?color=gold&label=PyPI)](https://pypi.org/project/openmedallion/)
[![Python](https://img.shields.io/badge/python-3.11%2B-blue)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![CI](https://github.com/tummalaahri/openmedallion/actions/workflows/ci.yml/badge.svg)](https://github.com/tummalaahri/openmedallion/actions)

OpenMedallion is an opinionated open-source library for building **Bronze → Silver → Gold** data warehouse and lakehouse pipelines using **dlt**, **Polars**, and **Hamilton** — without depending on expensive enterprise platforms or proprietary tooling.

---

## Why OpenMedallion?

Modern open-source data tools are individually excellent — but combining them into a production-ready medallion architecture is still fragmented.

You already have great tools for ingestion, transformation, loading, orchestration, and validation. But you still have to stitch everything together yourself — writing glue code, defining project structure, creating naming conventions, managing layer boundaries, and maintaining all of it over time.

**OpenMedallion exists to reduce that friction.**

| Without OpenMedallion | With OpenMedallion |
| --- | --- |
| Glue code per project | Convention-driven project layout |
| Ad-hoc layer boundaries | Enforced Bronze / Silver / Gold contracts |
| Inline transforms | Composable Python UDFs |
| Manual orchestration | Hamilton DAG — wired automatically |
| Cloud-only dev loop | Local Parquet first, S3 with one config change |

---

## Quickstart

```bash
pip install openmedallion

medallion init my_project       # scaffold: YAML configs + UDF stubs + kestra_flow.yml
medallion run my_project        # Bronze → Silver → Gold in one command
medallion run my_project --layer silver   # re-run a single layer
```

---

## Key Features

- **Declarative YAML config** — define pipeline layers without writing boilerplate
- **Incremental loads** — append and merge modes via dlt cursor columns and primary keys
- **Rich silver transforms** — 12 built-in declarative transform types (`rename`, `cast`, `drop`, `fillna`, `clip`, `normalize`, `deduplicate`, `filter_rows`, `map_values`, `allowed_values`, `coerce_bool`, `udf`) — no Python needed for common operations
- **Expressive gold aggregations** — 11 aggregation functions (`count`, `sum`, `mean`, `min`, `max`, `median`, `std`, `var`, `first`, `last`, `count_distinct`) plus `having`, `sort`, and `limit` post-aggregation controls
- **Composable UDFs** — drop Python functions into `udf/silver/` or `udf/gold/`; no new framework to learn
- **Local first** — run the full pipeline against Parquet files with zero cloud credentials
- **Cloud portable** — swap `filesystem` for S3 in one line; logic stays unchanged
- **Source agnostic** — any dlt source: SQL databases, REST APIs, filesystems, and more
- **Fast by default** — Polars for all transforms; no pandas bottlenecks
- **Natural-language queries** — ask questions about your data in plain English; works with Ollama (local), OpenRouter, OpenAI, or any OpenAI-compatible endpoint

---

## How It Works

OpenMedallion wires three best-in-class open-source tools under a unified declarative config:

```text
YAML config
    │
    ▼
Hamilton DAG           ← orchestrates which layer runs and in what order
    │
    ├── Bronze  (dlt)     ← ingests raw data from any source into Parquet
    ├── Silver  (Polars)  ← typed UDF transforms: rename, cast, filter, enrich
    └── Gold    (Polars)  ← YAML-declared group-by aggregations + window metrics
```

| Layer | Tool | Role |
| --- | --- | --- |
| 🟤 Bronze | dlt | Schema-inferred raw load from any source |
| ⚪ Silver | Polars | Typed, composable Python UDFs |
| 🟡 Gold | Polars | YAML-declared group-by metrics |
| 📤 Export | Polars | Parquet + CSV for BI tools |
| 🔍 Explore | ydata-profiling / pygwalker | HTML data-quality and exploration reports |
| 🔗 Orchestration | Hamilton | DAG wiring and execution order |
| 🧠 Cerebrum | DuckDB + LLM | Natural-language SQL queries over your pipeline data |
| 📡 Neuron | FastAPI | HTTP server exposing Cerebrum over REST |
| 🖥️ Cortex | Dash | Chat UI with table + dashboard tabs and CSV/Excel download |

---

## Installation

```bash
pip install openmedallion
```

Optional extras:

```bash
pip install "openmedallion[s3]"        # S3 support via s3fs + boto3
pip install "openmedallion[oracle]"    # Oracle DB support via oracledb
pip install "openmedallion[profile]"   # Data profiling reports via ydata-profiling
pip install "openmedallion[explore]"   # Interactive exploration reports via pygwalker
pip install "openmedallion[cerebrum]"  # Natural-language queries: DuckDB engine + neuron FastAPI server
pip install "openmedallion[cortex]"    # Cortex Dash chat UI (requires [cerebrum])
```

> Requires Python 3.11+

---

## Project Structure

`medallion init my_project` generates a complete, ready-to-run project:

```text
my_project/
├── main.yaml                    # pipeline name + layer includes + paths
├── backend/
│   ├── bronze.yaml              # source connection + incremental config
│   ├── silver.yaml              # table transforms (rename, cast, filter, UDFs)
│   ├── gold.yaml                # aggregations (group_by + metrics + window fns)
│   └── udf/
│       ├── silver/              # Python UDFs called from silver.yaml
│       └── gold/                # Python UDFs called from gold.yaml
├── frontend/                    # dashboard files (Tableau, Power BI, etc.)
├── data/                        # gitignored pipeline outputs
├── summary/                     # analysis write-ups
├── kestra_flow.yml              # Kestra orchestration flow — mount via docker-compose.yml
└── README.md                    # pre-filled project documentation template
```

---

## Configuration

**`main.yaml`** — declare your layers and data paths:

```yaml
pipeline:
  name: customer_warehouse

includes:
  bronze: bronze.yaml
  silver: silver.yaml
  gold:   gold.yaml

paths:
  bronze: "./data/bronze"
  silver: "./data/silver"
  gold:   "./data/gold"
  export: "./data/export"
```

**`silver.yaml`** — declarative transforms with optional UDFs:

```yaml
bronze_to_silver:
  tables:
    - source_file: ORDERS.parquet
      output_file: orders.parquet
      transforms:
        - type: rename
          columns:
            ORDER_ID:    order_id
            CUSTOMER_ID: customer_id
        - type: cast
          columns:
            order_id: Int64
            amount:   Float64
        - type: clip                          # clamp numeric or date columns
          columns:
            amount: {min: 0}
            order_date: {min: "2020-01-01", max: "2030-12-31"}
        - type: allowed_values               # null out unexpected categories
          columns:
            status: [pending, shipped, delivered, cancelled]
        - type: normalize                    # strip whitespace + lowercase
          columns:
            region: strip_lower
        - type: fillna                       # fill remaining nulls
          columns:
            region: "unknown"
        - type: coerce_bool                  # "yes"/"1"/"true" → true, etc.
          columns: [is_priority]
        - type: deduplicate                  # remove exact duplicates
          subset: [order_id]
        - type: udf
          file: udf/silver/enrich.py
          function: flag_large_orders
          args:
            threshold: 500.0
```

**Built-in silver transform types:** `rename` · `cast` · `drop` · `fillna` · `clip` (numeric + date/datetime) · `normalize` (upper/lower/strip/strip_lower) · `deduplicate` · `filter_rows` (SQL expression) · `map_values` · `allowed_values` · `coerce_bool` · `udf`

**`gold.yaml`** — YAML-declared aggregations with post-aggregation controls:

```yaml
silver_to_gold:
  projects:
    - name: customer_warehouse
      aggregations:
        - source_file: orders.parquet
          group_by: [customer_id]
          metrics:
            - {column: order_id, agg: count,          alias: total_orders}
            - {column: amount,   agg: sum,             alias: total_spent}
            - {column: amount,   agg: median,          alias: median_order}
            - {column: amount,   agg: std,             alias: spend_std}
            - {column: order_id, agg: count_distinct,  alias: unique_products}
          having: "total_orders > 1"         # filter after aggregation
          sort:
            columns: [total_spent]
            descending: true
          limit: 100                         # top N rows
          output_file: customer_summary.parquet
```

**Built-in gold aggregations:** `count` · `sum` · `mean` · `min` · `max` · `median` · `std` · `var` · `first` · `last` · `count_distinct`

---

## Python UDFs

Business logic stays in plain Python — no custom DSL, no magic.

```python
# udf/silver/enrich.py
import polars as pl

def flag_large_orders(df: pl.DataFrame, threshold: float = 500.0) -> pl.DataFrame:
    return df.with_columns(
        (pl.col("amount") >= threshold).alias("is_large_order")
    )
```

Drop the file next to your config, reference it in `silver.yaml`, done.

---

## Incremental Loads

OpenMedallion supports dlt's native incremental strategies out of the box:

```yaml
# bronze.yaml
source:
  type: sql_database
  dialect: sqlite
  connection_string: "sqlite:///data/mydb.db"
  tables:
    - name: orders
      incremental:
        mode: append          # cursor-based — only new rows
        cursor_column: created_at
        initial_value: "2024-01-01"
    - name: customers
      incremental:
        mode: merge           # upsert — handles updates + deletes
        primary_key: customer_id
```

dlt tracks cursor state automatically. Re-running bronze only pulls the delta.

---

## Natural Language Queries

Ask questions about your pipeline data in plain English — no SQL required.

```bash
pip install "openmedallion[cerebrum]"

medallion query my_project "What are the top 5 customers by revenue?"
medallion query my_project "Show monthly trends" --model mistral
```

This runs the full **cerebrum** pipeline locally: builds a schema context from your silver/gold Parquet files, generates SQL with an LLM, validates it against DuckDB, executes it, and prints the results alongside a canonical reproducible prompt.

### Provider options

By default, cerebrum uses a local **Ollama** server (`ollama serve`). To use a cloud provider, set the provider and API key:

```bash
# OpenRouter (access to GPT-4o, Claude 3.5, Llama 3, Mistral, …)
MEDALLION_LLM_PROVIDER=openrouter \
MEDALLION_LLM_API_KEY=sk-or-... \
MEDALLION_LLM_MODEL=openai/gpt-4o \
medallion query my_project "Revenue by region"

# OpenAI
MEDALLION_LLM_PROVIDER=openai \
MEDALLION_LLM_API_KEY=sk-... \
MEDALLION_LLM_MODEL=gpt-4o-mini \
medallion query my_project "Revenue by region"

# Any OpenAI-compatible endpoint (LM Studio, Groq, vLLM, …)
MEDALLION_LLM_PROVIDER=lmstudio \
MEDALLION_LLM_BASE_URL=http://localhost:1234/v1 \
MEDALLION_LLM_MODEL=local-model \
medallion query my_project "Revenue by region"
```

Or configure once in `settings.yaml`:

```yaml
llm:
  provider: openrouter
  model:    openai/gpt-4o
  api_key:  sk-or-...
```

### HTTP server + chat UI

```bash
medallion ask    my_project              # start neuron FastAPI server on :8000
medallion cortex my_project             # start Dash chat UI on :8050
```

`neuron` exposes a `/query` endpoint (Swagger UI at `/docs`). `cortex` connects to it and provides a three-tab UI: chat, table, and dashboard with CSV/Excel download.

### Improving accuracy — metadata, relationships, and examples (RAG)

For larger schemas, curate what the LLM sees instead of dumping every table:

```bash
medallion metadata generate      my_project   # LLM-draft table/column descriptions
medallion metadata approve       my_project   # human review, one table at a time

medallion relationships generate my_project   # detect joins — no LLM call
medallion relationships approve  my_project

medallion examples generate      my_project --count 15   # LLM-draft Q→SQL pairs
medallion examples approve       my_project
```

Once `metadata.yaml` has `status: approved` tables and `examples/synthetic.jsonl` has
`verified: true` examples, `medallion query`/`ask` pick them up automatically — no
extra flag needed. Two things happen behind the scenes:

- **Dynamic few-shot retrieval** — the most relevant verified examples are ranked in as
  prompt context, instead of a fixed static list.
- **Confidence-gated schema pruning** — only the top-k most relevant *approved* tables
  are shown to the LLM. If similarity to the best match falls below a confidence
  threshold, it falls back to a raw-schema search over *every* silver table (any
  status, no `metadata.yaml` required) rather than trusting a weak match.

See [`docs/guides/rag-accuracy.md`](docs/guides/rag-accuracy.md) for the full design, and
[`examples/sales_intelligence_demo/`](examples/sales_intelligence_demo/) for a runnable,
offline (`show_rag_workflow.py`) walkthrough.

---

## Scheduling with Kestra

`medallion init` generates a `kestra_flow.yml` inside every new project — a ready-to-use [Kestra](https://kestra.io) flow that orchestrates bronze → silver → gold with per-task observability and retry support.

### 1. Start a local Kestra server

```bash
# from the repo root — requires Docker
make kestra-up
# UI available at http://localhost:8080
```

### 2. Register a project flow

Add one volume mount to the `kestra` service in `docker-compose.yml`:

```yaml
- ./my_project/kestra_flow.yml:/app/flows/my_project.yml
```

Kestra picks up the file automatically on the next `make kestra-up` — no copying needed.

### 3. Trigger a run

From the UI at `http://localhost:8080`, or via the API:

```bash
curl -X POST \
  http://localhost:8080/api/v1/executions/openmedallion.projects/my_project
```

### 4. Enable scheduled refresh

Uncomment the `triggers:` block in `kestra_flow.yml`:

```yaml
triggers:
  - id: daily_refresh
    type: io.kestra.plugin.core.trigger.Schedule
    cron: "0 6 * * *"   # every day at 06:00 UTC
```

Restart with `make kestra-up` and Kestra picks up the change immediately.

### Kestra vs GitHub Actions

| | Kestra | GitHub Actions |
| --- | --- | --- |
| Best for | Recurring pipeline runs, local/on-prem data | CI tests + PyPI publish on tag push |
| Scheduling | Cron + backfill | Cron only, no backfill |
| Observability | Per-task logs, run history, retry from failed task | Flat job log |
| Infrastructure | Self-hosted Docker | GitHub-managed runners |

**Recommended split:** GitHub Actions for CI + publish; Kestra for pipeline scheduling.

---

## Examples

Five self-contained examples — no cloud credentials required. See [`examples/README.md`](examples/README.md) for a side-by-side comparison.

| Example | Tables | What it demonstrates |
| --- | --- | --- |
| [`local_parquet_demo/`](examples/local_parquet_demo/) | 1 | Zero-credential quickstart: full Bronze → Silver → Gold with local Parquet files |
| [`incremental_sql_demo/`](examples/incremental_sql_demo/) | 2 | Incremental append + merge from SQLite; delta load simulation |
| [`ecommerce_analytics_demo/`](examples/ecommerce_analytics_demo/) | 3 | Multi-table joins, margin analysis, and monthly trends |
| [`oracle_hr_demo/`](examples/oracle_hr_demo/) | 3 | SQL `filter:`/`select:` pushdown, `credentials_file:`, real Oracle/Postgres swap |
| [`sales_intelligence_demo/`](examples/sales_intelligence_demo/) | 3 | cerebrum/neuron/cortex + the full RAG accuracy add-ons (metadata, relationships, examples, confidence-gated fallback) — most complete example |

---

## When to Use OpenMedallion

A great fit if you:

- Want a **standard medallion project layout** without inventing one from scratch
- Prefer **YAML-first config** with Python escape hatches for complex logic
- Need **local-first development** that can scale to S3 with minimal changes
- Want **full ownership** of your code and infrastructure
- Are building on a **tight budget** without enterprise platform procurement

**Not a fit if you need:**

- A full enterprise data platform (Databricks, Snowflake, BigQuery)
- A no-code or drag-and-drop ETL tool
- A universal framework for every possible pipeline architecture

---

## Tradeoffs

| You get | You accept |
| --- | --- |
| Lower cost — fully open-source | More engineering responsibility than a managed platform |
| Full control over code and infrastructure | Initial setup and config learning curve |
| No vendor lock-in | You own the infrastructure decisions |
| Transparent, inspectable pipeline | Not a drag-and-drop tool |

---

## Roadmap

| Item | Status |
| --- | --- |
| Bronze / Silver / Gold pipeline | ✅ 2026.4.1 |
| Hamilton DAG orchestration | ✅ 2026.4.1 |
| Local Parquet + S3 storage | ✅ 2026.4.1 |
| Incremental append + merge | ✅ 2026.4.1 |
| CLI scaffolding (`medallion init`) | ✅ 2026.4.1 |
| PyPI publish (OIDC trusted publishing) | ✅ 2026.4.1 |
| `select:` column projection + `credentials_file:` | ✅ 2026.5.4 |
| Inline `explore:` — profiling + interactive reports | ✅ 2026.6.2 |
| Natural-language queries (cerebrum + neuron + cortex) | ✅ 2026.6.2 |
| `medallion query` — direct CLI Q&A without a server | ✅ 2026.6.2 |
| Multi-provider LLM (Ollama, OpenRouter, OpenAI, custom) | ✅ 2026.6.3 |
| Declarative silver transforms (fillna, clip, normalize, deduplicate, filter_rows, map_values, allowed_values, coerce_bool) | ✅ 2026.6.9 |
| Declarative gold utilities (having, sort, limit) + extended aggregations (median, std, var, first, last, count_distinct) | ✅ 2026.6.9 |
| Schema contract enforcement (Pydantic config schemas) | ✅ 2026.7.1 |
| Named filter fragments (`filter_defs`) | ✅ 2026.7.1 |
| RAG accuracy — metadata, relationships, examples generate/approve, dynamic few-shot, confidence-gated schema pruning fallback | ✅ 2026.7.3 |
| REST API multi-resource support | 🔜 roadmap |
| Metadata drift detection (`medallion metadata refresh`) | 🔜 roadmap |
| Additional cloud destinations | 🔜 roadmap |

---

## Contributing

Contributions are welcome. Good areas to contribute:

- Bug fixes and edge-case handling
- Documentation improvements and example additions
- Tests and coverage
- New pipeline templates
- New source or destination adapters
- CLI enhancements

If you are interested in open-source data architecture, your help is appreciated.

---

## License

[MIT](LICENSE) — free to use, modify, and distribute.

---

> If OpenMedallion looks useful, consider starring the repo — it helps others find it.
