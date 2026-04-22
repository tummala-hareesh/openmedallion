# 🥇 OpenMedallion

**Declarative medallion pipelines in pure open-source Python — local first, cloud portable, fast by default.**

OpenMedallion is an opinionated open-source library for building **Bronze → Silver → Gold** data warehouse and lakehouse pipelines using **dlt**, **Polars**, and **Hamilton** — without depending on expensive enterprise platforms or proprietary tooling.

---

## 🤔 Why OpenMedallion?

Modern open-source data tools are individually excellent — but combining them into a production-ready medallion architecture is still fragmented.

You already have great tools for ingestion, transformation, loading, orchestration, and validation. But you still have to stitch everything together yourself.

That usually means writing glue code, defining your own project structure, creating naming conventions, managing layer boundaries manually, and maintaining all of it over time.

**OpenMedallion exists to reduce that friction.**

---

## 🎯 What Problem does it solve ?

| Without OpenMedallion | With OpenMedallion |
| --- | --- |
| Glue code per project | Convention-driven project layout |
| Ad-hoc layer boundaries | Enforced Bronze / Silver / Gold contracts |
| Inline transforms | Composable Python UDFs |
| Manual orchestration | Hamilton DAG — wired automatically |
| Cloud-only dev loop | Local Parquet first, S3 with one config change |

---

## ⚡ Key Features

- 🗂️ **Declarative YAML config** — define your pipeline layers without writing boilerplate
- 🔁 **Incremental loads** — append and merge modes via dlt cursor columns and primary keys
- 🧱 **Composable UDFs** — drop Python functions into `udf/silver/` or `udf/gold/` — no framework to learn
- 📊 **Live DAG tracker** — Hamilton-powered web UI to visualise and monitor pipeline execution
- 🪶 **Local first** — run the full pipeline against Parquet files with zero cloud credentials
- ☁️ **Cloud portable** — swap `filesystem` for S3 in one line; logic stays unchanged
- 🔌 **Source agnostic** — any dlt source: SQL databases, REST APIs, filesystems, and more
- 🚀 **Fast by default** — Polars for all transforms; no pandas bottlenecks

---

## 🔬 How It Works?

OpenMedallion wires together three best-in-class open-source tools under a unified declarative config:

```text
YAML config
    │
    ▼
Hamilton DAG          ← orchestrates which layer runs and in what order
    │
    ├── Bronze (dlt)  ← ingests raw data from any source into Parquet
    ├── Silver (Polars) ← typed UDF transforms: rename, cast, filter, enrich
    └── Gold (Polars) ← YAML-declared group-by aggregations + window metrics
```

| Layer | Tool | Role |
| --- | --- | --- |
| 🟤 Bronze | dlt | Schema-inferred raw load from any source |
| ⚪ Silver | Polars | Typed, composable Python UDFs |
| 🟡 Gold | Polars | YAML-declared group-by metrics |
| 📤 Export | Polars | Parquet + CSV for BI tools |
| 🔗 Orchestration | Hamilton | DAG wiring with live web tracker |

---

## 🏗️ Project Structure

When you run `medallion init my_project`, you get:

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
└── README.md                    # pre-filled project documentation template
```

---

## 📦 Installation

```bash
pip install openmedallion
```

Optional extras:

```bash
pip install "openmedallion[s3]"    # S3 support via s3fs + boto3
pip install "openmedallion[viz]"   # DAG visualisation via graphviz
```

> Requires Python 3.11+

---

## 🚀 Quickstart

```bash
# 1. Scaffold a new project
medallion init my_project

# 2. Run the full Bronze → Silver → Gold pipeline
medallion run my_project

# 3. Run only the Silver layer (assumes Bronze already ran)
medallion run my_project --layer silver

# 4. Print the Hamilton DAG structure
medallion dag

# 5. Launch the live pipeline tracker UI
medallion serve
```

---

## 🔧 Configuration Example

**`main.yaml`** — declare your layers and paths:

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
            order_id:    Int64
            amount:      Float64
        - type: udf
          file: udf/silver/enrich.py
          function: flag_large_orders
          args:
            threshold: 500.0
```

**`gold.yaml`** — YAML-declared aggregations:

```yaml
silver_to_gold:
  projects:
    - name: customer_warehouse
      aggregations:
        - source_file: orders.parquet
          group_by: [customer_id]
          metrics:
            - {column: order_id, agg: count, alias: total_orders}
            - {column: amount,   agg: sum,   alias: total_spent}
          output_file: customer_summary.parquet
```

---

## 🧩 Python UDFs

Business logic stays in plain Python. No custom DSL. No magic.

```python
# udf/silver/enrich.py
import polars as pl

def flag_large_orders(df: pl.DataFrame, threshold: float = 500.0) -> pl.DataFrame:
    return df.with_columns(
        (pl.col("amount") >= threshold).alias("is_large_order")
    )
```

Drop this file next to your config, reference it in `silver.yaml`, done.

---

## 🔄 Incremental Loads

OpenMedallion supports dlt's native incremental strategies:

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

dlt tracks state automatically. Re-running bronze only pulls the delta.

---

## ✅ When to Use OpenMedallion

OpenMedallion is a great fit if you:

- ✅ Want a **standard medallion project layout** without inventing one from scratch
- ✅ Prefer **YAML-first config** with Python escape hatches for complex logic
- ✅ Need **local-first development** that can scale to S3 with minimal changes
- ✅ Want **full ownership** of your code and infrastructure
- ✅ Are building on a **tight budget** without enterprise platform procurement

---

## 🚫 What OpenMedallion Is Not

- ❌ A full enterprise data platform (Databricks, Snowflake, BigQuery)
- ❌ A no-code or low-code ETL tool
- ❌ A replacement for dlt, Polars, or Hamilton individually
- ❌ A universal framework for every possible pipeline architecture

It is a **focused convention** that makes medallion pipelines easier to build, extend, and maintain.

---

## ⚖️ Tradeoffs

| You get | You accept |
| --- | --- |
| Lower cost (fully open-source) | More engineering responsibility than a managed platform |
| Full control over code and infra | Initial setup and config learning curve |
| No vendor lock-in | You own the infrastructure decisions |
| Transparent, inspectable pipeline | Not a drag-and-drop tool |

---

## 🗺️ Roadmap

| Item | Status |
| --- | --- |
| Bronze / Silver / Gold pipeline | ✅ 2026.4.1 |
| Hamilton DAG + live tracker | ✅ 2026.4.1 |
| Local Parquet + S3 storage | ✅ 2026.4.1 |
| Incremental append + merge | ✅ 2026.4.1 |
| CLI scaffolding (`medallion init`) | ✅ 2026.4.1 |
| PyPI publish (OIDC trusted publishing) | ✅ 2026.4.1 |
| LazyFrame UDF contract | 🔜 2026.5 |
| Schema contract enforcement | 🔜 2026.6 |
| Lineage + metadata helpers | 🔜 2026.6 |
| Additional cloud destinations | 🔜 2026.6 |

---

## 📂 Examples

Three self-contained examples runnable with no cloud credentials. See [`examples/README.md`](examples/README.md) for a side-by-side comparison.

| Example | Tables | What it shows |
| --- | --- | --- |
| [`local_parquet_demo/`](examples/local_parquet_demo/) | 1 | Zero-credential quickstart: full Bronze → Silver → Gold with Parquet files |
| [`incremental_sql_demo/`](examples/incremental_sql_demo/) | 2 | Incremental append + merge with a SQLite source; delta load simulation |
| [`ecommerce_analytics_demo/`](examples/ecommerce_analytics_demo/) | 3 | Multi-table joins, margin analysis, and monthly trends — most complete example |

---

## 🤝 Contributing

Contributions are welcome. Good contributions include:

- 🐛 Bug fixes
- 📖 Documentation improvements
- 🧪 Tests and coverage
- 💡 New pipeline templates or examples
- 🔌 New source or destination adapters
- ⚙️ CLI enhancements

If you are interested in open-source data architecture, your help is appreciated.

---

## 📄 License

[MIT](LICENSE) — free to use, modify, and distribute.

---

## ⭐ Star the Project

If OpenMedallion looks useful to you, consider starring the repo — it helps others find it.
