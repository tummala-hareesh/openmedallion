# 🛒 ecommerce_analytics_demo

**Multi-table enrichment, margin analysis, and temporal trends — no credentials required.**

This example shows the most complete OpenMedallion workflow:

- **3 bronze tables** — orders, products, customers
- **Silver phase 1** — rename + cast each table individually
- **Silver phase 2** — a derived UDF joins all three into a single enriched table
- **Gold** — 3 aggregations, two of which use a `pre_agg_udf` to derive columns before grouping

---

## 📐 Data Model

```mermaid
erDiagram
    ORDERS {
        int    order_id    PK
        int    product_id  FK
        int    customer_id FK
        int    qty
        string order_date
        string status
    }
    PRODUCTS {
        int    product_id  PK
        string product_name
        string category
        float  unit_price
        float  unit_cost
    }
    CUSTOMERS {
        int    customer_id PK
        string name
        string region
        string tier
    }
    ORDERS }o--|| PRODUCTS  : "product_id"
    ORDERS }o--|| CUSTOMERS : "customer_id"
```

---

## 🔄 Pipeline Flow

```mermaid
flowchart LR
    subgraph src["📂 Source CSVs"]
        O["orders.csv\n20 rows"]
        P["products.csv\n8 rows"]
        C["customers.csv\n5 rows"]
    end

    subgraph bronze["🟤 Bronze"]
        BO["ORDERS.parquet"]
        BP["PRODUCTS.parquet"]
        BC["CUSTOMERS.parquet"]
    end

    subgraph silver["⚪ Silver  (phase 1 → phase 2)"]
        SO["orders.parquet\nrenamed · cast"]
        SP["products.parquet\nrenamed · cast"]
        SC["customers.parquet\nrenamed · cast"]
        EN["order_lines_enriched.parquet\njoined · line_revenue · margin_amount"]
    end

    subgraph gold["🟡 Gold"]
        G1["revenue_by_category.parquet\ntotal_revenue · total_margin · num_orders"]
        G2["top_customers.parquet\ntotal_spent · num_orders"]
        G3["monthly_summary.parquet\nmonthly_revenue · num_orders"]
    end

    O -->|seed.py| BO
    P -->|seed.py| BP
    C -->|seed.py| BC

    BO --> SO
    BP --> SP
    BC --> SC

    SO & SP & SC -->|udf: build_order_lines_enriched| EN

    EN -->|pre_agg_udf: add_metrics| G1
    EN --> G2
    EN -->|pre_agg_udf: add_metrics| G3
```

---

## 📦 Source Data

**products.csv** — 8 products across 3 categories:

| product_id | product_name | category | unit_price | unit_cost |
| --- | --- | --- | --- | --- |
| 1 | Laptop Pro 15 | Electronics | 1200.00 | 900.00 |
| 2 | Wireless Headphones | Electronics | 150.00 | 80.00 |
| 3 | Webcam HD | Electronics | 80.00 | 40.00 |
| 4 | Winter Jacket | Clothing | 120.00 | 60.00 |
| 5 | Cotton T-Shirt | Clothing | 30.00 | 12.00 |
| 6 | Running Sneakers | Clothing | 90.00 | 45.00 |
| 7 | Python Mastery | Books | 45.00 | 20.00 |
| 8 | Data Science Handbook | Books | 55.00 | 25.00 |

**customers.csv** — 5 customers across regions and tiers:

| customer_id | name | region | tier |
| --- | --- | --- | --- |
| 1 | Alice Chen | US-West | gold |
| 2 | Bob Smith | US-East | silver |
| 3 | Carol Lee | Europe | gold |
| 4 | David Kim | US-West | bronze |
| 5 | Eve Brown | Asia | silver |

**orders.csv** — 20 orders across Jan–Apr 2024.

---

## ⚙️ How the UDFs Work

### Silver derived UDF — `build_order_lines_enriched`

After the 3 base tables are written to silver, a derived UDF reads them back and joins them:

```python
# ecommerce/backend/udf/silver/enrich.py
def build_order_lines_enriched(silver_dir):
    orders    = read_parquet(join(silver_dir, "orders.parquet"))
    products  = read_parquet(join(silver_dir, "products.parquet"))
    customers = read_parquet(join(silver_dir, "customers.parquet"))

    return (
        orders
        .join(products,  on="product_id",  how="left")
        .join(customers, on="customer_id", how="left")
        .with_columns([
            (pl.col("qty") * pl.col("unit_price")).alias("line_revenue"),
            (pl.col("qty") * pl.col("unit_cost")).alias("line_cost"),
        ])
        .with_columns(
            (pl.col("line_revenue") - pl.col("line_cost")).alias("margin_amount")
        )
    )
```

Sample rows from `order_lines_enriched.parquet`:

| order_id | name | product_name | category | qty | unit_price | line_revenue | margin_amount |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | Alice Chen | Laptop Pro 15 | Electronics | 1 | 1200.0 | 1200.0 | 300.0 |
| 2 | Alice Chen | Wireless Headphones | Electronics | 2 | 150.0 | 300.0 | 140.0 |
| 4 | Bob Smith | Python Mastery | Books | 3 | 45.0 | 135.0 | 75.0 |
| 6 | David Kim | Cotton T-Shirt | Clothing | 5 | 30.0 | 150.0 | 90.0 |

### Gold pre-aggregation UDF — `add_metrics`

Derives `order_month` (YYYY-MM) from `order_date` so the YAML aggregation can group by month:

```python
# ecommerce/backend/udf/gold/metrics.py
def add_metrics(df, silver_dir):
    return df.with_columns(
        pl.col("order_date").str.slice(0, 7).alias("order_month")
    )
```

---

## 🚀 Run the Demo

```bash
# From this directory (examples/ecommerce_analytics_demo/)

# Step 1 — seed bronze from source CSVs
python seed.py

# Step 2 — silver: rename/cast 3 tables + derive enriched join
medallion run ecommerce --layer silver

# Step 3 — gold: 3 aggregations
medallion run ecommerce --layer gold

# Step 4 — inspect results
python inspect.py
```

---

## 📊 Expected Output

### `revenue_by_category.parquet`

| category | total_revenue | total_margin | num_orders | margin_pct |
| --- | --- | --- | --- | --- |
| Electronics | 4440.0 | 1300.0 | 8 | 29.3% |
| Clothing | 1080.0 | 576.0 | 8 | 53.3% |
| Books | 345.0 | 190.0 | 4 | 55.1% |

Electronics leads on revenue. Books and Clothing lead on margin percentage.

### `top_customers.parquet`

| name | region | tier | total_spent | num_orders |
| --- | --- | --- | --- | --- |
| Alice Chen | US-West | gold | 2970.0 | 5 |
| Carol Lee | Europe | gold | 1790.0 | 5 |
| Eve Brown | Asia | silver | 430.0 | 3 |
| Bob Smith | US-East | silver | 380.0 | 4 |
| David Kim | US-West | bronze | 295.0 | 3 |

Gold-tier customers account for ~81% of total revenue.

### `monthly_summary.parquet`

| order_month | monthly_revenue | num_orders |
| --- | --- | --- |
| 2024-01 | 1755.0 | 4 |
| 2024-02 | 1710.0 | 5 |
| 2024-03 | 1905.0 | 6 |
| 2024-04 | 495.0 | 5 |

March is the peak month. April dips — no laptop orders.

---

## 🗂️ Project Layout

```text
ecommerce/
├── main.yaml                        # pipeline name + paths + includes
├── backend/
│   ├── bronze.yaml                  # placeholder (seed.py handles bronze)
│   ├── silver.yaml                  # 3 base tables + 1 derived join
│   ├── gold.yaml                    # 3 aggregations (2 with pre_agg_udf)
│   └── udf/
│       ├── silver/
│       │   └── enrich.py            # build_order_lines_enriched()
│       └── gold/
│           └── metrics.py           # add_metrics() — derives order_month
├── frontend/                        # dashboard files
├── data/                            # gitignored pipeline outputs
├── catalogue/                       # ERD, data dictionary
└── summary/                         # analysis summary
```

---

## 🔍 Things to Try

- Add a `margin_pct` column to `add_metrics()` and aggregate it with `agg: mean` in `backend/gold.yaml`
- Add a `filter` transform in `backend/silver.yaml` to exclude `status = 'cancelled'` orders
- Add a new gold aggregation: revenue by `region` grouped from `top_customers`
- Add a `pre_agg_udf` to `top_customers.parquet` to bucket customers by spend tier
