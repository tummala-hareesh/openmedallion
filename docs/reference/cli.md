# CLI Reference

The `medallion` command is the entry point for all pipeline operations. It is installed as a console script when you `pip install openmedallion`.

```
medallion <command> [options]
```

---

## Commands

| Command | Purpose |
| --- | --- |
| [`init`](#init) | Scaffold a new project directory |
| [`run`](#run) | Execute the pipeline for a project |
| [`dag`](#dag) | Print the Hamilton DAG as a text tree |
| [`visualize`](#visualize) | Export the Hamilton DAG as a PNG image |
| [`status`](#status) | Start the live status dashboard server |

---

## init

Scaffold a new project folder with YAML config templates and Python UDF stubs.

```bash
medallion init <project>
```

**Arguments:**

| Argument | Description |
| --- | --- |
| `project` | Project name. Creates `projects/<project>/` in the current directory. |

**What it creates:**

```text
projects/<project>/
├── main.yaml              # pipeline name, paths, bi_export
├── bronze.yaml            # Oracle SQL source template
├── silver.yaml            # rename + cast + UDF template
├── gold.yaml              # group_by aggregation template
└── udf/
    ├── silver/
    │   ├── base.py        # Silver base-table UDF stub
    │   └── derived.py     # Silver derived-table UDF stub
    └── gold/
        └── transforms.py  # Gold pre-agg UDF stub
```

If the project folder already exists, the command aborts with an error and prints the path to delete to reinitialise.

**Example:**
```bash
medallion init sales_project
# 🏗️  Scaffolding project 'sales_project' ...
# 🏗️   [init] created projects/sales_project/main.yaml
# ...
# ✅  Project 'sales_project' initialised.
```

---

## run

Execute the pipeline for a named project.

```bash
medallion run <project> [--layer LAYER] [--projects PATH] [--track]
```

**Arguments:**

| Argument | Description |
| --- | --- |
| `project` | Project name (must match a folder under `--projects`). |

**Options:**

| Flag | Default | Description |
| --- | --- | --- |
| `--layer` | `gold` | Which layer to run up to and including. One of: `bronze`, `silver`, `gold`, `export`. |
| `--projects` | `projects` | Projects root directory. Override when running from a different working directory. |
| `--track` | off | Start the live status dashboard at `http://localhost:8765` and attach the Hamilton tracker. |

**Layer behaviour:**

| `--layer` | Nodes executed | Use when |
| --- | --- | --- |
| `bronze` | config → bronze | Testing ingestion only; inspecting raw data |
| `silver` | config → silver (bronze skipped) | Re-running transforms without re-ingesting |
| `gold` | config → gold (bronze + silver skipped) | Re-running aggregations only |
| `export` | Full pipeline | Production run including BI export |

!!! note "Layer skipping uses overrides"
    When `--layer silver` or `--layer gold` is specified, existing bronze/silver Parquet files are discovered and injected as Hamilton `overrides`. The upstream nodes are not re-executed. See [Architecture — Skipping layers](../concepts/architecture.md#skipping-layers-with-overrides).

**Examples:**
```bash
# Full pipeline (bronze → silver → gold)
medallion run sales_project

# Ingest only
medallion run sales_project --layer bronze

# Re-run transforms after editing a UDF
medallion run sales_project --layer silver

# Production run with BI export and live tracker
medallion run sales_project --layer export --track

# Projects in a non-default directory
medallion run sales_project --projects /var/pipelines/projects
```

---

## dag

Print the full Hamilton pipeline DAG as an ASCII text tree. Does not require a project to be configured — it reflects the static structure of `pipeline/nodes.py`.

```bash
medallion dag
```

No arguments or options.

**Example output:**
```
config
└── bronze
    └── silver
        └── gold
            └── bi_export
```

---

## visualize

Export the Hamilton DAG as a PNG image using graphviz.

```bash
medallion visualize <project> [--layer LAYER] [--output PATH] [--open]
```

!!! warning "Requires graphviz"
    Install with `pip install "openmedallion[viz]"` and ensure the `graphviz` system package is also installed (`apt install graphviz` / `brew install graphviz`).

**Arguments:**

| Argument | Description |
| --- | --- |
| `project` | Project name (required when `--layer` is set). |

**Options:**

| Flag | Default | Description |
| --- | --- | --- |
| `--layer` | none | Export the execution DAG for a specific layer. Omit to export the full static DAG. |
| `--output` | `dag.png` | Output file path. Defaults to `dag_<layer>.png` when `--layer` is set. |
| `--open` | off | Open the exported image in the default system viewer. |

**Examples:**
```bash
# Full static DAG
medallion visualize sales_project

# Execution DAG for the gold layer
medallion visualize sales_project --layer gold

# Save to a specific path and open immediately
medallion visualize sales_project --output reports/dag.png --open
```

---

## status

Start the live pipeline status dashboard as a standalone server.

```bash
medallion status [--port PORT]
```

**Options:**

| Flag | Default | Description |
| --- | --- | --- |
| `--port` | `8765` | TCP port to listen on. |

The dashboard serves a FastAPI + Server-Sent Events (SSE) web page that shows node execution states and timings in real time. Open `http://localhost:8765` in a browser.

!!! tip "Starting the tracker from run"
    `medallion status` starts the server standalone (for debugging or demo use). In production, use `medallion run <project> --track` instead — that starts the server in the background *and* attaches the Hamilton tracker to the running pipeline so the dashboard updates live.

**Example:**
```bash
medallion status
# 🖥️   Starting status dashboard on port 8765 ...

medallion status --port 9000
```

---

## Global Behaviour

- The CLI always reconfigures `sys.stdout` to UTF-8 so emoji output works on Windows.
- All commands exit with a non-zero code on error (config validation failure, missing UDF file, etc.).
- Running `medallion --help` or `medallion <command> --help` prints usage.
