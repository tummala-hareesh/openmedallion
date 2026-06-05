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

---

## init

Scaffold a new project folder with YAML config templates and Python UDF stubs.

```bash
medallion init <project>
```

**Arguments:**

| Argument | Description |
| --- | --- |
| `project` | Project name. Creates `<project>/` in the current directory. |

**What it creates:**

```text
<project>/
├── main.yaml              # pipeline name, paths, bi_export
├── backend/
│   ├── bronze.yaml        # source connection + incremental config
│   ├── silver.yaml        # rename + cast + UDF template
│   ├── gold.yaml          # group_by aggregation template
│   └── udf/
│       ├── silver/
│       │   ├── base.py    # Silver base-table UDF stub
│       │   └── derived.py # Silver derived-table UDF stub
│       └── gold/
│           └── transforms.py  # Gold pre-agg UDF stub
├── frontend/              # dashboard files (Tableau, Power BI, etc.)
├── data/                  # gitignored pipeline outputs
├── summary/               # analysis write-ups
├── kestra_flow.yml        # Kestra orchestration flow
└── README.md              # pre-filled project documentation template
```

If the project folder already exists, the command aborts with an error and prints the path to delete to reinitialise.

**Example:**
```bash
medallion init sales_project
# 🏗️  Scaffolding project 'sales_project' ...
# ✅  Project 'sales_project' initialised.
```

---

## run

Execute the pipeline for a named project.

```bash
medallion run <project> [--layer LAYER] [--projects PATH]
```

**Arguments:**

| Argument | Description |
| --- | --- |
| `project` | Project name (must match a folder under `--projects`). |

**Options:**

| Flag | Default | Description |
| --- | --- | --- |
| `--layer` | `gold` | Which layer to run up to and including. One of: `bronze`, `silver`, `gold`, `export`, `explore`. |
| `--projects` | `.` | Projects root directory. Override when running from a different working directory. |

**Layer behaviour:**

| `--layer` | Nodes executed | Use when |
| --- | --- | --- |
| `bronze` | config → bronze | Testing ingestion only; inspecting raw data |
| `silver` | config → silver (bronze skipped) | Re-running transforms without re-ingesting |
| `gold` | config → gold (bronze + silver skipped) | Re-running aggregations only |
| `export` | Full pipeline | Production run including BI export |
| `explore` | Reads existing Parquet, generates reports | Generate data-quality / exploration HTML reports |

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

# Projects in a non-default directory
medallion run sales_project --projects /var/pipelines/projects
```

---

## Global Behaviour

- The CLI always reconfigures `sys.stdout` to UTF-8 so emoji output works on Windows.
- All commands exit with a non-zero code on error (config validation failure, missing UDF file, etc.).
- Running `medallion --help` or `medallion <command> --help` prints usage.
