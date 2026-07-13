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
| [`query`](#query) | Ask a natural-language question directly in the terminal |
| [`ask`](#ask) | Start the neuron FastAPI server (`/query`, `/feedback`, `/health`, `/docs`) |
| [`cortex`](#cortex) | Start the cortex Dash chat UI |
| [`metadata`](#metadata) | Generate / approve `metadata.yaml` (RAG accuracy) |
| [`relationships`](#relationships) | Generate / approve `relationships.yaml` (RAG accuracy) |
| [`examples`](#examples) | Generate / approve / harvest / review synthetic Q→SQL examples (RAG accuracy) |

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

## query

Ask a natural-language question about a project's silver/gold data, directly in the
terminal — runs the full `cerebrum` pipeline locally, no server needed. Requires
`pip install "openmedallion[cerebrum]"`.

```bash
medallion query <project> "<question>" [--projects PATH] [--model MODEL] [--provider PROVIDER] [--detect-ambiguity] [--decompose]
```

| Flag | Default | Description |
| --- | --- | --- |
| `--projects` | `.` | Projects root directory |
| `--model` | from settings / `llama3.2` | Model identifier |
| `--provider` | from settings / `ollama` | LLM backend: `ollama` \| `openrouter` \| `openai` \| custom |
| `--detect-ambiguity` | off | One extra LLM call checks whether the question is ambiguous before generating SQL; raises with a clarification message instead of guessing |
| `--decompose` | off | One extra LLM call checks whether the question contains independent sub-questions; each runs through the pipeline separately if so |

If a project has `status: approved` tables in `metadata.yaml` and/or
`verified: true` examples in `examples/synthetic.jsonl`, they're picked up
automatically — see [RAG Accuracy](../guides/rag-accuracy.md) for the full design
(dynamic few-shot retrieval, confidence-gated schema pruning).

```bash
medallion query sales_project "What are the top 5 products by revenue?"
medallion query sales_project "Show monthly trends" --model mistral
medallion query sales_project "Headcount by dept and revenue by region" --decompose
```

---

## ask

Start the `neuron` FastAPI server, exposing `cerebrum` over HTTP for `cortex` (or any
HTTP client) to call. Requires `pip install "openmedallion[cerebrum]"`.

```bash
medallion ask <project> [--projects PATH] [--port PORT] [--model MODEL] [--provider PROVIDER]
```

| Endpoint | Purpose |
| --- | --- |
| `POST /query` | Ask a question — same pipeline as `medallion query` |
| `POST /feedback` | Record cortex 👍/👎 into `examples/harvested.jsonl` / `examples/failures.jsonl` |
| `GET /health` | Liveness check |
| `GET /docs` | Swagger UI |

---

## cortex

Start the `cortex` Dash chat UI, which talks to a running `neuron` server over HTTP.

```bash
medallion cortex <project> [--projects PATH] [--port PORT] [--neuron-url URL] [--debug]
```

Three tabs: chat (with 👍/👎 feedback on every reply), table, and dashboard, with
CSV/Excel download.

---

## metadata

Manage a project's `metadata.yaml` — curated table/column descriptions used for
schema pruning (see [RAG Accuracy](../guides/rag-accuracy.md)).

```bash
medallion metadata generate <project> [--projects PATH] [--model MODEL] [--provider PROVIDER]
medallion metadata approve  <project> [--projects PATH]
```

`generate` requires `pip install "openmedallion[cerebrum]"`; `approve` does not (it's a
plain interactive `[a]pprove / [s]kip / [q]uit` review loop, no LLM call). Tables
already `status: approved` are always skipped and preserved untouched by `generate`.

---

## relationships

Manage a project's `relationships.yaml` — explicit join paths between silver/gold
tables, detected via deterministic pattern matching (no LLM call, either command).

```bash
medallion relationships generate <project> [--projects PATH]
medallion relationships approve  <project> [--projects PATH]
```

---

## examples

Manage a project's synthetic `(question, sql)` examples used for dynamic few-shot
retrieval (see [RAG Accuracy](../guides/rag-accuracy.md)).

```bash
medallion examples generate <project> [--projects PATH] [--count N] [--model MODEL] [--provider PROVIDER]
medallion examples approve  <project> [--projects PATH]
medallion examples harvest  <project> [--projects PATH]
medallion examples review   <project> [--projects PATH]
```

| Subcommand | Purpose |
| --- | --- |
| `generate` | LLM drafts `(question, sql)` pairs from approved metadata + relationships; each is validated (allowlist + DuckDB `EXPLAIN`) before being written. Requires `[cerebrum]`. |
| `approve` | Interactive review of unverified examples — no LLM call. |
| `harvest` | Promotes `examples/harvested.jsonl` (cortex 👍) candidates into `synthetic.jsonl` as unverified — still needs `approve`. |
| `review` | Lists `examples/failures.jsonl` (cortex 👎) entries — a plain listing, no LLM call. |

---

## Global Behaviour

- The CLI always reconfigures `sys.stdout` to UTF-8 so emoji output works on Windows.
- All commands exit with a non-zero code on error (config validation failure, missing UDF file, etc.).
- Running `medallion --help` or `medallion <command> --help` prints usage.
