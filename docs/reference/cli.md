# CLI Reference

The `medallion` command is the entry point for all pipeline operations. It is installed as a console script when you `pip install openmedallion`.

!!! tip "Quick reference"
    For every command and flag on one page without the prose, see the [Command Cheatsheet](command-cheatsheet.md).

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
| [`ask`](#ask) | Start the neuron FastAPI server (`/query`, `/feedback`, `/history`, `/session/end`, `/health`, `/docs`) |
| [`cortex`](#cortex) | Start the cortex Dash chat UI (chat, table, dashboard, history tabs) |
| [`metadata`](#metadata) | Generate / approve / refresh `metadata.yaml` (RAG accuracy) |
| [`relationships`](#relationships) | Generate / approve / render `relationships.yaml` (RAG accuracy) |
| [`examples`](#examples) | Generate / approve / harvest / review / eval synthetic Q→SQL examples (RAG accuracy) |

See [Per-person chat history](#per-person-chat-history) for `--user`, `session_id`, and the session-end curation loop.

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
medallion query <project> "<question>" [--projects PATH] [--model MODEL] [--provider PROVIDER] [--detect-ambiguity] [--decompose] [--use-templates] [--user NAME]
```

| Flag | Default | Description |
| --- | --- | --- |
| `--projects` | `.` | Projects root directory |
| `--model` | from settings / `llama3.2` | Model identifier |
| `--provider` | from settings / `ollama` | LLM backend: `ollama` \| `openrouter` \| `openai` \| custom |
| `--detect-ambiguity` | off | One extra, *dedicated* LLM call checks whether the question is ambiguous before any SQL generation is attempted. A second, always-on mechanism (no flag) already does this for free — see below; this flag adds a guaranteed check independent of it. |
| `--decompose` | off | One extra LLM call checks whether the question contains independent sub-questions; each runs through the pipeline separately if so |
| `--use-templates` | off | A high-confidence match to a curated `templated: true` example (see [Template-Routed Query Layer](../guides/rag-accuracy.md#6-template-routed-queries)) skips SQL generation entirely — the LLM only fills the template's declared `{param}` slots. Off by default; the filled SQL still goes through the same validation/execution safety net as any other query. |
| `--user` | OS username (`getpass.getuser()`) | Display name chat history is recorded under — see [Per-person chat history](#per-person-chat-history) below |

**Always on, no flag needed:** the main SQL-generation prompt itself instructs the LLM
to respond `CLARIFY: <question>` instead of guessing when the schema doesn't
disambiguate the question — checked on the same call every query already makes, zero
extra cost. Both this and `--detect-ambiguity` raise the same `AmbiguousQuestionError`
(`❓ Your question is ambiguous: ...`). Relies on the model following the instruction —
not a guarantee with smaller local models. See
[RAG Accuracy](../guides/rag-accuracy.md#ambiguity-handling-two-independent-mechanisms)
for the full design.

If a project has `status: approved` tables in `metadata.yaml` and/or
`verified: true` examples in `examples/synthetic.jsonl`, they're picked up
automatically — see [RAG Accuracy](../guides/rag-accuracy.md) for the full design
(dynamic few-shot retrieval, confidence-gated schema pruning).

```bash
medallion query sales_project "What are the top 5 products by revenue?"
medallion query sales_project "Show monthly trends" --model mistral
medallion query sales_project "Headcount by dept and revenue by region" --decompose
medallion query sales_project "Which rep leads in revenue?" --user alice
```

Every question — success or failure — is appended to
`<project>/chat_history/<user>.jsonl` (best-effort; a logging failure never
blocks the answer). One `--user`-less invocation = one CLI session (a fresh
`session_id` per run) — there's no thumbs-up UI on the CLI path, so these
turns stay unrated (`accepted: null`) unless promoted manually.

---

## Per-person chat history

Every `/query` call (from `cortex` or `medallion query`) is logged to
`<project>/chat_history/<username>.jsonl` — a personal audit trail, UI/audit
only, never fed back into the LLM prompt. `username` is a plain display
name, not real auth: `cortex` asks for one once (persisted in the browser's
localStorage), sent as an `X-Medallion-User` header; orthogonal to the
optional `MEDALLION_API_KEY` bearer gate.

Each turn also carries a `session_id` (ephemeral — one per browser tab or
CLI invocation, additive to `username`) and a three-state `accepted` field:
`true`/`false` only ever come from an explicit cortex 👍/👎 (never inferred
from interaction — a guessed satisfaction signal risks reinforcing
plausible-looking-but-wrong SQL), `null` means no feedback was given and
stays excluded from the learning loop forever.

**Closing a session** (`POST /session/end`) is a **curation-promotion
trigger, not a data-sync call** — chat_history is already written
server-side in real time on every `/query`. It rolls that session's rated
turns into the existing `examples/harvested.jsonl`/`failures.jsonl` (which
`medallion examples harvest`/`review` already consume — see
[examples](#examples) below). In cortex, a session closes via an explicit
"End Session" button, a 5-minute idle timeout, or a best-effort tab-close
beacon (`navigator.sendBeacon` — can't carry an `Authorization` header, so
this path only works when `MEDALLION_API_KEY` is unset).

cortex also has a "History" tab listing your own past turns, with a
"↻ Reuse" button per entry that reloads the question into chat.

---

## ask

Start the `neuron` FastAPI server, exposing `cerebrum` over HTTP for `cortex` (or any
HTTP client) to call. Requires `pip install "openmedallion[cerebrum]"`.

```bash
medallion ask <project> [--projects PATH] [--port PORT] [--model MODEL] [--provider PROVIDER]
```

| Endpoint | Purpose |
| --- | --- |
| `POST /query` | Ask a question — same pipeline as `medallion query`. Body: `{question, project, use_templates}` (`use_templates` defaults `false`). Logs a `chat_history` turn (success or failure); returns a `turn_id` for later `/feedback`. |
| `POST /feedback` | Record cortex 👍/👎 — sets the matching `chat_history` turn's `accepted` field in place (three-state: `true`/`false`/`null`, never inferred). Does **not** write `examples/harvested.jsonl`/`failures.jsonl` directly — see `/session/end`. |
| `GET /history?project=X` | Return the calling person's own past turns (cortex's "History" tab). |
| `POST /session/end` | Curation-promotion trigger, **not a data sync** (chat_history is already written in real time). Rolls a closed session's rated turns into `examples/harvested.jsonl`/`failures.jsonl`, which `medallion examples harvest`/`review` consume. |
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
medallion metadata generate <project> [--projects PATH] [--model MODEL] [--provider PROVIDER] [--profile]
medallion metadata approve  <project> [--projects PATH]
medallion metadata refresh  <project> [--projects PATH] [--model MODEL] [--provider PROVIDER] [--check] [--profile]
```

| Subcommand | Purpose |
| --- | --- |
| `generate` | LLM-draft `metadata.yaml` for silver/gold tables not yet `status: approved`. Approved tables are always skipped and preserved untouched. Requires `[cerebrum]`. |
| `approve` | Interactive `[a]pprove / [s]kip / [q]uit` review of draft/stale tables — no LLM call. |
| `refresh` | Detect schema drift (live Parquet vs. the `schema_hash` stored in `metadata.yaml`) and re-draft only affected/unapproved tables. An `approved` table that drifts is flipped to `status: stale` **without** a new LLM draft — review again with `approve` or re-run `refresh` to redraft it. Dropped tables (Parquet no longer exists) are flagged, never deleted. `--check` runs drift detection only (no LLM call, no write) and exits 1 if any table is drifted/dropped — useful in CI. Requires `[cerebrum]` (except `--check`, which makes no LLM call). |

```bash
medallion metadata refresh sales_project --check   # CI gate: exit 1 if schema drifted
medallion metadata refresh sales_project            # detect drift + re-draft affected tables
```

### `--profile` — ydata-profiling enrichment (opt-in)

On `generate` and `refresh`, `--profile` enriches every freshly-drafted table's columns
with three additional fields, computed deterministically via
[ydata-profiling](https://github.com/ydataai/ydata-profiling) (`minimal=True` — no
correlations/interactions) — zero extra LLM calls:

| Field | Populated when | Contains |
| --- | --- | --- |
| `dtype` | always | ydata-profiling's inferred type: `Numeric`, `Categorical`, `Boolean`, `DateTime`, `Text`, ... |
| `stats` | always | `null_pct`, `distinct_count`, plus `min`/`max`/`mean` for `Numeric` columns |
| `accepted_values` | only for low-cardinality `Categorical`/`Boolean` columns (≤ 20 distinct values) | the full sorted list of unique values — `None` for high-cardinality or numeric columns, to avoid dumping huge lists |

`--profile` is opt-in and off by default — the default path stays fast and
dependency-free. Approved-and-skipped tables are never profiled. Requires
`pip install "openmedallion[profile]"`; missing the dependency prints install
guidance instead of a raw traceback.

```bash
medallion metadata generate sales_project --profile
medallion metadata refresh  sales_project --profile
```

---

## relationships

Manage a project's `relationships.yaml` — explicit join paths between silver/gold
tables, detected via deterministic pattern matching (no LLM call for any subcommand).

```bash
medallion relationships generate <project> [--projects PATH]
medallion relationships approve  <project> [--projects PATH]
medallion relationships erd      <project> [--projects PATH] [--all]
```

| Subcommand | Purpose |
| --- | --- |
| `generate` | Detect relationships across silver/gold tables (FK naming, lineage, grain). |
| `approve` | Interactive review of draft/stale relationships. |
| `erd` | Render relationships as a Mermaid `erDiagram` to `<project>/relationships_erd.md`. Approved-only by default; `--all` includes draft/stale. Column types come from real Parquet dtypes, not `metadata.yaml`. No primary-key inference. |

---

## examples

Manage a project's synthetic `(question, sql)` examples used for dynamic few-shot
retrieval (see [RAG Accuracy](../guides/rag-accuracy.md)).

```bash
medallion examples generate <project> [--projects PATH] [--count N] [--model MODEL] [--provider PROVIDER]
medallion examples approve  <project> [--projects PATH] [--template]
medallion examples harvest  <project> [--projects PATH]
medallion examples review   <project> [--projects PATH]
medallion examples eval     <project> [--projects PATH] [--model MODEL] [--provider PROVIDER] [--use-templates]
```

| Subcommand | Purpose |
| --- | --- |
| `generate` | LLM drafts `(question, sql)` pairs from approved metadata + relationships; each is validated (allowlist + DuckDB `EXPLAIN`) before being written. Requires `[cerebrum]`. |
| `approve` | Interactive review of unverified examples — no LLM call. With `--template`, reviews already-`verified: true` examples instead, for promotion to `templated: true` (see [Template-Routed Query Layer](../guides/rag-accuracy.md#6-template-routed-queries)) — auto-detects `{slot}` placeholders in the SQL and prompts for a description per slot. |
| `harvest` | Promotes `examples/harvested.jsonl` (cortex 👍) candidates into `synthetic.jsonl` as unverified — still needs `approve`. |
| `review` | Lists `examples/failures.jsonl` (cortex 👎) entries — a plain listing, no LLM call. |
| `eval` | Admin regression check: re-runs every `verified: true` example's question, executes both the stored ("golden") SQL and the freshly-generated SQL, and compares the **results** (not raw SQL text). Reports a ✅/❌ per question and a match summary — run this after curating metadata/relationships/examples to see whether it helped or hurt. With `--use-templates`, additionally re-runs each question through `CerebrumPipeline(use_templates=True)` and flags `⚠️  template-routing changed the result` wherever that path's outcome diverges from the golden result — the regression signal for the template layer specifically. No model retraining happens anywhere in this project; this checks curation changes, not weight changes. Requires `[cerebrum]`. |

```bash
medallion examples approve  sales_intel --template   # promote a verified example to templated:true
medallion examples eval     sales_intel --use-templates
```

---

## Global Behaviour

- The CLI always reconfigures `sys.stdout` to UTF-8 so emoji output works on Windows.
- All commands exit with a non-zero code on error (config validation failure, missing UDF file, etc.).
- Running `medallion --help` or `medallion <command> --help` prints usage.
