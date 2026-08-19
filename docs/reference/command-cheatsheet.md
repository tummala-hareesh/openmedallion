# Command Cheatsheet

Every `medallion` command and its flags, in one page. For prose explanations of *why*
each flag exists, see the [CLI Reference](cli.md) and the [RAG Accuracy guide](../guides/rag-accuracy.md).

```
medallion <command> [subcommand] [args] [flags]
```

---

## Pipeline

```bash
medallion init <project> [--path-project PATH] [--path-data PATH]
medallion run  <project> [--layer bronze|silver|gold|export|explore] [--projects PATH] [--no-explore]
```

| Command | Flags |
| --- | --- |
| `init` | `--path-project` (default `.`) · `--path-data` (default `data`) |
| `run` | `--layer` (default `gold`) · `--projects` (default `.`) · `--no-explore` (skip all inline `explore:` report generation) |

```bash
medallion init sales_intel
medallion run  sales_intel --layer silver
medallion run  sales_intel --no-explore
```

---

## Ask questions (`cerebrum`) — requires `openmedallion[cerebrum]`

```bash
medallion query <project> "<question>" [--projects PATH] [--model MODEL] [--provider PROVIDER]
                [--nlg-model MODEL] [--nlg-provider PROVIDER]
                [--detect-ambiguity] [--decompose] [--use-templates] [--user NAME]

medallion ask <project> [--projects PATH] [--port PORT] [--model MODEL] [--provider PROVIDER]
```

| Flag | Command | Default | What it does |
| --- | --- | --- | --- |
| `--projects` | both | `.` | Projects root directory |
| `--model` | both | settings / `llama3.2` | SQL-generation model |
| `--provider` | both | settings / `ollama` | LLM backend: `ollama` \| `openrouter` \| `openai` \| custom |
| `--nlg-model` / `--nlg-provider` | `query` | same as `--model`/`--provider` | Separate model for `recommend()` + schema/meta-question answers |
| `--detect-ambiguity` | `query` | off | 1 extra LLM call: dedicated ambiguity pre-check before any SQL generation |
| `--decompose` | `query` | off | 1 extra LLM call: splits a multi-part question into independent sub-questions |
| `--use-templates` | `query` | off | High-confidence match to a curated `templated: true` example skips SQL generation entirely — see [Template-Routed Query Layer](../guides/rag-accuracy.md#6-template-routed-queries) |
| `--user` | `query` | OS username | Display name chat history is recorded under |
| `--port` | `ask` | `8000` | neuron HTTP server port |

```bash
medallion query sales_intel "What are the top 5 products by revenue?"
medallion query sales_intel "Show me the good ones" --detect-ambiguity
medallion query sales_intel "Headcount by dept and revenue by region" --decompose
medallion query sales_intel "Revenue by region for 2024-Q2" --use-templates
medallion query sales_intel "Which rep leads in revenue?" --user alice

medallion ask sales_intel --port 8001 --model mistral
```

**Always on, no flag:** the SQL-generation prompt itself can respond `CLARIFY: <question>`
instead of guessing when a question is ambiguous — zero extra cost, no guarantee (model
must choose to follow it). `--detect-ambiguity` adds a dedicated, guaranteed check.

**neuron endpoints** (`medallion ask`): `POST /query` `{question, project, use_templates}`
· `POST /feedback` `{project, turn_id, thumbs_up}` · `GET /history?project=X` ·
`POST /session/end` `{project, session_id, username}` · `GET /health` · `GET /docs`

---

## UI (`cortex`) — requires `openmedallion[cortex]`

```bash
medallion cortex <project> [--neuron-url URL] [--port PORT] [--debug]
```

| Flag | Default | What it does |
| --- | --- | --- |
| `--neuron-url` | `http://localhost:8000` | Base URL of a running `medallion ask` server |
| `--port` | `8050` | Dash server port |
| `--debug` | off | Dash hot-reload (development mode) |

```bash
medallion ask    sales_intel &
medallion cortex sales_intel --neuron-url http://localhost:8000
```

Tabs: **chat** (👍/👎 feedback, "End Session" button) · **table** · **dashboard** ·
**history** ("↻ Reuse" a past question).

---

## Curate accuracy knowledge — RAG accuracy roadmap

All three families below are optional, human-in-the-loop review loops that feed
`medallion query`/`ask` automatically once curated — no extra query-time flag needed
(except templates, which are opt-in — see below). Full design: [RAG Accuracy guide](../guides/rag-accuracy.md).

### `metadata` — table & column descriptions

```bash
medallion metadata generate <project> [--projects PATH] [--model MODEL] [--provider PROVIDER] [--profile]
medallion metadata approve  <project> [--projects PATH]
medallion metadata refresh  <project> [--projects PATH] [--model MODEL] [--provider PROVIDER] [--check] [--profile]
```

| Subcommand | Flags | What it does |
| --- | --- | --- |
| `generate` | `--profile` | LLM drafts `metadata.yaml` for un-approved silver/gold tables. `--profile` adds `dtype`/`stats`/`accepted_values` via ydata-profiling (requires `[profile]`), zero extra LLM calls. |
| `approve` | — | Interactive `[a]pprove / [s]kip / [q]uit` review — no LLM call |
| `refresh` | `--check`, `--profile` | Detects schema drift, re-drafts affected tables. `--check` is drift-detection-only, no write, CI-friendly exit code |

```bash
medallion metadata generate sales_intel --profile
medallion metadata approve  sales_intel
medallion metadata refresh  sales_intel --check   # CI gate
```

### `relationships` — explicit join paths (no LLM, ever)

```bash
medallion relationships generate <project> [--projects PATH]
medallion relationships approve  <project> [--projects PATH]
medallion relationships erd      <project> [--projects PATH] [--all]
```

| Subcommand | Flags | What it does |
| --- | --- | --- |
| `generate` | — | Pattern-matches FK naming / lineage / grain across silver+gold tables |
| `approve` | — | Interactive review |
| `erd` | `--all` | Renders `relationships.yaml` as a Mermaid `erDiagram` → `<project>/relationships_erd.md`. Approved-only by default; `--all` includes draft/stale |

```bash
medallion relationships generate sales_intel
medallion relationships erd      sales_intel --all
```

### `examples` — synthetic Q→SQL pairs, self-improvement loop, templates

```bash
medallion examples generate <project> [--projects PATH] [--count N] [--model MODEL] [--provider PROVIDER]
medallion examples approve  <project> [--projects PATH] [--template]
medallion examples harvest  <project> [--projects PATH]
medallion examples review   <project> [--projects PATH]
medallion examples eval     <project> [--projects PATH] [--model MODEL] [--provider PROVIDER] [--use-templates]
```

| Subcommand | Flags | What it does |
| --- | --- | --- |
| `generate` | `--count` | LLM drafts `(question, sql)` pairs from approved metadata + relationships; each validated via allowlist + `EXPLAIN` |
| `approve` | `--template` | Interactive review of unverified examples. With `--template`: reviews already-`verified: true` examples for promotion to `templated: true` — auto-detects `{slot}` params, prompts for a description each |
| `harvest` | — | Promotes cortex 👍 candidates (`harvested.jsonl`, written at session-end) into `synthetic.jsonl` as unverified |
| `review` | — | Lists cortex 👎 failures (`failures.jsonl`) — plain listing, no LLM |
| `eval` | `--use-templates` | Re-runs verified questions, compares golden vs. freshly-generated SQL results. `--use-templates` additionally compares against a `use_templates=True` run, flagging where template-routing changed the outcome |

```bash
medallion examples generate sales_intel --count 15
medallion examples approve  sales_intel
medallion examples approve  sales_intel --template
medallion examples harvest  sales_intel
medallion examples review   sales_intel
medallion examples eval     sales_intel
medallion examples eval     sales_intel --use-templates
```

---

## Typical first-run sequence

```bash
medallion init sales_intel
# ... edit sales_intel/{main,bronze,silver,gold}.yaml ...
medallion run sales_intel

# curate accuracy knowledge (optional, all human-reviewed)
medallion metadata      generate sales_intel && medallion metadata      approve sales_intel
medallion relationships generate sales_intel && medallion relationships approve sales_intel
medallion examples      generate sales_intel && medallion examples      approve sales_intel

# promote a few high-value verified examples into deterministic templates
medallion examples approve sales_intel --template

# check curation actually helped before shipping
medallion examples eval sales_intel --use-templates

# go live
medallion ask    sales_intel &
medallion cortex sales_intel
```

---

## Environment variables

Set directly, or via `settings.yaml` (project root or `~/.medallion/settings.yaml`) —
env vars win. See `examples/settings.yaml.example`.

| Variable | Default | Purpose |
| --- | --- | --- |
| `MEDALLION_PROJECTS_ROOT` | `.` | Parent directory of per-project folders (neuron/cortex) |
| `MEDALLION_LLM_PROVIDER` | `ollama` | SQL-model backend |
| `MEDALLION_LLM_MODEL` | `llama3.2` | SQL-model identifier |
| `MEDALLION_LLM_API_KEY` | — | API key for non-Ollama providers |
| `MEDALLION_LLM_BASE_URL` | — | Override provider endpoint URL |
| `MEDALLION_LLM_NLG_PROVIDER` / `_MODEL` / `_API_KEY` / `_BASE_URL` | falls back to SQL-model settings | Separate NLG model for `recommend()` + meta-questions |
| `MEDALLION_OLLAMA_URL` | `http://localhost:11434` | Ollama base URL |
| `MEDALLION_API_KEY` | unset | If set, enables Bearer-token auth on neuron |
| `MEDALLION_RATE_LIMIT` | `60` | Max requests/min per IP on neuron |
| `MEDALLION_AUDIT_LOG` | `medallion_audit.jsonl` | JSONL audit log path |
