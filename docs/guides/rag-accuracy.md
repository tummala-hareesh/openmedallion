# RAG Accuracy — Metadata, Relationships, Examples, Confidence-Gated Retrieval & Templates

For small schemas, `cerebrum` shows the LLM every silver table's raw DDL and a static
few-shot list — that's enough. As a schema grows, showing everything hurts accuracy
(bigger prompts, more irrelevant tables to confuse the model) and stale/wrong column
descriptions have no guardrail. This guide covers the accuracy layer built on top of
`cerebrum`: curated, human-reviewed knowledge with a graceful fallback when that
knowledge is missing or doesn't match the question — plus the Template-Routed Query
Layer (§6), a second, deterministic query path that skips SQL *generation* entirely for
known, curated questions, closing a correctness gap the rest of this guide's mitigations
can reduce but not eliminate.

See [`examples/sales_intelligence_demo/`](https://github.com/tummala-hareesh/openmedallion/tree/main/examples/sales_intelligence_demo)
for a complete, runnable, offline walkthrough (`show_rag_workflow.py`).

---

## Overview

```
metadata.yaml            table + column descriptions, synonyms, value examples
relationships.yaml       explicit join paths between tables
examples/synthetic.jsonl LLM-generated, human-verified (question, sql) pairs
examples/harvested.jsonl cortex thumbs-up candidates (append-only)
examples/failures.jsonl  cortex thumbs-down failures (append-only)
```

All four files live at the project root (sibling of `main.yaml`) and are all optional —
a project with none of them behaves exactly as before (every table shown, static
few-shot list, no confidence gating).

**Locked scope:** silver and gold layers only. Bronze is excluded — raw dlt ingestion
isn't suitable for user-facing NL queries. In practice `cerebrum`'s runtime only
executes against the **silver** layer (gold Parquet isn't registered as a DuckDB view),
so schema pruning and the confidence gate operate on silver tables specifically.

---

## 1. Metadata — table & column descriptions

```bash
medallion metadata generate my_project     # LLM drafts descriptions for un-approved tables
medallion metadata approve  my_project     # human review: [a]pprove / [s]kip / [q]uit
medallion metadata refresh  my_project     # detect schema drift, re-draft affected tables
```

`generate` samples each silver/gold table's column dtypes and distinct values via DuckDB
(deterministically — no LLM call needed for that part), then asks the LLM only for the
description/synonym text. Tables already `status: approved` are **always skipped and
preserved byte-for-byte** — regenerating never clobbers reviewed work.

`refresh` closes the drift gap: it compares each table's stored schema fingerprint
against a live DuckDB `DESCRIBE` of the actual Parquet. An `approved` table whose schema
has drifted is flipped to `status: stale` **without** a new LLM draft — approval stays a
trust boundary, so a human re-approves or explicitly re-runs `refresh`/`generate` to get
fresh content. `draft`/`stale` tables are re-drafted regardless of drift. Tables dropped
from Parquet entirely are flagged, never deleted. Pass `--check` to detect drift only
(no LLM call, no write) and get a CI-friendly exit code:

```bash
medallion metadata refresh my_project --check   # exit 1 if any table drifted or was dropped
```

### `--profile` — ydata-profiling enrichment (opt-in)

Both `generate` and `refresh` accept `--profile`, which enriches every freshly-drafted
table's columns with three deterministic facts computed via
[ydata-profiling](https://github.com/ydataai/ydata-profiling) — zero extra LLM calls:

```bash
medallion metadata generate my_project --profile   # requires openmedallion[profile]
```

```yaml
columns:
  status:
    description: "Current lifecycle state of the deal."
    dtype: Categorical
    stats: {null_pct: 0.0, distinct_count: 3}
    accepted_values: [closed, open, pending]   # only for low-cardinality categorical/boolean columns
  amount:
    description: "Deal value in USD."
    dtype: Numeric
    stats: {null_pct: 0.02, distinct_count: 214, min: 10.0, max: 98000.0, mean: 4210.5}
    accepted_values: null   # numeric/high-cardinality columns never get one
```

`accepted_values` is only populated for `Categorical`/`Boolean` columns with 20 or fewer
distinct values — high-cardinality and numeric columns get `stats` only, to avoid
dumping huge lists into the prompt context. `--profile` is off by default; the plain
`generate`/`refresh` path stays fast and dependency-free (DuckDB `DESCRIBE` +
8-sample `DISTINCT`, no ydata-profiling call).

```yaml
# metadata.yaml
tables:
  rep_performance:
    layer: silver
    status: approved          # draft | approved | stale — only approved tables
    description: "Every transaction enriched with rep, region, team, and attainment."
    synonyms: [performance, attainment, "revenue by rep"]
    columns:
      attainment_pct:
        description: "Percent of the regional quarterly target this deal represents."
```

Only a table whose `status` is `approved` gets its columns injected into the LLM prompt
context or considered for schema pruning (see §3) — `draft`/`stale` tables are invisible
to both.

`metadata_enhancements.yaml` (optional, user-maintained) deep-merges on top of
`metadata.yaml` at load time — hand-written descriptions/synonyms survive a
regeneration untouched, the same way `.env` overrides a `.env.example`.

!!! note "No structured metrics glossary"
    The LLM may suggest candidate business metrics as prose inside a column's
    `description:` (e.g. "this looks like it could be `revenue`") — there's no separate
    `metrics:` schema. Formal metric definitions are a deferred, out-of-scope idea.

---

## 2. Relationships — explicit join paths

```bash
medallion relationships generate my_project   # no LLM call — pure pattern matching
medallion relationships approve  my_project
```

Detection is three deterministic rules, no LLM involved:

| Rule | Signal | Confidence |
| --- | --- | --- |
| `fk_naming` | A `<entity>_id` column shared across tables, resolved to a target table via pluralization | `high` |
| `lineage` | Table A's columns are a proper subset of table B's (B enriches A) | `medium` |
| `grain` | A non-`_id` string column shared across tables (a dimension without FK naming) | `low` |

```yaml
# relationships.yaml
relationships:
  - from_table: rep_performance
    to_table: targets
    join_on: [region, quarter]
    confidence: high
    status: approved
    # method: omitted — hand-added relationships have no detection rule behind them
```

Regenerating **always keeps** `status: approved` entries and any hand-added entry with
no `method` set untouched; `draft`/`stale` entries with a detection method are replaced
by a fresh detection pass.

### Visualizing relationships — `medallion relationships erd`

```bash
medallion relationships erd my_project           # approved relationships only (default)
medallion relationships erd my_project --all      # include draft/stale too
```

Renders `relationships.yaml` as a Mermaid `erDiagram`, written to
`<project>/relationships_erd.md` — a Markdown file with a fenced `mermaid` code
block, directly viewable on GitHub and embeddable in mkdocs via `--8<--`. No LLM call.

- **Approved-only by default** — matches the "approved is the trust boundary"
  convention used everywhere else in this guide; pass `--all` to see drafts too
  (useful while reviewing what the detector found, before approving anything).
- **Only tables referenced by an included relationship are drawn** — no orphan
  tables cluttering the diagram.
- **Column types come from real Parquet dtypes** (DuckDB `DESCRIBE` against the
  actual silver/gold files), not `metadata.yaml`'s optional `columns:` dict —
  accurate even for a project with no curated metadata at all. A table referenced
  by a relationship but missing from Parquet still renders (an empty entity block)
  rather than failing.
- **No primary-key inference** — nothing in `relationships.yaml`/`metadata.yaml`
  actually asserts a primary key, so none is guessed at. Columns render as plain
  typed fields; the relationship line itself implies the FK, with a default
  many-to-one cardinality (`from_table` is the "many" side).

---

## 3. Dynamic few-shot retrieval + confidence-gated schema pruning

Both features are driven by the same embedding-based ranking technique
(`cerebrum/retrieval.py`), with **ChromaDB used only as an embedding-function
provider** — ranking itself is plain cosine similarity, since a corpus of a few dozen
examples/tables doesn't need a real vector database.

### Dynamic few-shot retrieval

```bash
medallion examples generate my_project --count 15   # LLM drafts (question, sql) pairs
medallion examples approve  my_project
```

`generate` reads `status: approved` **silver-layer** tables and approved relationships
between them, asks the LLM for a batch of representative `(question, sql)` pairs, and
validates every generated SQL through the same allowlist + DuckDB `EXPLAIN` check
`cerebrum` uses at query time — an example whose SQL never validates is dropped, not
written with an error flag.

```json
{"question": "Which rep had the highest revenue in Q2 2024?", "sql": "SELECT name, SUM(amount) ...", "verified": true}
```

Only `verified: true` examples are eligible as few-shot context — `CerebrumPipeline`
ranks them against the current question and injects the top 3, falling back to the
static built-in few-shot list when there's no `examples/` directory or no verified
examples yet.

### Confidence-gated schema pruning

For a question, `rank_relevant_tables_scored()` ranks every `status: approved`
silver-layer table by embedding-similarity to the question and returns **both** the
top-k table names and the top-1 similarity score — the confidence signal.

```
confidence >= 0.7   →  structured ranking is trusted; only the top-k approved
                        tables' DDL is shown to the LLM

confidence <  0.7   →  the curated corpus doesn't confidently answer this question,
                        so the pipeline falls back to a raw-schema search: every
                        silver table's DDL (any status, no metadata.yaml required)
                        is ranked instead
```

This is the **fallback** half of the locked hybrid-retrieval architecture: structured
knowledge is the primary path, and a broader raw-schema search is the safety net —
important for a project with only partial metadata coverage (some tables curated,
others not yet), or none at all.

```python
from openmedallion.cerebrum.schema import rank_relevant_tables_scored, describe_all_tables
from openmedallion.cerebrum.retrieval import CONFIDENCE_THRESHOLD  # 0.7

names, confidence = rank_relevant_tables_scored(question, metadata, embed_fn, top_k=5)
if confidence < CONFIDENCE_THRESHOLD:
    # describe_all_tables() needs no metadata.yaml at all
    raw_tables = describe_all_tables(silver_dir)
```

`CerebrumPipeline` wires this automatically — nothing to configure. When a fallback
fires, it surfaces via the `on_step` progress callback (visible in `medallion query`'s
CLI output):

```
·  schema confidence 0.42 below threshold 0.7 — using raw-schema fallback search
```

Both the approved-tables embedding and the raw-schema embedding are built lazily and
cached **per `CerebrumPipeline` instance** — computed once on first use, never
persisted to disk, and re-ranked (cheaply) against every new question.

!!! note "Single confidence score, no blending"
    Confidence comes from the table-ranking score only — it is **not** blended with
    the few-shot example-ranking score. Showing the LLM the wrong or incomplete schema
    is the higher-leverage failure mode; few-shot retrieval degrades more gracefully
    (an irrelevant example still leaves the schema intact) and is left ungated.

---

## 4. Self-improvement loop — per-person chat history + session-based curation

Every `/query` call — from `cortex` or `medallion query` — is logged to
`<project>/chat_history/<username>.jsonl`: a personal, UI/audit-only turn log, never
fed back into the LLM prompt. `username` is a plain display name, not real auth
(`cortex` asks for one once, persisted in the browser's localStorage) — orthogonal to
the optional `MEDALLION_API_KEY` bearer gate. Each turn also carries an ephemeral
`session_id` (one per browser tab or CLI invocation) and a three-state `accepted`
field.

```yaml
# one line of chat_history/alice.jsonl
{ts: ..., turn_id: "a1b2c3", session_id: "sess-1", question: ..., sql: ...,
 response_generated: true, accepted: null, promoted: false}
```

**`accepted` is locked as three-state and never inferred.** `true`/`false` only ever
come from an explicit cortex 👍/👎 click — never from interaction heuristics like "no
re-ask within N minutes" or "the query returned rows." A non-empty result can still be
semantically wrong (a `MIN(salary)` bug for a "highest paid employee" question is
syntactically valid, wrong SQL, and only a human catches it) — a guessed satisfaction
signal risks reinforcing a wrong answer into the few-shot corpus, which is worse than
having no signal at all. `null` (no feedback given) stays permanently excluded from
learning.

`cortex`'s chat tab shows 👍/👎 buttons on every assistant reply. A click POSTs to
neuron's `/feedback` endpoint, which sets that turn's `accepted` field **in place** —
it does **not** write `examples/harvested.jsonl`/`failures.jsonl` directly (cortex
never touches project files directly either way — it only ever talks to neuron over
HTTP).

**Closing a session is the curation trigger**, not a data-sync step — chat_history is
already written server-side in real time on every `/query`, so there's nothing left to
"sync." `POST /session/end` rolls a closed session's rated turns into
`harvested.jsonl`/`failures.jsonl`. A session closes three ways in cortex:

- an explicit **"End Session"** button in the sidebar,
- a **5-minute idle timeout**,
- a best-effort **tab-close beacon** (`navigator.sendBeacon` — can't carry an
  `Authorization` header, so this path only works when `MEDALLION_API_KEY` is unset).

```bash
medallion examples harvest my_project   # promote 👍 candidates into synthetic.jsonl
                                         # as verified: false — still needs human review
medallion examples review  my_project   # list 👎 failures for triage
```

A thumbs-up means "this looked right to a user," not "the SQL is definitely correct" —
harvested candidates are promoted as **unverified**, so they go through
`medallion examples approve` like any other draft example before becoming real
few-shot context.

cortex also has a "History" tab listing your own past turns, with a "↻ Reuse" button
per entry that reloads the question into chat.

---

## 5. Checking whether curation helped — `medallion examples eval`

No model retraining happens anywhere in this project — "learning" means curating
better few-shot examples and metadata that get stuffed into the prompt every time, not
changing an LLM's weights. After curating (`metadata generate`/`approve`,
`relationships generate`/`approve`, `examples generate`/`approve`/`harvest`), check
whether it actually helped:

```bash
medallion examples eval my_project
```

For every `verified: true` example in `synthetic.jsonl`, this re-runs the question
through the current pipeline and **executes both the stored ("golden") SQL and the
freshly-generated SQL, comparing the results** — not the raw SQL text, since
syntactically different SQL (whitespace, `count` vs `COUNT`, column order) can be
semantically identical, and a text diff would manufacture false regressions.

```
✅  How many orders?
❌  What is the total revenue by region?
       golden : SELECT region, SUM(amount) AS total_revenue FROM rep_performance ...
       new    : SELECT region, SUM(amount) AS total_revenue FROM transactions ...
       error  : Binder Error: Referenced column "region" not found in FROM clause!

  1/2 matched
```

A generated SQL that fails to execute is reported as a mismatch with the DuckDB error
attached, never raised — one bad example never aborts the whole eval run.

---

## 6. Template-routed queries

Every mitigation above (schema pruning, dynamic few-shot, error-feedback retries, the
post-execution sanity check) reduces the *rate* of wrong SQL — none can guarantee
correctness, because the LLM re-derives the query's join/aggregation/filter logic from
scratch on every call. The classic failure mode this can't catch: `SELECT MIN(salary)
...` for "who is the highest paid employee?" — syntactically valid, passes
`validate_and_fix`'s `EXPLAIN` check, returns a non-empty (so `check_result_sanity`
never fires) but **semantically wrong** result. Only human review catches this today.

The Template-Routed Query Layer closes that gap for **known, frequently-asked
questions** by promoting an already-verified example's SQL into a reusable,
parameterized template — reviewed once by a human, then executed as-is (with only its
declared parameter *values* re-chosen by the LLM) on every future match, instead of
regenerated from scratch.

```bash
medallion examples approve my_project --template   # promote a verified example
```

`--template` reviews already-`verified: true` examples (a stricter, later, separate pass
than verification itself), auto-detects `{slot}` placeholders in the stored SQL, and
prompts for a description/constraint per slot:

```json
{"question": "What is the total revenue by region for a given quarter?",
 "sql": "SELECT region, SUM(amount) AS total_revenue FROM rep_performance WHERE quarter = {quarter} GROUP BY region ORDER BY total_revenue DESC",
 "verified": true, "templated": true,
 "params": {"quarter": "SQL string literal quarter code, e.g. '2024-Q2'"}}
```

### How a match is routed

Template matching reuses the same embedding-ranking machinery as dynamic few-shot
retrieval and schema pruning (`cerebrum/retrieval.py`) — a third application of the same
`embed_payloads()`/`rank_payloads_scored()` core — but gated by a **stricter** threshold,
`TEMPLATE_CONFIDENCE_THRESHOLD = 0.85` (vs. schema pruning's `CONFIDENCE_THRESHOLD =
0.7`), because a false-positive template match skips SQL generation entirely — a
costlier mistake than schema pruning showing slightly the wrong table set.

```
confidence >= 0.85   →  matched. The LLM is asked ONLY to fill the template's declared
                         {param} slots (a narrow, single-purpose prompt that never shows
                         or invites editing the SQL itself) — never to author query logic.

confidence <  0.85,
or no templates exist  →  falls through to the normal SQL-generation pipeline, unchanged.
```

**The safety net is not bypassed — only regeneration is.** Filled SQL still goes through
the exact same `validate_and_fix()` (allowlist + `EXPLAIN` + retry-on-error) and
`check_result_sanity()` as any other query. A malformed parameter fill (bad date format,
an out-of-constraint value) is still caught before/after execution; it just can never
introduce a *new* join/aggregation bug, because that logic was written once and reviewed.

```
·  matched template 'What is the total revenue by region for a given quarter?' (confidence 0.91) — filling parameters
·  validating filled template SQL
```

### Opt-in, off by default

Unlike schema pruning/few-shot (which only ever narrow *context*, never change *which*
query logic runs), a template match changes what SQL actually executes — so this is
opt-in, matching `detect_ambiguity`/`decompose_queries`'s convention rather than
`metadata`/`examples_dir`'s existence-gated one:

```bash
medallion query my_project "Revenue by region for 2024-Q2" --use-templates
```

```python
CerebrumPipeline(silver_dir, examples_dir=examples_dir, use_templates=True)
```

neuron's `/query` takes the same flag in its request body (`use_templates`, default
`false`) — see [ask](../reference/cli.md#ask). Not wired into cortex's UI yet (flagged,
not silently bundled — matches this roadmap's own precedent for deferred UI work).

### Checking whether templating helped — `examples eval --use-templates`

```bash
medallion examples eval my_project --use-templates
```

Extends `examples eval` (§5): each verified question is *additionally* run through
`CerebrumPipeline(use_templates=True)`, and its result compared against the golden
result — flagging `⚠️  template-routing changed the result` wherever the two diverge.
This is the concrete regression signal for the template layer specifically, distinct
from §5's normal-path-vs-golden check.

See [`examples/sales_intelligence_demo/show_rag_workflow.py`](https://github.com/tummala-hareesh/openmedallion/tree/main/examples/sales_intelligence_demo)
(§4 of that script) for a full, runnable, offline demonstration of a template match,
fill, and safety-net validation.

---

## Ambiguity handling — two independent mechanisms

`AmbiguousQuestionError` (with a `.clarification` message) can be raised by either
mechanism below — callers only need one `except AmbiguousQuestionError` clause.

**Always on, zero extra cost.** The main SQL-generation prompt (`cerebrum/prompt.py`'s
`_SYSTEM`) instructs the LLM: if the schema doesn't disambiguate the question, respond
`CLARIFY: <question>` instead of guessing SQL. This is checked on the *same* LLM call
every query already makes — no separate call, no flag needed:

```bash
medallion query my_project "Show me the good ones"
# → ❓ Your question is ambiguous: Good by which measure — revenue, units, or something else?
```

Relies on the model choosing to follow that instruction, so it's not a guarantee —
smaller local models sometimes hallucinate SQL against an ambiguous question instead of
asking. `detect_ambiguity` below exists as a heavier, more reliable alternative when
you want a dedicated check regardless of the main call's behavior.

**Opt-in, one extra LLM call.** `detect_ambiguity` makes a *dedicated* call, before any
SQL generation is attempted at all:

```bash
medallion query my_project "Show me the good ones" --detect-ambiguity
```

`decompose_queries` is a separate, also opt-in feature (each costs one extra LLM call):

```bash
medallion query my_project "Headcount by department and revenue by region" --decompose
# splits into independent sub-questions, each run through the full pipeline
# separately — no attempt to merge results into one table
```

The inline `CLARIFY:` check still applies to each sub-question's own SQL-generation
call under `--decompose`.

---

## What's not built yet

- **`MultiQueryResult`/`AmbiguousQuestionError` in neuron/cortex** — only
  `medallion query`'s CLI currently handles these; the HTTP `/query` endpoint and
  cortex's chat UI would need new response shapes to surface a clarification prompt or
  multiple sub-answers.
- **Template routing (§6) has no cortex UI surfacing** — `--use-templates`/`use_templates`
  work identically via `medallion query` and neuron's `/query`, but cortex's chat UI
  doesn't yet distinguish "answered via template" from "answered via generated SQL."

See the CLI reference's [`metadata`](../reference/cli.md#metadata),
[`relationships`](../reference/cli.md#relationships), and
[`examples`](../reference/cli.md#examples) sections for the full flag list.
