# RAG Accuracy — Metadata, Relationships, Examples & Confidence-Gated Retrieval

For small schemas, `cerebrum` shows the LLM every silver table's raw DDL and a static
few-shot list — that's enough. As a schema grows, showing everything hurts accuracy
(bigger prompts, more irrelevant tables to confuse the model) and stale/wrong column
descriptions have no guardrail. This guide covers the accuracy layer built on top of
`cerebrum`: curated, human-reviewed knowledge with a graceful fallback when that
knowledge is missing or doesn't match the question.

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
```

`generate` samples each silver/gold table's column dtypes and distinct values via DuckDB
(deterministically — no LLM call needed for that part), then asks the LLM only for the
description/synonym text. Tables already `status: approved` are **always skipped and
preserved byte-for-byte** — regenerating never clobbers reviewed work.

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

## 4. Self-improvement loop — cortex feedback

`cortex`'s chat tab shows 👍/👎 buttons on every assistant reply. A click POSTs to
neuron's `/feedback` endpoint (cortex never touches project files directly — it only
ever talks to neuron over HTTP):

```bash
medallion examples harvest my_project   # promote 👍 candidates into synthetic.jsonl
                                         # as verified: false — still needs human review
medallion examples review  my_project   # list 👎 failures for triage
```

A thumbs-up means "this looked right to a user," not "the SQL is definitely correct" —
harvested candidates are promoted as **unverified**, so they go through
`medallion examples approve` like any other draft example before becoming real
few-shot context.

---

## Opt-in query features

Two more `CerebrumPipeline` features are off by default (each costs one extra LLM call)
and surfaced via `medallion query` flags:

```bash
medallion query my_project "Show me the good ones" --detect-ambiguity
# raises before any SQL is generated if the question is ambiguous, with a
# clarification question to show the user

medallion query my_project "Headcount by department and revenue by region" --decompose
# splits into independent sub-questions, each run through the full pipeline
# separately — no attempt to merge results into one table
```

---

## What's not built yet

- **Metadata drift detection** — `metadata.yaml`'s `status: stale` value exists in the
  schema, but nothing currently sets a table to `stale` or offers a
  `medallion metadata refresh` that regenerates only stale entries.
- **`MultiQueryResult`/`AmbiguousQuestionError` in neuron/cortex** — only
  `medallion query`'s CLI currently handles these; the HTTP `/query` endpoint and
  cortex's chat UI would need new response shapes to surface a clarification prompt or
  multiple sub-answers.

See the CLI reference's [`metadata`](../reference/cli.md#metadata),
[`relationships`](../reference/cli.md#relationships), and
[`examples`](../reference/cli.md#examples) sections for the full flag list.
