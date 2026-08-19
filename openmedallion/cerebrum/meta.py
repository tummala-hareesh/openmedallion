"""cerebrum/meta.py — Route schema/meta-questions ("what tables exist?",
"what columns does orders have?") to the NLG model instead of the NL→SQL
model.

Two models now sit in ``CerebrumPipeline``: one specialized for writing
SQL against silver/gold data (``model``/``provider``), one for natural-
language answers (``nlg_model``/``nlg_provider`` — defaults to the same
client as the SQL model when not explicitly configured, so this is fully
backward compatible / zero-config).

:func:`is_schema_meta_question` is a pure, LLM-free regex classifier — no
extra LLM call is spent deciding *where* to route a question, matching this
project's "zero cost by default" convention. It intentionally only matches
questions clearly about the database's structure ("what tables", "which
columns", "describe the schema") — anything it doesn't recognise falls
through to the normal NL→SQL pipeline unchanged (which still has its own
information_schema fallback baked into ``prompt.py``'s ``_SYSTEM`` prompt,
as a safety net for a meta-question this classifier misses).

:func:`answer_schema_question` answers a recognised meta-question directly
from the already-built schema context string — no SQL is generated or
executed. The prompt explicitly restricts the model to the schema text
given to it and forbids outside/general knowledge, so "no external data
permitted" holds for this path the same way it holds for the SQL path
(there the guarantee comes from only registering silver/gold Parquet as
DuckDB views; here it comes from the prompt only ever showing that same
schema text).
"""
from __future__ import annotations

import re
from collections.abc import Callable

_META_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"\b(what|which|list)\b.*\btables?\b", re.IGNORECASE),
    re.compile(r"\btables?\b.*\b(exist|available|is there|are there|do (you|we) have)\b", re.IGNORECASE),
    re.compile(r"\b(what|which|list)\b.*\bcolumns?\b", re.IGNORECASE),
    re.compile(r"\bcolumns?\b.*\b(exist|available|does .* have|are there)\b", re.IGNORECASE),
    re.compile(r"\bdescribe\b.*\b(table|schema|database)\b", re.IGNORECASE),
    re.compile(r"\b(database|table)\s+schema\b", re.IGNORECASE),
]


def is_schema_meta_question(question: str) -> bool:
    """Return True if *question* is asking about the database's structure
    (tables/columns/schema) rather than its data. Pure, no LLM call."""
    if not question:
        return False
    return any(p.search(question) for p in _META_PATTERNS)


def answer_schema_question(
    question: str,
    schema_context: str,
    llm_fn: Callable[[str], str],
) -> str:
    """Answer a schema/meta-question directly from *schema_context* — no SQL.

    Parameters
    ----------
    question:
        The user's natural-language question about the database's structure.
    schema_context:
        DDL-style schema string from
        :func:`~openmedallion.cerebrum.schema.build_schema_context`.
    llm_fn:
        Callable that accepts a prompt string and returns the model's raw
        text response (the pipeline's NLG client).

    Returns
    -------
    str
        A plain-language answer, stripped of surrounding whitespace.
    """
    prompt_text = (
        "You are answering a question about the STRUCTURE of a database "
        "(its tables and columns), not its data.\n"
        "Use ONLY the schema below — never mention or invent a table/column "
        "not listed here, and never answer using outside/general knowledge.\n\n"
        f"Schema:\n{schema_context}\n\n"
        f"Question: {question}\n"
        "Answer in one or two plain sentences:"
    )
    return llm_fn(prompt_text).strip()
