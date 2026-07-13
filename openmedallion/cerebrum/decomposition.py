"""cerebrum/decomposition.py — ambiguity detection + query decomposition
(RAG roadmap Phase 2, build order step 12).

Both functions here are pure — no pipeline state, an injectable ``llm_fn``
stands in for the LLM client in tests, matching the module-per-concern
pattern already used by ``validator.py``/``retrieval.py``/``recommender.py``.

Locked design decisions (see CLAUDE.md "Roadmap: RAG Accuracy Improvement"):
- Both features are **opt-in** on ``CerebrumPipeline`` (``detect_ambiguity``/
  ``decompose_queries`` constructor flags, off by default) — matches the
  locked "zero cost by default" philosophy; neither silently adds an LLM
  call to every query for existing callers.
- Query decomposition returns a **list of sub-questions**, not a merged
  result — merging heterogeneous DataFrames (different schemas/grains) has
  no well-defined general solution, so ``CerebrumPipeline`` runs each
  sub-question independently and returns a ``MultiQueryResult`` wrapping the
  list, rather than attempting to synthesize one unified answer.
- ``decompose_question()`` never raises on a malformed LLM response — falls
  back to treating the question as a single (non-decomposed) question, since
  a parsing failure here shouldn't break an otherwise-working query.
"""
from __future__ import annotations

import json
import re
from collections.abc import Callable

_FENCE_PATTERN = re.compile(r"^```(?:json)?\s*|\s*```$", re.IGNORECASE)


def detect_ambiguity(question: str, schema_context: str, llm_fn: Callable[[str], str]) -> str | None:
    """Ask the LLM whether *question* is ambiguous given the schema.

    Args:
        question: The user's natural-language question.
        schema_context: DDL-style schema string (see ``cerebrum/schema.py``).
        llm_fn: Callable that accepts a prompt and returns the LLM's raw response.

    Returns:
        str | None: A clarification question to ask the user, or ``None`` if
        the question is clear enough to generate SQL from directly.
    """
    prompt = f"""You are checking whether a natural-language question is clear enough to
translate into SQL against the schema below, before any SQL is generated.

Schema:
{schema_context}

Question: {question}

If the question is clear and unambiguous given this schema, respond with exactly: CLEAR
If the question is ambiguous or missing information needed to write correct SQL
(e.g. an undefined term, multiple plausible interpretations), respond with ONLY
one clarifying question to ask the user — no explanation, no preamble."""

    response = llm_fn(prompt).strip()
    if response.rstrip(".").upper() == "CLEAR":
        return None
    return response


def decompose_question(question: str, llm_fn: Callable[[str], str]) -> list[str]:
    """Ask the LLM whether *question* contains multiple independent sub-questions.

    Args:
        question: The user's natural-language question.
        llm_fn: Callable that accepts a prompt and returns the LLM's raw response.

    Returns:
        list[str]: The independent sub-questions if *question* decomposes,
        else ``[question]`` unchanged (including on any parsing failure —
        this function never raises).
    """
    prompt = f"""Does the following question actually ask multiple independent things that
would each need a separate SQL query to answer?

Question: {question}

If it is a single question, respond with exactly: SINGLE
If it contains multiple independent sub-questions, respond with ONLY a JSON
array of the separate sub-questions as strings — no explanation, no markdown
code fences."""

    response = llm_fn(prompt).strip()
    if response.rstrip(".").upper() == "SINGLE":
        return [question]

    try:
        text = _FENCE_PATTERN.sub("", response).strip()
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return [question]

    if isinstance(parsed, list) and parsed and all(isinstance(item, str) for item in parsed):
        return parsed
    return [question]
