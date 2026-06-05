"""cerebrum/recommender.py — Generate a canonical reproducible prompt.

Makes a second LLM call with the original question, the executed SQL, and
a brief result summary to produce a precise, self-contained question that
can be copy-pasted to reproduce the same query later.
"""
from __future__ import annotations

from collections.abc import Callable

import polars as pl


def _result_summary(df: pl.DataFrame, max_rows: int = 5) -> str:
    """Return a brief text description of a query result."""
    n_rows, n_cols = df.shape
    col_names = ", ".join(df.columns[:10])
    sample_rows = "\n".join(str(r) for r in df.head(max_rows).to_dicts())
    return (
        f"{n_rows} row(s) × {n_cols} column(s)\n"
        f"Columns: {col_names}\n"
        f"Sample:\n{sample_rows}"
    )


def recommend(
    question: str,
    sql: str,
    result: pl.DataFrame,
    llm_fn: Callable[[str], str],
) -> str:
    """Return a canonical reproducible question for this query.

    Parameters
    ----------
    question:
        Original natural-language question from the user.
    sql:
        The SQL that was executed (already validated).
    result:
        Polars DataFrame returned by the executor.
    llm_fn:
        Callable that accepts a prompt string and returns the LLM response.

    Returns
    -------
    str
        A concise, self-contained question string.
    """
    summary = _result_summary(result)
    prompt = (
        "You were asked to analyse data and produced the following result.\n\n"
        f"Original question: {question}\n\n"
        f"SQL executed:\n{sql}\n\n"
        f"Result summary:\n{summary}\n\n"
        "Write ONE concise, precise question that would reproduce this exact analysis. "
        "It must be self-contained — no references to 'the previous query' or 'above'. "
        "Return ONLY the question, nothing else."
    )
    raw = llm_fn(prompt).strip()
    return raw.strip('"').strip("'")
