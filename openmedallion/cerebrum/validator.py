"""cerebrum/validator.py — SQL allowlist + DuckDB EXPLAIN dry-run + retry loop
+ post-execution result sanity check (RAG roadmap Phase 2, build order step 11).

Two-stage validation:
1. Structural allowlist — the statement must start with SELECT or WITH,
   and must not contain any forbidden DDL/DML keywords.
2. DuckDB EXPLAIN — registers all silver Parquet files as views and runs
   ``EXPLAIN <sql>`` to catch syntax and reference errors before execution.

The public helper :func:`validate_and_fix` wraps a 2-round retry loop:
if the initial SQL fails validation, it calls the LLM again and tries once
more before raising. This is the **pre-execution** check — it catches SQL
that DuckDB would reject outright (syntax errors, unknown columns/tables).

:func:`check_result_sanity` is a separate, **post-execution** check: even
syntactically-valid SQL can silently answer the wrong question (e.g. an
overly-strict WHERE clause that matches nothing). Locked design decisions:
- Only triggers on a genuinely unambiguous signal — **zero rows returned**.
  ("Suspiciously large" was in the original Phase 2 plan wording but has no
  natural threshold without knowing a table's typical size, so it was
  deliberately left out rather than inventing an arbitrary constant.)
- Costs **zero extra LLM calls** for the common case of a non-empty result —
  the LLM is only asked to double-check when the result is actually empty.
- The LLM gets **one** confirm-or-fix attempt, not a retry loop: if it
  confirms 0 rows is correct (a legitimately empty result, e.g. "orders from
  a date with no orders"), or if its fix also fails validation, the original
  SQL/result is returned unchanged — never raises, since this is a quality
  improvement pass, not a correctness gate.
"""
from __future__ import annotations

import re
from collections.abc import Callable
from pathlib import Path

import duckdb
import polars as pl

from openmedallion.cerebrum import executor as _executor

_BLOCKLIST = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|GRANT|REVOKE|COPY|ATTACH|DETACH)\b",
    re.IGNORECASE,
)


# ── internal helpers ──────────────────────────────────────────────────────────

def _is_select(sql: str) -> bool:
    normalised = sql.strip().lstrip(";").strip().upper()
    return normalised.startswith("SELECT") or normalised.startswith("WITH")


def _has_forbidden(sql: str) -> bool:
    return bool(_BLOCKLIST.search(sql))


def _explain_ok(sql: str, silver_dir: str | Path) -> tuple[bool, str]:
    """Return (ok, error_message) from a DuckDB EXPLAIN dry-run."""
    silver_dir = Path(silver_dir)
    con = duckdb.connect()
    try:
        for path in sorted(silver_dir.glob("*.parquet")):
            con.execute(
                f"CREATE OR REPLACE VIEW {path.stem} AS "
                f"SELECT * FROM read_parquet('{path}')"
            )
        con.execute(f"EXPLAIN {sql}")
        return True, ""
    except duckdb.Error as exc:
        return False, str(exc)
    finally:
        con.close()


# ── public API ────────────────────────────────────────────────────────────────

def validate(sql: str, silver_dir: str | Path) -> tuple[bool, str]:
    """Return ``(valid, reason)`` for *sql*.

    Parameters
    ----------
    sql:
        Raw SQL string from the LLM.
    silver_dir:
        Path to silver layer — used to register views for the EXPLAIN check.

    Returns
    -------
    tuple[bool, str]
        ``(True, "")`` on success; ``(False, "<reason>")`` on failure.
    """
    if not _is_select(sql):
        return False, "SQL must begin with SELECT or WITH"
    if _has_forbidden(sql):
        m = _BLOCKLIST.search(sql)
        keyword = m.group() if m else "?"
        return False, f"Forbidden keyword: {keyword}"
    return _explain_ok(sql, silver_dir)


def validate_and_fix(
    sql: str,
    silver_dir: str | Path,
    llm_retry_fn: Callable[[str], str],
    original_prompt: str,
    max_retries: int = 2,
) -> str:
    """Validate *sql*; call *llm_retry_fn* up to *max_retries* times on failure.

    Parameters
    ----------
    sql:
        Initial SQL string from the first LLM call.
    silver_dir:
        Path to silver layer (for view registration in EXPLAIN).
    llm_retry_fn:
        Callable that accepts a retry prompt and returns a new SQL string.
    original_prompt:
        Original user question — embedded in the retry message.
    max_retries:
        Number of additional LLM calls allowed (default 2).

    Returns
    -------
    str
        A validated SQL string.

    Raises
    ------
    ValueError
        If all retries are exhausted without producing valid SQL.
    """
    reason = ""
    for attempt in range(max_retries + 1):
        ok, reason = validate(sql, silver_dir)
        if ok:
            return sql
        if attempt < max_retries:
            retry_prompt = (
                f"The following SQL is invalid — {reason}\n"
                f"Original question: {original_prompt}\n"
                f"Bad SQL: {sql}\n\n"
                "Please write a corrected DuckDB SELECT query. "
                "Return ONLY the raw SQL, no explanation."
            )
            sql = llm_retry_fn(retry_prompt)

    raise ValueError(
        f"Could not produce valid SQL after {max_retries} retr{'y' if max_retries == 1 else 'ies'}. "
        f"Last error: {reason}"
    )


def check_result_sanity(
    question: str,
    sql: str,
    result: pl.DataFrame,
    silver_dir: str | Path,
    llm_fn: Callable[[str], str],
) -> tuple[str, pl.DataFrame]:
    """If *result* is empty, ask the LLM to confirm the SQL or fix it once.

    Parameters
    ----------
    question:
        Original user question.
    sql:
        The SQL that produced *result*.
    result:
        The executed query's Polars DataFrame.
    silver_dir:
        Path to silver layer (for validating and re-executing a fix).
    llm_fn:
        Callable that accepts a prompt and returns the LLM's raw response.

    Returns
    -------
    tuple[str, pl.DataFrame]
        ``(sql, result)`` — unchanged if *result* is non-empty, if the LLM
        confirms the original SQL is correct, or if a suggested fix fails
        validation. Otherwise ``(fixed_sql, fixed_result)`` from re-executing
        the LLM's corrected SQL.
    """
    if len(result) > 0:
        return sql, result

    check_prompt = (
        "The following SQL was generated to answer a question, but it returned 0 rows.\n"
        f"Question: {question}\n"
        f"SQL: {sql}\n\n"
        "If this SQL is correct and 0 rows is a genuinely correct answer, respond with exactly: OK\n"
        "Otherwise, respond with ONLY a corrected DuckDB SELECT query — no explanation."
    )
    response = llm_fn(check_prompt).strip()
    if response.rstrip(".").upper() == "OK":
        return sql, result

    ok, _ = validate(response, silver_dir)
    if not ok:
        return sql, result

    fixed_result = _executor.execute(response, silver_dir)
    return response, fixed_result
