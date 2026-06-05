"""cerebrum/validator.py — SQL allowlist + DuckDB EXPLAIN dry-run + retry loop.

Two-stage validation:
1. Structural allowlist — the statement must start with SELECT or WITH,
   and must not contain any forbidden DDL/DML keywords.
2. DuckDB EXPLAIN — registers all silver Parquet files as views and runs
   ``EXPLAIN <sql>`` to catch syntax and reference errors before execution.

The public helper :func:`validate_and_fix` wraps a 2-round retry loop:
if the initial SQL fails validation, it calls the LLM again and tries once
more before raising.
"""
from __future__ import annotations

import re
from collections.abc import Callable
from pathlib import Path

import duckdb

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
