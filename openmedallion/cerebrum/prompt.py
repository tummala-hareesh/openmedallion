"""cerebrum/prompt.py — System prompt + few-shot DuckDB SQL builder."""
from __future__ import annotations

_SYSTEM = """\
You are an expert data analyst writing DuckDB SQL queries against silver-layer Parquet tables.

Rules:
- Write only a single SELECT statement or a WITH … SELECT CTE block.
- Never use INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, TRUNCATE, or any DDL/DML.
- Reference tables exactly as shown in the schema — they are registered as DuckDB views.
- Use DuckDB dialect: strftime, date_trunc, list_agg, epoch, etc. where appropriate.
- Return ONLY the raw SQL — no markdown fences, no explanation, no leading/trailing text.
"""

_FEW_SHOT: list[dict[str, str]] = [
    {
        "question": "How many orders were placed each month?",
        "sql": (
            "SELECT strftime(order_date, '%Y-%m') AS month, COUNT(*) AS orders "
            "FROM orders GROUP BY month ORDER BY month"
        ),
    },
    {
        "question": "What are the top 5 customers by total spend?",
        "sql": (
            "SELECT customer_id, SUM(amount) AS total_spend "
            "FROM orders GROUP BY customer_id "
            "ORDER BY total_spend DESC LIMIT 5"
        ),
    },
    {
        "question": "Show average salary by department",
        "sql": (
            "SELECT department_id, AVG(salary) AS avg_salary "
            "FROM employees GROUP BY department_id ORDER BY avg_salary DESC"
        ),
    },
]


def build_prompt(
    schema_context: str,
    question: str,
    *,
    few_shot: list[dict[str, str]] | None = None,
) -> str:
    """Return a fully-formed LLM prompt embedding the schema and question.

    Parameters
    ----------
    schema_context:
        DDL-style schema string from :func:`~cerebrum.schema.build_schema_context`.
    question:
        Natural-language question from the user.
    few_shot:
        Optional dynamically-retrieved examples (see
        :mod:`~openmedallion.cerebrum.retrieval`) — a list of
        ``{"question": ..., "sql": ...}`` dicts, most-relevant first. When
        omitted (or empty — e.g. no ``verified: true`` examples exist yet),
        falls back to the static built-in few-shot list unchanged.

    Returns
    -------
    str
        Complete prompt ready for the LLM.
    """
    examples = few_shot or _FEW_SHOT
    few_shot_block = "\n".join(
        f"Q: {ex['question']}\nSQL: {ex['sql']}" for ex in examples
    )
    return (
        f"{_SYSTEM}\n\n"
        f"Schema:\n{schema_context}\n\n"
        f"Examples:\n{few_shot_block}\n\n"
        f"Q: {question}\nSQL:"
    )


def system_prompt() -> str:
    """Return the bare system prompt string (for chat-mode API calls)."""
    return _SYSTEM
