"""cerebrum/prompt.py — System prompt + few-shot DuckDB SQL builder.

Inline clarification (``CLARIFY:`` contract)
---------------------------------------------
The main SQL-generation call itself may now respond with a clarification
question instead of SQL, when the schema doesn't disambiguate the question
(e.g. an undefined term, or a plausible match to more than one table/column).
This is deliberately **inline** — no separate LLM call — unlike the opt-in
``detect_ambiguity`` pre-check in ``cerebrum/decomposition.py``, which makes
a dedicated call *before* attempting SQL generation at all. The two are
independent and complementary: ``detect_ambiguity`` catches ambiguity before
any SQL-generation attempt (useful when you want a guaranteed check), this
inline path catches it for free within the SQL-generation call every query
already makes, at zero extra cost. Either can trigger
:class:`~openmedallion.cerebrum.pipeline.AmbiguousQuestionError`.

``pipeline.py`` calls :func:`is_clarification_response` on the raw LLM
response *before* handing it to ``validator.validate_and_fix()`` — a
``CLARIFY:``-prefixed response is not SQL and must never enter the
SQL-allowlist/retry path (it would just look like invalid SQL and burn
retries pointlessly instead of surfacing the clarification cleanly).
"""
from __future__ import annotations

import json
import re

_CLARIFY_PREFIX = "CLARIFY:"
_FENCE_PATTERN  = re.compile(r"^```(?:\w*)?\s*|\s*```$", re.IGNORECASE)

_SYSTEM = """\
You are an expert data analyst writing DuckDB SQL queries against silver-layer Parquet tables.

Rules:
- Write only a single SELECT statement or a WITH … SELECT CTE block.
- Never use INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, TRUNCATE, or any DDL/DML.
- Reference tables exactly as shown in the schema — they are registered as DuckDB views.
- Use DuckDB dialect: strftime, date_trunc, list_agg, epoch, etc. where appropriate.
- Return ONLY the raw SQL — no markdown fences, no explanation, no leading/trailing text.
- If the question is about the database or tables themselves (e.g. "what tables are
  there?", "what columns does orders have?", "describe the schema"), answer it with a
  SELECT against DuckDB's information_schema (information_schema.tables,
  information_schema.columns, etc.) — this only exposes the tables shown to you above,
  nothing external. Never invent table/column names not present in the schema; if a
  meta-question can't be answered from information_schema alone, use CLARIFY: instead.
- If the question is ambiguous or missing information you need to write correct SQL
  (an undefined term, or it plausibly matches more than one table/column in the
  schema), do NOT guess. Instead respond with ONLY: CLARIFY: <your question to the user>
  — nothing else, no SQL, no explanation. Only do this when genuinely necessary;
  prefer writing SQL whenever the schema gives you enough to answer correctly.
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
    {
        "question": "Show me the good ones",
        "sql": "CLARIFY: Good by which measure — highest revenue, most units, or something else?",
    },
    {
        "question": "What tables are in this database?",
        "sql": "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'",
    },
    {
        "question": "What columns does the orders table have?",
        "sql": (
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name = 'orders'"
        ),
    },
]


def is_clarification_response(raw: str) -> str | None:
    """Return the clarification text if *raw* uses the ``CLARIFY:`` contract,
    else ``None``. Pure — no LLM call.

    The prefix must anchor at the start of the (fence-stripped, whitespace-
    trimmed) response — a SQL comment that happens to contain the word
    "clarify" elsewhere must never be misdetected as a clarification.
    """
    if not raw:
        return None
    cleaned = _FENCE_PATTERN.sub("", raw.strip()).strip()
    if cleaned.upper().startswith(_CLARIFY_PREFIX):
        return cleaned[len(_CLARIFY_PREFIX):].strip()
    return None


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


# ── Template-Routed Query Layer (see CLAUDE.md), build order step 4 ───────────
#
# A narrow, single-purpose prompt — analogous in spirit to decomposition.py's
# detect_ambiguity/decompose_question — used only when cerebrum/retrieval.py
# has already matched the question to a curated, human-reviewed template
# above TEMPLATE_CONFIDENCE_THRESHOLD. The LLM's job here is strictly
# narrower than normal SQL generation: choose parameter *values* for the
# template's already-declared, already-reviewed {param} slots. It is never
# shown an invitation to rewrite the query logic itself.

def build_template_fill_prompt(question: str, template: dict) -> str:
    """Return a prompt asking the LLM to fill *template*'s declared params.

    Parameters
    ----------
    question:
        The user's actual natural-language question (may differ in wording/
        specifics from the template's own ``question``, e.g. a different
        date range or region — that's exactly what params exist to capture).
    template:
        One entry from :func:`~cerebrum.retrieval.load_templated_examples` —
        a ``{"question": ..., "sql": ..., "params": {...}}`` dict. Deliberately
        does **not** interpolate ``template["sql"]`` into the prompt — the
        LLM is asked only about params, never invited to see or edit SQL.

    Returns
    -------
    str
        Complete prompt ready for the LLM. The expected response is a bare
        JSON object of ``{param_name: value}`` — see
        :func:`parse_template_fill_response`.
    """
    params = template.get("params") or {}
    if params:
        params_block = "\n".join(f"- {name}: {desc}" for name, desc in params.items())
    else:
        params_block = "(none)"

    return (
        "A user's question has already been matched to a pre-approved, reviewed "
        "query template. Your only job is to choose values for the template's "
        "parameters based on the user's actual question — do NOT write SQL, do "
        "NOT change the query logic.\n\n"
        f"Template question (example): {template.get('question', '')}\n"
        f"Parameters to fill:\n{params_block}\n\n"
        f"User's actual question: {question}\n\n"
        "Respond with ONLY a JSON object mapping each parameter name to its "
        "value as a string, ready to substitute literally into the SQL — no "
        "explanation, no markdown code fences. If there are no parameters, "
        "respond with an empty JSON object: {}"
    )


def parse_template_fill_response(raw: str) -> dict[str, str] | None:
    """Parse the LLM's response to :func:`build_template_fill_prompt`.

    Returns ``None`` (never raises) on invalid JSON, a non-object top-level
    value, or any non-string value — callers should treat ``None`` as "the
    fill failed" and fall back to normal SQL generation rather than trusting
    a malformed substitution.
    """
    if not raw:
        return None
    cleaned = _FENCE_PATTERN.sub("", raw.strip()).strip()
    try:
        parsed = json.loads(cleaned)
    except json.JSONDecodeError:
        return None
    if not isinstance(parsed, dict):
        return None
    if not all(isinstance(v, str) for v in parsed.values()):
        return None
    return parsed


def fill_template(sql: str, values: dict[str, str]) -> str:
    """Substitute literal ``{param}`` placeholders in *sql* with *values*.

    Pure string substitution — no SQL parsing. Keys in *values* not present
    in *sql* are silently ignored (a fill response may be a superset of the
    template's actual placeholders without harm). Keys present in *sql* but
    missing from *values* are left as literal ``{param}`` text, which will
    then fail ``validate_and_fix()``'s ``EXPLAIN`` dry-run downstream rather
    than silently executing malformed SQL.
    """
    result = sql
    for name, value in values.items():
        result = result.replace("{" + name + "}", value)
    return result
