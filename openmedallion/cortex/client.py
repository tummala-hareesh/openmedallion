"""cortex/client.py — HTTP client to the neuron /query, /feedback,
/history, and /session/end endpoints.

Two implementations share the same interface:

``NeuronClient``
    Production client — calls neuron over HTTP using httpx.

``MockClient``
    Deterministic offline stub — no network or Ollama required.
    Used for UI development and CI (``USE_MOCK_CLIENT=1``).

cortex never writes harvested.jsonl/failures.jsonl or chat_history/*.jsonl
directly — it never imports cerebrum/pipeline internals, only talks to
neuron over HTTP.

Identity headers (``X-Medallion-User``/``X-Medallion-Session``) are additive:
``username`` is a persistent display name (cortex: localStorage), ``session_id``
is ephemeral per browser tab (cortex: sessionStorage). Neither is real auth —
orthogonal to the ``Authorization: Bearer`` API key set in ``__init__``.

``feedback()`` now targets a ``turn_id`` (returned by ``ask()``) rather than
re-sending question/sql/columns/row_count — the endpoint updates the matching
chat_history turn's ``accepted`` field in place instead of writing
harvested.jsonl/failures.jsonl directly (that now happens at session-end,
via ``end_session()``, not at click-time).
"""
from __future__ import annotations

from dataclasses import dataclass, field

import httpx


@dataclass
class QueryResult:
    answer:             str
    sql:                str
    rows:               list[dict]
    recommended_prompt: str
    row_count:          int
    columns:            list[str] = field(default_factory=list)
    turn_id:             str      = ""


class NeuronClient:
    """Production client that calls the neuron FastAPI server."""

    def __init__(
        self,
        base_url: str = "http://localhost:8000",
        api_key: str | None = None,
        timeout: float = 120.0,
    ) -> None:
        headers: dict[str, str] = {}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        self._client = httpx.Client(
            base_url=base_url, headers=headers, timeout=timeout
        )

    @staticmethod
    def _identity_headers(username: str | None, session_id: str | None = None) -> dict[str, str]:
        headers: dict[str, str] = {}
        if username:
            headers["X-Medallion-User"] = username
        if session_id:
            headers["X-Medallion-Session"] = session_id
        return headers

    @staticmethod
    def _raise_for_error(r: httpx.Response) -> None:
        if not r.is_success:
            try:
                detail = r.json().get("detail") or r.text
            except Exception:
                detail = r.text or f"HTTP {r.status_code}"
            raise RuntimeError(detail)

    def ask(
        self,
        question: str,
        project: str,
        username: str | None = None,
        session_id: str | None = None,
    ) -> QueryResult:
        r = self._client.post(
            "/query", json={"question": question, "project": project},
            headers=self._identity_headers(username, session_id),
        )
        self._raise_for_error(r)
        return QueryResult(**r.json())

    def feedback(
        self,
        turn_id: str,
        thumbs_up: bool,
        project: str,
        username: str | None = None,
    ) -> None:
        r = self._client.post(
            "/feedback",
            json={"project": project, "turn_id": turn_id, "thumbs_up": thumbs_up},
            headers=self._identity_headers(username),
        )
        self._raise_for_error(r)

    def history(self, project: str, username: str | None = None) -> list[dict]:
        """Return the named person's own past turns for a project (cortex's
        "History" tab). UI/audit only — never fed back into the LLM prompt."""
        r = self._client.get(
            "/history", params={"project": project},
            headers=self._identity_headers(username),
        )
        self._raise_for_error(r)
        return r.json().get("turns", [])

    def end_session(self, project: str, session_id: str, username: str | None = None) -> dict:
        """Curation-promotion trigger — NOT a data sync (chat_history is
        already written server-side in real time). project/session_id/
        username live in the body, not headers, because the tab-close
        beacon path (navigator.sendBeacon) can't set custom headers."""
        r = self._client.post(
            "/session/end",
            json={"project": project, "session_id": session_id, "username": username or ""},
        )
        self._raise_for_error(r)
        return r.json()

    def close(self) -> None:
        self._client.close()


class MockClient:
    """Deterministic offline stub — no network required."""

    def ask(
        self,
        question: str,
        project: str,
        username: str | None = None,
        session_id: str | None = None,
    ) -> QueryResult:
        rows = [
            {"region": "North", "revenue": 120_000, "orders": 430},
            {"region": "South", "revenue":  98_500, "orders": 310},
            {"region": "East",  "revenue": 145_200, "orders": 520},
            {"region": "West",  "revenue":  87_300, "orders": 270},
        ]
        sql = (
            "SELECT region, SUM(revenue) AS revenue, COUNT(*) AS orders "
            "FROM orders GROUP BY region ORDER BY revenue DESC"
        )
        return QueryResult(
            answer=f"Found {len(rows)} rows for: {question}",
            sql=sql,
            rows=rows,
            recommended_prompt=(
                "Show total revenue and order count by region, "
                "sorted by revenue descending"
            ),
            row_count=len(rows),
            columns=["region", "revenue", "orders"],
            turn_id="mock-turn-id",
        )

    def feedback(
        self,
        turn_id: str,
        thumbs_up: bool,
        project: str,
        username: str | None = None,
    ) -> None:
        pass  # offline stub — nothing to record

    def history(self, project: str, username: str | None = None) -> list[dict]:
        return [
            {
                "ts": "2026-08-01T12:00:00Z",
                "question": "What are the top 5 products by revenue?",
                "sql": "SELECT product, SUM(revenue) AS revenue FROM orders GROUP BY product ORDER BY revenue DESC LIMIT 5",
                "answer": "Found 5 row(s) for: What are the top 5 products by revenue?",
                "row_count": 5,
                "columns": ["product", "revenue"],
            },
            {
                "ts": "2026-08-01T11:45:00Z",
                "question": "Show monthly trends",
                "sql": "SELECT month, SUM(revenue) AS revenue FROM orders GROUP BY month ORDER BY month",
                "answer": "Found 12 row(s) for: Show monthly trends",
                "row_count": 12,
                "columns": ["month", "revenue"],
            },
        ]

    def end_session(self, project: str, session_id: str, username: str | None = None) -> dict:
        return {"harvested": 0, "failed": 0}

    def close(self) -> None:
        pass
