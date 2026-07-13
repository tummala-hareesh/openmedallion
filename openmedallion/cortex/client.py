"""cortex/client.py — HTTP client to the neuron /query and /feedback endpoints.

Two implementations share the same interface:

``NeuronClient``
    Production client — calls neuron over HTTP using httpx.

``MockClient``
    Deterministic offline stub — no network or Ollama required.
    Used for UI development and CI (``USE_MOCK_CLIENT=1``).

cortex never writes harvested.jsonl/failures.jsonl directly (RAG roadmap
Phase 3, build order step 13) — it never imports cerebrum/pipeline internals,
only talks to neuron over HTTP, so ``feedback()`` here is a thin POST just
like ``ask()``, matching this module's whole reason for existing.
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

    def ask(self, question: str, project: str) -> QueryResult:
        r = self._client.post("/query", json={"question": question, "project": project})
        if not r.is_success:
            try:
                detail = r.json().get("detail") or r.text
            except Exception:
                detail = r.text or f"HTTP {r.status_code}"
            raise RuntimeError(detail)
        return QueryResult(**r.json())

    def feedback(
        self,
        question: str,
        sql: str,
        columns: list[str],
        row_count: int,
        thumbs_up: bool,
        project: str,
    ) -> None:
        r = self._client.post("/feedback", json={
            "project": project, "question": question, "sql": sql,
            "columns": columns, "row_count": row_count, "thumbs_up": thumbs_up,
        })
        if not r.is_success:
            try:
                detail = r.json().get("detail") or r.text
            except Exception:
                detail = r.text or f"HTTP {r.status_code}"
            raise RuntimeError(detail)

    def close(self) -> None:
        self._client.close()


class MockClient:
    """Deterministic offline stub — no network required."""

    def ask(self, question: str, project: str) -> QueryResult:
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
        )

    def feedback(
        self,
        question: str,
        sql: str,
        columns: list[str],
        row_count: int,
        thumbs_up: bool,
        project: str,
    ) -> None:
        pass  # offline stub — nothing to record

    def close(self) -> None:
        pass
