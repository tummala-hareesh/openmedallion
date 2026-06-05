"""cortex/client.py — HTTP client to the neuron /query endpoint.

Two implementations share the same interface:

``NeuronClient``
    Production client — calls neuron over HTTP using httpx.

``MockClient``
    Deterministic offline stub — no network or Ollama required.
    Used for UI development and CI (``USE_MOCK_CLIENT=1``).
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

    def close(self) -> None:
        pass
