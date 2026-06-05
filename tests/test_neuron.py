"""tests/test_neuron.py — Unit tests for openmedallion.neuron.

Uses FastAPI's TestClient; cerebrum pipeline is mocked so no Ollama or
silver Parquet files are required.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from fastapi.testclient import TestClient

from openmedallion.neuron.models  import QueryRequest, QueryResponse
from openmedallion.neuron.server  import app


@pytest.fixture()
def client() -> TestClient:
    return TestClient(app)


@pytest.fixture()
def silver_dir(tmp_path: Path) -> Path:
    """Minimal silver dir so server-side path checks pass."""
    pl.DataFrame({"order_id": [1], "amount": [100.0]}).write_parquet(
        tmp_path / "orders.parquet"
    )
    return tmp_path


# ── /health ───────────────────────────────────────────────────────────────────

class TestHealth:
    def test_returns_ok(self, client):
        r = client.get("/health")
        assert r.status_code == 200
        assert r.json() == {"status": "ok"}


# ── models ───────────────────────────────────────────────────────────────────

class TestModels:
    def test_query_request_valid(self):
        req = QueryRequest(question="How many orders?", project="demo")
        assert req.question == "How many orders?"
        assert req.project  == "demo"

    def test_query_request_missing_field(self):
        with pytest.raises(Exception):
            QueryRequest(question="q")  # project missing

    def test_query_response_fields(self):
        resp = QueryResponse(
            answer="3 rows",
            sql="SELECT 1",
            rows=[{"a": 1}],
            recommended_prompt="Count rows",
            row_count=1,
            columns=["a"],
        )
        assert resp.row_count == 1
        assert resp.columns   == ["a"]


# ── /query endpoint ───────────────────────────────────────────────────────────

class TestQueryEndpoint:
    def _mock_pipeline_result(self):
        qr = MagicMock()
        qr.sql    = "SELECT COUNT(*) AS n FROM orders"
        qr.result = pl.DataFrame({"n": [3]})
        qr.recommended_prompt = "Count all orders"
        return qr

    def test_returns_200_with_valid_payload(self, client, silver_dir, monkeypatch):
        monkeypatch.setenv("MEDALLION_PROJECTS_ROOT", str(silver_dir.parent))

        {
            "paths": {"silver": str(silver_dir)},
            "pipeline": {"name": "demo"},
        }
        mock_qr = self._mock_pipeline_result()

        with (
            patch("openmedallion.neuron.server._silver_dir", return_value=silver_dir),
            patch("openmedallion.neuron.server.CerebrumPipeline") as mock_cls,
        ):
            mock_cls.return_value.ask.return_value = mock_qr
            r = client.post("/query", json={"question": "How many orders?", "project": "demo"})

        assert r.status_code == 200
        body = r.json()
        assert "sql"                in body
        assert "rows"               in body
        assert "recommended_prompt" in body
        assert body["row_count"]    == 1
        assert body["columns"]      == ["n"]

    def test_404_when_silver_missing(self, client, tmp_path):
        missing = tmp_path / "nonexistent_silver"
        with patch("openmedallion.neuron.server._silver_dir", return_value=missing):
            r = client.post("/query", json={"question": "q", "project": "x"})
        assert r.status_code == 404

    def test_422_on_validation_failure(self, client, silver_dir):
        with (
            patch("openmedallion.neuron.server._silver_dir", return_value=silver_dir),
            patch("openmedallion.neuron.server.CerebrumPipeline") as mock_cls,
        ):
            mock_cls.return_value.ask.side_effect = ValueError("no valid SQL")
            r = client.post("/query", json={"question": "q", "project": "demo"})
        assert r.status_code == 422

    def test_500_on_unexpected_error(self, client, silver_dir):
        with (
            patch("openmedallion.neuron.server._silver_dir", return_value=silver_dir),
            patch("openmedallion.neuron.server.CerebrumPipeline") as mock_cls,
        ):
            mock_cls.return_value.ask.side_effect = RuntimeError("boom")
            r = client.post("/query", json={"question": "q", "project": "demo"})
        assert r.status_code == 500

    def test_answer_contains_question(self, client, silver_dir):
        mock_qr = self._mock_pipeline_result()
        with (
            patch("openmedallion.neuron.server._silver_dir", return_value=silver_dir),
            patch("openmedallion.neuron.server.CerebrumPipeline") as mock_cls,
        ):
            mock_cls.return_value.ask.return_value = mock_qr
            r = client.post("/query", json={"question": "How many orders?", "project": "demo"})

        assert "How many orders?" in r.json()["answer"]

    def test_empty_result_answer(self, client, silver_dir):
        qr = MagicMock()
        qr.sql              = "SELECT * FROM orders WHERE 1=0"
        qr.result           = pl.DataFrame({"n": []})
        qr.recommended_prompt = "empty"

        with (
            patch("openmedallion.neuron.server._silver_dir", return_value=silver_dir),
            patch("openmedallion.neuron.server.CerebrumPipeline") as mock_cls,
        ):
            mock_cls.return_value.ask.return_value = qr
            r = client.post("/query", json={"question": "missing rows", "project": "demo"})

        body = r.json()
        assert body["row_count"] == 0
        assert "No results" in body["answer"]


# ── auth middleware ───────────────────────────────────────────────────────────

class TestAuthMiddleware:
    def test_no_key_env_allows_all(self, client, silver_dir, monkeypatch):
        monkeypatch.delenv("MEDALLION_API_KEY", raising=False)
        mock_qr = MagicMock()
        mock_qr.sql    = "SELECT 1 AS x"
        mock_qr.result = pl.DataFrame({"x": [1]})
        mock_qr.recommended_prompt = "r"

        with (
            patch("openmedallion.neuron.server._silver_dir", return_value=silver_dir),
            patch("openmedallion.neuron.server.CerebrumPipeline") as mock_cls,
        ):
            mock_cls.return_value.ask.return_value = mock_qr
            r = client.post("/query", json={"question": "q", "project": "demo"})

        assert r.status_code == 200

    def test_correct_api_key_allowed(self, client, silver_dir, monkeypatch):
        monkeypatch.setenv("MEDALLION_API_KEY", "secret123")
        mock_qr = MagicMock()
        mock_qr.sql    = "SELECT 1 AS x"
        mock_qr.result = pl.DataFrame({"x": [1]})
        mock_qr.recommended_prompt = "r"

        with (
            patch("openmedallion.neuron.server._silver_dir", return_value=silver_dir),
            patch("openmedallion.neuron.server.CerebrumPipeline") as mock_cls,
        ):
            mock_cls.return_value.ask.return_value = mock_qr
            r = client.post(
                "/query",
                json={"question": "q", "project": "demo"},
                headers={"Authorization": "Bearer secret123"},
            )

        assert r.status_code == 200

    def test_wrong_api_key_rejected(self, client, monkeypatch):
        monkeypatch.setenv("MEDALLION_API_KEY", "secret123")
        r = client.post(
            "/query",
            json={"question": "q", "project": "demo"},
            headers={"Authorization": "Bearer wrong"},
        )
        assert r.status_code == 401
