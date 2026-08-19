"""tests/test_neuron.py — Unit tests for openmedallion.neuron.

Uses FastAPI's TestClient; cerebrum pipeline is mocked so no Ollama or
silver Parquet files are required.
"""
from __future__ import annotations

import json
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
        qr.answer = None
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
        qr.answer = None

        with (
            patch("openmedallion.neuron.server._silver_dir", return_value=silver_dir),
            patch("openmedallion.neuron.server.CerebrumPipeline") as mock_cls,
        ):
            mock_cls.return_value.ask.return_value = qr
            r = client.post("/query", json={"question": "missing rows", "project": "demo"})

        body = r.json()
        assert body["row_count"] == 0
        assert "No results" in body["answer"]


class TestQueryEndpointUseTemplates:
    """Template-Routed Query Layer roadmap (see CLAUDE.md), build order step 6:
    QueryRequest.use_templates passes straight through to CerebrumPipeline's
    opt-in constructor flag."""

    def _mock_pipeline_result(self):
        qr = MagicMock()
        qr.sql    = "SELECT COUNT(*) AS n FROM orders"
        qr.result = pl.DataFrame({"n": [3]})
        qr.recommended_prompt = "Count all orders"
        qr.answer = None
        return qr

    def test_defaults_false_when_omitted(self, client, silver_dir):
        mock_qr = self._mock_pipeline_result()
        with (
            patch("openmedallion.neuron.server._silver_dir", return_value=silver_dir),
            patch("openmedallion.neuron.server.CerebrumPipeline") as mock_cls,
        ):
            mock_cls.return_value.ask.return_value = mock_qr
            client.post("/query", json={"question": "q", "project": "demo"})
        assert mock_cls.call_args.kwargs["use_templates"] is False

    def test_forwarded_true(self, client, silver_dir):
        mock_qr = self._mock_pipeline_result()
        with (
            patch("openmedallion.neuron.server._silver_dir", return_value=silver_dir),
            patch("openmedallion.neuron.server.CerebrumPipeline") as mock_cls,
        ):
            mock_cls.return_value.ask.return_value = mock_qr
            client.post("/query", json={"question": "q", "project": "demo", "use_templates": True})
        assert mock_cls.call_args.kwargs["use_templates"] is True


# ── /feedback endpoint (build order step 13) ─────────────────────────────────

class TestFeedbackEndpoint:
    """/feedback no longer writes harvested.jsonl/failures.jsonl directly —
    it updates the matching chat_history turn's `accepted` field in place
    (via chat_history.update_turn_accepted). harvested/failures.jsonl are now
    written later, at session-end curation (see /session/end), reusing the
    unchanged examples/feedback.py:record_feedback()."""

    def test_thumbs_up_sets_accepted_true_on_the_matching_turn(self, tmp_path, client, monkeypatch):
        monkeypatch.setattr("openmedallion.neuron.server.settings.PROJECTS_ROOT", str(tmp_path))
        from openmedallion.neuron.chat_history import record_chat_turn

        record_chat_turn(
            "demo", tmp_path, "alice", question="q", sql="SELECT 1", answer="a",
            row_count=1, columns=["x"], turn_id="t1",
        )
        r = client.post(
            "/feedback", json={"project": "demo", "turn_id": "t1", "thumbs_up": True},
            headers={"X-Medallion-User": "alice"},
        )
        assert r.status_code == 200

        path = tmp_path / "demo" / "chat_history" / "alice.jsonl"
        entry = json.loads(path.read_text().splitlines()[0])
        assert entry["accepted"] is True
        # harvested.jsonl is NOT written yet — that's session-end's job
        assert not (tmp_path / "demo" / "examples" / "harvested.jsonl").exists()

    def test_thumbs_down_sets_accepted_false(self, tmp_path, client, monkeypatch):
        monkeypatch.setattr("openmedallion.neuron.server.settings.PROJECTS_ROOT", str(tmp_path))
        from openmedallion.neuron.chat_history import record_chat_turn

        record_chat_turn(
            "demo", tmp_path, "alice", question="q", sql="SELECT 1", answer="a",
            row_count=1, columns=["x"], turn_id="t1",
        )
        client.post(
            "/feedback", json={"project": "demo", "turn_id": "t1", "thumbs_up": False},
            headers={"X-Medallion-User": "alice"},
        )
        entry = json.loads((tmp_path / "demo" / "chat_history" / "alice.jsonl").read_text().splitlines()[0])
        assert entry["accepted"] is False

    def test_missing_required_field_returns_422(self, client):
        r = client.post("/feedback", json={"project": "demo"})
        assert r.status_code == 422

    def test_unknown_turn_id_still_returns_200(self, tmp_path, client, monkeypatch):
        """update_turn_accepted() never raises for an unknown turn_id — the
        endpoint stays a no-op success rather than surfacing a 404/500 for a
        stale client-side turn_id (e.g. from an old page load)."""
        monkeypatch.setattr("openmedallion.neuron.server.settings.PROJECTS_ROOT", str(tmp_path))
        r = client.post(
            "/feedback", json={"project": "demo", "turn_id": "does-not-exist", "thumbs_up": True},
            headers={"X-Medallion-User": "alice"},
        )
        assert r.status_code == 200


# ── auth middleware ───────────────────────────────────────────────────────────

class TestAuthMiddleware:
    def test_no_key_env_allows_all(self, client, silver_dir, monkeypatch):
        monkeypatch.delenv("MEDALLION_API_KEY", raising=False)
        mock_qr = MagicMock()
        mock_qr.sql    = "SELECT 1 AS x"
        mock_qr.result = pl.DataFrame({"x": [1]})
        mock_qr.recommended_prompt = "r"
        mock_qr.answer = None

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
        mock_qr.answer = None

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


# ── per-person chat history ────────────────────────────────────────────────

class TestQueryEndpointRecordsChatHistory:
    """/query writes one chat_history/<username>.jsonl line per successful
    call, keyed by the X-Medallion-User header (default "local_user" when
    absent). Orthogonal to the bearer-token auth gate — this is a display-name
    tag, not a security mechanism."""

    def _mock_pipeline_result(self):
        qr = MagicMock()
        qr.sql    = "SELECT COUNT(*) AS n FROM orders"
        qr.result = pl.DataFrame({"n": [3]})
        qr.recommended_prompt = "Count all orders"
        qr.answer = None
        return qr

    def test_successful_query_writes_a_turn_for_the_named_user(self, tmp_path, client, silver_dir, monkeypatch):
        monkeypatch.setattr("openmedallion.neuron.server.settings.PROJECTS_ROOT", str(tmp_path))
        mock_qr = self._mock_pipeline_result()

        with (
            patch("openmedallion.neuron.server._silver_dir", return_value=silver_dir),
            patch("openmedallion.neuron.server.CerebrumPipeline") as mock_cls,
        ):
            mock_cls.return_value.ask.return_value = mock_qr
            r = client.post(
                "/query",
                json={"question": "How many orders?", "project": "demo"},
                headers={"X-Medallion-User": "alice"},
            )

        assert r.status_code == 200
        path = tmp_path / "demo" / "chat_history" / "alice.jsonl"
        assert path.exists()
        entry = json.loads(path.read_text().splitlines()[0])
        assert entry["question"] == "How many orders?"
        assert entry["row_count"] == 1

    def test_missing_header_defaults_to_local_user(self, tmp_path, client, silver_dir, monkeypatch):
        monkeypatch.setattr("openmedallion.neuron.server.settings.PROJECTS_ROOT", str(tmp_path))
        mock_qr = self._mock_pipeline_result()

        with (
            patch("openmedallion.neuron.server._silver_dir", return_value=silver_dir),
            patch("openmedallion.neuron.server.CerebrumPipeline") as mock_cls,
        ):
            mock_cls.return_value.ask.return_value = mock_qr
            client.post("/query", json={"question": "q", "project": "demo"})

        assert (tmp_path / "demo" / "chat_history" / "local_user.jsonl").exists()

    # NOTE: a failed query now DOES write a chat_history turn
    # (response_generated=False) — see TestQueryEndpointFailureLogging below,
    # which supersedes what used to be tested here as "writes no history".

class TestQueryEndpointFailureLogging:
    """/query now logs a turn even on failure (response_generated=False),
    and success responses carry turn_id + honor X-Medallion-Session."""

    def test_success_response_includes_turn_id(self, tmp_path, client, silver_dir, monkeypatch):
        monkeypatch.setattr("openmedallion.neuron.server.settings.PROJECTS_ROOT", str(tmp_path))
        mock_qr = MagicMock()
        mock_qr.sql = "SELECT 1 AS x"
        mock_qr.result = pl.DataFrame({"x": [1]})
        mock_qr.recommended_prompt = "r"
        mock_qr.answer = None

        with (
            patch("openmedallion.neuron.server._silver_dir", return_value=silver_dir),
            patch("openmedallion.neuron.server.CerebrumPipeline") as mock_cls,
        ):
            mock_cls.return_value.ask.return_value = mock_qr
            r = client.post("/query", json={"question": "q", "project": "demo"})

        assert r.status_code == 200
        assert r.json()["turn_id"]

    def test_success_turn_carries_session_id(self, tmp_path, client, silver_dir, monkeypatch):
        monkeypatch.setattr("openmedallion.neuron.server.settings.PROJECTS_ROOT", str(tmp_path))
        mock_qr = MagicMock()
        mock_qr.sql = "SELECT 1 AS x"
        mock_qr.result = pl.DataFrame({"x": [1]})
        mock_qr.recommended_prompt = "r"
        mock_qr.answer = None

        with (
            patch("openmedallion.neuron.server._silver_dir", return_value=silver_dir),
            patch("openmedallion.neuron.server.CerebrumPipeline") as mock_cls,
        ):
            mock_cls.return_value.ask.return_value = mock_qr
            client.post(
                "/query", json={"question": "q", "project": "demo"},
                headers={"X-Medallion-User": "alice", "X-Medallion-Session": "sess1"},
            )

        turn = json.loads((tmp_path / "demo" / "chat_history" / "alice.jsonl").read_text().splitlines()[0])
        assert turn["session_id"] == "sess1"
        assert turn["response_generated"] is True
        assert turn["accepted"] is None

    def test_validation_failure_writes_a_failed_turn(self, tmp_path, client, silver_dir, monkeypatch):
        monkeypatch.setattr("openmedallion.neuron.server.settings.PROJECTS_ROOT", str(tmp_path))
        with (
            patch("openmedallion.neuron.server._silver_dir", return_value=silver_dir),
            patch("openmedallion.neuron.server.CerebrumPipeline") as mock_cls,
        ):
            mock_cls.return_value.ask.side_effect = ValueError("no valid SQL")
            r = client.post(
                "/query", json={"question": "q", "project": "demo"},
                headers={"X-Medallion-User": "alice"},
            )

        assert r.status_code == 422
        path = tmp_path / "demo" / "chat_history" / "alice.jsonl"
        assert path.exists()
        turn = json.loads(path.read_text().splitlines()[0])
        assert turn["response_generated"] is False
        assert turn["question"] == "q"

    def test_unexpected_error_writes_a_failed_turn(self, tmp_path, client, silver_dir, monkeypatch):
        monkeypatch.setattr("openmedallion.neuron.server.settings.PROJECTS_ROOT", str(tmp_path))
        with (
            patch("openmedallion.neuron.server._silver_dir", return_value=silver_dir),
            patch("openmedallion.neuron.server.CerebrumPipeline") as mock_cls,
        ):
            mock_cls.return_value.ask.side_effect = RuntimeError("boom")
            client.post(
                "/query", json={"question": "q", "project": "demo"},
                headers={"X-Medallion-User": "alice"},
            )

        turn = json.loads((tmp_path / "demo" / "chat_history" / "alice.jsonl").read_text().splitlines()[0])
        assert turn["response_generated"] is False

    def test_missing_silver_does_not_write_a_turn(self, tmp_path, client, monkeypatch):
        monkeypatch.setattr("openmedallion.neuron.server.settings.PROJECTS_ROOT", str(tmp_path))
        missing = tmp_path / "nonexistent_silver"
        with patch("openmedallion.neuron.server._silver_dir", return_value=missing):
            client.post(
                "/query", json={"question": "q", "project": "demo"},
                headers={"X-Medallion-User": "alice"},
            )
        assert not (tmp_path / "demo" / "chat_history" / "alice.jsonl").exists()


class TestHistoryEndpoint:

    def test_returns_turns_for_the_named_user_only(self, tmp_path, client, monkeypatch):
        monkeypatch.setattr("openmedallion.neuron.server.settings.PROJECTS_ROOT", str(tmp_path))
        from openmedallion.neuron.chat_history import record_chat_turn

        record_chat_turn("demo", tmp_path, "alice", question="qa", sql="s", answer="a", row_count=1, columns=["x"])
        record_chat_turn("demo", tmp_path, "bob",   question="qb", sql="s", answer="a", row_count=1, columns=["x"])

        r = client.get("/history", params={"project": "demo"}, headers={"X-Medallion-User": "alice"})
        assert r.status_code == 200
        turns = r.json()["turns"]
        assert len(turns) == 1
        assert turns[0]["question"] == "qa"

    def test_no_history_returns_empty_list(self, tmp_path, client, monkeypatch):
        monkeypatch.setattr("openmedallion.neuron.server.settings.PROJECTS_ROOT", str(tmp_path))
        r = client.get("/history", params={"project": "demo"}, headers={"X-Medallion-User": "alice"})
        assert r.status_code == 200
        assert r.json()["turns"] == []

    def test_missing_header_defaults_to_local_user(self, tmp_path, client, monkeypatch):
        monkeypatch.setattr("openmedallion.neuron.server.settings.PROJECTS_ROOT", str(tmp_path))
        from openmedallion.neuron.chat_history import record_chat_turn

        record_chat_turn("demo", tmp_path, "local_user", question="q", sql="s", answer="a", row_count=0, columns=[])
        r = client.get("/history", params={"project": "demo"})
        assert r.json()["turns"][0]["question"] == "q"


class TestSessionEndEndpoint:
    """POST /session/end is a curation-promotion trigger, not a data-sync
    endpoint — chat_history is already written server-side in real time on
    every /query. project/session_id/username live in the request BODY, not
    headers, because the tab-close beacon path (navigator.sendBeacon) can't
    set custom headers."""

    def test_promotes_accepted_turns_and_returns_counts(self, tmp_path, client, monkeypatch):
        monkeypatch.setattr("openmedallion.neuron.server.settings.PROJECTS_ROOT", str(tmp_path))
        from openmedallion.neuron.chat_history import record_chat_turn, update_turn_accepted

        record_chat_turn(
            "demo", tmp_path, "alice", question="q", sql="SELECT 1", answer="a",
            row_count=1, columns=["x"], session_id="sess1", turn_id="t1",
        )
        update_turn_accepted("demo", tmp_path, "alice", "t1", accepted=True)

        r = client.post("/session/end", json={"project": "demo", "session_id": "sess1", "username": "alice"})

        assert r.status_code == 200
        assert r.json() == {"harvested": 1, "failed": 0}
        assert (tmp_path / "demo" / "examples" / "harvested.jsonl").exists()

    def test_no_username_falls_back_to_local_user(self, tmp_path, client, monkeypatch):
        monkeypatch.setattr("openmedallion.neuron.server.settings.PROJECTS_ROOT", str(tmp_path))
        from openmedallion.neuron.chat_history import record_chat_turn, update_turn_accepted

        record_chat_turn(
            "demo", tmp_path, "local_user", question="q", sql="s", answer="a",
            row_count=0, columns=[], session_id="sess1", turn_id="t1",
        )
        update_turn_accepted("demo", tmp_path, "local_user", "t1", accepted=True)

        r = client.post("/session/end", json={"project": "demo", "session_id": "sess1"})
        assert r.json() == {"harvested": 1, "failed": 0}

    def test_unrated_session_returns_zero_counts(self, tmp_path, client, monkeypatch):
        monkeypatch.setattr("openmedallion.neuron.server.settings.PROJECTS_ROOT", str(tmp_path))
        r = client.post("/session/end", json={"project": "demo", "session_id": "empty", "username": "alice"})
        assert r.json() == {"harvested": 0, "failed": 0}
