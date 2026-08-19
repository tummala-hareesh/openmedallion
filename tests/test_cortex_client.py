"""tests/test_cortex_client.py — cortex/client.py (NeuronClient + MockClient).

First test file for cortex/client.py — no cortex tests existed before this
(a pre-existing gap noted in CLAUDE.md; cortex UI/Dash callbacks are still
verified via real browser sessions, not pytest, but client.py is plain
Python — an HTTP client class — so it's directly testable).

Covers per-person chat history: `username=`/`session_id=` on `ask()`, the
`turn_id`-based `feedback()` (matches the /feedback endpoint's schema —
thumbs up/down updates chat_history in place rather than writing
harvested.jsonl directly), `history()`, and `end_session()` (the
curation-promotion trigger, not a data-sync call).
"""
from __future__ import annotations

import httpx

from openmedallion.cortex.client import MockClient, NeuronClient, QueryResult


class _StubTransport(httpx.BaseTransport):
    """Records every request and returns a canned JSON response."""

    def __init__(self, response_json: dict, status_code: int = 200):
        self.requests: list[httpx.Request] = []
        self._response_json = response_json
        self._status_code = status_code

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return httpx.Response(self._status_code, json=self._response_json)


def _client_with_stub(response_json: dict, status_code: int = 200) -> tuple[NeuronClient, _StubTransport]:
    client = NeuronClient(base_url="http://testserver")
    transport = _StubTransport(response_json, status_code)
    client._client._transport = transport
    return client, transport


class TestNeuronClientIdentityHeaders:

    def test_ask_sends_username_header(self):
        client, transport = _client_with_stub({
            "answer": "a", "sql": "s", "rows": [], "recommended_prompt": "r",
            "row_count": 0, "columns": [], "turn_id": "t1",
        })
        client.ask("How many orders?", "demo", username="alice")
        assert transport.requests[0].headers["x-medallion-user"] == "alice"

    def test_ask_sends_session_id_header(self):
        client, transport = _client_with_stub({
            "answer": "a", "sql": "s", "rows": [], "recommended_prompt": "r",
            "row_count": 0, "columns": [], "turn_id": "t1",
        })
        client.ask("q", "demo", username="alice", session_id="sess1")
        assert transport.requests[0].headers["x-medallion-session"] == "sess1"

    def test_ask_without_username_omits_header(self):
        client, transport = _client_with_stub({
            "answer": "a", "sql": "s", "rows": [], "recommended_prompt": "r",
            "row_count": 0, "columns": [], "turn_id": "t1",
        })
        client.ask("q", "demo")
        assert "x-medallion-user" not in transport.requests[0].headers
        assert "x-medallion-session" not in transport.requests[0].headers

    def test_ask_returns_turn_id(self):
        client, _ = _client_with_stub({
            "answer": "a", "sql": "s", "rows": [], "recommended_prompt": "r",
            "row_count": 0, "columns": [], "turn_id": "abc123",
        })
        result = client.ask("q", "demo")
        assert result.turn_id == "abc123"

    def test_feedback_sends_turn_id_and_thumbs_up(self):
        client, transport = _client_with_stub({"status": "ok"})
        client.feedback("t1", True, "demo", username="bob")
        req = transport.requests[0]
        assert req.headers["x-medallion-user"] == "bob"
        import json as _json
        body = _json.loads(req.content)
        assert body == {"project": "demo", "turn_id": "t1", "thumbs_up": True}

    def test_history_sends_username_header_and_parses_turns(self):
        client, transport = _client_with_stub({"turns": [{"question": "q1", "sql": "s", "answer": "a", "row_count": 0, "columns": [], "ts": "x"}]})
        turns = client.history("demo", username="alice")
        assert transport.requests[0].headers["x-medallion-user"] == "alice"
        assert turns[0]["question"] == "q1"

    def test_history_returns_empty_list_when_no_turns(self):
        client, _ = _client_with_stub({"turns": []})
        assert client.history("demo", username="alice") == []

    def test_end_session_sends_body_not_headers(self):
        """navigator.sendBeacon (cortex's tab-close path) can't set custom
        headers, so /session/end carries project/session_id/username in the
        POST body — verify the client matches that contract."""
        client, transport = _client_with_stub({"harvested": 1, "failed": 0})
        result = client.end_session("demo", "sess1", username="alice")
        import json as _json
        body = _json.loads(transport.requests[0].content)
        assert body == {"project": "demo", "session_id": "sess1", "username": "alice"}
        assert result == {"harvested": 1, "failed": 0}


class TestMockClientHistory:

    def test_history_returns_stub_data_without_network(self):
        client = MockClient()
        turns = client.history("demo", username="alice")
        assert isinstance(turns, list)
        assert len(turns) > 0
        assert "question" in turns[0]

    def test_ask_and_feedback_accept_new_kwargs_without_error(self):
        client = MockClient()
        result = client.ask("q", "demo", username="alice", session_id="sess1")
        assert isinstance(result, QueryResult)
        client.feedback("t1", True, "demo", username="alice")  # no exception
        assert client.end_session("demo", "sess1", username="alice") == {"harvested": 0, "failed": 0}
