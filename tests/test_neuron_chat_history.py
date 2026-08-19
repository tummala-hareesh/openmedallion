"""tests/test_neuron_chat_history.py — per-person chat history.

Locked design decisions (confirmed via AskUserQuestion, not assumed):
- Identity: a plain display-name/username string (no real auth) — sent by
  cortex as an `X-Medallion-User` header, or supplied directly by a local
  CLI caller (`medallion query --user`). No login system, no session infra.
- Storage: <project>/chat_history/<username>.jsonl, one line per turn —
  same append-only pattern as examples/harvested.jsonl (examples/feedback.py).
- Usage: UI/audit only — never fed back into the LLM prompt.

Pure file I/O, no HTTP/FastAPI/LLM involved — see tests/test_neuron.py for
the /query and /history endpoint wiring.
"""
from __future__ import annotations

import json
from pathlib import Path

from openmedallion.neuron.chat_history import (
    _safe_username,
    list_chat_history,
    promote_session,
    record_chat_turn,
    update_turn_accepted,
)


class TestSafeUsername:

    def test_plain_name_passes_through(self):
        assert _safe_username("alice") == "alice"

    def test_spaces_become_underscores(self):
        assert _safe_username("Alice Chen") == "Alice_Chen"

    def test_path_traversal_characters_stripped(self):
        result = _safe_username("../../etc/passwd")
        assert "/" not in result
        assert ".." not in result

    def test_empty_or_whitespace_falls_back_to_local_user(self):
        assert _safe_username("") == "local_user"
        assert _safe_username("   ") == "local_user"

    def test_none_falls_back_to_local_user(self):
        assert _safe_username(None) == "local_user"

    def test_long_name_is_truncated(self):
        result = _safe_username("a" * 200)
        assert len(result) <= 64


class TestRecordChatTurn:

    def test_writes_one_jsonl_line(self, tmp_path: Path):
        record_chat_turn(
            "proj", tmp_path, "alice",
            question="How many orders?", sql="SELECT COUNT(*) FROM orders",
            answer="Found 3 rows", row_count=3, columns=["n"],
        )
        path = tmp_path / "proj" / "chat_history" / "alice.jsonl"
        assert path.exists()
        lines = path.read_text().splitlines()
        assert len(lines) == 1
        entry = json.loads(lines[0])
        assert entry["question"] == "How many orders?"
        assert entry["sql"] == "SELECT COUNT(*) FROM orders"
        assert entry["row_count"] == 3
        assert entry["columns"] == ["n"]
        assert "ts" in entry

    def test_appends_across_multiple_calls(self, tmp_path: Path):
        for i in range(3):
            record_chat_turn(
                "proj", tmp_path, "alice",
                question=f"q{i}", sql="SELECT 1", answer="a", row_count=1, columns=[],
            )
        path = tmp_path / "proj" / "chat_history" / "alice.jsonl"
        assert len(path.read_text().splitlines()) == 3

    def test_different_users_get_separate_files(self, tmp_path: Path):
        record_chat_turn("proj", tmp_path, "alice", question="qa", sql="s", answer="a", row_count=0, columns=[])
        record_chat_turn("proj", tmp_path, "bob",   question="qb", sql="s", answer="a", row_count=0, columns=[])

        alice = json.loads((tmp_path / "proj" / "chat_history" / "alice.jsonl").read_text().splitlines()[0])
        bob   = json.loads((tmp_path / "proj" / "chat_history" / "bob.jsonl").read_text().splitlines()[0])
        assert alice["question"] == "qa"
        assert bob["question"]   == "qb"

    def test_username_is_sanitized_for_the_filename(self, tmp_path: Path):
        record_chat_turn("proj", tmp_path, "../../etc/passwd", question="q", sql="s", answer="a", row_count=0, columns=[])
        chat_dir = tmp_path / "proj" / "chat_history"
        files = list(chat_dir.iterdir())
        assert len(files) == 1
        assert ".." not in files[0].name
        assert files[0].parent == chat_dir  # never escaped chat_history/


class TestListChatHistory:

    def test_no_history_file_returns_empty_list(self, tmp_path: Path):
        assert list_chat_history("proj", tmp_path, "alice") == []

    def test_returns_newest_first(self, tmp_path: Path):
        for i in range(3):
            record_chat_turn(
                "proj", tmp_path, "alice",
                question=f"q{i}", sql="s", answer="a", row_count=0, columns=[],
            )
        turns = list_chat_history("proj", tmp_path, "alice")
        assert [t["question"] for t in turns] == ["q2", "q1", "q0"]

    def test_respects_limit(self, tmp_path: Path):
        for i in range(5):
            record_chat_turn(
                "proj", tmp_path, "alice",
                question=f"q{i}", sql="s", answer="a", row_count=0, columns=[],
            )
        turns = list_chat_history("proj", tmp_path, "alice", limit=2)
        assert len(turns) == 2
        assert [t["question"] for t in turns] == ["q4", "q3"]

    def test_isolates_users_from_each_other(self, tmp_path: Path):
        record_chat_turn("proj", tmp_path, "alice", question="qa", sql="s", answer="a", row_count=0, columns=[])
        record_chat_turn("proj", tmp_path, "bob",   question="qb", sql="s", answer="a", row_count=0, columns=[])

        assert [t["question"] for t in list_chat_history("proj", tmp_path, "alice")] == ["qa"]
        assert [t["question"] for t in list_chat_history("proj", tmp_path, "bob")]   == ["qb"]

    def test_malformed_line_is_skipped_not_raised(self, tmp_path: Path):
        chat_dir = tmp_path / "proj" / "chat_history"
        chat_dir.mkdir(parents=True)
        (chat_dir / "alice.jsonl").write_text("not json\n" + json.dumps({
            "ts": "x", "question": "ok", "sql": "s", "answer": "a", "row_count": 0, "columns": [],
        }) + "\n")
        turns = list_chat_history("proj", tmp_path, "alice")
        assert len(turns) == 1
        assert turns[0]["question"] == "ok"


# ── unified schema: session_id, turn_id, response_generated, accepted ──────

class TestUnifiedSchemaFields:
    """Locked (confirmed via AskUserQuestion): three-state `accepted`
    (true/false/None), None excluded from learning. `response_generated`
    tracks failed turns too. `turn_id` lets a later thumbs click target this
    exact line for an in-place update. `promoted` guards against
    double-promotion into harvested/failures.jsonl."""

    def test_new_fields_default_sensibly(self, tmp_path: Path):
        record_chat_turn(
            "proj", tmp_path, "alice",
            question="q", sql="s", answer="a", row_count=0, columns=[],
        )
        turn = list_chat_history("proj", tmp_path, "alice")[0]
        assert turn["session_id"] == ""
        assert turn["response_generated"] is True
        assert turn["accepted"] is None
        assert turn["promoted"] is False
        assert "turn_id" in turn and turn["turn_id"]

    def test_turn_id_is_unique_per_call(self, tmp_path: Path):
        record_chat_turn("proj", tmp_path, "alice", question="q1", sql="s", answer="a", row_count=0, columns=[])
        record_chat_turn("proj", tmp_path, "alice", question="q2", sql="s", answer="a", row_count=0, columns=[])
        turns = list_chat_history("proj", tmp_path, "alice")
        assert turns[0]["turn_id"] != turns[1]["turn_id"]

    def test_explicit_turn_id_is_respected(self, tmp_path: Path):
        record_chat_turn(
            "proj", tmp_path, "alice",
            question="q", sql="s", answer="a", row_count=0, columns=[],
            turn_id="fixed-id-123",
        )
        assert list_chat_history("proj", tmp_path, "alice")[0]["turn_id"] == "fixed-id-123"

    def test_response_generated_false_for_failed_turns(self, tmp_path: Path):
        record_chat_turn(
            "proj", tmp_path, "alice",
            question="q", sql="", answer="LLM never returned valid SQL",
            row_count=0, columns=[], response_generated=False,
        )
        assert list_chat_history("proj", tmp_path, "alice")[0]["response_generated"] is False

    def test_session_id_is_stored(self, tmp_path: Path):
        record_chat_turn(
            "proj", tmp_path, "alice",
            question="q", sql="s", answer="a", row_count=0, columns=[],
            session_id="sess-abc",
        )
        assert list_chat_history("proj", tmp_path, "alice")[0]["session_id"] == "sess-abc"


class TestUpdateTurnAccepted:

    def test_updates_matching_turn_in_place(self, tmp_path: Path):
        record_chat_turn("proj", tmp_path, "alice", question="q1", sql="s", answer="a", row_count=0, columns=[], turn_id="t1")
        record_chat_turn("proj", tmp_path, "alice", question="q2", sql="s", answer="a", row_count=0, columns=[], turn_id="t2")

        result = update_turn_accepted("proj", tmp_path, "alice", "t1", accepted=True)

        assert result is True
        turns = {t["turn_id"]: t for t in list_chat_history("proj", tmp_path, "alice")}
        assert turns["t1"]["accepted"] is True
        assert turns["t2"]["accepted"] is None  # untouched

    def test_can_flip_from_true_to_false(self, tmp_path: Path):
        record_chat_turn("proj", tmp_path, "alice", question="q", sql="s", answer="a", row_count=0, columns=[], turn_id="t1")
        update_turn_accepted("proj", tmp_path, "alice", "t1", accepted=True)
        update_turn_accepted("proj", tmp_path, "alice", "t1", accepted=False)
        turn = list_chat_history("proj", tmp_path, "alice")[0]
        assert turn["accepted"] is False

    def test_unknown_turn_id_returns_false_and_raises_nothing(self, tmp_path: Path):
        record_chat_turn("proj", tmp_path, "alice", question="q", sql="s", answer="a", row_count=0, columns=[], turn_id="t1")
        result = update_turn_accepted("proj", tmp_path, "alice", "does-not-exist", accepted=True)
        assert result is False

    def test_no_history_file_returns_false(self, tmp_path: Path):
        assert update_turn_accepted("proj", tmp_path, "alice", "t1", accepted=True) is False

    def test_preserves_other_fields(self, tmp_path: Path):
        record_chat_turn("proj", tmp_path, "alice", question="q", sql="SELECT 1", answer="ans", row_count=3, columns=["x"], turn_id="t1")
        update_turn_accepted("proj", tmp_path, "alice", "t1", accepted=True)
        turn = list_chat_history("proj", tmp_path, "alice")[0]
        assert turn["question"] == "q"
        assert turn["sql"] == "SELECT 1"
        assert turn["row_count"] == 3


class TestPromoteSession:

    def test_promotes_accepted_true_turns_to_harvested(self, tmp_path: Path):
        record_chat_turn(
            "proj", tmp_path, "alice", question="q", sql="SELECT 1", answer="a",
            row_count=1, columns=["x"], session_id="sess1", turn_id="t1",
        )
        update_turn_accepted("proj", tmp_path, "alice", "t1", accepted=True)

        result = promote_session("proj", tmp_path, "alice", "sess1")

        assert result == {"harvested": 1, "failed": 0}
        harvested_path = tmp_path / "proj" / "examples" / "harvested.jsonl"
        assert harvested_path.exists()
        entry = json.loads(harvested_path.read_text().splitlines()[0])
        assert entry["question"] == "q"

    def test_promotes_accepted_false_turns_to_failures(self, tmp_path: Path):
        record_chat_turn(
            "proj", tmp_path, "alice", question="q", sql="SELECT 1", answer="a",
            row_count=1, columns=["x"], session_id="sess1", turn_id="t1",
        )
        update_turn_accepted("proj", tmp_path, "alice", "t1", accepted=False)

        result = promote_session("proj", tmp_path, "alice", "sess1")

        assert result == {"harvested": 0, "failed": 1}
        assert (tmp_path / "proj" / "examples" / "failures.jsonl").exists()

    def test_unrated_turns_are_never_promoted(self, tmp_path: Path):
        record_chat_turn(
            "proj", tmp_path, "alice", question="q", sql="SELECT 1", answer="a",
            row_count=1, columns=["x"], session_id="sess1", turn_id="t1",
        )
        # accepted stays None — no update_turn_accepted call

        result = promote_session("proj", tmp_path, "alice", "sess1")

        assert result == {"harvested": 0, "failed": 0}
        assert not (tmp_path / "proj" / "examples" / "harvested.jsonl").exists()

    def test_already_promoted_turns_are_not_promoted_twice(self, tmp_path: Path):
        record_chat_turn(
            "proj", tmp_path, "alice", question="q", sql="SELECT 1", answer="a",
            row_count=1, columns=["x"], session_id="sess1", turn_id="t1",
        )
        update_turn_accepted("proj", tmp_path, "alice", "t1", accepted=True)
        promote_session("proj", tmp_path, "alice", "sess1")
        second = promote_session("proj", tmp_path, "alice", "sess1")

        assert second == {"harvested": 0, "failed": 0}
        harvested_path = tmp_path / "proj" / "examples" / "harvested.jsonl"
        assert len(harvested_path.read_text().splitlines()) == 1  # not duplicated

    def test_promoted_flag_is_set_in_chat_history(self, tmp_path: Path):
        record_chat_turn(
            "proj", tmp_path, "alice", question="q", sql="SELECT 1", answer="a",
            row_count=1, columns=["x"], session_id="sess1", turn_id="t1",
        )
        update_turn_accepted("proj", tmp_path, "alice", "t1", accepted=True)
        promote_session("proj", tmp_path, "alice", "sess1")

        turn = list_chat_history("proj", tmp_path, "alice")[0]
        assert turn["promoted"] is True

    def test_only_matching_session_id_is_promoted(self, tmp_path: Path):
        record_chat_turn(
            "proj", tmp_path, "alice", question="q1", sql="s", answer="a",
            row_count=0, columns=[], session_id="sess1", turn_id="t1",
        )
        record_chat_turn(
            "proj", tmp_path, "alice", question="q2", sql="s", answer="a",
            row_count=0, columns=[], session_id="sess2", turn_id="t2",
        )
        update_turn_accepted("proj", tmp_path, "alice", "t1", accepted=True)
        update_turn_accepted("proj", tmp_path, "alice", "t2", accepted=True)

        result = promote_session("proj", tmp_path, "alice", "sess1")

        assert result == {"harvested": 1, "failed": 0}
