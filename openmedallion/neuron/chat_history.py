"""neuron/chat_history.py — per-person chat history + curation promotion.

Locked design decisions (confirmed via AskUserQuestion, not assumed):
- Identity: a plain display-name/username string, no real auth. cortex sends
  it as an ``X-Medallion-User`` header (persisted client-side in browser
  localStorage); a local CLI caller (``medallion query --user``) supplies it
  directly. Orthogonal to the existing optional ``MEDALLION_API_KEY`` bearer
  gate — this is a label, not a security mechanism. ``session_id`` is a
  second, ephemeral identifier (cortex: ``dcc.Store(storage_type="session")``,
  fresh per browser tab; CLI: one per invocation) sent as
  ``X-Medallion-Session`` — additive to ``username``, never a replacement.
- Storage: ``<project>/chat_history/<username>.jsonl``, one line per turn.
  Unlike the original append-only design, entries can now be updated in
  place (``update_turn_accepted``) — this uses the same read-all/rewrite-all
  pattern already established in ``metadata/approve.py``/
  ``relationships/approve.py``, not a database.
- ``accepted`` is three-state: ``True``/``False`` (explicit thumbs-up/down,
  via ``update_turn_accepted``) or ``None`` (no feedback given — excluded
  from learning, NEVER inferred from interaction. A human judgment beats a
  guessed one, and this project has no reliable satisfaction proxy today).
- Usage stays UI/audit only for the raw turns — never fed back into the LLM
  prompt directly. The one exception is curation: ``promote_session()``
  rolls a closed session's ``accepted`` turns into the *existing*, unchanged
  ``examples/harvested.jsonl``/``failures.jsonl`` (via
  ``examples/feedback.py:record_feedback()``) — session-end is a curation
  trigger, not a data-sync trigger, since chat_history is already written
  server-side in real time on every ``/query``.
"""
from __future__ import annotations

import json
import re
import time
import uuid
from pathlib import Path

_MAX_USERNAME_LEN = 64
_SAFE_CHARS = re.compile(r"[^A-Za-z0-9_.-]")


def _safe_username(username: str | None) -> str:
    """Sanitize a display name into a safe filename — never escapes chat_history/."""
    if not username or not username.strip():
        return "local_user"
    cleaned = _SAFE_CHARS.sub("_", username.strip().replace(" ", "_"))
    cleaned = cleaned.strip("._") or "local_user"
    return cleaned[:_MAX_USERNAME_LEN]


def _history_path(project: str, projects_root: str | Path, username: str) -> Path:
    safe = _safe_username(username)
    return Path(projects_root) / project / "chat_history" / f"{safe}.jsonl"


def _read_all(path: Path) -> list[dict]:
    """Read every line, skipping malformed ones — never raises."""
    if not path.exists():
        return []
    turns: list[dict] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            turns.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return turns


def _write_all(path: Path, turns: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for turn in turns:
            f.write(json.dumps(turn) + "\n")


def record_chat_turn(
    project: str,
    projects_root: str | Path,
    username: str,
    *,
    question: str,
    sql: str,
    answer: str,
    row_count: int,
    columns: list[str],
    session_id: str = "",
    response_generated: bool = True,
    turn_id: str | None = None,
    accepted: bool | None = None,
) -> str:
    """Append one turn to ``<project>/chat_history/<username>.jsonl``.

    Returns the turn's ``turn_id`` (generated if not supplied) so a caller
    (e.g. neuron's ``/query`` response) can hand it back to the client for a
    later ``update_turn_accepted`` call.
    """
    path = _history_path(project, projects_root, username)
    path.parent.mkdir(parents=True, exist_ok=True)

    resolved_turn_id = turn_id or uuid.uuid4().hex

    entry = {
        "ts":                 time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "turn_id":            resolved_turn_id,
        "session_id":         session_id,
        "question":           question,
        "sql":                sql,
        "answer":             answer,
        "row_count":          row_count,
        "columns":            columns,
        "response_generated": response_generated,
        "accepted":           accepted,
        "promoted":           False,
    }
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")

    return resolved_turn_id


def list_chat_history(
    project: str,
    projects_root: str | Path,
    username: str,
    limit: int = 50,
) -> list[dict]:
    """Return this user's past turns, newest first, capped at ``limit``.

    Malformed lines are skipped rather than raising — a corrupted entry
    should never take down the whole history view.
    """
    path = _history_path(project, projects_root, username)
    turns = _read_all(path)
    return list(reversed(turns))[:limit]


def update_turn_accepted(
    project: str,
    projects_root: str | Path,
    username: str,
    turn_id: str,
    accepted: bool,
) -> bool:
    """Set ``accepted`` on the matching turn (thumbs-up/down). Read-all,
    rewrite-all — same pattern as metadata/relationships approve.py.

    Returns ``True`` if a matching turn was found and updated, ``False``
    otherwise (never raises for an unknown ``turn_id``).
    """
    path = _history_path(project, projects_root, username)
    turns = _read_all(path)

    found = False
    for turn in turns:
        if turn.get("turn_id") == turn_id:
            turn["accepted"] = accepted
            found = True
            break

    if found:
        _write_all(path, turns)
    return found


def promote_session(
    project: str,
    projects_root: str | Path,
    username: str,
    session_id: str,
) -> dict[str, int]:
    """Roll a closed session's rated-but-unpromoted turns into
    examples/harvested.jsonl (accepted=True) / failures.jsonl (accepted=False)
    via the existing, unchanged examples/feedback.py:record_feedback().

    Turns with accepted=None (no explicit feedback) are never promoted —
    the three-state design means "no signal" stays "no signal", not an
    inferred one. Idempotent: already-promoted turns are skipped, so calling
    this twice for the same session never double-counts.
    """
    from openmedallion.examples.feedback import record_feedback

    path = _read_all_path = _history_path(project, projects_root, username)
    turns = _read_all(_read_all_path)

    harvested = 0
    failed = 0
    changed = False

    for turn in turns:
        if turn.get("session_id") != session_id:
            continue
        if turn.get("promoted"):
            continue
        accepted = turn.get("accepted")
        if accepted is None:
            continue

        record_feedback(
            project, projects_root,
            question=turn.get("question", ""), sql=turn.get("sql", ""),
            columns=turn.get("columns") or [], row_count=turn.get("row_count", 0),
            thumbs_up=accepted,
        )
        turn["promoted"] = True
        changed = True
        if accepted:
            harvested += 1
        else:
            failed += 1

    if changed:
        _write_all(path, turns)

    return {"harvested": harvested, "failed": failed}
