"""examples/feedback.py — cortex thumbs up/down feedback (RAG roadmap Phase 3,
build order step 13).

Locked design decision: cortex never writes these files directly — it never
imports cerebrum/pipeline internals, only talks to neuron over HTTP (a
pre-existing convention). This module is called from a new neuron
``POST /feedback`` endpoint (``neuron/server.py``), not from cortex.

``harvested.jsonl`` and ``failures.jsonl`` are simple append-only logs —
unlike ``synthetic.jsonl``'s generate/approve in-place-rewrite pattern,
nothing here ever rewrites or deduplicates prior entries when *recording*
feedback. Promoting ``harvested.jsonl`` candidates into ``synthetic.jsonl``
(``medallion examples harvest``) and listing ``failures.jsonl``
(``medallion examples review``) are handled by ``examples/harvest.py``
(build order step 14).
"""
from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict


class HarvestedCandidate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    question: str
    sql: str
    result_shape: dict
    # "harvested" is set by examples/harvest.py once a candidate has been
    # promoted into synthetic.jsonl, so re-running harvest never duplicates it.
    status: Literal["candidate", "harvested"] = "candidate"


class FailureRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    question: str
    sql: str
    status: Literal["failed"] = "failed"


def record_feedback(
    project: str,
    projects_root: str | Path = "projects",
    *,
    question: str,
    sql: str,
    columns: list[str] | None = None,
    row_count: int = 0,
    thumbs_up: bool,
) -> None:
    """Append one feedback entry to a project's harvested/failures log.

    Args:
        project: Project name — folder name under ``projects_root``.
        projects_root: Parent directory containing project folders.
        question: The natural-language question that was asked.
        sql: The SQL that was executed to answer it.
        columns: Column names of the result, if any.
        row_count: Number of rows returned.
        thumbs_up: ``True`` appends to ``examples/harvested.jsonl``
            (``status: candidate``); ``False`` appends to
            ``examples/failures.jsonl`` (``status: failed``).
    """
    examples_dir = Path(projects_root) / project / "examples"
    examples_dir.mkdir(parents=True, exist_ok=True)

    if thumbs_up:
        entry = HarvestedCandidate(
            question=question, sql=sql,
            result_shape={"rows": row_count, "columns": columns or []},
        )
        path = examples_dir / "harvested.jsonl"
    else:
        entry = FailureRecord(question=question, sql=sql)
        path = examples_dir / "failures.jsonl"

    with open(path, "a") as f:
        f.write(entry.model_dump_json() + "\n")
