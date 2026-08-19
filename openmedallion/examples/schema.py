"""examples/schema.py — Pydantic model for one synthetic.jsonl row.

Phase 1 of the RAG Accuracy Improvement roadmap (see CLAUDE.md), build order
step 7. Not to be confused with the repo-root ``examples/`` directory (demo
projects) — this is ``openmedallion.examples``, a subpackage alongside
``openmedallion.metadata`` and ``openmedallion.relationships``.

``synthetic.jsonl`` is JSONL, not YAML — one JSON object per line — so unlike
``metadata.yaml``/``relationships.yaml`` there's no single top-level
``*Config`` wrapper model; each line validates independently as a
``SyntheticExample``.

Template-Routed Query Layer roadmap (see CLAUDE.md), build order step 1:
``templated``/``params`` promote an already-verified example into a
deterministic, parameter-fillable SQL template consumed by
``cerebrum``'s template-routing fast path. ``templated: true`` is a
stricter claim than ``verified: true`` and is never reachable without it —
promotion happens at a separate, later review step
(``medallion examples approve --template``), never implied by verification
alone. ``params`` is a flat ``{slot_name: description/constraint}`` dict
describing the ``{param}`` placeholders literally present in ``sql``,
matching the ``{ref:name}`` substitution style already used by
``filter_defs`` in ``pipeline/bronze.py``.
"""
from __future__ import annotations

from pydantic import BaseModel, ConfigDict, model_validator


class SyntheticExample(BaseModel):
    model_config = ConfigDict(extra="forbid")

    question: str
    sql: str
    verified: bool = False
    templated: bool = False
    params: dict[str, str] | None = None

    @model_validator(mode="after")
    def _check_templated_requires_verified(self) -> "SyntheticExample":
        if self.templated and not self.verified:
            raise ValueError("templated=True requires verified=True — templates are promoted from already-verified examples")
        return self
