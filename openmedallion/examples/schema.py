"""examples/schema.py — Pydantic model for one synthetic.jsonl row.

Phase 1 of the RAG Accuracy Improvement roadmap (see CLAUDE.md), build order
step 7. Not to be confused with the repo-root ``examples/`` directory (demo
projects) — this is ``openmedallion.examples``, a subpackage alongside
``openmedallion.metadata`` and ``openmedallion.relationships``.

``synthetic.jsonl`` is JSONL, not YAML — one JSON object per line — so unlike
``metadata.yaml``/``relationships.yaml`` there's no single top-level
``*Config`` wrapper model; each line validates independently as a
``SyntheticExample``.
"""
from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class SyntheticExample(BaseModel):
    model_config = ConfigDict(extra="forbid")

    question: str
    sql: str
    verified: bool = False
