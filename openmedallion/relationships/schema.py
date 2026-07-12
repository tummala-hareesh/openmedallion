"""relationships/schema.py — Pydantic models for a project's relationships.yaml.

Phase 1 of the RAG Accuracy Improvement roadmap (see CLAUDE.md). Locked design
decisions this schema encodes:

- Single ``relationships.yaml`` file — each entry carries its own
  ``status: draft/approved/stale``, mirroring ``metadata.yaml``'s pattern.
  There is no separate ``relationships_recommended.yaml`` staging file.
- No ``relationships_enhancements.yaml`` overlay — detection (FK naming,
  source-table lineage, shared grain columns) is deterministic pattern
  matching, not LLM-drafted, so there's no regeneration-clobber risk to
  guard against the way ``metadata_enhancements.yaml`` does. Hand-edit
  ``relationships.yaml`` directly.
- ``join_on`` is always ``list[str]``, even for a single-column join — keeps
  the schema uniform instead of a ``str | list[str]`` union.
- ``method`` records which detection rule found the relationship
  (``fk_naming`` / ``lineage`` / ``grain``); optional, since a human can
  hand-add a relationship with no detection rule behind it.
- ``extra="forbid"`` everywhere, matching the ``config/schema.py`` /
  ``metadata/schema.py`` pattern — a typo is rejected at load time.
"""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class RelationshipEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    from_table: str
    to_table: str
    join_on: list[str] = Field(min_length=1)
    confidence: Literal["high", "medium", "low"] = "high"
    status: Literal["draft", "approved", "stale"] = "draft"
    method: Literal["fk_naming", "lineage", "grain"] | None = None


class RelationshipsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    relationships: list[RelationshipEntry] = Field(default_factory=list)
