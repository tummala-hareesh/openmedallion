"""metadata/schema.py — Pydantic models for a project's metadata.yaml.

Phase 1 of the RAG Accuracy Improvement roadmap (see CLAUDE.md). Locked design
decisions this schema encodes:

- ``status`` (``draft`` / ``approved`` / ``stale``) is table-level only — no
  per-column status. A table's column descriptions are injected into the LLM
  prompt (Phase 2) only once its table-level status is ``approved``.
- ``glossary`` is a flat ``dict[str, str]`` of business terms not tied to any
  single column.
- No structured ``metrics:`` section — candidate business metrics the LLM
  notices while drafting (e.g. "this looks like it could be `revenue`")
  surface as prose inside a column's ``description:`` instead.
- ``extra="forbid"`` everywhere, matching the ``config/schema.py`` pattern
  from T-TODO-4 — a typo in metadata.yaml is rejected at load time.
"""
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class ColumnMeta(BaseModel):
    model_config = ConfigDict(extra="forbid")

    description: str | None = None
    value_examples: list[Any] | None = None
    synonyms: list[str] | None = None
    # Opt-in ydata-profiling enrichment (`metadata generate/refresh --profile`,
    # see openmedallion/metadata/profiling.py). None unless a profiling run has
    # populated them — never set from the LLM draft or plain DuckDB sampling.
    dtype: str | None = None
    stats: dict[str, Any] | None = None
    accepted_values: list[Any] | None = None


class TableMeta(BaseModel):
    model_config = ConfigDict(extra="forbid")

    layer: Literal["silver", "gold"]
    description: str | None = None
    status: Literal["draft", "approved", "stale"] = "draft"
    synonyms: list[str] | None = None
    columns: dict[str, ColumnMeta] = Field(default_factory=dict)
    schema_hash: str | None = None


class MetadataConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tables: dict[str, TableMeta] = Field(default_factory=dict)
    glossary: dict[str, str] | None = None
