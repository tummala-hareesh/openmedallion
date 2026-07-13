"""relationships/generator.py — scan silver/gold schemas and regenerate
relationships.yaml (RAG roadmap Phase 1, build order step 5).

No LLM call — ``detect_relationships()`` is pure structural pattern matching.
This module's job is purely the I/O + merge-with-existing-file layer around it.

Merge policy on regeneration (mirrors ``metadata/generator.py``'s
"never touch approved" rule, adapted for a list keyed by
``(from_table, to_table, join_on)`` identity instead of a table name):

- Existing entries with ``method: null`` (hand-added by a human, no detection
  rule behind them) are **always kept** — the detector has no way to know
  about these and must never discard human-authored content.
- Existing entries with a ``method`` set **and** ``status: approved`` are
  **always kept, byte-for-byte untouched**.
- Existing entries with a ``method`` set **and** ``status: draft``/``stale``
  are **dropped** — they're prior auto-drafts, safe to replace with a fresh
  detection run rather than sitting duplicated alongside it.
- Fresh detections are added, skipping any whose identity already exists in
  the kept set.
"""
from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import duckdb
import yaml

from openmedallion.config.loader import load_project
from openmedallion.relationships.detector import _identity, detect_relationships
from openmedallion.relationships.schema import RelationshipEntry, RelationshipsConfig


def _describe_columns(path: Path) -> list[tuple[str, str]]:
    """Return ``[(column_name, dtype), ...]`` for one Parquet file via DuckDB."""
    con = duckdb.connect()
    try:
        table = path.stem
        con.execute(f'CREATE OR REPLACE VIEW "{table}" AS SELECT * FROM read_parquet(\'{path}\')')
        return [(row[0], row[1]) for row in con.execute(f'DESCRIBE "{table}"').fetchall()]
    finally:
        con.close()


def generate_relationships(
    project: str,
    projects_root: str | Path = "projects",
    *,
    on_step: Callable[[str], None] | None = None,
) -> RelationshipsConfig:
    """Detect relationships across a project's silver + gold tables and write them back.

    Args:
        project: Project name — folder name under ``projects_root``.
        projects_root: Parent directory containing project folders.
        on_step: Optional callback invoked with progress messages.

    Returns:
        RelationshipsConfig: The merged, validated model — also written to
        ``<project>/relationships.yaml``.
    """
    cfg        = load_project(project, projects_root)
    silver_dir = Path(cfg["paths"]["silver"])
    gold_dir   = Path(cfg["paths"]["gold"]) / cfg["pipeline"]["name"]

    if on_step:
        on_step("relationships generate: scanning silver/gold schemas")

    table_typed_columns: dict[str, list[tuple[str, str]]] = {}
    for layer_dir in (silver_dir, gold_dir):
        if not layer_dir.exists():
            continue
        for path in sorted(layer_dir.glob("*.parquet")):
            table_typed_columns[path.stem] = _describe_columns(path)

    if on_step:
        on_step(f"relationships generate: {len(table_typed_columns)} table(s) found, detecting relationships")

    detected = detect_relationships(table_typed_columns)

    root     = Path(projects_root) / project
    rel_path = root / "relationships.yaml"
    existing_raw: dict = {}
    if rel_path.exists():
        with open(rel_path) as f:
            existing_raw = yaml.safe_load(f) or {}
    existing_entries = existing_raw.get("relationships") or []

    kept: list[dict] = []
    kept_identities: set[tuple] = set()
    for entry in existing_entries:
        is_hand_added = entry.get("method") is None
        is_approved   = entry.get("status") == "approved"
        if is_hand_added or is_approved:
            kept.append(entry)
            kept_identities.add((entry["from_table"], entry["to_table"], tuple(sorted(entry["join_on"]))))
            if on_step:
                reason = "hand-added" if is_hand_added else "approved"
                on_step(f"relationships generate: {entry['from_table']} -> {entry['to_table']} — {reason}, kept")

    new_entries: list[RelationshipEntry] = [e for e in detected if _identity(e) not in kept_identities]

    result = RelationshipsConfig(
        relationships=[RelationshipEntry(**e) for e in kept] + new_entries
    )

    with open(rel_path, "w") as f:
        yaml.safe_dump(result.model_dump(exclude_none=True), f, sort_keys=False)

    if on_step:
        on_step(f"relationships generate: {len(new_entries)} new, {len(kept)} preserved, "
                f"{len(result.relationships)} total")

    return result
