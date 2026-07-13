"""relationships/erd.py — render relationships.yaml + real Parquet dtypes as
a Mermaid ``erDiagram`` (a follow-on to the RAG accuracy roadmap, folded into
``medallion relationships`` per the design discussion, not a new command).

Locked design decisions:
- **Approved-only by default** (``include_all=False``) — matches the
  "approved is the trust boundary" convention used everywhere else in the
  RAG roadmap (schema pruning, examples generation). ``include_all=True``
  draws every relationship regardless of ``status``.
- **Only tables that are an endpoint of an included relationship are
  drawn** — no orphan tables with no relationships plotted.
- **Column types come from real Parquet dtypes** (DuckDB ``DESCRIBE``, same
  helper pattern as ``relationships/generator.py:_describe_columns``), not
  from ``metadata.yaml``'s optional/sparse ``columns:`` dict — accurate even
  for a project with no curated metadata at all. A table referenced by a
  relationship but missing from silver/gold Parquet still renders (an empty
  entity block) rather than raising.
- **No PK inference** — nothing in relationships.yaml/metadata.yaml actually
  asserts a primary key, so none is guessed at. Columns render as plain
  typed fields; the Mermaid relationship line itself implies the FK.
- Default cardinality is many-to-one, ``from_table`` (many) to ``to_table``
  (one) — matches the direction convention ``fk_naming``/``lineage``
  detection already use, and the hand-written ERDs in
  ``examples/sales_intelligence_demo/README.md`` (e.g. ``TRANSACTIONS
  }o--|| REPS``).
- Written to a fixed file name, ``<project>/relationships_erd.md`` — a
  Markdown file with a fenced ```mermaid block, directly viewable on GitHub
  and embeddable via mkdocs' ``--8<--`` snippet syntax.
"""
from __future__ import annotations

import re
from pathlib import Path

import duckdb

from openmedallion.config.loader import load_project
from openmedallion.relationships.loader import load_relationships
from openmedallion.relationships.schema import RelationshipEntry

_SANITIZE = re.compile(r"[^A-Za-z0-9_]+")


def _describe_columns(path: Path) -> list[tuple[str, str]]:
    """Return ``[(column_name, dtype), ...]`` for one Parquet file via DuckDB."""
    con = duckdb.connect()
    try:
        table = path.stem
        con.execute(f'CREATE OR REPLACE VIEW "{table}" AS SELECT * FROM read_parquet(\'{path}\')')
        return [(row[0], row[1]) for row in con.execute(f'DESCRIBE "{table}"').fetchall()]
    finally:
        con.close()


def _find_table_columns(table: str, silver_dir: Path, gold_dir: Path) -> list[tuple[str, str]]:
    """Locate *table*'s Parquet file in silver or gold and describe its columns.

    Returns an empty list (not an error) if the table isn't found — a
    relationship can reference a table that's since been renamed/removed,
    and the diagram should still render everything else.
    """
    for layer_dir in (silver_dir, gold_dir):
        path = layer_dir / f"{table}.parquet"
        if path.exists():
            return _describe_columns(path)
    return []


def _sanitize_mermaid_type(dtype: str) -> str:
    """Mermaid entity attribute types must be a single token — strip anything
    that isn't alphanumeric/underscore (e.g. ``DECIMAL(18,2)`` -> ``DECIMAL``)."""
    return _SANITIZE.sub("", dtype) or "unknown"


def _render_entity(table: str, columns: list[tuple[str, str]]) -> str:
    lines = [f"    {table} {{"]
    for col, dtype in columns:
        lines.append(f"        {_sanitize_mermaid_type(dtype)} {col}")
    lines.append("    }")
    return "\n".join(lines)


def _render_relationship(rel: RelationshipEntry) -> str:
    label = ", ".join(rel.join_on)
    # Many-to-one: from_table (many) -> to_table (one) — see module docstring.
    return f'    {rel.from_table} }}o--|| {rel.to_table} : "{label}"'


def generate_erd(
    project: str,
    projects_root: str | Path = "projects",
    *,
    include_all: bool = False,
) -> str:
    """Render *project*'s relationships as a Mermaid ``erDiagram`` and write it
    to ``<project>/relationships_erd.md``.

    Args:
        project: Project name — folder name under ``projects_root``.
        projects_root: Parent directory containing project folders.
        include_all: When ``False`` (default), only ``status: approved``
            relationships are drawn. When ``True``, every relationship is
            drawn regardless of status.

    Returns:
        str: The full Markdown file content (also written to disk).
    """
    cfg        = load_project(project, projects_root)
    silver_dir = Path(cfg["paths"]["silver"])
    gold_dir   = Path(cfg["paths"]["gold"]) / cfg["pipeline"]["name"]

    relationships = load_relationships(project, projects_root).relationships
    if not include_all:
        relationships = [r for r in relationships if r.status == "approved"]

    tables: list[str] = []
    for rel in relationships:
        for table in (rel.from_table, rel.to_table):
            if table not in tables:
                tables.append(table)

    entity_blocks = [
        _render_entity(table, _find_table_columns(table, silver_dir, gold_dir))
        for table in tables
    ]
    relationship_lines = [_render_relationship(rel) for rel in relationships]

    diagram = "\n\n".join(["erDiagram", *entity_blocks, *relationship_lines]) \
        if entity_blocks or relationship_lines else "erDiagram"

    content = (
        f"# {project} — Relationship ERD\n\n"
        f"<!-- autogenerated by `medallion relationships erd {project}` "
        f"— edit relationships.yaml, not this file -->\n\n"
        f"```mermaid\n{diagram}\n```\n"
    )

    root = Path(projects_root) / project
    erd_path = root / "relationships_erd.md"
    with open(erd_path, "w") as f:
        f.write(content)

    return content
