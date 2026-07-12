"""relationships/detector.py — three deterministic relationship-detection rules
(RAG roadmap Phase 1, build order step 5).

No LLM call — every rule here is pure structural pattern matching over
column names (and dtypes, for the grain rule), operating on already-parsed
table schemas. This corrects a claim in the original roadmap sketch that
implied LLM involvement in relationship detection (see CLAUDE.md).

Rules
-----
``fk_naming``
    A ``<entity>_id`` column shared by two or more tables, where one table's
    name matches ``pluralize(entity)`` — that table is the target
    (``to_table``); every other table sharing the column becomes a
    ``from_table``. Confidence: ``high``.

``lineage``
    Table A's full column set is a proper subset of table B's — B is very
    likely built by enriching A (e.g. a derived silver table containing every
    column of its base table plus more). ``join_on`` is A's own natural
    primary key (``<singular(A)>_id``) if present in both, else the full
    shared column set. Confidence: ``medium``.

``grain``
    A non-``_id`` string (``VARCHAR``)-typed column shared by two or more
    tables — a dimension/grain column without FK naming (e.g.
    ``department_name`` appearing in both a silver table and a gold rollup).
    Confidence: ``low`` — the weakest signal, most prone to false positives.

:func:`detect_relationships` orchestrates all three and deduplicates entries
that land on the exact same ``(from_table, to_table, join_on)`` identity,
preferring the more specific/reliable method: ``fk_naming`` > ``lineage`` >
``grain``.
"""
from __future__ import annotations

import re
from itertools import combinations

from openmedallion.relationships.schema import RelationshipEntry

_ID_SUFFIX = re.compile(r"^(\w+)_id$")
_METHOD_PREFERENCE = {"fk_naming": 0, "lineage": 1, "grain": 2}


def _pluralize(word: str) -> str:
    if word.endswith("y") and word[-2:-1] not in "aeiou":
        return word[:-1] + "ies"
    if word.endswith(("s", "x", "ch", "sh")):
        return word + "es"
    return word + "s"


def _singularize(word: str) -> str:
    if word.endswith("ies"):
        return word[:-3] + "y"
    if word.endswith(("ses", "xes", "ches", "shes")):
        return word[:-2]
    if word.endswith("s"):
        return word[:-1]
    return word


def _detect_fk_naming(table_columns: dict[str, list[str]]) -> list[RelationshipEntry]:
    """Find ``<entity>_id`` columns shared across tables, resolved by pluralization."""
    tables_by_column: dict[str, list[str]] = {}
    for table, columns in table_columns.items():
        for col in columns:
            tables_by_column.setdefault(col, []).append(table)

    entries: list[RelationshipEntry] = []
    for column, tables in sorted(tables_by_column.items()):
        if len(tables) < 2:
            continue
        m = _ID_SUFFIX.match(column)
        if not m:
            continue
        entity = m.group(1)
        target_candidates = {entity, _pluralize(entity)}
        targets = [t for t in tables if t in target_candidates]
        if not targets:
            continue
        target = targets[0]
        for table in sorted(tables):
            if table == target:
                continue
            entries.append(RelationshipEntry(
                from_table=table, to_table=target, join_on=[column],
                confidence="high", status="draft", method="fk_naming",
            ))
    return entries


def _detect_lineage(table_columns: dict[str, list[str]]) -> list[RelationshipEntry]:
    """Find table pairs where one's columns are a proper subset of the other's."""
    entries: list[RelationshipEntry] = []
    names = sorted(table_columns)
    for a in names:
        cols_a = set(table_columns[a])
        if not cols_a:
            continue
        for b in names:
            if a == b:
                continue
            cols_b = set(table_columns[b])
            if not (cols_a < cols_b):
                continue
            candidate_pk = f"{_singularize(a)}_id"
            if candidate_pk in cols_a and candidate_pk in cols_b:
                join_on = [candidate_pk]
            else:
                join_on = sorted(cols_a)
            entries.append(RelationshipEntry(
                from_table=b, to_table=a, join_on=join_on,
                confidence="medium", status="draft", method="lineage",
            ))
    return entries


def _detect_grain(table_typed_columns: dict[str, list[tuple[str, str]]]) -> list[RelationshipEntry]:
    """Find non-id VARCHAR columns shared across tables (grain/dimension columns)."""
    tables_by_column: dict[str, list[str]] = {}
    for table, cols in table_typed_columns.items():
        for name, dtype in cols:
            if dtype.upper() != "VARCHAR" or _ID_SUFFIX.match(name):
                continue
            tables_by_column.setdefault(name, []).append(table)

    col_count = {table: len(cols) for table, cols in table_typed_columns.items()}

    entries: list[RelationshipEntry] = []
    for column, tables in sorted(tables_by_column.items()):
        if len(tables) < 2:
            continue
        for t1, t2 in combinations(sorted(tables), 2):
            # More columns = "from" (detail/fact side); fewer = "to" (dimension side).
            from_table, to_table = (t1, t2) if col_count[t1] >= col_count[t2] else (t2, t1)
            entries.append(RelationshipEntry(
                from_table=from_table, to_table=to_table, join_on=[column],
                confidence="low", status="draft", method="grain",
            ))
    return entries


def _identity(entry: RelationshipEntry) -> tuple:
    return (entry.from_table, entry.to_table, tuple(sorted(entry.join_on)))


def detect_relationships(table_typed_columns: dict[str, list[tuple[str, str]]]) -> list[RelationshipEntry]:
    """Run all three detection rules and dedup by ``(from_table, to_table, join_on)``.

    Args:
        table_typed_columns: ``{table_name: [(column_name, dtype), ...]}``.

    Returns:
        list[RelationshipEntry]: Deduplicated draft relationships, sorted for
        deterministic output. When two rules land on the identical identity,
        the more specific/reliable one wins: ``fk_naming`` > ``lineage`` > ``grain``.
    """
    table_columns = {t: [c for c, _ in cols] for t, cols in table_typed_columns.items()}

    all_entries = (
        _detect_fk_naming(table_columns)
        + _detect_lineage(table_columns)
        + _detect_grain(table_typed_columns)
    )

    best: dict[tuple, RelationshipEntry] = {}
    for entry in all_entries:
        key = _identity(entry)
        current = best.get(key)
        if current is None or _METHOD_PREFERENCE[entry.method] < _METHOD_PREFERENCE[current.method]:
            best[key] = entry

    return sorted(best.values(), key=lambda e: (e.from_table, e.to_table, e.join_on))
