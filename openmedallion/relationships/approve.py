"""relationships/approve.py — human review loop for relationships.yaml
(RAG roadmap Phase 1, build order step 6).

Near-direct port of ``metadata/approve.py``'s pattern, adapted for
relationships being a *list* (no natural name to key by) rather than a dict
of tables. A decision is keyed by the identity tuple
``(from_table, to_table, tuple(sorted(join_on)))`` — the same "sameness"
concept ``relationships/detector.py``'s ``_identity()`` uses for
``RelationshipEntry`` objects, recomputed here against raw dicts since there
is no ``relationships_enhancements.yaml`` overlay to keep separate (locked
design decision — see ``relationships/schema.py``), so operating directly on
the loaded ``RelationshipsConfig`` isn't a concern the way it is for metadata.
"""
from __future__ import annotations

import copy
from pathlib import Path

import yaml

from openmedallion.config.loader import _deep_merge
from openmedallion.relationships.loader import _validate_relationships
from openmedallion.relationships.schema import RelationshipEntry, RelationshipsConfig

_Identity = tuple[str, str, tuple[str, ...]]


def _raw_identity(entry: dict) -> _Identity:
    return (entry["from_table"], entry["to_table"], tuple(sorted(entry["join_on"])))


def apply_approvals(raw: dict, decisions: dict[_Identity, dict]) -> dict:
    """Return a copy of *raw* with each decision deep-merged into its entry.

    Args:
        raw: The raw ``relationships.yaml`` dict (as loaded by ``yaml.safe_load``).
        decisions: ``{(from_table, to_table, sorted_join_on): partial_entry_dict}``.
            Include ``{"status": "approved"}`` to approve; omit ``status``
            (or the identity entirely) to leave it untouched — the "skip for
            now, review again later" path. May also include ``confidence``
            to edit the detected value before approving — deep-merged, not
            replaced.

    Returns:
        dict: A new dict — *raw* is never mutated.

    Raises:
        ValueError: If a decision names an identity not present in ``raw``.
    """
    result = copy.deepcopy(raw)
    entries = result.setdefault("relationships", [])
    by_identity = {_raw_identity(e): e for e in entries}
    for key, edits in decisions.items():
        norm_key = (key[0], key[1], tuple(sorted(key[2])))
        if norm_key not in by_identity:
            raise ValueError(f"[relationships] relationship {key!r} not found in relationships.yaml")
        _deep_merge(by_identity[norm_key], edits)
    return result


def list_reviewable_relationships(
    project: str, projects_root: str | Path = "projects"
) -> list[RelationshipEntry]:
    """Return every ``draft``/``stale`` relationship, sorted deterministically.

    Reads the raw ``relationships.yaml`` only. Returns an empty list if the
    file doesn't exist yet.
    """
    root = Path(projects_root) / project
    rel_path = root / "relationships.yaml"
    if not rel_path.exists():
        return []

    with open(rel_path) as f:
        raw = yaml.safe_load(f) or {}

    config = _validate_relationships(raw)
    return sorted(
        (r for r in config.relationships if r.status in ("draft", "stale")),
        key=lambda r: (r.from_table, r.to_table, r.join_on),
    )


def approve_relationships(
    project: str,
    projects_root: str | Path = "projects",
    decisions: dict[_Identity, dict] | None = None,
) -> RelationshipsConfig:
    """Apply approval decisions to ``relationships.yaml`` and write the result back.

    Args:
        project: Project name — folder name under ``projects_root``.
        projects_root: Parent directory containing project folders.
        decisions: See :func:`apply_approvals`. ``None``/``{}`` is a no-op
            (file is still re-validated and re-written unchanged).

    Returns:
        RelationshipsConfig: The updated, validated model.

    Raises:
        FileNotFoundError: If ``relationships.yaml`` doesn't exist yet — run
            ``medallion relationships generate`` first.
        ValueError: If the resulting relationships fail schema validation.
    """
    root = Path(projects_root) / project
    rel_path = root / "relationships.yaml"
    if not rel_path.exists():
        raise FileNotFoundError(
            f"No relationships.yaml found for project '{project}' at {rel_path}\n"
            f"Run:  medallion relationships generate {project}"
        )

    with open(rel_path) as f:
        raw = yaml.safe_load(f) or {}

    updated = apply_approvals(raw, decisions or {})
    config = _validate_relationships(updated)

    with open(rel_path, "w") as f:
        yaml.safe_dump(updated, f, sort_keys=False)

    return config
