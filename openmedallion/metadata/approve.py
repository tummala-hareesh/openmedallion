"""metadata/approve.py — human review loop for metadata.yaml (RAG roadmap
Phase 1, build order step 3).

Operates on the raw ``metadata.yaml`` dict directly (``yaml.safe_load``, not
``metadata/loader.py``'s merged ``load_metadata()``) so approving a table
never accidentally bakes ``metadata_enhancements.yaml``'s overlay content
into ``metadata.yaml`` — the two files stay separate, exactly as designed in
``metadata/loader.py``.

``apply_approvals`` is the pure, testable core: given a decisions dict keyed
by table name, it deep-merges each decision into the matching table entry
(reusing ``config/loader.py``'s ``_deep_merge``) so a decision can both flip
``status: approved`` and optionally edit ``description``/``synonyms``/
``columns`` in the same step. Omitting ``status`` from a decision (or
omitting the table from ``decisions`` entirely) leaves it exactly as it was
— the "skip for now, review again later" path.
"""
from __future__ import annotations

import copy
from pathlib import Path

import yaml

from openmedallion.config.loader import _deep_merge
from openmedallion.metadata.loader import _validate_metadata
from openmedallion.metadata.schema import MetadataConfig, TableMeta


def apply_approvals(raw: dict, decisions: dict[str, dict]) -> dict:
    """Return a copy of *raw* with each decision deep-merged into its table.

    Args:
        raw: The raw ``metadata.yaml`` dict (as loaded by ``yaml.safe_load``).
        decisions: ``{table_name: partial_table_dict}``. Include
            ``{"status": "approved"}`` to approve; omit ``status`` (or the
            table entirely) to leave it untouched. May also include
            ``description``, ``synonyms``, or ``columns`` keys to edit the
            LLM draft before approving — these are deep-merged, not replaced.

    Returns:
        dict: A new dict — *raw* is never mutated.

    Raises:
        ValueError: If a decision names a table not present in ``raw``.
    """
    result = copy.deepcopy(raw)
    tables = result.setdefault("tables", {})
    for name, edits in decisions.items():
        if name not in tables:
            raise ValueError(f"[metadata] table '{name}' not found in metadata.yaml")
        _deep_merge(tables[name], edits)
    return result


def list_reviewable_tables(project: str, projects_root: str | Path = "projects") -> list[tuple[str, TableMeta]]:
    """Return ``[(table_name, TableMeta)]`` for every ``draft``/``stale`` table.

    Reads the raw ``metadata.yaml`` only (not merged with
    ``metadata_enhancements.yaml``) — matches what ``approve_metadata`` will
    actually write back. Sorted by table name. Returns an empty list if
    ``metadata.yaml`` doesn't exist yet.
    """
    root = Path(projects_root) / project
    meta_path = root / "metadata.yaml"
    if not meta_path.exists():
        return []

    with open(meta_path) as f:
        raw = yaml.safe_load(f) or {}

    config = _validate_metadata(raw)
    return sorted(
        (name, table) for name, table in config.tables.items()
        if table.status in ("draft", "stale")
    )


def approve_metadata(
    project: str,
    projects_root: str | Path = "projects",
    decisions: dict[str, dict] | None = None,
) -> MetadataConfig:
    """Apply approval decisions to ``metadata.yaml`` and write the result back.

    Args:
        project: Project name — folder name under ``projects_root``.
        projects_root: Parent directory containing project folders.
        decisions: ``{table_name: partial_table_dict}`` — see
            :func:`apply_approvals`. ``None`` or ``{}`` is a no-op (file is
            still re-validated and re-written unchanged).

    Returns:
        MetadataConfig: The updated, validated metadata model.

    Raises:
        FileNotFoundError: If ``metadata.yaml`` doesn't exist yet — run
            ``medallion metadata generate`` first.
        ValueError: If the resulting metadata fails schema validation.
    """
    root = Path(projects_root) / project
    meta_path = root / "metadata.yaml"
    if not meta_path.exists():
        raise FileNotFoundError(
            f"No metadata.yaml found for project '{project}' at {meta_path}\n"
            f"Run:  medallion metadata generate {project}"
        )

    with open(meta_path) as f:
        raw = yaml.safe_load(f) or {}

    updated = apply_approvals(raw, decisions or {})
    config = _validate_metadata(updated)

    with open(meta_path, "w") as f:
        yaml.safe_dump(updated, f, sort_keys=False)

    return config
