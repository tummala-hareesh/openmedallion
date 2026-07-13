"""examples/approve.py — human review loop for synthetic.jsonl (RAG roadmap
Phase 1/2 boundary, build order step 8).

``synthetic.jsonl`` has no natural unique key the way ``metadata.yaml``
(table name) or ``relationships.yaml`` (``(from_table, to_table, join_on)``)
do. Locked design decision: identity is a **content hash** of
``(question, sql)``, computed on the fly — no schema change to the
already-shipped ``SyntheticExample``/``generate_examples()``. Editing the
question/sql text between generate and approve produces a different hash,
which is correctly treated as "not the same example" — matching how
relationships identity already behaves when its join columns change.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from openmedallion.config.loader import _deep_merge
from openmedallion.examples.schema import SyntheticExample


def content_hash(question: str, sql: str) -> str:
    """Derive a short, deterministic identity for one (question, sql) pair."""
    return hashlib.sha256(f"{question}\x00{sql}".encode()).hexdigest()[:12]


def apply_approvals(raw: list[dict], decisions: dict[str, dict]) -> list[dict]:
    """Return a copy of *raw* with each decision deep-merged into its entry.

    Args:
        raw: The raw ``synthetic.jsonl`` lines (each already ``json.loads``'d).
        decisions: ``{content_hash(question, sql): partial_entry_dict}``.
            Include ``{"verified": True}`` to approve; omit ``verified`` (or
            the hash entirely) to leave it untouched — the "skip for now,
            review again later" path. May also include ``question``/``sql``
            to edit before approving — deep-merged, not replaced.

    Returns:
        list[dict]: A new list — *raw* is never mutated.

    Raises:
        ValueError: If a decision names a hash not present in ``raw``.
    """
    result = [dict(entry) for entry in raw]
    by_hash = {content_hash(e["question"], e["sql"]): e for e in result}
    for key, edits in decisions.items():
        if key not in by_hash:
            raise ValueError(f"[examples] example {key!r} not found in synthetic.jsonl")
        _deep_merge(by_hash[key], edits)
    return result


def _read_raw(project: str, projects_root: str | Path) -> list[dict]:
    path = Path(projects_root) / project / "examples" / "synthetic.jsonl"
    if not path.exists():
        return []
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


def list_reviewable_examples(
    project: str, projects_root: str | Path = "projects"
) -> list[SyntheticExample]:
    """Return every ``verified: false`` example, in file order.

    Returns an empty list if ``synthetic.jsonl`` doesn't exist yet.
    """
    raw = _read_raw(project, projects_root)
    return [e for e in (SyntheticExample(**line) for line in raw) if not e.verified]


def approve_examples(
    project: str,
    projects_root: str | Path = "projects",
    decisions: dict[str, dict] | None = None,
) -> list[SyntheticExample]:
    """Apply approval decisions to ``synthetic.jsonl`` and write the result back.

    Args:
        project: Project name — folder name under ``projects_root``.
        projects_root: Parent directory containing project folders.
        decisions: See :func:`apply_approvals`. ``None``/``{}`` is a no-op
            (file is still re-validated and re-written unchanged).

    Returns:
        list[SyntheticExample]: The updated, validated examples, in file order.

    Raises:
        FileNotFoundError: If ``synthetic.jsonl`` doesn't exist yet — run
            ``medallion examples generate`` first.
        ValueError: If a decision names an unknown example, or the resulting
            data fails schema validation.
    """
    path = Path(projects_root) / project / "examples" / "synthetic.jsonl"
    if not path.exists():
        raise FileNotFoundError(
            f"No synthetic.jsonl found for project '{project}' at {path}\n"
            f"Run:  medallion examples generate {project}"
        )

    with open(path) as f:
        raw = [json.loads(line) for line in f if line.strip()]

    updated  = apply_approvals(raw, decisions or {})
    examples = [SyntheticExample(**entry) for entry in updated]

    with open(path, "w") as f:
        for example in examples:
            f.write(example.model_dump_json() + "\n")

    return examples
