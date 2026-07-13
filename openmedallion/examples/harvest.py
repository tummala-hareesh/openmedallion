"""examples/harvest.py — promote harvested examples + list failures for review
(RAG roadmap Phase 3, build order step 14 — the final step of the roadmap).

Locked design decisions:
- ``harvest_candidates()`` promotes ``harvested.jsonl``'s ``status: candidate``
  entries into ``synthetic.jsonl`` as ``verified: false`` — still needs a
  human pass via the existing ``medallion examples approve``, since a
  thumbs-up means "this looked right to a user", not "the SQL is definitely
  correct". Promoted entries are marked ``status: "harvested"`` (not
  deleted), so re-running harvest never promotes the same entry twice, and
  candidates whose ``(question, sql)`` already exists in ``synthetic.jsonl``
  are skipped (still marked harvested — they were processed either way).
- ``list_failures()`` is a plain, honest listing of ``failures.jsonl`` — no
  fabricated "error type" grouping, since ``failures.jsonl`` (already
  shipped) has no reason/error-type field to group by.
"""
from __future__ import annotations

import json
from pathlib import Path

from openmedallion.examples.approve import content_hash
from openmedallion.examples.feedback import FailureRecord, HarvestedCandidate
from openmedallion.examples.schema import SyntheticExample


def harvest_candidates(
    project: str, projects_root: str | Path = "projects"
) -> list[SyntheticExample]:
    """Promote harvested.jsonl candidates into synthetic.jsonl as unverified.

    Args:
        project: Project name — folder name under ``projects_root``.
        projects_root: Parent directory containing project folders.

    Returns:
        list[SyntheticExample]: The newly-promoted examples (empty if there
        were no ``status: candidate`` entries, or all were duplicates of
        existing ``synthetic.jsonl`` entries). Returns an empty list if
        ``harvested.jsonl`` doesn't exist yet.
    """
    examples_dir   = Path(projects_root) / project / "examples"
    harvested_path = examples_dir / "harvested.jsonl"
    synthetic_path = examples_dir / "synthetic.jsonl"

    if not harvested_path.exists():
        return []

    with open(harvested_path) as f:
        harvested_raw = [json.loads(line) for line in f if line.strip()]

    existing: list[SyntheticExample] = []
    if synthetic_path.exists():
        with open(synthetic_path) as f:
            existing = [SyntheticExample(**json.loads(line)) for line in f if line.strip()]

    known_hashes = {content_hash(e.question, e.sql) for e in existing}

    promoted: list[SyntheticExample] = []
    updated_harvested: list[dict] = []
    for raw in harvested_raw:
        candidate = HarvestedCandidate(**raw)
        if candidate.status != "candidate":
            updated_harvested.append(raw)
            continue

        h = content_hash(candidate.question, candidate.sql)
        if h not in known_hashes:
            example = SyntheticExample(question=candidate.question, sql=candidate.sql, verified=False)
            promoted.append(example)
            known_hashes.add(h)
        updated_harvested.append({**raw, "status": "harvested"})

    if promoted:
        synthetic_path.parent.mkdir(parents=True, exist_ok=True)
        with open(synthetic_path, "w") as f:
            for example in existing + promoted:
                f.write(example.model_dump_json() + "\n")

    if any(HarvestedCandidate(**raw).status == "candidate" for raw in harvested_raw):
        with open(harvested_path, "w") as f:
            for raw in updated_harvested:
                f.write(json.dumps(raw) + "\n")

    return promoted


def list_failures(project: str, projects_root: str | Path = "projects") -> list[FailureRecord]:
    """Return every entry in failures.jsonl, in file order.

    A plain listing — deliberately not grouped by "error type", since
    ``failures.jsonl`` has no reason/error-type field to group by (thumbs-down
    feedback doesn't currently capture *why*). Never modifies the file.

    Returns an empty list if ``failures.jsonl`` doesn't exist yet.
    """
    path = Path(projects_root) / project / "examples" / "failures.jsonl"
    if not path.exists():
        return []

    with open(path) as f:
        return [FailureRecord(**json.loads(line)) for line in f if line.strip()]
