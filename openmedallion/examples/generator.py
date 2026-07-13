"""examples/generator.py — LLM-drafted synthetic Q->SQL pairs (RAG roadmap
Phase 1, build order step 7).

Reads ``status: approved`` **silver-layer** tables from ``metadata.yaml``
(via the merged ``load_metadata()`` — unlike ``metadata/generator.py``/
``relationships/generator.py``, this module only *reads* the approved
knowledge, it never writes back to ``metadata.yaml``/``relationships.yaml``,
so there's no "never touch approved" concern here) and approved
relationships between them, asks the LLM in a single call for a batch of
representative ``(question, sql)`` pairs, validates each SQL via
``cerebrum/validator.py``'s ``validate_and_fix()`` (SQL allowlist + DuckDB
``EXPLAIN`` dry-run, with retry-on-error), and writes the result to
``<project>/examples/synthetic.jsonl``.

Locked design decisions
------------------------
- **Silver-layer tables only for v1** — cerebrum's runtime (``schema.py``,
  ``validator.py``, ``executor.py``) doesn't register gold Parquet as DuckDB
  views yet, so gold-referencing SQL would never validate. Revisit once
  Phase 2 extends cerebrum's runtime to gold.
- **One LLM call for the whole batch**, not one call per example — same
  retry-on-invalid-JSON pattern as ``metadata/generator.py``.
- An example whose SQL never validates (even after retry) is **dropped**,
  not written with an error flag — an unusable example has no review value.
- On regeneration, existing ``verified: true`` entries are **kept
  untouched**; ``verified: false`` entries are **replaced** by a fresh batch
  — same "keep approved, replace draft" policy as
  ``metadata.yaml``/``relationships.yaml``.
"""
from __future__ import annotations

import json
import re
from collections.abc import Callable
from pathlib import Path

from openmedallion.cerebrum import llm as _llm
from openmedallion.cerebrum import validator as _validator
from openmedallion.config.loader import load_project
from openmedallion.examples.schema import SyntheticExample
from openmedallion.metadata.loader import load_metadata
from openmedallion.metadata.schema import TableMeta
from openmedallion.relationships.loader import load_relationships
from openmedallion.relationships.schema import RelationshipEntry

_MAX_RETRIES   = 2
_FENCE_PATTERN = re.compile(r"^```(?:json)?\s*|\s*```$", re.IGNORECASE)


def _build_prompt(tables: dict[str, TableMeta], relationships: list[RelationshipEntry], count: int) -> str:
    lines = ["Tables available:"]
    for name, table in sorted(tables.items()):
        lines.append(f"- {name}: {table.description or ''}")
        if table.synonyms:
            lines.append(f"  synonyms: {', '.join(table.synonyms)}")
        for col_name, col in table.columns.items():
            lines.append(f"  - {col_name}: {col.description or ''}")

    if relationships:
        lines.append("\nRelationships:")
        for rel in relationships:
            lines.append(f"- {rel.from_table} -> {rel.to_table} on {', '.join(rel.join_on)}")

    schema_block = "\n".join(lines)

    return f"""You are generating training examples for a natural-language-to-SQL assistant.

{schema_block}

Generate {count} realistic, diverse (question, sql) pairs a business user might
ask, covering aggregations, filters, time-based questions, rankings, and joins
across the relationships above where relevant. SQL must be valid DuckDB SELECT
statements querying only the tables listed above — no other tables.

Respond with ONLY a valid JSON array, no markdown code fences, in this exact shape:
[
  {{"question": "...", "sql": "SELECT ..."}}
]"""


def _parse_batch_json(raw: str) -> list[dict]:
    text = _FENCE_PATTERN.sub("", raw.strip()).strip()
    parsed = json.loads(text)
    if not isinstance(parsed, list):
        raise ValueError("expected a JSON array")
    return parsed


def _draft_batch(
    client: Callable[[str], str],
    prompt: str,
    on_step: Callable[[str], None] | None,
) -> list[dict]:
    last_error: str | None = None
    for attempt in range(_MAX_RETRIES + 1):
        p = prompt if last_error is None else (
            f"{prompt}\n\nYour previous response was invalid JSON ({last_error}). "
            "Return ONLY a valid JSON array, nothing else."
        )
        raw = client(p)
        try:
            return _parse_batch_json(raw)
        except (json.JSONDecodeError, ValueError) as exc:
            last_error = str(exc)
            if on_step:
                on_step(f"examples generate: invalid JSON, retry {attempt + 1}/{_MAX_RETRIES}")

    raise ValueError(
        f"[examples] LLM did not return a valid JSON array after {_MAX_RETRIES} retries: {last_error}"
    )


def generate_examples(
    project: str,
    projects_root: str | Path = "projects",
    *,
    count: int = 20,
    model: str = "llama3.2",
    provider: str = "ollama",
    api_key: str | None = None,
    base_url: str | None = None,
    _client: Callable[[str], str] | None = None,
    on_step: Callable[[str], None] | None = None,
) -> list[SyntheticExample]:
    """Generate synthetic Q->SQL pairs from approved metadata + relationships.

    Args:
        project: Project name — folder name under ``projects_root``.
        projects_root: Parent directory containing project folders.
        count: Target number of (question, sql) pairs to request.
        model: LLM model identifier.
        provider: LLM backend — ``"ollama"`` (default), ``"openrouter"``,
            ``"openai"``, or any custom label with a matching ``base_url``.
        api_key: API key for non-Ollama providers.
        base_url: Override the provider's default endpoint URL.
        _client: Inject a pre-built LLM client directly, bypassing the
            factory. Intended for testing only.
        on_step: Optional callback invoked with progress messages.

    Returns:
        list[SyntheticExample]: The full merged set (preserved ``verified:
        true`` entries + the freshly generated, validated batch), also
        written to ``<project>/examples/synthetic.jsonl``.

    Raises:
        ValueError: If there are no ``status: approved`` silver-layer tables
            in ``metadata.yaml`` (nothing to generate examples against), or
            if the LLM never returns a valid JSON array after retries.
    """
    cfg        = load_project(project, projects_root)
    silver_dir = Path(cfg["paths"]["silver"])

    metadata      = load_metadata(project, projects_root)
    relationships = load_relationships(project, projects_root)

    approved_tables = {
        name: table for name, table in metadata.tables.items()
        if table.status == "approved" and table.layer == "silver"
    }
    if not approved_tables:
        raise ValueError(
            "[examples] no approved silver-layer tables in metadata.yaml — "
            f"run: medallion metadata approve {project}"
        )

    approved_relationships = [
        rel for rel in relationships.relationships
        if rel.status == "approved"
        and rel.from_table in approved_tables and rel.to_table in approved_tables
    ]

    client = _client or _llm.get_client(provider, model, api_key=api_key, base_url=base_url)

    if on_step:
        on_step(
            f"examples generate: {len(approved_tables)} approved table(s), "
            f"{len(approved_relationships)} approved relationship(s)"
        )

    prompt    = _build_prompt(approved_tables, approved_relationships, count)
    raw_pairs = _draft_batch(client, prompt, on_step)

    validated: list[SyntheticExample] = []
    for pair in raw_pairs:
        question, sql = pair.get("question"), pair.get("sql")
        if not question or not sql:
            continue
        try:
            good_sql = _validator.validate_and_fix(sql, silver_dir, client, question, max_retries=2)
        except ValueError:
            if on_step:
                on_step(f"examples generate: dropped invalid example — {question[:60]!r}")
            continue
        validated.append(SyntheticExample(question=question, sql=good_sql, verified=False))
        if on_step:
            on_step(f"examples generate: validated — {question[:60]!r}")

    root           = Path(projects_root) / project
    examples_dir   = root / "examples"
    synthetic_path = examples_dir / "synthetic.jsonl"

    kept: list[SyntheticExample] = []
    if synthetic_path.exists():
        with open(synthetic_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                obj = SyntheticExample(**json.loads(line))
                if obj.verified:
                    kept.append(obj)

    result = kept + validated

    examples_dir.mkdir(parents=True, exist_ok=True)
    with open(synthetic_path, "w") as f:
        for example in result:
            f.write(example.model_dump_json() + "\n")

    if on_step:
        on_step(f"examples generate: {len(validated)} new, {len(kept)} preserved, {len(result)} total")

    return result
