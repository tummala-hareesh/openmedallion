"""metadata/generator.py — LLM-drafted metadata.yaml (RAG roadmap Phase 1, build order step 2).

For every silver + gold Parquet table not already ``status: approved``, this
samples real column dtypes + distinct values via DuckDB (deterministic — no
LLM involved) and asks the LLM for a table-level description + synonyms and
per-column description + synonyms (one LLM call per table). Writes the result
to ``<project>/metadata.yaml``.

``status: approved`` tables are always left completely untouched — the
"auto-generate, then human-approve" workflow pattern (see CLAUDE.md roadmap)
means regenerating must never clobber human-reviewed metadata.
``metadata_enhancements.yaml`` is never written to; it stays a separate
hand-maintained overlay applied only at load time (see ``metadata/loader.py``).
"""
from __future__ import annotations

import json
import re
from collections.abc import Callable
from pathlib import Path

import duckdb
import yaml

from openmedallion.cerebrum import llm as _llm
from openmedallion.config.loader import load_project
from openmedallion.metadata.drift import _schema_fingerprint, detect_drift
from openmedallion.metadata.profiling import profile_columns
from openmedallion.metadata.schema import ColumnMeta, MetadataConfig, TableMeta

_MAX_VALUE_EXAMPLES = 8
_MAX_RETRIES        = 2
_FENCE_PATTERN      = re.compile(r"^```(?:json)?\s*|\s*```$", re.IGNORECASE)


def _describe_table(path: Path) -> tuple[list[tuple[str, str]], dict[str, list]]:
    """Return ``(columns, sampled distinct values per column)`` via DuckDB — no LLM."""
    con = duckdb.connect()
    try:
        table = path.stem
        con.execute(f'CREATE OR REPLACE VIEW "{table}" AS SELECT * FROM read_parquet(\'{path}\')')
        col_info = [(row[0], row[1]) for row in con.execute(f'DESCRIBE "{table}"').fetchall()]
        samples: dict[str, list] = {}
        for name, _ in col_info:
            rows = con.execute(
                f'SELECT DISTINCT "{name}" FROM "{table}" WHERE "{name}" IS NOT NULL LIMIT {_MAX_VALUE_EXAMPLES}'
            ).fetchall()
            samples[name] = [r[0] for r in rows]
        return col_info, samples
    finally:
        con.close()


def _build_prompt(table_name: str, layer: str, col_info: list[tuple[str, str]], samples: dict[str, list]) -> str:
    lines = [f"Table: {table_name} (layer: {layer})", "Columns:"]
    for name, dtype in col_info:
        lines.append(f"  - {name} ({dtype}), sample values: {samples.get(name, [])!r}")
    schema_block = "\n".join(lines)
    column_names = ", ".join(name for name, _ in col_info)

    return f"""You are documenting a data warehouse table for a natural-language query assistant.

{schema_block}

Respond with ONLY valid JSON, no markdown code fences, in this exact shape:
{{
  "description": "one sentence describing what a row in this table represents",
  "synonyms": ["alternative", "terms", "users", "might", "say"],
  "columns": {{
    "<column_name>": {{
      "description": "one sentence describing this column",
      "synonyms": ["alternative", "terms"]
    }}
  }}
}}

Include every one of these columns as a key under "columns": {column_names}.
Keep descriptions concise (one sentence each)."""


def _parse_llm_json(raw: str) -> dict:
    text = _FENCE_PATTERN.sub("", raw.strip()).strip()
    return json.loads(text)


def _draft_table(
    client: Callable[[str], str],
    table_name: str,
    layer: str,
    path: Path,
    on_step: Callable[[str], None] | None,
) -> TableMeta:
    col_info, samples = _describe_table(path)
    prompt = _build_prompt(table_name, layer, col_info, samples)

    if on_step:
        on_step(f"metadata generate: {table_name} — drafting via LLM")

    parsed: dict | None = None
    last_error: str | None = None
    for attempt in range(_MAX_RETRIES + 1):
        p = prompt if last_error is None else (
            f"{prompt}\n\nYour previous response was invalid JSON ({last_error}). "
            "Return ONLY valid JSON, nothing else."
        )
        raw = client(p)
        try:
            parsed = _parse_llm_json(raw)
            break
        except (json.JSONDecodeError, ValueError) as exc:
            last_error = str(exc)
            if on_step:
                on_step(f"metadata generate: {table_name} — invalid JSON, retry {attempt + 1}/{_MAX_RETRIES}")

    if parsed is None:
        raise ValueError(
            f"[metadata] LLM did not return valid JSON for table '{table_name}' "
            f"after {_MAX_RETRIES} retries: {last_error}"
        )

    columns: dict[str, ColumnMeta] = {}
    col_drafts = parsed.get("columns") or {}
    for name, _ in col_info:
        draft = col_drafts.get(name) or {}
        columns[name] = ColumnMeta(
            description=draft.get("description"),
            value_examples=samples.get(name) or None,
            synonyms=draft.get("synonyms") or None,
        )

    return TableMeta(
        layer=layer,
        description=parsed.get("description"),
        status="draft",
        synonyms=parsed.get("synonyms") or None,
        columns=columns,
        schema_hash=_schema_fingerprint(col_info),
    )


def _apply_profiling(
    table: TableMeta,
    path: Path,
    profile_fn: Callable[[Path], dict[str, dict]],
    on_step: Callable[[str], None] | None,
) -> TableMeta:
    """Enrich a freshly-drafted table's columns with dtype/stats/accepted_values.

    Opt-in only (``use_profiling=True``) — never called for approved-and-skipped
    tables. Zero LLM calls: ydata-profiling is deterministic.
    """
    if on_step:
        on_step(f"metadata generate: {table.layer} table at {path.name} — profiling via ydata-profiling")
    profile = profile_fn(path)
    for col_name, col_meta in table.columns.items():
        summary = profile.get(col_name)
        if summary is None:
            continue
        table.columns[col_name] = col_meta.model_copy(update=summary)
    return table


def generate_metadata(
    project: str,
    projects_root: str | Path = "projects",
    *,
    model: str = "llama3.2",
    provider: str = "ollama",
    api_key: str | None = None,
    base_url: str | None = None,
    use_profiling: bool = False,
    _client: Callable[[str], str] | None = None,
    _profile_fn: Callable[[Path], dict[str, dict]] | None = None,
    on_step: Callable[[str], None] | None = None,
) -> MetadataConfig:
    """Draft ``metadata.yaml`` for every silver/gold table not already approved.

    Args:
        project: Project name — folder name under ``projects_root``.
        projects_root: Parent directory containing project folders.
        model: LLM model identifier.
        provider: LLM backend — ``"ollama"`` (default), ``"openrouter"``,
            ``"openai"``, or any custom label with a matching ``base_url``.
        api_key: API key for non-Ollama providers.
        base_url: Override the provider's default endpoint URL.
        use_profiling: Opt-in — when True, every freshly-drafted table's columns
            are additionally enriched with ``dtype``/``stats``/``accepted_values``
            via ydata-profiling (``openmedallion[profile]`` extra required).
            Zero extra LLM calls; approved-and-skipped tables are never profiled.
        _client: Inject a pre-built :class:`~openmedallion.cerebrum.llm.LLMClient`
            directly, bypassing the factory. Intended for testing only.
        _profile_fn: Inject a fake profiling function, bypassing the real
            ydata-profiling call. Intended for testing only.
        on_step: Optional callback invoked with progress messages.

    Returns:
        MetadataConfig: The merged model, also written to
        ``<project>/metadata.yaml``.

    Raises:
        ValueError: If the LLM never returns valid JSON for a table after
            retries, or the final metadata fails schema validation.
        ModuleNotFoundError: If ``use_profiling=True`` and ``ydata_profiling``
            isn't installed.
    """
    cfg        = load_project(project, projects_root)
    silver_dir = Path(cfg["paths"]["silver"])
    gold_dir   = Path(cfg["paths"]["gold"]) / cfg["pipeline"]["name"]

    root      = Path(projects_root) / project
    meta_path = root / "metadata.yaml"
    existing_raw: dict = {}
    if meta_path.exists():
        with open(meta_path) as f:
            existing_raw = yaml.safe_load(f) or {}
    existing_tables: dict = existing_raw.get("tables") or {}

    client     = _client or _llm.get_client(provider, model, api_key=api_key, base_url=base_url)
    profile_fn = _profile_fn or profile_columns

    tables: dict[str, TableMeta] = {}
    for layer, layer_dir in (("silver", silver_dir), ("gold", gold_dir)):
        if not layer_dir.exists():
            continue
        for path in sorted(layer_dir.glob("*.parquet")):
            name = path.stem
            existing_entry = existing_tables.get(name)
            if existing_entry and existing_entry.get("status") == "approved":
                if on_step:
                    on_step(f"metadata generate: {name} — approved, skipped")
                tables[name] = TableMeta(**existing_entry)
                continue
            drafted = _draft_table(client, name, layer, path, on_step)
            if use_profiling:
                drafted = _apply_profiling(drafted, path, profile_fn, on_step)
            tables[name] = drafted

    result = MetadataConfig(tables=tables, glossary=existing_raw.get("glossary"))

    with open(meta_path, "w") as f:
        yaml.safe_dump(result.model_dump(exclude_none=True), f, sort_keys=False)

    return result


def refresh_metadata(
    project: str,
    projects_root: str | Path = "projects",
    *,
    model: str = "llama3.2",
    provider: str = "ollama",
    api_key: str | None = None,
    base_url: str | None = None,
    use_profiling: bool = False,
    _client: Callable[[str], str] | None = None,
    _profile_fn: Callable[[Path], dict[str, dict]] | None = None,
    on_step: Callable[[str], None] | None = None,
) -> MetadataConfig:
    """Detect schema drift and refresh only affected/unapproved tables.

    Locked behavior (see CLAUDE.md roadmap):

    - ``approved`` + drifted -> flipped to ``stale``, ``schema_hash`` updated,
      but NOT re-drafted this run (no LLM call) — "approved is a trust
      boundary" stays strict; a human must explicitly re-run ``refresh`` (now
      picking it up as ``stale``) or ``generate`` to get new LLM content.
    - ``draft``/``stale`` (drifted or not) -> re-drafted via ``_draft_table``,
      same as ``generate_metadata``.
    - ``approved`` + unchanged -> untouched, no LLM call.
    - Table dropped from Parquet -> left in ``metadata.yaml`` untouched,
      flagged via ``on_step``, never silently deleted.

    Args:
        project: Project name — folder name under ``projects_root``.
        projects_root: Parent directory containing project folders.
        model: LLM model identifier.
        provider: LLM backend — ``"ollama"`` (default), ``"openrouter"``,
            ``"openai"``, or any custom label with a matching ``base_url``.
        api_key: API key for non-Ollama providers.
        base_url: Override the provider's default endpoint URL.
        use_profiling: Opt-in — when True, every re-drafted table's columns are
            additionally enriched with ``dtype``/``stats``/``accepted_values``
            via ydata-profiling (``openmedallion[profile]`` extra required).
            Zero extra LLM calls; tables flipped to ``stale`` (not re-drafted)
            or left untouched are never profiled.
        _client: Inject a pre-built :class:`~openmedallion.cerebrum.llm.LLMClient`
            directly, bypassing the factory. Intended for testing only.
        _profile_fn: Inject a fake profiling function, bypassing the real
            ydata-profiling call. Intended for testing only.
        on_step: Optional callback invoked with progress messages.

    Returns:
        MetadataConfig: The merged model, also written to
        ``<project>/metadata.yaml``.
    """
    cfg        = load_project(project, projects_root)
    silver_dir = Path(cfg["paths"]["silver"])
    gold_dir   = Path(cfg["paths"]["gold"]) / cfg["pipeline"]["name"]

    root      = Path(projects_root) / project
    meta_path = root / "metadata.yaml"
    existing_raw: dict = {}
    if meta_path.exists():
        with open(meta_path) as f:
            existing_raw = yaml.safe_load(f) or {}
    existing_tables: dict = existing_raw.get("tables") or {}

    drift, dropped = detect_drift(project, projects_root)
    for name in dropped:
        if on_step:
            on_step(f"metadata refresh: {name} — Parquet file no longer found, left as-is")

    client     = _client or _llm.get_client(provider, model, api_key=api_key, base_url=base_url)
    profile_fn = _profile_fn or profile_columns

    tables: dict[str, TableMeta] = {}
    for name, entry in existing_tables.items():
        if name in dropped:
            tables[name] = TableMeta(**entry)
            continue

        status = entry.get("status")
        if status == "approved" and name in drift:
            if on_step:
                on_step(f"metadata refresh: {name} — schema drift detected, flipped to stale")
            layer = entry.get("layer", "silver")
            layer_dir = silver_dir if layer == "silver" else gold_dir
            live_hash = _schema_fingerprint(_describe_table(layer_dir / f"{name}.parquet")[0])
            tables[name] = TableMeta(**{**entry, "status": "stale", "schema_hash": live_hash})
            continue

        if status == "approved":
            if on_step:
                on_step(f"metadata refresh: {name} — approved, unchanged, skipped")
            tables[name] = TableMeta(**entry)
            continue

        # draft / stale (drifted or not) -> re-drafted
        layer = entry.get("layer", "silver")
        layer_dir = silver_dir if layer == "silver" else gold_dir
        path = layer_dir / f"{name}.parquet"
        if not path.exists():
            # shouldn't happen (detect_drift would have flagged it as dropped),
            # but guard defensively rather than crash mid-refresh.
            tables[name] = TableMeta(**entry)
            continue
        drafted = _draft_table(client, name, layer, path, on_step)
        if use_profiling:
            drafted = _apply_profiling(drafted, path, profile_fn, on_step)
        tables[name] = drafted

    result = MetadataConfig(tables=tables, glossary=existing_raw.get("glossary"))

    with open(meta_path, "w") as f:
        yaml.safe_dump(result.model_dump(exclude_none=True), f, sort_keys=False)

    return result
