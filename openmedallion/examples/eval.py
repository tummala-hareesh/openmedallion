"""examples/eval.py — `medallion examples eval`: admin regression check.

Maps onto the RAG roadmap's flagged-but-unbuilt "RAG eval set" idea
(CLAUDE.md: "10-20 hand-verified Q->SQL pairs ... doubles as a manual
regression check when cerebrum internals change"), reframed per user request
as an admin-triggered command: after curating metadata/relationships/
examples, re-run every `verified: true` synthetic.jsonl question through the
current pipeline and check whether the answer still matches.

No real model retraining exists anywhere in this project — "eval" checks
whether a *curation* change (new metadata, new examples, new relationships)
improved or regressed answers, not whether an LLM's weights changed.

Locked: compare EXECUTED RESULTS, not raw SQL text — syntactically different
SQL (whitespace, `count` vs `COUNT`, column order) can be semantically
identical, and comparing text would produce false regressions.
"""
from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from openmedallion.cerebrum.executor import execute
from openmedallion.cerebrum.pipeline import CerebrumPipeline
from openmedallion.cerebrum.prompt import build_prompt
from openmedallion.cerebrum.retrieval import EmbedFn, load_verified_examples
from openmedallion.cerebrum.schema import build_schema_context

_FENCE_PATTERN = re.compile(r"^```(?:sql)?\s*|\s*```$", re.IGNORECASE | re.MULTILINE)


@dataclass
class EvalResult:
    question:   str
    golden_sql: str
    new_sql:    str
    match:      bool
    error:      str = ""
    templated_sql:   str = ""
    """Template-Routed Query Layer roadmap (see CLAUDE.md), build order step 7.
    Set only when run_eval(use_templates=True) — the SQL CerebrumPipeline
    produced when routed with use_templates=True (may equal ``new_sql`` if no
    template matched, or come from a filled template on a match)."""
    templated_match: bool | None = None
    """None when use_templates=False (not run). Otherwise whether the
    use_templates=True pipeline's result matched the golden SQL's result —
    the actual regression signal: did enabling template routing change the
    outcome for this question?"""
    templated_error: str = ""


def _clean_sql(raw: str) -> str:
    return _FENCE_PATTERN.sub("", raw.strip()).strip()


def _results_equal(a, b) -> bool:
    """Compare two Polars DataFrames by value, ignoring row/column order —
    a query re-ordered by a different ORDER BY (or none) shouldn't count as
    a mismatch if the underlying rows are identical."""
    if a.shape != b.shape:
        return False
    if sorted(a.columns) != sorted(b.columns):
        return False
    a_sorted = a.select(sorted(a.columns)).sort(sorted(a.columns))
    b_sorted = b.select(sorted(b.columns)).sort(sorted(b.columns))
    return a_sorted.equals(b_sorted)


def run_eval(
    silver_dir: str | Path,
    examples_dir: str | Path,
    *,
    model: str = "llama3.2",
    provider: str = "ollama",
    api_key: str | None = None,
    base_url: str | None = None,
    _client: Callable[[str], str] | None = None,
    on_step: Callable[[str], None] | None = None,
    use_templates: bool = False,
    _embed_fn: EmbedFn | None = None,
) -> list[EvalResult]:
    """Re-run every `verified: true` synthetic.jsonl question through the
    current pipeline and compare its result against the stored golden SQL's
    result (executed, not text-compared).

    Args:
        silver_dir: Directory of silver Parquet files.
        examples_dir: Directory containing synthetic.jsonl.
        model: LLM model identifier (used only if ``_client`` isn't given).
        provider: LLM backend — ``"ollama"`` (default), ``"openrouter"``,
            ``"openai"``, or any custom label with a matching ``base_url``.
        api_key: API key for non-Ollama providers.
        base_url: Override the provider's default endpoint URL.
        _client: Inject a pre-built callable (``prompt -> raw_llm_text``),
            bypassing the factory. Intended for testing only — matches
            ``metadata/generator.py``'s ``_client=`` pattern.
        on_step: Optional progress callback.
        use_templates: Template-Routed Query Layer roadmap (see CLAUDE.md),
            build order step 7. When ``True``, each verified question is
            *additionally* run through a
            ``CerebrumPipeline(use_templates=True)`` (reusing the same
            ``_client``) and its result compared against the golden SQL's
            result — populating ``EvalResult.templated_sql``/
            ``templated_match``/``templated_error``. Off by default — zero
            extra cost/calls when omitted, matching the rest of this
            roadmap's "zero cost by default" convention. A pipeline
            exception (e.g. exhausted validate_and_fix retries) is reported
            via ``templated_error``, never raised — one bad example must
            never abort the whole eval run.
        _embed_fn: Inject a pre-built embedding function for the
            ``use_templates=True`` pipeline runs, bypassing the ChromaDB-backed
            factory. Testing only — matches ``CerebrumPipeline``'s own
            ``_embed_fn=`` parameter.

    Returns:
        One EvalResult per verified example, in file order. Empty list if
        synthetic.jsonl doesn't exist or has no verified examples.
    """
    examples = load_verified_examples(examples_dir)
    if not examples:
        return []

    if _client is None:
        from openmedallion.cerebrum import llm as _llm
        _client = _llm.get_client(provider, model, api_key=api_key, base_url=base_url)

    schema_context = build_schema_context(silver_dir)
    results: list[EvalResult] = []

    for ex in examples:
        question, golden_sql = ex["question"], ex["sql"]
        if on_step:
            on_step(f"examples eval: {question!r} — re-running")

        prompt = build_prompt(schema_context, question)
        new_sql = _clean_sql(_client(prompt))

        try:
            golden_df = execute(golden_sql, silver_dir)
            new_df    = execute(new_sql, silver_dir)
            match     = _results_equal(golden_df, new_df)
            error     = ""
        except Exception as exc:
            match = False
            error = str(exc)

        templated_sql = ""
        templated_match: bool | None = None
        templated_error = ""
        if use_templates:
            if on_step:
                on_step(f"examples eval: {question!r} — re-running with use_templates=True")
            try:
                templated_pipeline = CerebrumPipeline(
                    silver_dir, examples_dir=examples_dir, _client=_client,
                    use_templates=True, _embed_fn=_embed_fn,
                )
                qr = templated_pipeline.ask(question)
                templated_sql = qr.sql
                golden_df_for_compare = execute(golden_sql, silver_dir)
                templated_match = _results_equal(golden_df_for_compare, qr.result)
            except Exception as exc:
                templated_match = False
                templated_error = str(exc)

        results.append(EvalResult(
            question=question, golden_sql=golden_sql, new_sql=new_sql,
            match=match, error=error,
            templated_sql=templated_sql, templated_match=templated_match,
            templated_error=templated_error,
        ))

    return results
