"""cerebrum/pipeline.py — Orchestrate the full cerebrum RAG pipeline.

Flow:
    build_schema_context
        → build_prompt
        → LLMClient        (first call — generates SQL)
        → validate_and_fix (allowlist + DuckDB EXPLAIN, up to 2 retries)
        → execute          (DuckDB → Polars DataFrame)
        → recommend        (second call — canonical question string)

Ambiguity detection + query decomposition (RAG roadmap Phase 2, build order
step 12) are **opt-in** (``detect_ambiguity``/``decompose_queries``
constructor flags, both off by default) — matches the locked "zero cost by
default" philosophy: neither silently adds an LLM call to every query for
existing callers. When ``detect_ambiguity=True`` and the question is
ambiguous, :meth:`CerebrumPipeline.ask` raises :class:`AmbiguousQuestionError`
*before* generating any SQL — this is a deliberate interface change (unlike
steps 9-11, which were all additive optional params): callers must add a
``except AmbiguousQuestionError`` clause to handle it, matching the existing
pattern of specific except clauses in ``cli/main.py:cmd_query()``. When
``decompose_queries=True`` and the question contains multiple independent
sub-questions, ``ask()`` returns a :class:`MultiQueryResult` instead of a
:class:`QueryResult` — each sub-question runs through the full single-question
pipeline independently; there is **no attempt to merge results into one
DataFrame** (no well-defined general solution when sub-questions have
different schemas/grains — left as a presentation/synthesis concern for a
caller such as cortex, not core pipeline logic).
"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from openmedallion.cerebrum import decomposition as _decomposition
from openmedallion.cerebrum import executor as _executor
from openmedallion.cerebrum import llm as _llm
from openmedallion.cerebrum import prompt as _prompt
from openmedallion.cerebrum import recommender as _recommender
from openmedallion.cerebrum import retrieval as _retrieval
from openmedallion.cerebrum import schema as _schema
from openmedallion.cerebrum import validator as _validator
from openmedallion.metadata.schema import MetadataConfig


@dataclass
class QueryResult:
    """The complete output of one single-question pipeline run."""

    question:           str
    sql:                str
    result:             pl.DataFrame
    recommended_prompt: str


@dataclass
class MultiQueryResult:
    """The output of :meth:`CerebrumPipeline.ask` when ``decompose_queries=True``
    and *question* decomposed into multiple independent sub-questions."""

    question:      str
    sub_questions: list[str]
    results:       list[QueryResult]


class AmbiguousQuestionError(Exception):
    """Raised by :meth:`CerebrumPipeline.ask` when ``detect_ambiguity=True``
    and the question is ambiguous — callers should surface ``clarification``
    to the user instead of treating this as a normal failure."""

    def __init__(self, question: str, clarification: str) -> None:
        self.question      = question
        self.clarification = clarification
        super().__init__(f"Ambiguous question: {clarification}")


class CerebrumPipeline:
    """End-to-end LLM-powered natural-language query pipeline.

    Parameters
    ----------
    silver_dir:
        Path to the silver layer containing the Parquet tables.
    examples_dir:
        Path to a project's ``examples/`` directory (containing
        ``synthetic.jsonl``), enabling dynamic few-shot retrieval (build
        order step 9). When omitted, ``build_prompt()`` falls back to its
        static built-in few-shot list unchanged — this is fully optional.
    metadata:
        A project's loaded metadata (``metadata.load_metadata()``), enabling
        schema pruning (build order step 10) — only the top-k tables most
        relevant to the current question are shown to the LLM, instead of
        every silver table's full DDL. When omitted, or when there are no
        ``status: approved`` silver-layer tables, falls back to showing
        every table unchanged — this is fully optional.
    model:
        Model identifier passed to the LLM provider (e.g. ``"llama3.2"``,
        ``"openai/gpt-4o"``, ``"mistral"``).
    provider:
        LLM backend: ``"ollama"`` (default), ``"openrouter"``, ``"openai"``,
        or any custom label with a matching ``base_url``.
    api_key:
        API key for non-Ollama providers.  Falls back to
        ``MEDALLION_LLM_API_KEY`` env var / ``settings.yaml``.
    base_url:
        Override the provider's default endpoint URL (e.g. a self-hosted
        OpenAI-compatible server such as LM Studio or vLLM).
    _client:
        Inject a pre-built :class:`~openmedallion.cerebrum.llm.LLMClient`
        directly, bypassing the factory.  Intended for testing only.
    _embed_fn:
        Inject a pre-built embedding function, bypassing the ChromaDB-backed
        factory (:func:`~openmedallion.cerebrum.retrieval.get_embed_fn`).
        Intended for testing only.
    detect_ambiguity:
        When ``True``, ``ask()`` makes one extra LLM call before generating
        SQL to check whether the question is ambiguous; if so, raises
        :class:`AmbiguousQuestionError` instead of proceeding. Off by
        default — opt-in, matches "zero cost by default".
    decompose_queries:
        When ``True``, ``ask()`` makes one extra LLM call to check whether
        the question contains multiple independent sub-questions; if so,
        returns a :class:`MultiQueryResult` (each sub-question run
        independently) instead of a :class:`QueryResult`. Off by default —
        opt-in, matches "zero cost by default".
    """

    def __init__(
        self,
        silver_dir: str | Path,
        *,
        examples_dir: str | Path | None = None,
        metadata: MetadataConfig | None = None,
        model: str = "llama3.2",
        provider: str = "ollama",
        api_key: str | None = None,
        base_url: str | None = None,
        _client: Callable[[str], str] | None = None,
        _embed_fn: _retrieval.EmbedFn | None = None,
        detect_ambiguity: bool = False,
        decompose_queries: bool = False,
    ) -> None:
        self._silver_dir   = Path(silver_dir)
        self._examples_dir = Path(examples_dir) if examples_dir else None
        self._metadata      = metadata
        self._client = _client or _llm.get_client(
            provider, model, api_key=api_key, base_url=base_url
        )
        self._embed_fn = _embed_fn
        # Sentinel: None = not yet attempted; [] = attempted, nothing to embed.
        self._embedded_examples: list[tuple[dict[str, str], list[float]]] | None = None
        self._embedded_tables: list[tuple[str, list[float]]] | None = None
        # Confidence-gated fallback corpus: every silver table's raw DDL,
        # regardless of metadata/approval status. Only built the first time
        # structured (approved-only) confidence falls below the threshold.
        self._embedded_fallback_tables: list[tuple[str, list[float]]] | None = None
        self._detect_ambiguity  = detect_ambiguity
        self._decompose_queries = decompose_queries

    # ── internal ─────────────────────────────────────────────────────────────

    def _llm_call(self, prompt_text: str) -> str:
        return self._client(prompt_text)

    def _get_few_shot(self, question: str) -> list[dict[str, str]] | None:
        """Return dynamically-retrieved few-shot examples, or None to fall
        back to the static list (no examples_dir, or no verified examples)."""
        if self._examples_dir is None:
            return None

        if self._embedded_examples is None:
            examples = _retrieval.load_verified_examples(self._examples_dir)
            if not examples:
                self._embedded_examples = []
            else:
                self._embed_fn = self._embed_fn or _retrieval.get_embed_fn()
                self._embedded_examples = _retrieval.embed_examples(examples, self._embed_fn)

        if not self._embedded_examples:
            return None

        return _retrieval.rank_examples(question, self._embedded_examples, self._embed_fn, top_k=3)

    def _get_relevant_tables(
        self, question: str, on_step: Callable[[str], None] | None = None
    ) -> list[str] | None:
        """Return pruned table names for build_schema_context(), or None to
        fall back to showing every table (no metadata at all — the caller
        never opted into metadata-based features, so this stays zero-cost).

        When metadata *is* provided but the curated (approved-only) corpus's
        top-1 similarity to *question* falls below
        ``retrieval.CONFIDENCE_THRESHOLD`` (including the "no approved tables
        yet" case, confidence 0.0), falls back to a raw-schema search over
        every silver table's DDL — no metadata/approval status required.
        """
        _notify = on_step or (lambda _: None)

        if self._metadata is None:
            return None

        if self._embedded_tables is None:
            approved = [
                (name, table) for name, table in self._metadata.tables.items()
                if table.status == "approved" and table.layer == "silver"
            ]
            if not approved:
                self._embedded_tables = []
            else:
                self._embed_fn = self._embed_fn or _retrieval.get_embed_fn()
                self._embedded_tables = _retrieval.embed_payloads(
                    approved, lambda p: _schema._table_text(*p), self._embed_fn
                )

        confidence = 0.0
        if self._embedded_tables:
            self._embed_fn = self._embed_fn or _retrieval.get_embed_fn()
            ranked = _retrieval.rank_payloads_scored(
                question, self._embedded_tables, self._embed_fn, top_k=5
            )
            confidence = ranked[0][1] if ranked else 0.0
            if confidence >= _retrieval.CONFIDENCE_THRESHOLD:
                return [name for (name, _table), _score in ranked]

        _notify(
            f"schema confidence {confidence:.2f} below threshold "
            f"{_retrieval.CONFIDENCE_THRESHOLD} — using raw-schema fallback search"
        )
        return self._get_fallback_tables(question)

    def _get_fallback_tables(self, question: str) -> list[str] | None:
        """Rank every silver table's raw DDL (any status, no metadata.yaml
        required) — the confidence-gated fallback corpus."""
        if self._embedded_fallback_tables is None:
            raw = _schema.describe_all_tables(self._silver_dir)
            if not raw:
                self._embedded_fallback_tables = []
            else:
                self._embed_fn = self._embed_fn or _retrieval.get_embed_fn()
                self._embedded_fallback_tables = _retrieval.embed_payloads(
                    raw, lambda p: _schema._raw_table_text(*p), self._embed_fn
                )

        if not self._embedded_fallback_tables:
            return None

        ranked = _retrieval.rank_payloads(question, self._embedded_fallback_tables, self._embed_fn, top_k=5)
        return [name for name, _columns in ranked]

    def _ask_single(
        self,
        question: str,
        on_step: Callable[[str], None] | None = None,
    ) -> QueryResult:
        """Run the single-question pipeline (no ambiguity/decomposition checks).

        Steps
        -----
        1. Build schema context from silver Parquet files.
        2. Build full LLM prompt (system + schema + few-shot + question).
        3. First LLM call — generates raw SQL.
        4. Validate SQL; retry up to 2×  on failure (each retry = one LLM call).
        5. Execute validated SQL → Polars DataFrame.
        6. If the result is empty, one extra LLM call asks it to confirm or
           fix the SQL (see :func:`~cerebrum.validator.check_result_sanity`)
           — skipped entirely for non-empty results.
        7. Final LLM call — generate canonical reproducible prompt.
        """
        _notify = on_step or (lambda _: None)

        _notify("building schema context")
        relevant_tables = self._get_relevant_tables(question, on_step=on_step)
        schema_ctx  = _schema.build_schema_context(self._silver_dir, tables=relevant_tables)
        few_shot    = self._get_few_shot(question)
        full_prompt = _prompt.build_prompt(schema_ctx, question, few_shot=few_shot)

        _notify("generating SQL")
        raw_sql = self._llm_call(full_prompt)

        retry_count = 0

        def _llm_retry(prompt: str) -> str:
            nonlocal retry_count
            retry_count += 1
            _notify(f"SQL invalid — retrying ({retry_count}/2)")
            return self._llm_call(prompt)

        _notify("validating SQL")
        sql = _validator.validate_and_fix(
            raw_sql,
            silver_dir=self._silver_dir,
            llm_retry_fn=_llm_retry,
            original_prompt=question,
        )

        _notify("executing query")
        result = _executor.execute(sql, self._silver_dir)

        if len(result) == 0:
            _notify("result is empty — asking LLM to double-check")
            sql, result = _validator.check_result_sanity(
                question, sql, result, self._silver_dir, self._llm_call
            )

        _notify("generating recommended prompt")
        recommended = _recommender.recommend(question, sql, result, llm_fn=self._llm_call)

        return QueryResult(
            question=question,
            sql=sql,
            result=result,
            recommended_prompt=recommended,
        )

    # ── public API ────────────────────────────────────────────────────────────

    def ask(
        self,
        question: str,
        on_step: Callable[[str], None] | None = None,
    ) -> QueryResult | MultiQueryResult:
        """Run the full pipeline for *question*.

        Parameters
        ----------
        question:
            Natural-language question to answer.
        on_step:
            Optional callback invoked with a short status string before each
            blocking operation (LLM calls, SQL validation, query execution).
            Useful for displaying progress in CLI or UI contexts.

        Returns
        -------
        QueryResult | MultiQueryResult
            A single :class:`QueryResult` by default. If ``decompose_queries``
            was set and *question* decomposes into multiple independent
            sub-questions, a :class:`MultiQueryResult` instead.

        Raises
        ------
        AmbiguousQuestionError
            If ``detect_ambiguity`` was set and *question* is ambiguous —
            raised before any SQL is generated.
        """
        _notify = on_step or (lambda _: None)

        if self._detect_ambiguity:
            _notify("checking for ambiguity")
            relevant_tables = self._get_relevant_tables(question, on_step=on_step)
            schema_ctx = _schema.build_schema_context(self._silver_dir, tables=relevant_tables)
            clarification = _decomposition.detect_ambiguity(question, schema_ctx, self._llm_call)
            if clarification:
                raise AmbiguousQuestionError(question, clarification)

        if self._decompose_queries:
            _notify("checking if question decomposes")
            sub_questions = _decomposition.decompose_question(question, self._llm_call)
            if len(sub_questions) > 1:
                results = [self._ask_single(q, on_step) for q in sub_questions]
                return MultiQueryResult(question=question, sub_questions=sub_questions, results=results)

        return self._ask_single(question, on_step)
