"""cerebrum/pipeline.py — Orchestrate the full cerebrum RAG pipeline.

Flow:
    build_schema_context
        → build_prompt
        → LLMClient        (first call — generates SQL)
        → validate_and_fix (allowlist + DuckDB EXPLAIN, up to 2 retries)
        → execute          (DuckDB → Polars DataFrame)
        → recommend        (second call — canonical question string)
"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from openmedallion.cerebrum import executor as _executor
from openmedallion.cerebrum import llm as _llm
from openmedallion.cerebrum import prompt as _prompt
from openmedallion.cerebrum import recommender as _recommender
from openmedallion.cerebrum import schema as _schema
from openmedallion.cerebrum import validator as _validator


@dataclass
class QueryResult:
    """The complete output of one :meth:`CerebrumPipeline.ask` call."""

    question:           str
    sql:                str
    result:             pl.DataFrame
    recommended_prompt: str


class CerebrumPipeline:
    """End-to-end LLM-powered natural-language query pipeline.

    Parameters
    ----------
    silver_dir:
        Path to the silver layer containing the Parquet tables.
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
    """

    def __init__(
        self,
        silver_dir: str | Path,
        *,
        model: str = "llama3.2",
        provider: str = "ollama",
        api_key: str | None = None,
        base_url: str | None = None,
        _client: Callable[[str], str] | None = None,
    ) -> None:
        self._silver_dir = Path(silver_dir)
        self._client = _client or _llm.get_client(
            provider, model, api_key=api_key, base_url=base_url
        )

    # ── internal ─────────────────────────────────────────────────────────────

    def _llm_call(self, prompt_text: str) -> str:
        return self._client(prompt_text)

    # ── public API ────────────────────────────────────────────────────────────

    def ask(
        self,
        question: str,
        on_step: Callable[[str], None] | None = None,
    ) -> QueryResult:
        """Run the full pipeline for *question* and return a :class:`QueryResult`.

        Parameters
        ----------
        question:
            Natural-language question to answer.
        on_step:
            Optional callback invoked with a short status string before each
            blocking operation (LLM calls, SQL validation, query execution).
            Useful for displaying progress in CLI or UI contexts.

        Steps
        -----
        1. Build schema context from silver Parquet files.
        2. Build full LLM prompt (system + schema + few-shot + question).
        3. First LLM call — generates raw SQL.
        4. Validate SQL; retry up to 2×  on failure (each retry = one LLM call).
        5. Execute validated SQL → Polars DataFrame.
        6. Second LLM call — generate canonical reproducible prompt.
        """
        _notify = on_step or (lambda _: None)

        _notify("building schema context")
        schema_ctx  = _schema.build_schema_context(self._silver_dir)
        full_prompt = _prompt.build_prompt(schema_ctx, question)

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

        _notify("generating recommended prompt")
        recommended = _recommender.recommend(question, sql, result, llm_fn=self._llm_call)

        return QueryResult(
            question=question,
            sql=sql,
            result=result,
            recommended_prompt=recommended,
        )
