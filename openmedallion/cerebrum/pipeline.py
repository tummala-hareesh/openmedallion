"""cerebrum/pipeline.py — Orchestrate the full cerebrum RAG pipeline.

Flow:
    build_schema_context
        → build_prompt
        → llm.query          (first LLM call — generates SQL)
        → validate_and_fix   (allowlist + DuckDB EXPLAIN, up to 2 retries)
        → execute            (DuckDB → Polars DataFrame)
        → recommend          (second LLM call — canonical question string)
"""
from __future__ import annotations

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
        Ollama model tag (default: ``"llama3.2"``).
    ollama_base_url:
        Base URL of the Ollama server.
    """

    def __init__(
        self,
        silver_dir: str | Path,
        *,
        model: str = "llama3.2",
        ollama_base_url: str = "http://localhost:11434",
    ) -> None:
        self._silver_dir = Path(silver_dir)
        self._model      = model
        self._base_url   = ollama_base_url

    # ── internal ─────────────────────────────────────────────────────────────

    def _llm_call(self, prompt_text: str) -> str:
        return _llm.query(prompt_text, model=self._model, base_url=self._base_url)

    # ── public API ────────────────────────────────────────────────────────────

    def ask(self, question: str) -> QueryResult:
        """Run the full pipeline for *question* and return a :class:`QueryResult`.

        Steps
        -----
        1. Build schema context from silver Parquet files.
        2. Build full LLM prompt (system + schema + few-shot + question).
        3. First LLM call — generates raw SQL.
        4. Validate SQL; retry up to 2×  on failure.
        5. Execute validated SQL → Polars DataFrame.
        6. Second LLM call — generate canonical reproducible prompt.
        """
        schema_ctx   = _schema.build_schema_context(self._silver_dir)
        full_prompt  = _prompt.build_prompt(schema_ctx, question)
        raw_sql      = self._llm_call(full_prompt)

        sql = _validator.validate_and_fix(
            raw_sql,
            silver_dir=self._silver_dir,
            llm_retry_fn=self._llm_call,
            original_prompt=question,
        )

        result      = _executor.execute(sql, self._silver_dir)
        recommended = _recommender.recommend(question, sql, result, llm_fn=self._llm_call)

        return QueryResult(
            question=question,
            sql=sql,
            result=result,
            recommended_prompt=recommended,
        )
