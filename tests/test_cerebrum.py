"""tests/test_cerebrum.py — Unit tests for openmedallion.cerebrum.

All tests use real DuckDB against temp Parquet fixtures; LLM calls are
replaced with mock callables so no running server is required.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from openmedallion.cerebrum.executor    import execute
from openmedallion.cerebrum.llm         import (
    LLMClient,
    OllamaClient,
    OpenAICompatibleClient,
    get_client,
)
from openmedallion.cerebrum.pipeline    import (
    AmbiguousQuestionError,
    CerebrumPipeline,
    MultiQueryResult,
    QueryResult,
)
from openmedallion.cerebrum.prompt      import (
    build_prompt,
    build_template_fill_prompt,
    fill_template,
    is_clarification_response,
    parse_template_fill_response,
    system_prompt,
)
from openmedallion.cerebrum.recommender import recommend
from openmedallion.cerebrum.schema      import (
    build_schema_context,
    describe_all_tables,
    rank_relevant_tables,
    rank_relevant_tables_scored,
)
from openmedallion.metadata.schema      import MetadataConfig
from openmedallion.cerebrum.validator   import check_result_sanity, validate, validate_and_fix


# ── fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture()
def silver_dir(tmp_path: Path) -> Path:
    """Tiny silver directory with two Parquet tables."""
    pl.DataFrame({
        "order_id":    [1, 2, 3],
        "customer_id": [10, 11, 12],
        "amount":      [100.0, 200.0, 150.0],
        "region":      ["North", "South", "East"],
    }).write_parquet(tmp_path / "orders.parquet")

    pl.DataFrame({
        "product_id": [101, 102],
        "name":       ["Widget", "Gadget"],
        "price":      [9.99, 19.99],
    }).write_parquet(tmp_path / "products.parquet")

    return tmp_path


# ── schema ────────────────────────────────────────────────────────────────────

class TestBuildSchemaContext:
    def test_returns_string(self, silver_dir):
        ctx = build_schema_context(silver_dir)
        assert isinstance(ctx, str)

    def test_includes_table_names(self, silver_dir):
        ctx = build_schema_context(silver_dir)
        assert "orders" in ctx
        assert "products" in ctx

    def test_includes_column_names(self, silver_dir):
        ctx = build_schema_context(silver_dir)
        assert "order_id" in ctx
        assert "product_id" in ctx

    def test_empty_dir_returns_empty_string(self, tmp_path):
        assert build_schema_context(tmp_path) == ""

    def test_single_table(self, tmp_path):
        pl.DataFrame({"a": [1]}).write_parquet(tmp_path / "t.parquet")
        ctx = build_schema_context(tmp_path)
        assert "TABLE t" in ctx
        assert "a" in ctx

    def test_tables_param_prunes_to_subset(self, silver_dir):
        ctx = build_schema_context(silver_dir, tables=["orders"])
        assert "TABLE orders" in ctx
        assert "TABLE products" not in ctx

    def test_tables_param_none_includes_everything(self, silver_dir):
        ctx = build_schema_context(silver_dir, tables=None)
        assert "TABLE orders" in ctx
        assert "TABLE products" in ctx

    def test_tables_param_ignores_nonexistent_names(self, silver_dir):
        ctx = build_schema_context(silver_dir, tables=["orders", "nonexistent"])
        assert "TABLE orders" in ctx
        assert "nonexistent" not in ctx


class TestDescribeAllTables:
    """describe_all_tables backs the confidence-gated fallback — unlike
    rank_relevant_tables, it needs no metadata.yaml or approval status."""

    def test_returns_every_table_unfiltered(self, silver_dir):
        result = describe_all_tables(silver_dir)
        names = {name for name, _cols in result}
        assert names == {"orders", "products"}

    def test_returns_column_name_dtype_pairs(self, silver_dir):
        result = dict(describe_all_tables(silver_dir))
        col_names = {col for col, _dtype in result["orders"]}
        assert "order_id" in col_names
        assert "customer_id" in col_names

    def test_empty_dir_returns_empty_list(self, tmp_path):
        assert describe_all_tables(tmp_path) == []


# ── schema pruning (build order step 10) ─────────────────────────────────────

class TestRankRelevantTables:

    def _metadata(self, tables: dict) -> MetadataConfig:
        return MetadataConfig(tables=tables)

    def _fake_embed_fn(self, vectors: dict[str, list[float]]):
        # _table_text() prefixes the table name onto the description, so
        # match by substring rather than requiring callers to know the
        # exact concatenated string.
        def _embed(texts: list[str]) -> list[list[float]]:
            result = []
            for text in texts:
                match = next((v for k, v in vectors.items() if k in text), None)
                if match is None:
                    raise KeyError(f"no vector fixture matches text: {text!r}")
                result.append(match)
            return result
        return _embed

    def test_ranks_approved_silver_tables_by_relevance(self):
        metadata = self._metadata({
            "orders": {
                "layer": "silver", "status": "approved",
                "description": "revenue by region", "columns": {},
            },
            "customers": {
                "layer": "silver", "status": "approved",
                "description": "how many customers", "columns": {},
            },
        })
        vectors = {
            "revenue by region":  [1.0, 0.0],
            "how many customers": [0.0, 1.0],
            "total revenue per region": [0.9, 0.1],
        }
        embed_fn = self._fake_embed_fn(vectors)
        result = rank_relevant_tables("total revenue per region", metadata, embed_fn, top_k=1)
        assert result == ["orders"]

    def test_excludes_draft_tables(self):
        metadata = self._metadata({
            "orders": {"layer": "silver", "status": "approved", "description": "d", "columns": {}},
            "customers": {"layer": "silver", "status": "draft", "description": "d", "columns": {}},
        })
        embed_fn = self._fake_embed_fn({"d": [1.0, 0.0], "q": [1.0, 0.0]})
        result = rank_relevant_tables("q", metadata, embed_fn, top_k=5)
        assert result == ["orders"]

    def test_excludes_gold_tables(self):
        metadata = self._metadata({
            "orders":  {"layer": "silver", "status": "approved", "description": "d", "columns": {}},
            "summary": {"layer": "gold",   "status": "approved", "description": "d", "columns": {}},
        })
        embed_fn = self._fake_embed_fn({"d": [1.0, 0.0], "q": [1.0, 0.0]})
        result = rank_relevant_tables("q", metadata, embed_fn, top_k=5)
        assert result == ["orders"]

    def test_no_approved_tables_returns_empty(self):
        metadata = self._metadata({
            "orders": {"layer": "silver", "status": "draft", "description": "d", "columns": {}},
        })
        embed_fn = self._fake_embed_fn({})
        assert rank_relevant_tables("q", metadata, embed_fn, top_k=5) == []

    def test_fewer_approved_than_top_k_returns_all(self):
        metadata = self._metadata({
            "orders": {"layer": "silver", "status": "approved", "description": "d", "columns": {}},
        })
        embed_fn = self._fake_embed_fn({"d": [1.0, 0.0], "q": [1.0, 0.0]})
        result = rank_relevant_tables("q", metadata, embed_fn, top_k=5)
        assert result == ["orders"]

    def test_includes_column_descriptions_in_ranked_text(self):
        # Column-level descriptions should influence the embedded text, not
        # just the table-level description — verified via the embed_fn spy.
        metadata = self._metadata({
            "orders": {
                "layer": "silver", "status": "approved", "description": "d",
                "columns": {"amount": {"description": "total order amount"}},
            },
        })
        seen_texts: list[str] = []
        def _embed(texts: list[str]) -> list[list[float]]:
            seen_texts.extend(texts)
            return [[1.0, 0.0] for _ in texts]
        rank_relevant_tables("q", metadata, _embed, top_k=5)
        assert any("total order amount" in t for t in seen_texts)


class TestRankRelevantTablesScored:
    """rank_relevant_tables_scored backs the confidence gate — same ranking
    as rank_relevant_tables, plus the top-1 similarity score."""

    def _metadata(self, tables: dict) -> MetadataConfig:
        return MetadataConfig(tables=tables)

    def _fake_embed_fn(self, vectors: dict[str, list[float]]):
        def _embed(texts: list[str]) -> list[list[float]]:
            result = []
            for text in texts:
                match = next((v for k, v in vectors.items() if k in text), None)
                if match is None:
                    raise KeyError(f"no vector fixture matches text: {text!r}")
                result.append(match)
            return result
        return _embed

    def test_returns_same_names_as_rank_relevant_tables(self):
        metadata = self._metadata({
            "orders": {"layer": "silver", "status": "approved", "description": "revenue by region", "columns": {}},
        })
        embed_fn = self._fake_embed_fn({"revenue by region": [1.0, 0.0], "q": [1.0, 0.0]})
        names, _confidence = rank_relevant_tables_scored("q", metadata, embed_fn, top_k=5)
        assert names == rank_relevant_tables("q", metadata, embed_fn, top_k=5)

    def test_confidence_is_top1_cosine_similarity(self):
        metadata = self._metadata({
            "orders": {"layer": "silver", "status": "approved", "description": "revenue by region", "columns": {}},
        })
        embed_fn = self._fake_embed_fn({
            "revenue by region": [1.0, 0.0], "total revenue per region": [0.9, 0.1],
        })
        _names, confidence = rank_relevant_tables_scored("total revenue per region", metadata, embed_fn, top_k=5)
        assert confidence == pytest.approx(0.994, abs=0.01)

    def test_no_approved_tables_confidence_is_zero_sentinel(self):
        metadata = self._metadata({
            "orders": {"layer": "silver", "status": "draft", "description": "d", "columns": {}},
        })
        embed_fn = self._fake_embed_fn({})
        names, confidence = rank_relevant_tables_scored("q", metadata, embed_fn, top_k=5)
        assert names == []
        assert confidence == 0.0


# ── prompt ────────────────────────────────────────────────────────────────────

class TestBuildPrompt:
    def test_contains_schema(self):
        prompt = build_prompt("TABLE orders (order_id BIGINT)", "How many orders?")
        assert "TABLE orders" in prompt
        assert "order_id" in prompt

    def test_contains_question(self):
        prompt = build_prompt("schema", "What is the total revenue?")
        assert "What is the total revenue?" in prompt

    def test_contains_sql_marker(self):
        prompt = build_prompt("schema", "q")
        assert "SQL:" in prompt

    def test_contains_few_shot_examples(self):
        prompt = build_prompt("schema", "q")
        assert "SELECT" in prompt

    def test_dynamic_few_shot_overrides_static(self):
        dynamic = [{"question": "custom dynamic example", "sql": "SELECT custom_col FROM custom_table"}]
        prompt = build_prompt("schema", "q", few_shot=dynamic)
        assert "custom dynamic example" in prompt
        assert "custom_table" in prompt
        # Static examples should not also appear — dynamic fully replaces them.
        assert "top 5 customers by total spend" not in prompt

    def test_empty_dynamic_few_shot_falls_back_to_static(self):
        prompt_empty = build_prompt("schema", "q", few_shot=[])
        prompt_none  = build_prompt("schema", "q", few_shot=None)
        assert prompt_empty == prompt_none

    def test_system_prompt_non_empty(self):
        assert len(system_prompt()) > 20

    def test_system_prompt_instructs_inline_clarification(self):
        # The main SQL-generation prompt itself now tells the LLM to ask for
        # clarification (via the CLARIFY: contract) instead of guessing when
        # the question is ambiguous — no separate LLM call required, unlike
        # the opt-in detect_ambiguity pre-check.
        assert "CLARIFY:" in system_prompt()

    def test_system_prompt_instructs_information_schema_for_meta_questions(self):
        # "What tables exist?" / "What columns does orders have?" have no
        # natural SELECT ... FROM <data-table> — the LLM must be told it can
        # answer these via DuckDB's information_schema, scoped to whatever
        # tables/views are registered (i.e. this database only, nothing
        # external).
        assert "information_schema" in system_prompt()

    def test_default_few_shot_includes_a_schema_meta_question(self):
        prompt = build_prompt("schema", "q")
        assert "information_schema" in prompt


class TestIsClarificationResponse:
    """Pure function — no LLM needed. Detects the CLARIFY: contract baked
    into prompt.py's _SYSTEM prompt, so pipeline.py can intercept an
    ambiguous-question response from the *same* SQL-generation call instead
    of treating it as invalid SQL and retrying uselessly."""

    def test_plain_clarify_response(self):
        assert is_clarification_response("CLARIFY: Which region do you mean?") == "Which region do you mean?"

    def test_case_insensitive_prefix(self):
        assert is_clarification_response("clarify: lowercase works too") == "lowercase works too"

    def test_strips_surrounding_whitespace(self):
        assert is_clarification_response("  CLARIFY:   extra spaces around it   ") == "extra spaces around it"

    def test_strips_markdown_fences(self):
        assert is_clarification_response("```\nCLARIFY: fenced question?\n```") == "fenced question?"

    def test_normal_sql_returns_none(self):
        assert is_clarification_response("SELECT * FROM orders") is None

    def test_prefix_must_anchor_at_start_not_appear_mid_text(self):
        # A SELECT with the word "clarify" somewhere in a comment must NOT
        # be misdetected as a clarification response.
        sql = "-- please clarify: is this right?\nSELECT * FROM orders"
        assert is_clarification_response(sql) is None

    def test_empty_string_returns_none(self):
        assert is_clarification_response("") is None


class TestBuildTemplateFillPrompt:
    """Template-Routed Query Layer roadmap (see CLAUDE.md), build order step 4:
    a narrow, single-purpose prompt — analogous in spirit to decomposition.py's
    detect_ambiguity/decompose_question — that asks the LLM only to fill a
    matched template's declared parameters, never to author SQL logic."""

    def test_prompt_includes_question_and_params(self):
        template = {
            "question": "Revenue by region for {date_range}?",
            "sql": "SELECT region, SUM(amount) FROM orders WHERE order_date BETWEEN {date_range} GROUP BY region",
            "params": {"date_range": "ISO date range, e.g. 2026-Q1"},
        }
        prompt = build_template_fill_prompt("What was revenue by region last quarter?", template)
        assert "What was revenue by region last quarter?" in prompt
        assert "date_range" in prompt
        assert "ISO date range, e.g. 2026-Q1" in prompt

    def test_prompt_never_includes_raw_sql_as_something_to_edit(self):
        # The LLM must not be invited to rewrite the SQL — only fill params.
        template = {
            "question": "q",
            "sql": "SELECT region, SUM(amount) FROM orders WHERE order_date BETWEEN {date_range} GROUP BY region",
            "params": {"date_range": "ISO date range"},
        }
        prompt = build_template_fill_prompt("q2", template)
        assert "SQL" not in prompt or "GROUP BY" not in prompt

    def test_prompt_handles_no_params(self):
        template = {"question": "count everything", "sql": "SELECT COUNT(*) FROM orders", "params": {}}
        prompt = build_template_fill_prompt("how many rows total?", template)
        assert "how many rows total?" in prompt


class TestParseTemplateFillResponse:

    def test_parses_plain_json_object(self):
        result = parse_template_fill_response('{"date_range": "2026-Q1"}')
        assert result == {"date_range": "2026-Q1"}

    def test_strips_markdown_fences(self):
        result = parse_template_fill_response('```json\n{"region": "US"}\n```')
        assert result == {"region": "US"}

    def test_multiple_params(self):
        result = parse_template_fill_response('{"region": "US", "date_range": "2026-Q1"}')
        assert result == {"region": "US", "date_range": "2026-Q1"}

    def test_invalid_json_returns_none(self):
        assert parse_template_fill_response("not json at all") is None

    def test_non_object_json_returns_none(self):
        assert parse_template_fill_response('["a", "b"]') is None

    def test_non_string_values_returns_none(self):
        assert parse_template_fill_response('{"count": 5}') is None

    def test_empty_object_is_valid_for_no_params(self):
        assert parse_template_fill_response("{}") == {}


class TestFillTemplate:

    def test_substitutes_single_param(self):
        sql = "SELECT * FROM orders WHERE region = {region}"
        result = fill_template(sql, {"region": "'US'"})
        assert result == "SELECT * FROM orders WHERE region = 'US'"

    def test_substitutes_multiple_params(self):
        sql = "SELECT region, SUM(amount) FROM orders WHERE order_date BETWEEN {start} AND {end} GROUP BY region"
        result = fill_template(sql, {"start": "'2026-01-01'", "end": "'2026-03-31'"})
        assert "'2026-01-01'" in result
        assert "'2026-03-31'" in result
        assert "{start}" not in result
        assert "{end}" not in result

    def test_no_params_returns_sql_unchanged(self):
        sql = "SELECT COUNT(*) FROM orders"
        assert fill_template(sql, {}) == sql

    def test_extra_values_not_in_sql_are_ignored(self):
        sql = "SELECT * FROM orders WHERE region = {region}"
        result = fill_template(sql, {"region": "'US'", "unused": "ignored"})
        assert result == "SELECT * FROM orders WHERE region = 'US'"


# ── validator ────────────────────────────────────────────────────────────────

class TestValidator:
    def test_valid_select(self, silver_dir):
        ok, msg = validate("SELECT * FROM orders", str(silver_dir))
        assert ok, msg

    def test_valid_cte(self, silver_dir):
        sql = "WITH t AS (SELECT * FROM orders) SELECT * FROM t"
        ok, msg = validate(sql, str(silver_dir))
        assert ok, msg

    def test_valid_aggregation(self, silver_dir):
        sql = "SELECT region, COUNT(*) AS n FROM orders GROUP BY region"
        ok, msg = validate(sql, str(silver_dir))
        assert ok, msg

    def test_valid_information_schema_tables_query(self, silver_dir):
        sql = "SELECT table_name FROM information_schema.tables"
        ok, msg = validate(sql, str(silver_dir))
        assert ok, msg

    def test_valid_information_schema_columns_query(self, silver_dir):
        sql = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'orders'"
        ok, msg = validate(sql, str(silver_dir))
        assert ok, msg

    def test_rejects_insert(self, silver_dir):
        ok, msg = validate("INSERT INTO orders VALUES (1,2,3,4)", str(silver_dir))
        assert not ok
        assert msg  # caught by SELECT/WITH check (INSERT doesn't start with SELECT or WITH)

    def test_rejects_drop(self, silver_dir):
        ok, _ = validate("DROP TABLE orders", str(silver_dir))
        assert not ok

    def test_rejects_delete(self, silver_dir):
        ok, _ = validate("DELETE FROM orders", str(silver_dir))
        assert not ok

    def test_rejects_bad_column(self, silver_dir):
        ok, _ = validate("SELECT nonexistent_col FROM orders", str(silver_dir))
        assert not ok

    def test_rejects_missing_table(self, silver_dir):
        ok, _ = validate("SELECT * FROM ghost_table", str(silver_dir))
        assert not ok

    def test_retry_loop_succeeds(self, silver_dir):
        bad  = "DELETE FROM orders"
        good = "SELECT * FROM orders"
        result = validate_and_fix(
            bad, str(silver_dir),
            llm_retry_fn=lambda _: good,
            original_prompt="show all orders",
        )
        assert result == good

    def test_retry_loop_exhausted_raises(self, silver_dir):
        with pytest.raises(ValueError, match="retr"):
            validate_and_fix(
                "DROP TABLE orders",
                str(silver_dir),
                llm_retry_fn=lambda _: "DELETE FROM orders",
                original_prompt="q",
                max_retries=2,
            )

    def test_first_attempt_passes(self, silver_dir):
        sql    = "SELECT COUNT(*) AS n FROM orders"
        result = validate_and_fix(
            sql, str(silver_dir),
            llm_retry_fn=lambda _: (_ for _ in ()).throw(AssertionError("should not be called")),
            original_prompt="count orders",
            max_retries=1,
        )
        assert result == sql


# ── result sanity check (build order step 11) ────────────────────────────────

class TestCheckResultSanity:

    def test_non_empty_result_skips_llm_entirely(self, silver_dir):
        sql = "SELECT * FROM orders"
        result = execute(sql, silver_dir)
        llm_fn = MagicMock(side_effect=AssertionError("should not be called"))
        out_sql, out_result = check_result_sanity("show orders", sql, result, silver_dir, llm_fn)
        llm_fn.assert_not_called()
        assert out_sql == sql
        assert out_result.equals(result)

    def test_zero_rows_llm_confirms_ok_returns_unchanged(self, silver_dir):
        sql = "SELECT * FROM orders WHERE region = 'Nonexistent'"
        result = execute(sql, silver_dir)
        assert len(result) == 0
        llm_fn = MagicMock(return_value="OK")
        out_sql, out_result = check_result_sanity("orders in Mars region", sql, result, silver_dir, llm_fn)
        assert out_sql == sql
        assert len(out_result) == 0
        llm_fn.assert_called_once()

    def test_zero_rows_llm_fix_is_reexecuted(self, silver_dir):
        bad_sql  = "SELECT * FROM orders WHERE region = 'Nonexistent'"
        good_sql = "SELECT * FROM orders WHERE region = 'North'"
        result = execute(bad_sql, silver_dir)
        llm_fn = MagicMock(return_value=good_sql)
        out_sql, out_result = check_result_sanity("orders in north region", bad_sql, result, silver_dir, llm_fn)
        assert out_sql == good_sql
        assert len(out_result) == 1

    def test_zero_rows_llm_fix_that_fails_validation_falls_back_silently(self, silver_dir):
        bad_sql = "SELECT * FROM orders WHERE region = 'Nonexistent'"
        result = execute(bad_sql, silver_dir)
        llm_fn = MagicMock(return_value="DROP TABLE orders")
        out_sql, out_result = check_result_sanity("orders in nowhere", bad_sql, result, silver_dir, llm_fn)
        assert out_sql == bad_sql
        assert len(out_result) == 0

    def test_zero_rows_llm_confirms_ok_case_insensitive_and_trailing_period(self, silver_dir):
        sql = "SELECT * FROM orders WHERE region = 'Nonexistent'"
        result = execute(sql, silver_dir)
        llm_fn = MagicMock(return_value="ok.")
        out_sql, _ = check_result_sanity("q", sql, result, silver_dir, llm_fn)
        assert out_sql == sql

    def test_only_one_reexecution_attempt(self, silver_dir):
        # The LLM's fix also returns 0 rows — must not loop again.
        bad_sql   = "SELECT * FROM orders WHERE region = 'Nonexistent'"
        also_zero = "SELECT * FROM orders WHERE region = 'StillNothing'"
        result = execute(bad_sql, silver_dir)
        llm_fn = MagicMock(return_value=also_zero)
        out_sql, out_result = check_result_sanity("q", bad_sql, result, silver_dir, llm_fn)
        assert out_sql == also_zero
        assert len(out_result) == 0
        llm_fn.assert_called_once()


# ── executor ─────────────────────────────────────────────────────────────────

class TestExecutor:
    def test_returns_polars_dataframe(self, silver_dir):
        df = execute("SELECT * FROM orders", silver_dir)
        assert isinstance(df, pl.DataFrame)

    def test_correct_row_count(self, silver_dir):
        df = execute("SELECT * FROM orders", silver_dir)
        assert len(df) == 3

    def test_aggregation(self, silver_dir):
        df = execute("SELECT COUNT(*) AS n FROM orders", silver_dir)
        assert df["n"][0] == 3

    def test_filter(self, silver_dir):
        df = execute("SELECT * FROM orders WHERE amount > 100", silver_dir)
        assert len(df) == 2

    def test_join_tables(self, silver_dir):
        sql = (
            "SELECT o.order_id, p.name "
            "FROM orders o JOIN products p ON o.order_id = p.product_id"
        )
        df = execute(sql, silver_dir)
        assert isinstance(df, pl.DataFrame)
        assert "name" in df.columns

    def test_cte(self, silver_dir):
        sql = (
            "WITH big AS (SELECT * FROM orders WHERE amount > 100) "
            "SELECT * FROM big"
        )
        df = execute(sql, silver_dir)
        assert len(df) == 2

    def test_information_schema_lists_registered_tables_only(self, silver_dir):
        # "What tables are in the database?" — must reflect exactly the
        # views registered from silver_dir (orders, products), and nothing
        # external (no other schemas, no system tables leaking in).
        df = execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main' ORDER BY table_name",
            silver_dir,
        )
        assert df["table_name"].to_list() == ["orders", "products"]

    def test_information_schema_lists_columns_for_one_table(self, silver_dir):
        df = execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'orders' ORDER BY ordinal_position",
            silver_dir,
        )
        assert df["column_name"].to_list() == ["order_id", "customer_id", "amount", "region"]


# ── recommender ──────────────────────────────────────────────────────────────

class TestRecommender:
    def test_returns_string(self):
        df = pl.DataFrame({"region": ["North"], "revenue": [100.0]})
        result = recommend(
            question="show revenue by region",
            sql="SELECT region, SUM(amount) AS revenue FROM orders GROUP BY region",
            result=df,
            llm_fn=lambda _: "Show total revenue grouped by region, descending",
        )
        assert isinstance(result, str)
        assert len(result) > 0

    def test_strips_quotes(self):
        df = pl.DataFrame({"a": [1]})
        result = recommend("q", "SELECT 1 AS a", df, llm_fn=lambda _: '"Count all rows"')
        assert not result.startswith('"')

    def test_empty_dataframe(self):
        df = pl.DataFrame({"n": []})
        result = recommend("count rows", "SELECT COUNT(*) AS n FROM t", df,
                           llm_fn=lambda _: "How many rows are in the table?")
        assert isinstance(result, str)


# ── get_client factory ────────────────────────────────────────────────────────

class TestGetClient:
    def test_ollama_returns_ollama_client(self):
        client = get_client("ollama", "llama3.2", base_url="http://localhost:11434")
        assert isinstance(client, OllamaClient)

    def test_openrouter_returns_openai_compatible(self):
        client = get_client("openrouter", "openai/gpt-4o", api_key="sk-test")
        assert isinstance(client, OpenAICompatibleClient)

    def test_openai_returns_openai_compatible(self):
        client = get_client("openai", "gpt-4o", api_key="sk-test")
        assert isinstance(client, OpenAICompatibleClient)

    def test_custom_with_base_url_returns_openai_compatible(self):
        client = get_client("lmstudio", "local-model", api_key="ignored",
                            base_url="http://localhost:1234/v1")
        assert isinstance(client, OpenAICompatibleClient)

    def test_openrouter_without_api_key_raises(self):
        with pytest.raises(ValueError, match="API key"):
            with patch("openmedallion.cerebrum.llm.settings") as mock_settings:
                mock_settings.LLM_API_KEY = None
                mock_settings.OLLAMA_URL  = "http://localhost:11434"
                get_client("openrouter", "openai/gpt-4o")

    def test_unknown_provider_without_base_url_raises(self):
        with pytest.raises(ValueError, match="base_url"):
            with patch("openmedallion.cerebrum.llm.settings") as mock_settings:
                mock_settings.LLM_API_KEY = "sk-test"   # key present; should fail on missing base_url
                mock_settings.OLLAMA_URL  = "http://localhost:11434"
                get_client("mycompany-llm", "custom-model")

    def test_client_satisfies_protocol(self):
        client = get_client("ollama", "llama3.2", base_url="http://localhost:11434")
        assert isinstance(client, LLMClient)


# ── pipeline (integration, mocked LLM) ───────────────────────────────────────

class TestCerebrumPipeline:
    def test_ask_returns_query_result(self, silver_dir):
        good_sql = "SELECT * FROM orders LIMIT 1"
        mock_llm = MagicMock(side_effect=[good_sql, "Show the first order row"])
        pipeline = CerebrumPipeline(silver_dir, _client=mock_llm)
        result   = pipeline.ask("Show me one order")

        assert isinstance(result, QueryResult)
        assert result.sql == good_sql
        assert isinstance(result.result, pl.DataFrame)
        assert len(result.result) == 1
        assert isinstance(result.recommended_prompt, str)

    def test_ask_retries_bad_sql(self, silver_dir):
        bad_sql  = "DELETE FROM orders"
        good_sql = "SELECT COUNT(*) AS n FROM orders"
        # call sequence: initial SQL (bad) → retry SQL (good) → recommender
        mock_llm = MagicMock(side_effect=[bad_sql, good_sql, "Count all orders"])
        pipeline = CerebrumPipeline(silver_dir, _client=mock_llm)
        result   = pipeline.ask("How many orders?")

        assert result.sql == good_sql
        assert result.result["n"][0] == 3

    def test_ask_raises_after_max_retries(self, silver_dir):
        mock_llm = MagicMock(return_value="DROP TABLE orders")
        pipeline = CerebrumPipeline(silver_dir, _client=mock_llm)

        with pytest.raises(ValueError):
            pipeline.ask("destroy everything")

    def test_result_columns(self, silver_dir):
        sql      = "SELECT region, SUM(amount) AS total FROM orders GROUP BY region"
        mock_llm = MagicMock(side_effect=[sql, "Revenue by region"])
        pipeline = CerebrumPipeline(silver_dir, _client=mock_llm)
        result   = pipeline.ask("revenue by region")

        assert "region" in result.result.columns
        assert "total"  in result.result.columns

    def test_empty_result_triggers_sanity_check(self, silver_dir):
        empty_sql = "SELECT * FROM orders WHERE region = 'Nonexistent'"
        # call sequence: initial SQL (empty result) → sanity check (confirms OK) → recommend
        mock_llm = MagicMock(side_effect=[empty_sql, "OK", "Orders in a nonexistent region"])
        pipeline = CerebrumPipeline(silver_dir, _client=mock_llm)
        result   = pipeline.ask("orders in a made-up region")

        assert result.sql == empty_sql
        assert len(result.result) == 0
        assert mock_llm.call_count == 3

    def test_empty_result_sanity_check_fix_is_used(self, silver_dir):
        empty_sql = "SELECT * FROM orders WHERE region = 'Nonexistent'"
        fixed_sql = "SELECT * FROM orders WHERE region = 'North'"
        mock_llm = MagicMock(side_effect=[empty_sql, fixed_sql, "Orders in the north region"])
        pipeline = CerebrumPipeline(silver_dir, _client=mock_llm)
        result   = pipeline.ask("orders in the north region")

        assert result.sql == fixed_sql
        assert len(result.result) == 1

    def test_non_empty_result_skips_sanity_check_llm_call(self, silver_dir):
        sql = "SELECT * FROM orders LIMIT 1"
        # Only 2 calls in side_effect — a 3rd (sanity check) would raise StopIteration if made.
        mock_llm = MagicMock(side_effect=[sql, "Show one order"])
        pipeline = CerebrumPipeline(silver_dir, _client=mock_llm)
        result   = pipeline.ask("show one order")

        assert mock_llm.call_count == 2
        assert len(result.result) == 1

    def test_default_provider_is_ollama(self, silver_dir):
        with patch("openmedallion.cerebrum.llm.get_client") as mock_factory:
            mock_factory.return_value = MagicMock(side_effect=["SELECT 1", "q"])
            CerebrumPipeline(silver_dir, model="llama3.2")
            mock_factory.assert_called_once_with(
                "ollama", "llama3.2", api_key=None, base_url=None
            )

    def test_openrouter_provider_forwarded(self, silver_dir):
        with patch("openmedallion.cerebrum.llm.get_client") as mock_factory:
            mock_factory.return_value = MagicMock()
            CerebrumPipeline(
                silver_dir,
                provider="openrouter",
                model="openai/gpt-4o",
                api_key="sk-test",
            )
            mock_factory.assert_called_once_with(
                "openrouter", "openai/gpt-4o", api_key="sk-test", base_url=None
            )


# ── dynamic few-shot retrieval (build order step 9) ──────────────────────────

class TestCerebrumPipelineDynamicFewShot:

    def _fake_embed_fn(self, vectors: dict[str, list[float]]):
        def _embed(texts: list[str]) -> list[list[float]]:
            return [vectors[t] for t in texts]
        return _embed

    def test_no_examples_dir_passes_none_to_build_prompt(self, silver_dir):
        mock_llm = MagicMock(side_effect=["SELECT * FROM orders LIMIT 1", "q"])
        pipeline = CerebrumPipeline(silver_dir, _client=mock_llm)
        with patch("openmedallion.cerebrum.pipeline._prompt.build_prompt", wraps=build_prompt) as mock_bp:
            pipeline.ask("Show me one order")
        assert mock_bp.call_args.kwargs["few_shot"] is None

    def test_examples_dir_with_no_verified_examples_passes_none(self, tmp_path, silver_dir):
        examples_dir = tmp_path / "examples"
        examples_dir.mkdir()
        (examples_dir / "synthetic.jsonl").write_text(
            '{"question": "unverified", "sql": "SELECT 1", "verified": false}\n'
        )
        mock_llm = MagicMock(side_effect=["SELECT * FROM orders LIMIT 1", "q"])
        pipeline = CerebrumPipeline(silver_dir, examples_dir=examples_dir, _client=mock_llm)
        with patch("openmedallion.cerebrum.pipeline._prompt.build_prompt", wraps=build_prompt) as mock_bp:
            pipeline.ask("Show me one order")
        assert mock_bp.call_args.kwargs["few_shot"] is None

    def test_examples_dir_with_verified_examples_ranks_and_passes_them(self, tmp_path, silver_dir):
        examples_dir = tmp_path / "examples"
        examples_dir.mkdir()
        (examples_dir / "synthetic.jsonl").write_text(
            '{"question": "revenue by region", "sql": "SELECT region, SUM(amount) FROM orders GROUP BY region", "verified": true}\n'
            '{"question": "how many products", "sql": "SELECT COUNT(*) FROM products", "verified": true}\n'
        )
        vectors = {
            "revenue by region":  [1.0, 0.0],
            "how many products":  [0.0, 1.0],
            "total revenue per region": [0.9, 0.1],
        }
        embed_fn = self._fake_embed_fn(vectors)
        mock_llm = MagicMock(side_effect=["SELECT region, SUM(amount) FROM orders GROUP BY region", "q"])
        pipeline = CerebrumPipeline(
            silver_dir, examples_dir=examples_dir, _client=mock_llm, _embed_fn=embed_fn,
        )
        with patch("openmedallion.cerebrum.pipeline._prompt.build_prompt", wraps=build_prompt) as mock_bp:
            pipeline.ask("total revenue per region")

        passed_few_shot = mock_bp.call_args.kwargs["few_shot"]
        assert passed_few_shot[0]["question"] == "revenue by region"

    def test_embeddings_cached_across_multiple_ask_calls(self, tmp_path, silver_dir):
        examples_dir = tmp_path / "examples"
        examples_dir.mkdir()
        (examples_dir / "synthetic.jsonl").write_text(
            '{"question": "revenue by region", "sql": "SELECT region, SUM(amount) FROM orders GROUP BY region", "verified": true}\n'
        )
        embed_calls: list[list[str]] = []

        def _embed(texts: list[str]) -> list[list[float]]:
            embed_calls.append(texts)
            return [[1.0, 0.0] for _ in texts]

        mock_llm = MagicMock(side_effect=[
            "SELECT * FROM orders LIMIT 1", "q1",
            "SELECT * FROM orders LIMIT 1", "q2",
        ])
        pipeline = CerebrumPipeline(
            silver_dir, examples_dir=examples_dir, _client=mock_llm, _embed_fn=_embed,
        )
        pipeline.ask("first question")
        pipeline.ask("second question")

        # The example corpus ("revenue by region") should be embedded exactly
        # once (cached on the instance); only the per-question embed calls repeat.
        corpus_embed_calls = [c for c in embed_calls if c == ["revenue by region"]]
        assert len(corpus_embed_calls) == 1


class TestCerebrumPipelineSchemaPruning:

    def _fake_embed_fn(self, vectors: dict[str, list[float]]):
        def _embed(texts: list[str]) -> list[list[float]]:
            result = []
            for text in texts:
                match = next((v for k, v in vectors.items() if k in text), None)
                if match is None:
                    raise KeyError(f"no vector fixture matches text: {text!r}")
                result.append(match)
            return result
        return _embed

    def test_no_metadata_passes_none_to_build_schema_context(self, silver_dir):
        mock_llm = MagicMock(side_effect=["SELECT * FROM orders LIMIT 1", "q"])
        pipeline = CerebrumPipeline(silver_dir, _client=mock_llm)
        with patch("openmedallion.cerebrum.pipeline._schema.build_schema_context", wraps=build_schema_context) as mock_bsc:
            pipeline.ask("Show me one order")
        assert mock_bsc.call_args.kwargs["tables"] is None

    def test_no_approved_tables_falls_back_to_raw_schema_search(self, silver_dir):
        # No approved tables -> confidence is the 0.0 sentinel, which is
        # below CONFIDENCE_THRESHOLD, so this now triggers the raw-schema
        # fallback (describe_all_tables) instead of showing every table
        # unpruned — this is the "confidence-signal gap" the roadmap flagged
        # as unwired, now closed.
        metadata = MetadataConfig(tables={
            "orders": {"layer": "silver", "status": "draft", "description": "d", "columns": {}},
        })
        embed_fn = self._fake_embed_fn({
            "orders": [1.0, 0.0], "products": [0.0, 1.0], "Show me one order": [1.0, 0.0],
        })
        mock_llm = MagicMock(side_effect=["SELECT * FROM orders LIMIT 1", "q"])
        pipeline = CerebrumPipeline(silver_dir, metadata=metadata, _client=mock_llm, _embed_fn=embed_fn)
        with patch("openmedallion.cerebrum.pipeline._schema.build_schema_context", wraps=build_schema_context) as mock_bsc:
            pipeline.ask("Show me one order")
        pruned = mock_bsc.call_args.kwargs["tables"]
        assert pruned is not None
        assert set(pruned) == {"orders", "products"}

    def test_low_confidence_approved_match_falls_back_to_raw_schema_search(self, silver_dir):
        # An approved table exists, but its description is near-orthogonal to
        # the question -> structured confidence lands below the 0.7 gate, so
        # the raw-schema fallback corpus (all silver tables, any status) is
        # used instead of the structured (approved-only) ranking.
        metadata = MetadataConfig(tables={
            "orders": {"layer": "silver", "status": "approved", "description": "irrelevant topic", "columns": {}},
        })
        vectors = {
            "irrelevant topic":          [0.0, 1.0],
            "total revenue per region":  [1.0, 0.0],
            "orders":                    [1.0, 0.0],
            "products":                  [0.9, 0.1],
        }
        embed_fn = self._fake_embed_fn(vectors)
        mock_llm = MagicMock(side_effect=["SELECT * FROM orders LIMIT 1", "q"])
        pipeline = CerebrumPipeline(silver_dir, metadata=metadata, _client=mock_llm, _embed_fn=embed_fn)

        steps: list[str] = []
        pipeline.ask("total revenue per region", on_step=steps.append)

        assert pipeline._embedded_fallback_tables is not None
        assert pipeline._embedded_fallback_tables != []
        assert any("confidence" in s.lower() and "fallback" in s.lower() for s in steps)

    def test_high_confidence_does_not_build_fallback_corpus(self, silver_dir):
        # Confirms the fallback corpus is genuinely lazy -- when structured
        # confidence clears the gate, describe_all_tables()/the fallback
        # embedding is never triggered (no wasted work).
        metadata = MetadataConfig(tables={
            "orders": {"layer": "silver", "status": "approved", "description": "revenue by region", "columns": {}},
        })
        vectors = {"revenue by region": [1.0, 0.0], "total revenue per region": [0.9, 0.1]}
        embed_fn = self._fake_embed_fn(vectors)
        mock_llm = MagicMock(side_effect=["SELECT * FROM orders LIMIT 1", "q"])
        pipeline = CerebrumPipeline(silver_dir, metadata=metadata, _client=mock_llm, _embed_fn=embed_fn)
        pipeline.ask("total revenue per region")
        assert pipeline._embedded_fallback_tables is None

    def test_approved_tables_prune_the_schema_context(self, silver_dir):
        # More approved tables than the pipeline's top_k=5 so pruning actually
        # narrows the set, not just reorders it.
        tables = {
            "orders": {"layer": "silver", "status": "approved", "description": "revenue by region", "columns": {}},
        }
        vectors = {"revenue by region": [1.0, 0.0], "total revenue per region": [0.9, 0.1]}
        for i in range(6):
            name = f"filler{i}"
            tables[name] = {"layer": "silver", "status": "approved", "description": f"filler table {i}", "columns": {}}
            vectors[f"filler table {i}"] = [0.0, 1.0]
        metadata = MetadataConfig(tables=tables)

        embed_fn = self._fake_embed_fn(vectors)
        mock_llm = MagicMock(side_effect=["SELECT region, SUM(amount) FROM orders GROUP BY region", "q"])
        pipeline = CerebrumPipeline(silver_dir, metadata=metadata, _client=mock_llm, _embed_fn=embed_fn)
        with patch("openmedallion.cerebrum.pipeline._schema.build_schema_context", wraps=build_schema_context) as mock_bsc:
            pipeline.ask("total revenue per region")

        pruned = mock_bsc.call_args.kwargs["tables"]
        assert pruned[0] == "orders"
        assert len(pruned) == 5  # top_k=5, out of 7 total approved tables

    def test_table_embeddings_cached_across_multiple_ask_calls(self, silver_dir):
        metadata = MetadataConfig(tables={
            "orders": {"layer": "silver", "status": "approved", "description": "revenue by region", "columns": {}},
        })
        embed_calls: list[list[str]] = []

        def _embed(texts: list[str]) -> list[list[float]]:
            embed_calls.append(texts)
            return [[1.0, 0.0] for _ in texts]

        mock_llm = MagicMock(side_effect=[
            "SELECT * FROM orders LIMIT 1", "q1",
            "SELECT * FROM orders LIMIT 1", "q2",
        ])
        pipeline = CerebrumPipeline(silver_dir, metadata=metadata, _client=mock_llm, _embed_fn=_embed)
        pipeline.ask("first question")
        pipeline.ask("second question")

        corpus_embed_calls = [c for c in embed_calls if len(c) == 1 and "orders" in c[0]]
        assert len(corpus_embed_calls) == 1


# ── template-routed query layer (Template-Routed Query Layer roadmap,
#    build order step 5) ──────────────────────────────────────────────────

class TestCerebrumPipelineTemplateRouting:

    _TEMPLATE_QUESTION = "orders in a given region?"
    _TEMPLATE_SQL = "SELECT * FROM orders WHERE region = {region}"

    def _fake_embed_fn(self, vectors: dict[str, list[float]]):
        def _embed(texts: list[str]) -> list[list[float]]:
            return [vectors[t] for t in texts]
        return _embed

    def _write_templated_synthetic(self, examples_dir):
        examples_dir.mkdir(exist_ok=True)
        (examples_dir / "synthetic.jsonl").write_text(
            '{"question": "%s", "sql": "%s", "verified": true, "templated": true, '
            '"params": {"region": "SQL string literal, e.g. \'North\'"}}\n'
            % (self._TEMPLATE_QUESTION, self._TEMPLATE_SQL.replace('"', '\\"'))
        )

    def test_default_off_never_matches_even_with_templates_present(self, tmp_path, silver_dir):
        examples_dir = tmp_path / "examples"
        self._write_templated_synthetic(examples_dir)
        embed_fn = self._fake_embed_fn({
            self._TEMPLATE_QUESTION: [1.0, 0.0],
            "orders in the north region": [1.0, 0.0],
        })
        # use_templates defaults False -- normal SQL-gen flow runs even though
        # a high-confidence template match exists.
        mock_llm = MagicMock(side_effect=["SELECT * FROM orders WHERE region = 'North'", "q"])
        pipeline = CerebrumPipeline(
            silver_dir, examples_dir=examples_dir, _client=mock_llm, _embed_fn=embed_fn,
        )
        result = pipeline.ask("orders in the north region")
        assert mock_llm.call_count == 2
        assert result.sql == "SELECT * FROM orders WHERE region = 'North'"

    def test_high_confidence_match_skips_sql_generation(self, tmp_path, silver_dir):
        examples_dir = tmp_path / "examples"
        self._write_templated_synthetic(examples_dir)
        embed_fn = self._fake_embed_fn({
            self._TEMPLATE_QUESTION: [1.0, 0.0],
            "orders in the north region": [1.0, 0.0],
        })
        # call sequence: fill-params call (JSON) -> recommend call. No raw
        # SQL-generation call is ever made.
        mock_llm = MagicMock(side_effect=['{"region": "\'North\'"}', "Orders in the north region"])
        pipeline = CerebrumPipeline(
            silver_dir, examples_dir=examples_dir, _client=mock_llm, _embed_fn=embed_fn,
            use_templates=True,
        )
        result = pipeline.ask("orders in the north region")

        assert result.sql == "SELECT * FROM orders WHERE region = 'North'"
        assert len(result.result) == 1
        assert mock_llm.call_count == 2

    def test_low_confidence_falls_through_to_normal_generation(self, tmp_path, silver_dir):
        examples_dir = tmp_path / "examples"
        self._write_templated_synthetic(examples_dir)
        embed_fn = self._fake_embed_fn({
            self._TEMPLATE_QUESTION: [1.0, 0.0],
            "how many products are there": [0.0, 1.0],  # orthogonal -> confidence 0.0
        })
        mock_llm = MagicMock(side_effect=["SELECT COUNT(*) AS n FROM products", "q"])
        pipeline = CerebrumPipeline(
            silver_dir, examples_dir=examples_dir, _client=mock_llm, _embed_fn=embed_fn,
            use_templates=True,
        )
        result = pipeline.ask("how many products are there")

        assert result.sql == "SELECT COUNT(*) AS n FROM products"
        assert mock_llm.call_count == 2

    def test_on_step_reports_the_match(self, tmp_path, silver_dir):
        examples_dir = tmp_path / "examples"
        self._write_templated_synthetic(examples_dir)
        embed_fn = self._fake_embed_fn({
            self._TEMPLATE_QUESTION: [1.0, 0.0],
            "orders in the north region": [1.0, 0.0],
        })
        mock_llm = MagicMock(side_effect=['{"region": "\'North\'"}', "q"])
        pipeline = CerebrumPipeline(
            silver_dir, examples_dir=examples_dir, _client=mock_llm, _embed_fn=embed_fn,
            use_templates=True,
        )
        steps: list[str] = []
        pipeline.ask("orders in the north region", on_step=steps.append)
        assert any("matched template" in s.lower() for s in steps)

    def test_invalid_fill_response_falls_back_to_normal_generation(self, tmp_path, silver_dir):
        examples_dir = tmp_path / "examples"
        self._write_templated_synthetic(examples_dir)
        embed_fn = self._fake_embed_fn({
            self._TEMPLATE_QUESTION: [1.0, 0.0],
            "orders in the north region": [1.0, 0.0],
        })
        # call sequence: fill attempt (malformed) -> normal SQL-gen -> recommend
        mock_llm = MagicMock(side_effect=[
            "not valid json",
            "SELECT * FROM orders WHERE region = 'North'",
            "q",
        ])
        pipeline = CerebrumPipeline(
            silver_dir, examples_dir=examples_dir, _client=mock_llm, _embed_fn=embed_fn,
            use_templates=True,
        )
        result = pipeline.ask("orders in the north region")

        assert result.sql == "SELECT * FROM orders WHERE region = 'North'"
        assert mock_llm.call_count == 3

    def test_no_examples_dir_never_matches(self, silver_dir):
        mock_llm = MagicMock(side_effect=["SELECT * FROM orders LIMIT 1", "q"])
        pipeline = CerebrumPipeline(silver_dir, _client=mock_llm, use_templates=True)
        result = pipeline.ask("show one order")
        assert mock_llm.call_count == 2
        assert result.sql == "SELECT * FROM orders LIMIT 1"

    def test_embedded_templates_cached_across_multiple_ask_calls(self, tmp_path, silver_dir):
        examples_dir = tmp_path / "examples"
        self._write_templated_synthetic(examples_dir)
        embed_calls: list[list[str]] = []

        def _embed(texts: list[str]) -> list[list[float]]:
            embed_calls.append(texts)
            return [[1.0, 0.0] for _ in texts]

        mock_llm = MagicMock(side_effect=[
            '{"region": "\'North\'"}', "q1",
            '{"region": "\'North\'"}', "q2",
        ])
        pipeline = CerebrumPipeline(
            silver_dir, examples_dir=examples_dir, _client=mock_llm, _embed_fn=_embed,
            use_templates=True,
        )
        pipeline.ask("orders in the north region")
        pipeline.ask("orders in the north region")

        corpus_embed_calls = [c for c in embed_calls if c == [self._TEMPLATE_QUESTION]]
        assert len(corpus_embed_calls) == 1

    def test_filled_sql_still_goes_through_validate_and_fix(self, tmp_path, silver_dir):
        # A bad fill value produces SQL that fails EXPLAIN -- the retry loop
        # (an LLM call, not a silent execution of malformed SQL) must still
        # fire, proving the safety net is not bypassed on the template path.
        examples_dir = tmp_path / "examples"
        examples_dir.mkdir()
        (examples_dir / "synthetic.jsonl").write_text(
            '{"question": "orders over a threshold?", '
            '"sql": "SELECT * FROM orders WHERE amount > {threshold}", '
            '"verified": true, "templated": true, '
            '"params": {"threshold": "numeric literal"}}\n'
        )
        embed_fn = self._fake_embed_fn({
            "orders over a threshold?": [1.0, 0.0],
            "orders over 100": [1.0, 0.0],
        })
        # fill call returns an unquoted non-numeric placeholder -> invalid SQL
        # -> validate_and_fix's retry closure fires a real LLM call to fix it.
        mock_llm = MagicMock(side_effect=[
            '{"threshold": "not_a_number"}',
            "SELECT * FROM orders WHERE amount > 100",
            "q",
        ])
        pipeline = CerebrumPipeline(
            silver_dir, examples_dir=examples_dir, _client=mock_llm, _embed_fn=embed_fn,
            use_templates=True,
        )
        result = pipeline.ask("orders over 100")
        assert result.sql == "SELECT * FROM orders WHERE amount > 100"


# ── ambiguity detection + query decomposition (build order step 12) ─────────

class TestCerebrumPipelineAmbiguityAndDecomposition:

    def test_ambiguity_check_off_by_default(self, silver_dir):
        # No ambiguity-check call in the side_effect list — would raise
        # StopIteration if the pipeline tried to make one.
        mock_llm = MagicMock(side_effect=["SELECT * FROM orders LIMIT 1", "q"])
        pipeline = CerebrumPipeline(silver_dir, _client=mock_llm)
        result = pipeline.ask("show me one order")
        assert isinstance(result, QueryResult)
        assert mock_llm.call_count == 2

    def test_ambiguity_check_clear_proceeds_normally(self, silver_dir):
        mock_llm = MagicMock(side_effect=["CLEAR", "SELECT * FROM orders LIMIT 1", "q"])
        pipeline = CerebrumPipeline(silver_dir, _client=mock_llm, detect_ambiguity=True)
        result = pipeline.ask("show me one order")
        assert isinstance(result, QueryResult)
        assert mock_llm.call_count == 3

    def test_ambiguity_check_raises_before_generating_sql(self, silver_dir):
        clarification = "Which region — sales or shipping?"
        mock_llm = MagicMock(return_value=clarification)
        pipeline = CerebrumPipeline(silver_dir, _client=mock_llm, detect_ambiguity=True)
        with pytest.raises(AmbiguousQuestionError) as exc_info:
            pipeline.ask("show me revenue by region")
        assert exc_info.value.clarification == clarification
        assert exc_info.value.question == "show me revenue by region"
        mock_llm.assert_called_once()  # no SQL generation attempted

    def test_decompose_off_by_default(self, silver_dir):
        mock_llm = MagicMock(side_effect=["SELECT * FROM orders LIMIT 1", "q"])
        pipeline = CerebrumPipeline(silver_dir, _client=mock_llm)
        result = pipeline.ask("show me one order")
        assert isinstance(result, QueryResult)
        assert mock_llm.call_count == 2

    def test_decompose_single_proceeds_normally(self, silver_dir):
        mock_llm = MagicMock(side_effect=["SINGLE", "SELECT * FROM orders LIMIT 1", "q"])
        pipeline = CerebrumPipeline(silver_dir, _client=mock_llm, decompose_queries=True)
        result = pipeline.ask("show me one order")
        assert isinstance(result, QueryResult)
        assert mock_llm.call_count == 3

    def test_decompose_multi_returns_multi_query_result(self, silver_dir):
        decompose_response = '["How many orders?", "What is total revenue?"]'
        mock_llm = MagicMock(side_effect=[
            decompose_response,
            "SELECT COUNT(*) AS n FROM orders", "sub-q 1 recommend",
            "SELECT SUM(amount) AS total FROM orders", "sub-q 2 recommend",
        ])
        pipeline = CerebrumPipeline(silver_dir, _client=mock_llm, decompose_queries=True)
        result = pipeline.ask("How many orders are there and what is total revenue?")

        assert isinstance(result, MultiQueryResult)
        assert result.sub_questions == ["How many orders?", "What is total revenue?"]
        assert len(result.results) == 2
        assert all(isinstance(r, QueryResult) for r in result.results)
        assert result.results[0].question == "How many orders?"
        assert result.results[1].question == "What is total revenue?"
        assert mock_llm.call_count == 5

    def test_ambiguity_checked_before_decomposition(self, silver_dir):
        clarification = "What time range do you mean?"
        mock_llm = MagicMock(return_value=clarification)
        pipeline = CerebrumPipeline(
            silver_dir, _client=mock_llm, detect_ambiguity=True, decompose_queries=True,
        )
        with pytest.raises(AmbiguousQuestionError):
            pipeline.ask("show me recent orders")
        mock_llm.assert_called_once()  # decomposition check never runs


class TestCerebrumPipelineInlineClarification:
    """The main SQL-generation prompt itself can now ask for clarification
    (CLARIFY: contract in prompt.py's _SYSTEM) instead of guessing — a
    single-call, always-on mechanism, distinct from and independent of the
    opt-in detect_ambiguity pre-check (which makes a dedicated extra call
    before any SQL generation is attempted)."""

    def test_clarify_response_raises_with_no_extra_calls(self, silver_dir):
        clarification = "Which region — sales territory or shipping region?"
        mock_llm = MagicMock(return_value=f"CLARIFY: {clarification}")
        pipeline = CerebrumPipeline(silver_dir, _client=mock_llm)  # detect_ambiguity NOT set
        with pytest.raises(AmbiguousQuestionError) as exc_info:
            pipeline.ask("show me revenue by region")
        assert exc_info.value.clarification == clarification
        assert exc_info.value.question == "show me revenue by region"
        mock_llm.assert_called_once()  # no retry, no execute, no recommend call

    def test_normal_sql_response_is_unaffected(self, silver_dir):
        mock_llm = MagicMock(side_effect=["SELECT * FROM orders LIMIT 1", "recommended"])
        pipeline = CerebrumPipeline(silver_dir, _client=mock_llm)
        result = pipeline.ask("show me one order")
        assert isinstance(result, QueryResult)
        assert mock_llm.call_count == 2

    def test_inline_clarify_is_independent_of_detect_ambiguity_flag(self, silver_dir):
        # detect_ambiguity=True's pre-check says CLEAR, but the main
        # SQL-generation call itself still asks for clarification — both
        # mechanisms are independent and either can trigger it.
        clarification = "Which time period?"
        mock_llm = MagicMock(side_effect=["CLEAR", f"CLARIFY: {clarification}"])
        pipeline = CerebrumPipeline(silver_dir, _client=mock_llm, detect_ambiguity=True)
        with pytest.raises(AmbiguousQuestionError) as exc_info:
            pipeline.ask("show me recent orders")
        assert exc_info.value.clarification == clarification
        assert mock_llm.call_count == 2  # ambiguity pre-check + SQL-gen call, no more

    def test_inline_clarify_aborts_remaining_sub_questions(self, silver_dir):
        decompose_response = '["How many orders?", "What is total revenue?"]'
        clarification = "Which time range for total revenue?"
        mock_llm = MagicMock(side_effect=[
            decompose_response,
            "SELECT COUNT(*) AS n FROM orders", "sub-q 1 recommend",
            f"CLARIFY: {clarification}",
        ])
        pipeline = CerebrumPipeline(silver_dir, _client=mock_llm, decompose_queries=True)
        with pytest.raises(AmbiguousQuestionError) as exc_info:
            pipeline.ask("How many orders are there and what is total revenue?")
        assert exc_info.value.clarification == clarification
        assert exc_info.value.question == "What is total revenue?"


# ── dual LLM: separate SQL vs NLG models ─────────────────────────────────────

class TestCerebrumPipelineSchemaMetaQuestions:
    """Schema/meta-questions ("what tables exist?") are routed to the NLG
    model directly — no SQL is generated/executed at all, and the SQL
    client is never called."""

    def test_meta_question_never_calls_sql_client(self, silver_dir):
        sql_client = MagicMock(side_effect=AssertionError("SQL client must not be called"))
        nlg_client = MagicMock(return_value="There are two tables: orders and products.")
        pipeline = CerebrumPipeline(silver_dir, _client=sql_client, _nlg_client=nlg_client)

        result = pipeline.ask("What tables are in the database?")

        sql_client.assert_not_called()
        nlg_client.assert_called_once()
        assert isinstance(result, QueryResult)
        assert result.answer == "There are two tables: orders and products."
        assert result.sql == ""

    def test_meta_question_answer_uses_real_schema(self, silver_dir):
        captured = {}

        def _fake_nlg(prompt_text: str) -> str:
            captured["prompt"] = prompt_text
            return "answer"

        pipeline = CerebrumPipeline(
            silver_dir, _client=MagicMock(), _nlg_client=_fake_nlg,
        )
        pipeline.ask("What columns does the orders table have?")
        assert "TABLE orders" in captured["prompt"]

    def test_normal_data_question_still_uses_sql_client(self, silver_dir):
        sql_client = MagicMock(return_value="SELECT * FROM orders LIMIT 1")
        nlg_client = MagicMock(return_value="Show the first order row")
        pipeline = CerebrumPipeline(silver_dir, _client=sql_client, _nlg_client=nlg_client)

        result = pipeline.ask("Show me one order")

        sql_client.assert_called_once()
        nlg_client.assert_called_once()  # recommend() only
        assert result.sql == "SELECT * FROM orders LIMIT 1"
        assert result.answer is None


class TestCerebrumPipelineDualClientConstruction:
    """Without an explicit nlg_client/nlg_model/nlg_provider, the NLG path
    reuses the same SQL client — fully backward compatible, zero config."""

    def test_recommend_uses_same_client_by_default(self, silver_dir):
        mock_llm = MagicMock(side_effect=["SELECT * FROM orders LIMIT 1", "recommended text"])
        pipeline = CerebrumPipeline(silver_dir, _client=mock_llm)
        result = pipeline.ask("show one order")
        assert mock_llm.call_count == 2
        assert result.recommended_prompt == "recommended text"

    def test_explicit_nlg_model_builds_a_distinct_client(self, silver_dir):
        with patch("openmedallion.cerebrum.llm.get_client") as mock_factory:
            sql_client = MagicMock(return_value="SELECT 1")
            nlg_client = MagicMock(return_value="recommended")
            mock_factory.side_effect = [sql_client, nlg_client]

            pipeline = CerebrumPipeline(
                silver_dir, model="llama3.2", nlg_model="mistral",
            )
            pipeline.ask("q")

            assert mock_factory.call_count == 2
            _, nlg_kwargs = mock_factory.call_args_list[1]
            assert mock_factory.call_args_list[1].args[1] == "mistral"
