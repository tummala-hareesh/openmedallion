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
from openmedallion.cerebrum.pipeline    import CerebrumPipeline, QueryResult
from openmedallion.cerebrum.prompt      import build_prompt, system_prompt
from openmedallion.cerebrum.recommender import recommend
from openmedallion.cerebrum.schema      import build_schema_context
from openmedallion.cerebrum.validator   import validate, validate_and_fix


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

    def test_system_prompt_non_empty(self):
        assert len(system_prompt()) > 20


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
