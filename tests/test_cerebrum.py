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
from openmedallion.cerebrum.prompt      import build_prompt, system_prompt
from openmedallion.cerebrum.recommender import recommend
from openmedallion.cerebrum.schema      import build_schema_context, rank_relevant_tables
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

    def test_no_approved_tables_passes_none(self, silver_dir):
        metadata = MetadataConfig(tables={
            "orders": {"layer": "silver", "status": "draft", "description": "d", "columns": {}},
        })
        mock_llm = MagicMock(side_effect=["SELECT * FROM orders LIMIT 1", "q"])
        pipeline = CerebrumPipeline(silver_dir, metadata=metadata, _client=mock_llm)
        with patch("openmedallion.cerebrum.pipeline._schema.build_schema_context", wraps=build_schema_context) as mock_bsc:
            pipeline.ask("Show me one order")
        assert mock_bsc.call_args.kwargs["tables"] is None

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
