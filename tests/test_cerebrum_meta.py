"""tests/test_cerebrum_meta.py — Unit tests for openmedallion.cerebrum.meta.

Pure functions, no LLM/DuckDB needed — `answer_schema_question` takes an
injectable ``llm_fn`` the same way `cerebrum/recommender.py` etc. do.
"""
from __future__ import annotations

from openmedallion.cerebrum.meta import answer_schema_question, is_schema_meta_question


class TestIsSchemaMetaQuestion:
    def test_what_tables(self):
        assert is_schema_meta_question("What tables are in the database?")

    def test_which_tables_exist(self):
        assert is_schema_meta_question("Which tables exist?")

    def test_list_tables(self):
        assert is_schema_meta_question("List the tables")

    def test_tables_do_you_have(self):
        assert is_schema_meta_question("What tables do you have?")

    def test_what_columns(self):
        assert is_schema_meta_question("What columns does the orders table have?")

    def test_which_columns(self):
        assert is_schema_meta_question("Which columns are in transactions?")

    def test_describe_schema(self):
        assert is_schema_meta_question("Describe the database schema")

    def test_describe_table(self):
        assert is_schema_meta_question("Describe the orders table")

    def test_database_schema_phrase(self):
        assert is_schema_meta_question("What is the database schema?")

    def test_case_insensitive(self):
        assert is_schema_meta_question("WHAT TABLES EXIST")

    def test_normal_data_question_not_meta(self):
        assert not is_schema_meta_question("How many orders were placed each month?")

    def test_top_customers_question_not_meta(self):
        assert not is_schema_meta_question("What are the top 5 customers by total spend?")

    def test_average_salary_question_not_meta(self):
        assert not is_schema_meta_question("Show average salary by department")

    def test_total_revenue_question_not_meta(self):
        assert not is_schema_meta_question("What is the total revenue?")

    def test_empty_string_not_meta(self):
        assert not is_schema_meta_question("")


class TestAnswerSchemaQuestion:
    def test_returns_llm_output_stripped(self):
        answer = answer_schema_question(
            "What tables are there?",
            "TABLE orders (order_id BIGINT)",
            llm_fn=lambda _: "  There is one table: orders.  ",
        )
        assert answer == "There is one table: orders."

    def test_prompt_includes_schema_and_question(self):
        captured = {}

        def _fake_llm(prompt_text: str) -> str:
            captured["prompt"] = prompt_text
            return "answer"

        answer_schema_question("What columns does orders have?", "TABLE orders (order_id BIGINT)", llm_fn=_fake_llm)
        assert "TABLE orders" in captured["prompt"]
        assert "What columns does orders have?" in captured["prompt"]

    def test_prompt_forbids_outside_knowledge(self):
        captured = {}

        def _fake_llm(prompt_text: str) -> str:
            captured["prompt"] = prompt_text
            return "answer"

        answer_schema_question("q", "TABLE t (a INT)", llm_fn=_fake_llm)
        assert "ONLY" in captured["prompt"] or "only" in captured["prompt"]
