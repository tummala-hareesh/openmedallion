"""tests/test_cerebrum_decomposition.py — RAG roadmap Phase 2, build order
step 12: ambiguity detection + query decomposition (openmedallion.cerebrum.decomposition).

Locked design decisions (see CLAUDE.md "Roadmap: RAG Accuracy Improvement"):
- Both detect_ambiguity() and decompose_question() are pure functions taking
  an injectable llm_fn — no pipeline state, matching the module-per-concern
  pattern already used by validator.py/retrieval.py/recommender.py.
- decompose_question() never raises on a malformed LLM response — falls back
  to treating the question as a single (non-decomposed) question, since a
  parsing failure here shouldn't break an otherwise-working query.
"""
from __future__ import annotations

from unittest.mock import MagicMock

from openmedallion.cerebrum.decomposition import decompose_question, detect_ambiguity


class TestDetectAmbiguity:

    def test_clear_returns_none(self):
        llm_fn = MagicMock(return_value="CLEAR")
        assert detect_ambiguity("How many orders were placed?", "TABLE orders (...)", llm_fn) is None

    def test_clear_case_insensitive_with_trailing_period(self):
        llm_fn = MagicMock(return_value="clear.")
        assert detect_ambiguity("q", "schema", llm_fn) is None

    def test_ambiguous_returns_clarification_text(self):
        llm_fn = MagicMock(return_value="Which region do you mean — sales region or shipping region?")
        result = detect_ambiguity("Show me revenue by region", "TABLE orders (...)", llm_fn)
        assert result == "Which region do you mean — sales region or shipping region?"

    def test_prompt_includes_question_and_schema(self):
        llm_fn = MagicMock(return_value="CLEAR")
        detect_ambiguity("my question", "TABLE orders (order_id BIGINT)", llm_fn)
        prompt = llm_fn.call_args.args[0]
        assert "my question" in prompt
        assert "TABLE orders" in prompt


class TestDecomposeQuestion:

    def test_single_returns_original_question(self):
        llm_fn = MagicMock(return_value="SINGLE")
        assert decompose_question("How many orders?", llm_fn) == ["How many orders?"]

    def test_single_case_insensitive_with_trailing_period(self):
        llm_fn = MagicMock(return_value="single.")
        assert decompose_question("q", llm_fn) == ["q"]

    def test_valid_json_array_returns_sub_questions(self):
        llm_fn = MagicMock(return_value='["What is total revenue?", "How many customers are there?"]')
        result = decompose_question("What is total revenue and how many customers are there?", llm_fn)
        assert result == ["What is total revenue?", "How many customers are there?"]

    def test_markdown_fenced_json_is_parsed(self):
        llm_fn = MagicMock(return_value='```json\n["a", "b"]\n```')
        assert decompose_question("q", llm_fn) == ["a", "b"]

    def test_invalid_json_falls_back_to_single_question(self):
        llm_fn = MagicMock(return_value="not valid json at all")
        assert decompose_question("original question", llm_fn) == ["original question"]

    def test_json_object_instead_of_array_falls_back(self):
        llm_fn = MagicMock(return_value='{"question": "a"}')
        assert decompose_question("original question", llm_fn) == ["original question"]

    def test_empty_array_falls_back(self):
        llm_fn = MagicMock(return_value="[]")
        assert decompose_question("original question", llm_fn) == ["original question"]

    def test_array_with_non_string_elements_falls_back(self):
        llm_fn = MagicMock(return_value="[1, 2]")
        assert decompose_question("original question", llm_fn) == ["original question"]
