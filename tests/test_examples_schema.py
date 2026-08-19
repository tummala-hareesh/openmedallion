"""tests/test_examples_schema.py — Template-Routed Query Layer roadmap
(see CLAUDE.md), build order step 1: extend SyntheticExample with
`templated`/`params` fields.

Locked decisions under test:
- Backward compatible — every existing synthetic.jsonl line (no
  `templated`/`params` keys at all) must still validate unchanged, with
  `templated` defaulting False and `params` defaulting None.
- `templated: true` is a stricter claim than `verified: true` and must never
  be reachable without it — a template is a promotion of an already-verified
  example, never a shortcut around verification.
- `params`, when present, is a flat dict[str, str] (slot name -> human
  readable description/constraint), matching the `{ref:name}` /
  `filter_defs` substitution style already used in pipeline/bronze.py.
- extra="forbid" still applies — unknown keys must still be rejected.
"""
import pytest
from pydantic import ValidationError

from openmedallion.examples.schema import SyntheticExample


class TestBackwardCompatibleDefaults:

    def test_existing_line_without_new_fields_still_validates(self):
        ex = SyntheticExample(question="How many orders?", sql="SELECT COUNT(*) FROM orders")
        assert ex.templated is False
        assert ex.params is None

    def test_existing_verified_line_without_new_fields_still_validates(self):
        ex = SyntheticExample(
            question="Total revenue?", sql="SELECT SUM(amount) FROM orders", verified=True
        )
        assert ex.templated is False
        assert ex.params is None


class TestTemplatedRequiresVerified:

    def test_templated_true_with_verified_true_is_valid(self):
        ex = SyntheticExample(
            question="Revenue by region for {date_range}?",
            sql="SELECT region, SUM(amount) FROM orders WHERE order_date BETWEEN {date_range} GROUP BY region",
            verified=True,
            templated=True,
            params={"date_range": "ISO date range, e.g. 2026-Q1"},
        )
        assert ex.templated is True
        assert ex.params == {"date_range": "ISO date range, e.g. 2026-Q1"}

    def test_templated_true_with_verified_false_raises(self):
        with pytest.raises(ValidationError, match="templated"):
            SyntheticExample(
                question="Revenue by region?",
                sql="SELECT region, SUM(amount) FROM orders GROUP BY region",
                verified=False,
                templated=True,
            )

    def test_templated_true_with_verified_default_false_raises(self):
        with pytest.raises(ValidationError, match="templated"):
            SyntheticExample(
                question="Revenue by region?",
                sql="SELECT region, SUM(amount) FROM orders GROUP BY region",
                templated=True,
            )

    def test_templated_false_with_verified_false_is_valid(self):
        ex = SyntheticExample(question="q", sql="SELECT 1", verified=False, templated=False)
        assert ex.templated is False


class TestParamsShape:

    def test_params_omitted_defaults_none(self):
        ex = SyntheticExample(question="q", sql="SELECT 1", verified=True, templated=True, params=None)
        assert ex.params is None

    def test_params_accepts_flat_string_dict(self):
        ex = SyntheticExample(
            question="q",
            sql="SELECT * FROM orders WHERE region = {region}",
            verified=True,
            templated=True,
            params={"region": "one of: US, EU, APAC"},
        )
        assert ex.params == {"region": "one of: US, EU, APAC"}

    def test_params_present_without_templated_is_still_allowed(self):
        # params on a non-templated example is unusual but not forbidden —
        # only the templated=True + verified=False combination is invalid.
        ex = SyntheticExample(
            question="q", sql="SELECT 1", verified=True, templated=False, params={"x": "y"}
        )
        assert ex.params == {"x": "y"}


class TestExtraForbidUnchanged:

    def test_unknown_field_still_rejected(self):
        with pytest.raises(ValidationError):
            SyntheticExample(question="q", sql="SELECT 1", not_a_real_field=True)
