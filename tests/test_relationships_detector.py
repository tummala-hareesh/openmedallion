"""tests/test_relationships_detector.py — RAG roadmap Phase 1, build order
step 5: `medallion relationships generate` detection rules
(openmedallion.relationships.detector).

Pure, no I/O — each rule operates on plain dicts of table -> columns (or
table -> [(column, dtype)] for the grain rule, which needs dtype to restrict
itself to string-typed dimension columns).
"""
import pytest

from openmedallion.relationships.detector import (
    _pluralize,
    _singularize,
    _detect_fk_naming,
    _detect_lineage,
    _detect_grain,
    detect_relationships,
)


class TestPluralizeSingularize:

    @pytest.mark.parametrize("word,plural", [
        ("job", "jobs"),
        ("department", "departments"),
        ("employee", "employees"),
        ("company", "companies"),
        ("class", "classes"),
    ])
    def test_pluralize(self, word, plural):
        assert _pluralize(word) == plural

    @pytest.mark.parametrize("plural,word", [
        ("jobs", "job"),
        ("departments", "department"),
        ("employees", "employee"),
        ("companies", "company"),
        ("classes", "class"),
    ])
    def test_singularize(self, plural, word):
        assert _singularize(plural) == word


class TestDetectFkNaming:

    def test_resolves_target_table_by_pluralized_entity(self):
        tables = {
            "employees":   ["employee_id", "department_id"],
            "departments": ["department_id", "department_name"],
        }
        rels = _detect_fk_naming(tables)
        assert len(rels) == 1
        rel = rels[0]
        assert rel.from_table == "employees"
        assert rel.to_table == "departments"
        assert rel.join_on == ["department_id"]
        assert rel.method == "fk_naming"
        assert rel.confidence == "high"

    def test_column_in_only_one_table_produces_nothing(self):
        tables = {"employees": ["employee_id", "salary"]}
        assert _detect_fk_naming(tables) == []

    def test_unresolvable_entity_produces_nothing(self):
        # "manager_id" doesn't match any table name via pluralization —
        # isolated from any resolvable column so this test targets only that case.
        tables = {
            "employees":          ["manager_id", "salary"],
            "employees_enriched": ["manager_id", "salary"],
        }
        assert _detect_fk_naming(tables) == []

    def test_target_table_does_not_get_relationship_to_itself(self):
        tables = {"departments": ["department_id", "department_name"]}
        assert _detect_fk_naming(tables) == []

    def test_multiple_referencing_tables_to_same_target(self):
        tables = {
            "employees":          ["employee_id", "department_id"],
            "employees_enriched": ["employee_id", "department_id", "department_name"],
            "departments":        ["department_id", "department_name"],
        }
        rels = _detect_fk_naming(tables)
        pairs = {(r.from_table, r.to_table) for r in rels}
        assert ("employees", "departments") in pairs
        assert ("employees_enriched", "departments") in pairs


class TestDetectLineage:

    def test_proper_subset_produces_relationship_from_bigger_to_smaller(self):
        tables = {
            "departments":        ["department_id", "department_name"],
            "employees_enriched": ["employee_id", "department_id", "department_name"],
        }
        rels = _detect_lineage(tables)
        assert len(rels) == 1
        rel = rels[0]
        assert rel.from_table == "employees_enriched"
        assert rel.to_table == "departments"
        assert rel.method == "lineage"
        assert rel.confidence == "medium"

    def test_uses_singular_pk_column_as_join_on_when_present(self):
        tables = {
            "departments":        ["department_id", "department_name"],
            "employees_enriched": ["employee_id", "department_id", "department_name"],
        }
        rels = _detect_lineage(tables)
        assert rels[0].join_on == ["department_id"]

    def test_falls_back_to_full_shared_columns_without_matching_pk(self):
        tables = {
            "a": ["region", "status"],
            "b": ["region", "status", "amount"],
        }
        rels = _detect_lineage(tables)
        assert len(rels) == 1
        assert rels[0].join_on == ["region", "status"]

    def test_equal_column_sets_produce_nothing(self):
        tables = {
            "a": ["x", "y"],
            "b": ["x", "y"],
        }
        assert _detect_lineage(tables) == []

    def test_disjoint_columns_produce_nothing(self):
        tables = {
            "a": ["x", "y"],
            "b": ["z", "w"],
        }
        assert _detect_lineage(tables) == []


class TestDetectGrain:

    def test_shared_varchar_column_produces_relationship(self):
        typed = {
            "departments":           [("department_id", "BIGINT"), ("department_name", "VARCHAR")],
            "headcount_by_dept":     [("department_name", "VARCHAR"), ("headcount", "UINTEGER")],
        }
        rels = _detect_grain(typed)
        assert len(rels) == 1
        rel = rels[0]
        assert rel.join_on == ["department_name"]
        assert rel.method == "grain"
        assert rel.confidence == "low"

    def test_id_suffixed_columns_excluded_even_if_varchar(self):
        # job_id is VARCHAR in the real oracle_hr dataset — must still be
        # excluded from grain (fk_naming's territory, not grain's).
        typed = {
            "jobs":      [("job_id", "VARCHAR"), ("job_title", "VARCHAR")],
            "employees": [("job_id", "VARCHAR"), ("salary", "DOUBLE")],
        }
        rels = _detect_grain(typed)
        assert all("job_id" not in r.join_on for r in rels)

    def test_numeric_shared_columns_excluded(self):
        typed = {
            "a": [("amount", "DOUBLE")],
            "b": [("amount", "DOUBLE")],
        }
        assert _detect_grain(typed) == []

    def test_column_in_only_one_table_produces_nothing(self):
        typed = {"a": [("region", "VARCHAR")]}
        assert _detect_grain(typed) == []

    def test_direction_more_columns_is_from_table(self):
        typed = {
            "small": [("region", "VARCHAR")],
            "big":   [("region", "VARCHAR"), ("amount", "DOUBLE"), ("qty", "BIGINT")],
        }
        rels = _detect_grain(typed)
        assert rels[0].from_table == "big"
        assert rels[0].to_table == "small"

    def test_three_way_shared_column_produces_all_pairs(self):
        typed = {
            "a": [("region", "VARCHAR")],
            "b": [("region", "VARCHAR"), ("x", "DOUBLE")],
            "c": [("region", "VARCHAR"), ("x", "DOUBLE"), ("y", "DOUBLE")],
        }
        rels = _detect_grain(typed)
        assert len(rels) == 3


class TestDetectRelationshipsOrchestration:

    def test_dedups_identical_identity_preferring_fk_naming(self):
        # employees ⊆ employees_enriched by columns, AND employees_id
        # resolves via pluralization to table "employees" — same
        # (from,to,join_on) identity found by both fk_naming and lineage.
        typed = {
            "employees":          [("employee_id", "BIGINT")],
            "employees_enriched": [("employee_id", "BIGINT"), ("extra", "DOUBLE")],
        }
        rels = detect_relationships(typed)
        matching = [r for r in rels if r.from_table == "employees_enriched" and r.to_table == "employees"]
        assert len(matching) == 1
        assert matching[0].method == "fk_naming"

    def test_distinct_join_on_between_same_pair_both_kept(self):
        typed = {
            "departments":        [("department_id", "BIGINT"), ("department_name", "VARCHAR")],
            "employees_enriched": [
                ("employee_id", "BIGINT"), ("department_id", "BIGINT"), ("department_name", "VARCHAR"),
            ],
        }
        rels = detect_relationships(typed)
        join_ons = {tuple(r.join_on) for r in rels
                    if r.from_table == "employees_enriched" and r.to_table == "departments"}
        assert ("department_id",) in join_ons
        assert ("department_name",) in join_ons
