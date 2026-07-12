"""tests/test_filter_defs.py — T-TODO-3: named filter fragments (filter_defs + {ref:name})."""
import pytest

from openmedallion.pipeline.bronze import _expand_filter_refs, BronzeLoader


# ---------------------------------------------------------------------------
# _expand_filter_refs — pure string substitution helper
# ---------------------------------------------------------------------------

class TestExpandFilterRefs:

    def test_no_refs_returns_unchanged(self):
        assert _expand_filter_refs("status = 'ACTIVE'", {}) == "status = 'ACTIVE'"

    def test_single_ref_expanded(self):
        defs = {"active_only": "status = 'ACTIVE'"}
        assert _expand_filter_refs("{ref:active_only}", defs) == "status = 'ACTIVE'"

    def test_ref_embedded_in_larger_clause(self):
        defs = {"active_only": "status = 'ACTIVE'"}
        result = _expand_filter_refs("{ref:active_only} AND region = 'US'", defs)
        assert result == "status = 'ACTIVE' AND region = 'US'"

    def test_same_ref_used_twice(self):
        defs = {"cond": "x > 0"}
        result = _expand_filter_refs("{ref:cond} AND {ref:cond}", defs)
        assert result == "x > 0 AND x > 0"

    def test_multiple_distinct_refs(self):
        defs = {"a": "x > 0", "b": "y < 10"}
        result = _expand_filter_refs("{ref:a} AND {ref:b}", defs)
        assert result == "x > 0 AND y < 10"

    def test_unknown_ref_raises(self):
        with pytest.raises(ValueError, match="review_processcodes"):
            _expand_filter_refs("{ref:review_processcodes}", {})

    def test_unknown_ref_raises_even_with_other_defs_present(self):
        with pytest.raises(ValueError, match="missing_one"):
            _expand_filter_refs("{ref:known} AND {ref:missing_one}", {"known": "x = 1"})


# ---------------------------------------------------------------------------
# BronzeLoader._resolve_filter_clause — combines filter_propagate + filter_defs,
# pure (no DB access), matches the TODOS.md worked example.
# ---------------------------------------------------------------------------

def _loader(source: dict) -> BronzeLoader:
    cfg = {
        "pipeline": {"name": "p"},
        "paths": {"bronze": "./b"},
        "sources": [source],
    }
    loader = BronzeLoader(cfg)
    loader.src = source
    return loader


class TestResolveFilterClause:

    def test_filter_with_ref_is_expanded(self):
        source = {
            "type": "sql_database",
            "filter_defs": {
                "review_processcodes": (
                    "processcode IN (SELECT vp.processcode FROM IBMS.validprocess vp "
                    "WHERE vp.processdesc LIKE '%Review%')"
                ),
            },
            "tables": [
                {"name": "folderprocess", "filter": "{ref:review_processcodes}"},
            ],
        }
        loader = _loader(source)
        tbl = source["tables"][0]
        result = loader._resolve_filter_clause(tbl, source["tables"], {}, None)
        assert result == source["filter_defs"]["review_processcodes"]

    def test_two_tables_share_one_filter_def(self):
        review = (
            "processcode IN (SELECT vp.processcode FROM IBMS.validprocess vp "
            "WHERE vp.processdesc LIKE '%Review%')"
        )
        source = {
            "type": "sql_database",
            "filter_defs": {"review_processcodes": review},
            "tables": [
                {"name": "folderprocess", "filter": "{ref:review_processcodes}"},
                {"name": "folderprocessattempt", "filter": "{ref:review_processcodes} AND resultcode IN (31,896)"},
            ],
        }
        loader = _loader(source)
        r1 = loader._resolve_filter_clause(source["tables"][0], source["tables"], {}, None)
        r2 = loader._resolve_filter_clause(source["tables"][1], source["tables"], {}, None)
        assert r1 == review
        assert r2 == f"{review} AND resultcode IN (31,896)"

    def test_filter_propagate_with_ref_in_referenced_filter(self):
        # folder.filter uses a ref; folderprocess propagates from folder —
        # the expansion must apply to the final combined clause.
        source = {
            "type": "sql_database",
            "filter_defs": {"active": "status = 'ACTIVE'"},
            "tables": [
                {"name": "folder", "filter": "{ref:active}"},
                {
                    "name": "folderprocess",
                    "filter_propagate": "folder",
                    "incremental": {"mode": "merge", "primary_key": "folder_id"},
                },
            ],
        }
        loader = _loader(source)
        tbl = source["tables"][1]
        inc = tbl["incremental"]
        result = loader._resolve_filter_clause(tbl, source["tables"], inc, None)
        assert result == "folder_id IN (SELECT folder_id FROM folder WHERE status = 'ACTIVE')"

    def test_no_filter_defs_block_is_ok(self):
        source = {
            "type": "sql_database",
            "tables": [{"name": "orders", "filter": "status = 'ACTIVE'"}],
        }
        loader = _loader(source)
        result = loader._resolve_filter_clause(source["tables"][0], source["tables"], {}, None)
        assert result == "status = 'ACTIVE'"

    def test_no_filter_returns_none(self):
        source = {"type": "sql_database", "tables": [{"name": "orders"}]}
        loader = _loader(source)
        result = loader._resolve_filter_clause(source["tables"][0], source["tables"], {}, None)
        assert result is None

    def test_unresolvable_ref_raises(self):
        source = {
            "type": "sql_database",
            "tables": [{"name": "orders", "filter": "{ref:nonexistent}"}],
        }
        loader = _loader(source)
        with pytest.raises(ValueError, match="nonexistent"):
            loader._resolve_filter_clause(source["tables"][0], source["tables"], {}, None)
