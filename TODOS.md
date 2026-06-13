# TODOS

## T-TODO-2: REST API multi-resource support

**What:** Extend `_rest_api_source()` to support a `resources:` list in `bronze.yaml`, analogous to the `tables:` list for SQL sources. Keep `resource:` (singular) as a backward-compatible alias for a single-item list.

**Why:** Currently `self.src["resource"]` is a single string. A user who wants to ingest 3 REST endpoints must create 3 separate pipeline projects. SQL sources support N tables; REST should too.

**Context:** `_rest_api_source()` at `bronze.py:386–404`. `_collect_parquets()` already handles multi-table name lookup at lines 411–413 via `table_names = [t["name"] for t in tables_cfg]` or `[self.src["resource"]]` fallback. The REST path would populate `table_names` from `resources:` list. Config change: `resource: str` → `resources: list[dict]` (with optional singular `resource: str` kept for backward compat).

**Depends on / blocked by:** Independent.

---

## T-TODO-3: Named filter fragments (`filter_defs`) in `bronze.yaml`

**What:** Allow users to define reusable SQL filter snippets once under a top-level `filter_defs:` key, then reference them by name inside any `filter:` string using a `{ref:name}` placeholder. The loader expands all `{ref:...}` tokens before passing the clause to SQLAlchemy.

**Why:** Complex pipelines repeat the same subquery in multiple tables. For example, the `processdesc LIKE '%Review%'` subquery currently appears verbatim in `validprocess.filter`, `folderprocess.filter`, and `folderprocessattempt.filter`. A change to that condition (e.g. adding another keyword) requires editing 3 places. Named fragments make it a single edit.

**Example `bronze.yaml`:**

```yaml
filter_defs:
  review_processcodes: >-
    processcode IN (
      SELECT vp.processcode FROM IBMS.validprocess vp
      WHERE vp.processdesc LIKE '%Review%'
    )

tables:
  - name: folderprocess
    filter_propagate: folder
    filter: "{ref:review_processcodes}"

  - name: folderprocessattempt
    filter_propagate: folder
    filter: >-
      processrsn IN (
        SELECT fp.processrsn FROM IBMS.folderprocess fp
        WHERE fp.{ref:review_processcodes}
      )
      AND resultcode IN (31,896,505,...)
```

**Next steps to implement:**

1. Parse `filter_defs:` from the top-level source config in `BronzeLoader.__init__` or `_sql_source()` — store as `dict[str, str]`.
2. Write a helper `_expand_filter_refs(clause: str, defs: dict) -> str` that replaces all `{ref:name}` tokens. Raise `ValueError` if a referenced name is not in `filter_defs`.
3. Call the helper on `filter_clause` after it is resolved (whether from `filter:` directly or built by `filter_propagate` logic) — before the `query_adapter_callback` lambda captures it.
4. Update the docstring at the top of `bronze.py` to document `filter_defs` and `{ref:name}` syntax.
5. Add a test: config with one `filter_defs` entry referenced in two tables — assert the expanded SQL matches expected string.

**Context:** `_sql_source()` at `bronze.py:430`. Filter resolution currently at lines 457–495. Expansion step slots in at line 496, before the `if filter_clause:` block.

**Depends on / blocked by:** Independent.
