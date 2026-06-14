# TODOS

## Implementation Rules

- **TDD first** — write pytests before any code change. Tests must fail before implementation begins.
- **Docs + examples** — every implemented TODO must also update relevant `docs/` pages and at least one example in `examples/` to reflect the new capability.

---

## WON'T DO: Pandera for DataFrame schema validation

**Decision:** Do not add Pandera.

**Reasons:**
- Polars already raises on failed casts, missing columns, wrong dtypes — silent drift is not a real risk here.
- `select:` in `bronze.yaml` catches source column removal at ingestion time.
- cerebrum's DuckDB execution fails loudly if a gold column vanishes.
- `pandera[polars]` pulls in pandas transitively — too heavy for the core package.
- User-maintained schema blocks in YAML create a second place to update on every source change.
- `contracts/udf.py:check_return()` is sufficient for UDF boundary enforcement.

**Better alternative:** T-TODO-4 (Pydantic config schemas) catches errors before the pipeline runs — higher value, no runtime overhead.

---

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

---

## T-TODO-4: Pydantic schemas for `config/` layer

**What:** Replace the manual dict-based config handling and `config/validator.py` structural checks with Pydantic models covering all YAML config structures (bronze, silver, gold, settings, secrets).

**Why:** Currently, typos like `filter_propogate` silently pass through until runtime. Pydantic would catch them at load time with clear error messages, add IDE autocomplete, and make cross-field validation (e.g. `filter_propagate` referencing a valid table alias) declarative rather than imperative.

**Scope:**
- `BronzeConfig`, `SilverConfig`, `GoldConfig` — nested models for sources, tables, transforms, incremental modes, explore blocks.
- `SettingsConfig` — mirrors `config/settings.py` fields.
- Keep `config/validator.py` logic but express it as Pydantic `@model_validator` / `@field_validator` methods.
- Preserve backward compatibility — existing YAML files must load without changes.

**Depends on / blocked by:** Independent.

---

## T-TODO-5: Declarative silver transforms — fillna, clip, normalize, deduplicate, filter_rows, map_values

**What:** Extend `SilverTransformer._apply()` with commonly needed transform types so users avoid writing UDFs for simple operations.

**New transform types:**
- `fillna` — fill nulls per column with a literal value
- `clip` — clamp numeric columns to `min`/`max` range
- `normalize` — string standardisation (`upper`, `lower`, `strip`, `strip_lower`)
- `deduplicate` — `df.unique(subset, keep=first|last|none)`
- `filter_rows` — row-level SQL expression filter (drop sentinel/test rows)
- `map_values` — categorical replacement via a mapping dict with optional default

**Implementation order (TDD):**

1. **Write pytests first** — one test per transform type in `tests/`, using an in-memory `pl.DataFrame`. Assert output shape, dtypes, and values. Tests must fail before any code change.
2. **Extend `_apply()`** — add one `if t ==` branch per type in `pipeline/silver.py`. Each is 2–5 lines of native Polars.
3. **Update `config/validator.py`** — recognise and validate each new transform type's required keys.

**Extension point:** `pipeline/silver.py:_apply()` — single dispatch method, no new files needed.

**What stays as UDF:** multi-column derived logic, cross-table joins, complex conditional expressions.

**Depends on / blocked by:** Independent.

---

## T-TODO-6: Declarative gold utilities — sort, limit, having, extended AGG_MAP

**What:** Extend `GoldAggregator._apply_agg()` with post-aggregation utilities so users avoid writing `pre_agg_udf` for common analytical operations.

**New capabilities:**

- `sort` — order output by one or more columns (`descending: true/false`)
- `limit` — top-N rows (composes after `sort`)
- `having` — filter after aggregation via SQL expression string (equivalent to SQL HAVING)
- Extended `AGG_MAP` entries — `median`, `std`, `var`, `first`, `last`, `count_distinct`

**Execution order within `_apply_agg()`:**
```
group_by + metrics → having → sort → limit
```

**Example YAML:**
```yaml
- output_file: top_departments.parquet
  source_file: employees.parquet
  group_by: [department]
  metrics:
    - {agg: count,  alias: headcount}
    - {agg: sum,    column: salary, alias: total_salary}
  having: "headcount > 5"
  sort:
    columns: [total_salary]
    descending: true
  limit: 10
```

**What is NOT added:** `join` — cross-silver joins belong in silver `derived_tables` or `pre_agg_udf`, not gold.

**Implementation order (TDD):**

1. **Write pytests first** — one test per capability in `tests/`, using in-memory `pl.DataFrame`. Assert output shape, ordering, and values. Tests must fail before any code change.
2. **Extend `_apply_agg()`** — add `having`, `sort`, `limit` steps after the group_by block; add new keys to `AGG_MAP` in `pipeline/gold.py`.
3. **Update `config/validator.py`** — recognise and validate new keys.

**Extension point:** `pipeline/gold.py:_apply_agg()` and `AGG_MAP` — no new files needed.

**Depends on / blocked by:** Independent.
