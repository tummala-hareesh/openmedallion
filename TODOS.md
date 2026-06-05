# TODOS

## T-TODO-1: Cross-check configured table names in `_probe_connection()`

**What:** In `_probe_connection()` at `bronze.py:306`, after fetching the table list from the DB, compare `source.tables[].name` against the discovered tables. Raise an early `ValueError` if any configured table is not found.

**Why:** Currently a typo like `emplyees` instead of `employees` in `bronze.yaml` passes config validation, passes the probe, and only surfaces as a silent warning in `_collect_parquets()` after dlt has already run. Early detection gives a clear error before any data movement.

**Context:** `_probe_connection()` at `bronze.py:306` already fetches and prints the full table list via `sa_inspect(engine).get_table_names(schema=schema)` at line 328. The cross-check is 3 additional lines after the table list is built. Error format: `[bronze] table 'emplyees' not found in schema HR. Available: ['employees', 'jobs', 'departments']`.

**Depends on / blocked by:** Independent of all other tasks.

---

## T-TODO-2: REST API multi-resource support

**What:** Extend `_rest_api_source()` to support a `resources:` list in `bronze.yaml`, analogous to the `tables:` list for SQL sources. Keep `resource:` (singular) as a backward-compatible alias for a single-item list.

**Why:** Currently `self.src["resource"]` is a single string. A user who wants to ingest 3 REST endpoints must create 3 separate pipeline projects. SQL sources support N tables; REST should too.

**Context:** `_rest_api_source()` at `bronze.py:386–404`. `_collect_parquets()` already handles multi-table name lookup at lines 411–413 via `table_names = [t["name"] for t in tables_cfg]` or `[self.src["resource"]]` fallback. The REST path would populate `table_names` from `resources:` list. Config change: `resource: str` → `resources: list[dict]` (with optional singular `resource: str` kept for backward compat).

**Depends on / blocked by:** Independent.
