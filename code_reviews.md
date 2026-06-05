# OpenMedallion Code Review — v2026.5.1

**Reviewer:** Senior Python Developer (Claude Sonnet 4.6)
**Date:** 2026-05-07
**Scope:** Full codebase review — `openmedallion/` package only
**Constraint:** Read-only analysis. No code changes made.

---

## Table of Contents

1. [High-End Python Skills](#1-high-end-python-skills)
2. [Ease of Use](#2-ease-of-use)
3. [Test Coverage](#3-test-coverage)
4. [Automated Workflow (CI/CD)](#4-automated-workflow-cicd)
5. [Documentation](#5-documentation)
6. [Unused & Redundant Code](#6-unused--redundant-code)
7. [Heavy or Replaceable Packages](#7-heavy-or-replaceable-packages)
8. [Design Layout Simplifications](#8-design-layout-simplifications)
9. [Lessons & Articles](#9-lessons--articles)
10. [TO DO Action Table](#10-to-do-action-table)

---

## 1. High-End Python Skills

### What is done well

| Pattern | Location | Why it matters |
|---------|----------|----------------|
| `match` statement for source/dest routing | `bronze.py` | Idiomatic Python 3.10+ structural pattern matching instead of `elif` chains |
| Walrus operator (`:=`) | `loader.py` | Tight loop logic with readable one-line assignment + branch |
| `importlib.util` for dynamic UDF loading | `silver.py` | Allows arbitrary user-written `.py` modules to be loaded at runtime without polluting `sys.modules` |
| Lambda closure fix via default arg | `bronze.py:207` | `lambda sel, _t, _sql=filter_clause: sel.where(_sa_text(_sql))` — correctly captures per-iteration value, avoids classic loop-closure bug |
| `_deep_merge()` recursive YAML merge | `loader.py` | Clean functional recursion; handles nested dicts without mutation side-effects |
| `_ENV_PATTERN` with named groups | `loader.py` | `re.compile(r"\$\{(\w+)(?::-(.*?))?\}")` handles both `${VAR}` and `${VAR:-default}` in one pass |
| Per-instance UDF module cache | `silver.py` | `_udf_cache: dict[str, object]` avoids re-importing the same file on every row-group |
| S3 lazy import with try/import guard | `fs.py` | `s3fs` and `boto3` only imported when an `s3://` path is encountered — keeps cold-start fast |
| Hamilton DAG wiring by argument names | `nodes.py` | Zero-boilerplate DAG: `silver(config, bronze)` implicitly runs after `bronze(config)` |
| `override` pattern for layer skipping | `nodes.py` + `cli/main.py` | Hamilton `overrides` dict is used to skip already-run layers without rewriting the DAG |

### Gaps and silent bugs

**Critical — `order_by` is accepted but never used in `windows.py`**

Five public functions (`running_total`, `lag_column`, `lead_column`, `rolling_avg`, `first_last_within`) accept an `order_by` parameter, store it to a local variable, but then never reference it in the Polars expression. Results are non-deterministic if the DataFrame is not pre-sorted by the caller.

```python
# windows.py — actual code
def running_total(df, col, partition_by, order_by, alias=None):
    partition = partition_by if isinstance(partition_by, list) else [partition_by]
    out_name = alias or f"running_{col}"
    return df.with_columns(
        pl.col(col).cum_sum().over(partition).alias(out_name)
        # order_by is NEVER referenced here
    )
```

Fix: either sort before `over()` (`df.sort(order_by).with_columns(...)`) or add a docstring stating data must be pre-sorted and remove the parameter.

**Type annotation mismatch in `bronze.py`**

`BronzeLoader.load()` returns `dict[str, str]` (string paths) but the Hamilton node annotation in `nodes.py` declares `-> dict[str, Path]`. Pyright flags this silently (because `|| true` swallows CI lint).

**No TypedDict for pipeline config**

The config dict passed everywhere is `dict[str, Any]`. A `TypedDict` or `dataclass` would give IDE autocomplete, catch key typos at static-analysis time, and serve as living documentation of the schema.

**No Protocol for UDF contracts**

User-written UDFs have no enforced signature. A `UDFTransform` or `UDFDerived` `Protocol` would let mypy/pyright verify user UDFs at import time instead of failing at runtime mid-pipeline.

---

## 2. Ease of Use

### Strong points

- `medallion init <project>` scaffolds a complete project in seconds — excellent developer experience
- Four-file YAML layout (`main.yaml`, `bronze.yaml`, `silver.yaml`, `gold.yaml`) is intuitive and well-segmented
- `${VAR:-default}` env expansion means users can commit non-secret configs and override at runtime
- `medallion run <project> --layer bronze|silver|gold` allows partial runs during development
- Optional extras (`pip install openmedallion[s3]`) keep the base install lightweight

### Issues

| Issue | File | Impact |
|-------|------|--------|
| README quickstart uses `medallion serve` | `README.md` | Actual command is `medallion status` — first-run users get `command not found` |
| Scaffold docstring mentions `frontend/streamlit/` | `templates.py` docstring | Code only creates `frontend/tableau/` and `frontend/powerbi/` — stale copy-paste |
| Inconsistent path key naming | bronze/silver/gold YAML examples | Some YAMLs use `bronze_path`, others `source_path` — confusing during first setup |
| `nodes.py` params mislead readers | `nodes.py` | `silver(config, bronze)` accepts `bronze` but never uses it — exists only for DAG ordering. Not obvious without reading the source |

---

## 3. Test Coverage

### Summary

| Module | Tests | Coverage |
|--------|-------|---------|
| `config/loader.py` | 31 | High — best-covered module |
| `pipeline/silver.py` | 16 | Good — all transform types + UDF patterns |
| `pipeline/gold.py` | 10 | Good |
| `contracts/udf.py` | ~12 | Adequate |
| `cli/main.py` | ~8 | Basic smoke tests |
| `helpers/windows.py` | ~12 | Unit coverage, but `order_by` bug untested |
| **`pipeline/bronze.py`** | **0** | **Complete gap** |
| **`storage/fs.py`** | **0** | **Complete gap** |
| **`viz/server.py`** | **0** | **Complete gap** |
| **`scaffold/templates.py`** | **0** | **Complete gap** |
| **Total** | **~133** | ~45% of modules untested |

### Critical gaps

**`bronze.py` (0 tests)** — The most complex module in the codebase handles 4 source types × 4 destinations. Any change to filter push-down, dlt configuration, or incremental cursor logic has no safety net.

**`fs.py` (0 tests)** — All S3 code paths are untested. The local-path branch works because examples use it, but the S3 branch could be silently broken.

**`scaffold/templates.py` (0 tests)** — If `medallion init` generates broken YAML or a corrupt notebook, users hit errors before they can run their first pipeline.

**`viz/server.py` (0 tests)** — FastAPI routes and SSE logic are completely untested.

### Running tests

```bash
cd openmedallion/
uv run pytest              # all tests
uv run pytest -x -q        # stop on first failure, quiet
uv run pytest --tb=short   # compact tracebacks
```

---

## 4. Automated Workflow (CI/CD)

### `.github/workflows/ci.yml`

| Finding | Severity |
|---------|----------|
| Workflow name is **"OpenMedallian"** — typo | Low |
| CI triggers on push to `develop` and PR to `master`/`develop` — **NOT on push to `master`** | High — a direct commit to master skips all CI |
| `uv run ruff check openmedallion/ \|\| true` — lint failures are **silently swallowed** | High — broken imports and type errors never fail the build |
| `uv run pyright openmedallion/ \|\| true` — type errors also swallowed | Medium |
| No coverage reporting or thresholds | Medium |
| No test parallelism across Python versions (only 3.12 tested) | Low |

### `.github/workflows/docs.yml`

| Finding | Severity |
|---------|----------|
| Workflow name is **"OpenMedallian"** — same typo | Low |
| Deploys docs on push to **`develop`** — should deploy on push to **`master`** | High — published docs will reflect unreleased work |

### Recommended CI trigger matrix

```yaml
on:
  push:
    branches: [master, develop]   # add master
  pull_request:
    branches: [master, develop]
```

---

## 5. Documentation

### Strong points

- `docs/architecture.md` contains Mermaid diagrams showing the full Bronze→Silver→Gold flow — excellent for onboarding
- Each example has an `ipynb/0_walkthrough.ipynb` with narrative cells explaining each step
- `pyproject.toml` keywords and classifiers are thorough and PyPI-friendly
- `CHANGELOG.md` exists and is maintained

### Issues

| Issue | Location | Fix |
|-------|----------|-----|
| CI badge URL uses `tummalaahri` | `README.md` line ~6 | Should be `tummala-hareesh` |
| `medallion serve` → `medallion status` | `README.md` quickstart | Wrong command name |
| `helpers/` module has no API docs page | `docs/` | Window functions, table utils, format helpers are undiscoverable |
| `nodes.py` DAG ordering trick not documented | `nodes.py` + docs | Confusing to contributors who wonder why Silver receives `bronze` and ignores it |
| `scaffold/templates.py` docstring stale | `templates.py` | References `streamlit/` which doesn't exist |

---

## 6. Unused & Redundant Code

### `websockets` dependency — never used

`websockets>=12.0` is listed in `pyproject.toml` core dependencies. A full grep of `openmedallion/` shows zero imports of `websockets` anywhere. The live dashboard uses SSE (Server-Sent Events) over plain HTTP, not WebSockets. This adds ~1 MB and a transitive dependency tree for no benefit.

**Action:** Remove from `pyproject.toml`.

### `order_by` parameter — accepted but silently ignored

In `helpers/windows.py`, five functions accept `order_by` as a parameter, assign it to a local variable, and then never reference it:

- `running_total()`
- `lag_column()`
- `lead_column()`
- `rolling_avg()`
- `first_last_within()`

This is a silent correctness bug, not just dead code. A user who passes `order_by="date"` trusts the function to sort — it does not.

### Jupyter checkpoint files inside the package

The following files exist inside `openmedallion/` subdirectories and will be bundled into the PyPI wheel:

```
openmedallion/cli/.ipynb_checkpoints/main-checkpoint.py
openmedallion/config/.ipynb_checkpoints/loader-checkpoint.py
openmedallion/config/.ipynb_checkpoints/validator-checkpoint.py
openmedallion/pipeline/.ipynb_checkpoints/bronze-checkpoint.py
openmedallion/pipeline/.ipynb_checkpoints/nodes-checkpoint.py
```

These are Jupyter auto-save artifacts. They may shadow the real modules in certain import scenarios and inflate the wheel size.

**Action:** Add `**/.ipynb_checkpoints` to both `.gitignore` and `[tool.hatch.build.targets.wheel]` exclude list.

### Empty `__init__.py` files

- `openmedallion/helpers/__init__.py` — empty (no exports)
- `openmedallion/viz/__init__.py` — empty (no exports)

Not a bug, but users have no import surface for these modules. Adding re-exports makes them discoverable via tab-completion.

---

## 7. Heavy or Replaceable Packages

| Package | Status | Recommendation |
|---------|--------|---------------|
| `websockets>=12.0` | **Unused** | Remove entirely from `pyproject.toml` |
| `fastapi>=0.115` + `uvicorn>=0.30` | Used only by `viz/server.py` (live dashboard) | Move to optional `[viz]` extra — users who never call `medallion status` don't need a web framework |
| `sf-hamilton>=1.82` | Core — used heavily | Keep. Hamilton is the backbone of the DAG; no lighter alternative |
| `dlt>=1.4` | Core — used heavily | Keep. `[filesystem,sql_database,parquet]` extras keep the install focused |
| `polars>=1.0` | Core — excellent choice | Keep. Polars is faster and leaner than pandas for this workload |
| `pyyaml>=6.0` | Core — used everywhere | Keep. `ruamel.yaml` would add comment-preservation but is heavier; not needed here |

**Net saving from `websockets` removal + moving `fastapi`/`uvicorn` to optional:** ~15 MB and 4 transitive packages removed from a base `pip install openmedallion`.

---

## 8. Design Layout Simplifications

### Single-file YAML option

Most small pipelines don't need four separate YAML files. A `pipeline.yaml` with `bronze:`, `silver:`, `gold:` sections that gets auto-split by `load_project()` would lower the barrier for new users, while the four-file layout remains available for larger projects.

### TypedDict / dataclass for config

Replace `dict[str, Any]` threading through every function with a `PipelineConfig` dataclass or `TypedDict`. This eliminates key-typo bugs, gives IDE autocomplete, and serves as schema documentation.

```python
# Proposed
@dataclass
class PipelineConfig:
    name: str
    bronze_path: Path
    silver_path: Path
    gold_path: Path
    storage: StorageConfig
    ...
```

### Cache S3 clients in `fs.py`

Every `read_parquet()` and `write_parquet()` call that hits S3 currently creates a new `s3fs.S3FileSystem` instance. A module-level `_s3fs_instance: s3fs.S3FileSystem | None = None` with lazy init would avoid repeated authentication round-trips in multi-table pipelines.

### Unify bronze code paths

`_local_files_load()` and `_collect_parquets()` in `bronze.py` are two independent code paths that both write to `bronze_path`. They share ~60% of their logic. Extracting a `_write_parquet_batch(df, dest)` helper would reduce the duplication.

### Protocol for UDF contracts

```python
# contracts/udf.py — proposed addition
from typing import Protocol
import polars as pl

class TransformUDF(Protocol):
    def transform(self, df: pl.DataFrame, config: dict) -> pl.DataFrame: ...

class DerivedUDF(Protocol):
    def derive(self, silver_path: str, config: dict) -> pl.DataFrame: ...
```

This lets mypy/pyright verify user-written UDFs at static analysis time.

---

## 9. Lessons & Articles

### Suggested learning path (in order)

1. **"Hello Bronze"** — A 5-minute quickstart: one CSV, one `bronze.yaml`, one `medallion run`. No Silver, no Gold, no databases. Just get something working.
2. **Incremental loads deep-dive** — Walk through the SQLite incremental demo step by step. Explain cursor columns, append vs merge modes, and how dlt tracks state.
3. **Window functions UDF cookbook** — Show `running_total`, `rolling_avg`, and `lag_column` with real data. Include the `order_by` requirement (pre-sort your data) and example patterns.
4. **Multi-source pipeline composition** — Combine a SQL source and a CSV source in a single `bronze.yaml`. Show how Silver joins them. This is the "power user" level.

### Article pitches

| Title | Target publication | Hook |
|-------|-------------------|------|
| "I replaced 400 lines of dbt + Airflow with 30 lines of YAML" | Towards Data Science / Medium | The "I built a thing" narrative that performs well on Medium |
| "The medallion pattern for the rest of us: open-source, no cloud required" | dev.to / Hashnode | Anti-Databricks / anti-cloud positioning — strong SEO |
| "Hamilton DAG + Polars = the perfect data transform stack" | PyCoders Weekly / Real Python | Technical depth piece targeting Python practitioners |
| "From CSV to BI dashboard in one Python command" | Analytics Vidhya / KDnuggets | Tutorial-first, result-first framing |
| "Testing declarative pipelines: a practical guide" | TestDriven.io | Test coverage is the current weak point — a guide would also drive improvements |

---

## 10. TO DO Action Table

Effort key: **T** = Trivial (< 30 min) | **S** = Small (< 4 h) | **M** = Medium (< 1 day) | **L** = Large (1–2 days)

| # | Task | File(s) | Effort | Time | % Gain | Priority |
|---|------|---------|--------|------|--------|---------|
| T1 | Fix `order_by` silently ignored in 5 window functions — wire `sort().over()` or document pre-sort requirement | `helpers/windows.py` | S | 1 h | +10% correctness | P1 — silent bug |
| T2 | Fix type annotation: `BronzeLoader.load()` returns `dict[str,str]` not `dict[str,Path]` | `pipeline/bronze.py`, `pipeline/nodes.py` | T | 15 min | +3% type safety | P3 |
| T3 | Remove unused `websockets>=12.0` from core dependencies | `pyproject.toml` | T | 5 min | +3% install size | P2 |
| T4 | Move `fastapi` and `uvicorn` to optional `[viz]` extra | `pyproject.toml` | T | 15 min | +5% install size | P2 |
| T5 | Add `TypedDict` or `dataclass` for `PipelineConfig` | `config/loader.py` | M | 4 h | +15% type safety | P2 |
| T6 | Enforce lint in CI — remove `|| true` from ruff and pyright steps | `.github/workflows/ci.yml` | T | 15 min | +8% build integrity | P1 |
| T7 | Add CI trigger on push to `master` | `.github/workflows/ci.yml` | T | 15 min | +8% build integrity | P1 |
| T8 | Fix workflow name typo: "OpenMedallian" → "OpenMedallion" (both workflows) | `ci.yml`, `docs.yml` | T | 5 min | +2% professionalism | P3 |
| T9 | Fix docs workflow trigger: deploy on `master` push, not `develop` | `.github/workflows/docs.yml` | T | 15 min | +5% docs reliability | P2 |
| T10 | Write unit tests for `BronzeLoader` — local_files, sql_database, filters | `tests/test_bronze.py` | L | 2 days | +20% confidence | P1 |
| T11 | Write tests for `storage/fs.py` S3 paths (use `moto` mock) | `tests/test_fs.py` | M | 4 h | +10% confidence | P2 |
| T12 | Write end-to-end integration test: full bronze→silver→gold run using SQLite fixture | `tests/test_integration.py` | M | 1 day | +15% confidence | P1 |
| T13 | Write tests for `scaffold/templates.py` — verify generated YAML is valid and notebook is parseable | `tests/test_scaffold.py` | S | 2 h | +8% confidence | P2 |
| T14 | Write smoke tests for `viz/server.py` FastAPI routes | `tests/test_viz.py` | S | 2 h | +5% confidence | P3 |
| T15 | Fix CI badge URL in README: `tummalaahri` → `tummala-hareesh` | `README.md` | T | 5 min | +2% credibility | P3 |
| T16 | Fix README quickstart: `medallion serve` → `medallion status` | `README.md` | T | 5 min | +3% DX | P2 |
| T17 | Fix `templates.py` docstring: remove `streamlit/` reference | `scaffold/templates.py` | T | 5 min | +2% accuracy | P3 |
| T18 | Add code comment in `nodes.py` explaining that upstream params exist for DAG ordering only | `pipeline/nodes.py` | T | 15 min | +5% contributor DX | P2 |
| T19 | Add `TransformUDF` and `DerivedUDF` `Protocol` classes to `contracts/udf.py` | `contracts/udf.py` | S | 2 h | +8% type safety | P3 |
| T20 | Exclude `.ipynb_checkpoints/` from wheel build in `pyproject.toml` and add to `.gitignore` | `pyproject.toml`, `.gitignore` | T | 15 min | +5% wheel cleanliness | P2 |
| T21 | Write "Hello Bronze" quickstart tutorial in `docs/` | `docs/quickstart.md` | M | 1 day | +15% adoption | P1 |
| T22 | Write article: "I replaced 400 lines of dbt+Airflow with 30 lines of YAML" | External publication | L | 2 days | +20% visibility | P1 |

### Effort summary

| Priority | Count | Total time |
|---------|-------|-----------|
| P1 (do first) | 7 | ~5.5 days |
| P2 (next sprint) | 9 | ~2 days |
| P3 (polish) | 6 | ~3 h |

### Top 5 by ROI (impact vs effort)

1. **T6 + T7** — Enforce lint + trigger CI on master push. Two config lines, zero risk, prevents silent regressions forever.
2. **T1** — Fix `order_by` bug in windows.py. 1 hour to eliminate silent non-determinism for all window UDF users.
3. **T3 + T4** — Remove `websockets`, move `fastapi`/`uvicorn` to optional. 20 minutes, cuts base install by ~15 MB.
4. **T10 + T12** — BronzeLoader tests + integration test. Highest-effort but highest coverage gain for the most-used code path.
5. **T21** — "Hello Bronze" quickstart. The single most impactful thing for adoption; new users currently have no fast on-ramp.

---

*Report generated from static analysis of `openmedallion/` v2026.5.1. No code was modified.*
