# Changelog

All notable changes to openmedallion are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versions follow [Semantic Versioning](https://semver.org/).

---

## [Unreleased]

---

## [2026.6.9] — 2026-06-14 (silver transforms + gold utilities)

### Added

- **Declarative silver transforms** — 8 new built-in transform types in `pipeline/silver.py:_apply()`, removing the need for UDFs on common operations. All types are validated by `config/validator.py` and documented in `docs/reference/yaml-schema.md`:

  | Type | What it does |
  | --- | --- |
  | `fillna` | Fill nulls per column with a literal value |
  | `clip` | Clamp numeric **or** `Date`/`Datetime` columns to `min`/`max` bounds (ISO string bounds for dates) |
  | `normalize` | String standardisation: `upper`, `lower`, `strip`, `strip_lower` |
  | `deduplicate` | Remove duplicate rows; optional `subset` and `keep` (`first`/`last`/`none`) |
  | `filter_rows` | Keep rows matching a SQL expression via `pl.sql_expr` |
  | `map_values` | Categorical replacement dict with optional `default` for unmatched values |
  | `allowed_values` | String allowlist — values not in the list become `null` (row is kept) |
  | `coerce_bool` | Coerce `true`/`yes`/`1`/`on` → `True`, `false`/`no`/`0`/`off` → `False`, else `null` (case-insensitive; already-bool columns pass through) |

- **Declarative gold utilities** — post-aggregation controls and extended aggregation functions in `pipeline/gold.py:_apply_agg()`. Execution order: `group_by → having → sort → limit`:

  - `having` — SQL expression filter applied after aggregation (equivalent to SQL `HAVING`)
  - `sort` — order result by one or more columns with `descending: true/false`
  - `limit` — keep the top N rows (composes with `sort`)
  - Extended `AGG_MAP`: `median`, `std`, `var`, `first`, `last`, `count_distinct` added alongside the existing `count`, `sum`, `mean`, `min`, `max`

- **`ecommerce_analytics_demo` updated** — `silver.yaml` now demonstrates `allowed_values`, `normalize`, and `fillna` on the customers table; `gold.yaml` uses `having`, `sort`, `limit`, `median`, `std`, and `count_distinct` on existing aggregations

- **`docs/reference/yaml-schema.md` updated** — full tabbed reference for all 12 silver transform types; `aggregations[]` table extended with `having`/`sort`/`limit`; `metrics[]` table now lists all 11 `agg` values with descriptions

---

## [2026.6.9] — 2026-06-12

### Fixed

- **Bronze `merge` mode actually merges** — `_collect_parquets()` now deduplicates on `primary_key` (keeping the last shard's version of each row) when `incremental.mode == "merge"` and multiple shards are present. Previously, each pipeline run appended new shards and `pl.concat` produced duplicate rows for tables like `folderprocess` and `folderprocessattempt`. (`bronze.py: _collect_parquets`)

### Added

- **Declarative DuckDB registration for silver and gold layers** — add a `duckdb:` block inside `bronze_to_silver:` or `silver_to_gold:` in the layer YAML to automatically write a `.duckdb` file after the layer runs. Two modes: `views` (lightweight pointer, data stays in Parquet — default, recommended for local use and `cerebrum`) and `tables` (data embedded, file is self-contained and shareable standalone — recommended for gold). All gold project subdirectories are registered into a single file. Implemented in new `pipeline/duckdb_views.py`; `silver.py`, `gold.py`, `config/validator.py` updated. Example YAML added to `sales_intelligence_demo`. (`pipeline/duckdb_views.py`, `pipeline/silver.py`, `pipeline/gold.py`, `config/validator.py`)

- **Early table-name validation in `_probe_connection()`** — configured table names from `bronze.yaml` are now cross-checked against actual schema tables immediately after connecting. Missing tables raise `ValueError: [bronze] table(s) not found in schema X: [...]. Available: [...]` before any data movement starts. Each configured table is also printed with ✅/❌ during the probe. (`bronze.py: _probe_connection`)

- **`filter:` + `filter_propagate:` can now be combined** on the same table entry. Previously a `ValueError` was raised if both were set. Now the propagated subquery (`key IN (SELECT key FROM ref WHERE ref_filter)`) is prepended and the explicit `filter:` is appended with `AND`. (`bronze.py: _sql_source`)

---

## [2026.6.5] — 2026-06-05

### Added

- **cortex UI — Data Studio Pro redesign** — complete visual overhaul targeting business users:
  - `cortex/theme.py` — centralised design token module: teal accent palette, sidebar colours, WCAG AA text contrast tokens, `CHART_PALETTE`, `FONT_UI` (DM Sans), `FONT_MONO` (JetBrains Mono), `nav_style()` / `panel_style()` helpers
  - `cortex/assets/cortex.css` — Google Fonts import, body/scrollbar reset, z-index fix for Dash dropdowns
  - `cortex/viz.py` — dynamic VizEngine: `build_dashboard()` selects chart layout from data shape and question intent (trend → line chart(s); distribution → pie+bar; comparison → bar(s); aggregate/categorical → bar+pie; single-row → big centred KPIs; pure-numeric → histograms); intent detected from question keywords; temporal columns detected by Polars dtype or column name patterns

- **cortex app shell redesign** (`cortex/app.py`):
  - Dark sidebar (220 px) with brand, Bootstrap Icons navigation, and LLM status footer
  - `dcc.Store`-driven tab routing — all panels always in DOM, shown/hidden via style callback
  - Topbar title updates on nav switch; "Table", "Chat", "Dashboard" labels

- **Chat tab redesign** (`cortex/tabs/chat.py`):
  - Empty state with 3 example chip buttons that pre-fill the textarea
  - Conversation layout: user bubble (teal-tinted, right-aligned) / assistant bubble (white card, left-aligned)
  - Collapsible SQL via native HTML5 `<details>`/`<summary>` — no JavaScript required
  - Suggested follow-up prompt displayed below the input after each answer

- **Table tab redesign** (`cortex/tabs/table.py`):
  - Collapsible SQL panel (`dbc.Collapse`) with "Show SQL ▸ / Hide SQL ▾" toggle
  - Row count label and CSV/Excel download buttons repositioned to top-right of table
  - Numeric columns auto-detected and right-aligned via `style_cell_conditional`
  - Suggested follow-up prompt shown above the table

- **Dashboard tab redesign** (`cortex/tabs/dashboard.py`):
  - Fully dynamic layout powered by `viz.build_dashboard()` — chart selection adapts to every query result
  - Region / Category filter dropdowns with persistent labels; refresh timestamp; Export PDF button

### Changed

- `cortex/charts.py` — updated `bar_chart`, `line_chart`, `pie_chart` to use `CHART_PALETTE` and shared `_LAYOUT_BASE` (DM Sans font, white background, compact margins); added `empty_fig()` helper

### Fixed

- `medallion_audit.jsonl` added to `.gitignore` — runtime audit log was unintentionally tracked

---

## [2026.6.3] — 2026-06-05

### Added

- **Multi-provider LLM support** — `cerebrum` now works with any LLM backend, not just local Ollama:
  - `"openrouter"` — OpenRouter.ai (access to GPT-4o, Claude 3.5 Sonnet, Llama 3, Mistral, etc.)
  - `"openai"` — OpenAI API directly
  - Any OpenAI-compatible endpoint (LM Studio, Groq, vLLM, Anyscale, Together AI…) via `base_url`
  - `"ollama"` remains the default — no breaking change for existing local setups
- `cerebrum/llm.py` — `LLMClient` Protocol, `OllamaClient`, `OpenAICompatibleClient`, and `get_client(provider, model, *, api_key, base_url)` factory
- `CerebrumPipeline` — new `provider=`, `api_key=`, and `base_url=` constructor parameters; `ollama_base_url` removed
- `settings.py` — three new settings: `LLM_PROVIDER` (`MEDALLION_LLM_PROVIDER`), `LLM_API_KEY` (`MEDALLION_LLM_API_KEY`), `LLM_BASE_URL` (`MEDALLION_LLM_BASE_URL`)
- `settings.yaml` — new `llm.provider`, `llm.api_key`, and `llm.base_url` keys
- `medallion query` / `medallion ask` — new `--provider` flag to select the LLM backend at the CLI

### Changed

- `neuron` 503 error message is now provider-aware: Ollama gets "Start it with: ollama serve"; other providers get a generic connectivity message
- `neuron` now returns HTTP 503 (not 500) on 401/403 from LLM provider — surfaces authentication failures clearly
- `examples/settings.yaml.example` updated to document all provider options with commented examples

---

## [2026.6.2] — 2026-06-05

### Added

- `medallion query <project> "<question>"` — ask a natural-language question directly in the terminal without starting a server; prints SQL, result table, and recommended prompt
- `config/settings.py` — single source of truth for all `MEDALLION_*` environment variables; supports layered config: env var → `settings.yaml` → built-in default
- `settings.yaml` file support — copy `examples/settings.yaml.example` to project root or `~/.medallion/settings.yaml`; configure LLM model, Ollama URL, rate limit, audit log, API key, and mock client flag without env vars
- `examples/settings.yaml.example` — template for runtime configuration
- `CerebrumPipeline.ask()` `on_step` callback — optional `Callable[[str], None]` parameter; fires before each blocking step (schema build, SQL generation, validation retries, execution, recommended prompt) so callers can surface live progress
- `medallion ask` startup now prints `/docs` (Swagger UI) and `/health` URLs alongside the server URL

### Fixed

- `cortex` CSV download raised 500 — `download_csv` returned a raw dict missing the `base64` field required by Dash 4; fixed to use `dcc.send_string()`
- `cortex` chat spinner never appeared — `dbc.Spinner` wrapped a div that was never a callback output; replaced with `dcc.Loading` wrapping `chat-history` which is updated by the ask callback
- `neuron` returned HTTP 500 with raw `[Errno 111] Connection refused` when Ollama was not running — now returns HTTP 503 with "Ollama is not reachable at … Start it with: ollama serve"
- `cortex` chat bubble showed raw `httpx.HTTPStatusError` URL string on server errors — `client.ask()` now reads `r.json().get("detail")` and raises `RuntimeError(detail)` so the meaningful message is displayed
- `make lint` failed on Python 3.14 — `ydata-profiling` and `pygwalker` were duplicated in main `dependencies`, pulling in `numba` which blocks Python 3.14; removed from main deps (remain in `[profile]` and `[explore]` optional extras)
- Redundant `pl.Utf8` alias removed from `cortex/tabs/dashboard.py` (`pl.Utf8 == pl.String` in Polars 1.x)

### Added (explore layer — carried from previous unreleased work)

- Inline `explore:` key on tables in `bronze.yaml`, `silver.yaml`, and `gold.yaml` — attach report specs directly to any table; reports are generated immediately after that layer writes
- `openmedallion/explore/` module — `profile.py` (ydata-profiling HTML reports) + `walker.py` (pygwalker interactive explorer)
- `pipeline/explore.py` — `ExploreGenerator` + shared `_dispatch_reports()` helper; called by Bronze, Silver, and Gold after each write; optional deps imported lazily
- `openmedallion[profile]` optional extra (`ydata-profiling>=4.0`)
- `openmedallion[explore]` optional extra (`pygwalker>=0.4`)
- `--layer explore` CLI flag — re-generates HTML reports from existing Parquet files without re-running upstream layers
- Report output convention — co-located with layer data under `add-ons/` subdirectory

### Removed (explore layer — carried from previous unreleased work)

- `openmedallion/viz/` module (`server.py`, `tracker.py`, `notebook.py`, `dag.py`)
- `fastapi`, `uvicorn`, `websockets`, `panel`, `jupyter_bokeh` core dependencies
- `viz` optional extra from `pyproject.toml`
- `dag`, `visualize`, `status`, `--track` CLI commands

---

## [2026.5.4] — 2026-05-14

### Added

- `select:` — column projection at bronze ingestion (SQL: pushed to DB via `query_adapter_callback`; local_files: Polars post-read; filesystem/REST: Polars post-shard)
- `credentials_file:` + `dialect:` — structured credential YAML; builds SQLAlchemy URL internally
- Connection probe — `_probe_connection()` tests connectivity and lists schema tables before ingestion
- Shared `examples/secrets.yaml` pattern — one file at `examples/` level; SQLite demos use `connection_string` directly
- `make examples` runner (`examples/run_examples.py`) — cleans, seeds, and runs all 4 examples; reports PASS/FAIL with timing
- GitLab CI fixes (`uv sync --group dev`, `uv.lock` cleanup)

---

## [2026.5.1] — 2026-05-14

### Added

- GitHub Actions CI workflow (multi-Python matrix, lint)
- GitHub Actions publish workflow (TestPyPI → PyPI via OIDC trusted publishing)
- Expanded `medallion init` scaffold: `backend/`, `frontend/`, `data/` (gitignored),
  `summary/`, and full `README.md` template
- `oracle_hr_demo` example — Oracle HR schema (employees/departments/jobs), bronze SQL
  filter, silver three-table join UDF, gold salary analytics (headcount, salary bands,
  salary utilisation with pre-agg UDF)
- `bronze.yaml` `filter` field — SQL WHERE clause pushed to dlt ingestion via
  `query_adapter_callback`; rows excluded at source before entering the data lake
- `openmedallion[oracle]` optional extra (`oracledb>=1.0`) for real Oracle connections

---

## [2026.4.1] — 2026-04-22

### Added

- `openmedallion.config` — `load_project`, `expand_env_str`, `_deep_merge`, `_validate_config`
- `openmedallion.contracts.udf` — `load_udf`, `check_return`
- `openmedallion.pipeline` — `BronzeLoader`, `SilverTransformer`, `GoldAggregator`, `BIExporter`
- `openmedallion.pipeline.nodes` — Hamilton DAG node functions
- `openmedallion.storage` — `read_parquet`, `write_parquet`, `write_csv`, `join`, `exists`,
  `mkdir`, `ls_parquets`, `copy`, `is_s3`, `storage_opts`
- `openmedallion.helpers.joins` — `join_tables`, `lookup_join`, `safe_join`, `multi_join`,
  `asof_join`, `cross_join_filtered`
- `openmedallion.helpers.windows` — `rank_within`, `row_number`, `running_total`, `lag_column`,
  `lead_column`, `pct_of_total`, `rolling_avg`, `first_last_within`
- `openmedallion.helpers.aggregations` — `attach_group_stats`, `top_n_within`,
  `pivot_to_columns`, `unpivot_columns`, `flag_outliers`
- `openmedallion.helpers.dates` — `date_trunc`, `days_between`, `classify_recency`,
  `add_calendar_columns`
- `openmedallion.scaffold.templates` — `init_project`
- `medallion` CLI — `run`, `init` subcommands
- S3 support via `openmedallion[s3]` optional extra (s3fs + boto3)
- LocalStack compatibility via `AWS_ENDPOINT_URL` environment variable

[Unreleased]: https://github.com/tummala-hareesh/openmedallion/compare/v2026.6.9...HEAD
[2026.6.9]: https://github.com/tummala-hareesh/openmedallion/compare/v2026.6.2...v2026.6.9
[2026.6.2]: https://github.com/tummala-hareesh/openmedallion/compare/v2026.5.4...v2026.6.2
[2026.5.4]: https://github.com/tummala-hareesh/openmedallion/compare/v2026.5.1...v2026.5.4
[2026.5.1]: https://github.com/tummala-hareesh/openmedallion/compare/v2026.4.1...v2026.5.1
[2026.4.1]: https://github.com/tummala-hareesh/openmedallion/releases/tag/v2026.4.1
