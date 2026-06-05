# Changelog

All notable changes to openmedallion are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versions follow [Semantic Versioning](https://semver.org/).

---

## [Unreleased]

### Added

- Inline `explore:` key on tables in `bronze.yaml`, `silver.yaml`, and `gold.yaml` — attach report specs directly to any table; reports are generated immediately after that layer writes
- `openmedallion/explore/` module — `profile.py` (ydata-profiling HTML reports) + `walker.py` (pygwalker interactive explorer)
- `pipeline/explore.py` — `ExploreGenerator` + shared `_dispatch_reports()` helper; called by Bronze, Silver, and Gold after each write; optional deps imported lazily
- `openmedallion[profile]` optional extra (`ydata-profiling>=4.0`)
- `openmedallion[explore]` optional extra (`pygwalker>=0.4`)
- `--layer explore` CLI flag — re-generates HTML reports from existing Parquet files without re-running upstream layers
- Report output convention — co-located with layer data under `add-ons/` subdirectory

### Removed

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

[Unreleased]: https://github.com/tummala-hareesh/openmedallion/compare/v2026.5.4...HEAD
[2026.5.4]: https://github.com/tummala-hareesh/openmedallion/compare/v2026.5.1...v2026.5.4
[2026.5.1]: https://github.com/tummala-hareesh/openmedallion/compare/v2026.4.1...v2026.5.1
[2026.4.1]: https://github.com/tummala-hareesh/openmedallion/releases/tag/v2026.4.1
