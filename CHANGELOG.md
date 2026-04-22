# Changelog

All notable changes to openmedallion are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versions follow [Semantic Versioning](https://semver.org/).

---

## [Unreleased]

### Added
- GitHub Actions CI workflow (multi-Python matrix, lint)
- GitHub Actions publish workflow (TestPyPI → PyPI via OIDC trusted publishing)
- Expanded `medallion init` scaffold: `backend/`, `frontend/`, `data/` (gitignored),
  `catalogue/` (ERD + data dictionary), `summary/`, and full `README.md` template

---

## [0.1.0] — 2026-04-22

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
- `openmedallion.viz` — DAG visualiser, live-reload server, run tracker
- `medallion` CLI — `run`, `init`, `dag`, `serve`, `ui` subcommands
- S3 support via `openmedallion[s3]` optional extra (s3fs + boto3)
- LocalStack compatibility via `AWS_ENDPOINT_URL` environment variable

[Unreleased]: https://github.com/tummala-hareesh/openmedallion/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/tummala-hareesh/openmedallion/releases/tag/v0.1.0
