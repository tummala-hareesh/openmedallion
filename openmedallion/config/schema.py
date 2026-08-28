"""config/schema.py — Pydantic models for a merged project config dict.

Mirrors the structural checks that used to live entirely in
``config/validator.py`` as imperative ``require()`` calls.  Using typed models
gets us three things imperative checks couldn't:

- ``extra="forbid"`` on every block — a typo like ``filter_propogate`` or
  ``pipline`` is rejected at load time instead of being silently ignored.
- Cross-field validation expressed declaratively (e.g. ``filter_propagate``
  must reference a table that exists and has a ``filter`` set).
- IDE autocomplete / type-checking on config objects if code chooses to work
  with the model directly instead of the merged dict.

``_validate_config`` (in ``validator.py``) builds a :class:`ProjectConfig` from
the merged dict and translates ``pydantic.ValidationError`` into the plain
``ValueError`` messages callers already depend on.
"""
from __future__ import annotations

import re
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

_VALID_DIALECTS      = {"oracle", "postgres", "mysql", "mssql", "sqlite"}
_VALID_NORMALIZE_OPS = {"upper", "lower", "strip", "strip_lower"}
_REF_PATTERN         = re.compile(r"\{ref:(\w+)\}")


class _Base(BaseModel):
    model_config = ConfigDict(extra="forbid")


def _non_empty_str(v: str, path: str) -> str:
    if not isinstance(v, str) or not v.strip():
        raise ValueError(f"'{path}' must be a non-empty string")
    return v


class PipelineBlock(_Base):
    name: str

    @field_validator("name")
    @classmethod
    def _name_non_empty(cls, v: str) -> str:
        return _non_empty_str(v, "pipeline.name")


class PathsBlock(_Base):
    bronze: str
    silver: str
    gold: str
    export: str


class ExploreSpec(_Base):
    report_type: Literal["profile", "walker"]
    output_file: str | None = None
    title: str | None = None
    minimal: bool | None = None


class IncrementalBlock(_Base):
    mode: Literal["replace", "append", "merge"] = "replace"
    cursor_column: str | None = None
    initial_value: Any = None
    primary_key: str | list[str] | None = None
    merge_key: str | list[str] | None = None


class DestinationBlock(BaseModel):
    # dlt destinations (bigquery, snowflake, ...) take backend-specific kwargs
    # not enumerated here — intentionally permissive.
    model_config = ConfigDict(extra="allow")
    type: Literal["filesystem", "duckdb", "bigquery", "snowflake"] | None = None
    bucket_url: str | None = None
    db_path: str | None = None


class SourceTable(_Base):
    name: str
    alias: str | None = None
    path: str | None = None
    select: list[str] | None = Field(default=None, min_length=1)
    filter: str | None = None
    filter_propagate: str | None = None
    incremental: IncrementalBlock | None = None
    explore: list[ExploreSpec] | None = None


class SourceBlock(_Base):
    type: Literal["sql_database", "rest_api", "filesystem", "local_files"]
    tables: list[SourceTable] | None = None
    credentials_file: str | None = None
    dialect: Literal["oracle", "postgres", "mysql", "mssql", "sqlite"] | None = None
    connection_string: str | None = None
    schema_: str | None = Field(default=None, alias="schema")
    destination: DestinationBlock | None = None
    bucket_url: str | None = None
    file_glob: str | None = None
    format: str | None = None
    resource: str | None = None
    endpoint: str | None = None
    base_url: str | None = None
    incremental: IncrementalBlock | None = None
    alias: str | None = None
    select: list[str] | None = Field(default=None, min_length=1)
    filter_defs: dict[str, str] | None = None

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    @model_validator(mode="after")
    def _check_credentials_dialect(self) -> "SourceBlock":
        if self.credentials_file and self.dialect is None:
            raise ValueError(
                "dialect is required when using credentials_file. "
                f"Must be one of {sorted(_VALID_DIALECTS)}"
            )
        return self

    @model_validator(mode="after")
    def _check_filter_propagate(self) -> "SourceBlock":
        if not self.tables:
            return self
        by_key = {(t.alias or t.name): t for t in self.tables}
        for tbl in self.tables:
            if not tbl.filter_propagate:
                continue
            ref = by_key.get(tbl.filter_propagate)
            if ref is None:
                raise ValueError(
                    f"filter_propagate: table '{tbl.filter_propagate}' "
                    f"referenced by '{tbl.alias or tbl.name}' not found in "
                    "source config (match by alias, then name)"
                )
            if not ref.filter:
                raise ValueError(
                    f"filter_propagate: table '{tbl.filter_propagate}' "
                    f"referenced by '{tbl.alias or tbl.name}' has no 'filter' to propagate"
                )
        return self

    @model_validator(mode="after")
    def _check_filter_defs_refs(self) -> "SourceBlock":
        if not self.tables:
            return self
        defs = self.filter_defs or {}
        for tbl in self.tables:
            if not tbl.filter:
                continue
            for name in _REF_PATTERN.findall(tbl.filter):
                if name not in defs:
                    raise ValueError(
                        f"filter_defs: '{name}' referenced via '{{ref:{name}}}' in table "
                        f"'{tbl.alias or tbl.name}' not found in filter_defs"
                    )
        return self


class TransformSpec(_Base):
    type: Literal[
        "rename", "cast", "drop", "udf", "fillna", "clip", "normalize",
        "deduplicate", "filter_rows", "map_values", "allowed_values", "coerce_bool",
    ]
    columns: dict | list | None = None
    file: str | None = None
    function: str | None = None
    args: dict | None = None
    expr: str | None = None
    column: str | None = None
    mapping: dict | None = None
    default: Any = None
    subset: list[str] | None = None
    keep: Literal["first", "last", "none"] | None = None

    @model_validator(mode="after")
    def _check_type_specific(self) -> "TransformSpec":
        t = self.type
        if t == "udf":
            if not self.file:
                raise ValueError("(udf): 'file' is required")
            if not self.function:
                raise ValueError("(udf): 'function' is required")
        if t in ("fillna", "clip"):
            if not isinstance(self.columns, dict):
                raise ValueError(f"({t}): 'columns' must be a dict")
        if t == "normalize":
            if not isinstance(self.columns, dict):
                raise ValueError("(normalize): 'columns' must be a dict")
            for c, op in self.columns.items():
                if op not in _VALID_NORMALIZE_OPS:
                    raise ValueError(
                        f"(normalize): op '{op}' for column '{c}' must be "
                        f"one of {sorted(_VALID_NORMALIZE_OPS)}"
                    )
        if t == "filter_rows":
            if not isinstance(self.expr, str) or not self.expr.strip():
                raise ValueError("(filter_rows): 'expr' must be a non-empty string")
        if t == "map_values":
            if not isinstance(self.column, str) or not self.column.strip():
                raise ValueError("(map_values): 'column' must be a non-empty string")
            if not isinstance(self.mapping, dict):
                raise ValueError("(map_values): 'mapping' must be a dict")
        if t == "allowed_values":
            if not isinstance(self.columns, dict):
                raise ValueError("(allowed_values): 'columns' must be a dict mapping column names to lists")
            for c, vals in self.columns.items():
                if not isinstance(vals, list) or not vals:
                    raise ValueError(f"(allowed_values): values for column '{c}' must be a non-empty list")
        if t == "coerce_bool":
            if not isinstance(self.columns, list) or not self.columns:
                raise ValueError("(coerce_bool): 'columns' must be a non-empty list of column names")
        return self


class SilverTable(_Base):
    source_file: str
    output_file: str
    transforms: list[TransformSpec] | None = None
    explore: list[ExploreSpec] | None = None


class UdfBlock(_Base):
    file: str
    function: str
    args: dict | None = None


class DerivedTable(_Base):
    udf: UdfBlock
    output_file: str
    select: list[str] | None = None
    explore: list[ExploreSpec] | None = None


class DuckdbBlock(_Base):
    enabled: bool | None = None
    path: str | None = None
    mode: Literal["views", "tables"] = "views"

    @model_validator(mode="after")
    def _check_path_if_enabled(self) -> "DuckdbBlock":
        if self.enabled and not self.path:
            raise ValueError("'path' is required when duckdb is enabled")
        return self


class BronzeToSilver(_Base):
    tables: list[SilverTable] = Field(default_factory=list)
    derived_tables: list[DerivedTable] = Field(default_factory=list)
    duckdb: DuckdbBlock | None = None


class MetricSpec(_Base):
    agg: Literal["count", "sum", "mean", "min", "max", "median", "std", "var", "first", "last", "count_distinct"]
    column: str | None = None
    alias: str


class SortSpec(_Base):
    columns: list[str] = Field(min_length=1)
    descending: bool | list[bool] | None = None


class PreAggUdf(_Base):
    file: str
    function: str
    args: dict | None = None


class Aggregation(_Base):
    source_file: str | None = None
    output_file: str | None = None
    pre_agg_udf: PreAggUdf | None = None
    group_by: list[str] | None = None
    metrics: list[MetricSpec] = Field(default_factory=list)
    select: list[str] | None = None
    having: str | None = None
    sort: SortSpec | None = None
    limit: int | None = Field(default=None, gt=0)
    explore: list[ExploreSpec] | None = None

    @field_validator("having")
    @classmethod
    def _having_non_empty(cls, v: str | None) -> str | None:
        if v is not None and not v.strip():
            raise ValueError("having must be a non-empty string")
        return v


class GoldProject(_Base):
    name: str
    aggregations: list[Aggregation] = Field(default_factory=list)


class SilverToGold(_Base):
    projects: list[GoldProject] = Field(default_factory=list)
    duckdb: DuckdbBlock | None = None


class BiExportProject(_Base):
    name: str
    tables: list[str] = Field(min_length=1)


class BiExport(_Base):
    enabled: bool
    projects: list[BiExportProject] = Field(default_factory=list)


class ProjectConfig(_Base):
    pipeline: PipelineBlock
    paths: PathsBlock
    source: SourceBlock | None = None
    sources: list[SourceBlock] | None = None
    destination: DestinationBlock | None = None  # legacy top-level sibling of `source:`, see bronze.py
    bronze_to_silver: BronzeToSilver | None = None
    silver_to_gold: SilverToGold | None = None
    bi_export: BiExport | None = None

    @model_validator(mode="after")
    def _check_source_xor_sources(self) -> "ProjectConfig":
        if self.source is not None and self.sources is not None:
            raise ValueError("use either 'sources:' (list) or 'source:' (dict), not both")
        if self.sources is not None and len(self.sources) == 0:
            raise ValueError("'sources' must be a non-empty list")
        return self
