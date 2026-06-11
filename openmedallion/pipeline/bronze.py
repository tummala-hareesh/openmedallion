"""pipeline/bronze.py — dlt ingestion engine: source → raw Parquet (bronze layer).

Supported sources (``source.type``)
------------------------------------
``sql_database``
    Oracle, Postgres, MySQL, MSSQL, SQLite via SQLAlchemy.

``rest_api``
    Any REST API via dlt's rest_api source (pagination, auth, cursors built-in).

``filesystem``
    Read Parquet or CSV files from a local path or cloud bucket.

``local_files``
    Read local CSV or Parquet files directly (no dlt).  Each table entry
    specifies a ``name`` and a ``path``; files are written straight to
    ``{paths.bronze}/{name}.parquet``.

Supported destinations (``destination.type``)
---------------------------------------------
``filesystem``  Local path or cloud storage — writes Parquet shards.
``duckdb``      Local DuckDB file.
``bigquery``    Google BigQuery.
``snowflake``   Snowflake.

Incremental modes (``source.tables[].incremental.mode``)
---------------------------------------------------------
``replace``  Full overwrite each run.
``append``   Adds only new rows using a cursor column.
``merge``    Upserts on a primary key.

Credential resolution (``source.type == sql_database``)
--------------------------------------------------------
Either ``connection_string`` (raw SQLAlchemy URL, env-var expanded) or the
``credentials_file`` + ``dialect`` pair.  When ``credentials_file`` is given,
openmedallion reads the named YAML file, looks up the ``dialect`` block, and
assembles the connection string internally.
"""
import dlt
import dlt.sources
from dlt.sources.rest_api import rest_api_source
import glob as _glob
import gzip
import io
import polars as pl
from pathlib import Path
from urllib.parse import urlparse

from openmedallion import storage
from openmedallion.config.loader import expand_env_str


# ---------------------------------------------------------------------------
# Credential helpers
# ---------------------------------------------------------------------------

_DIALECT_DRIVERS = {
    "oracle":   "oracle+oracledb",
    "postgres": "postgresql+psycopg2",
    "mysql":    "mysql+pymysql",
    "mssql":    "mssql+pyodbc",
    "sqlite":   "sqlite",
}


def _load_credentials_file(file_path: str, dialect: str) -> dict:
    """Read *dialect* credentials from a YAML file.

    Args:
        file_path: Path to the credentials YAML (absolute or relative to CWD).
        dialect:   Top-level key to read from the file (e.g. ``"oracle"``).

    Returns:
        dict: Credentials block for the requested dialect.

    Raises:
        FileNotFoundError: If the file does not exist.
        KeyError: If *dialect* has no entry in the file.
    """
    import yaml
    p = Path(file_path)
    if not p.exists():
        raise FileNotFoundError(
            f"[bronze] credentials file not found: {p.resolve()}\n"
            f"  Create one from the example:  cp secrets.yaml.example {p}"
        )
    with open(p) as fh:
        all_creds = yaml.safe_load(fh) or {}
    if dialect not in all_creds:
        available = sorted(all_creds.keys())
        raise KeyError(
            f"[bronze] no '{dialect}' entry in {p}. "
            f"Available keys: {available}"
        )
    return all_creds[dialect]


def _build_conn_str(dialect: str, creds: dict) -> str:
    """Assemble a SQLAlchemy connection string from a credentials dict.

    Args:
        dialect: One of ``oracle``, ``postgres``, ``mysql``, ``mssql``, ``sqlite``.
        creds:   Credentials block (keys vary by dialect — see secrets.yaml.example).

    Returns:
        str: A valid SQLAlchemy connection string.

    Raises:
        ValueError: If *dialect* is not recognised.
    """
    if dialect not in _DIALECT_DRIVERS:
        raise ValueError(
            f"[bronze] unsupported dialect '{dialect}'. "
            f"Choose from: {sorted(_DIALECT_DRIVERS)}"
        )
    driver = _DIALECT_DRIVERS[dialect]

    if dialect == "sqlite":
        return f"sqlite:///{creds['path']}"

    host     = creds["host"]
    username = creds["username"]
    password = creds["password"]

    if dialect == "oracle":
        port    = creds.get("port", 1521)
        service = creds["service"]
        return f"{driver}://{username}:{password}@{host}:{port}/?service_name={service}"

    database = creds["database"]

    if dialect == "mssql":
        port        = creds.get("port", 1433)
        drv_name    = creds.get("driver", "ODBC+Driver+17+for+SQL+Server")
        return f"{driver}://{username}:{password}@{host}:{port}/{database}?driver={drv_name}"

    port = creds.get("port", 5432 if dialect == "postgres" else 3306)
    return f"{driver}://{username}:{password}@{host}:{port}/{database}"


def _conn_display(conn_str: str, dialect: str) -> str:
    """Return a safe display string for the connection (no password)."""
    if dialect == "sqlite":
        return conn_str.split("///")[-1]
    try:
        p = urlparse(conn_str)
        host_part = p.hostname or ""
        if p.port:
            host_part += f":{p.port}"
        if p.path:
            host_part += p.path
        return host_part
    except Exception:
        return "(unknown host)"


# ---------------------------------------------------------------------------
# Shard helpers — format-agnostic reading of dlt output files
# ---------------------------------------------------------------------------

def _ls_shards(directory: str) -> list[str]:
    """List all dlt shard files directly inside *directory* (any format)."""
    if storage.is_s3(directory):
        return storage.ls_parquets(directory)
    files: list[str] = []
    for ext in ("*.parquet", "*.jsonl.gz", "*.jsonl", "*.csv"):
        files.extend(_glob.glob(str(Path(directory) / ext)))
    return sorted(files)


def _read_shard(path: str) -> pl.DataFrame:
    """Read one dlt shard file into a Polars DataFrame regardless of format."""
    if path.endswith(".parquet"):
        return storage.read_parquet(path)
    if path.endswith(".jsonl.gz"):
        with gzip.open(path, "rb") as fh:
            return pl.read_ndjson(io.BytesIO(fh.read()))
    if path.endswith(".jsonl"):
        return pl.read_ndjson(path)
    if path.endswith(".csv"):
        return pl.read_csv(path)
    raise ValueError(f"[bronze] unsupported shard format: {path}")


class BronzeLoader:
    """Ingest source data into the bronze Parquet layer.

    Args:
        cfg: Merged project config dict. Must contain ``pipeline.name`` and
            ``paths.bronze``.

        Supports two config forms:

        *New (multi-source)*::

            sources:
              - type: local_files
                tables: [...]
              - type: sql_database
                connection_string: ...
                tables: [...]
                destination:
                  type: filesystem
                  bucket_url: data

        *Legacy (single source)*::

            source:
              type: sql_database
              ...
            destination:
              type: filesystem
              bucket_url: data

        Both forms are normalised to ``sources`` internally.
    """

    def __init__(self, cfg: dict):
        self.pipeline_name = cfg["pipeline"]["name"]
        self.bronze_path   = cfg["paths"]["bronze"]

        # Normalise legacy source: + destination: to sources: list.
        if cfg.get("sources"):
            self.sources: list[dict] = cfg["sources"]
        elif cfg.get("source"):
            src = dict(cfg["source"])
            if cfg.get("destination"):
                src.setdefault("destination", cfg["destination"])
            self.sources = [src]
        else:
            self.sources = []

        # Set per-source during load(); pre-populated from the first source so
        # helpers like _resolve_conn_str() work when called outside load().
        self.src: dict = self.sources[0] if self.sources else {}
        self.dst: dict = self.sources[0].get("destination", {}) if self.sources else {}

        self.explore_enabled: bool = cfg.get("_explore", True)
        self.debug_enabled: bool   = cfg.get("_debug", True)

        if self.debug_enabled:
            print(f"[DEBUG] BronzeLoader: pipeline={self.pipeline_name}, bronze_path={self.bronze_path}, sources={len(self.sources)}")

    def load(self) -> dict[str, str]:
        """Run ingestion for all sources and return a ``{name: path}`` dict."""
        if not self.sources:
            return self._discover_existing()

        print(f"\n── Bronze {'─' * 49}")
        results: dict[str, str] = {}
        for source in self.sources:
            self.src = source
            self.dst = source.get("destination", {})
            src_type = self.src.get("type")

            if self.debug_enabled:
                print(f"[DEBUG] load: src_type={src_type!r}, destination={self.dst.get('type')!r}")

            if src_type == "local_files":
                results.update(self._local_files_load())
            elif src_type:
                pipeline = self._build_pipeline()
                sources  = self._build_sources()
                info = pipeline.run(sources, loader_file_format="parquet")
                print(f"📥  [bronze] dlt pipeline complete: {info}")
                results.update(self._collect_parquets())

        return results

    # ------------------------------------------------------------------
    # local_files source — reads CSV/Parquet directly, no dlt
    # ------------------------------------------------------------------

    def _local_files_load(self) -> dict[str, str]:
        storage.mkdir(self.bronze_path)
        results: dict[str, str] = {}
        for tbl in self.src.get("tables", []):
            name  = tbl["name"]
            alias = tbl.get("alias") or name
            path  = tbl["path"]
            if self.debug_enabled:
                print(f"[DEBUG] local_files: table={name!r}, alias={alias!r}, path={path!r}, select={tbl.get('select')!r}")
            if path.endswith(".csv"):
                df = pl.read_csv(path)
            elif path.endswith(".parquet"):
                df = storage.read_parquet(path)
            else:
                raise ValueError(f"[bronze] unsupported local file format: {path}")
            if cols := tbl.get("select"):
                df = df.select(cols)
            out = storage.join(self.bronze_path, f"{alias}.parquet")
            storage.write_parquet(df, out)
            print(f"📥  [bronze] {path} → {out}  ({len(df)} rows)")
            results[alias] = out
            if self.explore_enabled and (explore_specs := tbl.get("explore")):
                from pathlib import Path as _Path
                from openmedallion.pipeline.explore import _dispatch_reports
                _dispatch_reports(
                    src     = _Path(out),
                    out_dir = _Path(self.bronze_path) / "add-ons",
                    specs   = explore_specs,
                    context = "explore/bronze",
                )
        return results

    def _discover_existing(self) -> dict[str, str]:
        bronze_dir = Path(self.bronze_path)
        if not bronze_dir.exists():
            return {}
        return {p.stem: str(p) for p in sorted(bronze_dir.glob("*.parquet"))}

    # ------------------------------------------------------------------
    # dlt pipeline helpers
    # ------------------------------------------------------------------

    def _build_pipeline(self) -> dlt.Pipeline:
        dst = self.dst
        match dst["type"]:
            case "filesystem":
                destination = dlt.destinations.filesystem(bucket_url=dst["bucket_url"])
            case "duckdb":
                destination = dlt.destinations.duckdb(dst.get("db_path", "bronze.duckdb"))
            case "bigquery":
                destination = dlt.destinations.bigquery()
            case "snowflake":
                destination = dlt.destinations.snowflake()
            case _:
                raise NotImplementedError(f"Destination '{dst['type']}' not wired.")

        return dlt.pipeline(
            pipeline_name=self.pipeline_name,
            destination=destination,
            dataset_name="bronze",
            progress='tqdm'
        )

    def _build_sources(self):
        src_type = self.src["type"]

        if src_type == "sql_database":
            return self._sql_source()

        if src_type == "rest_api":
            return [self._rest_api_source()]

        if src_type == "filesystem":
            from dlt.sources.filesystem import filesystem, read_parquet, read_csv
            fs  = filesystem(bucket_url=self.src["bucket_url"], file_glob=self.src["file_glob"])
            fmt = self.src.get("format", "parquet")
            return fs | (read_parquet() if fmt == "parquet" else read_csv())

        raise NotImplementedError(f"Source type '{src_type}' not wired.")

    # ------------------------------------------------------------------
    # SQL credential resolution + connection probe
    # ------------------------------------------------------------------

    def _resolve_conn_str(self) -> str:
        """Return a ready-to-use SQLAlchemy connection string.

        Prefers ``credentials_file`` + ``dialect`` when present; falls back
        to the raw ``connection_string`` field (env-var expanded).
        """
        if "credentials_file" in self.src:
            dialect = self.src["dialect"]
            if self.debug_enabled:
                print(f"[DEBUG] _resolve_conn_str: using credentials_file={self.src['credentials_file']!r}, dialect={dialect!r}")
            creds = _load_credentials_file(self.src["credentials_file"], dialect)
            return _build_conn_str(dialect, creds)
        if self.debug_enabled:
            print("[DEBUG] _resolve_conn_str: using connection_string (env-expanded)")
        return expand_env_str(self.src["connection_string"])

    def _probe_connection(self, conn_str: str) -> None:
        """Verify DB connectivity and check that configured tables exist in the schema.

        Args:
            conn_str: SQLAlchemy connection string (already resolved).

        Raises:
            ConnectionError: If the database cannot be reached.
            ValueError: If any configured table is not found in the schema.
        """
        from sqlalchemy import create_engine, text, inspect as sa_inspect

        dialect = self.src.get("dialect") or conn_str.split(":")[0]
        schema  = self.src.get("schema") or None
        display = _conn_display(conn_str, dialect)

        label = f"schema {schema}" if schema else "default schema"
        print(f"🔌  Connecting to {dialect} at {display} ({label}) ...")

        try:
            engine = create_engine(conn_str)
            with engine.connect() as con:
                if (dialect == 'oracle'): 
                    con.execute(text("SELECT 1 FROM DUAL"))
                else:
                    con.execute(text("SELECT 1"))
            db_tables = set(sa_inspect(engine).get_table_names(schema=schema))
        except Exception as exc:
            raise ConnectionError(
                f"[bronze] DB connection failed: {exc}"
            ) from exc

        configured = [t["name"] for t in self.src.get("tables", [])]
        missing    = [t for t in configured if t not in db_tables]
        if self.debug_enabled:
            print(f"[DEBUG] _probe_connection: db_tables_count={len(db_tables)}, configured={configured}, missing={missing}")

        for name in configured:
            mark = "✅" if name not in missing else "❌"
            print(f"  {mark}  {name}")

        if missing:
            raise ValueError(
                f"[bronze] table(s) not found in {label}: {missing}. "
                f"Check spelling in your bronze.yaml."
            )

    def _sql_source(self):
        from dlt.sources.sql_database import sql_table
        from sqlalchemy import text as _sa_text

        conn   = self._resolve_conn_str()
        self._probe_connection(conn)
        schema = self.src.get("schema") or None
        tables_cfg = self.src.get("tables", [])

        resources = []
        for tbl in tables_cfg:
            inc = tbl.get("incremental", {})
            mode = inc.get("mode", "replace")
            if self.debug_enabled:
                print(f"[DEBUG] _sql_source: table={tbl['name']!r}, mode={mode!r}, filter={tbl.get('filter')!r}, select={tbl.get('select')!r}")
            kwargs = dict(
                credentials=conn,
                schema=schema,
                table=tbl["name"],
            )

            if mode == "append":
                kwargs["incremental"] = dlt.sources.incremental(
                    inc["cursor_column"],
                    initial_value=inc.get("initial_value"),
                )
            elif mode == "merge":
                kwargs["write_disposition"] = "merge"
                kwargs["primary_key"]       = inc["primary_key"]
                if (mk := inc.get("merge_key")):
                    kwargs["merge_key"]     = mk

            # filter: push WHERE clause to the DB via query_adapter_callback.
            # select: NOT applied here — sel.with_only_columns() inside the
            # adapter corrupts dlt's incremental cursor tracking. Column pruning
            # for SQL sources is applied in _collect_parquets() instead.
            filter_clause     = tbl.get("filter")
            filter_propagate  = tbl.get("filter_propagate", None)

            if filter_clause and filter_propagate:
                raise ValueError(
                    f"[bronze] table '{tbl['name']}': use either 'filter' or 'filter_propagate', not both."
                )

            if filter_propagate:
                if self.debug_enabled:
                    print(f"[DEBUG] _sql_source: filter_propagate={filter_propagate!r} for table={tbl['name']!r}")
                ref = next(
                    (t for t in tables_cfg if (t.get("alias") or t["name"]) == filter_propagate),
                    None,
                )
                if ref is None:
                    raise ValueError(
                        f"[bronze] filter_propagate: table '{filter_propagate}' not found in source config "
                        f"(match by alias, then name)."
                    )
                ref_filter = ref.get("filter")
                if not ref_filter:
                    raise ValueError(
                        f"[bronze] filter_propagate: table '{filter_propagate}' has no 'filter' to propagate."
                    )
                join_key = inc.get("merge_key") or inc.get("primary_key")
                if not join_key:
                    raise ValueError(
                        f"[bronze] filter_propagate on '{tbl['name']}': set 'primary_key' or 'merge_key' "
                        f"under 'incremental' so the join column can be determined."
                    )
                if isinstance(join_key, list):
                    join_key = join_key[0]

                filter_clause = (
                    f"{join_key} IN ("
                    f"SELECT {join_key} FROM {filter_propagate} WHERE {ref_filter})"
                )

            if filter_clause:
                if self.debug_enabled:
                    print(f"[DEBUG] _sql_source: applying filter to {tbl['name']!r}: {filter_clause!r}")
                from sqlalchemy import text as _sa_text
                kwargs["query_adapter_callback"] = (
                    lambda sel, _t, _sql=filter_clause: sel.where(_sa_text(_sql))
                )
            resources.append(sql_table(**kwargs)) # pyright: ignore[reportArgumentType]

        return resources

    def _rest_api_source(self):
        resource = self.src["resource"]
        resource_cfg: dict = {
            "name": resource,
            "endpoint": self.src.get("endpoint", resource),
        }
        if self.debug_enabled:
            print(f"[DEBUG] _rest_api_source: resource={resource!r}, base_url={self.src.get('base_url')!r}, endpoint={resource_cfg['endpoint']!r}")
        inc = self.src.get("incremental")
        if inc:
            if inc["mode"] == "append":
                resource_cfg["incremental"] = dlt.sources.incremental(
                    inc["cursor_column"], initial_value=inc.get("initial_value")
                )
            elif inc["mode"] == "merge":
                resource_cfg["write_disposition"] = "merge"
                resource_cfg["primary_key"]       = inc["primary_key"]
                if mk := inc.get("merge_key"):
                    resource_cfg["merge_key"]     = mk

        return rest_api_source(
            {"client": {"base_url": self.src["base_url"]}, "resources": [resource_cfg]} # pyright: ignore[reportArgumentType]
        )

    def _collect_parquets(self) -> dict[str, str]:
        results = {}
        tables_cfg  = self.src.get("tables", [])
        table_names = [t["name"] for t in tables_cfg]

        # alias: user-defined output name; falls back to source table name
        per_table_alias   = {t["name"]: t.get("alias") or t["name"] for t in tables_cfg}

        if not table_names and self.src.get("resource"):
            table_names = [self.src["resource"]]
            # REST/filesystem: top-level alias applies to the single resource
            resource_alias = self.src.get("alias") or self.src["resource"]
            per_table_alias[self.src["resource"]] = resource_alias

        # Per-table select and explore lookups (SQL / local_files sources).
        # Filesystem / REST sources use a single top-level select key instead.
        per_table_select  = {t["name"]: t["select"]  for t in tables_cfg if t.get("select")}
        per_table_explore = {t["name"]: t["explore"] for t in tables_cfg if t.get("explore")}

        # dlt always writes shards to {bucket_url}/{dataset_name}/{table}/
        # dataset_name is hardcoded as "bronze" in _build_pipeline above
        bucket_url = self.dst["bucket_url"]

        for name in table_names:
            alias     = per_table_alias.get(name, name)
            shard_dir = storage.join(bucket_url, "bronze", name)
            shards    = _ls_shards(shard_dir)
            if self.debug_enabled:
                print(f"[DEBUG] _collect_parquets: table={name!r}, alias={alias!r}, shard_dir={shard_dir!r}, shards_found={len(shards)}, select={per_table_select.get(name) or self.src.get('select')!r}")
            if not shards:
                print(f"⚠️   [bronze] no shards found for '{name}' at {shard_dir}")
                continue

            dfs = [_read_shard(s) for s in shards]
            df  = pl.concat(dfs) if len(dfs) > 1 else dfs[0]
            cols = per_table_select.get(name) or self.src.get("select")
            if cols:
                df = df.select(cols)

            out = storage.join(self.bronze_path, f"{alias}.parquet")
            storage.mkdir(self.bronze_path)
            storage.write_parquet(df, out)
            print(f"📥  [bronze] merged {len(shards)} shard(s) → {out}")
            results[alias] = out
            if self.explore_enabled and (explore_specs := per_table_explore.get(name)):
                from pathlib import Path as _Path
                from openmedallion.pipeline.explore import _dispatch_reports
                _dispatch_reports(
                    src     = _Path(out),
                    out_dir = _Path(self.bronze_path) / "add-ons",
                    specs   = explore_specs,
                    context = "explore/bronze",
                )

        return results
