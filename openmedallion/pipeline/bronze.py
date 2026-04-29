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
"""
import dlt
import dlt.sources
from dlt.sources.rest_api import rest_api_source
import glob as _glob
import gzip
import io
import polars as pl
from pathlib import Path

from openmedallion import storage
from openmedallion.config.loader import expand_env_str


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
            ``paths.bronze``. ``source`` and ``destination`` are optional
            (omitting them causes bronze to no-op and return existing files).
    """

    def __init__(self, cfg: dict):
        self.src           = cfg.get("source", {})
        self.dst           = cfg.get("destination", {})
        self.pipeline_name = cfg["pipeline"]["name"]
        self.bronze_path   = cfg["paths"]["bronze"]

    def load(self) -> dict[str, str]:
        """Run ingestion and return a ``{name: path}`` dict for every table."""
        src_type = self.src.get("type")

        if src_type == "local_files":
            return self._local_files_load()

        if not src_type:
            # No source configured — discover any pre-existing bronze parquets.
            return self._discover_existing()

        pipeline = self._build_pipeline()
        sources  = self._build_sources()
        info = pipeline.run(sources, loader_file_format="parquet")
        print(f"📥  [bronze] dlt pipeline complete: {info}")
        return self._collect_parquets()

    # ------------------------------------------------------------------
    # local_files source — reads CSV/Parquet directly, no dlt
    # ------------------------------------------------------------------

    def _local_files_load(self) -> dict[str, str]:
        storage.mkdir(self.bronze_path)
        results: dict[str, str] = {}
        for tbl in self.src.get("tables", []):
            name = tbl["name"]
            path = tbl["path"]
            if path.endswith(".csv"):
                df = pl.read_csv(path)
            elif path.endswith(".parquet"):
                df = storage.read_parquet(path)
            else:
                raise ValueError(f"[bronze] unsupported local file format: {path}")
            out = storage.join(self.bronze_path, f"{name}.parquet")
            storage.write_parquet(df, out)
            print(f"📥  [bronze] local   {path} → {out}  ({len(df)} rows)")
            results[name] = out
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

    def _sql_source(self):
        from dlt.sources.sql_database import sql_table

        conn = expand_env_str(self.src["connection_string"])
        schema = self.src.get("schema")
        tables_cfg = self.src.get("tables", [])

        resources = []
        for tbl in tables_cfg:
            inc  = tbl.get("incremental", {})
            mode = inc.get("mode", "replace")

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

            resources.append(sql_table(**kwargs))

        return resources

    def _rest_api_source(self):
        resource = self.src["resource"]
        resource_cfg: dict = {
            "name": resource,
            "endpoint": self.src.get("endpoint", resource),
        }
        inc = self.src.get("incremental")
        if inc:
            if inc["mode"] == "append":
                resource_cfg["incremental"] = dlt.sources.incremental(
                    inc["cursor_column"], initial_value=inc.get("initial_value")
                )
            elif inc["mode"] == "merge":
                resource_cfg["write_disposition"] = "merge"
                resource_cfg["primary_key"]       = inc["primary_key"]

        return rest_api_source(
            {"client": {"base_url": self.src["base_url"]}, "resources": [resource_cfg]}
        )

    def _collect_parquets(self) -> dict[str, str]:
        results = {}
        table_names = [t["name"] for t in self.src.get("tables", [])]

        if not table_names and self.src.get("resource"):
            table_names = [self.src["resource"]]

        # dlt always writes shards to {bucket_url}/{dataset_name}/{table}/
        # dataset_name is hardcoded as "bronze" in _build_pipeline above
        bucket_url = self.dst["bucket_url"]

        for name in table_names:
            shard_dir = storage.join(bucket_url, "bronze", name)
            shards    = _ls_shards(shard_dir)
            if not shards:
                print(f"⚠️   [bronze] no shards found for '{name}' at {shard_dir}")
                continue

            dfs = [_read_shard(s) for s in shards]
            df  = pl.concat(dfs) if len(dfs) > 1 else dfs[0]

            out = storage.join(self.bronze_path, f"{name}.parquet")
            storage.mkdir(self.bronze_path)
            storage.write_parquet(df, out)
            print(f"📥  [bronze] merged {len(shards)} shard(s) → {out}")
            results[name] = out

        return results
